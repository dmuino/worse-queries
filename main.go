package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	chagent "stash.corp.netflix.com/cldmta/clickhouse-agent-libs"
	"strconv"
	"time"
)

const oldAsg = "clickhouse-logs-v000"
const newAsg = "clickhouse-logs-v001"

var logger = chagent.GetLogger("main")

func getBaseUrl(env *chagent.NetflixEnv) string {
	return fmt.Sprintf("https://slotting-%s.%s.%s.netflix.net", env.AccountType, env.Region, env.Account)
}

func getNodesFromAsg(env *chagent.NetflixEnv, asg string) []chagent.InstanceInfo {
	baseUrl := getBaseUrl(env)
	url := fmt.Sprintf("%s/api/v1/autoScalingGroups/%s", baseUrl, asg)
	logger.Debugf("Getting all nodes for asg=%s using url: %s", asg, url)

	// make http get request to get all nodes in our ASG from the slotting service
	// and return the list of nodes
	resp, err := http.Get(url)
	logger.CheckErr(err)

	body, err := io.ReadAll(resp.Body)
	logger.CheckErr(err)

	// parse body as AsgInfo
	var asgInfo chagent.AsgInfo
	err = json.Unmarshal(body, &asgInfo)
	logger.CheckErr(err)
	return asgInfo.Instances
}

func pickNodeFromAsg(env *chagent.NetflixEnv, slotInfo *chagent.SlotInfo, asg string) chagent.InstanceInfo {
	nodes := getNodesFromAsg(env, asg)
	if len(nodes) == 0 {
		logger.Fatalf("No nodes found for asg: %s", asg)
	}
	// pick the first node
	return nodes[0]
}

type ResultMetadata struct {
	hostname      string
	profileEvents map[string]uint64
	queryDuration time.Duration
	readRows      int
	readBytes     int
}

var queriesToIds = make(map[string]string)
var idsToQueries = make(map[string]string)
var idsToMeta = make(map[string]QueryMeta)

var connections = make(map[string]*sql.DB)

func getConnection(node chagent.InstanceInfo) *sql.DB {
	var db *sql.DB
	address := fmt.Sprintf("[%s]:9000", node.IPv6Address)
	db = clickhouse.OpenDB(&clickhouse.Options{
		Addr:        []string{address},
		DialTimeout: 5 * time.Second,
		Protocol:    clickhouse.Native,
	})

	err := db.Ping()
	if err == nil {
		logger.Infof("Connected to clickhouse-server")
		return db
	}
	return db
}

func getQueryLogQuery(queryId string, isNew bool) string {
	cluster := "clickhouse_logs"
	return fmt.Sprintf(`SELECT hostname(),
		read_rows,
		read_bytes,
		query_duration_ms,
		ProfileEvents
    FROM clusterAllReplicas(%s, system.query_log)
    WHERE (event_date >= today()) AND (initial_query_id = '%s') AND (type = 'QueryFinish')`,
		cluster, queryId)
}

type Results struct {
	queryId       string
	numResults    int
	elapsed       time.Duration
	serverElapsed time.Duration
	perHost       []ResultMetadata
}

func sendOneQuery(conn *sql.DB, ctx context.Context, query string) (int, time.Duration) {
	start := time.Now()
	res, err := conn.QueryContext(ctx, query)
	if err != nil {
		logger.Fatalf("Error sending query: %s", err)
	}
	numResults := 0
	for res.Next() {
		// read all results
		numResults++
	}
	_ = res.Close()
	elapsed := time.Since(start)
	logger.Debugf("Query: %s - elapsed: %s", query, elapsed)
	return numResults, elapsed
}

// reimplements a basic StringLatin1.hashCode() from java
func hash(s string) int32 {
	// hash the ascii string to an int
	n := len(s)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return int32(s[0] & 0xFF)
	}

	var h int32 = 0
	for i := 0; i < n; i++ {
		h = 31*h + int32(s[i]&0xFF)
	}
	return h
}

func getTagIndex(tag string) int {
	// absolute value: hash(tag) % 31
	maybeNegative := hash(tag) % 31
	if maybeNegative < 0 {
		return int(-maybeNegative)
	}
	return int(maybeNegative)
}

func splitTags(query string) string {
	// replace tags['foo'] with tags0['foo'], tags1['foo'], etc
	// based on the result of hash('foo') % 31
	re := regexp.MustCompile(`tags\['([^']+)'\]`)
	return re.ReplaceAllStringFunc(query, func(tag string) string {
		matches := re.FindStringSubmatch(tag)
		if len(matches) > 1 {
			tagName := matches[1]
			index := getTagIndex(tagName)
			return fmt.Sprintf("tags%d['%s']", index, tagName)
		}
		return tag
	})
}

func sendToNewCluster(conn *sql.DB, query string) (string, int, time.Duration) {
	uuid := uuid.NewString()
	ctx := clickhouse.Context(context.Background(), clickhouse.WithQueryID(uuid))
	actualQuery := splitTags(query)
	numResults, elapsed := sendOneQuery(conn, ctx, actualQuery)
	return uuid, numResults, elapsed
}

func sendQueryToCluster(node chagent.InstanceInfo, isNew bool, query string) Results {
	// get a connection to the node
	conn, ok := connections[node.InstanceId]
	if !ok {
		conn = getConnection(node)
		connections[node.InstanceId] = conn
	}
	if conn == nil {
		logger.Fatalf("Could not get a connection to the node: %v", node)
	}

	numResults := 0
	queryId := ""
	var elapsed time.Duration
	if isNew {
		queryId, numResults, elapsed = sendToNewCluster(conn, query)
	} else {
		queryId, ok = queriesToIds[query]
		if !ok {
			logger.Fatalf("Need to send to new asg first to generate UUID")
		}
		logger.Debugf("Sending query: %s to old node %s (%s)", query, node.InstanceId, queryId)
		ctx := clickhouse.Context(context.Background(), clickhouse.WithQueryID(queryId))
		numResults, elapsed = sendOneQuery(conn, ctx, query)
	}
	queriesToIds[query] = queryId
	idsToQueries[queryId] = query

	allMetadata := make([]ResultMetadata, 0)
	// get the metadata results from system.query_log
	meta := ResultMetadata{}
	oldNew := "old"
	if isNew {
		oldNew = "new"
	}
	// send "system flush logs" command to ensure we have query_logs
	_, _ = conn.Exec("SYSTEM FLUSH LOGS")

	logger.Infof("Getting query log results for %s queryId: %s - %s", oldNew, queryId, query)
	res, err := conn.Query(getQueryLogQuery(queryId, isNew))
	if err != nil {
		logger.Fatalf("Error getting query log results: %v", err)
	}
	defer func(res *sql.Rows) {
		_ = res.Close()
	}(res)
	found := false
	serverElapsed := time.Duration(0)
	for res.Next() {
		found = true
		var queryDurationMillis int
		err = res.Scan(&meta.hostname, &meta.readRows, &meta.readBytes, &queryDurationMillis, &meta.profileEvents)
		if err != nil {
			logger.Fatalf("Error scanning query log results: %v", err)
		}
		meta.queryDuration = time.Duration(queryDurationMillis) * time.Millisecond
		serverElapsed = max(serverElapsed, meta.queryDuration)
		allMetadata = append(allMetadata, meta)
	}
	if !found {
		logger.Errorf("No results found for %s queryId: %s - %s", oldNew, queryId, query)
	}

	logger.Debugf("Done getting query log results for %s queryId: %s", oldNew, queryId)
	return Results{queryId, numResults, elapsed, serverElapsed, allMetadata}
}

type QueryStats struct {
	numResults    int
	elapsed       time.Duration
	serverElapsed time.Duration
	hours         int
}

func getHoursFromQuery(query string) int {
	// all queries generated by firewood include a predicate of the form:
	// dateTime >= toDateTime64('1714503240000000000', 9) AND dateTime <= toDateTime64('1714506900000000000', 9)
	// we can extract the hours from the start and end times
	// and return the difference
	p := regexp.MustCompile(`dateTime >= toDateTime64\('(\d+)', 9\) AND dateTime <= toDateTime64\('(\d+)', 9\)`)
	matches := p.FindStringSubmatch(query)
	if len(matches) < 3 {
		return -1
	}
	start, end := matches[1], matches[2]
	// start is the number of nanoseconds since the epoch, convert to a time
	startNanos, err := strconv.ParseInt(start, 10, 64)
	if err != nil {
		logger.Errorf("Error parsing start time: %s", err)
		return -1
	}
	endNanos, err := strconv.ParseInt(end, 10, 64)
	if err != nil {
		logger.Errorf("Error parsing end time: %s", err)
		return -1
	}
	// elapsed nanoseconds
	hours := (endNanos - startNanos) / 1e9 / 3600
	return int(hours)
}

func analyzeQueries(node chagent.InstanceInfo, isNew bool, queries map[QueryMeta][]string) (map[QueryMeta][]Results, map[string]QueryStats) {
	result := make(map[QueryMeta][]Results)
	queryStats := make(map[string]QueryStats)
	oldNew := "old"
	if isNew {
		oldNew = "new"
	}
	// send queries to old asg
	for queryMeta, queryList := range queries {
		logger.Infof("Sending queries to %s asg for table %s of type %s",
			oldNew,
			queryMeta.Table, queryTypeStr(queryMeta.Type))
		for _, query := range queryList {
			resultMeta := sendQueryToCluster(node, isNew, query)
			queryId := resultMeta.queryId
			queryStats[queryId] = QueryStats{
				numResults:    resultMeta.numResults,
				elapsed:       resultMeta.elapsed,
				serverElapsed: resultMeta.serverElapsed,
				hours:         getHoursFromQuery(query),
			}
			result[queryMeta] = append(result[queryMeta], resultMeta)
		}
	}
	return result, queryStats
}

func writeResults(oldNew string, results map[QueryMeta][]Results) error {
	// create results/old or results/new directory
	isNew := oldNew == "new"

	for queryMeta, queryResults := range results {
		if len(queryResults) == 0 {
			logger.Infof("No results for table %s of type %s", queryMeta.Table, queryTypeStr(queryMeta.Type))
			continue
		}
		dir := getDirForQuery(queryMeta, isNew)
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return err
		}
		for _, result := range queryResults {
			fileName := fmt.Sprintf("%s/%s.txt", dir, result.queryId)
			file, err := os.Create(fileName)
			if err != nil {
				return err
			}

			query := idsToQueries[result.queryId]
			_, _ = file.WriteString("------------------------------\n")
			_, _ = file.WriteString(fmt.Sprintf("Query ID: %s -- %s\n", result.queryId, query))
			_, _ = file.WriteString(fmt.Sprintf("Num Results: %d\n", result.numResults))
			_, _ = file.WriteString(fmt.Sprintf("Client Elapsed: %s\n", result.elapsed))
			_, _ = file.WriteString(fmt.Sprintf("Server Elapsed: %s\n", result.serverElapsed))
			for _, r := range result.perHost {
				_, _ = file.WriteString(fmt.Sprintf("Hostname: %s\n", r.hostname))
				_, _ = file.WriteString(fmt.Sprintf("Read Rows: %d\n", r.readRows))
				_, _ = file.WriteString(fmt.Sprintf("Read Bytes: %d\n", r.readBytes))
				_, _ = file.WriteString(fmt.Sprintf("Query Duration: %s\n", r.queryDuration))
				_, _ = file.WriteString(fmt.Sprintf("Profile Events: %v\n------\n", r.profileEvents))
			}
			_ = file.Close()
		}
	}
	return nil
}

func selectQueries(rawList []string, max int) []string {
	queryList := make([]string, 0)
	for _, query := range rawList {
		// remove queries that use more than 10 hours
		if getHoursFromQuery(query) <= 10 {
			queryList = append(queryList, query)
		}
	}
	if len(queryList) <= max {
		return queryList
	}
	return queryList[:max]
}

func main() {
	logger.SetLevel(chagent.DEBUG)
	env := chagent.NewNetflixEnv()
	env.Environment = "test"
	env.Account = "ieptest"
	slotInfo := chagent.NewSlotInfo(env)
	slotInfo.SetLevel(chagent.DEBUG)

	// get one node for the old deployment
	oldNode := pickNodeFromAsg(env, slotInfo, oldAsg)
	logger.Infof("Will use node %s for old deployment", oldNode.InstanceId)

	newNode := pickNodeFromAsg(env, slotInfo, newAsg)
	logger.Infof("Will use node %s for new deployment", newNode.InstanceId)

	// read all queries to be sent to both asgs
	queries, err := ReadQueries("queries.txt")
	if err != nil {
		logger.Fatalf("Error reading queries: %s", err)
	}

	for queryMeta, queryList := range queries {
		// log how many queries we have for each type
		logger.Infof("Found %d queries for table %s of type %s", len(queryList), queryMeta.Table,
			queryTypeStr(queryMeta.Type))
	}

	const MaxQueriesPerType = 5
	for queryMeta, queryList := range queries {
		maxNumber := MaxQueriesPerType
		if queryMeta.Type == QueryTypeFilter || queryMeta.Type == QueryTypeFilterTags {
			maxNumber *= 2
		}
		queries[queryMeta] = selectQueries(queryList, maxNumber)
	}

	analyzedNewQueries, newStats := analyzeQueries(newNode, true, queries)
	analyzedOldQueries, oldStats := analyzeQueries(oldNode, false, queries)
	for queryMeta, queryResults := range analyzedOldQueries {
		for _, queryResult := range queryResults {
			idsToMeta[queryResult.queryId] = queryMeta
		}
	}

	// write the results to disk
	err = writeResults("old", analyzedOldQueries)
	if err != nil {
		logger.Fatalf("Error writing old results: %s", err)
	}
	err = writeResults("new", analyzedNewQueries)
	if err != nil {
		logger.Fatalf("Error writing new results: %s", err)
	}

	logger.Infof("-----------------------------")
	// print the times for each query
	file, err := os.Create("results/summary.txt")
	if err != nil {
		logger.Fatalf("Error creating summary file: %s", err)
	}
	for queryId, oldStat := range oldStats {
		newStat, ok := newStats[queryId]
		if !ok {
			logger.Errorf("No stats found for queryId: %s", queryId)
			continue
		}
		meta, ok := idsToMeta[queryId]
		if !ok {
			logger.Errorf("No stats found for queryId: %s", queryId)
			continue
		}
		name := fmt.Sprintf("%s_%s/%s", meta.Table, queryTypeStr(meta.Type), queryId)
		clientElapsedStr := fmt.Sprintf("%s", newStat.elapsed)
		tooSlow := oldStat.serverElapsed.Nanoseconds() > newStat.elapsed.Nanoseconds()*2
		interesting := oldStat.numResults != newStat.numResults || tooSlow
		interestingStr := ""
		if interesting {
			interestingStr = ">>>> "
		}
		line := fmt.Sprintf("%sQuery: %s hours=%d - Old: %s %d, New: Server %s Client %s %d", interestingStr, name, oldStat.hours, oldStat.elapsed,
			oldStat.numResults, newStat.serverElapsed, clientElapsedStr, newStat.numResults)
		if interesting || newStat.serverElapsed >= 1*time.Second {
			logger.Infof(line)
		}
		_, _ = file.WriteString(line)
		_, _ = file.WriteString("\n")
	}
	_ = file.Close()
	logger.Infof("Sleeping for 1 minute to allow the logs to be written to our backend")
	time.Sleep(1 * time.Minute)

	writeLogs(false, analyzedOldQueries)
	writeLogs(true, analyzedNewQueries)
	writeTextLogs(newNode)
}

func writeTextLogs(node chagent.InstanceInfo) {
	textLogQuery := `SELECT
    query_id,
    hostname(),
    event_time_microseconds,
    thread_name,
    thread_id,
    level,
    query_id,
    logger_name,
    message
FROM clusterAllReplicas('clickhouse_logs', system.text_log)
WHERE query_id IN (?)
ORDER BY event_time_microseconds ASC`
	conn := getConnection(node)

	queryIds := make([]string, 0)
	for queryId := range idsToQueries {
		queryIds = append(queryIds, queryId)
	}
	logger.Infof("Getting text logs for %d queries: %v", len(queryIds), queryIds)
	logger.Infof("Query: %s", textLogQuery)
	rows, err := conn.Query(textLogQuery, queryIds)
	if err != nil {
		logger.Fatalf("Error getting text logs: %s", err)
	}
	for rows.Next() {
		var queryId, hostname, threadName, level, loggerName, message, threadId string
		var eventTime int64
		err = rows.Scan(&queryId, &hostname, &eventTime, &threadName, &threadId, &level, &loggerName, &message)
		if err != nil {
			logger.Fatalf("Error scanning text logs: %s", err)
		}
		fmt.Printf("QueryId: %s, Hostname: %s, EventTime: %d, ThreadName: %s, ThreadId: %s, Level: %s, LoggerName: %s, Message: %s\n",
			queryId, hostname, eventTime, threadName, threadId, level, loggerName, message)
	}
	_ = rows.Close()
}

func writeLogs(isNew bool, analyzedQueries map[QueryMeta][]Results) {
	// for each query in analyzedOldQueries, get the results from the clickhouse nodes
	// and write them to disk
	for meta, queryResults := range analyzedQueries {
		for _, results := range queryResults {
			logResults := getResultsFromNode(isNew, results.queryId)
			dir := getDirForQuery(meta, isNew)
			fileName := fmt.Sprintf("%s/logs_%s.txt", dir, results.queryId)
			file, err := os.Create(fileName)
			if err != nil {
				logger.Errorf("Error creating file: %s", err)
				continue
			}
			_, _ = file.Write(logResults)
			_ = file.Close()
		}
	}
}

func getDirForQuery(meta QueryMeta, isNew bool) string {
	oldNew := "old"
	if isNew {
		oldNew = "new"
	}
	return fmt.Sprintf("results/%s/%s_%s", oldNew, meta.Table, queryTypeStr(meta.Type))
}

func getResultsFromNode(isNew bool, queryId string) []byte {
	// execute the external command nflxlog to get the logs
	query := "nf.env,test,:eq,nf.app,clickhouse,:eq,:and,formattedMessage," +
		queryId + ",:contains,:and"

	if isNew {
		return getOutFromCommand("nflxlog", "q", query, "-s", "e-30min")
	} else {
		return getOutFromCommand("nflxlog", "q", query, "-s", "e-30min", "--env", "test")
	}
}

func getOutFromCommand(name string, args ...string) []byte {
	// log the command
	logger.Infof("Running command: %s %v", name, args)
	cmd := exec.Command(name, args...)
	out, err := cmd.Output()
	if err != nil {
		logger.Errorf("Error running command %s: %s", name, err)
	}
	return out
}
