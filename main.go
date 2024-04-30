package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"os"
	"os/exec"
	"regexp"
	"slices"
	chagent "stash.corp.netflix.com/cldmta/clickhouse-agent-libs"
	"strconv"
	"time"
)

const oldCluster = "clickhouse-logs-r1"
const smtCluster = "clickhouse-iep"

var logger = chagent.GetLogger("main")

func pickNodeFromCluster(env *chagent.NetflixEnv, slotInfo *chagent.SlotInfo, cluster string) chagent.InstanceInfo {
	clusterInfo := slotInfo.GetAllNodesInCluster(env, cluster)
	if len(clusterInfo) == 0 {
		logger.Fatalf("No nodes found in cluster %s", cluster)
	}
	return clusterInfo[0]
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

const OldSettings = "SETTINGS enable_positional_arguments = 1, log_queries = 1, skip_unavailable_shards = 1"
const NewSettings = OldSettings +
	", max_parallel_replicas = 100, allow_experimental_parallel_reading_from_replicas = 1" +
	", use_hedged_requests = 0, cluster_for_parallel_replicas = 'default'"

func withFixedTableName(query string, isNew bool) string {
	// replace the table name if
	var settings string
	q := query
	if isNew {
		settings = NewSettings
		// replace the table name with one that does not have the _distributed suffix
		p := regexp.MustCompile(`FROM (insight_\w+)_distributed`)
		q = p.ReplaceAllString(q, "FROM ${1}_prod")
	} else {
		settings = OldSettings
	}
	return q + " " + settings
}

func getQueryLogQuery(queryId string, isNew bool) string {
	cluster := "default"
	if !isNew {
		cluster = "clickhouse_logs"
	}
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
	queryId    string
	numResults int
	elapsed    time.Duration
	perHost    []ResultMetadata
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

	queryId, ok := queriesToIds[query]
	if ok {
		logger.Infof("Reusing query id %s for query: %s", queryId, query)
	} else {
		queryId = uuid.NewString()
		logger.Infof("Generated queryId %s for query: %s", queryId, query)
		queriesToIds[query] = queryId
		idsToQueries[queryId] = query
	}

	finalQuery := withFixedTableName(query, isNew)

	oldNew := "old"
	if isNew {
		oldNew = "new"
	}
	logger.Infof("Sending query: %s to %s node %s (%s)", finalQuery, oldNew, node.InstanceId, queryId)

	ctx := clickhouse.Context(context.Background(), clickhouse.WithQueryID(queryId))
	start := time.Now()
	res, err := conn.QueryContext(ctx, finalQuery)
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
	logger.Debugf("Done sending query: %s to %s node %s. Received %d results back", finalQuery, oldNew, node.InstanceId, numResults)

	allMetadata := make([]ResultMetadata, 0)
	// get the metadata results from system.query_log
	meta := ResultMetadata{}
	logger.Infof("Getting query log results for %s queryId: %s - %s", oldNew, queryId, finalQuery)
	res, err = conn.Query(getQueryLogQuery(queryId, isNew))
	if err != nil {
		logger.Fatalf("Error getting query log results: %v", err)
	}
	defer func(res *sql.Rows) {
		_ = res.Close()
	}(res)
	found := false
	for res.Next() {
		found = true
		var queryDurationMillis int
		err = res.Scan(&meta.hostname, &meta.readRows, &meta.readBytes, &queryDurationMillis, &meta.profileEvents)
		if err != nil {
			logger.Fatalf("Error scanning query log results: %v", err)
		}
		meta.queryDuration = time.Duration(queryDurationMillis) * time.Millisecond
		allMetadata = append(allMetadata, meta)
	}
	if !found {
		logger.Errorf("No results found for %s queryId: %s - %s", oldNew, queryId, query)
	}

	logger.Debugf("Done getting query log results for %s queryId: %s", oldNew, queryId)
	return Results{queryId, numResults, elapsed, allMetadata}
}

type QueryStats struct {
	numResults int
	elapsed    time.Duration
	hours      int
}

func getHoursFromQuery(query string) int {
	// queries include a predicate of the form:
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
	// send queries to old cluster
	for queryMeta, queryList := range queries {
		logger.Infof("Sending queries to %s cluster for table %s of type %s",
			oldNew,
			queryMeta.Table, queryTypeStr(queryMeta.Type))
		for _, query := range queryList {
			resultMeta := sendQueryToCluster(node, isNew, query)
			queryId := resultMeta.queryId
			queryStats[queryId] = QueryStats{
				numResults: resultMeta.numResults,
				elapsed:    resultMeta.elapsed,
				hours:      getHoursFromQuery(query),
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

			query := withFixedTableName(idsToQueries[result.queryId], isNew)
			_, _ = file.WriteString("------------------------------\n")
			_, _ = file.WriteString(fmt.Sprintf("Query ID: %s -- %s\n", result.queryId, query))
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

func selectQueries(queryList []string, max int) []string {
	if len(queryList) <= max {
		return queryList
	}

	// filter out the queries that are over 4 hours
	filtered := make([]string, 0)
	for _, query := range queryList {
		hours := getHoursFromQuery(query)
		if hours <= 4 {
			filtered = append(filtered, query)
		}
	}

	toReturn := filtered[:max]
	slices.Reverse(toReturn)
	return toReturn
}

func main() {
	env := chagent.NewNetflixEnv()
	env.Environment = "prod"
	env.Account = "iepprod"
	slotInfo := chagent.NewSlotInfo(env)
	slotInfo.SetLevel(chagent.DEBUG)

	// get one node for the old deployment
	oldNode := pickNodeFromCluster(env, slotInfo, oldCluster)
	logger.Infof("Will use node %s for old deployment", oldNode.InstanceId)

	newNode := pickNodeFromCluster(env, slotInfo, smtCluster)
	logger.Infof("Will use node %s for new deployment", newNode.InstanceId)

	// read all queries to be sent to both clusters
	queries, err := ReadQueries("queries.txt")
	if err != nil {
		logger.Fatalf("Error reading queries: %s", err)
	}

	for queryMeta, queryList := range queries {
		// log how many queries we have for each type
		logger.Infof("Found %d queries for table %s of type %s", len(queryList), queryMeta.Table,
			queryTypeStr(queryMeta.Type))
	}

	const MaxQueriesPerType = 3
	for queryMeta, queryList := range queries {
		maxNumber := MaxQueriesPerType
		if queryMeta.Type == QueryTypeFilter || queryMeta.Type == QueryTypeFilterTags {
			maxNumber *= 2
		}
		queries[queryMeta] = selectQueries(queryList, maxNumber)
	}

	analyzedOldQueries, oldStats := analyzeQueries(oldNode, false, queries)
	analyzedNewQueries, newStats := analyzeQueries(newNode, true, queries)
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
		line := fmt.Sprintf("Query: %s hours=%d - Old: %s %d, New: %s %d", name, oldStat.hours, oldStat.elapsed, oldStat.numResults, newStat.elapsed, newStat.numResults)
		logger.Infof(line)
		_, _ = file.WriteString(line)
		_, _ = file.WriteString("\n")
	}
	_ = file.Close()
	// sleep 1 min
	logger.Infof("Sleeping for 1 minute to allow the logs to be written to our backend")
	time.Sleep(1 * time.Minute)

	writeLogs(false, analyzedOldQueries)
	writeLogs(true, analyzedNewQueries)

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
	query := "nf.env,prod,:eq,nf.app,clickhouse,:eq,:and,formattedMessage," +
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
