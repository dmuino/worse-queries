package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"math/rand"
	"os"
	"regexp"
	chagent "stash.corp.netflix.com/cldmta/clickhouse-agent-libs"
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

var connections = make(map[string]*sql.DB)

func getConnection(node chagent.InstanceInfo) *sql.DB {
	var db *sql.DB
	address := fmt.Sprintf("[%s]:9000", node.IPv6Address)
	for i := 0; i < 10; i++ {
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

		logger.Errorf("Could not connect to clickhouse-server, will retry after 5s (%d/10): %v", i+1, err)
		time.Sleep(5 * time.Second)
	}
	return db
}

const OldSettings = "SETTINGS enable_positional_arguments = 1, log_queries = 1, skip_unavailable_shards = 1"
const NewSettings = OldSettings +
	", max_parallel_replicas = 16, allow_experimental_parallel_reading_from_replicas = 1" +
	", use_hedged_requests = 0, cluster_for_parallel_replicas = 'default'"

func withFixedTableName(query string, isNew bool) string {
	// replace the table name if
	var settings string
	q := query
	if isNew {
		settings = NewSettings
	} else {
		settings = OldSettings
		// replace the table name with one that has a _distributed suffix
		p := regexp.MustCompile(`FROM (insight_\w+)`)
		q = p.ReplaceAllString(q, "FROM ${1}_distributed")
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
	queryId string
	perHost []ResultMetadata
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
	res, err := conn.QueryContext(ctx, finalQuery)
	if err != nil {
		logger.Fatalf("Error sending query: %s", err)
	}
	i := 0
	for res.Next() {
		// read all results
		i++
	}
	_ = res.Close()
	logger.Debugf("Done sending query: %s to %s node %s. Received %d results back", finalQuery, oldNew, node.InstanceId, i)

	allMetadata := make([]ResultMetadata, 0)
	// get the metadata results from system.query_log
	meta := ResultMetadata{}
	time.Sleep(5 * time.Second)
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
	return Results{queryId, allMetadata}
}

func analyzeQueries(node chagent.InstanceInfo, isNew bool, queries map[QueryMeta][]string) map[QueryMeta][]Results {
	result := make(map[QueryMeta][]Results)
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
			result[queryMeta] = append(result[queryMeta], resultMeta)
		}
	}
	return result
}

func writeResults(oldNew string, results map[QueryMeta][]Results) error {
	// create results/old or results/new directory
	dir := fmt.Sprintf("results/%s", oldNew)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	for queryMeta, queryResults := range results {
		if len(queryResults) == 0 {
			logger.Infof("No results for table %s of type %s", queryMeta.Table, queryTypeStr(queryMeta.Type))
			continue
		}
		fileName := fmt.Sprintf("%s/%s_%s.txt", dir, queryMeta.Table, queryTypeStr(queryMeta.Type))
		file, err := os.Create(fileName)
		if err != nil {
			return err
		}
		for _, result := range queryResults {
			_, _ = file.WriteString(fmt.Sprintf("Query ID: %s -- %s\n", result.queryId, idsToQueries[result.queryId]))
			for _, r := range result.perHost {
				_, _ = file.WriteString(fmt.Sprintf("Hostname: %s\n", r.hostname))
				_, _ = file.WriteString(fmt.Sprintf("Read Rows: %d\n", r.readRows))
				_, _ = file.WriteString(fmt.Sprintf("Read Bytes: %d\n", r.readBytes))
				_, _ = file.WriteString(fmt.Sprintf("Query Duration: %s\n", r.queryDuration))
				_, _ = file.WriteString(fmt.Sprintf("Profile Events: %v\n", r.profileEvents))
			}
		}
		_ = file.Close()
	}
	return nil
}

// to avoid multiple runs hitting cached results
func selectRandomQueries(queryList []string, max int) []string {
	if len(queryList) <= max {
		return queryList
	}

	// shuffle the slice
	rand.Shuffle(len(queryList), func(i, j int) {
		queryList[i], queryList[j] = queryList[j], queryList[i]
	})

	return queryList[:max]
}

func main() {
	env := chagent.NewNetflixEnv()
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

	const MaxQueriesPerType = 1
	for queryMeta, queryList := range queries {
		queries[queryMeta] = selectRandomQueries(queryList, MaxQueriesPerType)
	}

	analyzedOldQueries := analyzeQueries(oldNode, false, queries)
	analyzedNewQueries := analyzeQueries(newNode, true, queries)

	for queryMeta, queryList := range queries {
		fmt.Printf("Table: %s, Type: %s\n", queryMeta.Table, queryTypeStr(queryMeta.Type))
		for _, query := range queryList {
			fmt.Printf("%s\n", query)
			fmt.Printf("Query ID: %s\n", queriesToIds[query])

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
}
