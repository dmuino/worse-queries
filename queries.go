package main

import (
	"bufio"
	"os"
	"regexp"
	"strings"
)

// QueryType is an enum for the type of query
type QueryType int

const (
	QueryTypeAggr         QueryType = iota
	QueryTypeTimeline     QueryType = iota
	QueryTypeGroupBy      QueryType = iota
	QueryTypeFilter       QueryType = iota
	QueryTypeFilterTags   QueryType = iota
	QueryTypeFilterIssues QueryType = iota
	QueryTypeUnknown      QueryType = iota
)

func queryTypeStr(queryType QueryType) string {
	// return a string representation of the query type
	switch queryType {
	case QueryTypeAggr:
		return "Aggr"
	case QueryTypeTimeline:
		return "Timeline"
	case QueryTypeGroupBy:
		return "GroupBy"
	case QueryTypeFilter:
		return "Filter"
	case QueryTypeFilterTags:
		return "FilterTags"
	case QueryTypeFilterIssues:
		return "FilterIssues"
	case QueryTypeUnknown:
		return "Unknown"
	}
	return ""
}

// get the query type from the query
func getQueryType(table string, query string) QueryType {
	if strings.HasSuffix(table, "_distributed") {
		table = strings.TrimSuffix(table, "_distributed")
	}

	if strings.Contains(query, " STEP ") {
		return QueryTypeTimeline
	}

	if strings.Contains(table, "aggregates") {
		return QueryTypeAggr
	}

	if strings.Contains(query, "GROUP BY") {
		return QueryTypeGroupBy
	}

	whereIndex := strings.Index(query, " WHERE ")
	if whereIndex == -1 {
		return QueryTypeUnknown
	}

	if strings.Contains(query[whereIndex:], "tags['") {
		return QueryTypeFilterTags
	}

	if table == "insight_logs" {
		return QueryTypeFilter
	}

	if table == "insight_logs_issues" {
		return QueryTypeFilterIssues
	}

	return QueryTypeUnknown
}

type QueryMeta struct {
	Table string
	Type  QueryType
}

// ReadQueries returns a map of query tables to queries
// the queries exclude the SETTINGS and the table names are normalized
// excluding any _distributed suffix
func ReadQueries(filename string) (map[QueryMeta][]string, error) {
	// read filename line by line
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	// return all lines form file
	scanner := bufio.NewScanner(file)
	queries := make(map[QueryMeta][]string)
	regex := regexp.MustCompile(`FROM (\w+)`)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		// remove any settings
		if strings.Contains(line, " SETTINGS ") {
			line = line[:strings.Index(line, "SETTINGS")]
		}

		// get the table name (the word following FROM)
		if !strings.Contains(line, "FROM") {
			logger.Errorf("Query does not contain FROM: %s", line)
			continue
		}

		// get the table name, the word following FROM
		// match the regex FROM (\w+) and extract that group
		matches := regex.FindStringSubmatch(line)
		if len(matches) < 2 {
			logger.Errorf("Could not extract table name from query: %s", line)
			continue
		}
		table := matches[1]
		// remove any _distributed suffix
		table = strings.TrimSuffix(table, "_distributed")
		key := QueryMeta{Table: table, Type: getQueryType(table, line)}
		queries[key] = append(queries[key], line)
	}
	return queries, nil
}
