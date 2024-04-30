package main

import "testing"

func Test_getHoursFromQuery(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  int
	}{
		{
			name:  "0 hour",
			query: "SELECT level,formattedMessage,dateTime,logger,nf_node FROM insight_logs_prod WHERE (nf_app IS NOT null and LENGTH(nf_app) != 0) AND nf_ns = 'ocinstaller' AND dateTime >= toDateTime64('1714509901331000000', 9) AND dateTime <= toDateTime64('1714510224067000000', 9) ORDER BY dateTime DESC LIMIT 1000000",
			want:  0,
		},
		{
			name:  "3 hours",
			query: "SELECT foo FROM insight_logs_prod WHERE (position(formattedMessage, 'b54f75f9-a6ee-429c-ac1a-510be72b039e') != 0) AND nf_ns = 'socialitebackend' AND dateTime >= toDateTime64('1714496016504266398', 9) AND dateTime <= toDateTime64('1714506816504266398', 9) ORDER BY dateTime",
			want:  3,
		},
		{
			name:  "-1 hour",
			query: "SELECT foo FROM insight_logs_prod WHERE (position(formattedMessage, 'b54f75f9-a6ee-429c-ac1a-510be72b039e') != 0) AND nf_ns = 'socialitebackend' AND dateTime >= toDateTime64('1714496016504266398', 9)",
			want:  -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getHoursFromQuery(tt.query); got != tt.want {
				t.Errorf("getHoursFromQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getQueryType(t *testing.T) {
	tests := []struct {
		name  string
		table string
		query string
		want  QueryType
	}{
		{
			name:  "Aggregate",
			table: "insight_logs_aggregates_test",
			query: "SELECT count() FROM insight_logs_aggregates_test WHERE bar = 1",
			want:  QueryTypeAggr,
		},
		{
			name:  "GroupBy",
			table: "insight_logs_test",
			query: "SELECT foo, count() FROM insight_logs_test WHERE bar = 1 GROUP BY foo",
			want:  QueryTypeGroupBy,
		},
		{
			name:  "Timeline",
			table: "insight_logs_test",
			query: "SELECT count() FROM insight_logs_test WHERE bar = 1 GROUP BY 1 ORDER BY 1 " +
				"DESC WITH FILL FROM 1706889600 TO 1706799600 STEP INTERVAL -1 HOUR LIMIT 24 SETTINGS foo=1",
			want: QueryTypeTimeline,
		},
		{
			name:  "Filter with tags as predicate",
			table: "insight_logs_test",
			query: "SELECT nf_app, message FROM insight_logs_test WHERE nf_ns='go2' AND tags['foo'] = 1",
			want:  QueryTypeFilterTags,
		},
		{
			name:  "Filter with only top level cols as predicate",
			table: "insight_logs_test",
			query: "SELECT nf_app, message FROM insight_logs_test WHERE nf_ns='go2' AND position(message, 'tags') > 0",
			want:  QueryTypeFilter,
		},
		{
			name:  "Filter from issues table",
			table: "insight_logs_issues_test",
			query: "SELECT nf_app, message FROM insight_logs_issues_test WHERE nf_ns='go2' AND level='error'",
			want:  QueryTypeFilterIssues,
		},
		{
			name:  "Unknown",
			table: "system.clusters",
			query: "SELECT * FROM system.clusters",
			want:  QueryTypeUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getQueryType(tt.table, tt.query); got != tt.want {
				t.Errorf("getQueryType() = %v, want %v", got, tt.want)
			}
		})
	}
}
