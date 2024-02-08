package main

import "testing"

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
