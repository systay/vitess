/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/callinfo/fakecallinfo"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func testFormat(stats *LogStats, params url.Values) string {
	var b bytes.Buffer
	stats.Logf(&b, params)
	return b.String()
}

func TestLogStatsFormat(t *testing.T) {
	defer func() {
		*streamlog.RedactDebugUIQueries = false
		*streamlog.QueryLogFormat = "text"
	}()
	logStats := NewLogStats(context.Background(), "test", "sql1", map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)})
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	logStats.Keyspace = "ks"
	logStats.Table = "table"
	logStats.TabletType = "PRIMARY"
	params := map[string][]string{"full": {}}

	*streamlog.RedactDebugUIQueries = false
	*streamlog.QueryLogFormat = "text"
	got := testFormat(logStats, params)
	want := `test			''	''	2017-01-01 01:02:03.000000	2017-01-01 01:02:04.000001	1.000001	0.000000	0.000000	0.000000		"sql1"	map[intVal:type:INT64 value:"1"]	0	0	""	"ks"	"table"	"PRIMARY"	
`
	assert.Equal(t, want, got)

	*streamlog.RedactDebugUIQueries = true
	*streamlog.QueryLogFormat = "text"
	got = testFormat(logStats, params)
	want = `test			''	''	2017-01-01 01:02:03.000000	2017-01-01 01:02:04.000001	1.000001	0.000000	0.000000	0.000000		"sql1"	"[REDACTED]"	0	0	""	"ks"	"table"	"PRIMARY"	
`
	assert.Equal(t, want, got)

	*streamlog.RedactDebugUIQueries = false
	*streamlog.QueryLogFormat = "json"
	got = testFormat(logStats, params)
	var parsed map[string]any
	err := json.Unmarshal([]byte(got), &parsed)
	assert.NoError(t, err, "logstats format: error unmarshaling json: %v -- got:\n%v", err, got)
	formatted, err := json.MarshalIndent(parsed, "", "    ")
	assert.NoError(t, err, "logstats format: error marshaling json: %v -- got:\n%v", err, got)
	want = `{
    "BindVars": {
        "intVal": {
            "type": "INT64",
            "value": 1
        }
    },
    "CommitTime": 0,
    "Effective Caller": "",
    "End": "2017-01-01 01:02:04.000001",
    "Error": "",
    "ExecuteTime": 0,
    "ImmediateCaller": "",
    "Keyspace": "ks",
    "Method": "test",
    "PlanTime": 0,
    "RemoteAddr": "",
    "RowsAffected": 0,
    "SQL": "sql1",
    "ShardQueries": 0,
    "Start": "2017-01-01 01:02:03.000000",
    "StmtType": "",
    "Table": "table",
    "TabletType": "PRIMARY",
    "TotalTime": 1.000001,
    "Username": ""
}`
	assert.Equal(t, want, string(formatted))

	*streamlog.RedactDebugUIQueries = true
	*streamlog.QueryLogFormat = "json"
	got = testFormat(logStats, params)
	err = json.Unmarshal([]byte(got), &parsed)
	assert.NoError(t, err, "logstats format: error unmarshaling json: %v -- got:\n%v", err, got)
	formatted, err = json.MarshalIndent(parsed, "", "    ")
	assert.NoError(t, err, "logstats format: error marshaling json: %v -- got:\n%v", err, got)
	want = `{
    "BindVars": "[REDACTED]",
    "CommitTime": 0,
    "Effective Caller": "",
    "End": "2017-01-01 01:02:04.000001",
    "Error": "",
    "ExecuteTime": 0,
    "ImmediateCaller": "",
    "Keyspace": "ks",
    "Method": "test",
    "PlanTime": 0,
    "RemoteAddr": "",
    "RowsAffected": 0,
    "SQL": "sql1",
    "ShardQueries": 0,
    "Start": "2017-01-01 01:02:03.000000",
    "StmtType": "",
    "Table": "table",
    "TabletType": "PRIMARY",
    "TotalTime": 1.000001,
    "Username": ""
}`
	assert.Equal(t, want, string(formatted))

	*streamlog.RedactDebugUIQueries = false

	// Make sure formatting works for string bind vars. We can't do this as part of a single
	// map because the output ordering is undefined.
	logStats.BindVariables = map[string]*querypb.BindVariable{"strVal": sqltypes.StringBindVariable("abc")}

	*streamlog.QueryLogFormat = "text"
	got = testFormat(logStats, params)
	want = `test			''	''	2017-01-01 01:02:03.000000	2017-01-01 01:02:04.000001	1.000001	0.000000	0.000000	0.000000		"sql1"	map[strVal:type:VARCHAR value:"abc"]	0	0	""	"ks"	"table"	"PRIMARY"	
`
	assert.Equal(t, want, got)

	*streamlog.QueryLogFormat = "json"
	got = testFormat(logStats, params)
	err = json.Unmarshal([]byte(got), &parsed)
	assert.NoError(t, err, "logstats format: error unmarshaling json: %v -- got:\n%v", err, got)
	formatted, err = json.MarshalIndent(parsed, "", "    ")
	assert.NoError(t, err, "logstats format: error marshaling json: %v -- got:\n%v", err, got)
	want = `{
    "BindVars": {
        "strVal": {
            "type": "VARCHAR",
            "value": "abc"
        }
    },
    "CommitTime": 0,
    "Effective Caller": "",
    "End": "2017-01-01 01:02:04.000001",
    "Error": "",
    "ExecuteTime": 0,
    "ImmediateCaller": "",
    "Keyspace": "ks",
    "Method": "test",
    "PlanTime": 0,
    "RemoteAddr": "",
    "RowsAffected": 0,
    "SQL": "sql1",
    "ShardQueries": 0,
    "Start": "2017-01-01 01:02:03.000000",
    "StmtType": "",
    "Table": "table",
    "TabletType": "PRIMARY",
    "TotalTime": 1.000001,
    "Username": ""
}`
	assert.Equal(t, want, string(formatted))

	*streamlog.QueryLogFormat = "json"
	*streamlog.RedactDebugUIQueries = true
	logStats.Keyspace = "ks_ks2_ks"
	logStats.Table = "table_tabl2_table 3"
	logStats.TablesUsed = []string{"ks.table", "ks2.table2", "ks.table 3"}
	got = testFormat(logStats, params)
	err = json.Unmarshal([]byte(got), &parsed)
	assert.NoError(t, err, "logstats format: error unmarshaling json: %v -- got:\n%v", err, got)
	formatted, err = json.MarshalIndent(parsed, "", "    ")
	assert.NoError(t, err, "logstats format: error marshaling json: %v -- got:\n%v", err, got)
	want = `{
    "BindVars": "[REDACTED]",
    "CommitTime": 0,
    "Effective Caller": "",
    "End": "2017-01-01 01:02:04.000001",
    "Error": "",
    "ExecuteTime": 0,
    "ImmediateCaller": "",
    "Keyspace": "ks_ks2_ks",
    "Method": "test",
    "PlanTime": 0,
    "RemoteAddr": "",
    "RowsAffected": 0,
    "SQL": "sql1",
    "ShardQueries": 0,
    "Start": "2017-01-01 01:02:03.000000",
    "StmtType": "",
    "Table": "table_tabl2_table 3",
    "TablesUsed": [
        "ks.table",
        "ks2.table2",
        "ks.table 3"
    ],
    "TabletType": "PRIMARY",
    "TotalTime": 1.000001,
    "Username": ""
}`
	assert.Equal(t, want, string(formatted))

	*streamlog.QueryLogFormat = "text"
	got = testFormat(logStats, params)
	want = `test			''	''	2017-01-01 01:02:03.000000	2017-01-01 01:02:04.000001	1.000001	0.000000	0.000000	0.000000		"sql1"	"[REDACTED]"	0	0	""	"ks_ks2_ks"	"table_tabl2_table 3"	"PRIMARY"	["ks.table","ks2.table2","ks.table 3"]	
`
	assert.Equal(t, want, got)
}

func TestLogStatsFilter(t *testing.T) {
	defer func() { *streamlog.QueryLogFilterTag = "" }()

	logStats := NewLogStats(context.Background(), "test", "sql1 /* LOG_THIS_QUERY */", map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)})
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	params := map[string][]string{"full": {}}

	got := testFormat(logStats, params)
	want := `test			''	''	2017-01-01 01:02:03.000000	2017-01-01 01:02:04.000001	1.000001	0.000000	0.000000	0.000000		"sql1 /* LOG_THIS_QUERY */"	map[intVal:type:INT64 value:"1"]	0	0	""	""	""	""	
`
	assert.Equal(t, want, got)

	*streamlog.QueryLogFilterTag = "LOG_THIS_QUERY"
	got = testFormat(logStats, params)
	want = `test			''	''	2017-01-01 01:02:03.000000	2017-01-01 01:02:04.000001	1.000001	0.000000	0.000000	0.000000		"sql1 /* LOG_THIS_QUERY */"	map[intVal:type:INT64 value:"1"]	0	0	""	""	""	""	
`
	assert.Equal(t, want, got)

	*streamlog.QueryLogFilterTag = "NOT_THIS_QUERY"
	got = testFormat(logStats, params)
	want = ""
	assert.Equal(t, want, got)
}

func TestLogStatsRowThreshold(t *testing.T) {
	defer func() { *streamlog.QueryLogRowThreshold = 0 }()

	logStats := NewLogStats(context.Background(), "test", "sql1 /* LOG_THIS_QUERY */", map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)})
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	params := map[string][]string{"full": {}}

	got := testFormat(logStats, params)
	want := `test			''	''	2017-01-01 01:02:03.000000	2017-01-01 01:02:04.000001	1.000001	0.000000	0.000000	0.000000		"sql1 /* LOG_THIS_QUERY */"	map[intVal:type:INT64 value:"1"]	0	0	""	""	""	""	
`
	assert.Equal(t, want, got)

	*streamlog.QueryLogRowThreshold = 0
	got = testFormat(logStats, params)
	want = `test			''	''	2017-01-01 01:02:03.000000	2017-01-01 01:02:04.000001	1.000001	0.000000	0.000000	0.000000		"sql1 /* LOG_THIS_QUERY */"	map[intVal:type:INT64 value:"1"]	0	0	""	""	""	""	
`
	assert.Equal(t, want, got)

	*streamlog.QueryLogRowThreshold = 1
	got = testFormat(logStats, params)
	assert.Empty(t, got)
}

func TestLogStatsContextHTML(t *testing.T) {
	html := "HtmlContext"
	callInfo := &fakecallinfo.FakeCallInfo{
		Html: html,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	logStats := NewLogStats(ctx, "test", "sql1", map[string]*querypb.BindVariable{})
	if string(logStats.ContextHTML()) != html {
		t.Fatalf("expect to get html: %s, but got: %s", html, string(logStats.ContextHTML()))
	}
}

func TestLogStatsErrorStr(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test", "sql1", map[string]*querypb.BindVariable{})
	if logStats.ErrorStr() != "" {
		t.Fatalf("should not get error in stats, but got: %s", logStats.ErrorStr())
	}
	errStr := "unknown error"
	logStats.Error = errors.New(errStr)
	if !strings.Contains(logStats.ErrorStr(), errStr) {
		t.Fatalf("expect string '%s' in error message, but got: %s", errStr, logStats.ErrorStr())
	}
}

func TestLogStatsRemoteAddrUsername(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test", "sql1", map[string]*querypb.BindVariable{})
	addr, user := logStats.RemoteAddrUsername()
	if addr != "" {
		t.Fatalf("remote addr should be empty")
	}
	if user != "" {
		t.Fatalf("username should be empty")
	}

	remoteAddr := "1.2.3.4"
	username := "vt"
	callInfo := &fakecallinfo.FakeCallInfo{
		Remote: remoteAddr,
		User:   username,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	logStats = NewLogStats(ctx, "test", "sql1", map[string]*querypb.BindVariable{})
	addr, user = logStats.RemoteAddrUsername()
	if addr != remoteAddr {
		t.Fatalf("expected to get remote addr: %s, but got: %s", remoteAddr, addr)
	}
	if user != username {
		t.Fatalf("expected to get username: %s, but got: %s", username, user)
	}
}
