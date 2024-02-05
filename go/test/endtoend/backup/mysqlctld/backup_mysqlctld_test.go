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

=== RUN   TestOne/onecase.json/Add_your_test_case_here_for_debugging_and_run_go_test_-run=One.

	plan_test.go:652: onecase.json
	    Diff:
	    {
	        "Instructions": {
	            "Aggregates": "sum_count_star(0) AS count(*)",
	            "Inputs": [
	                {
	                    "Expressions": [
	                        "count(*) * count(*) as count(*)"
	                    ],
	                    "Inputs": [
	                        {
	                            "Inputs": [
	                                {
	                                    "FieldQuery": "select count(*), ue.foo from user_extra as ue where 1 != 1 group by ue.foo",
	                                    "Keyspace": {
	                                        "Name": "user",
	                                        "Sharded": true
	                                    },
	                                    "OperatorType": "Route",
	                                    "Query": "select count(*), ue.foo from user_extra as ue group by ue.foo",
	                                    "Table": "user_extra",
	                                    "Variant": "Scatter"
	                                },
	                                {
	                                    "FieldQuery": "select count(*) from `user` as u where 1 != 1 group by .0",
	                                    "Keyspace": {
	                                        "Name": "user",
	                                        "Sharded": true
	                                    },
	                                    "OperatorType": "Route",
	                                    "Query": "select count(*) from `user` as u where u.id = :ue_foo group by .0",
	                                    "Table": "`user`",
	                                    "Values": [
	                                        ":ue_foo"
	                                    ],
	                                    "Variant": "EqualUnique",
	                                    "Vindex": "user_vindex"
	                                }
	                            ],
	                            "JoinColumnIndexes": "L:0,R:0",
	                            "JoinVars": {
	                                "ue_foo": 1
	                            },
	                            "OperatorType": "Join",
	                            "TableName": "user_extra_`user`",
	                            "Variant": "Join"
	                        }
	                    ],
	                    "OperatorType": "Projection"
	                }
	            ],
	            "OperatorType": "Aggregate",
	            "Variant": "Scalar"
	        },
	        "Original": "select count(*) from user u join user_extra ue on u.id = ue.foo",
	        "QueryType": "SELECT",
	        "TablesUsed": [
	            "user.user",
	            "user.user_extra"
	        ]
	    }
	    [{

	        }]
	    [{
	      "QueryType": "SELECT",
	      "Original": "select count(*) from user u join user_extra ue on u.id = ue.foo",
	      "Instructions": {
	        "OperatorType": "Aggregate",
	        "Variant": "Scalar",
	        "Aggregates": "sum_count_star(0) AS count(*)",
	        "Inputs": [
	          {
	            "OperatorType": "Projection",
	            "Expressions": [
	              "count(*) * count(*) as count(*)"
	            ],
	            "Inputs": [
	              {
	                "OperatorType": "Join",
	                "Variant": "Join",
	                "JoinColumnIndexes": "L:0,R:0",
	                "JoinVars": {
	                  "ue_foo": 1
	                },
	                "TableName": "user_extra_`user`",
	                "Inputs": [
	                  {
	                    "OperatorType": "Route",
	                    "Variant": "Scatter",
	                    "Keyspace": {
	                      "Name": "user",
	                      "Sharded": true
	                    },
	                    "FieldQuery": "select count(*), ue.foo from user_extra as ue where 1 != 1 group by ue.foo",
	                    "Query": "select count(*), ue.foo from user_extra as ue group by ue.foo",
	                    "Table": "user_extra"
	                  },
	                  {
	                    "OperatorType": "Route",
	                    "Variant": "EqualUnique",
	                    "Keyspace": {
	                      "Name": "user",
	                      "Sharded": true
	                    },
	                    "FieldQuery": "select count(*) from `user` as u where 1 != 1 group by .0",
	                    "Query": "select count(*) from `user` as u where u.id = :ue_foo group by .0",
	                    "Table": "`user`",
	                    "Values": [
	                      ":ue_foo"
	                    ],
	                    "Vindex": "user_vindex"
	                  }
	                ]
	              }
	            ]
	          }
	        ]
	      },
	      "TablesUsed": [
	        "user.user",
	        "user.user_extra"
	      ]
	    }
	    ]

--- FAIL: TestOne (0.00s)

	--- FAIL: TestOne/onecase.json (0.00s)
	    --- FAIL: TestOne/onecase.json/Add_your_test_case_here_for_debugging_and_run_go_test_-run=One. (0.00s)

# FAIL

Process finished with the exit code 1
*/
package mysqlctld

import (
	"testing"

	backup "vitess.io/vitess/go/test/endtoend/backup/vtctlbackup"
	"vitess.io/vitess/go/vt/mysqlctl"
)

// TestBackupMysqlctld - tests the backup using mysqlctld.
func TestBackupMysqlctld(t *testing.T) {
	backup.TestBackup(t, backup.Mysqlctld, "xbstream", 0, nil, nil)
}

func TestBackupMysqlctldWithlz4Compression(t *testing.T) {
	defer setDefaultCompressionFlag()
	cDetails := &backup.CompressionDetails{
		CompressorEngineName: "lz4",
	}

	backup.TestBackup(t, backup.Mysqlctld, "xbstream", 0, cDetails, []string{"TestReplicaBackup", "TestPrimaryBackup"})
}

func setDefaultCompressionFlag() {
	mysqlctl.CompressionEngineName = "pgzip"
	mysqlctl.ExternalCompressorCmd = ""
	mysqlctl.ExternalCompressorExt = ""
	mysqlctl.ExternalDecompressorCmd = ""
}
