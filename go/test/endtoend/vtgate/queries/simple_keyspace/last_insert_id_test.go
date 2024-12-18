/*
Copyright 2024 The Vitess Authors.

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

package misc

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"vitess.io/vitess/go/test/endtoend/utils"
)

// This file contains tests for the last_insert_id() function with arguments in sharded keyspaces.

var lastInsertIDQueries = []string{
	"select last_insert_id(%d)",
	"select last_insert_id(%d), id, name from user limit 1",
	"select last_insert_id(%d), id, name from t1 where 1 = 2",
	"select 12 from t1 where last_insert_id(%d)",
	"update t1 set name = last_insert_id(%d) where id = 1",
	"update t1 set name = last_insert_id(%d) where id = 2",
	"update t1 set name = 88 where id = last_insert_id(%d)",
	"delete from t1 where id = last_insert_id(%d)",
	"select name, last_insert_id(count(*)) from t1 where %d group by name",
}

func TestSetAndGetLastInsertIDSolo(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1 (id, name) values (1, 10)")
	checkQuery("select name, last_insert_id(count(*)) from t1 where %d group by name", "olap", false, mcmp, 1)
}

// TestSetAndGetLastInsertID tests the last_insert_id() function in various queries.
func TestSetAndGetLastInsertID(t *testing.T) {
	counter := 1
	for _, workload := range []string{"olap", "oltp"} {
		for _, tx := range []bool{true, false} {
			testAllQueries(t, workload, tx, &counter)
		}
	}
}

// testAllQueries runs all queries for a given workload and transaction mode.
func testAllQueries(t *testing.T, workload string, tx bool, counter *int) {
	mcmp, closer := start(t)
	defer closer()
	_, err := mcmp.VtConn.ExecuteFetch(fmt.Sprintf("set workload = %s", workload), 1000, false)
	require.NoError(t, err)
	if tx {
		_, err := mcmp.VtConn.ExecuteFetch("begin", 1000, false)
		require.NoError(t, err)
	}

	// Insert a few rows for UPDATE tests
	mcmp.Exec("insert into t1 (id, name) values (1, 10)")

	for _, query := range lastInsertIDQueries {
		checkQuery(query, workload, tx, mcmp, *counter)
		*counter++
	}
}

// checkQuery runs a query and checks the result of last_insert_id().
func checkQuery(i string, workload string, tx bool, mcmp utils.MySQLCompare, counter int) {
	for _, val := range []int{0, counter} {
		query := fmt.Sprintf(i, val)
		name := fmt.Sprintf("%s - %s", workload, query)
		if tx {
			name = "tx - " + name
		}
		mcmp.Run(name, func(mcmp *utils.MySQLCompare) {
			mcmp.Exec(query)
			mcmp.Exec("select last_insert_id()")
			if mcmp.AsT().Failed() {
				plan := mcmp.VExplain(query)
				mcmp.AsT().Logf("Plan: %s", plan)
			}
		})
	}
}
