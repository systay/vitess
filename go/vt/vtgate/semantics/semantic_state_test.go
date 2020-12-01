/*
Copyright 2020 The Vitess Authors.

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

package semantics

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

func extract(in *sqlparser.Select, idx int) sqlparser.Expr {
	return in.SelectExprs[idx].(*sqlparser.AliasedExpr).Expr
}

func TestScope(t *testing.T) {
	query := "select col, (select 2) from (select col from t) as x where 3 in (select 4 from t where t.col = x.col)"
	stmt, semTable := parseAndAnalyze(t, query)
	sel, _ := stmt.(*sqlparser.Select)

	s1 := semTable.scope(extract(sel, 0))
	s2 := semTable.scope(extract(sel.From[0].(*sqlparser.AliasedTableExpr).Expr.(*sqlparser.DerivedTable).Select.(*sqlparser.Select), 0))
	require.False(t, &s1 == &s2, "different scope expected")

	s3 := semTable.scope(extract(extract(sel, 1).(*sqlparser.Subquery).Select.(*sqlparser.Select), 0))
	require.False(t, &s1 == &s3, "different scope expected")
	require.False(t, &s2 == &s3, "different scope expected")

	s4 := semTable.scope(sel.Where.Expr.(*sqlparser.ComparisonExpr).Left)
	require.Truef(t, s1.i == s4.i, "want: %v, got %v", s1, s4)
}

func TestBindingSingleTable(t *testing.T) {
	queries := []string{
		"select col from tabl",
		"select tabl.col from tabl",
		"select d.tabl.col from tabl",
		"select col from d.tabl",
		"select tabl.col from d.tabl",
		"select d.tabl.col from d.tabl",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query)
			sel, _ := stmt.(*sqlparser.Select)

			d := semTable.dependencies(extract(sel, 0))
			require.NotEmpty(t, d)
			require.NotNil(t, d[0])
			require.Contains(t, sqlparser.String(d[0]), "tabl")
		})
	}
}

func TestBindingMultiTable(t *testing.T) {
	type testCase struct {
		query string
		deps  []string
	}
	d := func(i ...string) []string { return i }
	queries := []testCase{{
		query: "select t.col from t, s",
		deps:  d("t"),
	}, {
		query: "select max(t.col+s.col) from t, s",
		deps:  d("s", "t"),
	}, {
		query: "select case t.col when s.col then r.col else w.col end from t, s, r, w, u",
		deps:  d("r", "s", "t", "w"),
	}, {
		// make sure that we don't let sub-query dependencies leak out by mistake
		query: "select t.col + (select 42 from s) from t",
		deps:  d("t"),
	}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query.query)
			sel, _ := stmt.(*sqlparser.Select)

			d := semTable.dependencies(extract(sel, 0))
			var deps []string
			for _, t2 := range d {
				deps = append(deps, sqlparser.String(t2))
			}
			sort.Strings(deps)
			assert.Equal(t, deps, query.deps)
		})
	}
}

func TestBindingSingleDepPerTable(t *testing.T) {
	query := "select t.col + t.col2 from t"
	stmt, semTable := parseAndAnalyze(t, query)
	sel, _ := stmt.(*sqlparser.Select)

	d := semTable.dependencies(extract(sel, 0))
	assert.Equal(t, 1, len(d), "size wrong")
	assert.Equal(t, "t", sqlparser.String(d[0]))
}

func TestNotUniqueTableName(t *testing.T) {
	queries := []string{
		"select * from t, t",
		"select * from t, (select 1 from x) as t",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, _ := sqlparser.Parse(query)
			_, err := Analyse(parse, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Not unique table/alias")
		})
	}

}

func parseAndAnalyze(t *testing.T, query string) (sqlparser.Statement, *SemTable) {
	parse, err := sqlparser.Parse(query)
	require.NoError(t, err)
	semTable, err := Analyse(parse, nil)
	require.NoError(t, err)
	return parse, semTable
}
