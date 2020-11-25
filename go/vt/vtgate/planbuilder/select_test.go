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

package planbuilder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestExpandStars(t *testing.T) {
	type testCase struct {
		name            string
		input, expected sqlparser.SelectExprs
		stillStars      bool
		tables          []*table
	}

	tableT := &table{
		alias: sqlparser.TableName{
			Name: sqlparser.NewTableIdent("t"),
		},
		columns: map[string]*column{
			"foo": {},
			"bar": {},
		},
		columnNames:     []sqlparser.ColIdent{sqlparser.NewColIdent("foo"), sqlparser.NewColIdent("bar")},
		isAuthoritative: true,
	}
	tableTNotAuthorative := &table{
		alias: sqlparser.TableName{
			Name: sqlparser.NewTableIdent("t"),
		},
		columns:         map[string]*column{},
		columnNames:     []sqlparser.ColIdent{},
		isAuthoritative: false,
	}
	tableS := &table{
		alias: sqlparser.TableName{
			Name: sqlparser.NewTableIdent("s"),
		},
		columns: map[string]*column{
			"baz": {},
			"lur": {},
		},
		columnNames:     []sqlparser.ColIdent{sqlparser.NewColIdent("baz"), sqlparser.NewColIdent("lur")},
		isAuthoritative: true,
	}

	colName := func(table, column string) sqlparser.SelectExpr {
		return &sqlparser.AliasedExpr{Expr: &sqlparser.ColName{
			Name:      sqlparser.NewColIdent(column),
			Qualifier: sqlparser.TableName{Qualifier: sqlparser.NewTableIdent(table)},
		}}
	}
	aliasedColName := func(table, column, alias string) sqlparser.SelectExpr {
		return &sqlparser.AliasedExpr{
			Expr: &sqlparser.ColName{
				Name:      sqlparser.NewColIdent(column),
				Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent(table)},
			},
			As: sqlparser.NewColIdent(alias),
		}
	}

	selectStar := sqlparser.SelectExprs{&sqlparser.StarExpr{}}
	tests := []testCase{
		{
			name:  "SELECT * FROM t",
			input: selectStar,
			expected: sqlparser.SelectExprs{
				colName("", "foo"),
				colName("", "bar"),
			},
			stillStars: false,
			tables:     []*table{tableT},
		}, {
			name:       "SELECT * FROM t, nonAuthoritative",
			input:      selectStar,
			expected:   selectStar,
			stillStars: true,
			tables:     []*table{tableTNotAuthorative},
		}, {
			name:       "SELECT 42 FROM t",
			input:      sqlparser.SelectExprs{&sqlparser.AliasedExpr{Expr: sqlparser.NewIntLiteral([]byte("42"))}},
			expected:   sqlparser.SelectExprs{&sqlparser.AliasedExpr{Expr: sqlparser.NewIntLiteral([]byte("42"))}},
			stillStars: false,
			tables:     []*table{tableT},
		}, {
			name:  "SELECT * FROM t, s",
			input: selectStar,
			expected: sqlparser.SelectExprs{
				aliasedColName("t", "foo", "foo"),
				aliasedColName("t", "bar", "bar"),
				aliasedColName("s", "baz", "baz"),
				aliasedColName("s", "lur", "lur"),
			},
			stillStars: false,
			tables:     []*table{tableT, tableS},
		},
	}

	for _, test := range tests {
		findTable := func(name sqlparser.TableName) (*table, error) {
			for _, t2 := range test.tables {
				if name.Name.String() == t2.alias.Name.String() {
					return t2, nil
				}
			}
			return nil, fmt.Errorf("oh noes")
		}
		t.Run(test.name, func(t *testing.T) {
			hasStars, result, err := expandStars(test.tables, test.input, findTable)
			require.NoError(t, err)
			assert.Equal(t, test.stillStars, hasStars, "still stars")
			utils.MustMatch(t, sqlparser.String(test.expected), sqlparser.String(result), "columns")
		})
	}
}
