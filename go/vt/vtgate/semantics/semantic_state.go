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
	"fmt"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	table = *sqlparser.AliasedTableExpr
	// SemTable contains semantic analysis information about the query.
	SemTable struct {
		exprDependencies map[*sqlparser.ColName]table
	}
	schemaInformation interface {
		FindTable(tablename sqlparser.TableName) (*vindexes.Table, error)
	}
	scope struct {
		parent *scope
		tables map[string]*sqlparser.AliasedTableExpr
	}
)

// Dependencies return the table dependencies of the expression.
func (t *SemTable) Dependencies(expr sqlparser.Expr) []table {
	type Void struct{}
	var void Void
	depTable := map[table]Void{}
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		colName, ok := node.(*sqlparser.ColName)
		if ok {
			depTable[t.exprDependencies[colName]] = void
		}
		return true, nil
	}, expr)

	var uniqTable []table
	for tab := range depTable {
		uniqTable = append(uniqTable, tab)
	}
	return uniqTable
}

func newScope(parent *scope) *scope {
	return &scope{tables: map[string]*sqlparser.AliasedTableExpr{}, parent: parent}
}

func (s *scope) addTable(name string, table *sqlparser.AliasedTableExpr) error {
	_, found := s.tables[name]
	if found {
		return mysql.NewSQLError(mysql.ERNonUniqTable, mysql.SSSyntaxErrorOrAccessViolation, "Not unique table/alias: '%s'", name)
	}
	s.tables[name] = table
	return nil
}

// Analyse analyzes the parsed query.
func Analyse(statement sqlparser.Statement, si schemaInformation) (*SemTable, error) {
	analyzer := newAnalyzer(si)
	// Initial scope
	err := analyzer.analyze(statement)
	if err != nil {
		return nil, err
	}
	return &SemTable{exprDependencies: analyzer.exprDeps}, nil
}

func log(node sqlparser.SQLNode, format string, args ...interface{}) {
	if debug {
		fmt.Printf(format, args...)
		if node == nil {
			fmt.Println()
		} else {
			fmt.Println(" - " + sqlparser.String(node))
		}
	}
}
