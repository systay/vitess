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

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	SemTable struct {
		exprScope  map[sqlparser.Expr]*scope
		outerScope *scope
	}
	scope struct {
	}
)

func (t *SemTable) scope(expr sqlparser.Expr) *scope {
	return t.exprScope[expr]
}

type analyzer struct {
	exprScope map[sqlparser.Expr]*scope
	scopes    []*scope
}

func NewAnalyzer() *analyzer {
	return &analyzer{
		exprScope: map[sqlparser.Expr]*scope{},
	}
}

func Analyse(statement sqlparser.Statement) (*SemTable, error) {
	analyzer := NewAnalyzer()
	// Initial scope
	analyzer.push(&scope{})
	err := analyzer.analyze(statement)
	if err != nil {
		return nil, err
	}
	return &SemTable{outerScope: analyzer.peek(), exprScope: analyzer.exprScope}, nil
}

func log(node sqlparser.SQLNode, format string, args ...interface{}) {
	fmt.Printf(format, args...)
	if node == nil {
		fmt.Println()
	} else {
		fmt.Println(" - " + sqlparser.String(node))
	}
}
func (a *analyzer) analyze(statement sqlparser.Statement) error {
	log(statement, "analyse %T", statement)
	switch stmt := statement.(type) {
	case *sqlparser.Select:
		sqlparser.Rewrite(stmt.SelectExprs, a.analyzeExprs, nil)
		sqlparser.Rewrite(stmt.Where, a.analyzeExprs, nil)
		sqlparser.Rewrite(stmt.OrderBy, a.analyzeExprs, nil)
		sqlparser.Rewrite(stmt.GroupBy, a.analyzeExprs, nil)
		sqlparser.Rewrite(stmt.Having, a.analyzeExprs, nil)
		sqlparser.Rewrite(stmt.Limit, a.analyzeExprs, nil)
		for _, tableExpr := range stmt.From {
			a.analyzeTableExpr(tableExpr)
		}
	}
	return nil
}

func (a *analyzer) analyzeExprs(cursor *sqlparser.Cursor) bool {
	n := cursor.Node()
	log(n, "analyzeExprs %T", n)
	switch expr := n.(type) {
	case *sqlparser.Subquery:
		a.exprScope[expr] = a.peek()
		a.push(&scope{})
		a.analyze(expr.Select)
		_ = a.pop()
		return false
	case sqlparser.Expr:
		a.exprScope[expr] = a.peek()
	}
	return true
}

func (a *analyzer) analyzeTableExprs(tablExprs sqlparser.TableExprs) {
	for _, tableExpr := range tablExprs {
		a.analyzeTableExpr(tableExpr)
	}
}

func (a *analyzer) analyzeTableExpr(tableExpr sqlparser.TableExpr) bool {
	log(tableExpr, "analyzeTableExpr %T", tableExpr)
	switch table := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		a.analyzeSimpleTableExpr(table.Expr)
	case *sqlparser.JoinTableExpr:
		a.analyzeTableExpr(table.LeftExpr)
		a.analyzeTableExpr(table.RightExpr)
	case *sqlparser.ParenTableExpr:
		a.analyzeTableExprs(table.Exprs)
	}
	return true
}

func (a *analyzer) analyzeSimpleTableExpr(expr sqlparser.SimpleTableExpr) {
	log(expr, "analyzeSimpleTableExpr %T", expr)
	dt, derived := expr.(*sqlparser.DerivedTable)
	if derived {
		a.push(&scope{})
		a.analyze(dt.Select)
		_ = a.pop()
	}
}

func (a *analyzer) push(s *scope) {
	log(nil, "pushScope")
	a.scopes = append(a.scopes, s)
}

func (a *analyzer) pop() *scope {
	log(nil, "popScope")
	len := len(a.scopes) - 1
	scope := a.scopes[len]
	a.scopes = a.scopes[:len]
	return scope
}

func (a *analyzer) peek() *scope {
	return a.scopes[len(a.scopes)-1]
}
