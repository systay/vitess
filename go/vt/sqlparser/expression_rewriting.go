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

package sqlparser

import (
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// PrepareAST will normalize the query
func PrepareAST(in Statement, bindVars map[string]*querypb.BindVariable, prefix string) (*RewriteASTResult, error) {
	Normalize(in, bindVars, prefix)
	return RewriteAST(in)
}

// BindVarNeeds represents the bind vars that need to be provided as the result of expression rewriting.
type BindVarNeeds struct {
	NeedLastInsertID bool
	NeedDatabase     bool
}

// RewriteAST rewrites the whole AST, replacing function calls and adding column aliases to queries
func RewriteAST(in Statement) (*RewriteASTResult, error) {
	er := newExpressionRewriter()
	Rewrite(in, er.goingDown, nil)

	r := &RewriteASTResult{
		AST: in,
	}
	if _, ok := er.bindVars[LastInsertIDName]; ok {
		r.NeedLastInsertID = true
	}
	if _, ok := er.bindVars[DBVarName]; ok {
		r.NeedDatabase = true
	}

	return r, nil
}

// RewriteASTResult contains the rewritten ast and meta information about it
type RewriteASTResult struct {
	BindVarNeeds
	AST Statement // The rewritten AST
}

type expressionRewriter struct {
	bindVars map[string]struct{}
	err      error
}

func newExpressionRewriter() *expressionRewriter {
	return &expressionRewriter{bindVars: make(map[string]struct{})}
}

const (
	//LastInsertIDName is a reserved bind var name for last_insert_id()
	LastInsertIDName = "__lastInsertId"

	//DBVarName is a reserved bind var name for database()
	DBVarName = "__vtdbname"
)

type funcRewrite struct {
	checkValid  func(f *FuncExpr) error
	bindVarName string
}

var lastInsertID = funcRewrite{
	checkValid: func(f *FuncExpr) error {
		if len(f.Exprs) > 0 {
			return vterrors.New(vtrpc.Code_UNIMPLEMENTED, "Argument to LAST_INSERT_ID() not supported")
		}
		return nil
	},
	bindVarName: LastInsertIDName,
}

var dbName = funcRewrite{
	checkValid: func(f *FuncExpr) error {
		if len(f.Exprs) > 0 {
			return vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "Argument to DATABASE() not supported")
		}
		return nil
	},
	bindVarName: DBVarName,
}

var functions = map[string]*funcRewrite{
	"last_insert_id": &lastInsertID,
	"database":       &dbName,
	"schema":         &dbName,
}

// instead of creating new objects, we'll reuse this one
var token = struct{}{}

func (er *expressionRewriter) goingDown(cursor *Cursor) bool {
	switch node := cursor.Node().(type) {
	case *AliasedExpr:
		if node.As.IsEmpty() {
			buf := NewTrackedBuffer(nil)
			node.Expr.Format(buf)
			inner := newExpressionRewriter()
			tmp := Rewrite(node.Expr, inner.goingDown, nil)
			newExpr, ok := tmp.(Expr)
			if !ok {
				log.Errorf("failed to rewrite AST. function expected to return Expr returned a %s", String(tmp))
				return false
			}
			node.Expr = newExpr
			if inner.didAnythingChange() {
				node.As = NewColIdent(buf.String())
			}
			for k := range inner.bindVars {
				er.bindVars[k] = token
			}
			return false
		}

	case *FuncExpr:
		functionRewriter := functions[node.Name.Lowered()]
		if functionRewriter != nil {
			er.err = functionRewriter.checkValid(node)
			if er.err == nil {
				cursor.Replace(bindVarExpression(functionRewriter.bindVarName))
				er.bindVars[functionRewriter.bindVarName] = token
			}
		}
	}
	return true
}

func (er *expressionRewriter) didAnythingChange() bool {
	return len(er.bindVars) > 0
}

func bindVarExpression(name string) *SQLVal {
	return NewValArg([]byte(":" + name))
}
