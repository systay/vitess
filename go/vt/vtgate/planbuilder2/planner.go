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

package planbuilder2

import (
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// BuildFromStmt builds a plan based on the AST provided.
func BuildFromStmt(query string, stmt sqlparser.Statement, vschema planbuilder.ContextVSchema, bindVarNeeds sqlparser.BindVarNeeds) (*engine.Plan, error) {
	instruction, err := createInstructionFor(query, stmt, vschema)
	if err != nil {
		return nil, err
	}
	plan := &engine.Plan{
		Type:         sqlparser.ASTToStatementType(stmt),
		Original:     query,
		Instructions: instruction,
		BindVarNeeds: bindVarNeeds,
	}

	return plan, nil
}

// Build builds a plan for a query based on the specified vschema.
// This method is only used from tests
func Build(query string, vschema planbuilder.ContextVSchema) (*engine.Plan, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	result, err := sqlparser.PrepareAST(stmt, map[string]*querypb.BindVariable{}, "", false)
	if err != nil {
		return nil, err
	}

	return BuildFromStmt(query, result.AST, vschema, result.BindVarNeeds)
}

func createInstructionFor(query string, stmt sqlparser.Statement, vschema planbuilder.ContextVSchema) (engine.Primitive, error) {
	switch n := stmt.(type) {
	case *sqlparser.Select:
		return planSelect(n, vschema)
	default:
		return nil, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "implement me")
	}
}

func planSelect(stmt *sqlparser.Select, vschema planbuilder.ContextVSchema) (engine.Primitive, error) {
	var plans []logicalPlan
	for _, tableExpr := range stmt.From {
		plan, err := planTableExpr(tableExpr, vschema)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}

	if len(plans) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "implement me")
	}

	return plans[0].Primitive(), nil

}

func planTableExpr(expr sqlparser.TableExpr, vschema planbuilder.ContextVSchema) (logicalPlan, error) {
	switch n := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch t := n.Expr.(type) {
		case sqlparser.TableName:
			table, _, _, _, err := vschema.FindTable(t)
			if err != nil {
				return nil, err
			}
			return &route{
				opcode:   engine.SelectUnsharded,
				keyspace: table.Keyspace,
				query:    &sqlparser.Select{From: sqlparser.TableExprs{expr}},
			}, nil
		}
	}

	return nil, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "implement me")
}

type logicalPlan interface {
	Primitive() engine.Primitive
}

var _ logicalPlan = (*route)(nil)

type route struct {
	opcode   engine.RouteOpcode
	keyspace *vindexes.Keyspace
	query    *sqlparser.Select
}

func (r *route) Primitive() engine.Primitive {
	fullQuery := sqlparser.String(r.query)
	fieldQuery := *r.query
	cmp := &sqlparser.ComparisonExpr{
		Operator: sqlparser.NotEqualStr,
		Left:     sqlparser.NewIntLiteral([]byte("1")),
		Right:    sqlparser.NewIntLiteral([]byte("1")),
	}
	fieldQuery.Where = sqlparser.NewWhere(sqlparser.WhereStr, cmp)

	route := engine.NewRoute(r.opcode, r.keyspace, fullQuery, sqlparser.String(&fieldQuery))
	route.TableName = "unsharded"
	return route
}
