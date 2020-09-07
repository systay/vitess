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
	"vitess.io/vitess/go/vt/semantic"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
)

// BuildFromStmt builds a plan based on the AST provided.
func BuildFromStmt(query string, stmt sqlparser.Statement, vschema planbuilder.ContextVSchema, bindVarNeeds sqlparser.BindVarNeeds) (*engine.Plan, error) {
	semantic.Analyse(stmt)
	instruction, err := createInstructionFor(stmt, vschema)
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

func createInstructionFor(stmt sqlparser.Statement, vschema planbuilder.ContextVSchema) (engine.Primitive, error) {
	switch n := stmt.(type) {
	case *sqlparser.Select:
		return planSelect(n, vschema)
	default:
		return nil, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "implement me")
	}
}

func findCoveringPlan(plans []logicalPlan, expr sqlparser.Expr) int {
	deps := NewSetFor(expr)
	for i, plan := range plans {
		if deps.CoveredBy(plan.Covers()) {
			return i
		}
	}
	return -1
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

	if plans == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "failed to build anything")
	}

	if len(plans) != 1 {
		// no joins yet
		return nil, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "implement me - joins not yet supported")
	}

	if stmt.Where != nil {
		err := addFilters(stmt.Where.Expr, plans)
		if err != nil {
			return nil, err
		}
	}

	err := addProjections(stmt.SelectExprs, plans)
	if err != nil {
		return nil, err
	}

	// TODO: aggregation, order, limit, having, etc, etc...

	optimizedPlan, _ := optimize(plans[0])

	return optimizedPlan.Primitive(), nil
}

func addProjections(exprs []sqlparser.SelectExpr, plans []logicalPlan) error {
	for _, node := range exprs {
		switch expr := node.(type) {
		case *sqlparser.AliasedExpr:
			idx := findCoveringPlan(plans, expr.Expr)
			if idx < 0 {
				return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "could not find plan for %v", expr)
			} else {
				oldPlan := plans[idx]
				p, isProject := oldPlan.(*project)
				if isProject {
					p.columns = append(p.columns, expr)
				} else {
					plans[idx] = &project{
						input:   oldPlan,
						columns: []sqlparser.SelectExpr{expr},
					}
				}
			}
		}
	}
	return nil
}

func addFilters(expr sqlparser.Expr, plans []logicalPlan) error {
	predicates := sqlparser.SplitAndExpression(nil, expr)
	for _, predicate := range predicates {
		idx := findCoveringPlan(plans, predicate)
		if idx < 0 {
			return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "could not find plan for %v", predicate)
		} else {
			newPlan := &filter{
				input:     plans[idx],
				predicate: predicate,
			}
			plans[idx] = newPlan
		}
	}
	return nil
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
			id := n.Metadata.(int)
			return &route{
				opcode:   engine.SelectUnsharded,
				keyspace: table.Keyspace,
				query:    &sqlparser.Select{From: sqlparser.TableExprs{expr}},
				covers:   NewSetWith(id),
			}, nil
		}
	}

	return nil, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "implement me")
}
