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

package planbuilder

import (
	"fmt"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type (
	Horizon struct {
		aggregateFuncs []AnalysedAliasedExpr
		projections    []AnalysedAliasedExpr
		hasStar        bool
	}
	AnalysedExpr struct {
		pullouts []*pulloutSubquery
		origin   logicalPlan
		expr     sqlparser.Expr
	}
	AnalysedAliasedExpr struct {
		pullouts []*pulloutSubquery
		origin   logicalPlan
		expr     *sqlparser.AliasedExpr
	}
)

func createColumnsFor(tables []*table) sqlparser.SelectExprs {
	result := sqlparser.SelectExprs{}
	singleTable := false
	if len(tables) == 1 {
		singleTable = true
	}
	for _, t := range tables {
		for _, col := range t.columnNames {
			var expr *sqlparser.AliasedExpr
			if singleTable {
				// If there's only one table, we use unqualified column names.
				expr = &sqlparser.AliasedExpr{
					Expr: &sqlparser.ColName{
						Name: col,
					},
				}
			} else {
				// If a and b have id as their column, then
				// select * from a join b should result in
				// select a.id as id, b.id as id from a join b.
				expr = &sqlparser.AliasedExpr{
					Expr: columnForQualifiedStar(col, t.alias),
					As:   col,
				}
			}
			result = append(result, expr)
		}
	}
	return result
}

func expandStars(
	tables []*table,
	selectExprs sqlparser.SelectExprs,
	findTable func(sqlparser.TableName) (*table, error),
) (stillHasStars bool, result sqlparser.SelectExprs, err error) {
	isAuthoritative := func(t *table, expr *sqlparser.StarExpr) bool {
		if t.isAuthoritative {
			return true
		}
		// we don't know the columns of this table, we have to just return the star and hope we are dealing with a route
		stillHasStars = true
		result = append(result, expr)
		return false
	}

	for _, expr := range selectExprs {
		star, isStar := expr.(*sqlparser.StarExpr)
		if !isStar {
			result = append(result, expr)
			continue
		}

		if star.TableName.IsEmpty() {
			// SELECT *
			for _, t := range tables {
				// All tables must have authoritative column lists.
				if !isAuthoritative(t, star) {
					return stillHasStars, result, nil
				}
			}
			result = append(result, createColumnsFor(tables)...)
		} else {
			// star qualified with table name
			// SELECT user.*
			t, err := findTable(star.TableName)
			if err != nil {
				return false, nil, err
			}
			if !isAuthoritative(t, star) {
				return stillHasStars, result, nil
			}

			// we have all we need - let's expand
			for _, col := range t.columnNames {
				result = append(result, &sqlparser.AliasedExpr{Expr: columnForQualifiedStar(col, star.TableName)})
			}
		}
	}

	return stillHasStars, result, nil
}

func columnForQualifiedStar(col sqlparser.ColIdent, tableName sqlparser.TableName) *sqlparser.ColName {
	return &sqlparser.ColName{
		Name:      col,
		Qualifier: tableName,
	}
}

func (pb *primitiveBuilder) analyseSelectExpr(sel *sqlparser.Select) (*Horizon, error) {
	stillHasStars, selectExprs, err := expandStars(pb.st.AllTables(), sel.SelectExprs, pb.st.FindTable)
	if err != nil {
		return nil, err
	}
	sel.SelectExprs = selectExprs

	result := &Horizon{hasStar: stillHasStars}
	if stillHasStars {
		// We'll allow select * for simple routes.
		return result, nil
	}
	for _, node := range selectExprs {
		switch node := node.(type) {
		case *sqlparser.AliasedExpr:
			pullouts, origin, expr, err := pb.findOrigin(node.Expr)
			if err != nil {
				return nil, err
			}
			node.Expr = expr
			analysedExpr := AnalysedAliasedExpr{
				pullouts: pullouts,
				origin:   origin,
				expr:     node,
			}
			if isAggregateExpression(expr) {
				result.aggregateFuncs = append(result.aggregateFuncs, analysedExpr)
			} else {
				result.projections = append(result.projections, analysedExpr)
			}

		default:
			return nil, fmt.Errorf("BUG: unexpected select expression type: %T", node)
		}
	}
	return result, nil
}

func isAggregateExpression(expr sqlparser.Expr) bool {
	if inner, ok := expr.(*sqlparser.FuncExpr); ok {
		if _, ok := engine.SupportedAggregates[inner.Name.Lowered()]; ok {
			return true
		}
	}
	return false
}

func (pb *primitiveBuilder) planHorizon(sel *sqlparser.Select, horizon *Horizon) error {
	rb, isRoute := pb.plan.(*route)
	if isRoute {
		// since we can push down all of the aggregation to the route,
		// we don't need to do anything else here
		rb.Select = sel
		return nil
	}
	resultColumns := make([]*resultColumn, 0, len(horizon.projections)+len(horizon.aggregateFuncs))
	for _, projection := range horizon.projections {
		newBuilder, rc, _, err := pb.pushProjection(pb.plan, projection.expr, projection.origin)
		if err != nil {
			return err
		}
		pb.plan = newBuilder
		resultColumns = append(resultColumns, rc)
	}
	pb.st.SetResultColumns(resultColumns)
	return nil
}

func (pb *primitiveBuilder) pushProjection(in logicalPlan, expr *sqlparser.AliasedExpr, origin logicalPlan) (logicalPlan, *resultColumn, int, error) {
	switch node := in.(type) {
	case *join:
		var rc *resultColumn
		if node.isOnLeft(origin.Order()) {
			newLeft, col, colNumber, err := pb.pushProjection(node.Left, expr, origin)
			if err != nil {
				return nil, nil, 0, err
			}
			node.ejoin.Cols = append(node.ejoin.Cols, -colNumber-1)
			rc = col
			node.Left = newLeft
		} else {
			// Pushing of non-trivial expressions not allowed for RHS of left joins.
			if _, ok := expr.Expr.(*sqlparser.ColName); !ok && node.ejoin.Opcode == engine.LeftJoin {
				return nil, nil, 0, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard left join and column expressions")
			}

			newRight, col, colNumber, err := pb.pushProjection(node.Right, expr, origin)
			if err != nil {
				return nil, nil, 0, err
			}
			node.ejoin.Cols = append(node.ejoin.Cols, colNumber+1)
			rc = col
			node.Right = newRight
		}
		node.resultColumns = append(node.resultColumns, rc)
		return in, rc, len(node.resultColumns) - 1, nil

	case *route:
		sel := node.Select.(*sqlparser.Select)
		sel.SelectExprs = append(sel.SelectExprs, expr)

		rc := newResultColumn(expr, node)
		node.resultColumns = append(node.resultColumns, rc)
		return node, rc, len(node.resultColumns) - 1, nil

	default:
		return nil, nil, 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T.pushProjection: unreachable", in)
	}
}
