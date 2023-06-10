/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"fmt"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// expandHorizon is used to split a horizon into it's components -
// projections, aggregation, grouping, limit, offset, having & ordering
func expandHorizon(ctx *plancontext.PlanningContext, horizon horizonLike) (ops.Operator, *rewrite.ApplyResult, error) {
	sel, isSel := horizon.selectStatement().(*sqlparser.Select)
	if !isSel {
		return nil, nil, errHorizonNotPlanned()
	}

	proj, err := createProjectionFromSelect(ctx, horizon)
	if err != nil {
		return nil, nil, err
	}
	var op ops.Operator = proj

	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, nil, err
	}

	if qp.NeedsDistinct() {
		op = &Distinct{
			Source: op,
			QP:     qp,
		}
	}

	if sel.Having != nil {
		//columns, err := op.GetColumns()
		//if err != nil {
		//	return nil, nil, err
		//}
		expr := sel.Having.Expr
		//sqlparser.Rewrite(expr, func(cursor *sqlparser.Cursor) bool {
		//	e, ok := cursor.Node().(sqlparser.Expr)
		//	if !ok {
		//		return true
		//	}
		//	offset := slices.IndexFunc(columns, func(expr *sqlparser.AliasedExpr) bool {
		//		return ctx.SemTable.EqualsExprWithDeps(expr.Expr, e)
		//	})
		//
		//	if offset >= 0 {
		//		// this expression can be fetched from the input - we can stop here
		//		return false
		//	}
		//
		//	if offsettable(e) {
		//		// this expression has to be fetched from the input, but we didn't find it in the input. let's add it
		//		_, addToGroupBy := e.(*sqlparser.ColName)
		//		proj.addColumnWithoutPushing(aeWrap(e), addToGroupBy)
		//		columns, err = op.GetColumns()
		//		if err != nil {
		//			panic("this should not happen")
		//		}
		//		return false
		//	}
		//	return true
		//}, nil)

		op = &Filter{
			Source:         op,
			Predicates:     sqlparser.SplitAndExpression(nil, expr),
			FinalPredicate: nil,
		}
	}

	if len(qp.OrderExprs) > 0 {
		op = &Ordering{
			Source: op,
			Order:  qp.OrderExprs,
		}
	}

	if sel.Limit != nil {
		op = &Limit{
			Source: op,
			AST:    sel.Limit,
		}
	}

	return op, rewrite.NewTree("expand horizon into smaller components", op), nil
}

func checkInvalid(aggregations []Aggr, horizon horizonLike) error {
	for _, aggregation := range aggregations {
		if aggregation.Distinct {
			return errHorizonNotPlanned()
		}
	}
	if _, isDerived := horizon.(*Derived); isDerived {
		return errHorizonNotPlanned()
	}
	return nil
}

func createProjectionFromSelect(ctx *plancontext.PlanningContext, horizon horizonLike) (out selectExpressions, err error) {
	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, err
	}

	if !qp.NeedsAggregation() {
		projX, err := createProjectionWithoutAggr(qp, horizon.src())
		if err != nil {
			return nil, err
		}
		if derived, isDerived := horizon.(*Derived); isDerived {
			id := derived.TableId
			projX.TableID = &id
			projX.Alias = derived.Alias
		}
		out = projX

		return out, nil
	}

	err = checkAggregationSupported(horizon)
	if err != nil {
		return nil, err
	}

	aggregations, err := qp.AggregationExpressions(ctx)
	if err != nil {
		return nil, err
	}

	if err := checkInvalid(aggregations, horizon); err != nil {
		return nil, err
	}

	a := &Aggregator{
		Source:       horizon.src(),
		Original:     true,
		QP:           qp,
		Grouping:     qp.GetGrouping(),
		Aggregations: aggregations,
	}

	if derived, isDerived := horizon.(*Derived); isDerived {
		id := derived.TableId
		a.TableID = &id
		a.Alias = derived.Alias
	}

outer:
	for colIdx, expr := range qp.SelectExprs {
		ae, err := expr.GetAliasedExpr()
		if err != nil {
			return nil, err
		}
		addedToCol := false
		for idx, groupBy := range a.Grouping {
			if ctx.SemTable.EqualsExprWithDeps(groupBy.SimplifiedExpr, ae.Expr) {
				if !addedToCol {
					a.Columns = append(a.Columns, ae)
					addedToCol = true
				}
				if groupBy.ColOffset < 0 {
					a.Grouping[idx].ColOffset = colIdx
				}
			}
		}
		if addedToCol {
			continue
		}
		for idx, aggr := range a.Aggregations {
			if ctx.SemTable.EqualsExprWithDeps(aggr.Original.Expr, ae.Expr) && aggr.ColOffset < 0 {
				a.Columns = append(a.Columns, ae)
				a.Aggregations[idx].ColOffset = colIdx
				continue outer
			}
		}
		return nil, vterrors.VT13001(fmt.Sprintf("Could not find the %s in aggregation in the original query", sqlparser.String(ae)))
	}

	return a, nil
}

func createProjectionWithoutAggr(qp *QueryProjection, src ops.Operator) (*Projection, error) {
	proj := &Projection{
		Source: src,
	}

	for _, e := range qp.SelectExprs {
		if _, isStar := e.Col.(*sqlparser.StarExpr); isStar {
			return nil, errHorizonNotPlanned()
		}
		ae, err := e.GetAliasedExpr()

		if err != nil {
			return nil, err
		}
		expr := ae.Expr
		if sqlparser.ContainsAggregation(expr) {
			aggr, ok := expr.(sqlparser.AggrFunc)
			if !ok {
				// need to add logic to extract aggregations and pushed them to the top level
				return nil, errHorizonNotPlanned()
			}
			expr = aggr.GetArg()
			if expr == nil {
				expr = sqlparser.NewIntLiteral("1")
			}
		}

		proj.addUnexploredExpr(ae, expr)
	}
	return proj, nil
}
