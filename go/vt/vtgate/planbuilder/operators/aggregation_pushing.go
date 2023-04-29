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
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func tryPushingDownAggregator(ctx *plancontext.PlanningContext, aggregator *Aggregator) (ops.Operator, rewrite.ApplyResult, error) {
	if aggregator.Pushed {
		return aggregator, rewrite.SameTree, nil
	}
	aggregator.Pushed = true
	switch src := aggregator.Source.(type) {
	case *Route:
		return pushDownAggregationThroughRoute(aggregator, src)
	case *ApplyJoin:
		return pushDownAggregationThroughJoin(ctx, aggregator, src)
	default:
		return aggregator, rewrite.SameTree, nil
	}
}

func pushDownAggregationThroughRoute(aggregator *Aggregator, src *Route) (ops.Operator, rewrite.ApplyResult, error) {
	// If the route is single-shard, just swap the aggregator and route.
	if src.IsSingleShard() {
		return swap(aggregator, src)
	}

	// Create a new aggregator to be placed below the route.
	aggrBelowRoute := &Aggregator{
		Source:  src.Source,
		Columns: slices.Clone(aggregator.Columns),
		Pushed:  false,
	}

	// Create an empty slice for ordering columns, if needed.
	var ordering []ops.OrderBy

	// Iterate through the aggregator columns, modifying them as needed.
	for i, col := range aggregator.Columns {
		switch param := col.(type) {
		case *Aggr:
			// Handle different aggregation operations when pushing down through a sharded route.
			switch param.OpCode {
			case opcode.AggregateCount, opcode.AggregateCountStar, opcode.AggregateCountDistinct:
				// All count variations turn into SUM above the Route.
				// Think of it as we are SUMming together a bunch of distributed COUNTs.
				param.OriginalOpCode, param.OpCode = param.OpCode, opcode.AggregateSum
			}
			aggregator.Columns[i] = param
		case *GroupBy:
			// If there is a GROUP BY, add the corresponding order by column.
			ordering = append(ordering, param.AsOrderBy())
		}
	}

	// Set the source of the route to the new aggregator placed below the route.
	src.Source = aggrBelowRoute

	if !aggregator.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return aggregator.Source, rewrite.NewTree, nil
	}

	// If ordering is required (i.e., there is a GROUP BY), create an Ordering operation.
	if len(ordering) > 0 {
		aggregator.Source = &Ordering{
			Source: src,
			Order:  ordering,
		}
	}

	return aggregator, rewrite.NewTree, nil
}

/*
We push down aggregations using the logic from the paper Orthogonal Optimization of Subqueries and Aggregation, by
Cesar A. Galindo-Legaria and Milind M. Joshi from Microsoft Corp.

It explains how one can split an aggregation into local aggregates that depend on only one side of the join.
The local aggregates can then be gathered together to produce the global
group by/aggregate query that the user asked for.

In Vitess, this is particularly useful because it allows us to push aggregation down to the routes, even when
we have to join the results at the vtgate level. Instead of doing all the grouping and aggregation at the
vtgate level, we can offload most of the work to MySQL, and at the vtgate just summarize the results.

# For a query, such as

select count(*) from R1 JOIN R2 on R1.id = R2.id

Original:

		 GB         <- This is the original grouping, doing count(*)
		 |
		JOIN
		/  \
	  R1   R2

Transformed:

			 GB1    <- This grouping is now SUMing together the distributed `count(*)` we got back
			  |
			Proj
			  |
			Sort
			  |
			JOIN
		   /    \
		 GB2    GB3
		/         \
	   R1          R2
*/
func pushDownAggregationThroughJoin(ctx *plancontext.PlanningContext, aggregator *Aggregator, join *ApplyJoin) (ops.Operator, rewrite.ApplyResult, error) {
	// First we separate columns according to if they need data from the LHS/RHS
	// lhs/rhs will contain the aggregation columns we need from GB2/3 in the illustration above
	// joinColumns are the new column passed through the join. we can safely remove the old join columns
	// projections will contain any arithmetic operations we might need to do, such as multiplying values
	lhs, rhs, joinColumns, projections, err := splitAggrColumnsToLeftAndRight(ctx, aggregator, join)
	if err != nil {
		return nil, false, err
	}

	// We need to add any columns coming from the lhs of the join to the group by on that side
	// If we don't, the LHS will not be able to return the column, and it can't be used to send down to the RHS
	for _, pred := range join.JoinPredicates {
		for _, expr := range pred.LHSExprs {
			lhs = append(lhs, &GroupBy{
				Inner:       expr,
				aliasedExpr: aeWrap(expr),
			})
		}
	}

	gb2 := &Aggregator{
		Source:  join.LHS,
		Columns: lhs,
	}
	gb3 := &Aggregator{
		Source:  join.RHS,
		Columns: rhs,
	}
	join.LHS, join.RHS = gb2, gb3
	join.ColumnsAST = joinColumns
	proj := &Projection{
		Source:      join,
		ColumnNames: []string{""},
		Columns:     projections,
	}

	if !aggregator.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return proj, rewrite.NewTree, nil
	}

	aggregator.Source = proj
	return aggregator, rewrite.NewTree, nil
}

func splitAggrColumnsToLeftAndRight(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	join *ApplyJoin,
) (lhs, rhs []AggrColumn, joinColumns []JoinColumn, projections []ProjExpr, err error) {
	lhsTS := TableID(join.LHS)
	rhsTS := TableID(join.RHS)

	handleAggr := func(aggr *Aggr) {
		if aggr.OpCode == opcode.AggregateCountStar {
			lhsAggr := aggr.Clone()
			rhsAggr := aggr.Clone()
			lhsExpr := sqlparser.CloneExpr(lhsAggr.Original.Expr)
			rhsExpr := sqlparser.CloneExpr(rhsAggr.Original.Expr)
			if lhsExpr == rhsExpr {
				panic(432)
			}
			ctx.SemTable.Direct[lhsExpr] = lhsTS
			ctx.SemTable.Direct[rhsExpr] = rhsTS
			ctx.SemTable.Recursive[lhsExpr] = lhsTS
			ctx.SemTable.Recursive[rhsExpr] = rhsTS
			lhs = append(lhs, lhsAggr)
			rhs = append(rhs, rhsAggr)

			joinColumns = append(joinColumns,
				JoinColumn{
					Original: lhsAggr.Original,
					LHSExprs: []sqlparser.Expr{lhsExpr},
				},
				JoinColumn{
					Original: rhsAggr.Original,
					RHSExpr:  rhsExpr,
				})
			projExpr := &sqlparser.BinaryExpr{
				Operator: sqlparser.MultOp,
				Left:     lhsExpr,
				Right: &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("coalesce"),
					Exprs: sqlparser.SelectExprs{
						&sqlparser.AliasedExpr{Expr: rhsExpr},
						&sqlparser.AliasedExpr{Expr: sqlparser.NewIntLiteral("1")},
					},
				},
			}
			projections = append(projections, Expr{
				E: projExpr,
			})
			aggr.Original = aeWrap(projExpr)
			aggr.Func = nil
			aggr.OriginalOpCode = opcode.AggregateCountStar
			aggr.OpCode = opcode.AggregateSum

			return
		}
		// deps := ctx.SemTable.RecursiveDeps(aggr.Original.Expr)
		// var other *Aggr
		// // if we are sending down min/max/random, we don't have to multiply the results with anything
		// if !isMinOrMax(newAggr.OpCode) && !isRandom(newAggr.OpCode) {
		// 	other = countStarAggr()
		// }
		// switch {
		// case deps.IsSolvedBy(lhsTS):
		// 	lhs = append(lhs, &newAggr)
		// 	rhs = append(rhs, other)
		// case deps.IsSolvedBy(rhsTS):
		// 	rhs = append(rhs, &newAggr)
		// 	lhs = append(lhs, other)
		// default:
		// 	err = vterrors.VT12001("aggregation on columns from different sources")
		// 	return
		// }

	}

	for _, col := range aggregator.Columns {
		switch col := col.(type) {
		case *Aggr:
			handleAggr(col)
			if err != nil {
				return
			}
		case *GroupBy:
			err = errHorizonNotPlanned()
			return
			// deps := ctx.SemTable.RecursiveDeps(col.Inner)
			// switch {
			// case deps.IsSolvedBy(lhsTS):
			//
			// 	groupingOffsets = append(groupingOffsets, -(len(lhsGrouping) + 1))
			// 	lhsGrouping = append(lhsGrouping, groupBy)
			// case deps.IsSolvedBy(rhsTS):
			// 	groupingOffsets = append(groupingOffsets, len(rhsGrouping)+1)
			// 	rhsGrouping = append(rhsGrouping, groupBy)
			// default:
			// 	return nil, nil, nil, vterrors.VT12001("grouping on columns from different sources")
			// }
		}
	}
	return
}

func isMinOrMax(in opcode.AggregateOpcode) bool {
	switch in {
	case opcode.AggregateMin, opcode.AggregateMax:
		return true
	default:
		return false
	}
}

func isCount(in opcode.AggregateOpcode) bool {
	switch in {
	case opcode.AggregateCount, opcode.AggregateCountStar, opcode.AggregateCountDistinct:
		return true
	default:
		return false
	}
}

func isRandom(in opcode.AggregateOpcode) bool {
	return in == opcode.AggregateRandom
}

func countStarAggr() *Aggr {
	f := &sqlparser.CountStar{}

	return &Aggr{
		Original: &sqlparser.AliasedExpr{Expr: f},
		OpCode:   opcode.AggregateCountStar,
		Alias:    "count(*)",
	}
}