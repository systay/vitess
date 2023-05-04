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

	"vitess.io/vitess/go/vt/vtgate/semantics"

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
	aggrBelowRoute := aggregator.Clone([]ops.Operator{src.Source}).(*Aggregator)
	aggrBelowRoute.Pushed = false // this is a new aggregator which is not pushed

	// Iterate through the aggregations, modifying them as needed.
	for _, aggr := range aggregator.Aggregations {
		// Handle different aggregation operations when pushing down through a sharded route.
		switch aggr.OpCode {
		case opcode.AggregateCount, opcode.AggregateCountStar, opcode.AggregateCountDistinct:
			// All count variations turn into SUM above the Route.
			// Think of it as we are SUMming together a bunch of distributed COUNTs.
			aggr.OriginalOpCode, aggr.OpCode = aggr.OpCode, opcode.AggregateSum
		}
	}

	// Create an empty slice for ordering columns, if needed.
	var ordering []ops.OrderBy
	for _, gb := range aggregator.GroupingOrder {
		// If there is a GROUP BY, add the corresponding order by column.
		ordering = append(ordering, aggregator.generateOrderBy(gb.col))
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
			lhs = append(lhs, NewGroupBy(expr, nil, aeWrap(expr)))
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

type aggrColumns struct {
	Columns      []*sqlparser.AliasedExpr
	Aggregations []*Aggr
	AggrIndex    []int
}

func splitAggrColumnsToLeftAndRight(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	join *ApplyJoin,
) (lhs, rhs aggrColumns, joinColumns []JoinColumn, projections []ProjExpr, err error) {
	lhsTS := TableID(join.LHS)
	rhsTS := TableID(join.RHS)
	if len(aggregator.GroupingOrder) > 0 {
		err = errHorizonNotPlanned()
		return
	}

	handleAggr := func(aggr *Aggr, colIdx int) error {
		switch aggr.OpCode {
		case opcode.AggregateCountStar:
			l, r, j, p := splitCountStar(ctx, aggr, lhsTS, rhsTS)
			lhs.Columns = append(lhs.Columns, l.Columns...)
			lhs.AggrIndex = append(lhs.AggrIndex, l.AggrIndex...)
			lhs.Aggregations = append(lhs.Aggregations, l.Aggregations...)

			rhs.Columns = append(rhs.Columns, r.Columns...)
			rhs.AggrIndex = append(rhs.AggrIndex, r.AggrIndex...)
			rhs.Aggregations = append(rhs.Aggregations, r.Aggregations...)

			joinColumns = append(joinColumns, j...)
			projections = append(projections, p...)
			return nil
		default:
			return errHorizonNotPlanned()
		}
	}

	for _, colIdx := range aggregator.AggrIndex {
		aggr := aggregator.Aggregations[colIdx]
		err = handleAggr(aggr, colIdx)
		if err != nil {
			return
		}
	}

	return
}

// splitCountStar takes a `count(*)` that was above a join and creates the information to
// push it on both sides of the join.
func splitCountStar(
	ctx *plancontext.PlanningContext,
	aggr *Aggr,
	lhsTS, rhsTS semantics.TableSet,
) (lhs, rhs aggrColumns, joinColumns []JoinColumn, projections []ProjExpr) {
	lhsAggr := aggr.Clone()
	rhsAggr := aggr.Clone()
	lhsExpr := sqlparser.CloneExpr(lhsAggr.Original.Expr)
	rhsExpr := sqlparser.CloneExpr(rhsAggr.Original.Expr)

	if lhsExpr == rhsExpr {
		panic(fmt.Sprintf("Need the two produced expressions to be different. %T %T", lhsExpr, rhsExpr))
	}
	ctx.SemTable.Direct[lhsExpr] = lhsTS
	ctx.SemTable.Direct[rhsExpr] = rhsTS
	ctx.SemTable.Recursive[lhsExpr] = lhsTS
	ctx.SemTable.Recursive[rhsExpr] = rhsTS
	lhs = aggrColumns{
		Columns:      []*sqlparser.AliasedExpr{aggr.Original},
		Aggregations: []*Aggr{aggr.Clone()},
		AggrIndex:    []int{0},
	}
	rhs = aggrColumns{
		Columns:      []*sqlparser.AliasedExpr{aggr.Original},
		Aggregations: []*Aggr{aggr.Clone()},
		AggrIndex:    []int{0},
	}

	joinColumns = []JoinColumn{{
		Original: lhsAggr.Original,
		LHSExprs: []sqlparser.Expr{lhsExpr},
	}, {
		Original: rhsAggr.Original,
		RHSExpr:  rhsExpr,
	}}

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
	projections = []ProjExpr{Expr{
		E: projExpr,
	}}
	aggr.Original = aeWrap(projExpr)
	aggr.Func = nil
	aggr.OriginalOpCode = opcode.AggregateCountStar
	aggr.OpCode = opcode.AggregateSum

	return
}
