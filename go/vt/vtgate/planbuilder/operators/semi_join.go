/*
Copyright 2022 The Vitess Authors.

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
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// SemiJoin is a correlated subquery that is used for filtering rows from the outer query.
// It is a join between the outer query and the subquery, where the subquery is the RHS.
// We are only interested in the existence of rows in the RHS, so we only need to know if
type SemiJoin struct {
	Outer    ops.Operator
	Subquery ops.Operator

	// JoinCols are the columns from the LHS used for the join.
	// These are the same columns pushed on the LHS that are now used in the Vars field
	JoinVars map[string]*sqlparser.ColName

	// arguments that need to be copied from the outer to inner
	// this field is filled in at offset planning time
	JoinVarOffsets map[string]int

	// Original is the original expression, including comparison operator or EXISTS expression
	Original sqlparser.Expr

	// inside and outside are the columns from the LHS and RHS respectively that are used in the semi join
	// only if the expressions are pure/bare/simple ColName:s, otherwise they are not added to these lists
	// for the predicate: tbl.id IN (SELECT bar(foo) from user WHERE tbl.id = user.id)
	// for the predicate: EXISTS (select 1 from user where tbl.ud = bar(foo) AND tbl.id = user.id limit)
	// We would store `tbl.id` in JoinVars, but nothing on the inside, since the expression
	// `foo(tbl.id)` is not a bare column
	// the first offset is the outer column, and the second is the inner
	comparisonColumns [][2]*sqlparser.ColName

	_sq *sqlparser.Subquery // (SELECT foo from user LIMIT 1)

	// if we are unable to
	rhsPredicate sqlparser.Expr
}

func (sj *SemiJoin) planOffsets(ctx *plancontext.PlanningContext) error {
	sj.JoinVarOffsets = make(map[string]int, len(sj.JoinVars))
	for bindvarName, col := range sj.JoinVars {
		offsets, err := sj.Outer.AddColumns(ctx, true, []bool{false}, []*sqlparser.AliasedExpr{aeWrap(col)})
		if err != nil {
			return err
		}
		sj.JoinVarOffsets[bindvarName] = offsets[0]
	}
	return nil
}

func (sj *SemiJoin) SetOuter(operator ops.Operator) {
	sj.Outer = operator
}

func (sj *SemiJoin) OuterExpressionsNeeded() []*sqlparser.ColName {
	return maps.Values(sj.JoinVars)
}

var _ SubQuery = (*SemiJoin)(nil)

func (sj *SemiJoin) Inner() ops.Operator {
	return sj.Subquery
}

func (sj *SemiJoin) OriginalExpression() sqlparser.Expr {
	return sj.Original
}

func (sj *SemiJoin) sq() *sqlparser.Subquery {
	return sj._sq
}

// Clone implements the Operator interface
func (sj *SemiJoin) Clone(inputs []ops.Operator) ops.Operator {
	klone := *sj
	switch len(inputs) {
	case 1:
		klone.Subquery = inputs[0]
	case 2:
		klone.Outer = inputs[0]
		klone.Subquery = inputs[1]
	default:
		panic("wrong number of inputs")
	}
	klone.JoinVars = maps.Clone(sj.JoinVars)
	klone.JoinVarOffsets = maps.Clone(sj.JoinVarOffsets)
	return &klone
}

func (sj *SemiJoin) GetOrdering() ([]ops.OrderBy, error) {
	return nil, nil
}

// Inputs implements the Operator interface
func (sj *SemiJoin) Inputs() []ops.Operator {
	if sj.Outer == nil {
		return []ops.Operator{sj.Subquery}
	}

	return []ops.Operator{sj.Outer, sj.Subquery}
}

// SetInputs implements the Operator interface
func (sj *SemiJoin) SetInputs(inputs []ops.Operator) {
	switch len(inputs) {
	case 1:
		sj.Subquery = inputs[0]
	case 2:
		sj.Outer = inputs[0]
		sj.Subquery = inputs[1]
	default:
		panic("wrong number of inputs")
	}
}

func (sj *SemiJoin) ShortDescription() string {
	return ""
}

func (sj *SemiJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	//TODO implement me
	panic("implement me")
}

func (sj *SemiJoin) AddColumns(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	return sj.Outer.AddColumns(ctx, reuseExisting, addToGroupBy, exprs)
}

func (sj *SemiJoin) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	return sj.Outer.FindCol(ctx, expr, underRoute)
}

func (sj *SemiJoin) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	//TODO implement me
	panic("implement me")
}

func (sj *SemiJoin) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	//TODO implement me
	panic("implement me")
}
