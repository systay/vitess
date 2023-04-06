/*
Copyright 2021 The Vitess Authors.

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
	"golang.org/x/exp/slices"
	"vitess.io/vitess/go/slices2"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// ApplyJoin is a nested loop join - for each row on the LHS,
// we'll execute the plan on the RHS, feeding data from left to right
type ApplyJoin struct {
	// Pre-Offset Binding - these fields are filled in before offset binding
	LHS, RHS ops.Operator

	// LeftJoin will be true in the case of an outer join
	LeftJoin bool

	// JoinCols are the columns from the LHS used for the join.
	// These are the same columns pushed on the LHS that are now used in the Vars field
	LHSColumns []*sqlparser.ColName

	Predicate sqlparser.Expr

	// VarsToExpr are the arguments that we need to copy from LHS to RHS
	VarsToExpr map[string]sqlparser.Expr

	// ColumnsAST keeps track of what AST expression is represented in the Columns array
	ColumnsAST []JoinCol[sqlparser.Expr]

	// Post Offset Binding - these fields will not contain anything until after offset-binding

	// Columns stores the column indexes of the columns coming from the left and right side
	// negative value comes from LHS and positive from RHS
	Columns []int

	// Vars are the arguments that need to be copied from the LHS to the RHS
	Vars map[string]int
}

type JoinCol[K any] struct {
	Left bool
	Expr K
}

var _ ops.PhysicalOperator = (*ApplyJoin)(nil)

func NewApplyJoin(lhs, rhs ops.Operator, predicate sqlparser.Expr, leftOuterJoin bool) *ApplyJoin {
	return &ApplyJoin{
		LHS:        lhs,
		RHS:        rhs,
		Vars:       map[string]int{},
		VarsToExpr: map[string]sqlparser.Expr{},
		Predicate:  predicate,
		LeftJoin:   leftOuterJoin,
	}
}

// IPhysical implements the PhysicalOperator interface
func (a *ApplyJoin) IPhysical() {}

// Clone implements the Operator interface
func (a *ApplyJoin) Clone(inputs []ops.Operator) ops.Operator {
	return &ApplyJoin{
		LHS:        inputs[0],
		RHS:        inputs[1],
		Columns:    slices.Clone(a.Columns),
		ColumnsAST: slices.Clone(a.ColumnsAST),
		Vars:       maps.Clone(a.Vars),
		VarsToExpr: maps.Clone(a.VarsToExpr),
		LeftJoin:   a.LeftJoin,
		Predicate:  sqlparser.CloneExpr(a.Predicate),
		LHSColumns: slices.Clone(a.LHSColumns),
	}
}

func (a *ApplyJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	return AddPredicate(a, ctx, expr, false, newFilter)
}

// Inputs implements the Operator interface
func (a *ApplyJoin) Inputs() []ops.Operator {
	return []ops.Operator{a.LHS, a.RHS}
}

// SetInputs implements the Operator interface
func (a *ApplyJoin) SetInputs(inputs []ops.Operator) {
	a.LHS, a.RHS = inputs[0], inputs[1]
}

var _ JoinOp = (*ApplyJoin)(nil)

func (a *ApplyJoin) GetLHS() ops.Operator {
	return a.LHS
}

func (a *ApplyJoin) GetRHS() ops.Operator {
	return a.RHS
}

func (a *ApplyJoin) SetLHS(operator ops.Operator) {
	a.LHS = operator
}

func (a *ApplyJoin) SetRHS(operator ops.Operator) {
	a.RHS = operator
}

func (a *ApplyJoin) MakeInner() {
	a.LeftJoin = false
}

func (a *ApplyJoin) IsInner() bool {
	return !a.LeftJoin
}

func (a *ApplyJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error {
	bvName, cols, predicate, err := BreakExpressionInLHSandRHS(ctx, expr, TableID(a.LHS))
	if err != nil {
		return err
	}
	for i, col := range cols {
		offset, err := a.pushColLeft(ctx, col)
		if err != nil {
			return err
		}
		a.Vars[bvName[i]] = offset
	}
	a.LHSColumns = append(a.LHSColumns, cols...)

	rhs, err := a.RHS.AddPredicate(ctx, predicate)
	if err != nil {
		return err
	}
	a.RHS = rhs

	a.Predicate = ctx.SemTable.AndExpressions(expr, a.Predicate)
	return nil
}

func (a *ApplyJoin) pushColLeft(ctx *plancontext.PlanningContext, e sqlparser.Expr) error {
	newSrc, err := a.LHS.AddColumn(ctx, e)
	if err != nil {
		return err
	}
	a.LHS = newSrc
	return nil
}

func (a *ApplyJoin) pushColRight(ctx *plancontext.PlanningContext, e sqlparser.Expr) error {
	newSrc, err := a.RHS.AddColumn(ctx, e)
	if err != nil {
		return err
	}
	a.RHS = newSrc
	return nil
}

func (a *ApplyJoin) GetColumns() ([]sqlparser.Expr, error) {
	return a.expressions(), nil
}

func (a *ApplyJoin) expressions() []sqlparser.Expr {
	return slices2.Map(a.ColumnsAST, func(from JoinCol[sqlparser.Expr]) sqlparser.Expr {
		return from.Expr
	})
}

func (a *ApplyJoin) AddColumn(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	if _, found := canReuseColumn(ctx, a.expressions(), expr); found {
		return a, nil
	}

	lhs := TableID(a.LHS)
	rhs := TableID(a.RHS)
	both := lhs.Merge(rhs)
	deps := ctx.SemTable.RecursiveDeps(expr)

	// if we get here, it's a new expression we are dealing with.
	// We need to decide if we can push it all on either side,
	// or if we have to break the expression into left and right parts
	switch {
	case deps.IsSolvedBy(lhs):
		offset, err := a.pushColLeft(ctx, expr)
		if err != nil {
			return nil, err
		}
		a.Columns = append(a.Columns, -offset-1)
		a.ColumnsAST = append(a.ColumnsAST, left(expr))
	case deps.IsSolvedBy(both) && !deps.IsSolvedBy(rhs):
		bvNames, lhsExprs, rhsExpr, err := BreakExpressionInLHSandRHS(ctx, expr, lhs)
		if err != nil {
			return nil, err
		}
		for i, lhsExpr := range lhsExprs {
			offset, err := a.pushColLeft(ctx, lhsExpr)
			if err != nil {
				return nil, err
			}
			a.Vars[bvNames[i]] = offset
		}
		expr = rhsExpr
		fallthrough // what remains of the expression is pushed on the RHS
	case deps.IsSolvedBy(rhs):
		offset, err := a.pushColRight(ctx, expr)
		if err != nil {
			return nil, err
		}
		a.Columns = append(a.Columns, offset+1)
		a.ColumnsAST = append(a.ColumnsAST, right(expr))
	default:
		return nil, vterrors.VT13002(sqlparser.String(expr))
	}

	return a, nil
}

func (a *ApplyJoin) GetOffsetFor(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (int, error) {
	if offset, found := canReuseColumn(ctx, a.expressions(), expr); found {
		return offset, nil
	}
	return 0, vterrors.VT13001("no new columns should be pushed during offset calculations")
}

func left(e sqlparser.Expr) JoinCol[sqlparser.Expr] {
	return JoinCol[sqlparser.Expr]{
		Left: true,
		Expr: e,
	}
}

func right(e sqlparser.Expr) JoinCol[sqlparser.Expr] {
	return JoinCol[sqlparser.Expr]{
		Left: false,
		Expr: e,
	}
}
