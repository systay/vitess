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
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// Projection is used when we need to evaluate expressions on the vtgate
	// It uses the evalengine to accomplish its goal
	Projection struct {
		Source      ops.Operator
		columnNames []string
		columns     []ProjExpr
	}
	ProjExpr interface {
		expr() sqlparser.Expr
	}

	// Offset is used when we are only passing through data from an incoming column
	Offset struct {
		Expr   sqlparser.Expr
		Offset int
	}

	// Eval is used for expressions that have to be evaluated in the vtgate using the evalengine
	Eval struct {
		Expr  sqlparser.Expr
		EExpr evalengine.Expr
	}

	// Expr is used before we have planned, or if we are able to push this down to mysql
	Expr struct {
		E sqlparser.Expr
	}
)

func (po Offset) expr() sqlparser.Expr { return po.Expr }
func (po Eval) expr() sqlparser.Expr   { return po.Expr }
func (po Expr) expr() sqlparser.Expr   { return po.E }

func NewProjection(src ops.Operator) *Projection {
	return &Projection{
		Source: src,
	}
}

func (p *Projection) Clone(inputs []ops.Operator) ops.Operator {
	return &Projection{
		Source:      inputs[0],
		columnNames: slices.Clone(p.columnNames),
		columns:     slices.Clone(p.columns),
	}
}

func (p *Projection) Inputs() []ops.Operator {
	return []ops.Operator{p.Source}
}

func (p *Projection) SetInputs(operators []ops.Operator) {
	p.Source = operators[0]
}

func (p *Projection) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	// we just pass through the predicate to our source
	src, err := p.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	p.Source = src
	return p, nil
}

func (p *Projection) expressions() (result []sqlparser.Expr) {
	for _, col := range p.columns {
		result = append(result, col.expr())
	}
	return
}

func (p *Projection) PushDown(ctx *plancontext.PlanningContext) (ops.Operator, error) {
	switch src := p.Source.(type) {
	case *Route:
		return rewrite.Swap(p, p.Source)
	case *ApplyJoin:
		lhs := TableID(src.LHS)
		rhs := TableID(src.RHS)
		both := lhs.Merge(rhs)
		var lhsColumns []ProjExpr
		var lhsColumnNames []string
		var rhsColumns []ProjExpr
		var rhsColumnNames []string
		for idx, column := range p.columns {
			deps := ctx.SemTable.RecursiveDeps(column.expr())
			// if we get here, it's a new expression we are dealing with.
			// We need to decide if we can push it all on either side,
			// or if we have to break the expression into left and right parts
			switch {
			case deps.IsEmpty(), deps.IsSolvedBy(lhs):
				lhsColumns = append(lhsColumns, column)
				lhsColumnNames = append(lhsColumnNames, p.columnNames[idx])
			case deps.IsSolvedBy(rhs):
				rhsColumns = append(rhsColumns, column)
				rhsColumnNames = append(rhsColumnNames, p.columnNames[idx])
			case deps.IsSolvedBy(both):
				bvNames, lhsExprs, rhsExpr, err := BreakExpressionInLHSandRHS(ctx, column.expr(), lhs)
				if err != nil {
					return nil, err
				}
				src.LHSColumns = append(src.LHSColumns, lhsExprs...)
				for i, lhsExpr := range lhsExprs {
					lhsColumns = append(lhsColumns, &Expr{E: lhsExpr})
					lhsColumnNames = append(lhsColumnNames, lhsExpr.Name.String())
					src.VarsToExpr[bvNames[i]] = lhsExpr
				}

				rhsColumns = append(rhsColumns, &Expr{E: rhsExpr})
				rhsColumnNames = append(rhsColumnNames, p.columnNames[idx])
			default:
				return nil, vterrors.VT13002(sqlparser.String(column.expr()))
			}
		}
		if len(lhsColumns) > 0 {
			lhsProj := NewProjection(src.LHS)
			lhsProj.columnNames = lhsColumnNames
			lhsProj.columns = lhsColumns
			src.LHS = lhsProj
		}
		if len(rhsColumns) > 0 {
			rhsProj := NewProjection(src.RHS)
			rhsProj.columnNames = rhsColumnNames
			rhsProj.columns = rhsColumns
			src.RHS = rhsProj
		}
		return src, nil
	}
	return p, nil
}

func (p *Projection) AddColumn(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	if _, found := canReuseColumn(ctx, p.expressions(), expr); found {
		return p, nil
	}

	if _, isLit := expr.(*sqlparser.Literal); isLit {
		// we are dealing with a literal or something like that. no need to push anything down to our source
		p.columns = append(p.columns, Eval{
			Expr: expr,
		})
		p.columnNames = append(p.columnNames, sqlparser.String(expr))
		return p, nil
	}

	newSrc, err := p.Source.AddColumn(ctx, expr)
	if err != nil {
		return nil, err
	}

	p.Source = newSrc
	p.columns = append(p.columns, Expr{E: expr})

	return p, nil
}
func (p *Projection) GetOffsetFor(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (int, error) {
	if offset, found := canReuseColumn(ctx, p.expressions(), expr); found {
		return offset, nil
	}

	if ctx.SemTable.RecursiveDeps(expr).IsEmpty() {
		// we are dealing with a literal or something like that. no need to push anything down to our source
		p.columns = append(p.columns, Eval{Expr: expr})
		p.columnNames = append(p.columnNames, "")
		return len(p.columns) - 1, nil
	}

	offset, err := p.Source.GetOffsetFor(ctx, expr)
	if err != nil {
		return 0, err
	}

	p.columns = append(p.columns, Offset{
		Expr:   expr,
		Offset: offset,
	})

	return len(p.columns) - 1, nil
}

func (p *Projection) GetColumns() ([]sqlparser.Expr, error) {
	return p.expressions(), nil
}

func (p *Projection) IPhysical() {}
