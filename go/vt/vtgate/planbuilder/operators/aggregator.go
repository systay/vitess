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
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Aggregator struct {
	Source  ops.Operator
	Columns []AggrColumn

	// Pushed will be set to true once this aggregation has been pushed deeper in the tree
	Pushed bool

	// Original will only be true for the original aggregator created from the AST
	Original bool
}

func (a *Aggregator) Clone(inputs []ops.Operator) ops.Operator {
	return &Aggregator{
		Source:   inputs[0],
		Columns:  slices.Clone(a.Columns),
		Pushed:   a.Pushed,
		Original: a.Original,
	}
}

func (a *Aggregator) Inputs() []ops.Operator {
	return []ops.Operator{a.Source}
}

func (a *Aggregator) SetInputs(operators []ops.Operator) {
	if len(operators) != 0 {
		panic(fmt.Sprintf("unexpected number of operators as input in aggregator: %d", len(operators)))
	}
	a.Source = operators[0]
}

func (a *Aggregator) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newOp, err := a.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	a.Source = newOp
	return a, nil
}

func (a *Aggregator) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, reuseExisting bool) (ops.Operator, int, error) {
	if !reuseExisting {
		return nil, 0, vterrors.VT12001("reuse columns on Aggregator")
	}
	columns, err := a.GetColumns()
	if err != nil {
		return nil, 0, err
	}
	colName, isColName := expr.Expr.(*sqlparser.ColName)
	for i, col := range columns {
		if ctx.SemTable.EqualsExpr(col.Expr, expr.Expr) {
			return a, i, nil
		}
		if isColName && colName.Name.EqualString(col.As.String()) {
			return a, i, nil
		}
	}

	// if we didn't already have this column, we add it as a grouping
	a.Columns = append(a.Columns, &GroupBy{
		Inner:       expr.Expr,
		aliasedExpr: expr,
	})

	return a, len(a.Columns) - 1, nil
}

func (a *Aggregator) GetColumns() (columns []*sqlparser.AliasedExpr, err error) {
	return slices2.Map(a.Columns, func(from AggrColumn) *sqlparser.AliasedExpr {
		return from.GetOriginal()
	}), nil
}

func (a *Aggregator) Description() ops.OpDescription {
	return ops.OpDescription{
		OperatorType: "Aggregator",
	}
}

func (a *Aggregator) ShortDescription() string {
	var grouping []string

	columnnStrings := slices2.Map(a.Columns, func(from AggrColumn) string {
		gb, ok := from.(*GroupBy)
		if ok {
			grouping = append(grouping, sqlparser.String(gb.Inner))
		}
		return from.GetOriginal().ColumnName()
	})

	gb := ""
	if len(grouping) > 0 {
		gb = " group by " + strings.Join(grouping, ",")
	}

	return strings.Join(columnnStrings, ", ") + gb
}

func (a *Aggregator) GetOrdering() ([]ops.OrderBy, error) {
	return a.Source.GetOrdering()
}

var _ ops.Operator = (*Aggregator)(nil)

func (a *Aggregator) planOffsets(ctx *plancontext.PlanningContext) error {
	for idx, column := range a.Columns {
		switch col := column.(type) {
		case *Aggr:
			arg := col.Original.Expr
			if col.Func != nil {
				arg = col.Func.GetArg()
				if arg == nil {
					arg = sqlparser.NewIntLiteral("1")
				}
			}
			newSrc, offset, err := a.Source.AddColumn(ctx, aeWrap(arg), false)
			if err != nil {
				return err
			}
			if offset != idx {
				return vterrors.VT13001("the input column and output columns need to be aligned")
			}
			a.Source = newSrc
		case *GroupBy:
			newSrc, offset, err := a.Source.AddColumn(ctx, aeWrap(col.Inner), true)
			if err != nil {
				return err
			}
			a.Source = newSrc
			if offset != idx {
				return vterrors.VT13001("the input column and output columns need to be aligned")
			}
			if !ctx.SemTable.NeedsWeightString(col.WeightStrExpr) {
				col.WOffset = -1
				continue
			}

			wsExpr := &sqlparser.WeightStringFuncExpr{Expr: col.WeightStrExpr}
			newSrc, offset, err = a.Source.AddColumn(ctx, aeWrap(wsExpr), true)
			if err != nil {
				return err
			}
			col.WOffset = offset
			a.Source = newSrc
		}

	}
	return nil
}