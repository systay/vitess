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

	"vitess.io/vitess/go/vt/vtgate/engine/opcode"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	Aggregator struct {
		Source       ops.Operator
		Columns      []*sqlparser.AliasedExpr
		Aggregations []*Aggr
		AggrIndex    []int

		// GroupingOrder will contain all the information about the GROUP BY that we are producing
		// the outer slice will have one element per expression that we are using in the grouping
		// the inner slice limited to two elements will have the first one pointing to the raw
		// expression and the second one pointing to the WS expression
		GroupingOrder []*offsets

		// Pushed will be set to true once this aggregation has been pushed deeper in the tree
		Pushed bool

		// Original will only be true for the original aggregator created from the AST
		Original      bool
		ResultColumns int

		QP *QueryProjection
	}

	AColumns struct {
		Column *sqlparser.AliasedExpr
	}

	offsets struct {
		col, wsCol int
	}

	// AggrColumn is either an Aggr or a GroupBy - the only types of columns allowed on an Aggregator
	AggrColumn interface {
		GetOriginal() *sqlparser.AliasedExpr
	}
)

func (a *Aggregator) Clone(inputs []ops.Operator) ops.Operator {
	aggrs := slices2.Map(a.Aggregations, func(from *Aggr) *Aggr {
		cpy := *from
		return &cpy
	})
	grouping := slices2.Map(a.GroupingOrder, func(from *offsets) *offsets {
		cpy := *from
		return &cpy
	})
	return &Aggregator{
		Source:        inputs[0],
		Columns:       slices.Clone(a.Columns),
		Aggregations:  aggrs,
		AggrIndex:     slices.Clone(a.AggrIndex),
		GroupingOrder: grouping,
		Pushed:        a.Pushed,
		Original:      a.Original,
		QP:            a.QP,
	}
}

func (a *Aggregator) Inputs() []ops.Operator {
	return []ops.Operator{a.Source}
}

func (a *Aggregator) SetInputs(operators []ops.Operator) {
	if len(operators) != 1 {
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

	op, offset, err := a.Source.AddColumn(ctx, expr, false)
	if err != nil {
		return nil, 0, err
	}
	if len(columns) != offset {
		return nil, 0, vterrors.VT13001("columns do not align")
	}
	a.Source = op

	// if we didn't already have this column, we add it as a random aggregation
	a.Columns = append(a.Columns, expr)
	a.Aggregations = append(a.Aggregations, &Aggr{
		Original: expr,
		OpCode:   opcode.AggregateRandom,
	})

	return a, len(columns) - 1, nil
}

func (a *Aggregator) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return a.Columns, nil
}

func (a *Aggregator) Description() ops.OpDescription {
	return ops.OpDescription{
		OperatorType: "Aggregator",
	}
}

func (a *Aggregator) ShortDescription() string {
	var grouping []string

	columnStrings := slices2.Map(a.Columns, func(from *sqlparser.AliasedExpr) string {
		return sqlparser.String(from)
	})

	for _, gb := range a.GroupingOrder {
		grouping = append(grouping, sqlparser.String(a.Columns[gb.col].Expr))
	}
	var gb string
	if len(grouping) > 0 {
		gb = " group by " + strings.Join(grouping, ",")
	}

	return strings.Join(columnStrings, ", ") + gb
}

func (a *Aggregator) GetOrdering() ([]ops.OrderBy, error) {
	return a.Source.GetOrdering()
}

var _ ops.Operator = (*Aggregator)(nil)

func (a *Aggregator) planOffsets(ctx *plancontext.PlanningContext) error {
	for _, gb := range a.GroupingOrder {
		expr := a.Columns[gb.col]
		weightStrExpr := a.QP.GetSimplifiedExpr(expr.Expr)
		if !ctx.SemTable.NeedsWeightString(weightStrExpr) {
			gb.wsCol = -1
			continue
		}

		wsExpr := &sqlparser.WeightStringFuncExpr{Expr: weightStrExpr}
		newSrc, offset, err := a.Source.AddColumn(ctx, aeWrap(wsExpr), true)
		if err != nil {
			return err
		}
		gb.wsCol = offset
		a.Source = newSrc
		a.Columns = append(a.Columns, aeWrap(weightStrExpr))
	}
	return nil
}

func (a *Aggregator) setTruncateColumnCount(offset int) {
	a.ResultColumns = offset
}

//
// // VisitGroupBys iterates over the GroupBy columns in the Aggregator's grouping order
// // and applies the provided visitor function to each GroupBy column. The visitor
// // function takes the index of the GroupBy column in the grouping order and the GroupBy
// // column itself as arguments.
// func (a *Aggregator) VisitGroupBys(visitor func(grpIdx, colIdx int, gb *GroupBy)) error {
//	for idx, colIdx := range a.GroupingOrder {
//		groupingExpr, ok := a.Columns[colIdx].(*GroupBy)
//		if !ok {
//			return vterrors.VT13001("expected grouping here")
//		}
//		visitor(idx, colIdx, groupingExpr)
//	}
//	return nil
// }
