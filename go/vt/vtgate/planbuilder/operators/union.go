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
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Union struct {
	Sources  []ops.Operator
	Selects  []sqlparser.SelectExprs
	distinct bool

	columns       []*sqlparser.AliasedExpr
	offsetPlanned bool
}

func newUnion(srcs []ops.Operator, stmts []sqlparser.SelectExprs, distinct bool) *Union {
	return &Union{
		Sources:  srcs,
		Selects:  stmts,
		distinct: distinct,
	}
}

// Clone implements the Operator interface
func (u *Union) Clone(inputs []ops.Operator) ops.Operator {
	newOp := *u
	newOp.Sources = inputs
	newOp.Selects = slices.Clone(u.Selects)
	return &newOp
}

func (u *Union) GetOrdering() ([]ops.OrderBy, error) {
	return nil, nil
}

// Inputs implements the Operator interface
func (u *Union) Inputs() []ops.Operator {
	return u.Sources
}

// SetInputs implements the Operator interface
func (u *Union) SetInputs(ops []ops.Operator) {
	u.Sources = ops
}

// AddPredicate adds a predicate a UNION by pushing the predicate to all sources of the UNION.
/* this is done by offset and expression rewriting. Say we have a query like so:
select * (
 	select foo as col, bar from tbl1
	union
	select id, baz from tbl2
) as X where X.col = 42

We want to push down the `X.col = 42` as far down the operator tree as possible. We want
to end up with an operator tree that looks something like this:

select * (
 	select foo as col, bar from tbl1 where foo = 42
	union
	select id, baz from tbl2 where id = 42
) as X

Notice how `X.col = 42` has been translated to `foo = 42` and `id = 42` on respective WHERE clause.
The first SELECT of the union dictates the column names, and the second is whatever expression
can be found on the same offset. The names of the RHS are discarded.
*/
func (u *Union) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	offsets := make(map[string]int)
	sel, err := u.GetSelectFor(0)
	if err != nil {
		return nil, err
	}
	for i, selectExpr := range sel.SelectExprs {
		ae, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, vterrors.VT12001("pushing predicates on UNION where the first SELECT contains * or NEXT")
		}
		offsets[ae.ColumnName()] = i
	}

	for i := range u.Sources {
		var err error
		predicate := sqlparser.CopyOnRewrite(expr, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
			col, ok := cursor.Node().(*sqlparser.ColName)
			if !ok {
				return
			}

			idx, ok := offsets[col.Name.Lowered()]
			if !ok {
				err = vterrors.VT13001("cannot push predicates on concatenate, missing columns from the UNION")
				cursor.StopTreeWalk()
				return
			}

			var sel *sqlparser.Select
			sel, err = u.GetSelectFor(i)
			if err != nil {
				cursor.StopTreeWalk()
				return
			}

			ae, ok := sel.SelectExprs[idx].(*sqlparser.AliasedExpr)
			if !ok {
				err = vterrors.VT09015()
				cursor.StopTreeWalk()
				return
			}
			cursor.Replace(ae.Expr)
		}, nil).(sqlparser.Expr)
		if err != nil {
			return nil, err
		}
		u.Sources[i], err = u.Sources[i].AddPredicate(ctx, predicate)
		if err != nil {
			return nil, err
		}
	}

	return u, nil
}

func (u *Union) GetSelectFor(source int) (*sqlparser.Select, error) {
	src := u.Sources[source]
	for {
		switch op := src.(type) {
		case *Horizon:
			return sqlparser.GetFirstSelect(op.Query), nil
		case *Route:
			src = op.Source
		default:
			return nil, vterrors.VT13001("expected all sources of the UNION to be horizons")
		}
	}
}

func (u *Union) Compact(*plancontext.PlanningContext) (ops.Operator, *rewrite.ApplyResult, error) {
	if u.distinct {
		// first we remove unnecessary DISTINCTs
		for idx, source := range u.Sources {
			d, ok := source.(*Distinct)
			if !ok || !d.Original {
				continue
			}
			u.Sources[idx] = d.Source
		}
	}

	var newSources []ops.Operator
	var newSelects []sqlparser.SelectExprs
	merged := false

	for idx, source := range u.Sources {
		other, ok := source.(*Union)

		if ok && (u.distinct || !other.distinct) {
			newSources = append(newSources, other.Sources...)
			newSelects = append(newSelects, other.Selects...)
			merged = true
			continue
		}

		newSources = append(newSources, source)
		newSelects = append(newSelects, u.Selects[idx])
	}

	if !merged {
		return u, rewrite.SameTree, nil
	}

	u.Sources = newSources
	u.Selects = newSelects
	return u, rewrite.NewTree("merged UNIONs", u), nil
}

func (u *Union) AddColumn(ctx *plancontext.PlanningContext, ae *sqlparser.AliasedExpr, _, addToGroupBy bool) (ops.Operator, int, error) {
	col, err := u.FindCol(ctx, ae.Expr)
	if err != nil {
		return nil, 0, err
	}
	if col >= 0 {
		return u, col, nil
	}

	cols, err := u.GetColumns(ctx)
	if err != nil {
		return nil, 0, err
	}

	switch e := ae.Expr.(type) {
	case *sqlparser.ColName:
		// here we deal with pure column access on top of the union
		offset := slices.IndexFunc(cols, func(expr *sqlparser.AliasedExpr) bool {
			return e.Name.EqualString(expr.ColumnName())
		})
		if offset == -1 {
			return nil, 0, vterrors.VT13001(fmt.Sprintf("could not find the column '%s' on the UNION", sqlparser.String(e)))
		}
		return u, offset, nil
	case *sqlparser.WeightStringFuncExpr:
		wsArg := e.Expr
		argIdx := slices.IndexFunc(cols, func(expr *sqlparser.AliasedExpr) bool {
			return ctx.SemTable.EqualsExprWithDeps(wsArg, expr.Expr)
		})

		if argIdx == -1 {
			return nil, 0, vterrors.VT13001(fmt.Sprintf("could not find the argument to the weight_string function: %s", sqlparser.String(wsArg)))
		}

		outputOffset, err := u.addWeightStringToOffset(ctx, argIdx, addToGroupBy)
		if err != nil {
			return nil, 0, err
		}

		return u, outputOffset, nil
	default:
		return nil, 0, vterrors.VT13001(fmt.Sprintf("only weight_string function is expected - got %s", sqlparser.String(ae)))
	}

}

func (u *Union) addWeightStringToOffset(ctx *plancontext.PlanningContext, argIdx int, addToGroupBy bool) (outputOffset int, err error) {
	for i, src := range u.Sources {
		exprs := u.Selects[i]
		selectExpr := exprs[argIdx]
		ae, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			return 0, vterrors.VT09015()
		}
		newSrc, thisOffset, err := src.AddColumn(ctx, aeWrap(weightStringFor(ae.Expr)), false, addToGroupBy)
		if err != nil {
			return 0, err
		}

		// all offsets for the newly added ws need to line up
		if i == 0 {
			outputOffset = thisOffset
		} else {
			if thisOffset != outputOffset {
				return 0, vterrors.VT12001("weight_string offsets did not line up for UNION")
			}
		}

		u.Sources[i] = newSrc
	}
	return
}

func (u *Union) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (int, error) {
	columns, err := u.GetColumns(ctx)
	if err != nil {
		return 0, err
	}

	for idx, col := range columns {
		if ctx.SemTable.EqualsExprWithDeps(expr, col.Expr) {
			return idx, nil
		}
	}

	return -1, nil
}

func (u *Union) GetColumns(ctx *plancontext.PlanningContext) (result []*sqlparser.AliasedExpr, err error) {
	if u.columns != nil {
		return u.columns, nil
	}

	var columns [][]*sqlparser.AliasedExpr
	for _, source := range u.Sources {
		getColumns, err := source.GetColumns(ctx)
		if err != nil {
			return nil, err
		}
		columns = append(columns, getColumns)
	}

	for idx, column := range columns[0] {
		col := sqlparser.NewColName(column.ColumnName())
		result = append(result, aeWrap(col))

		dd := semantics.EmptyTableSet()
		rd := semantics.EmptyTableSet()
		for _, cols := range columns {
			e := cols[idx].Expr
			dd = dd.Merge(ctx.SemTable.DirectDeps(e))
			rd = rd.Merge(ctx.SemTable.RecursiveDeps(e))
		}

		ctx.SemTable.Direct[col] = dd
		ctx.SemTable.Recursive[col] = rd
	}
	u.columns = result
	return
}

func (u *Union) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return u.Sources[0].GetSelectExprs(ctx)
}

func (u *Union) NoLHSTableSet() {}

func (u *Union) ShortDescription() string {
	if u.distinct {
		return "DISTINCT"
	}
	return ""
}
