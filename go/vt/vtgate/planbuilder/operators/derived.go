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

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Derived struct {
	Source Operator

	Query         sqlparser.SelectStatement
	Alias         string
	ColumnAliases sqlparser.Columns

	// Columns needed to feed other plans
	Columns       []*sqlparser.ColName
	ColumnsOffset []int
}

var _ PhysicalOperator = (*Derived)(nil)

// IPhysical implements the PhysicalOperator interface
func (d *Derived) IPhysical() {}

// Clone implements the Operator interface
func (d *Derived) Clone(inputs []Operator) Operator {
	checkSize(inputs, 1)
	clone := *d
	clone.Source = inputs[0]
	clone.ColumnAliases = sqlparser.CloneColumns(d.ColumnAliases)
	clone.Columns = append([]*sqlparser.ColName{}, d.Columns...)
	clone.ColumnsOffset = make([]int, 0, len(d.ColumnsOffset))
	copy(clone.ColumnsOffset, d.ColumnsOffset)
	return &clone
}

// findOutputColumn returns the index on which the given name is found in the slice of
// *sqlparser.SelectExprs of the derivedTree. The *sqlparser.SelectExpr must be of type
// *sqlparser.AliasedExpr and match the given name.
// If name is not present but the query's select expressions contain a *sqlparser.StarExpr
// the function will return no error and an index equal to -1.
// If name is not present and the query does not have a *sqlparser.StarExpr, the function
// will return an unknown column error.
func (d *Derived) findOutputColumn(name *sqlparser.ColName) (int, error) {
	hasStar := false
	for j, exp := range sqlparser.GetFirstSelect(d.Query).SelectExprs {
		switch exp := exp.(type) {
		case *sqlparser.AliasedExpr:
			if !exp.As.IsEmpty() && exp.As.Equal(name.Name) {
				return j, nil
			}
			if exp.As.IsEmpty() {
				col, ok := exp.Expr.(*sqlparser.ColName)
				if !ok {
					return 0, vterrors.VT12001("complex expression needs column alias: %s", sqlparser.String(exp))
				}
				if name.Name.Equal(col.Name) {
					return j, nil
				}
			}
		case *sqlparser.StarExpr:
			hasStar = true
		}
	}

	// we have found a star but no matching *sqlparser.AliasedExpr, thus we return -1 with no error.
	if hasStar {
		return -1, nil
	}
	return 0, vterrors.VT03014(name.Name.String(), "field list")
}

// IsMergeable is not a great name for this function. Suggestions for a better one are welcome!
// This function will return false if the derived table inside it has to run on the vtgate side, and so can't be merged with subqueries
// This logic can also be used to check if this is a derived table that can be had on the left hand side of a vtgate join.
// Since vtgate joins are always nested loop joins, we can't execute them on the RHS
// if they do some things, like LIMIT or GROUP BY on wrong columns
func (d *Derived) IsMergeable(ctx *plancontext.PlanningContext) bool {
	return isMergeable(ctx, d.Query, d)
}

// Inputs implements the Operator interface
func (d *Derived) Inputs() []Operator {
	return []Operator{d.Source}
}

func (d *Derived) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Operator, error) {
	if _, isUNion := d.Source.(*Union); isUNion {
		// If we have a derived table on top of a UNION, we can let the UNION do the expression rewriting
		var err error
		d.Source, err = d.Source.AddPredicate(ctx, expr)
		return d, err
	}
	tableInfo, err := ctx.SemTable.TableInfoForExpr(expr)
	if err != nil {
		if err == semantics.ErrMultipleTables {
			return nil, semantics.ProjError{Inner: vterrors.VT12001(fmt.Sprintf("unable to split predicates to derived table: %s", sqlparser.String(expr)))}
		}
		return nil, err
	}

	newExpr, err := semantics.RewriteDerivedTableExpression(expr, tableInfo)
	if err != nil {
		return nil, err
	}
	d.Source, err = d.Source.AddPredicate(ctx, newExpr)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Derived) AddColumn(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (int, error) {
	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "can't push this expression to a table")
	}

	i, err := d.findOutputColumn(col)
	if err != nil {
		return 0, err
	}
	var pos int
	d.ColumnsOffset, pos = addToIntSlice(d.ColumnsOffset, i)

	// add it to the source if we were not already passing it through
	if i <= -1 {
		d.Columns = append(d.Columns, col)
		_, err := d.Source.AddColumn(ctx, sqlparser.NewColName(col.Name.String()))
		if err != nil {
			return 0, err
		}
	}
	return pos, nil
}

func addToIntSlice(columnOffset []int, valToAdd int) ([]int, int) {
	for idx, val := range columnOffset {
		if val == valToAdd {
			return columnOffset, idx
		}
	}
	columnOffset = append(columnOffset, valToAdd)
	return columnOffset, len(columnOffset) - 1
}
