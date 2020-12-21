/*
Copyright 2020 The Vitess Authors.

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

package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*join2)(nil)

// join is used to build a Join primitive.
// It's used to build a normal join or a left join
// operation.
type join2 struct {
	//resultColumns []sqlparser.Expr
	//joinPredicate sqlparser.Expr

	// Cols defines which columns from the left
	// or right results should be used to build the
	// return result. For results coming from the
	// left query, the index values go as -1, -2, etc.
	// For the right query, they're 1, 2, etc.
	// If Cols is {-1, -2, 1, 2}, it means that
	// the returned result will be {Left0, Left1, Right0, Right1}.
	Cols []int `json:",omitempty"`

	// Left and Right are the nodes for the join.
	Left, Right logicalPlan
	Opcode      engine.JoinOpcode
}

// Order implements the logicalPlan interface
func (j *join2) Order() int {
	panic("implement me")
}

// ResultColumns implements the logicalPlan interface
func (j *join2) ResultColumns() []*resultColumn {
	panic("implement me")
}

// Reorder implements the logicalPlan interface
func (j *join2) Reorder(i int) {
	panic("implement me")
}

// Wireup implements the logicalPlan interface
func (j *join2) Wireup(lp logicalPlan, jt *jointab) error {
	panic("implement me")
}

// Wireup2 implements the logicalPlan interface
func (j *join2) Wireup2(semTable *semantics.SemTable) error {
	panic("implement me")
}

// SupplyVar implements the logicalPlan interface
func (j *join2) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("implement me")
}

// SupplyCol implements the logicalPlan interface
func (j *join2) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	panic("implement me")
}

// SupplyWeightString implements the logicalPlan interface
func (j *join2) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	panic("implement me")
}

// Primitive implements the logicalPlan interface
func (j *join2) Primitive() engine.Primitive {
	lhs := j.Left.Primitive()
	rhs := j.Right.Primitive()
	return &engine.Join{
		Opcode: j.Opcode,
		Left:   lhs,
		Right:  rhs,
		Cols:   j.Cols,
		Vars:   nil,
	}
}

// Inputs implements the logicalPlan interface
func (j *join2) Inputs() []logicalPlan {
	panic("implement me")
}

// Rewrite implements the logicalPlan interface
func (j *join2) Rewrite(inputs ...logicalPlan) error {
	panic("implement me")
}

// Tables implements the logicalPlan interface
func (j *join2) Tables() []*sqlparser.AliasedTableExpr {
	panic("implement me")
}

// Tables2 implements the logicalPlan interface
func (j *join2) Tables2() semantics.TableSet {
	panic("implement me")
}
