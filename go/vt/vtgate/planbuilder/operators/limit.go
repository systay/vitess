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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Limit struct {
	Rowcount sqlparser.Expr
	Offset   sqlparser.Expr
	Source   Operator
}

var _ Operator = (*Limit)(nil)
var _ PhysicalOperator = (*Limit)(nil)
var _ compactable = (*Limit)(nil)

func (l *Limit) IPhysical() {}

func (l *Limit) clone(inputs []Operator) Operator {
	checkSize(inputs, 1)
	return &Limit{
		Rowcount: sqlparser.CloneExpr(l.Rowcount),
		Offset:   sqlparser.CloneExpr(l.Offset),
		Source:   inputs[0],
	}
}

func (l *Limit) inputs() []Operator {
	return []Operator{l.Source}
}

func (l *Limit) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Operator, error) {
	newSrc, err := l.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	l.Source = newSrc
	return l, nil
}

func (l *Limit) AddColumn(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (int, error) {
	return l.Source.AddColumn(ctx, expr)
}

func (l *Limit) compact(*plancontext.PlanningContext) (Operator, bool, error) {
	rb, ok := l.Source.(*Route)
	if !ok || !rb.IsSingleShard() {
		return l, false, nil
	}
	rb.Source, l.Source = l, rb.Source
	return rb, true, nil
}
