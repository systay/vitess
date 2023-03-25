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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Projection struct {
	Source      ops.Operator
	columnNames []string
	columns     []sqlparser.Expr
}

func NewProjection(src ops.Operator) ops.PhysicalOperator {
	return &Projection{
		Source: src,
	}
}

func (p *Projection) Clone(inputs []ops.Operator) ops.Operator {
	return &Projection{
		Source:      inputs[0],
		columnNames: slices.Clone(p.columnNames),
		columns:     sqlparser.CloneExprs(p.columns),
	}
}

func (p *Projection) Inputs() []ops.Operator {
	return []ops.Operator{p.Source}
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

func (p *Projection) AddColumn(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (int, error) {
	// TODO
	panic("implement me")
}

func (p *Projection) IPhysical() {}
