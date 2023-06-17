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

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// SemiJoin is used to perform an existential check
// It's a nested loop kind of operator - for each row coming from the LHS,
// one query will be fired against the RHS, and if at least one row is returned
// from the RHS, the row from the LHS is passed through.
// It is used to represent EXIST/NOT EXISTS and IN/NOT IN against a subquery
type SemiJoin struct {
	LHS, RHS ops.Operator

	// Anti will be true for negative checks - only if no rows are returned
	// from the RHS is a row from the LHS accepted
	Anti bool

	// These are the expressions being compared
	// WHERE LHSExpr IN (SELECT RHSExpr FROM ...)
	// WHERE EXISTS (SELECT 1 FROM ... WHERE LHSExpr = RHSExpr
	LHSExpr, RHSExpr []sqlparser.Expr

	// Fields below are filled in at offset planning

	// Vars are the arguments that need to be copied from the LHS to the RHS
	Vars map[string]int

	// Decides which columns from the input that will be used
	Cols []int
}

func (s *SemiJoin) Clone(inputs []ops.Operator) ops.Operator {
	return &SemiJoin{
		LHS:     inputs[0],
		RHS:     inputs[1],
		Anti:    s.Anti,
		LHSExpr: slices.Clone(s.LHSExpr),
		RHSExpr: slices.Clone(s.RHSExpr),
		Vars:    maps.Clone(s.Vars),
		Cols:    slices.Clone(s.Cols),
	}
}

func (s *SemiJoin) Inputs() []ops.Operator {
	return []ops.Operator{s.LHS, s.RHS}
}

func (s *SemiJoin) SetInputs(operators []ops.Operator) {
	s.LHS = operators[0]
	s.RHS = operators[1]
}

func (s *SemiJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newLHS, err := s.LHS.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	s.LHS = newLHS
	return s, nil
}

func (s *SemiJoin) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, reuseExisting, addToGroupBy bool) (ops.Operator, int, error) {
	newSrc, offset, err := s.LHS.AddColumn(ctx, expr, reuseExisting, addToGroupBy)
	if err != nil {
		return nil, 0, err
	}
	s.LHS = newSrc

	if reuseExisting {
		idx := slices.Index(s.Cols, offset)
		if idx >= 0 {
			return s, idx, nil
		}
	}

	s.Cols = append(s.Cols, offset)
	return s, len(s.Cols) - 1, nil
}

func (s *SemiJoin) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	columns, err := s.LHS.GetColumns()
	if err != nil {
		return nil, err
	}

	result := slices2.Map(s.Cols, func(i int) *sqlparser.AliasedExpr { return columns[i] })
	return result, nil
}

func (s *SemiJoin) ShortDescription() string {
	return fmt.Sprintf("(%s) = (%s)", sqlparser.String(sqlparser.Exprs(s.LHSExpr)), sqlparser.String(sqlparser.Exprs(s.RHSExpr)))
}

func (s *SemiJoin) GetOrdering() ([]ops.OrderBy, error) {
	return s.LHS.GetOrdering()
}

func (s *SemiJoin) planOffsets(ctx *plancontext.PlanningContext) error {
	//TODO implement me
	panic("implement me")
}
