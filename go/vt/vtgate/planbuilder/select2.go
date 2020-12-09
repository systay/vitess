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
	"fmt"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func (pb *primitiveBuilder) processSelect2(sel *sqlparser.Select, outer *symtab) error {
	err := pb.planJoinTree(sel, outer)
	if err != nil {
		return vterrors.Wrapf(err, "failed to plan the query horizon")
	}

	horizon, err := pb.analyseSelectExpr(sel)
	if err != nil {
		return vterrors.Wrapf(err, "failed to plan the query horizon")
	}
	err = pb.planHorizon(sel, horizon)
	if err != nil {
		return vterrors.Wrapf(err, "failed to plan the query horizon")
	}
	return nil
}

func hasValues(offset int) string { return fmt.Sprintf(":__sq_has_values%d", offset) }
func sqName(offset int) string    { return fmt.Sprintf(":__sq%d", offset) }

type subq struct {
	sq *sqlparser.Subquery

	// the expression we are rewriting, and what we are rewriting it to
	from, to sqlparser.SQLNode

	// offset of the subqueries seen. used for naming
	idx int

	// what kind of sub-query this is
	opCode engine.PulloutOpcode
}

func (pb *primitiveBuilder) planSubQueries(subqueries []subq) error {
	for _, sq := range subqueries {
		spb := newPrimitiveBuilder(pb.vschema, pb.jt)
		switch stmt := sq.sq.Select.(type) {
		case *sqlparser.Select:
			if err := spb.processSelect(stmt, pb.st, ""); err != nil {
				return err
			}
		case *sqlparser.Union:
			if err := spb.processUnion(stmt, pb.st); err != nil {
				return err
			}
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unexpected SELECT type: %T", stmt)
		}
		p2 := &pulloutSubquery{
			subquery: spb.plan,
			primitive: &engine.PulloutSubquery{
				Opcode:         engine.PulloutValue,
				SubqueryResult: sqName(sq.idx),
				HasValues:      hasValues(sq.idx),
			},
			underlying: pb.plan,
		}
		p2.underlying.Reorder(p2.subquery.Order())
		p2.order = p2.underlying.Order() + 1
		pb.plan = p2
		pb.plan.Reorder(0)
	}
	return nil
}

func extractSubqueries(stmt sqlparser.Statement) []subq {
	counter := 1
	var subqueries []subq
	sqlparser.Rewrite(stmt, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.Subquery:
			var opCode engine.PulloutOpcode
			var from sqlparser.SQLNode
			var to sqlparser.SQLNode
			switch construct := cursor.Parent().(type) {
			case *sqlparser.ComparisonExpr:
				from = cursor.Parent()
				var other sqlparser.Expr
				var operator sqlparser.ComparisonExprOperator
				switch construct.Operator {
				case sqlparser.InOp:
					// a in (subquery) -> (:__sq_has_values = 1 and (a in ::__sq))
					operator = sqlparser.InOp
					opCode = engine.PulloutIn
					other = sqlparser.NewIntLiteral([]byte("1"))
				case sqlparser.NotInOp:
					// a not in (subquery) -> (:__sq_has_values = 0 or (a not in ::__sq))
					operator = sqlparser.NotInOp
					opCode = engine.PulloutNotIn
					other = sqlparser.NewIntLiteral([]byte("0"))
				}
				to = &sqlparser.AndExpr{
					Left: &sqlparser.ComparisonExpr{
						Operator: sqlparser.EqualOp,
						Left:     sqlparser.NewArgument([]byte(hasValues(counter))),
						Right:    other,
					},
					Right: &sqlparser.ComparisonExpr{
						Operator: operator,
						Left:     construct.Left,
						Right:    sqlparser.NewArgument([]byte(sqName(counter))),
					},
				}

			case *sqlparser.ExistsExpr, *sqlparser.Where:
				opCode = engine.PulloutExists
				from = construct
				to = sqlparser.NewArgument([]byte(hasValues(counter)))
			default:
				opCode = engine.PulloutValue
				from = node
				to = sqlparser.NewArgument([]byte(sqName(counter)))
			}

			subqueries = append(subqueries, subq{
				sq:     node,
				from:   from,
				to:     to,
				opCode: opCode,
			})
		}
		return true
	}, nil)
	sqlparser.Rewrite(stmt, nil, func(cursor *sqlparser.Cursor) bool {
		for _, sq := range subqueries {
			if cursor.Node() == sq.from {
				cursor.Replace(sq.to)
			}
		}
		return true
	})
	return subqueries
}
