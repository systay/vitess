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
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type (
	// queryGraph represents the FROM and WHERE parts of a query. 
	// the predicates included all have their dependencies met by the tables in the QG  
	queryGraph struct {
		tables []*queryTable

		// crossTable contains the predicates that need multiple tables
		crossTable []sqlparser.Expr

		// noDeps contains the predicates that can be evaluated anywhere
		noDeps sqlparser.Expr
	}

	// queryTable is a single FROM table, including all predicates particular to this table 
	queryTable struct {
		alias      *sqlparser.AliasedTableExpr
		table      sqlparser.TableName
		predicates []sqlparser.Expr

		vtable       *vindexes.Table
		vindex       vindexes.Vindex
		keyspaceName string
		tabType      topodatapb.TabletType
		dest         key.Destination
		routeOpCode  engine.RouteOpcode
	}
)

func (qg *queryGraph) collectTable(t sqlparser.TableExpr) {
	switch table := t.(type) {
	case *sqlparser.AliasedTableExpr:
		tableName := table.Expr.(sqlparser.TableName)
		qt := &queryTable{alias: table, table: tableName}
		qg.tables = append(qg.tables, qt)
	case *sqlparser.JoinTableExpr:
		qg.collectTable(table.LeftExpr)
		qg.collectTable(table.RightExpr)
		qg.crossTable = append(qg.crossTable, table.Condition.On)
	case *sqlparser.ParenTableExpr:
		qg.collectTables(table.Exprs)
	}
}
func (qg *queryGraph) collectTables(t sqlparser.TableExprs) {
	for _, expr := range t {
		qg.collectTable(expr)
	}
}
func (pb *primitiveBuilder) planJoinTree(sel *sqlparser.Select) error {
	qg, err := createQGFromSelect(sel, pb)
	if err != nil {
		return err
	}
	err = annotateQGWithSchemaInfo(qg, pb.vschema)
	if err != nil {
		return err
	}

	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "implement me!")
}

func annotateQGWithSchemaInfo(qg queryGraph, vschema ContextVSchema) error {
	for _, t := range qg.tables {
		vschemaTable, vindex, keyspace, destTableType, destTarget, err := vschema.FindTableOrVindex(t.table)
		if err != nil {
			return vterrors.Wrapf(err, "failed to find information about %v", t.table)
		}
		t.dest = destTarget
		t.keyspaceName = keyspace
		t.vtable = vschemaTable
		t.vindex = vindex
		t.tabType = destTableType
		switch {
		case vschemaTable.Type == vindexes.TypeSequence:
			t.routeOpCode = engine.SelectNext
		case vschemaTable.Type == vindexes.TypeReference:
			t.routeOpCode = engine.SelectReference
		case !vschemaTable.Keyspace.Sharded:
			t.routeOpCode = engine.SelectUnsharded
		case vschemaTable.Pinned == nil:
			t.routeOpCode = engine.SelectScatter
			//eroute.TargetDestination = destTarget
			//eroute.TargetTabletType = destTableType
		default:
			// Pinned tables have their keyspace ids already assigned.
			// Use the Binary vindex, which is the identity function
			// for keyspace id.
			t.routeOpCode = engine.SelectEqualUnique
			//vindex, _ = vindexes.NewBinary("binary", nil)
			//eroute.Vindex, _ = vindex.(vindexes.SingleColumn)
			//eroute.Values = []sqltypes.PlanValue{{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, vschemaTable.Pinned)}}
		}

	}
	return nil
}

func createQGFromSelect(sel *sqlparser.Select, pb *primitiveBuilder) (queryGraph, error) {
	qg := queryGraph{
		tables:     nil,
		crossTable: nil,
	}
	qg.collectTables(sel.From)
	if sel.Where != nil {
		err := qg.collectPredicates(sel, pb.vschema.GetSemTable())
		if err != nil {
			return queryGraph{}, err
		}
	}
	return qg, nil
}

func (qg *queryGraph) collectPredicates(sel *sqlparser.Select, semTable *semantics.SemTable) error {
	predicates := splitAndExpression(nil, sel.Where.Expr)

	for _, predicate := range predicates {
		deps := semTable.Dependencies(predicate)
		switch len(deps) {
		case 0:
			qg.addNoDepsPredicate(predicate)
		case 1:
			found := qg.addToSingleTable(deps[0], predicate)
			if !found {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "table %s for predicate %s not found", sqlparser.String(deps[0]), sqlparser.String(predicate))
			}
		default:
			qg.crossTable = append(qg.crossTable, predicate)
		}
	}
	return nil
}

func (qg *queryGraph) addToSingleTable(depencency *sqlparser.AliasedTableExpr, predicate sqlparser.Expr) bool {
	for _, t := range qg.tables {
		if depencency == t.alias {
			t.predicates = append(t.predicates, predicate)
			return true
		}
	}
	return false
}

func (qg *queryGraph) addNoDepsPredicate(predicate sqlparser.Expr) {
	if qg.noDeps == nil {
		qg.noDeps = predicate
	} else {
		qg.noDeps = &sqlparser.AndExpr{
			Left:  qg.noDeps,
			Right: predicate,
		}
	}
}

func (pb *primitiveBuilder) processSelect2(sel *sqlparser.Select) error {
	err := pb.planJoinTree(sel)
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
				Opcode:         sq.opCode,
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

type (
	tableIndex uint8 // we can only join 8 tables with this limit

	joinTree interface {
		iPlan()
	}
	routePlan struct {
		routeOpCode     engine.RouteOpcode
		solves          tableIndex
		tables          []*queryTable
		extraPredicates []sqlparser.Expr
	}
	joinPlan struct {
		solves tableIndex
		a, b   joinTree
	}
)

func (*routePlan) iPlan() {}
func (*joinPlan) iPlan()  {}

/*
	we use dynamic programming to find the cheapest route/join tree possible,
	where the cost of a plan is the number of joins
*/
func (qg *queryGraph) solve() {
	size := len(qg.tables)
	dpTable := make(map[tableIndex]joinTree)
	// we start by seeding the table with the single routes
	for i, table := range qg.tables {
		solves := tableIndex(1 << i)
		dpTable[solves] = &routePlan{
			routeOpCode:     table.routeOpCode,
			solves:          solves,
			tables:          []*queryTable{table},
			extraPredicates: nil,
		}
	}

	for currentSize := 2; currentSize < size; currentSize++ {
		for lhsTable := 1; lhsTable < currentSize; lhsTable++ {
			lhs := dpTable[1<<lhsTable]
			for rhsTable := 1; lhsTable < currentSize; lhsTable++ {
				if lhs&rhsTable!=0{
					
				}
				lhs := dpTable[1<<lhsTable]

			}

		}
	}
}