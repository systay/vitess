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
	"strings"

	"vitess.io/vitess/go/sqltypes"

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

		semTable *semantics.SemTable
	}

	// queryTable is a single FROM table, including all predicates particular to this table
	queryTable struct {
		alias      *sqlparser.AliasedTableExpr
		table      sqlparser.TableName
		predicates []sqlparser.Expr

		vtable   *vindexes.Table
		vindex   vindexes.Vindex
		Keyspace *vindexes.Keyspace
		tabType  topodatapb.TabletType
		dest     key.Destination
		//routeOpCode engine.RouteOpcode
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

	tree := qg.solve()
	pb.plan, err = transformToLogicalPlan(tree)
	return err
}

func annotateQGWithSchemaInfo(qg *queryGraph, vschema ContextVSchema) error {
	for _, t := range qg.tables {
		err := addVSchemaInfoToQueryTable(vschema, t)
		if err != nil {
			return err
		}

		//switch t.routeOpCode {
		//For these opcodes, a new filter will not make any difference, so we can just exit early
		//case engine.SelectUnsharded, engine.SelectNext, engine.SelectDBA, engine.SelectReference, engine.SelectNone:
		//	continue
		//}

	}
	return nil
}
func addVSchemaInfoToQueryTable(vschema ContextVSchema, t *queryTable) error {
	vschemaTable, vindex, _, destTableType, destTarget, err := vschema.FindTableOrVindex(t.table)
	if err != nil {
		return vterrors.Wrapf(err, "failed to find information about %v", t.table)
	}
	t.dest = destTarget
	t.Keyspace = vschemaTable.Keyspace
	t.vtable = vschemaTable
	t.vindex = vindex
	t.tabType = destTableType
	return nil
}

func createQGFromSelect(sel *sqlparser.Select, pb *primitiveBuilder) (*queryGraph, error) {
	qg := &queryGraph{
		tables:     nil,
		crossTable: nil,
		semTable:   pb.vschema.GetSemTable(),
	}
	qg.collectTables(sel.From)
	if sel.Where != nil {
		err := qg.collectPredicates(sel, pb.vschema.GetSemTable())
		if err != nil {
			return nil, err
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
	bitSet uint8 // we can only join 8 tables with this limit

	joinTree interface {
		solves() bitSet
		cost() int
	}
	routePlan struct {
		routeOpCode     engine.RouteOpcode
		solved          bitSet
		tables          []*queryTable
		extraPredicates []sqlparser.Expr
		keyspace        *vindexes.Keyspace

		// vindex and conditions is set if an vindex will be used for this route.
		vindex     vindexes.Vindex
		conditions []sqlparser.Expr

		// this state keeps track of which vindexes are available and
		// whether we have seen enough predicates to satisfy the vindex
		vindexPlusPredicates []*vindexPlusPredicates
	}
	joinPlan struct {
		lhs, rhs joinTree
	}
	dpTableT map[bitSet]joinTree
)

func (rp *routePlan) solves() bitSet {
	return rp.solved
}
func (*routePlan) cost() int {
	return 1
}

// vindexPlusPredicates is a struct used to store all the predicates that the vindex can be used to query
type vindexPlusPredicates struct {
	vindex     *vindexes.ColumnVindex
	covered    bool
	predicates []sqlparser.Expr
}

func (rp *routePlan) addPredicate(predicates ...sqlparser.Expr) error {

	if len(rp.tables) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "addPredicate should only be called when the route has a single table")
	}

	if rp.vindexPlusPredicates == nil {
		// Add all the column vindexes to the list of vindexPlusPredicates
		for _, columnVindex := range rp.tables[0].vtable.ColumnVindexes {
			rp.vindexPlusPredicates = append(rp.vindexPlusPredicates, &vindexPlusPredicates{vindex: columnVindex})
		}
	}

	for _, filter := range predicates {
		switch node := filter.(type) {
		case *sqlparser.ComparisonExpr:
			switch node.Operator {
			case sqlparser.EqualOp:
				// TODO(Manan,Andres): Remove the predicates that are repeated eg. Id=1 AND Id=1
				for _, v := range rp.vindexPlusPredicates {
					column := node.Left.(*sqlparser.ColName)
					for _, col := range v.vindex.Columns {
						// If the column for the predicate matches any column in the vindex add it to the list
						if column.Name.Equal(col) {
							v.predicates = append(v.predicates, node)
							// Vindex is covered if all the columns in the vindex have a associated predicate
							v.covered = len(v.predicates) == len(v.vindex.Columns)
						}
					}
				}
			}
		}
	}

	//TODO (Manan,Andres): Improve cost metric for vindexes
	for _, v := range rp.vindexPlusPredicates {
		if !v.covered {
			continue
		}
		// Choose the minimum cost vindex from the ones which are covered
		if rp.vindex == nil || v.vindex.Vindex.Cost() < rp.vindex.Cost() {
			rp.vindex = v.vindex.Vindex
			rp.conditions = v.predicates
		}
	}

	if rp.vindex != nil {
		rp.routeOpCode = engine.SelectEqual
		if rp.vindex.IsUnique() {
			rp.routeOpCode = engine.SelectEqualUnique
		}
	}
	return nil
}

// Predicates takes all known predicates for this route and ANDs them together
func (rp *routePlan) Predicates() sqlparser.Expr {
	var result sqlparser.Expr
	add := func(e sqlparser.Expr) {
		if result == nil {
			result = e
			return
		}
		result = &sqlparser.AndExpr{
			Left:  result,
			Right: e,
		}
	}
	for _, t := range rp.tables {
		for _, predicate := range t.predicates {
			add(predicate)
		}
	}
	for _, p := range rp.extraPredicates {
		add(p)
	}
	return result
}

func (jp *joinPlan) solves() bitSet {
	return jp.lhs.solves() | jp.rhs.solves()
}
func (jp *joinPlan) cost() int {
	return jp.lhs.cost() + jp.rhs.cost()
}

func isOverlapping(a, b bitSet) bool { return a&b != 0 }

func (dpt dpTableT) bitSetsOfSize(wanted int) []joinTree {
	var result []joinTree
	for bs, jt := range dpt {
		size := countSetBits(bs)
		if size == wanted {
			result = append(result, jt)
		}
	}
	return result
}
func countSetBits(n bitSet) int {
	// Brian Kernighan’s Algorithm
	count := 0
	for n > 0 {
		n &= n - 1
		count++
	}
	return count
}

/*
	we use dynamic programming to find the cheapest route/join tree possible,
	where the cost of a plan is the number of joins
*/
func (qg *queryGraph) solve() joinTree {
	size := len(qg.tables)
	dpTable := make(dpTableT)

	var allTables bitSet

	// we start by seeding the table with the single routes
	for i, table := range qg.tables {
		solves := bitSet(1 << i)
		allTables |= solves
		dpTable[solves], _ = createRoutePlan(table, solves)
	}

	for currentSize := 2; currentSize <= size; currentSize++ {
		lefts := dpTable.bitSetsOfSize(1)
		rights := dpTable.bitSetsOfSize(currentSize - 1)
		for _, lhs := range lefts {
			for _, rhs := range rights {
				if isOverlapping(lhs.solves(), rhs.solves()) {
					// at least one of the tables is solved on both sides
					continue
				}
				solves := lhs.solves() | rhs.solves()
				oldPlan := dpTable[solves]
				if oldPlan != nil && oldPlan.cost() == 1 {
					// we already have the perfect plan. keep it
					continue
				}
				newPlan := qg.tryMerge(lhs, rhs)
				if newPlan == nil {
					newPlan = &joinPlan{
						lhs: lhs,
						rhs: rhs,
					}
				}
				if oldPlan == nil || newPlan.cost() < oldPlan.cost() {
					dpTable[solves] = newPlan
				}
			}
		}
	}

	return dpTable[allTables]
}

func createRoutePlan(table *queryTable, solves bitSet) (*routePlan, error) {
	vschemaTable := table.vtable

	plan := &routePlan{
		solved:   solves,
		tables:   []*queryTable{table},
		keyspace: table.Keyspace,
	}

	switch {
	case vschemaTable.Type == vindexes.TypeSequence:
		plan.routeOpCode = engine.SelectNext
	case vschemaTable.Type == vindexes.TypeReference:
		plan.routeOpCode = engine.SelectReference
	case !vschemaTable.Keyspace.Sharded:
		plan.routeOpCode = engine.SelectUnsharded
	case vschemaTable.Pinned != nil:

		// Pinned tables have their keyspace ids already assigned.
		// Use the Binary vindex, which is the identity function
		// for keyspace id.
		plan.routeOpCode = engine.SelectEqualUnique
		//eroute.TargetDestination = destTarget
		//eroute.TargetTabletType = destTableType
		//vindex, _ = vindexes.NewBinary("binary", nil)
		//eroute.Vindex, _ = vindex.(vindexes.SingleColumn)
		//eroute.Values = []sqltypes.PlanValue{{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, vschemaTable.Pinned)}}
	default:
		plan.routeOpCode = engine.SelectScatter
		err := plan.addPredicate(table.predicates...)
		if err != nil {
			return nil, err
		}
	}

	return plan, nil
}

func (qg *queryGraph) tryMerge(a, b joinTree) joinTree {
	aRoute, ok := a.(*routePlan)
	if !ok {
		return nil
	}
	bRoute, ok := b.(*routePlan)
	if !ok {
		return nil
	}
	if aRoute.keyspace != bRoute.keyspace {
		return nil
	}

	switch aRoute.routeOpCode {
	case engine.SelectUnsharded, engine.SelectDBA:
		if aRoute.routeOpCode != bRoute.routeOpCode {
			return nil
		}
	case engine.SelectEqualUnique:

		return nil

		//	Check if they target the same shard.
		//if bRoute.routeOpCode == engine.SelectEqualUnique &&
		//	bRoute.Vindex == bRoute.routeOpCode && valEqual(rb.condition, rrb.condition) {
		//	return true
		//}
		//case engine.SelectReference:
		//	return true
		//case engine.SelectNext:
		//	return false
	}
	//aRoute.tables = append(aRoute.tables, bRoute.tables...)
	//aRoute.solved |= bRoute.solved
	//aRoute.extraPredicates = append(aRoute.extraPredicates, bRoute.extraPredicates...)
	r := &routePlan{
		routeOpCode:          aRoute.routeOpCode,
		solved:               aRoute.solved | bRoute.solved,
		tables:               append(aRoute.tables, bRoute.tables...),
		extraPredicates:      append(aRoute.extraPredicates, bRoute.extraPredicates...),
		keyspace:             aRoute.keyspace,
		vindexPlusPredicates: append(aRoute.vindexPlusPredicates, bRoute.vindexPlusPredicates...),
	}

	return r
}

func transformToLogicalPlan(tree joinTree) (logicalPlan, error) {
	switch n := tree.(type) {
	case *routePlan:
		var tablesForSelect sqlparser.TableExprs
		var tablesForRoute []*sqlparser.AliasedTableExpr
		var tableNames []string

		for _, t := range n.tables {
			tablesForSelect = append(tablesForSelect, t.alias)
			tablesForRoute = append(tablesForRoute, t.alias)
			tableNames = append(tableNames, sqlparser.String(t.alias))
		}
		predicates := n.Predicates()
		var where *sqlparser.Where
		if predicates != nil {
			where = &sqlparser.Where{Expr: predicates, Type: sqlparser.WhereClause}
		}
		var values []sqltypes.PlanValue
		if len(n.conditions) == 1 {
			value, err := sqlparser.NewPlanValue(n.conditions[0].(*sqlparser.ComparisonExpr).Right)
			if err != nil {
				return nil, err
			}
			values = []sqltypes.PlanValue{value}
		}
		var singleColumn vindexes.SingleColumn
		if n.vindex != nil {
			singleColumn = n.vindex.(vindexes.SingleColumn)
		}
		return &route{
			eroute: &engine.Route{
				Opcode:    n.routeOpCode,
				TableName: strings.Join(tableNames, ", "),
				Keyspace:  n.keyspace,
				Vindex:    singleColumn,
				Values:    values,
			},
			Select: &sqlparser.Select{
				From:  tablesForSelect,
				Where: where,
			},
			tables: tablesForRoute,
		}, nil

	case *joinPlan:
		lhs, err := transformToLogicalPlan(n.lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := transformToLogicalPlan(n.rhs)
		if err != nil {
			return nil, err
		}
		return &join{
			ejoin: &engine.Join{
				Opcode: engine.NormalJoin,
			},
			Left:  lhs,
			Right: rhs,
		}, nil
	}
	panic(42)
}
