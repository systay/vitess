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

		vtable      *vindexes.Table
		vindex      vindexes.Vindex
		Keyspace    *vindexes.Keyspace
		tabType     topodatapb.TabletType
		dest        key.Destination
		routeOpCode engine.RouteOpcode
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
	pb.plan = transformToLogicalPlan(tree)
	return nil
}

func annotateQGWithSchemaInfo(qg *queryGraph, vschema ContextVSchema) error {
	for _, t := range qg.tables {
		err := addVSchemaInfoToQueryTable(vschema, t)
		if err != nil {
			return err
		}

		switch t.routeOpCode {
		// For these opcodes, a new filter will not make any difference, so we can just exit early
		case engine.SelectUnsharded, engine.SelectNext, engine.SelectDBA, engine.SelectReference, engine.SelectNone:
			continue
		}

	}
	return nil
}

// computePlan computes the plan for the specified filter.
func (qt *queryTable) addPredicate(predicates []sqlparser.Expr) (opcode engine.RouteOpcode, vindex vindexes.Vindex, conditions []sqlparser.Expr) {

	// VindexPlusPredicates is a struct used to store all the predicates that the vindex can be used to query
	type VindexPlusPredicates struct {
		vindex     *vindexes.ColumnVindex
		covered    bool
		predicates []sqlparser.Expr
	}

	var vindexPlusPredicates []*VindexPlusPredicates

	// Add all the column vindexes to the list of vindexPlusPredicates
	for _, columnVindex := range qt.vtable.ColumnVindexes {
		vindexPlusPredicates = append(vindexPlusPredicates, &VindexPlusPredicates{vindex: columnVindex})
	}

	for _, filter := range predicates {
		switch node := filter.(type) {
		case *sqlparser.ComparisonExpr:
			switch node.Operator {
			case sqlparser.EqualOp:
				// TODO(Manan,Andres): Remove the predicates that are repeated eg. Id=1 AND Id=1
				for _, v := range vindexPlusPredicates {
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
				//return qt.computeEqualPlan(node)
				//case sqlparser.InOp:
				//	return rb.computeINPlan(pb, node)
				//case sqlparser.NotInOp:
				//	return rb.computeNotInPlan(node.Right), nil, nil
			}
			//case *sqlparser.IsExpr:
			//	return rb.computeISPlan(pb, node)
		}
	}
	vindex = nil
	conditions = nil
	//TODO (Manan,Andres): Improve cost metric for vindexes
	for _, v := range vindexPlusPredicates {
		if !v.covered {
			continue
		}
		// Choose the minimum cost vindex from the ones which are covered
		if vindex == nil || v.vindex.Vindex.Cost() < vindex.Cost() {
			vindex = v.vindex.Vindex
			conditions = v.predicates
		}
	}

	opcode = engine.SelectScatter
	if vindex != nil {
		opcode = engine.SelectEqual
		if vindex.IsUnique() {
			opcode = engine.SelectEqualUnique
		}
	}
	return
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
	switch {
	case vschemaTable.Type == vindexes.TypeSequence:
		t.routeOpCode = engine.SelectNext
	case vschemaTable.Type == vindexes.TypeReference:
		t.routeOpCode = engine.SelectReference
	case !vschemaTable.Keyspace.Sharded:
		t.routeOpCode = engine.SelectUnsharded
	case vschemaTable.Pinned != nil:
		// Pinned tables have their keyspace ids already assigned.
		// Use the Binary vindex, which is the identity function
		// for keyspace id.
		t.routeOpCode = engine.SelectEqualUnique
		//eroute.TargetDestination = destTarget
		//eroute.TargetTabletType = destTableType
		//vindex, _ = vindexes.NewBinary("binary", nil)
		//eroute.Vindex, _ = vindex.(vindexes.SingleColumn)
		//eroute.Values = []sqltypes.PlanValue{{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, vschemaTable.Pinned)}}
	default:
		var vindexUsed vindexes.Vindex
		var predicatesUsed []sqlparser.Expr
		t.routeOpCode, vindexUsed, predicatesUsed = t.addPredicate(t.predicates)
		fmt.Printf("%v", vindexUsed)
		fmt.Printf("%v", predicatesUsed)

	}
	return nil
}

func createQGFromSelect(sel *sqlparser.Select, pb *primitiveBuilder) (*queryGraph, error) {
	qg := &queryGraph{
		tables:     nil,
		crossTable: nil,
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
		dpTable[solves] = createRoutePlan(table, solves)
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
				newPlan := tryMerge(lhs, rhs)
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

func createRoutePlan(table *queryTable, solves bitSet) *routePlan {
	return &routePlan{
		routeOpCode: table.routeOpCode,
		solved:      solves,
		tables:      []*queryTable{table},
		keyspace:    table.Keyspace,
	}
}

func tryMerge(a, b joinTree) joinTree {
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
	case engine.SelectScatter:

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
	return &routePlan{
		routeOpCode:     aRoute.routeOpCode,
		solved:          aRoute.solved | bRoute.solved,
		tables:          append(aRoute.tables, bRoute.tables...),
		extraPredicates: append(aRoute.extraPredicates, bRoute.extraPredicates...),
		keyspace:        aRoute.keyspace,
	}
}

func transformToLogicalPlan(tree joinTree) logicalPlan {
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
		return &route{
			eroute: &engine.Route{
				Opcode:    n.routeOpCode,
				TableName: strings.Join(tableNames, ", "),
				Keyspace:  n.keyspace,
			},
			Select: &sqlparser.Select{
				From: tablesForSelect,
			},
			tables: tablesForRoute,
		}

	case *joinPlan:
		return &join{
			ejoin: &engine.Join{
				Opcode: engine.NormalJoin,
			},
			Left:  transformToLogicalPlan(n.lhs),
			Right: transformToLogicalPlan(n.rhs),
		}
	}
	panic(42)
}
