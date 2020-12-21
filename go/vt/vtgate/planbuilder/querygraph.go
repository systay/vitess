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
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	/*
			queryGraph represents the FROM and WHERE parts of a query.
		It is an intermediate representation of the query that makes it easier for the planner
		to find all possible join combinations. Instead of storing the query information in a form that is close
		to the syntax (AST), we extract the interesting parts into a graph form with the nodes being tables in the FROM
		clause and the edges between them being predicates. We keep predicates in a hash map keyed by the dependencies of
		the predicate. This makes it very fast to look up connections between tables in the query.
	*/
	queryGraph struct {
		// the tables, including predicates that only depend on this particular table
		tables []*queryTable

		// crossTable contains the predicates that need multiple tables
		crossTable map[semantics.TableSet][]sqlparser.Expr

		// noDeps contains the predicates that can be evaluated anywhere.
		noDeps sqlparser.Expr

		semTable *semantics.SemTable
	}

	// queryTable is a single FROM table, including all predicates particular to this table
	queryTable struct {
		tableSet   semantics.TableSet
		alias      *sqlparser.AliasedTableExpr
		table      sqlparser.TableName
		predicates []sqlparser.Expr

		vtable   *vindexes.Table
		vindex   vindexes.Vindex
		Keyspace *vindexes.Keyspace
		tabType  topodatapb.TabletType
		dest     key.Destination
	}
)

func createQGFromSelect(sel *sqlparser.Select, pb *primitiveBuilder) (*queryGraph, error) {
	semTable := pb.vschema.GetSemTable()
	qg := newQueryGraph(semTable)
	qg.collectTables(sel.From)
	if sel.Where != nil {
		err := qg.collectPredicates(sel)
		if err != nil {
			return nil, err
		}
	}
	return qg, nil
}

func annotateQGWithSchemaInfo(qg *queryGraph, vschema ContextVSchema) error {
	for _, t := range qg.tables {
		err := addVSchemaInfoToQueryTable(vschema, t)
		if err != nil {
			return err
		}
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

func newQueryGraph(semTable *semantics.SemTable) *queryGraph {
	return &queryGraph{
		crossTable: map[semantics.TableSet][]sqlparser.Expr{},
		semTable:   semTable,
	}
}

func (qg *queryGraph) collectTable(t sqlparser.TableExpr) {
	switch table := t.(type) {
	case *sqlparser.AliasedTableExpr:
		tableName := table.Expr.(sqlparser.TableName)
		qt := &queryTable{alias: table, table: tableName, tableSet: qg.semTable.TableSetFor(table)}
		qg.tables = append(qg.tables, qt)
	case *sqlparser.JoinTableExpr:
		qg.collectTable(table.LeftExpr)
		qg.collectTable(table.RightExpr)
		qg.collectPredicate(table.Condition.On)
	case *sqlparser.ParenTableExpr:
		qg.collectTables(table.Exprs)
	}
}
func (qg *queryGraph) collectTables(t sqlparser.TableExprs) {
	for _, expr := range t {
		qg.collectTable(expr)
	}
}

func (qg *queryGraph) collectPredicates(sel *sqlparser.Select) error {
	predicates := splitAndExpression(nil, sel.Where.Expr)

	for _, predicate := range predicates {
		err := qg.collectPredicate(predicate)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qg *queryGraph) collectPredicate(predicate sqlparser.Expr) error {
	deps := qg.semTable.Dependencies(predicate)
	switch deps.NumberOfTables() {
	case 0:
		qg.addNoDepsPredicate(predicate)
	case 1:
		found := qg.addToSingleTable(deps, predicate)
		if !found {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "table %v for predicate %v not found", deps, sqlparser.String(predicate))
		}
	default:
		allPredicates, found := qg.crossTable[deps]
		if found {
			allPredicates = append(allPredicates, predicate)
		} else {
			allPredicates = []sqlparser.Expr{predicate}
		}
		qg.crossTable[deps] = allPredicates
	}
	return nil
}

func (qg *queryGraph) addToSingleTable(table semantics.TableSet, predicate sqlparser.Expr) bool {
	for _, t := range qg.tables {
		if table == t.tableSet {
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

func (dpt dpTableT) bitSetsOfSize(wanted int) []joinTree {
	var result []joinTree
	for bs, jt := range dpt {
		size := bs.NumberOfTables()
		if size == wanted {
			result = append(result, jt)
		}
	}
	return result
}

func (qg *queryGraph) tryMerge(a, b joinTree, joinPredicates []sqlparser.Expr) joinTree {
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
	newTabletSet := aRoute.solved | bRoute.solved
	r := &routePlan{
		routeOpCode:          aRoute.routeOpCode,
		solved:               newTabletSet,
		tables:               append(aRoute.tables, bRoute.tables...),
		extraPredicates:      append(aRoute.extraPredicates, bRoute.extraPredicates...),
		keyspace:             aRoute.keyspace,
		vindexPlusPredicates: append(aRoute.vindexPlusPredicates, bRoute.vindexPlusPredicates...),
	}

	//joinPredicates := qg.crossTable[newTabletSet]
	//for _, joinPredicate := range joinPredicates {
	//	comparison, ok := joinPredicate.(*sqlparser.ComparisonExpr)
	//	if !ok || comparison.Operator != sqlparser.EqualOp {
	//		return nil
	//	}
	//	left, ok := comparison.Left.(*sqlparser.ColName)
	//	if !ok {
	//		return nil
	//	}

	//table := qg.getTable(qg.semTable.Dependencies(left)) // TODO make sure we have a bitset for a single table
	//fmt.Print(table)

	//}
	r.extraPredicates = append(r.extraPredicates, joinPredicates...)

	return nil
}
