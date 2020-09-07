package planbuilder2

import (
	"vitess.io/vitess/go/vt/semantic"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	tableSet struct {
		s map[int]interface{}
	}

	logicalPlan interface {
		Primitive() engine.Primitive
		Covers() *tableSet
		Columns() []sqlparser.SelectExpr
		Inputs() []logicalPlan
		Rewrite1(logicalPlan)
	}

	route struct {
		opcode   engine.RouteOpcode
		keyspace *vindexes.Keyspace
		query    *sqlparser.Select
		covers   *tableSet
	}

	filter struct {
		input     logicalPlan
		predicate sqlparser.Expr
	}

	project struct {
		input   logicalPlan
		columns []sqlparser.SelectExpr
	}
)

var _ logicalPlan = (*route)(nil)
var _ logicalPlan = (*filter)(nil)
var _ logicalPlan = (*project)(nil)

func (f *filter) Inputs() []logicalPlan {
	return []logicalPlan{f.input}
}

func (p *project) Inputs() []logicalPlan {
	return []logicalPlan{p.input}
}

func (r *route) Inputs() []logicalPlan {
	return []logicalPlan{}
}

func (f *filter) Covers() *tableSet {
	return f.input.Covers()
}

func (p *project) Covers() *tableSet {
	panic("implement me")
}

func (r *route) Covers() *tableSet {
	return r.covers
}

func (f *filter) Columns() []sqlparser.SelectExpr {
	return f.input.Columns()
}

func (p *project) Columns() []sqlparser.SelectExpr {
	panic("implement me")
}

func (r *route) Columns() []sqlparser.SelectExpr {
	return r.query.SelectExprs
}

func (f *filter) Primitive() engine.Primitive {
	panic("should have been optimized away")
}

func (p *project) Primitive() engine.Primitive {

	var cols []string
	var exprs []evalengine.Expr

	for _, proj := range p.columns {
		switch e := proj.(type) {
		case *sqlparser.AliasedExpr:
			var name string
			if e.As.IsEmpty() {
				name = sqlparser.String(e.Expr)
			} else {
				name = e.As.String()
			}
			cols = append(cols, name)
		}
	}

	return &engine.Projection{
		Cols:  cols,
		Exprs: exprs,
		Input: p.input.Primitive(),
	}
}

func (r *route) Primitive() engine.Primitive {
	fullQuery := sqlparser.String(r.query)
	fieldQuery := *r.query
	cmp := &sqlparser.ComparisonExpr{
		Operator: sqlparser.NotEqualStr,
		Left:     sqlparser.NewIntLiteral([]byte("1")),
		Right:    sqlparser.NewIntLiteral([]byte("1")),
	}
	fieldQuery.Where = sqlparser.NewWhere(sqlparser.WhereStr, cmp)

	route := engine.NewRoute(r.opcode, r.keyspace, fullQuery, sqlparser.String(&fieldQuery))
	route.TableName = "unsharded"
	return route
}

func (r *route) Rewrite1(in logicalPlan) {
	panic(42)
}
func (f *filter) Rewrite1(in logicalPlan) {
	f.input = in
}
func (p *project) Rewrite1(in logicalPlan) {
	p.input = in
}

func NewSetWith(id int) *tableSet {
	return &tableSet{
		s: map[int]interface{}{id: nil},
	}
}
func NewSetFor(expr sqlparser.Expr) *tableSet {
	return &tableSet{s: semantic.DepencenciesFor(expr)}
}

func (ts *tableSet) Add(id int) {
	ts.s[id] = nil
}

func (ts *tableSet) CoveredBy(other *tableSet) bool {
	for k := range ts.s {
		_, ok := other.s[k]
		if !ok {
			return false
		}
	}
	return true
}
