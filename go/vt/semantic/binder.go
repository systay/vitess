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

package semantic

import (
	"fmt"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// table is an internal struct used while binding
	table struct {
		name, qualifier, alias string
		ID                     int
	}

	// Table is the information known about which table this column belongs to
	Table struct {
		ID              int
		Name, Qualifier string
		Local           bool
	}
)

func (t table) hasAlias() bool {
	return t.alias != ""
}

type identifier interface {
	next() int
}

//DoBinding annotates the provided scope with table info, or writes table info to the ColNames
func DoBinding(s *scope, node sqlparser.SQLNode, id identifier) error {
	switch n := node.(type) {
	case *sqlparser.AliasedTableExpr:
		switch t := n.Expr.(type) {
		case sqlparser.TableName:
			newTable := &table{
				name:      t.Name.String(),
				qualifier: t.Qualifier.String(),
				alias:     n.As.String(),
				ID:        id.next(),
			}
			s.tableExprs = append(s.tableExprs, newTable)
			n.Metadata = newTable.ID
		}
	case *sqlparser.ColName:
		table, local := s.FindTable(n.Qualifier.Qualifier.String(), n.Qualifier.Name.String())
		if table == nil {
			return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "ERROR 1054 (42S22): Unknown column '%s' in 'field list'", sqlparser.String(n))
		}
		n.Metadata = Table{
			Name:      table.name,
			Qualifier: table.qualifier,
			Local:     local,
			ID:        table.ID,
		}
	}
	return nil
}

func DepencenciesFor(expr sqlparser.Expr) map[int]interface{} {
	result := map[int]interface{}{}
	sqlparser.Rewrite(expr, func(cursor *sqlparser.Cursor) bool {
		switch col := cursor.Node().(type) {
		case *sqlparser.ColName:
			result[col.Metadata.(Table).ID] = nil
		}
		return true
	}, nil)
	return result
}

func (t Table) ToString() string {
	var source, local string
	if t.Qualifier == "" {
		source = t.Name
	} else {
		source = t.Qualifier + "." + t.Name
	}
	if t.Local {
		local = "L"
	} else {
		local = "NL"
	}
	return fmt.Sprintf("%s%d(%s)", source, t.ID, local)
}
