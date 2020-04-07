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

package engine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

func Evaluate(expr sqlparser.Expr, bindVars map[string]*querypb.BindVariable, row []sqltypes.Value) sqltypes.Value {
	switch node := expr.(type) {
	case *sqlparser.SQLVal:
		switch node.Type {
		case sqlparser.StrVal:
			return sqltypes.MakeTrusted(querypb.Type_VARCHAR, node.Val)
		}
	}
	return sqltypes.Value{}
}
