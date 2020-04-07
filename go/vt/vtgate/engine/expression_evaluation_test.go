/*
Copyright 2019 The Vitess Authors.

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
	"github.com/stretchr/testify/assert"
	"testing"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestEvalWithNoInputRows(t *testing.T) {
	type testCase struct {
		e        sqlparser.Expr
		bindvars map[string]*querypb.BindVariable
		expected sqltypes.Value
	}

	tests := []testCase{
		{
			e:        sqlparser.NewStrVal([]byte("hello")),
			expected: sqltypes.NewVarChar("hello"),
		},
		{
			e:        sqlparser.NewIntVal([]byte(42)),
			expected: sqltypes.NewVarChar("hello"),
		},
	}

	for _, test := range tests {
		result := Evaluate(test.e, test.bindvars, []sqltypes.Value{})
		assert.Equal(t, test.expected, result, "expected %s but got %s", test.expected.String(), result.String())
	}
}
