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

package sqlparser

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParens(t *testing.T) {
	tests := []struct {
		in, expected string
	}{
		{in: "12", expected: "12"},
		{in: "(12)", expected: "12"},
		{in: "((12))", expected: "12"},
		{in: "((true) and (false))", expected: "true and false"},
		{in: "((true) and (false))", expected: "true and false"},
		{in: "a=b and (c=d or e=f)", expected: "a = b and (c = d or e = f)"},
		{in: "(a=b and c=d) or e=f", expected: "a = b and c = d or e = f"},
		{in: "-(12)", expected: "-12"},
		{in: "-(12 + 12)", expected: "-(12 + 12)"},
		{in: "(1 > 2) and (1 = b)", expected: "1 > 2 and 1 = b"},
		{in: "(a / b) + c", expected: "a / b + c"},
		{in: "a / (b + c)", expected: "a / (b + c)"},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			stmt, err := Parse("select " + tc.in)
			require.NoError(t, err)
			out := String(Parenthesize(stmt))
			require.Equal(t, "select "+tc.expected+" from dual", out)
		})
	}
}

func TestParens3(t *testing.T) {
	expr := func() SQLNode {
		return &AliasedExpr{
			Expr: &ParenExpr{&SQLVal{
				Type: IntVal,
				Val:  []byte("10"),
			}}}
	}

	assert.Equal(t, expr(), Parenthesize(expr()))


}

func colName(name string) *ColName {
	return &ColName{Name: NewColIdent(name)}
}
