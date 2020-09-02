package semantic

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestDependencies(t *testing.T) {
	type testCase struct {
		name     string
		query    string
		col1Deps string
	}
	tests := []testCase{{
		name:     "simplest case",
		query:    "select col from x",
		col1Deps: "x(L)",
	}, {
		name:     "simple with alias",
		query:    "select col from x as alias",
		col1Deps: "x AS alias(L)",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)
			ast, err := sqlparser.Parse(tc.query)
			require.NoError(err)
			_, err = Analyse(ast)
			require.NoError(err)

			// get the first expression of the query as a ColName
			expr := ast.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr

			deps := DepencenciesFor(expr)
			var result []string
			for k := range deps {
				result = append(result, k.ToString())
			}

			assert.Equal(tc.col1Deps, strings.Join(result, ":"))
		})
	}
}
