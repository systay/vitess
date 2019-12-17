package sqlparser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizer(t *testing.T) {
	ast, err := Parse("select 1 + 2")
	require.NoError(t, err)
	rewritten := Apply(ast, pre, post)
	fmt.Println(String(rewritten))
}

func pre(c *Cursor) bool {
	fmt.Print(fmt.Sprintf("before: '%s' string: %s\n", c.name, String(c.node)))
	return true
}

func post(c *Cursor) bool {
	fmt.Print((fmt.Sprintf("after : '%s' string: %s\n", c.name, String(c.node))))
	return true
}
