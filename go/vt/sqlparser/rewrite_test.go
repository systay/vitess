package sqlparser

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFoldConstants(t *testing.T) {
	ast, err := Parse("select 1 + 2")
	require.NoError(t, err)
	rewritten := Apply(ast, pre, post)
	assert.Equal(t, "select 3", String(rewritten))
}

func pre(c *Cursor) bool {
	switch n := c.node.(type) {
	case nil:
	// nothing to do
	case *BinaryExpr:
		lft, lok := n.Left.(*SQLVal)
		rgt, rok := n.Right.(*SQLVal)

		if lok && rok {
			lval, _ := strconv.Atoi(string(lft.Val))
			rval, _ := strconv.Atoi(string(rgt.Val))
			replaceWith := &SQLVal{
				Type: IntVal,
				Val:  []byte(strconv.FormatInt(int64(lval+rval), 10)),
			}
			c.Replace(replaceWith)
			return false
		}

	}
	fmt.Print(fmt.Sprintf("before: '%s' string: %s\n", c.name, String(c.node)))
	return true
}

func post(c *Cursor) bool {
	fmt.Print((fmt.Sprintf("after : '%s' string: %s\n", c.name, String(c.node))))
	return true
}
