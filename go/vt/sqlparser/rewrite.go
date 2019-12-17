// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"fmt"
	"reflect"
)

// An ApplyFunc is invoked by Apply for each node n, even if n is nil,
// before and/or after the node's children, using a Cursor describing
// the current node and providing operations on it.
//
// The return value of ApplyFunc controls the syntax tree traversal.
// See Apply for details.
type ApplyFunc func(*Cursor) bool

// Apply traverses a syntax tree recursively, starting with root,
// and calling pre and post for each node as described below.
// Apply returns the syntax tree, possibly modified.
//
// If pre is not nil, it is called for each node before the node's
// children are traversed (pre-order). If pre returns false, no
// children are traversed, and post is not called for that node.
//
// If post is not nil, and a prior call of pre didn't return false,
// post is called for each node after its children are traversed
// (post-order). If post returns false, traversal is terminated and
// Apply returns immediately.
//
// Only fields that refer to AST nodes are considered children;
// i.e., token.Pos, Scopes, Objects, and fields of basic types
// (strings, etc.) are ignored.
//
// Children are traversed in the order in which they appear in the
// respective node's struct definition. A package's files are
// traversed in the filenames' alphabetical order.
//
func Apply(root SQLNode, pre, post ApplyFunc) (result SQLNode) {
	parent := &struct{ SQLNode }{root}
	defer func() {
		if r := recover(); r != nil && r != abort {
			panic(r)
		}
		result = parent.SQLNode
	}()
	a := &application{pre: pre, post: post}
	a.apply(parent, "root", nil, root)
	return
}

var abort = new(int) // singleton, to signal termination of Apply

// A Cursor describes a node encountered during Apply.
// Information about the node and its parent is available
// from the Node, Parent, Name, and Index methods.
//
// If p is a variable of type and value of the current parent node
// c.Parent(), and f is the field identifier with name c.Name(),
// the following invariants hold:
//
//   p.f            == c.Node()  if c.Index() <  0
//   p.f[c.Index()] == c.Node()  if c.Index() >= 0
//
// The methods Replace, Delete, InsertBefore, and InsertAfter
// can be used to change the AST without disrupting Apply.
type Cursor struct {
	parent SQLNode
	name   string
	iter   *iterator // valid if non-nil
	node   SQLNode
}

// Node returns the current Node.
func (c *Cursor) Node() SQLNode { return c.node }

// Parent returns the parent of the current Node.
func (c *Cursor) Parent() SQLNode { return c.parent }

// Name returns the name of the parent Node field that contains the current Node.
// If the parent is a *ast.Package and the current Node is a *ast.File, Name returns
// the filename for the current Node.
func (c *Cursor) Name() string { return c.name }

// Index reports the index >= 0 of the current Node in the slice of Nodes that
// contains it, or a value < 0 if the current Node is not part of a slice.
// The index of the current node changes if InsertBefore is called while
// processing the current node.
func (c *Cursor) Index() int {
	if c.iter != nil {
		return c.iter.index
	}
	return -1
}

// field returns the current node's parent field value.
func (c *Cursor) field() reflect.Value {
	return reflect.Indirect(reflect.ValueOf(c.parent)).FieldByName(c.name)
}

// Replace replaces the current Node with n.
// The replacement node is not walked by Apply.
func (c *Cursor) Replace(n SQLNode) {
	v := c.field()
	if i := c.Index(); i >= 0 {
		v = v.Index(i)
	}
	v.Set(reflect.ValueOf(n))
}

// Delete deletes the current Node from its containing slice.
// If the current Node is not part of a slice, Delete panics.
// As a special case, if the current node is a package file,
// Delete removes it from the package's Files map.
func (c *Cursor) Delete() {
	i := c.Index()
	if i < 0 {
		panic("Delete node not contained in slice")
	}
	v := c.field()
	l := v.Len()
	reflect.Copy(v.Slice(i, l), v.Slice(i+1, l))
	v.Index(l - 1).Set(reflect.Zero(v.Type().Elem()))
	v.SetLen(l - 1)
	c.iter.step--
}

// InsertAfter inserts n after the current Node in its containing slice.
// If the current Node is not part of a slice, InsertAfter panics.
// Apply does not walk n.
func (c *Cursor) InsertAfter(n SQLNode) {
	i := c.Index()
	if i < 0 {
		panic("InsertAfter node not contained in slice")
	}
	v := c.field()
	v.Set(reflect.Append(v, reflect.Zero(v.Type().Elem())))
	l := v.Len()
	reflect.Copy(v.Slice(i+2, l), v.Slice(i+1, l))
	v.Index(i + 1).Set(reflect.ValueOf(n))
	c.iter.step++
}

// InsertBefore inserts n before the current Node in its containing slice.
// If the current Node is not part of a slice, InsertBefore panics.
// Apply will not walk n.
func (c *Cursor) InsertBefore(n SQLNode) {
	i := c.Index()
	if i < 0 {
		panic("InsertBefore node not contained in slice")
	}
	v := c.field()
	v.Set(reflect.Append(v, reflect.Zero(v.Type().Elem())))
	l := v.Len()
	reflect.Copy(v.Slice(i+1, l), v.Slice(i, l))
	v.Index(i).Set(reflect.ValueOf(n))
	c.iter.index++
}

// application carries all the shared data so we can pass it around cheaply.
type application struct {
	pre, post ApplyFunc
	cursor    Cursor
	iter      iterator
}

func (a *application) apply(parent SQLNode, name string, iter *iterator, n SQLNode) {
	// convert typed nil into untyped nil
	if v := reflect.ValueOf(n); v.Kind() == reflect.Ptr && v.IsNil() {
		n = nil
	}

	// avoid heap-allocating a new cursor for each apply call; reuse a.cursor instead
	saved := a.cursor
	a.cursor.parent = parent
	a.cursor.name = name
	a.cursor.iter = iter
	a.cursor.node = n

	if a.pre != nil && !a.pre(&a.cursor) {
		a.cursor = saved
		return
	}

	// walk children
	// (the order of the cases matches the order of the corresponding node types in ast.go)
	switch n := n.(type) {
	case nil:
	// nothing to do
	case SelectExprs, *SQLVal, ColIdent, TableIdent, Partitions, Comments:

	// statement roots
	case *Select:
		a.apply(n, "Comments", nil, n.Comments)
		a.applyCollection(n, "SelectExprs", n.SelectExprs)
		a.applyList(n, "From")
		a.apply(n, "Where", nil, n.Where)
		a.applyList(n, "GroupBy")
		a.apply(n, "Having", nil, n.Where)
		a.applyList(n, "OrderBy")
		a.apply(n, "Limit", nil, n.Limit)

	case *Union:
		a.apply(n, "Left", nil, n.Left)
		a.apply(n, "Right", nil, n.Left)
		a.apply(n, "Right", nil, n.Left)
		a.applyList(n, "OrderBy")
		a.apply(n, "Limit", nil, n.Limit)

	case *Stream:
		a.apply(n, "Comments", nil, n.Comments)
		a.apply(n, "SelectExpr", nil, n.SelectExpr)
		a.apply(n, "TableName", nil, n.Table)

	case *AliasedExpr:
		a.apply(n, "Expr", nil, n.Expr)
		a.apply(n, "As", nil, n.As)

	case *BinaryExpr:
		a.apply(n, "Left", nil, n.Left)
		a.apply(n, "Right", nil, n.Right)

	case *AliasedTableExpr:
		a.apply(n, "Expr", nil, n.Expr)
		a.applyCollection(n, "Partitions", n.Partitions)
		a.apply(n, "As", nil, n.As)
		a.apply(n, "Hints", nil, n.Hints)

	case TableName:
		a.apply(n, "Name", nil, n.Name)
		a.apply(n, "Name", nil, n.Qualifier)

	default:
		panic(fmt.Sprintf("Apply: unexpected node type %T", n))
	}

	if a.post != nil && !a.post(&a.cursor) {
		panic(abort)
	}

	a.cursor = saved
}

// An iterator controls iteration over a slice of nodes.
type iterator struct {
	index, step int
}

func (a *application) applyCollection(parent SQLNode, name string, collection SQLNode) {
	a.apply(parent, name, nil, collection)

	// avoid heap-allocating a new iterator for each applyList call; reuse a.iter instead
	saved := a.iter
	a.iter.index = 0
	for {
		v := reflect.ValueOf(collection)
		if a.iter.index >= v.Len() {
			break
		}

		// element x may be nil in a bad AST - be cautious
		var x SQLNode
		if e := v.Index(a.iter.index); e.IsValid() {
			x = e.Interface().(SQLNode)
		}

		a.iter.step = 1
		a.apply(parent, name, &a.iter, x)
		a.iter.index += a.iter.step
	}
	a.iter = saved
}

func (a *application) applyList(parent SQLNode, name string) {
	// avoid heap-allocating a new iterator for each applyList call; reuse a.iter instead
	saved := a.iter
	a.iter.index = 0
	for {
		// must reload parent.name each time, since cursor modifications might change it
		indirect := reflect.Indirect(reflect.ValueOf(parent))
		v := indirect.FieldByName(name)
		if a.iter.index >= v.Len() {
			break
		}

		// element x may be nil in a bad AST - be cautious
		var x SQLNode
		if e := v.Index(a.iter.index); e.IsValid() {
			x = e.Interface().(SQLNode)
		}

		a.iter.step = 1
		a.apply(parent, name, &a.iter, x)
		a.iter.index += a.iter.step
	}
	a.iter = saved
}
