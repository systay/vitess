/*
Copyright 2021 The Vitess Authors.

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

//nolint
package integration

type (
	A struct {
		field1 uint64
		field2 uint64
	}
	B interface {
		iface()
	}

	Bimpl struct {
		field1 uint64
	}

	C struct {
		field1 B
	}

	D struct {
		field1 *Bimpl
	}

	Padded struct {
		field1 uint64
		field2 uint8
		field3 uint64
	}

	Slice1 struct {
		field1 []A
	}

	Slice2 struct {
		field1 []B
	}

	Slice3 struct {
		field1 []*Bimpl
	}

	Map1 struct {
		field1 map[uint8]uint8
	}

	Map2 struct {
		field1 map[uint64]A
	}

	Map3 struct {
		field1 map[uint64]B
	}

	String1 struct {
		field1 string
		field2 uint64
	}

	AST interface {
		i()
	}

	Plus struct {
		Left, Right AST
	}

	Array struct {
		Values []AST
	}

	UnaryMinus struct {
		Val *LiteralInt
	}

	LiteralInt struct {
		Val int
	}

	String struct {
		Val string
	}

	ArrayDef []AST
)

func (b *Bimpl) iface() {}
func (*Plus) i()        {}
func (*Array) i()       {}
func (*UnaryMinus) i()  {}
func (*LiteralInt) i()  {}
func (String) i()       {}
func (ArrayDef) i()     {}
