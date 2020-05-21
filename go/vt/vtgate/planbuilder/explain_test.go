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

package planbuilder

import (
	"golang.org/x/tools/go/ssa/interp/testdata/src/fmt"
	"strings"
	"testing"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type Descr = engine.PrimitiveDescription

func TestTreeStructure(t *testing.T) {
	var classical, popRock Descr
	{
		n1, n2 := node("Light"), node("Heavy")
		n3, n4 := node("Piano"), node("Orchestra", n1, n2)
		n5, n6 := node("Male"), node("Female")
		n7, n8 := node("Opera", n5, n6), node("Chorus")
		n9, n10 := node("Instrumental", n3, n4), node("Vocal", n7, n8)
		classical = node("Classical", n9, n10)
	}
	{
		n3 := node("Heavy metal")
		n4, n5 := node("Dancing"), node("Soft")
		n6, n7 := node("Rock", n3), node("Country", n4, n5)
		n8, n9 := node("Late pop"), node("Disco")
		n10, n11 := node("Soft techno"), node("Hard techno")
		n12, n13 := node("Pop", n8, n9), node("Techno", n10, n11)
		n14, n15 := node("Organic", n6, n7), node("Electronic", n12, n13)
		popRock = node("Pop/Rock", n14, n15)
	}
	music := node("Music", classical, popRock)

	descriptions := treeLines(music)

	output := ""
	for _, d := range descriptions {
		output += d.header + d.descr.OperatorType + "\n"
	}
	want :=
		`Music
├─ Classical
│  ├─ Instrumental
│  │  ├─ Piano
│  │  └─ Orchestra
│  │     ├─ Light
│  │     └─ Heavy
│  └─ Vocal
│     ├─ Opera
│     │  ├─ Male
│     │  └─ Female
│     └─ Chorus
└─ Pop/Rock
   ├─ Organic
   │  ├─ Rock
   │  │  └─ Heavy metal
   │  └─ Country
   │     ├─ Dancing
   │     └─ Soft
   └─ Electronic
      ├─ Pop
      │  ├─ Late pop
      │  └─ Disco
      └─ Techno
         ├─ Soft techno
         └─ Hard techno
`

	utils.MustMatch(t, want, output, "")
}

func node(name string, inputs ...Descr) Descr {
	return Descr{
		OperatorType: name,
		Inputs:       inputs,
	}
}

func TestSingleNode(t *testing.T) {
	single := node("single")

	output := toString(treeLines(single))

	utils.MustMatch(t, "single", output, "")
}

func TestTwoNodes(t *testing.T) {
	root := node("parent", node("child1"), node("child2"))

	descriptions := treeLines(root)
	output := toString(descriptions)

	want :=
		`parent
├─ child1
└─ child2`
	utils.MustMatch(t, want, output, "")
}

func TestThreeNodes(t *testing.T) {
	/*
		Electronic
		      ├─ Pop
		      │  ├─ Late pop
		      │  └─ Disco
		      └─ Techno
		         ├─ Soft techno
		         └─ Hard techno
	*/

	pop := node("pop", node("late pop"), node("disco"))
	techno := node("techno", node("soft techno"), node("hard techno"))
	electronic := node("electronic", pop, techno)

	descriptions := treeLines(electronic)
	output := toString(descriptions)
	want :=
		`electronic
├─ pop
│  ├─ late pop
│  └─ disco
└─ techno
   ├─ soft techno
   └─ hard techno`
	utils.MustMatch(t, want, output, "")
}

func TestFilteringOfColumns(t *testing.T) {

	type Descr = engine.PrimitiveDescription

	a := Descr{
		OperatorType:      "x",
		Variant:           "",
		Keyspace:          nil,
		TargetDestination: nil,
		Other:             nil,
	}
	b := Descr{
		OperatorType:      "",
		Variant:           "x",
		Keyspace:          nil,
		TargetDestination: nil,
		TargetTabletType:  0,
		Other:             nil,
	}
	c := Descr{
		OperatorType:      "",
		Variant:           "",
		Keyspace:          nil,
		TargetDestination: nil,
		TargetTabletType:  0,
		Other:             nil,
	}

	in := &fakePrimitive{
		descr:  a,
		inputs: []Descr{b, c},
	}
	fmt.Println(in)
}

func toString(descriptions []description) string {
	output := ""
	for _, d := range descriptions {
		output += d.header + d.descr.OperatorType + "\n"
	}
	return strings.Trim(output, " \n\t")
}

var _ engine.Primitive = (*fakePrimitive)(nil)

type fakePrimitive struct {
	descr  engine.PrimitiveDescription
	inputs []engine.PrimitiveDescription
}

func (f *fakePrimitive) Description() engine.PrimitiveDescription {
	return f.descr
}

func (f *fakePrimitive) RouteType() string {
	panic("implement me")
}

func (f *fakePrimitive) GetKeyspaceName() string {
	panic("implement me")
}

func (f *fakePrimitive) GetTableName() string {
	panic("implement me")
}

func (f *fakePrimitive) Execute(vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	panic("implement me")
}

func (f *fakePrimitive) StreamExecute(vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (f *fakePrimitive) GetFields(vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

func (f *fakePrimitive) NeedsTransaction() bool {
	panic("implement me")
}

func (f *fakePrimitive) Inputs() []engine.Primitive {
	panic("implement me")
}

func (f *fakePrimitive) description() engine.PrimitiveDescription {
	panic("implement me")
}
