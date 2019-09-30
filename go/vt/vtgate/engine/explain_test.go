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
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestExplainExecuteJson(t *testing.T) {
	source := &fakePrimitive{jsonObj: "the cake is a lie"}

	explain := &Explain{
		Input:    source,
		UseTable: false,
	}
	r, err := explain.Execute(noopVCursor{}, make(map[string]*querypb.BindVariable), true)
	assert.NoError(t, err)
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"column",
			"varchar",
		),
		"\"the cake is a lie\"",
	))
}

func TestExplainExecuteTable(t *testing.T) {
	leaf1 := newP("leaf1")
	leaf2 := newP("leaf2")
	leaf3 := newP("leaf3")
	middle2 := newP("middle2", leaf2)
	middle1 := newP("middle1", middle2, leaf1)
	middle3 := newP("middle3", leaf3)
	root := newP("root", middle1, middle3)

	explain := &Explain{
		Input:    root,
		UseTable: true,
	}
	r, err := explain.Execute(noopVCursor{}, make(map[string]*querypb.BindVariable), true)
	assert.NoError(t, err)
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|routeType|keyspace|table",
			"varchar|varchar|varchar|varchar",
		),
		"└── root|Fake|fakeKs|fakeTable",
		"    ├── middle1|Fake|fakeKs|fakeTable",
		"    │   ├── middle2|Fake|fakeKs|fakeTable",
		"    │   │   └── leaf2|Fake|fakeKs|fakeTable",
		"    │   └── leaf1|Fake|fakeKs|fakeTable",
		"    └── middle3|Fake|fakeKs|fakeTable",
		"        └── leaf3|Fake|fakeKs|fakeTable",
	))
}

func TestTreePrint(t *testing.T) {
	leaf1 := newP("leaf1")
	leaf2 := newP("leaf2")
	leaf3 := newP("leaf3")
	middle2 := newP("middle2", leaf2)
	middle1 := newP("middle1", middle2, leaf1)
	middle3 := newP("middle3", leaf3)
	root := newP("root", middle1, middle3)

	got := treePrint(root)
	want := `root
├── middle1
│   ├── middle2
│   │   └── leaf2
│   └── leaf1
└── middle3
    └── leaf3
`
	assert.Equal(t, want, got)
}

func newP(name string, inputs ...Primitive) Primitive {
	return &fakePrimitive{identifier: name, inputs: inputs}
}
