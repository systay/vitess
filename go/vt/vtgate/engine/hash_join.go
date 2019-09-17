/*
Copyright 2017 Google Inc.

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
	"encoding/json"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*HashJoin)(nil)

// HashJoin specifies the parameters for a join primitive. It does it work by building a hash map (a.k.a probe table)
// for the lhs input, and then uses this probe table to "probe" the rhs, finding matches by hashing
// the join column values
type HashJoin struct {
	// Left and Right are the LHS and RHS primitives
	// of the Join. They can be any primitive.
	Left, Right Primitive `json:",omitempty"`

	// Cols defines which columns from the left
	// or right results should be used to build the
	// return result. For results coming from the
	// left query, the index values go as -1, -2, etc.
	// For the right query, they're 1, 2, etc.
	// If Cols is {-1, -2, 1, 2}, it means that
	// the returned result will be {Left0, Left1, Right0, Right1}.
	Cols []int `json:",omitempty"`

	// LeftJoinCols defines which columns from the lhs are part of the ON comparison
	LeftJoinCols []int

	// RightJoinCols defines which columns from the rhs are part of the ON comparison
	RightJoinCols []int
}

// MarshalJSON allows us to add the opcode in, so we can see the join type used
func (jn *HashJoin) MarshalJSON() ([]byte, error) {
	type Alias HashJoin
	return json.Marshal(&struct {
		OpCode string `json:"Opcode"`
		*Alias
	}{
		OpCode: "HashJoin",
		Alias:  (*Alias)(jn),
	})
}

// Execute performs a non-streaming exec.
func (jn *HashJoin) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	lresult, err := jn.Left.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	table := newProbeTable()
	for _, row := range lresult.Rows {
		joinVals := extractJoinValues(row, jn.LeftJoinCols)
		table.Add(joinVals, row)
	}

	rresult, err := jn.Right.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	result := &sqltypes.Result{}
	if wantfields {
		result.Fields = joinFields(lresult.Fields, rresult.Fields, jn.Cols)
	}

	for _, rrow := range rresult.Rows {

		joinVals := extractJoinValues(rrow, jn.RightJoinCols)
		matches := table.Get(joinVals)
		for _, lrow := range matches {
			result.Rows = append(result.Rows, joinRows(lrow, rrow, jn.Cols))
			result.RowsAffected++
		}
	}

	return result, nil
}

func extractJoinValues(row []sqltypes.Value, joinCols []int) []sqltypes.Value {
	curr := make([]sqltypes.Value, len(joinCols))
	for i, column := range joinCols {
		curr[i] = row[column]
	}
	return curr
}

// StreamExecute performs a streaming exec.
func (jn *HashJoin) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	table := newProbeTable()
	var leftFields []*querypb.Field
	var resultFields []*querypb.Field

	err := jn.Left.StreamExecute(vcursor, bindVars, wantfields, func(lresult *sqltypes.Result) error {
		for _, row := range lresult.Rows {
			joinVals := extractJoinValues(row, jn.LeftJoinCols)
			table.Add(joinVals, row)
		}
		leftFields = lresult.Fields
		return nil
	})
	if err != nil {
		return err
	}

	err = jn.Right.StreamExecute(vcursor, bindVars, wantfields, func(rresult *sqltypes.Result) error {
		result := &sqltypes.Result{}

		if wantfields {
			// here we are setting the fields for every result object we return, but only calculating them once.
			if resultFields == nil {
				resultFields = joinFields(leftFields, rresult.Fields, jn.Cols)
			}
			result.Fields = resultFields
		}

		for _, rrow := range rresult.Rows {
			joinVals := extractJoinValues(rrow, jn.RightJoinCols)
			matches := table.Get(joinVals)
			for _, lrow := range matches {
				result.Rows = append(result.Rows, joinRows(lrow, rrow, jn.Cols))
			}
		}
		return callback(result)
	})

	return err
}

// GetFields fetches the field info.
func (jn *HashJoin) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	joinVars := make(map[string]*querypb.BindVariable)
	lresult, err := jn.Left.GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	result := &sqltypes.Result{}
	rresult, err := jn.Right.GetFields(vcursor, combineVars(bindVars, joinVars))
	if err != nil {
		return nil, err
	}
	result.Fields = joinFields(lresult.Fields, rresult.Fields, jn.Cols)
	return result, nil
}

// RouteType returns a description of the query routing type used by the primitive
func (jn *HashJoin) RouteType() string {
	return "HashJoin"
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (jn *HashJoin) GetKeyspaceName() string {
	if jn.Left.GetKeyspaceName() == jn.Right.GetKeyspaceName() {
		return jn.Left.GetKeyspaceName()
	}
	return jn.Left.GetKeyspaceName() + "_" + jn.Right.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (jn *HashJoin) GetTableName() string {
	return jn.Left.GetTableName() + "_" + jn.Right.GetTableName()
}

type probeTable struct {
	table map[int][][]sqltypes.Value
}

func newProbeTable() probeTable {
	return probeTable{table: make(map[int][][]sqltypes.Value)}
}

func (p *probeTable) Add(key []sqltypes.Value, value []sqltypes.Value) {
	hash := p.calculateHashFor(key)
	chunk, ok := p.table[hash]
	if !ok {
		p.table[hash] = [][]sqltypes.Value{value}
	} else {
		p.table[hash] = append(chunk, value)
	}
}

func (p *probeTable) calculateHashFor(key []sqltypes.Value) int {
	hash := 0
	for _, k := range key {
		hash = 31*hash + int(k.Type())
		for _, b := range k.Raw() {
			hash = 31*hash + int(b)
		}
	}
	return hash
}

func (p *probeTable) Get(key []sqltypes.Value) [][]sqltypes.Value {
	hash := p.calculateHashFor(key)

	return p.table[hash]
}

func (p *probeTable) IsEmpty() bool {
	return len(p.table) == 0
}
