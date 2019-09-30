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
	"encoding/json"
	"strings"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Explain)(nil)

// Explain will return the query plan for a query instead of actually running it
type Explain struct {
	Input    Primitive
	UseTable bool
}

// RouteType returns a description of the query routing type used by the primitive
func (e *Explain) RouteType() string {
	return e.Input.RouteType()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (e *Explain) GetKeyspaceName() string {
	return e.Input.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (e *Explain) GetTableName() string {
	return e.Input.GetTableName()
}

// MarshalJSON allows us to add the opcode in
func (e *Explain) MarshalJSON() ([]byte, error) {
	type Alias Explain
	return json.Marshal(&struct {
		OpCode string `json:"Opcode"`
		*Alias
	}{
		OpCode: "Explain",
		Alias:  (*Alias)(e),
	})
}

// Execute satisfies the Primitive interface.
func (e *Explain) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantFields bool) (*sqltypes.Result, error) {
	if e.UseTable {
		return e.tableResult()
	}
	return e.jsonResult()
}

func (e *Explain) jsonResult() (*sqltypes.Result, error) {
	var fields []*querypb.Field
	fields = append(fields, &querypb.Field{
		Name: "column",
		Type: querypb.Type_VARCHAR,
	})
	var rows [][]sqltypes.Value
	buf, err := json.MarshalIndent(e.Input, "", " ")
	if err != nil {
		return nil, err
	}
	outputText := string(buf)
	noOfRows := 0
	for _, line := range strings.Split(outputText, "\n") {
		rows = append(rows, []sqltypes.Value{sqltypes.NewVarChar(line)})
		noOfRows++
	}
	return &sqltypes.Result{Fields: fields, Rows: rows, RowsAffected: uint64(noOfRows), InsertID: 0}, nil
}

// StreamExecute satisfies the Primitive interface.
func (e *Explain) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantFields bool, callback func(*sqltypes.Result) error) error {
	result, err := e.Execute(vcursor, bindVars, wantFields)
	if err != nil {
		return err
	}

	return callback(result)
}

// GetFields satisfies the Primitive interface.
func (e *Explain) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return e.Input.GetFields(vcursor, bindVars)
}

// Inputs returns the single input of Explain
func (e *Explain) Inputs() []Primitive {
	return []Primitive{e.Input}
}

// Identifier satisfies the Primitive interface.
func (Explain) Identifier() string {
	return "Explain"
}

func createRow(p Primitive, name string) []sqltypes.Value {
	return []sqltypes.Value{
		sqltypes.NewVarChar(strings.TrimSpace(name)),
		sqltypes.NewVarChar(p.RouteType()),
		sqltypes.NewVarChar(p.GetKeyspaceName()),
		sqltypes.NewVarChar(p.GetTableName()),
	}
}

func (e *Explain) tableResult() (*sqltypes.Result, error) {
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_VARCHAR},
		{Name: "routeType", Type: querypb.Type_VARCHAR},
		{Name: "keyspace", Type: querypb.Type_VARCHAR},
		{Name: "table", Type: querypb.Type_VARCHAR},
	}
	fields = append(fields)
	rows := turnIntoTableResult([]Primitive{e.Input}, []bool{})
	size := len(rows)
	return &sqltypes.Result{
		Fields:       fields,
		RowsAffected: uint64(size),
		Rows:         rows,
	}, nil
}

const (
	newLine      = "\n"
	emptySpace   = "    "
	middleItem   = "├── "
	continueItem = "│   "
	lastItem     = "└── "
)

func treePrint(t Primitive) string {
	return t.Identifier() + newLine + printItems(t.Inputs(), []bool{})
}

func printText(text string, spaces []bool, last bool) string {
	var result string
	for _, space := range spaces {
		if space {
			result += emptySpace
		} else {
			result += continueItem
		}
	}

	indicator := middleItem
	if last {
		indicator = lastItem
	}

	var out string
	lines := strings.Split(text, "\n")
	for i := range lines {
		text := lines[i]
		if i == 0 {
			out += result + indicator + text + newLine
			continue
		}
		if last {
			indicator = emptySpace
		} else {
			indicator = continueItem
		}
		out += result + indicator + text + newLine
	}

	return out
}

func printItems(t []Primitive, spaces []bool) string {
	var result string
	for i, f := range t {
		last := i == len(t)-1
		result += printText(f.Identifier(), spaces, last)
		if len(f.Inputs()) > 0 {
			spacesChild := append(spaces, last)
			result += printItems(f.Inputs(), spacesChild)
		}
	}
	return result
}

func turnIntoTableResult(t []Primitive, spaces []bool) [][]sqltypes.Value {
	var result [][]sqltypes.Value
	for i, f := range t {
		last := i == len(t)-1
		name := printText(f.Identifier(), spaces, last)
		row := createRow(f, name)
		result = append(result, row)
		if len(f.Inputs()) > 0 {
			spacesChild := append(spaces, last)
			result = append(result, turnIntoTableResult(f.Inputs(), spacesChild)...)
		}
	}
	return result
}
