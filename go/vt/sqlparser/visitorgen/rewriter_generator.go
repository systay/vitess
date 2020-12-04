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

package visitorgen

import (
	"bytes"
	"fmt"
	"go/parser"
	"go/token"
	"io/ioutil"
)

// GenerateRewriter generates a rewriter and saves it to outfile if compareOnly is false
// if compareOnly is true, it will instead compare the actual file with what would have
// been generated, and returns an error if they are not equal
func GenerateRewriter(astFile, outfile string, compareOnly bool) error {
	fs := token.NewFileSet()
	file, err := parser.ParseFile(fs, astFile, nil, parser.DeclarationErrors)
	if err != nil {
		return err
	}

	astWalkResult := Walk(file)
	vp := Transform(astWalkResult)
	vd := ToVisitorPlan(vp)

	replacementMethods := EmitReplacementMethods(vd)
	applyTypeSwitch := EmitApplyTypeSwitches(vd)

	b := &bytes.Buffer{}
	fmt.Fprint(b, fileHeader)
	fmt.Fprintln(b)
	fmt.Fprintln(b, replacementMethods)
	fmt.Fprint(b, applyHeader)
	fmt.Fprintln(b, applyTypeSwitch)
	fmt.Fprintln(b, fileFooter)

	if compareOnly {
		currentFile, err := ioutil.ReadFile(outfile)
		if err != nil {
			return err
		}
		if !bytes.Equal(b.Bytes(), currentFile) {
			return fmt.Errorf("rewriter needs to be re-generated: go generate " + outfile)
		}
	} else {
		err = ioutil.WriteFile(outfile, b.Bytes(), 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

const fileHeader = `// Code generated by visitorgen/main/main.go. DO NOT EDIT.

package sqlparser

//go:generate go run ./visitorgen/main -input=ast.go -output=rewriter.go

import (
	"reflect"
)

type replacerFunc func(newNode, parent SQLNode)

// application carries all the shared data so we can pass it around cheaply.
type application struct {
	pre, post ApplyFunc
	cursor    Cursor
}
`

const applyHeader = `
// apply is where the visiting happens. Here is where we keep the big switch-case that will be used
// to do the actual visiting of SQLNodes
func (a *application) apply(parent, node SQLNode, replacer replacerFunc) {
	if node == nil || isNilValue(node) {
		return
	}

	// avoid heap-allocating a new cursor for each apply call; reuse a.cursor instead
	saved := a.cursor
	a.cursor.replacer = replacer
	a.cursor.node = node
	a.cursor.parent = parent

	if a.pre != nil && !a.pre(&a.cursor) {
		a.cursor = saved
		return
	}

	// walk children
	// (the order of the cases is alphabetical)
	switch n := node.(type) {
	`

const fileFooter = `
	default:
		panic("unknown ast type " + reflect.TypeOf(node).String())
	}

	if a.post != nil && !a.post(&a.cursor) {
		panic(abort)
	}

	a.cursor = saved
}

func isNilValue(i interface{}) bool {
	valueOf := reflect.ValueOf(i)
	kind := valueOf.Kind()
	isNullable := kind == reflect.Ptr || kind == reflect.Array || kind == reflect.Slice
	return isNullable && valueOf.IsNil()
}`
