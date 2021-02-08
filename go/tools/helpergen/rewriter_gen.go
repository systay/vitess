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

package main

import (
	"fmt"
	"go/types"
	"strings"

	"github.com/dave/jennifer/jen"
)

type (
	rewriterGen struct {
		lookup    typeLookup
		ifaceType *types.Named
	}
	rewriterState struct {
		replaceMethods []jen.Code
		// cases          []*jen.Statement
	}
)

func newRewriterGen(lookup typeLookup, ifaceType *types.Named) output {
	jen.Case()
	return &rewriterGen{lookup: lookup, ifaceType: ifaceType}
}

var _ output = (*rewriterGen)(nil)

func (r *rewriterGen) finalizeFile(file *codeFile, out *jen.File) {
	state := file.state.(*rewriterState)
	for _, method := range state.replaceMethods {
		out.Add(method)
		fmt.Printf("%#v\n", method)
	}
}

func (r *rewriterGen) implForStruct(file *codeFile, name *types.TypeName, st *types.Struct, _ types.Sizes, debugTypes bool) {
	var state *rewriterState
	if file.state == nil {
		state = &rewriterState{}
		file.state = state
	} else {
		state = file.state.(*rewriterState)
	}

	for i := 0; i < st.NumFields(); i++ {
		field := st.Field(i)
		namn := name.Type().String()
		t := field.Type()
		fmt.Println(t)
		pos := strings.LastIndexByte(namn, '.')
		if pos < 0 {
			panic(1)
		}

		typename := namn[pos+1:]

		apa := "replace" + typename + field.Name()
		s := jen.Func().Id(apa).Params(
			jen.Id("newNode"), jen.Id("parent").Qual("", typeString(r.ifaceType)),
		).Block(
			jen.Id("parent").Assert(jen.Id(typeString(name.Type()))).Dot(field.Name()).
				Op("=").
				Id("newNode").Assert(jen.Id(typeString(t))),
		)
		state.replaceMethods = append(state.replaceMethods, s)
	}
}

func (r *rewriterGen) fileName() string {
	return "rewriter.go"
}

func (r *rewriterGen) isEmpty(*codeFile) bool {
	return false
}

func typeString(typ types.Type) string {
	switch tt := typ.(type) {
	case *types.Named:
		return tt.Obj().Name()
	case *types.Pointer:
		return "*" + stripPackage(tt.String())
	case *types.Slice:
		return "[]" + stripPackage(tt.String())
	case *types.Map:
		k := stripPackage(tt.Key().String())
		e := stripPackage(tt.Elem().String())
		return fmt.Sprintf("map[%s]%s", k, e)
	case *types.Basic:
		return tt.String()
	default:
		panic(fmt.Sprintf("%T", typ))
	}
}

func stripPackage(s string) string {

	pos := strings.LastIndexByte(s, '.')
	if pos < 0 {
		return s
	}

	return s[pos+1:]
}
