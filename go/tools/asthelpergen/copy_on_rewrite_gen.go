/*
Copyright 2023 The Vitess Authors.

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

package asthelpergen

import (
	"go/types"

	"github.com/dave/jennifer/jen"
)

type cowGen struct {
	file     *jen.File
	baseType string
}

var _ generator = (*cowGen)(nil)

func newCOWGen(pkgname string, nt *types.Named) *cowGen {
	file := jen.NewFile(pkgname)
	file.HeaderComment(licenseFileHeader)
	file.HeaderComment("Code generated by ASTHelperGen. DO NOT EDIT.")

	return &cowGen{
		file:     file,
		baseType: nt.Obj().Id(),
	}
}

func (c *cowGen) addFunc(code *jen.Statement) {
	c.file.Add(code)
}

func (c *cowGen) genFile() (string, *jen.File) {
	return "ast_copy_on_rewrite.go", c.file
}

const cowName = "copyOnRewrite"

// readValueOfType produces code to read the expression of type `t`, and adds the type to the todo-list
func (c *cowGen) readValueOfType(t types.Type, expr jen.Code, spi generatorSPI) jen.Code {
	switch t.Underlying().(type) {
	case *types.Interface:
		if types.TypeString(t, noQualifier) == "any" {
			// these fields have to be taken care of manually
			return expr
		}
	}
	spi.addType(t)
	return jen.Id("c").Dot(cowName + printableTypeName(t)).Call(expr)
}

func (c *cowGen) sliceMethod(t types.Type, slice *types.Slice, spi generatorSPI) error {
	if !types.Implements(t, spi.iface()) {
		return nil
	}

	typeString := types.TypeString(t, noQualifier)

	changedVarName := "changed"
	fieldVar := "res"
	elemTyp := types.TypeString(slice.Elem(), noQualifier)

	name := printableTypeName(t)
	funcName := cowName + name
	var visitElements *jen.Statement

	if types.Implements(slice.Elem(), spi.iface()) {
		visitElements = ifPreNotNilOrReturnsTrue().Block(
			jen.Id(fieldVar).Op(":=").Id("make").Params(jen.Id(typeString), jen.Id("len").Params(jen.Id("n"))), // _Foo := make([]Typ, len(n))
			jen.For(jen.List(jen.Id("x"), jen.Id("el")).Op(":=").Id("range n")).Block(
				c.visitFieldOrElement("this", "change", slice.Elem(), jen.Id("el"), spi),
				// jen.Id(fieldVar).Index(jen.Id("x")).Op("=").Id("this").Op(".").Params(jen.Id(types.TypeString(elemTyp, noQualifier))),
				jen.Id(fieldVar).Index(jen.Id("x")).Op("=").Id("this").Op(".").Params(jen.Id(elemTyp)),
				jen.If(jen.Id("change")).Block(
					jen.Id(changedVarName).Op("=").True(),
				),
			),
			jen.If(jen.Id("changed")).Block(
				jen.Id("out").Op("=").Id("res"),
			),
		)
	} else {
		visitElements = jen.If(jen.Id("c.pre != nil")).Block(
			jen.Id("c.pre(n, parent)"),
		)
	}

	block := c.funcDecl(funcName, typeString).Block(
		ifNilReturnNilAndFalse("n"),
		jen.Id("out").Op("=").Id("n"),
		visitElements,
		ifPostNotNilVisit("out"),
		jen.Return(),
	)
	c.addFunc(block)
	return nil
}

func (c *cowGen) basicMethod(t types.Type, basic *types.Basic, spi generatorSPI) error {
	if !types.Implements(t, spi.iface()) {
		return nil
	}

	typeString := types.TypeString(t, noQualifier)
	typeName := printableTypeName(t)

	var stmts []jen.Code
	stmts = append(stmts,
		jen.If(jen.Id("c").Dot("cursor").Dot("stop")).Block(jen.Return(jen.Id("n"), jen.False())),
		ifNotNil("c.pre", jen.Id("c.pre").Params(jen.Id("n"), jen.Id("parent"))),
		ifNotNil("c.post", jen.List(jen.Id("out"), jen.Id("changed")).Op("=").Id("c.postVisit").Params(jen.Id("n"), jen.Id("parent"), jen.Id("changed"))).
			Else().Block(jen.Id("out = n")),
		jen.Return(),
	)
	funcName := cowName + typeName
	funcDecl := c.funcDecl(funcName, typeString).Block(stmts...)
	c.addFunc(funcDecl)
	return nil
}

func ifNotNil(id string, stmts ...jen.Code) *jen.Statement {
	return jen.If(jen.Id(id).Op("!=").Nil()).Block(stmts...)
}

func ifNilReturnNilAndFalse(id string) *jen.Statement {
	return jen.If(jen.Id(id).Op("==").Nil().Op("||").Id("c").Dot("cursor").Dot("stop")).Block(jen.Return(jen.Id("n"), jen.False()))
}

func ifPreNotNilOrReturnsTrue() *jen.Statement {
	//	if c.pre == nil || c.pre(n, parent) {
	return jen.If(
		jen.Id("c").Dot("pre").Op("==").Nil().Op("||").Id("c").Dot("pre").Params(
			jen.Id("n"),
			jen.Id("parent"),
		))

}

func (c *cowGen) interfaceMethod(t types.Type, iface *types.Interface, spi generatorSPI) error {
	if !types.Implements(t, spi.iface()) {
		return nil
	}

	// func (c cow) cowAST(in AST) (AST, bool) {
	//	if in == nil {
	//		return nil, false
	// 	}
	//
	//	if c.old == in {
	//		return c.new, true
	//	}
	//	switch in := in.(type) {
	// 	case *RefContainer:
	//			return c.CowRefOfRefContainer(in)
	// 	}
	//	// this should never happen
	//	return nil
	// }

	typeString := types.TypeString(t, noQualifier)
	typeName := printableTypeName(t)

	stmts := []jen.Code{ifNilReturnNilAndFalse("n")}

	var cases []jen.Code
	_ = findImplementations(spi.scope(), iface, func(t types.Type) error {
		if _, ok := t.Underlying().(*types.Interface); ok {
			return nil
		}
		spi.addType(t)
		typeString := types.TypeString(t, noQualifier)

		// case Type: return CloneType(in)
		block := jen.Case(jen.Id(typeString)).Block(jen.Return(c.readValueOfType(t, jen.List(jen.Id("n"), jen.Id("parent")), spi)))
		cases = append(cases, block)

		return nil
	})

	cases = append(cases,
		jen.Default().Block(
			jen.Comment("this should never happen"),
			jen.Return(jen.Nil(), jen.False()),
		))

	//	switch n := node.(type) {
	stmts = append(stmts, jen.Switch(jen.Id("n").Op(":=").Id("n").Assert(jen.Id("type")).Block(
		cases...,
	)))

	funcName := cowName + typeName
	funcDecl := c.funcDecl(funcName, typeString).Block(stmts...)
	c.addFunc(funcDecl)
	return nil
}

func (c *cowGen) ptrToBasicMethod(t types.Type, _ *types.Basic, spi generatorSPI) error {
	if !types.Implements(t, spi.iface()) {
		return nil
	}

	ptr := t.Underlying().(*types.Pointer)
	return c.ptrToOtherMethod(t, ptr, spi)
}

func (c *cowGen) ptrToOtherMethod(t types.Type, ptr *types.Pointer, spi generatorSPI) error {
	if !types.Implements(t, spi.iface()) {
		return nil
	}

	receiveType := types.TypeString(t, noQualifier)

	funcName := cowName + printableTypeName(t)
	c.addFunc(c.funcDecl(funcName, receiveType).Block(
		jen.Comment("apan was here"),
		jen.Return(jen.Id("n"), jen.False()),
	))
	return nil
}

// func (c cow) COWRefOfType(n *Type) (*Type, bool)
func (c *cowGen) funcDecl(funcName, typeName string) *jen.Statement {
	return jen.Func().Params(jen.Id("c").Id("*cow")).Id(funcName).Call(jen.List(jen.Id("n").Id(typeName), jen.Id("parent").Id(c.baseType))).Params(jen.Id("out").Id(c.baseType), jen.Id("changed").Id("bool"))
}

func (c *cowGen) visitFieldOrElement(varName, changedVarName string, typ types.Type, el *jen.Statement, spi generatorSPI) *jen.Statement {
	// _Field, changedField := c.COWType(n.<Field>, n)
	return jen.List(jen.Id(varName), jen.Id(changedVarName)).Op(":=").Add(c.readValueOfType(typ, jen.List(el, jen.Id("n")), spi))
}

func (c *cowGen) structMethod(t types.Type, strct *types.Struct, spi generatorSPI) error {
	if !types.Implements(t, spi.iface()) {
		return nil
	}

	c.visitStruct(t, strct, spi, nil, false)
	return nil
}

func (c *cowGen) ptrToStructMethod(t types.Type, strct *types.Struct, spi generatorSPI) error {
	if !types.Implements(t, spi.iface()) {
		return nil
	}
	start := ifNilReturnNilAndFalse("n")

	c.visitStruct(t, strct, spi, start, true)
	return nil
}

func (c *cowGen) visitStruct(t types.Type, strct *types.Struct, spi generatorSPI, start *jen.Statement, ref bool) {
	receiveType := types.TypeString(t, noQualifier)
	funcName := cowName + printableTypeName(t)

	funcDeclaration := c.funcDecl(funcName, receiveType)

	var fields []jen.Code
	out := "out"
	changed := "res"
	var fieldSetters []jen.Code
	kopy := jen.Id(changed).Op(":=")
	if ref {
		fieldSetters = append(fieldSetters, kopy.Op("*").Id("n")) // changed := *n
	} else {
		fieldSetters = append(fieldSetters, kopy.Id("n")) // changed := n
	}
	var changedVariables []string
	for i := 0; i < strct.NumFields(); i++ {
		field := strct.Field(i).Name()
		typ := strct.Field(i).Type()
		changedVarName := "changed" + field

		fieldType := types.TypeString(typ, noQualifier)
		fieldVar := "_" + field
		if types.Implements(typ, spi.iface()) {
			fields = append(fields, c.visitFieldOrElement(fieldVar, changedVarName, typ, jen.Id("n").Dot(field), spi))
			changedVariables = append(changedVariables, changedVarName)
			fieldSetters = append(fieldSetters, jen.List(jen.Id(changed).Dot(field), jen.Op("_")).Op("=").Id(fieldVar).Op(".").Params(jen.Id(fieldType)))
		} else {
			// _Foo := make([]*Type, len(n.Foo))
			// var changedFoo bool
			// for x, el := range n.Foo {
			// 	c, changed := c.COWSliceOfRefOfType(el, n)
			// 	if changed {
			// 		changedFoo = true
			// 	}
			// 	_Foo[i] = c.(*Type)
			// }

			slice, isSlice := typ.(*types.Slice)
			if isSlice && types.Implements(slice.Elem(), spi.iface()) {
				elemTyp := slice.Elem()
				spi.addType(elemTyp)
				x := jen.Id("x")
				el := jen.Id("el")
				// 	changed := jen.Id("changed")
				fields = append(fields,
					jen.Var().Id(changedVarName).Bool(), // var changedFoo bool
					jen.Id(fieldVar).Op(":=").Id("make").Params(jen.Id(fieldType), jen.Id("len").Params(jen.Id("n").Dot(field))), // _Foo := make([]Typ, len(n.Foo))
					jen.For(jen.List(x, el).Op(":=").Id("range n").Dot(field)).Block(
						c.visitFieldOrElement("this", "changed", elemTyp, jen.Id("el"), spi),
						jen.Id(fieldVar).Index(jen.Id("x")).Op("=").Id("this").Op(".").Params(jen.Id(types.TypeString(elemTyp, noQualifier))),
						jen.If(jen.Id("changed")).Block(
							jen.Id(changedVarName).Op("=").True(),
						),
					),
				)
				changedVariables = append(changedVariables, changedVarName)
				fieldSetters = append(fieldSetters, jen.Id(changed).Dot(field).Op("=").Id(fieldVar))
			}
		}
	}

	var cond *jen.Statement
	for _, variable := range changedVariables {
		if cond == nil {
			cond = jen.Id(variable)
		} else {
			cond = cond.Op("||").Add(jen.Id(variable))
		}

	}

	fieldSetters = append(fieldSetters,
		jen.Id(out).Op("=").Op("&").Id(changed),
		ifNotNil("c.cloned", jen.Id("c.cloned").Params(jen.Id("n, out"))),
		jen.Id("changed").Op("=").True(),
	)
	ifChanged := jen.If(cond).Block(fieldSetters...)

	var stmts []jen.Code
	if start != nil {
		stmts = append(stmts, start)
	}

	// handle all fields with CloneAble types
	var visitChildren []jen.Code
	visitChildren = append(visitChildren, fields...)
	if len(fieldSetters) > 4 /*we add three statements always*/ {
		visitChildren = append(visitChildren, ifChanged)
	}

	children := ifPreNotNilOrReturnsTrue().Block(visitChildren...)
	stmts = append(stmts,
		jen.Id(out).Op("=").Id("n"),
		children,
	)

	stmts = append(
		stmts,
		ifPostNotNilVisit(out),
		jen.Return(),
	)

	c.addFunc(funcDeclaration.Block(stmts...))
}

func ifPostNotNilVisit(out string) *jen.Statement {
	return ifNotNil("c.post", jen.List(jen.Id(out), jen.Id("changed")).Op("=").Id("c").Dot("postVisit").Params(jen.Id(out), jen.Id("parent"), jen.Id("changed")))
}
