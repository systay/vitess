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

package asthelpergen

import (
	"bytes"
	"fmt"
	"go/types"
	"io/ioutil"
	"log"
	"path"
	"strings"

	"github.com/dave/jennifer/jen"
	"golang.org/x/tools/go/packages"
)

const licenseFileHeader = `Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.`

type generatorSPI interface {
	addType(t types.Type)
	addFunc(name string, t methodType, code jen.Code)
	scope() *types.Scope
}

type generator interface {
	interfaceMethod(t types.Type, iface *types.Interface, spi generatorSPI) error
	structMethod(t types.Type, strct *types.Struct, spi generatorSPI) error
	ptrToStructMethod(t types.Type, strct *types.Struct, spi generatorSPI) error
	ptrToBasicMethod(t types.Type, basic *types.Basic, spi generatorSPI) error
	sliceMethod(t types.Type, slice *types.Slice, spi generatorSPI) error
}

// astHelperGen finds implementations of the given interface,
// and uses the supplied `generator`s to produce the output code
type astHelperGen struct {
	DebugTypes bool
	mod        *packages.Module
	sizes      types.Sizes
	namedIface *types.Named
	iface      *types.Interface
	gens       []generator
	todo       []types.Type
	methods    []jen.Code
	_scope     *types.Scope
}

func newGenerator(mod *packages.Module, sizes types.Sizes, named *types.Named, generators ...generator) *astHelperGen {
	return &astHelperGen{
		DebugTypes: true,
		mod:        mod,
		sizes:      sizes,
		namedIface: named,
		iface:      named.Underlying().(*types.Interface),
		gens:       generators,
	}
}

var _ generatorSPI = (*astHelperGen)(nil)

func (gen *astHelperGen) scope() *types.Scope {
	return gen._scope
}

func (gen *astHelperGen) addType(t types.Type) {
	gen.todo = append(gen.todo, t)
}

func (gen *astHelperGen) addFunc(name string, typ methodType, code jen.Code) {
	var comment string
	switch typ {
	case clone:
		comment = " creates a deep clone of the input."
	case equals:
		comment = " does deep equals between the two objects."
	}
	gen.methods = append(gen.methods, jen.Comment(name+comment), code)
}

func findImplementations(scope *types.Scope, iff *types.Interface, impl func(types.Type) error) error {
	for _, name := range scope.Names() {
		obj := scope.Lookup(name)
		if _, ok := obj.(*types.TypeName); !ok {
			continue
		}
		baseType := obj.Type()
		if types.Implements(baseType, iff) {
			err := impl(baseType)
			if err != nil {
				return err
			}
			continue
		}
		pointerT := types.NewPointer(baseType)
		if types.Implements(pointerT, iff) {
			err := impl(pointerT)
			if err != nil {
				return err
			}
			continue
		}
	}
	return nil
}

// GenerateCode is the main loop where we build up the code per file.
func (gen *astHelperGen) GenerateCode() (map[string]*jen.File, error) {
	pkg := gen.namedIface.Obj().Pkg()
	iface, ok := gen.iface.Underlying().(*types.Interface)
	if !ok {
		return nil, fmt.Errorf("expected interface, but got %T", gen.iface)
	}

	gen.todo = append(gen.todo, gen.iface)
	err := findImplementations(pkg.Scope(), iface, func(t types.Type) error {
		switch n := t.Underlying().(type) {
		case *types.Interface:
			return gen.visitInterface(t, n)
		default:
			// do nothing
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	result := map[string]*jen.File{}
	for _, g := range gen.gens {
		file, code := g.createFile(pkg.Name())
		fullPath := path.Join(gen.mod.Dir, strings.TrimPrefix(pkg.Path(), gen.mod.Path), file)
		result[fullPath] = code
	}

	return result, nil
}

// TypePaths are the packages
type TypePaths []string

func (t *TypePaths) String() string {
	return fmt.Sprintf("%v", *t)
}

// Set adds the package path
func (t *TypePaths) Set(path string) error {
	*t = append(*t, path)
	return nil
}

// VerifyFilesOnDisk compares the generated results from the codegen against the files that
// currently exist on disk and returns any mismatches
func VerifyFilesOnDisk(result map[string]*jen.File) (errors []error) {
	for fullPath, file := range result {
		existing, err := ioutil.ReadFile(fullPath)
		if err != nil {
			errors = append(errors, fmt.Errorf("missing file on disk: %s (%w)", fullPath, err))
			continue
		}

		var buf bytes.Buffer
		if err := file.Render(&buf); err != nil {
			errors = append(errors, fmt.Errorf("render error for '%s': %w", fullPath, err))
			continue
		}

		if !bytes.Equal(existing, buf.Bytes()) {
			errors = append(errors, fmt.Errorf("'%s' has changed", fullPath))
			continue
		}
	}
	return errors
}

// GenerateASTHelpers loads the input code, constructs the necessary generators,
// and generates the rewriter and clone methods for the AST
func GenerateASTHelpers(packagePatterns []string, rootIface, exceptCloneType string) (map[string]*jen.File, error) {
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesSizes | packages.NeedTypesInfo | packages.NeedDeps | packages.NeedImports | packages.NeedModule,
	}, packagePatterns...)

	if err != nil {
		return nil, err
	}

	scopes := make(map[string]*types.Scope)
	for _, pkg := range loaded {
		scopes[pkg.PkgPath] = pkg.Types.Scope()
	}

	pos := strings.LastIndexByte(rootIface, '.')
	if pos < 0 {
		return nil, fmt.Errorf("unexpected input type: %s", rootIface)
	}

	pkgname := rootIface[:pos]
	typename := rootIface[pos+1:]

	scope := scopes[pkgname]
	if scope == nil {
		return nil, fmt.Errorf("no scope found for type '%s'", rootIface)
	}

	tt := scope.Lookup(typename)
	if tt == nil {
		return nil, fmt.Errorf("no type called '%s' found in '%s'", typename, pkgname)
	}

	nt := tt.Type().(*types.Named)

	iface := nt.Underlying().(*types.Interface)

	interestingType := func(t types.Type) bool {
		return types.Implements(t, iface)
	}
	rewriter := newRewriterGen(interestingType, nt.Obj().Name())
	clone := newCloneGen(iface, scope, exceptCloneType)

	generator := newGenerator(loaded[0].Module, loaded[0].TypesSizes, nt, rewriter, clone)
	it, err := generator.GenerateCode()
	if err != nil {
		return nil, err
	}

	return it, nil
}

func (gen *astHelperGen) createFile(pkgName string) (string, *jen.File) {
	out := jen.NewFile(pkgName)
	out.HeaderComment(licenseFileHeader)
	out.HeaderComment("Code generated by ASTHelperGen. DO NOT EDIT.")
	alreadyDone := map[string]bool{}
	for len(gen.todo) > 0 {
		t := gen.todo[0]
		underlying := t.Underlying()
		typeName := printableTypeName(t)
		gen.todo = gen.todo[1:]

		if alreadyDone[typeName] {
			continue
		}

		switch underlying := underlying.(type) {
		case *types.Interface:
			gen.allGenerators(func(g generator) error {
				return g.interfaceMethod(t, underlying, gen)
			})
		case *types.Slice:
			gen.allGenerators(func(g generator) error {
				return g.sliceMethod(t, underlying, gen)
			})
		case *types.Struct:
			gen.allGenerators(func(g generator) error {
				return g.structMethod(t, underlying, gen)
			})
		case *types.Pointer:
			ptrToType := underlying.Elem().Underlying()

			switch ptrToType := ptrToType.(type) {
			case *types.Struct:
				gen.allGenerators(func(g generator) error {
					return g.ptrToStructMethod(t, ptrToType, gen)
				})
			case *types.Basic:
				gen.allGenerators(func(g generator) error {
					return g.ptrToBasicMethod(t, ptrToType, gen)
				})
			default:
				panic("oh noes")
				//c.makePtrCloneMethod(t, ptr)
			}

		default:
			log.Fatalf("don't know how to handle %s %T", typeName, underlying)
		}

		if c.tryInterface(underlying, t) ||
			c.trySlice(underlying, t) ||
			c.tryStruct(underlying, t) ||
			c.tryPtr(underlying, t) {
			alreadyDone[typeName] = true
			continue
		}

	}

	for _, method := range gen.methods {
		out.Add(method)
	}

	return "clone.go", out
}

func (gen *astHelperGen) allGenerators(f func(g generator) error) {
	for _, g := range gen.gens {
		err := f(g)

		if err != nil {
			log.Fatalf("%v", err)
		}
	}
}
