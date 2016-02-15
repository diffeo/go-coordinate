// Copyright 2016 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

// Cptest copies test functions from a package into a new file.  The
// test functions should be normal, public functions in non-test source
// files.  Usage:
//
//     cptest --output from_tests.go --package to_test pkg/from/tests
//
// This is intended to support shared tests for an interface, such as
// github.com/diffeo/go-coordinate/coordinate/coordinatetest.  "go test"
// requires test functions to be declared in the package itself -- even
// dot imports don't count -- and so if you do have a directory of
// reusable tests you need a tool like this to copy them.
//
// The generated file just calls the original test functions:
//
//     package to_test
//
//     import "testing"
//     import "pkg/from/tests"
//
//     func TestFromThing(t *testing.T) {
//             tests.TestFromThing(t)
//     }
//
// You probably need, in a separate file, an init or TestMain function
// that sets up a global variable to tell the tests what object is
// being tested, or how to construct that object.
package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"go/ast"
	"go/build"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	app := cli.NewApp()
	app.Usage = "Copy reusable tests to a new package"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "output",
			Usage: "write extracted tests to a file",
		},
		cli.StringFlag{
			Name:  "package",
			Usage: "put generated file in this package",
			Value: "test_test",
		},
		cli.StringSliceFlag{
			Name:  "except",
			Usage: "do not copy these functions",
		},
	}
	app.HideHelp = true
	app.HideVersion = true
	app.Action = func(context *cli.Context) {
		var err error
		pkgs := context.Args()
		pkgOut := context.String("package")
		except := context.StringSlice("except")
		output := os.Stdout
		outputFile := context.String("output")
		if outputFile != "" {
			output, err = os.Create(outputFile)
			defer output.Close()
		}
		if err == nil {
			err = extractTests(pkgs, except, pkgOut, output)
		}
		if err != nil {
			fmt.Printf("%v\n", err)
		}
	}

	app.RunAndExitOnError()
}

func extractTests(pkgs []string, except []string, pkgOut string, output io.Writer) error {
	here, err := os.Getwd()
	if err != nil {
		return err
	}
	fset := token.NewFileSet()
	result := &ast.File{}
	result.Name = &ast.Ident{
		Name: pkgOut,
	}
	result.Imports = append(result.Imports, &ast.ImportSpec{
		Path: &ast.BasicLit{
			Kind:  token.STRING,
			Value: "\"testing\"",
		},
	})

	for _, pkgName := range pkgs {
		pkg, err := build.Import(pkgName, here, 0)
		if err != nil {
			return err
		}
		result.Imports = append(result.Imports, &ast.ImportSpec{
			Path: &ast.BasicLit{
				Kind:  token.STRING,
				Value: "\"" + pkg.ImportPath + "\"",
			},
		})
		pkgFiles := token.NewFileSet()
		for _, src := range pkg.GoFiles {
			qsrc := filepath.Join(pkg.Dir, src)
			sfile, err := parser.ParseFile(pkgFiles, qsrc, nil, 0)
			if err != nil {
				return err
			}
			err = copyTests(result, sfile, except, pkg)
			if err != nil {
				return err
			}
		}
	}

	importDecls := make([]ast.Decl, len(result.Imports))
	for i, spec := range result.Imports {
		importDecls[i] = &ast.GenDecl{
			Tok:   token.IMPORT,
			Specs: []ast.Spec{spec},
		}
	}
	result.Decls = append(importDecls, result.Decls...)

	return format.Node(output, fset, result)
}

func copyTests(result, sfile *ast.File, except []string, pkg *build.Package) error {
	for _, decl := range sfile.Decls {
		funcDecl, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		// Test functions never have receivers
		if funcDecl.Recv != nil {
			continue
		}
		// Ignore specifically excluded functions
		// (...why, Go, why?)
		excluded := false
		for _, name := range except {
			if funcDecl.Name.Name == name {
				excluded = true
			}
		}
		if excluded {
			continue
		}
		// Test functions never return anything (see below)
		if !emptyFieldList(funcDecl.Type.Results) {
			continue
		}
		// We are looking for:
		//   Test...(*testing.T)
		//   Benchmark...(*testing.B)
		//   Example...()
		var param *ast.Field
		var args []ast.Expr
		if strings.HasPrefix(funcDecl.Name.Name, "Test") {
			if !takesATesting("T", funcDecl.Type.Params) {
				continue
			}
			param = &ast.Field{
				Names: []*ast.Ident{ast.NewIdent("t")},
				Type:  funcDecl.Type.Params.List[0].Type,
			}
			args = []ast.Expr{ast.NewIdent("t")}
		} else if strings.HasPrefix(funcDecl.Name.Name, "Benchmark") {
			if !takesATesting("B", funcDecl.Type.Params) {
				continue
			}
			param = &ast.Field{
				Names: []*ast.Ident{ast.NewIdent("b")},
				Type:  funcDecl.Type.Params.List[0].Type,
			}
			args = []ast.Expr{ast.NewIdent("b")}
		} else if strings.HasPrefix(funcDecl.Name.Name, "Example") {
			if !emptyFieldList(funcDecl.Type.Params) {
				continue
			}
		} else {
			continue
		}

		call := &ast.CallExpr{
			Fun: &ast.SelectorExpr{
				X:   ast.NewIdent(pkg.Name),
				Sel: funcDecl.Name,
			},
			Args: args,
		}
		stmts := []ast.Stmt{&ast.ExprStmt{X: call}}
		funcType := &ast.FuncType{}
		if param != nil {
			funcType.Params = &ast.FieldList{
				List: []*ast.Field{param},
			}
		}
		newFunc := &ast.FuncDecl{
			Name: funcDecl.Name,
			Type: funcType,
			Body: &ast.BlockStmt{List: stmts},
		}
		result.Decls = append(result.Decls, newFunc)
	}
	return nil
}

func emptyFieldList(fl *ast.FieldList) bool {
	if fl == nil {
		return true
	}
	return len(fl.List) == 0
}

func takesATesting(t string, fl *ast.FieldList) bool {
	if fl == nil || len(fl.List) != 1 {
		return false
	}
	param := fl.List[0]
	star, ok := param.Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	if pkg.Name != "testing" {
		return false
	}
	if sel.Sel.Name != t {
		return false
	}
	return true
}
