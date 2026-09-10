package snapshot

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
)

// TestBuildAccountsDbAutoIncrementalManifestReadPreservesRetryError guards a
// subtle Go scoping requirement in the retry loop. A short declaration here
// creates a loop-local err, leaving the function-scoped err nil when every
// manifest read fails and allowing bootstrap to continue after retry exhaustion.
func TestBuildAccountsDbAutoIncrementalManifestReadPreservesRetryError(t *testing.T) {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate retry regression test source")
	}
	sourceFile := filepath.Join(filepath.Dir(testFile), "build_db_with_incr.go")
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, sourceFile, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", sourceFile, err)
	}

	var retryLoop *ast.RangeStmt
	ast.Inspect(parsed, func(node ast.Node) bool {
		function, ok := node.(*ast.FuncDecl)
		if !ok || function.Name.Name != "BuildAccountsDbAuto" {
			return true
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			loop, ok := node.(*ast.RangeStmt)
			if !ok {
				return true
			}
			attempts, ok := loop.X.(*ast.Ident)
			if ok && attempts.Name == "maxIncrRetries" {
				retryLoop = loop
				return false
			}
			return true
		})
		return false
	})
	if retryLoop == nil {
		t.Fatal("incremental snapshot retry loop not found")
	}

	var manifestRead *ast.AssignStmt
	ast.Inspect(retryLoop.Body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for _, expression := range assignment.Rhs {
			call, ok := expression.(*ast.CallExpr)
			if !ok {
				continue
			}
			callee, ok := call.Fun.(*ast.Ident)
			if ok && callee.Name == "UnmarshalManifestFromSnapshot" {
				manifestRead = assignment
				return false
			}
		}
		return true
	})
	if manifestRead == nil {
		t.Fatal("incremental manifest read not found in retry loop")
	}
	if manifestRead.Tok != token.ASSIGN {
		t.Fatalf(
			"incremental manifest read uses %s; it must assign to the function-scoped retry error",
			manifestRead.Tok,
		)
	}

	var assignsManifest, assignsRetryError bool
	for _, expression := range manifestRead.Lhs {
		identifier, ok := expression.(*ast.Ident)
		if !ok {
			continue
		}
		switch identifier.Name {
		case "incrementalManifestCopy":
			assignsManifest = true
		case "err":
			assignsRetryError = true
		}
	}
	if !assignsManifest || !assignsRetryError {
		t.Fatalf("incremental manifest read must assign both manifest and retry error; lhs=%v", manifestRead.Lhs)
	}
}
