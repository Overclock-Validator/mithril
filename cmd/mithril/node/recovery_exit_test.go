package node

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/replay"
)

func TestLiveRunExitError(t *testing.T) {
	stall := fmt.Errorf("fetch: %w", replay.ErrBlockFetchStalled)
	fault := errors.New("vote history could not be sealed")
	stateWrite := errors.New("shutdown state write failed")
	for _, test := range []struct {
		name      string
		run       error
		cleanup   error
		cancelled bool
		rpcActive bool
		want      error
		retry     bool
	}{
		{name: "clean stall", run: stall, want: stall, retry: true},
		{name: "close failure", run: stall, cleanup: fault, want: fault},
		{name: "late safety fault", run: stall, cleanup: errors.Join(nil, fault), want: fault},
		{name: "state save failure", run: stall, cleanup: stateWrite, want: stateWrite},
		{name: "state save failure after clean cancellation", cleanup: stateWrite, cancelled: true, want: stateWrite},
		{name: "cancelled stall stays a failure", run: stall, cancelled: true, want: stall},
		{name: "cancel does not hide cleanup failure", run: stall, cleanup: fault, cancelled: true, want: fault},
		{name: "RPC readers not joined stays a failure", run: stall, rpcActive: true, want: stall},
		{name: "other error", run: fault, want: fault},
		{name: "cancellation does not hide other error", run: fault, cancelled: true, want: fault},
		{name: "clean cancellation", cancelled: true},
		{name: "successful run"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := liveRunExitError(test.run, test.cleanup, test.cancelled, test.rpcActive)
			if !errors.Is(got, test.want) {
				t.Fatalf("exit error = %v, want %v", got, test.want)
			}
			if test.run != nil && !errors.Is(got, test.run) {
				t.Errorf("exit error = %v, lost original cause %v", got, test.run)
			}
			if retry := errors.Is(got, ErrRetryableBlockFetchStall); retry != test.retry {
				t.Errorf("retry eligible = %t, want %t", retry, test.retry)
			}
		})
	}
}

// TestRunLiveForwardsTerminalErrors checks the wiring that a classifier unit test
// cannot exercise: the real runLive must forward replay and persistence errors.
// This structural check is not a substitute for the whole-runtime exit test.
func TestRunLiveForwardsTerminalErrors(t *testing.T) {
	if Run.RunE == nil || Run.Run != nil {
		t.Error("run command must use Cobra RunE to forward terminal errors")
	}
	file, err := parser.ParseFile(token.NewFileSet(), "node.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var run *ast.FuncDecl
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "runLive" {
			run = fn
		}
	}
	if run == nil {
		t.Fatal("runLive not found")
	}
	last, ok := run.Body.List[len(run.Body.List)-1].(*ast.ReturnStmt)
	if !ok || len(last.Results) != 1 {
		t.Fatal("runLive must return its replay result after persistence")
	}
	field, ok := last.Results[0].(*ast.SelectorExpr)
	if !ok || field.Sel.Name != "Error" {
		t.Errorf("runLive terminal return drops ReplayResult.Error")
	} else if name, ok := field.X.(*ast.Ident); !ok || name.Name != "result" {
		t.Errorf("runLive terminal return does not forward result.Error")
	}
	stateWrites := 0
	ast.Inspect(run.Body, func(n ast.Node) bool {
		branch, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		init, ok := branch.Init.(*ast.AssignStmt)
		if !ok || len(init.Rhs) != 1 {
			return true
		}
		call, ok := init.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || method.Sel.Name != "UpdateOnShutdown" {
			return true
		}
		stateWrites++
		forwards := false
		ast.Inspect(branch.Body, func(n ast.Node) bool {
			assign, ok := n.(*ast.AssignStmt)
			if ok && len(assign.Lhs) == 1 {
				name, ok := assign.Lhs[0].(*ast.Ident)
				if ok && name.Name == "cleanupErr" {
					forwards = true
				}
			}
			return true
		})
		if !forwards {
			t.Error("UpdateOnShutdown failure is not forwarded to cleanupErr")
		}
		return true
	})
	if stateWrites != 2 {
		t.Fatalf("checked %d shutdown state writes, want 2", stateWrites)
	}
}

// TestReplayShutdownResaveAllowsFinalRetry checks the second cancellation-save
// call site. A failed re-save must not tell runLive that persistence succeeded.
func TestReplayShutdownResaveAllowsFinalRetry(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "../../../pkg/replay/block.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var saves []*ast.IfStmt
	ast.Inspect(file, func(n ast.Node) bool {
		branch, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		init, ok := branch.Init.(*ast.AssignStmt)
		if !ok || len(init.Rhs) != 1 {
			return true
		}
		call, ok := init.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "onCancelWriteState" {
			saves = append(saves, branch)
		}
		return true
	})
	if len(saves) != 2 {
		t.Fatalf("found %d cancellation-save calls, want 2", len(saves))
	}
	for _, stmt := range saves[1].Body.List {
		assign, ok := stmt.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			continue
		}
		field, fieldOK := assign.Lhs[0].(*ast.SelectorExpr)
		value, valueOK := assign.Rhs[0].(*ast.Ident)
		if fieldOK && field.Sel.Name == "StateWrittenOnCancel" && valueOK && value.Name == "false" {
			return
		}
	}
	t.Fatal("failed shutdown re-save must clear StateWrittenOnCancel for runLive's final save")
}
