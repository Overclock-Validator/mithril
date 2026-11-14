package dashboard

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
	"time"
)

func TestRPCActivityAcceptsInstrumentedMethods(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "../rpcserver/monitoring.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var methods []RPCMethod
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok || len(spec.Names) != 1 || spec.Names[0].Name != "instrumentedRPCMethods" || len(spec.Values) != 1 {
			return true
		}
		registry, ok := spec.Values[0].(*ast.CompositeLit)
		if !ok {
			t.Fatal("instrumented RPC list is not a literal")
		}
		for _, entry := range registry.Elts {
			key, ok := entry.(*ast.BasicLit)
			if !ok || key.Kind != token.STRING {
				t.Fatal("instrumented RPC name is not a string literal")
			}
			name, err := strconv.Unquote(key.Value)
			if err != nil {
				t.Fatal(err)
			}
			methods = append(methods, RPCMethod{Name: name, DurationBuckets: [6]string{"0", "0", "0", "0", "0", "0"}})
		}
		return false
	})
	if len(methods) == 0 {
		t.Fatal("instrumented RPC list was not found or has no methods")
	}
	at := time.Now().UTC()
	activity := publicRPCActivity(&RPC{SampledAt: at, MonitoringStartedAt: at, Methods: methods}, at)
	if activity == nil || len(activity.Methods) != len(methods) {
		t.Fatalf("dashboard does not accept all %d instrumented RPC methods", len(methods))
	}
}
