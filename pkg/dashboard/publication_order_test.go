package dashboard

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"sync/atomic"
	"testing"
)

// cacheReadOrder checks the handler's initial selection; nested admission
// callbacks recheck that selection rather than replacing its two reads.
func cacheReadOrder(t *testing.T, file, receiver, method string) []string {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var reads []string
	found := false
	for _, declaration := range parsed.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != method || fn.Recv == nil {
			continue
		}
		pointer, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		name, ok := pointer.X.(*ast.Ident)
		if !ok || name.Name != receiver {
			continue
		}
		found = true
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if _, ok := node.(*ast.FuncLit); ok {
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			method, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			owner, ok := method.X.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if method.Sel.Name == "Load" && (owner.Sel.Name == "latest" || owner.Sel.Name == "failure") {
				reads = append(reads, owner.Sel.Name)
			}
			if method.Sel.Name == "lookup" && owner.Sel.Name == "cache" {
				reads = append(reads, "latest")
			}
			return true
		})
	}
	if !found {
		t.Fatalf("missing %s.%s in %s", receiver, method, file)
	}
	return reads
}

// retiredPublicationAllowed forces a successful replacement exactly between
// the two reads. Old data is failed before replacement and retired afterwards;
// there is no instant at which it is available during this request.
func retiredPublicationAllowed(order []string) bool {
	type publication struct{ generation int }
	old, next := &publication{generation: 1}, &publication{generation: 2}
	var latest atomic.Pointer[publication]
	var failure atomic.Pointer[struct{}]
	latest.Store(old)
	failure.Store(&struct{}{})
	firstRead, replaced, done := make(chan struct{}), make(chan struct{}), make(chan bool)
	go func() {
		var p *publication
		var f *struct{}
		for i, field := range order {
			if field == "latest" {
				p = latest.Load()
			} else {
				f = failure.Load()
			}
			if i == 0 {
				close(firstRead)
				<-replaced
			}
		}
		done <- p == old && f == nil
	}()
	<-firstRead
	latest.Store(next)
	failure.Store(nil)
	close(replaced)
	return <-done
}

func TestPublicationReadersCannotMixRetiredDataWithClearedFailure(t *testing.T) {
	if !retiredPublicationAllowed([]string{"latest", "failure"}) {
		t.Fatal("negative control did not reproduce the invalid interleaving")
	}
	for _, test := range []struct{ file, receiver, method string }{
		{"held_schedule_source.go", "heldScheduleSource", "serveHTTP"},
		{"held_schedule_reader.go", "heldScheduleReader", "serveHTTP"},
		{"rooted_history_reader.go", "rootedHistoryReader", "serveHTTP"},
		{"cache_detail_reader.go", "cacheDetailReader", "lookup"},
		{"storage_detail_reader.go", "storageDetailReader", "lookup"},
		{"production_detail_reader.go", "productionDetailReader", "lookup"},
		{"network_detail_reader.go", "networkDetailReader", "lookup"},
		{"replay_detail_source.go", "replayDetailSource", "serveHTTP"},
	} {
		t.Run(test.receiver, func(t *testing.T) {
			order := cacheReadOrder(t, test.file, test.receiver, test.method)
			if !reflect.DeepEqual(order, []string{"failure", "latest"}) {
				t.Fatalf("publication/failure read order %v can mix publication lifetimes", order)
			}
			if retiredPublicationAllowed(order) {
				t.Fatal("a retired publication became available")
			}
		})
	}
	if order := cacheReadOrder(t, "network_node_reader.go", "networkDetailReader", "lookupNode"); len(order) != 0 {
		t.Fatalf("node projection must use its caller's failure/publication snapshot, not reload %v", order)
	}
}
