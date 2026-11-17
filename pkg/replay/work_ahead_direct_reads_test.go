package replay

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The code an early execution runs may read account state in two ways only:
// through the slot context's Accounts, which for an early execution is the
// recording view, or from the parent bank, which does not change while the
// block's transactions run. Its result is adopted on the strength of what the
// view recorded, so a new shortcut that reaches state some other way would
// leave normal replay correct and make adoption silently unsafe.
//
// This test lists every place in the execution code that reaches past
// Accounts: the parent-bank helpers, and anything called on a slot context's
// AccountsDb, UnrootedRead or ParentAccts. Each is classified below. Adding,
// moving or removing one fails the test until the table is updated, which is
// the moment to check the new read against the two rules above (and against
// sealevel.NewSealedSlotCtx's comment). Changing a count is a review item,
// not a formality.
//
//	parent   reads the parent bank's version of an account: constant for the
//	         block, so the same in an early execution and at the turn
//	cache    program cache lookup, matched against program bytes that were
//	         themselves read through Accounts
//	publish  writes to a process-wide cache; must not be reachable from an
//	         early execution (guarded by ExecutionCtx.Speculative, or only in
//	         loader instructions, which are never executed early)
//	commit   runs at the transaction's turn or at block level, never inside an
//	         early execution
var directStateReads = map[string]string{
	"fees/fees.go DistributeTxFees GetAccountFromAccountsDb":                                         "1 commit",
	"fees/fees.go DistributeTxFees ParentAccts.SetAccountWithoutLock":                                "1 commit",
	"fees/payer.go loadPayer GetAccountFromAccountsDb":                                               "1 parent",
	"rent/rent.go CollectRentEagerly AccountsDb.KeysBetweenPrefixes":                                 "1 commit",
	"rent/rent.go collectRent AccountsDb.GetAccount":                                                 "1 commit",
	"replay/accounts.go loadAndValidateTxAccts GetAccountFromAccountsDb":                             "1 parent",
	"replay/accounts.go loadAndValidateTxAcctsSimd186 GetAccountFromAccountsDb":                      "2 parent",
	"replay/accounts.go programDataLen GetAccountDataLenFromAccountsDb":                              "1 parent",
	"replay/accounts.go programDataLen ParentAccountReadOnly":                                        "1 parent",
	"replay/transaction.go handleFailedTxWithTimings GetAccountFromAccountsDb":                       "1 commit",
	"replay/transaction_processing_pure.go feeOnlyRollbackAccountsDataSize GetAccountFromAccountsDb": "1 parent",
	"sealevel/blockhash_nonce.go IsTransactionAgeValid GetAccountFromAccountsDb":                     "1 parent",
	"sealevel/blockhash_nonce.go MaybeAdvanceNonceAccountForFailedTx GetAccountFromAccountsDb":       "1 commit",
	"sealevel/bpf_loader.go BpfLoaderProgramExecute GetAccountFromAccountsDb":                        "2 parent",
	"sealevel/bpf_loader.go BpfLoaderProgramExecute getProgramAccountSource":                         "1 parent",
	"sealevel/bpf_loader.go UpgradeableLoaderClose AccountsDb.RemoveProgramFromCache":                "1 publish",
	"sealevel/bpf_loader.go addProgramToCache AccountsDb.AddProgramToCache":                          "1 publish",
	"sealevel/bpf_loader.go executeProgramFromSource AccountsDb.MaybeGetProgramFromCache":            "1 cache",
	"sealevel/execution_ctx.go GetAccountDataLenFromAccountsDb GetAccountFromAccountsDb":             "1 parent",
	"sealevel/execution_ctx.go GetAccountFromAccountsDb AccountsDb.GetAccount":                       "1 parent",
	"sealevel/execution_ctx.go GetAccountFromAccountsDb UnrootedRead.GetAccount":                     "1 parent",
	"sealevel/execution_ctx.go GetParentAccount ParentAccts.GetAccountWithoutLock":                   "1 parent",
	"sealevel/execution_ctx.go ParentAccountReadOnly GetAccountFromAccountsDb":                       "1 parent",
	"sealevel/execution_ctx.go ParentAccountReadOnly getAccountFromAccountsDbReadOnly":               "1 parent",
	"sealevel/execution_ctx.go getAccountFromAccountsDbReadOnly getProgramAccountSource":             "1 parent",
	"sealevel/execution_ctx.go getProgramAccountSource GetAccountFromAccountsDb":                     "1 parent",
	"sealevel/loader_v4.go LoaderV4Execute GetAccountFromAccountsDb":                                 "1 parent",
	"sealevel/loader_v4.go LoaderV4ProcessDeploy AccountsDb.AddProgramToCache":                       "1 publish",
	"sealevel/loader_v4.go LoaderV4ProcessRetract AccountsDb.RemoveProgramFromCache":                 "1 publish",
}

// directReadHelpers are the SlotCtx methods that read the parent bank.
var directReadHelpers = map[string]bool{
	"GetAccountFromAccountsDb": true, "ParentAccountReadOnly": true, "GetAccountDataLenFromAccountsDb": true,
	"GetParentAccount": true, "getProgramAccountSource": true, "getAccountFromAccountsDbReadOnly": true,
}

// directReadFields are the SlotCtx fields that lead to state without going
// through Accounts.
var directReadFields = map[string]bool{"AccountsDb": true, "UnrootedRead": true, "ParentAccts": true}

// directReadScope is the code an early execution can run: whole packages, and
// the transaction-level files of this one (block-level replay code uses the
// database freely and never runs inside an early execution).
var directReadScope = []string{
	"../sealevel/*.go", "../fees/*.go", "../rent/*.go", "../sbpf/*.go", "../sbpf/loader/*.go",
	"accounts.go", "transaction.go", "transaction_processing_pure.go", "commit.go",
}

func collectDirectStateReads(t *testing.T) map[string]int {
	t.Helper()
	found := map[string]int{}
	fset := token.NewFileSet()
	for _, pattern := range directReadScope {
		files, err := filepath.Glob(pattern)
		if err != nil || len(files) == 0 {
			t.Fatalf("scope pattern %q matches nothing (%v): update directReadScope", pattern, err)
		}
		for _, path := range files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			name := filepath.Base(filepath.Dir(path)) + "/" + filepath.Base(path)
			if !strings.Contains(path, "/") {
				name = "replay/" + path
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					what := ""
					if directReadHelpers[sel.Sel.Name] {
						what = sel.Sel.Name
					} else if inner, ok := sel.X.(*ast.SelectorExpr); ok && directReadFields[inner.Sel.Name] {
						what = inner.Sel.Name + "." + sel.Sel.Name
					}
					if what != "" {
						found[name+" "+fn.Name.Name+" "+what]++
					}
					return true
				})
			}
		}
	}
	return found
}

func TestDirectStateReadsAreClassified(t *testing.T) {
	found := collectDirectStateReads(t)
	if os.Getenv("MITHRIL_PRINT_DIRECT_READS") == "1" {
		keys := make([]string, 0, len(found))
		for key := range found {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Printf("\t%q: \"%d \",\n", key, found[key])
		}
	}
	for key, count := range found {
		want, ok := directStateReads[key]
		if !ok {
			t.Errorf("unclassified direct state read: %s (x%d). An early execution's result is trusted only for what its recording view saw; classify this read in directStateReads after checking it", key, count)
			continue
		}
		if !strings.HasPrefix(want, fmt.Sprintf("%d ", count)) {
			t.Errorf("%s: %d call sites, table says %q; re-check the function and update the count", key, count, want)
		}
	}
	for key := range directStateReads {
		if _, ok := found[key]; !ok {
			t.Errorf("%s is classified but no longer exists; remove it from directStateReads", key)
		}
	}
}
