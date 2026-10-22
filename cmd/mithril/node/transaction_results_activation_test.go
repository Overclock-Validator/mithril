package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestSignatureStatusesNodeConfig(t *testing.T) {
	require.Equal(t, "false", Run.Flags().Lookup("rpc-signature-statuses").DefValue)
	for _, test := range []struct {
		cluster, mode string
		port          int
		valid         bool
	}{
		{"alpenglow", "verifying", 8899, true},
		{"alpenglow", "validator", 8899, false},
		{"mainnet-beta", "verifying", 8899, false},
		{"alpenglow", "", 8899, false},
		{"alpenglow", "verifying", 0, false},
		{"alpenglow", "verifying", -1, false},
		{"alpenglow", "verifying", 65536, false},
	} {
		err := validateSignatureStatuses(true, test.cluster, test.mode, test.port)
		require.Equal(t, test.valid, err == nil)
		require.NoError(t, validateSignatureStatuses(false, test.cluster, test.mode, test.port))
	}
	cache, session, err := newNodeTransactionResults(false, "mainnet-beta", "validator", 0, nil, nil)
	require.NoError(t, err, "disabled initialization must not parse state")
	require.Nil(t, cache)
	require.Nil(t, session)
	_, _, err = newNodeTransactionResults(true, "alpenglow", "verifying", 8899, nil, nil)
	require.Error(t, err, "enabled status cannot start with an invented parent")
	s := &state.MithrilState{ManifestParentSlot: 8, ManifestRecentBlockhashes: []state.BlockhashEntry{{Blockhash: (solana.Hash{8}).String()}}}
	cache, session, err = newNodeTransactionResults(true, "alpenglow", "verifying", 8899, s,
		&replay.ResumeState{ParentSlot: 20, LastBlockhash: [32]byte{20}})
	require.NoError(t, err)
	view, err := cache.Lookup(nil)
	require.NoError(t, err)
	require.Equal(t, txstatus.ResultBankID{Slot: 20, Blockhash: solana.Hash{20}}, view.Tip)
	require.Equal(t, view.Tip, view.Root)
	session.Invalidate()
	_, err = cache.Lookup(nil)
	require.ErrorIs(t, err, txstatus.ErrResultUnavailable, "replay and RPC borrow the same cache")
}

func TestSignatureStatusesNodeShutdown(t *testing.T) {
	joined, err := stopNodeOptionalRPC(nil, nil, nil, nil)
	require.True(t, joined)
	require.NoError(t, err, "default profile must preserve its existing shutdown path")
	for _, history := range []bool{false, true} {
		t.Run(map[bool]string{false: "status_only", true: "history_and_status"}[history], func(t *testing.T) {
			cache, session, err := newNodeTransactionResults(true, "alpenglow", "verifying", 8899, nil,
				&replay.ResumeState{ParentSlot: 20, LastBlockhash: [32]byte{20}})
			require.NoError(t, err)
			joined, err := stopNodeOptionalRPC(nil, nil, nil, session)
			require.False(t, joined)
			require.Error(t, err)
			store, writer, err := openNodeTransactionHistory(transactionHistoryConfig{Enabled: history,
				RetentionSlots: 16, MaxMiB: 80, ReserveMiB: 1}, "alpenglow", "verifying", 8899, t.TempDir(), (solana.Hash{1}).String())
			require.NoError(t, err)
			server := rpcserver.NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}, solana.Hash{1})
			server.InitTransactionHistory(store)
			require.NoError(t, server.InitTransactionResults(cache))
			joined, err = stopNodeOptionalRPC(server, writer, store, session)
			require.True(t, joined)
			require.NoError(t, err)
			require.Error(t, server.Start(), "status-only shutdown must own and close the listener too")
			_, err = cache.Lookup(nil)
			require.NoError(t, err, "RPC shutdown does not replace or own the borrowed cache")
		})
	}
}

// This pins the actual node owner wiring; it does not start a validator or prove
// a live retry. Behavioral seed, handler and recovery tests cover the helpers.
func TestSignatureStatusesNodeWiring(t *testing.T) {
	path := os.Getenv("RESULT_ACTIVATION_NODE")
	if path == "" {
		path = "node.go"
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	require.NoError(t, err)
	positions := make(map[string]token.Pos)
	sessionFields := 0
	ast.Inspect(file, func(n ast.Node) bool {
		if fn, ok := n.(*ast.FuncDecl); ok && fn.Name.Name != "runLive" {
			return false
		}
		if call, ok := n.(*ast.CallExpr); ok {
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				for _, want := range []string{"newNodeTransactionResults", "runReplayWithRecovery", "stopNodeOptionalRPC"} {
					if fn.Name == want {
						require.Zero(t, positions[want], "owner call must occur once")
						positions[want] = call.Pos()
					}
				}
			case *ast.SelectorExpr:
				if receiver, ok := fn.X.(*ast.Ident); ok &&
					((receiver.Name == "rpcServer" && (fn.Sel.Name == "InitTransactionResults" || fn.Sel.Name == "Start")) ||
						(receiver.Name == "accountsDb" && fn.Sel.Name == "CloseDb")) {
					positions[fn.Sel.Name] = call.Pos()
				}
			}
		}
		if field, ok := n.(*ast.KeyValueExpr); ok {
			if name, ok := field.Key.(*ast.Ident); ok && name.Name == "TransactionResults" {
				value, ok := field.Value.(*ast.Ident)
				require.True(t, ok)
				require.Equal(t, "resultSession", value.Name)
				sessionFields++
			}
		}
		return true
	})
	require.Equal(t, 1, sessionFields)
	order := []string{"newNodeTransactionResults", "InitTransactionResults", "Start", "runReplayWithRecovery", "stopNodeOptionalRPC", "CloseDb"}
	for i, name := range order {
		require.NotZero(t, positions[name], name)
		if i > 0 {
			require.Greater(t, positions[name], positions[order[i-1]], name)
		}
	}
}
