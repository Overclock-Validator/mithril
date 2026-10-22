package rpcserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestSignatureStatusesForwardCacheReselection(t *testing.T) {
	for _, mode := range []string{"stable", "once", "once-direct", "twice", "cancel", "invalidate", "reset", "unwind", "lineage", "recovery"} {
		t.Run(mode, func(t *testing.T) {
			server, block := transactionHistoryReadFixture(t)
			metadata, err := transactionhistory.UnmarshalMetadata(block.Records[0].Metadata)
			require.NoError(t, err)
			metadata.Error = json.RawMessage(`{"InstructionError":[1,{"Custom":1}]}`)
			block.Records[0].Metadata, err = metadata.MarshalBinary()
			require.NoError(t, err)
			require.NoError(t, server.transactionHistory.WriteBlock(block))
			parent := txstatus.ResultBankID{Slot: 20, Blockhash: solana.Hash(block.ID.Blockhash)}
			server.resultCache, err = txstatus.NewResultCache(parent)
			require.NoError(t, err)
			if mode == "unwind" {
				require.NoError(t, server.resultCache.Publish(txstatus.ResultBank{
					ID: txstatus.ResultBankID{Slot: 21, Blockhash: solana.Hash{21}}, Parent: parent, Complete: true}))
			}
			bank := server.rootedBank.Load()
			version, stable := server.acctsDb.CommittedAccountVersion()
			require.True(t, stable)
			durable := server.acctsDb.DurableThrough()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			hook := historyReselectionHook(t, server, func(read int) error {
				if mode == "stable" || (read > 1 && mode != "twice") {
					return nil
				}
				switch mode {
				case "cancel":
					cancel()
				case "invalidate":
					server.resultCache.Invalidate()
					return nil
				case "recovery":
					server.SetHistoryRecoveryPending(true)
					return nil
				case "reset":
					require.NoError(t, server.resultCache.Reset(parent))
				case "unwind":
					require.NoError(t, server.resultCache.Unwind(parent))
				case "lineage":
					require.ErrorIs(t, server.resultCache.Publish(txstatus.ResultBank{
						ID:     txstatus.ResultBankID{Slot: 22, Blockhash: solana.Hash{22}},
						Parent: txstatus.ResultBankID{Slot: 19, Blockhash: solana.Hash{19}}, Complete: true}), txstatus.ErrResultLineage)
					return nil
				}
				view, err := server.resultCache.Lookup(nil)
				if err != nil {
					return err
				}
				return server.resultCache.Publish(txstatus.ResultBank{
					ID:     txstatus.ResultBankID{Slot: 22 + uint64(read), Blockhash: solana.Hash{byte(22 + read)}},
					Parent: view.Tip, Complete: true})
			})
			signatures := []solana.Signature{block.Records[0].FirstSignature}
			var view txstatus.ResultView
			if mode == "once-direct" {
				view, err = server.readSignatureStatusesOnce(ctx, signatures, true, transactionhistory.NewReadBudget())
				require.Equal(t, errHistoryViewChanged, err)
				require.Equal(t, txstatus.ResultView{}, view, "a stale view must never escape the Once fence")
				require.Equal(t, 1, hook.reads)
				return
			}
			view, err = server.readSignatureStatuses(ctx, signatures, true)
			if mode != "stable" && mode != "once" {
				if mode == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
				}
				require.Equal(t, txstatus.ResultView{}, view)
				reads := 1
				if mode == "twice" {
					reads = 2
				}
				require.Equal(t, reads, hook.reads)
				return
			}
			require.NoError(t, err)
			current, stable := server.acctsDb.CommittedAccountVersion()
			require.True(t, stable)
			require.Equal(t, version, current)
			require.Same(t, bank, server.rootedBank.Load())
			require.Equal(t, durable, server.acctsDb.DurableThrough())
			require.Len(t, view.Results, 1)
			require.True(t, view.Results[0].Found && view.Results[0].Finalized)
			require.Equal(t, uint64(20), view.Results[0].Slot)
			require.JSONEq(t, string(metadata.Error), string(view.Results[0].Error))
			latest, err := server.resultCache.Lookup(nil)
			require.NoError(t, err)
			require.Equal(t, latest.Generation, view.Generation)
			require.Equal(t, latest.Tip, view.Tip)
			require.Equal(t, latest.Root, view.Root)
			require.Equal(t, map[string]int{"stable": 1, "once": 2}[mode], hook.reads)
		})
	}
}

// Pruning recent outcomes must not hide a canonical retained receipt when a
// later bank exceeds bounded capture.
func TestRetainedPositiveStatusAfterDenseBankAndPruning(t *testing.T) {
	for _, name := range []string{"no-gap-retained-fallback", "gap-before-prune", "gap-unknown-refuses", "gap-pruned-positive", "gap-mixed-positive", "gap-mixed-unknown", "gap-recent-only", "gap-latched", "gap-lineage", "gap-recovery", "gap-root-mismatch", "gap-cache-change"} {
		t.Run(name, func(t *testing.T) {
			server, block := transactionHistoryReadFixture(t)
			metadata, err := transactionhistory.UnmarshalMetadata(block.Records[0].Metadata)
			require.NoError(t, err)
			metadata.Error = json.RawMessage(`{"InstructionError":[1,{"Custom":1}]}`)
			block.Records[0].Metadata, err = metadata.MarshalBinary()
			require.NoError(t, err)
			require.NoError(t, server.transactionHistory.WriteBlock(block))
			signature := solana.Signature(block.Records[0].FirstSignature)
			server.resultCache, err = txstatus.NewResultCache(txstatus.ResultBankID{
				Slot: 10, Blockhash: solana.Hash(block.PreviousBlockhash),
			})
			require.NoError(t, err)
			old := txstatus.ResultBankID{Slot: 20, Blockhash: solana.Hash(block.ID.Blockhash)}
			require.NoError(t, server.resultCache.Publish(txstatus.ResultBank{ID: old,
				Parent:   txstatus.ResultBankID{Slot: 10, Blockhash: solana.Hash(block.PreviousBlockhash)},
				Complete: true, Results: []txstatus.SignatureResult{{Signature: signature, Error: metadata.Error}}}))
			require.NoError(t, server.resultCache.Root(old))
			end := uint64(320)
			if name == "gap-before-prune" {
				end = 319
			}
			parent := old
			for slot := uint64(21); slot <= end; slot++ {
				id := txstatus.ResultBankID{Slot: slot, Blockhash: solana.Hash{byte(slot), byte(slot >> 8), 1}}
				bank := txstatus.ResultBank{ID: id, Parent: parent, Complete: true}
				if slot == 100 && name != "no-gap-retained-fallback" {
					bank.Results = make([]txstatus.SignatureResult, 29222)
					require.Greater(t, len(bank.Results), txstatus.MaxResultBankEntries)
					require.ErrorIs(t, server.resultCache.Publish(bank), txstatus.ErrResultUnavailable)
				} else {
					require.NoError(t, server.resultCache.Publish(bank))
				}
				require.NoError(t, server.resultCache.Root(id))
				parent = id
			}
			_, err = server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: end,
				Delta: []*accounts.Account{{Key: solana.PublicKey{201}, Lamports: end}}}}, end, nil, nil)
			require.NoError(t, err)
			server.SetRootedBankState(end, 9, 1)
			// The authoritative root and cache identity are stable: this is not
			// a publication race or a globally latched unavailable cache.
			latest, err := server.resultCache.Lookup(nil)
			require.NoError(t, err)
			require.Equal(t, end, latest.Root.Slot)
			require.Equal(t, latest.Root, latest.Tip)
			if name == "gap-unknown-refuses" {
				_, err := server.readSignatureStatuses(t.Context(), []solana.Signature{{99}}, true)
				require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
				return
			}
			if name == "gap-pruned-positive" {
				_, err := server.resultCache.Lookup([]solana.Signature{signature})
				require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
				_, _, found, err := server.readTransactionHistoryTransaction(t.Context(), block.Records[0].FirstSignature)
				require.NoError(t, err)
				require.True(t, found, "the exact canonical historical receipt remains retained")
				version, stable := server.acctsDb.CommittedAccountVersion()
				require.True(t, stable)
				historical := latest
				historical.Results = make([]txstatus.CachedResult, 1)
				err = server.fillHistoricalStatuses(t.Context(), []solana.Signature{signature}, &historical,
					historyReadStamp{bank: server.rootedBank.Load(), version: version, durable: end},
					transactionhistory.NewReadBudget())
				require.NoError(t, err)
				require.True(t, historical.Results[0].Found)
				require.True(t, historical.Results[0].Finalized)
				require.JSONEq(t, string(metadata.Error), string(historical.Results[0].Error))
			}
			query := []solana.Signature{signature}
			history, refuse := true, false
			var hook *historyReselectionFS
			switch name {
			case "gap-mixed-positive":
				recent := txstatus.ResultBankID{Slot: end + 1, Blockhash: solana.Hash{99}}
				require.NoError(t, server.resultCache.Publish(txstatus.ResultBank{ID: recent, Parent: parent,
					Complete: true, Results: []txstatus.SignatureResult{{Signature: solana.Signature{99}}}}))
				query = []solana.Signature{signature, {99}, signature}
			case "gap-mixed-unknown":
				query, refuse = []solana.Signature{signature, {98}}, true
			case "gap-recent-only":
				history, refuse = false, true
			case "gap-latched":
				server.resultCache.Invalidate()
				refuse = true
			case "gap-lineage":
				require.ErrorIs(t, server.resultCache.Publish(txstatus.ResultBank{ID: txstatus.ResultBankID{
					Slot: end + 1, Blockhash: solana.Hash{99}}, Parent: old}), txstatus.ErrResultLineage)
				refuse = true
			case "gap-recovery":
				server.historyRecoveryPending, refuse = true, true
			case "gap-root-mismatch":
				server.SetRootedBankState(end-1, 9, 1)
				refuse = true
			case "gap-cache-change":
				hook = historyReselectionHook(t, server, func(read int) error {
					if read == 1 {
						return server.resultCache.Publish(txstatus.ResultBank{ID: txstatus.ResultBankID{
							Slot: end + 1, Blockhash: solana.Hash{99}}, Parent: parent, Complete: true})
					}
					return nil
				})
			}
			got, err := server.readSignatureStatuses(t.Context(), query, history)
			if refuse {
				require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
				require.Equal(t, txstatus.ResultView{}, got, "no partial status batch may escape refusal")
				if hook != nil {
					require.Equal(t, 1, hook.reads, "cache mutation must reach and fail the final generation fence")
				}
				return
			}
			require.NoError(t, err, "a canonical retained receipt must survive an unrelated optional cache gap")
			if hook != nil {
				require.Equal(t, 2, hook.reads, "one fresh selection after a forward cache append")
			}
			require.Len(t, got.Results, len(query))
			for i, result := range got.Results {
				require.True(t, result.Found)
				if query[i] == signature {
					require.True(t, result.Finalized)
					require.Equal(t, uint64(20), result.Slot)
					require.JSONEq(t, string(metadata.Error), string(result.Error))
				} else {
					require.False(t, result.Finalized)
					require.Equal(t, end+1, result.Slot)
				}
			}
		})
	}
}

// These are real HTTP/cache/store fixtures with explicitly supplied selected
// roots and accepted outcomes, not node owner/finality or replay execution proof.
func signatureStatusFixture(t *testing.T) (*RpcServer, transactionhistory.Block, *httptest.Server, *rpc.Client) {
	t.Helper()
	server, block, endpoint := getBlockHistoryHTTPFixture(t)
	var err error
	server.resultCache, err = txstatus.NewResultCache(txstatus.ResultBankID{Slot: 20, Blockhash: solana.Hash(block.ID.Blockhash)})
	require.NoError(t, err)
	client := rpc.New(endpoint.URL)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return server, block, endpoint, client
}

func statusPublish(t *testing.T, server *RpcServer, slot uint64, complete bool, results ...txstatus.SignatureResult) txstatus.ResultBankID {
	t.Helper()
	view, err := server.resultCache.Lookup(nil)
	require.NoError(t, err)
	id := txstatus.ResultBankID{Slot: slot, Blockhash: solana.Hash{byte(slot), 1}}
	err = server.resultCache.Publish(txstatus.ResultBank{ID: id, Parent: view.Tip, Complete: complete, Results: results})
	if complete {
		require.NoError(t, err)
	} else {
		require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
	}
	return id
}

func statusParams(signatures []solana.Signature, config string) string {
	encoded := make([]string, len(signatures))
	for i, signature := range signatures {
		encoded[i] = signature.String()
	}
	raw, _ := json.Marshal(encoded)
	return "[" + string(raw) + config + "]"
}

func statusCall(t *testing.T, endpoint *httptest.Server, signatures []solana.Signature, config string) map[string]json.RawMessage {
	t.Helper()
	return getBlockHistoryHTTPCall(t, endpoint, "getSignatureStatuses", statusParams(signatures, config))
}

func TestSignatureStatusesHTTPAndSDK(t *testing.T) {
	server, _, endpoint, client := signatureStatusFixture(t)
	signatures := []solana.Signature{{1}, {2}, {3}, {4}}
	errors := []json.RawMessage{nil, json.RawMessage(`{"InstructionError":[0,{"Custom":0}]}`),
		json.RawMessage(`{"InstructionError":[1,{"Custom":6001}]}`), json.RawMessage(`"AccountNotFound"`)}
	results := make([]txstatus.SignatureResult, len(signatures))
	for i := range signatures {
		results[i] = txstatus.SignatureResult{Signature: signatures[i], Error: append(json.RawMessage(nil), errors[i]...)}
	}
	tip := statusPublish(t, server, 25, true, results...) // Skipped numeric slots do not manufacture confirmations.
	for i := range results {
		if len(results[i].Error) > 0 {
			results[i].Error[0] = 'X'
		}
	}
	ordered := []solana.Signature{signatures[1], {99}, signatures[0], signatures[1], signatures[2], signatures[3]}
	got, err := client.GetSignatureStatuses(t.Context(), false, ordered...)
	require.NoError(t, err)
	require.Equal(t, uint64(25), got.Context.Slot)
	require.Len(t, got.Value, len(ordered))
	require.Nil(t, got.Value[1])
	require.Equal(t, got.Value[0], got.Value[3])
	require.Nil(t, got.Value[2].Err)
	for i, value := range got.Value {
		if i == 1 {
			continue
		}
		require.Equal(t, rpc.ConfirmationStatusProcessed, value.ConfirmationStatus)
		require.NotNil(t, value.Confirmations)
		require.Zero(t, *value.Confirmations)
	}
	body := statusCall(t, endpoint, signatures, "")
	var raw struct {
		Value []map[string]json.RawMessage `json:"value"`
	}
	require.NoError(t, json.Unmarshal(body["result"], &raw))
	for i, value := range raw.Value {
		if errors[i] == nil {
			require.JSONEq(t, "null", string(value["err"]))
			require.JSONEq(t, `{"Ok":null}`, string(value["status"]))
		} else {
			require.JSONEq(t, string(errors[i]), string(value["err"]))
			require.JSONEq(t, `{"Err":`+string(errors[i])+`}`, string(value["status"]))
		}
	}
	require.NoError(t, server.resultCache.Root(tip))
	_, err = server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 25}}, 25, nil, nil)
	require.NoError(t, err)
	server.SetRootedBankState(25, 8, 4)
	got, err = client.GetSignatureStatuses(t.Context(), false, signatures...)
	require.NoError(t, err)
	for _, value := range got.Value {
		require.Equal(t, rpc.ConfirmationStatusFinalized, value.ConfirmationStatus)
		require.Nil(t, value.Confirmations)
	}
	empty, err := client.GetSignatureStatuses(t.Context(), false, []solana.Signature{}...)
	require.NoError(t, err)
	require.NotNil(t, empty.Value)
	require.Empty(t, empty.Value)
}

func TestSignatureStatusesExactParameters(t *testing.T) {
	_, _, endpoint, _ := signatureStatusFixture(t)
	for _, count := range []int{0, 1, 256} {
		body := statusCall(t, endpoint, make([]solana.Signature, count), "")
		require.NotContains(t, body, "error")
		var result struct {
			Value []any `json:"value"`
		}
		require.NoError(t, json.Unmarshal(body["result"], &result))
		require.NotNil(t, result.Value)
		require.Len(t, result.Value, count)
	}
	for _, config := range []string{",null", `,{"searchTransactionHistory":false}`, `,{"searchTransactionHistory":false,"unknown":1,"commitment":"nonsense"}`} {
		require.NotContains(t, statusCall(t, endpoint, nil, config), "error")
	}
	for _, test := range []struct{ params, message string }{
		{"[]", "getSignatureStatuses requires signatures and optional config"},
		{"[[],null,null]", "getSignatureStatuses requires signatures and optional config"},
		{"[null]", "getSignatureStatuses requires an array of at most 256 signatures"},
		{"[{}]", "getSignatureStatuses requires an array of at most 256 signatures"},
		{statusParams(make([]solana.Signature, 257), ""), "getSignatureStatuses requires an array of at most 256 signatures"},
		{"[[null]]", "invalid transaction signature"}, {"[[1]]", "invalid transaction signature"},
		{`[["0"]]`, "invalid transaction signature"}, {`[["` + strings.Repeat("1", 89) + `"]]`, "invalid transaction signature"},
		{"[[],{}]", "invalid getSignatureStatuses config"},
		{"[[],[]]", "invalid getSignatureStatuses config"},
		{`[[],{"searchTransactionHistory":null}]`, "invalid getSignatureStatuses config"},
		{`[[],{"searchTransactionHistory":"true"}]`, "invalid getSignatureStatuses config"},
		{`[[],{"searchTransactionHistory":0}]`, "invalid getSignatureStatuses config"},
		{`[[],true]`, "invalid getSignatureStatuses config"},
		{`[[],0]`, "invalid getSignatureStatuses config"},
		{`[[],{"padding":"` + strings.Repeat("x", 33<<10) + `"}]`, "getSignatureStatuses parameters exceed 32 KiB"},
	} {
		body := getBlockHistoryHTTPCall(t, endpoint, "getSignatureStatuses", test.params)
		requireGetBlockHistoryError(t, body, -32602, test.message)
	}
}

func TestSignatureStatusesGapAndDisabledPolicy(t *testing.T) {
	server, _, endpoint, client := signatureStatusFixture(t)
	known := solana.Signature{1}
	statusPublish(t, server, 21, true, txstatus.SignatureResult{Signature: known})
	statusPublish(t, server, 22, false)
	got, err := client.GetSignatureStatuses(t.Context(), false, known, known)
	require.NoError(t, err)
	require.Len(t, got.Value, 2)
	for _, config := range []string{"", `,{"searchTransactionHistory":true}`} {
		requireGetBlockHistoryError(t, statusCall(t, endpoint, []solana.Signature{known, {99}}, config),
			-32000, "Signature status unavailable")
	}
	server.InitTransactionHistory(nil)
	require.NotContains(t, statusCall(t, endpoint, nil, ""), "error", "empty recent query remains [] with history disabled")
	requireGetBlockHistoryError(t, statusCall(t, endpoint, []solana.Signature{known}, `,{"searchTransactionHistory":true}`),
		-32011, "Transaction history is not available from this node")
	require.NotContains(t, statusCall(t, endpoint, []solana.Signature{known}, ""), "error")
	server.resultCache.Invalidate()
	requireGetBlockHistoryError(t, statusCall(t, endpoint, nil, ""), -32000, "Signature status unavailable")
}

func TestSignatureStatusesHistoricalFallback(t *testing.T) {
	for _, name := range []string{"available", "missing", "orphan", "metadata-corrupt", "parent-mismatch", "bankhash-short", "pending", "root-mismatch"} {
		t.Run(name, func(t *testing.T) {
			server, block, endpoint, client := signatureStatusFixture(t)
			signature := solana.Signature(block.Records[0].FirstSignature)
			switch name {
			case "orphan":
				block.ID.Blockhash[0]++
			case "metadata-corrupt":
				block.Records[0].Metadata = []byte("broken")
			case "parent-mismatch":
				block.ParentSlot++
			case "bankhash-short":
				require.NoError(t, server.acctsDb.StoreBankHashForSlot(20, []byte{1}))
			case "pending":
				server.SetHistoryRecoveryPending(true)
			case "root-mismatch":
				server.SetRootedBankState(19, 6, 0)
			}
			if name != "missing" {
				require.NoError(t, server.transactionHistory.WriteBlock(block))
			}
			if name == "available" || name == "missing" || name == "orphan" {
				got, err := client.GetSignatureStatuses(t.Context(), true, signature, solana.Signature{99}, signature)
				require.NoError(t, err)
				require.Equal(t, uint64(20), got.Context.Slot)
				require.Nil(t, got.Value[1])
				require.Equal(t, got.Value[0], got.Value[2])
				if name == "available" {
					require.NotNil(t, got.Value[0])
					require.Equal(t, rpc.ConfirmationStatusFinalized, got.Value[0].ConfirmationStatus)
					require.Nil(t, got.Value[0].Confirmations)
				} else {
					require.Nil(t, got.Value[0])
				}
			} else {
				requireGetBlockHistoryError(t, statusCall(t, endpoint, []solana.Signature{signature}, `,{"searchTransactionHistory":true}`),
					-32000, "Signature status unavailable")
			}
			require.Zero(t, len(server.transactionHistoryReads))
		})
	}
}

func TestSignatureStatusesHistoricalAmbiguityAndBudgets(t *testing.T) {
	for _, name := range []string{"ambiguous", "bytes", "operations"} {
		t.Run(name, func(t *testing.T) {
			server, block, endpoint, _ := signatureStatusFixture(t)
			signature := solana.Signature(block.Records[0].FirstSignature)
			if name == "bytes" {
				metadata, err := transactionhistory.UnmarshalMetadata(block.Records[0].Metadata)
				require.NoError(t, err)
				metadata.InnerInstructions = []transactionhistory.InnerInstructionGroup{{Index: 0}}
				for range 16 {
					metadata.InnerInstructions[0].Instructions = append(metadata.InnerInstructions[0].Instructions,
						transactionhistory.InnerInstruction{ProgramIDIndex: 1, Data: bytes.Repeat([]byte{1}, 10<<10)})
				}
				block.Records[0].Metadata, err = metadata.MarshalBinary()
				require.NoError(t, err)
			}
			require.NoError(t, server.transactionHistory.WriteBlock(block))
			if name == "ambiguous" {
				second := block
				second.ID = transactionhistory.BlockID{Slot: 10, Blockhash: block.PreviousBlockhash}
				second.ParentSlot, second.PreviousBlockhash = 0, [32]byte{9}
				second.Bankhash = block.ParentBankhash
				require.NoError(t, server.transactionHistory.WriteBlock(second))
				publishTransactionHistorySummary(t, server, second)
				// The helper's temporary root 10 correctly drops its old
				// future summary; republish 20 so both candidates really match.
				publishTransactionHistorySummary(t, server, block)
				_, err := server.blockHistory.Get(10)
				require.NoError(t, err)
				_, err = server.blockHistory.Get(20)
				require.NoError(t, err)
			}
			if name == "operations" {
				for i := byte(1); i <= 20; i++ {
					orphan := block
					orphan.ID.Blockhash[1] = i
					require.NoError(t, server.transactionHistory.WriteBlock(orphan))
				}
			}
			signatures := []solana.Signature{signature}
			if name != "ambiguous" {
				signatures = make([]solana.Signature, 256)
				for i := range signatures {
					signatures[i] = signature
				}
			}
			requireGetBlockHistoryError(t, statusCall(t, endpoint, signatures, `,{"searchTransactionHistory":true}`),
				-32000, "Signature status unavailable")
			if name != "ambiguous" {
				view, err := server.readSignatureStatuses(t.Context(), signatures, true)
				require.ErrorIs(t, err, transactionhistory.ErrLimit, "refusal must be the shared work budget, not another fixture error")
				require.Equal(t, txstatus.ResultView{}, view, "earlier selected records must not escape a later limit")
			}
			require.Zero(t, len(server.transactionHistoryReads))
		})
	}
}

func TestSignatureStatusesSharedAdmission(t *testing.T) {
	server, _, original, _ := signatureStatusFixture(t)
	data, err := historyHTTPPost(original.URL, `[
		{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[[]]},
		{"jsonrpc":"2.0","id":2,"method":"getSignatureStatuses","params":[[]]},
		{"jsonrpc":"2.0","id":3,"method":"getSignatureStatuses","params":[[]]}
	]`)
	require.NoError(t, err)
	var replies []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &replies))
	require.Len(t, replies, 3)
	require.Contains(t, replies[0], "result")
	require.Contains(t, replies[1], "result")
	requireGetBlockHistoryError(t, replies[2], -32000, "Signature status unavailable")
	original.Close()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	writers := make(chan *responseFlushGate, 2)
	var handlers sync.WaitGroup
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		gate := &responseFlushGate{ResponseWriter: w, entered: make(chan struct{}), release: release}
		writers <- gate
		server.ServeHTTP(gate, r)
	}))
	t.Cleanup(endpoint.Close)
	done := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := historyHTTPPost(endpoint.URL, `{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[[]]}`)
			done <- err
		}()
	}
	gates := make([]*responseFlushGate, 2)
	for i := range gates {
		select {
		case gates[i] = <-writers:
		case <-time.After(2 * time.Second):
			t.Fatal("request did not enter handler")
		}
		select {
		case <-gates[i].entered:
		case <-time.After(2 * time.Second):
			t.Fatal("request did not reach final flush")
		}
	}
	require.Equal(t, 2, len(server.queryResponses))
	// A separate endpoint uses the same actual server/permit pool without the
	// fixture's flush gate. Refusal cannot be mistaken for missing ownership.
	control := httptest.NewServer(server)
	t.Cleanup(control.Close)
	requireGetBlockHistoryError(t, statusCall(t, control, nil, ""), -32000, "Signature status unavailable")
	require.NotContains(t, getBlockHistoryHTTPCall(t, control, "getSlot", "[]"), "error")
	unblock()
	for range 2 {
		require.NoError(t, <-done)
	}
	handlers.Wait()
	require.Zero(t, len(server.queryResponses))
	for _, gate := range gates {
		require.Equal(t, int32(1), gate.flushes.Load())
		require.Equal(t, int32(1), gate.clears.Load())
	}
}

func TestSignatureStatusesNoOwnerAndPolicyCodec(t *testing.T) {
	server, _, endpoint, _ := signatureStatusFixture(t)
	_, err := server.GetSignatureStatuses(t.Context(), []byte("[[]]"))
	require.EqualError(t, err, "Signature status unavailable")
	codec := new(signatureStatusUnavailableError)
	wire, err := codec.ToJSONRPCError()
	require.NoError(t, err)
	require.NoError(t, codec.FromJSONRPCError(wire))
	wire.Code = -32011
	require.Error(t, codec.FromJSONRPCError(wire))
	require.Zero(t, len(server.queryResponses))
	connection, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(endpoint.URL, "http"), nil)
	require.Error(t, err)
	require.Nil(t, connection)
	require.NotNil(t, response)
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
	require.NoError(t, response.Body.Close())
	// Bypass only the existing outer upgrade guard, while retaining an actual
	// request owner. Refusal must come from WS transport, not a missing owner.
	transport := newQueryWebSocketProbe(t, server)
	connection, _, err = websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(transport.URL, "http"), nil)
	require.NoError(t, err)
	defer connection.Close()
	require.NoError(t, connection.SetReadDeadline(time.Now().Add(2*time.Second)))
	require.NoError(t, connection.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[[]]}`)))
	_, data, err := connection.ReadMessage()
	require.NoError(t, err)
	var reply map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &reply))
	requireGetBlockHistoryError(t, reply, -32000, "Signature status unavailable")
	require.NoError(t, connection.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}`)))
	_, data, err = connection.ReadMessage()
	require.NoError(t, err)
	reply = nil
	require.NoError(t, json.Unmarshal(data, &reply))
	require.JSONEq(t, "20", string(reply["result"]))
	require.Zero(t, len(server.queryResponses))
}
