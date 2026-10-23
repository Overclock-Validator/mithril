package rpcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func getBlocksFixture(t *testing.T) (*RpcServer, *httptest.Server) {
	t.Helper()
	db := newRPCAccountsDB(t)
	store, err := blockhistory.Open(t.TempDir(), 100)
	require.NoError(t, err)
	for _, slot := range []uint64{10, 12} {
		if slot == 12 {
			require.NoError(t, store.RecordSkipped(11))
		}
		require.NoError(t, store.RecordBlock(&b.Block{Slot: slot, Blockhash: solana.Hash{byte(slot)}}))
		require.NoError(t, store.Prepare(slot))
		_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{{Key: solana.PublicKey{205}, Lamports: slot}}}}, slot, nil, nil)
		require.NoError(t, err)
		require.NoError(t, store.SetRooted(slot))
	}
	server := NewRpcServer(db, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}, solana.Hash{})
	require.NoError(t, server.listener.Close())
	server.blockHistory = store
	server.SetRootedBankState(12, 2, 0)
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	return server, endpoint
}

func TestGetBlocksHTTPParameters(t *testing.T) {
	_, endpoint := getBlocksFixture(t)
	for _, test := range []struct {
		params string
		want   string
	}{
		{`[10]`, `[10,12]`},
		{`[10,null]`, `[10,12]`},
		{`[10,12,null]`, `[10,12]`},
		{`[10,{"commitment":"confirmed"}]`, `[10,12]`},
		{`[10,12,{"commitment":"finalized","minContextSlot":12}]`, `[10,12]`},
		{`[10,{"commitment":"processed"},{"commitment":"finalized"}]`, `[10,12]`},
		{`[10,{"commitment":"finalized","minContextSlot":99},{}]`, `[10,12]`},
		{`[10,{"commitment":null,"minContextSlot":null},null]`, `[10,12]`},
		{`[10,18446744073709551615]`, `[10,12]`},
		{`[10,9]`, `[]`},
		{`[11,11]`, `[]`},
		{`[13]`, `[]`},
		{`[18446744073709551615]`, `[]`},
		{`[18446744073709551615,18446744073709551615]`, `[]`},
	} {
		t.Run(test.params, func(t *testing.T) {
			body := getBlockHistoryHTTPCall(t, endpoint, "getBlocks", test.params)
			require.NotContains(t, body, "error")
			require.JSONEq(t, test.want, string(body["result"]))
		})
	}
	for _, params := range []string{
		`[]`, `null`, `{}`, `[10,12,{},{}]`, `[null]`, `[-1]`, `[1.1]`, `[1e1]`, `["10"]`, `[true]`, `[18446744073709551616]`,
		`[10,-1]`, `[10,12.5]`, `[10,"12"]`, `[10,true]`, `[10,[]]`, `[10,12,[]]`, `[10,12,"finalized"]`,
		`[10,{"commitment":"processed"}]`, `[10,{"commitment":"invalid"},{}]`, `[10,{"commitment":1}]`,
		`[10,{"minContextSlot":-1}]`, `[10,{"minContextSlot":1.5}]`, `[10,{"minContextSlot":1e1}]`,
		`[10,{"minContextSlot":"12"}]`, `[10,{"minContextSlot":18446744073709551616}]`,
	} {
		t.Run("invalid"+params, func(t *testing.T) {
			body := getBlockHistoryHTTPCall(t, endpoint, "getBlocks", params)
			require.NotContains(t, body, "result")
			var failure struct{ Code int }
			require.NoError(t, json.Unmarshal(body["error"], &failure))
			require.Equal(t, -32602, failure.Code)
		})
	}
	for _, params := range []string{`[10,{"minContextSlot":13}]`, `[13,12,{"minContextSlot":13}]`} {
		body := getBlockHistoryHTTPCall(t, endpoint, "getBlocks", params)
		require.NotContains(t, body, "result")
		require.JSONEq(t, `{"code":-32016,"message":"Minimum context slot has not been reached","data":{"contextSlot":12}}`, string(body["error"]))
	}
	requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[9,12]`), -32004, "Block not available for slot 9")
}

func TestGetBlocksRangeOrderAndExactIntegers(t *testing.T) {
	start, end, config, err := parseBlocksParams(jsonrpc.RawParams(`[9007199254740993,18446744073709551615,{"minContextSlot":9007199254740995}]`))
	require.NoError(t, err)
	require.Equal(t, uint64(9007199254740993), start)
	require.Equal(t, uint64(math.MaxUint64), *end)
	require.Equal(t, uint64(9007199254740995), *config.MinContextSlot)
	server, endpoint := getBlocksFixture(t)
	const root = uint64(blockhistory.MaxBlocksRange + 12)
	_, err = server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: root, Delta: []*accounts.Account{{Key: solana.PublicKey{205}, Lamports: root}}}}, root, nil, nil)
	require.NoError(t, err)
	require.NoError(t, server.SetRootedBlockHistorySlot(root))
	server.SetRootedBankState(root, 3, 0)
	// Clamp first: the huge explicit end becomes exactly the permitted span,
	// and reaches coverage checking (old slot 12 was pruned).
	requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[12,18446744073709551615]`), -32004, "Block not available for slot 12")
	requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[11,18446744073709551615]`), -32602, "slot range too large; max 500000")
}

func TestGetBlocksRecoveryAndRewind(t *testing.T) {
	server, endpoint := getBlocksFixture(t)
	server.SetHistoryRecoveryPending(true)
	requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[10,12]`), -32004, "Block not available for slot 10")
	server.SetHistoryRecoveryPending(false)
	// Actual partial AccountsDB rewind keeps slot watermarks but invalidates
	// its version; summary coverage alone cannot authorize enumeration.
	blocked := filepath.Join(server.acctsDb.AcctsDir, "rewound")
	require.NoError(t, os.WriteFile(blocked, nil, 0o600))
	_, err := server.acctsDb.RewindToBatchBoundary(10)
	require.Error(t, err)
	requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[10,12]`), -32004, "Block not available for slot 10")
	require.NoError(t, os.Remove(blocked))
	_, err = server.acctsDb.RewindToBatchBoundary(10)
	require.NoError(t, err)
	// Even a successful rewind remains unpublished while the bank is at 12.
	requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[10,12]`), -32004, "Block not available for slot 10")
	require.NoError(t, server.RewindBlockHistory(10))
	server.SetRootedBankState(10, 1, 0)
	body := getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[10,12]`)
	require.NotContains(t, body, "error")
	require.JSONEq(t, `[10]`, string(body["result"]))
	body = getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[11,12]`)
	require.NotContains(t, body, "error")
	require.JSONEq(t, `[]`, string(body["result"]))
}

type blocksReadContext struct {
	context.Context
	checks int
	action func()
}

func (ctx *blocksReadContext) Err() error {
	ctx.checks++
	if ctx.checks == 3 {
		ctx.action()
	}
	return ctx.Context.Err()
}

func TestGetBlocksRejectsPublicationChanges(t *testing.T) {
	for _, changeVersion := range []bool{false, true} {
		server, original := getBlocksFixture(t)
		original.Close()
		endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := &blocksReadContext{Context: r.Context(), action: func() { server.SetRootedBankState(12, 2, 0) }}
			if changeVersion {
				ctx.action = func() {
					if _, err := server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 13, Delta: []*accounts.Account{{Key: solana.PublicKey{205}, Lamports: 13}}}}, 13, nil, nil); err != nil {
						t.Errorf("change committed version: %v", err)
					}
				}
			}
			server.ServeHTTP(w, r.WithContext(ctx))
		}))
		requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[10,12]`), -32004, "Block not available for slot 10")
		endpoint.Close()
	}
	server, _ := getBlocksFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, err := server.GetBlocks(ctx, jsonrpc.RawParams(`[10,12]`))
	require.Nil(t, got)
	require.ErrorIs(t, err, context.Canceled)
}

func TestGetBlocksSharedQueryBatchAndWebSocket(t *testing.T) {
	server, endpoint := getBlocksFixture(t)
	data, err := historyHTTPPost(endpoint.URL, `[
		{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":[10,12]},
		{"jsonrpc":"2.0","id":2,"method":"getMultipleAccounts","params":[[]]},
		{"jsonrpc":"2.0","id":3,"method":"getBlocks","params":[10,12]},
		{"jsonrpc":"2.0","id":4,"method":"getSlot","params":[]}
	]`)
	require.NoError(t, err)
	var replies []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &replies))
	require.Len(t, replies, 4)
	require.JSONEq(t, `[10,12]`, string(replies[0]["result"]))
	require.Contains(t, replies[1], "result")
	requireGetBlockHistoryError(t, replies[2], 1, "query response unavailable")
	require.JSONEq(t, `12`, string(replies[3]["result"]))
	require.Eventually(t, func() bool { return len(server.queryResponses) == 0 }, time.Second, time.Millisecond)
	connection, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(endpoint.URL, "http"), nil)
	require.Error(t, err)
	require.Nil(t, connection)
	require.NotNil(t, response)
	require.Equal(t, http.StatusBadRequest, response.StatusCode, "existing outer probe guard rejects an empty upgrade GET")
	require.NoError(t, response.Body.Close())
	// Exercise the registered transport with real request ownership, bypassing
	// only the pre-existing outer probe guard, not query admission itself.
	transport := newQueryWebSocketProbe(t, server)
	connection, _, err = websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(transport.URL, "http"), nil)
	require.NoError(t, err)
	defer connection.Close()
	require.NoError(t, connection.SetReadDeadline(time.Now().Add(2*time.Second)))
	require.NoError(t, connection.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":[10,12]}`)))
	_, data, err = connection.ReadMessage()
	require.NoError(t, err)
	var reply map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &reply))
	requireGetBlockHistoryError(t, reply, 1, "query response unavailable")
	require.NoError(t, connection.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":2,"method":"getSlot","params":[]}`)))
	_, data, err = connection.ReadMessage()
	require.NoError(t, err)
	reply = nil
	require.NoError(t, json.Unmarshal(data, &reply))
	require.JSONEq(t, `12`, string(reply["result"]))
	require.Zero(t, len(server.queryResponses))
}

type blocksResponseWriteGate struct {
	http.ResponseWriter
	ctx     context.Context
	entered chan<- struct{}
	release <-chan struct{}
	once    sync.Once
}

func (w *blocksResponseWriteGate) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *blocksResponseWriteGate) Write(data []byte) (int, error) {
	w.once.Do(func() { w.entered <- struct{}{} })
	select {
	case <-w.release:
		return w.ResponseWriter.Write(data)
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	}
}

func TestGetBlocksSharedQueryWriteAndDisconnect(t *testing.T) {
	server, original := getBlocksFixture(t)
	original.Close()
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Hold") == "yes" {
			w = &blocksResponseWriteGate{ResponseWriter: w, ctx: r.Context(), entered: entered, release: release}
		}
		server.ServeHTTP(w, r)
	}))
	t.Cleanup(endpoint.Close)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	results := make(chan error, 2)
	for range 2 {
		go func() {
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":[10,12]}`))
			if err == nil {
				request.Header.Set("X-Hold", "yes")
				var response *http.Response
				response, err = endpoint.Client().Do(request)
				if response != nil {
					if closeErr := response.Body.Close(); err == nil {
						err = closeErr
					}
				}
			}
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("query responses did not reach the write boundary")
		}
	}
	require.Equal(t, 2, len(server.queryResponses))
	requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", `[[]]`), 1, "query response unavailable")
	requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[10,12]`), 1, "query response unavailable")
	require.JSONEq(t, `12`, string(getBlockHistoryHTTPCall(t, endpoint, "getSlot", `[]`)["result"]))
	cancel() // Close both real client requests while their responses still own permits.
	for range 2 {
		select {
		case err := <-results:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("canceled client did not return")
		}
	}
	require.Eventually(t, func() bool { return len(server.queryResponses) == 0 }, time.Second, time.Millisecond)
	unblock()
	// A failed range also returns its permit; the next successful query works.
	requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[9,12]`), -32004, "Block not available for slot 9")
	require.Eventually(t, func() bool { return len(server.queryResponses) == 0 }, time.Second, time.Millisecond)
	require.JSONEq(t, `[10,12]`, string(getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[10,12]`)["result"]))
}

func TestGetBlocksSDKScanner(t *testing.T) {
	server, block, endpoint := getBlockHistoryHTTPFixture(t)
	// Structural payloads and controlled publication exercise the actual SDK
	// and HTTP path, not replay or a production indexer's durable side effects.
	second := block
	second.ID.Slot, second.ID.Blockhash = 22, [32]byte{22}
	second.ParentSlot, second.PreviousBlockhash = 20, block.ID.Blockhash
	second.Bankhash = [32]byte{32}
	record, tx, _ := historyResponseFixture(t, solana.MessageVersionLegacy)
	tx.Signatures[0][2] = 22
	var err error
	record.Transaction, err = tx.MarshalBinary()
	require.NoError(t, err)
	record.FirstSignature = [64]byte(tx.Signatures[0])
	second.Records = append(second.Records[:0:0], record)
	_, err = server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 22, Delta: []*accounts.Account{{Key: solana.PublicKey{201}, Lamports: 22}}}}, 22, nil, nil)
	require.NoError(t, err)
	require.NoError(t, server.acctsDb.StoreBankHashForSlot(22, second.Bankhash[:]))
	server.blockHistory, err = blockhistory.Open(t.TempDir(), 100)
	require.NoError(t, err)
	require.NoError(t, server.RecordBlockHistory(&b.Block{Slot: 18, Blockhash: solana.Hash{18}}))
	require.NoError(t, server.RecordSkippedBlockHistory(19))
	require.NoError(t, server.RecordBlockHistory(&b.Block{Slot: 20, Blockhash: solana.Hash(block.ID.Blockhash), ParentSlot: block.ParentSlot, LastBlockhash: solana.Hash(block.PreviousBlockhash)}))
	require.NoError(t, server.RecordSkippedBlockHistory(21))
	publishTransactionHistorySummary(t, server, second)
	server.SetRootedBankState(22, 8, 2)
	require.NoError(t, server.transactionHistory.WriteBlock(block))
	client := rpc.New(endpoint.URL)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	end := uint64(22)
	listed, err := client.GetBlocks(t.Context(), 19, &end, rpc.CommitmentFinalized)
	require.NoError(t, err)
	require.Equal(t, rpc.BlocksResult{20, 22}, listed)
	type checkpoint struct {
		ScannedThrough uint64
		LastBlockSlot  uint64
		LastBlockhash  solana.Hash
		Seen           []solana.Signature
	}
	state := checkpoint{ScannedThrough: 18, LastBlockSlot: 18, LastBlockhash: solana.Hash{18}}
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	persist := func() {
		t.Helper()
		data, err := json.Marshal(state)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, data, 0o600))
	}
	reload := func() {
		t.Helper()
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		state = checkpoint{}
		require.NoError(t, json.Unmarshal(data, &state))
	}
	persist()
	wires := map[uint64][]byte{20: block.Records[0].Transaction, 22: second.Records[0].Transaction}
	scan := func(start, end uint64) error {
		saved, err := client.GetBlockWithOpts(t.Context(), state.LastBlockSlot, &rpc.GetBlockOpts{TransactionDetails: rpc.TransactionDetailsNone, Commitment: rpc.CommitmentFinalized})
		if err != nil {
			return err
		}
		if saved.Blockhash != state.LastBlockhash {
			return fmt.Errorf("saved block identity changed")
		}
		var slots rpc.BlocksResult
		// The typed v1.23 helper has no minContextSlot option. Its raw SDK call
		// prevents advancing a requested end that the node silently root-clamped.
		err = client.RPCCallForInto(t.Context(), &slots, "getBlocks", []any{start, end, map[string]any{"commitment": "finalized", "minContextSlot": end}})
		if err != nil {
			return err
		}
		for _, slot := range slots {
			got, err := client.GetBlockWithOpts(t.Context(), slot, &rpc.GetBlockOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentFinalized})
			if err != nil {
				return err
			}
			var signatures []solana.Signature
			for _, value := range got.Transactions {
				tx, err := value.GetTransaction()
				if err != nil {
					return err
				}
				wire, err := tx.MarshalBinary()
				if err != nil {
					return err
				}
				require.Equal(t, wires[slot], wire)
				signatures = append(signatures, tx.Signatures[0])
			}
			for _, signature := range signatures {
				if !slices.Contains(state.Seen, signature) {
					state.Seen = append(state.Seen, signature)
				}
			}
			state.LastBlockSlot, state.LastBlockhash = slot, got.Blockhash
			persist() // This fixture stores consumed identities with its checkpoint.
		}
		state.ScannedThrough = end
		persist()
		return nil
	}
	require.Error(t, scan(17, 22), "unknown coverage must not advance the scanner")
	require.Equal(t, uint64(18), state.ScannedThrough)
	require.Empty(t, state.Seen)
	require.Error(t, scan(19, 22), "second payload is not retained yet")
	require.Equal(t, uint64(18), state.ScannedThrough)
	require.Equal(t, uint64(20), state.LastBlockSlot)
	require.Len(t, state.Seen, 1)
	reload()
	require.NoError(t, server.transactionHistory.WriteBlock(second))
	require.NoError(t, scan(19, 22), "overlapping restart must not consume the first signature twice")
	require.Equal(t, uint64(22), state.ScannedThrough)
	require.Equal(t, []solana.Signature{solana.Signature(block.Records[0].FirstSignature), solana.Signature(second.Records[0].FirstSignature)}, state.Seen)
	reload()
	correct := state.LastBlockhash
	state.LastBlockhash[0]++
	persist()
	reload()
	require.ErrorContains(t, scan(19, 22), "saved block identity changed")
	require.Equal(t, uint64(22), state.ScannedThrough)
	state.LastBlockhash = correct
	persist()
	// Scanned-through is not the last produced slot: an all-skipped window
	// advances the former while retaining the latter's identity for resume.
	_, err = server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 25, Delta: []*accounts.Account{{Key: solana.PublicKey{201}, Lamports: 25}}}}, 25, nil, nil)
	require.NoError(t, err)
	require.NoError(t, server.RecordSkippedBlockHistory(23))
	require.NoError(t, server.RecordSkippedBlockHistory(24))
	require.NoError(t, server.RecordBlockHistory(&b.Block{Slot: 25, Blockhash: solana.Hash{25}, ParentSlot: 22, LastBlockhash: solana.Hash(second.ID.Blockhash)}))
	require.NoError(t, server.PrepareBlockHistory(25))
	require.NoError(t, server.SetRootedBlockHistorySlot(25))
	server.SetRootedBankState(25, 9, 2)
	require.NoError(t, scan(23, 24))
	require.Equal(t, uint64(24), state.ScannedThrough)
	require.Equal(t, uint64(22), state.LastBlockSlot)
	reload()
	require.Error(t, scan(25, 26), "minContextSlot prevents advancing beyond a behind/rewound node")
	require.Equal(t, uint64(24), state.ScannedThrough)
	removed, err := server.transactionHistory.PruneBefore(23, 64)
	require.NoError(t, err)
	require.Equal(t, uint32(2), removed)
	require.Error(t, scan(19, 22), "enumerated summary is not proof of retained transaction payload")
	require.Equal(t, uint64(24), state.ScannedThrough)
	require.Len(t, state.Seen, 2)
}
