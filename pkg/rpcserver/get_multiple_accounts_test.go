package rpcserver

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/zstd"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	alt "github.com/gagliardetto/solana-go/programs/address-lookup-table"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gorilla/websocket"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func multipleAccountsFixture(t *testing.T, delta ...*accounts.Account) (*RpcServer, *httptest.Server, *rpc.Client) {
	t.Helper()
	server, endpoint, client, _ := multipleAccountsFixtureWithCompletion(t, delta...)
	return server, endpoint, client
}

func multipleAccountsFixtureWithCompletion(t *testing.T, delta ...*accounts.Account) (*RpcServer, *httptest.Server, *rpc.Client, func()) {
	t.Helper()
	db := newRPCAccountsDB(t)
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 42, Delta: delta}}, 42, nil, nil)
	require.NoError(t, err)
	server := NewRpcServer(db, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	server.SetRootedBankState(42, 42, 0)
	var handlers sync.WaitGroup
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		server.ServeHTTP(w, r)
	}))
	t.Cleanup(endpoint.Close)
	client := rpc.New(endpoint.URL)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	// Call only after receiving the client response and before the next request.
	// Receiving a flushed body does not join ServeHTTP's deferred permit release.
	wait := func() {
		t.Helper()
		done := make(chan struct{})
		go func() {
			handlers.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("account response handler did not complete")
		}
	}
	return server, endpoint, client, wait
}

func TestGetMultipleAccountsHTTPAndSDKPositions(t *testing.T) {
	live := &accounts.Account{Key: solana.PublicKey{1}, Lamports: 77, Owner: [32]byte{4}, Data: []byte{1, 2, 3}, RentEpoch: math.MaxUint64, Executable: true}
	empty := &accounts.Account{Key: solana.PublicKey{2}, Lamports: 88}
	deleted := &accounts.Account{Key: solana.PublicKey{3}, Data: []byte{9}}
	server, endpoint, client, wait := multipleAccountsFixtureWithCompletion(t, live, empty, deleted)
	keys := []solana.PublicKey{empty.Key, {99}, live.Key, deleted.Key, live.Key}
	got, err := client.GetMultipleAccounts(t.Context(), keys...)
	require.NoError(t, err)
	require.Equal(t, uint64(42), got.Context.Slot)
	require.Nil(t, got.Context.ApiVersion)
	require.Len(t, got.Value, 5)
	require.NotNil(t, got.Value[0])
	require.Empty(t, got.Value[0].Data.GetBinary())
	require.Nil(t, got.Value[1])
	require.Nil(t, got.Value[3])
	require.Equal(t, live.Data, got.Value[2].Data.GetBinary())
	require.Equal(t, got.Value[2], got.Value[4])
	require.Equal(t, solana.PublicKey(live.Owner), got.Value[2].Owner)
	require.Equal(t, "18446744073709551615", got.Value[2].RentEpoch.String())
	require.True(t, got.Value[2].Executable)
	for _, count := range []int{0, 1, 100} {
		input := make([]solana.PublicKey, count)
		for i := range input {
			input[i] = live.Key
		}
		got, err := client.GetMultipleAccounts(t.Context(), input...)
		require.NoError(t, err)
		require.NotNil(t, got.Value)
		require.Len(t, got.Value, count)
	}
	_, err = client.GetMultipleAccounts(t.Context(), make([]solana.PublicKey, 101)...)
	require.ErrorContains(t, err, "at most 100")
	params := fmt.Sprintf(`[[%q,%q]]`, empty.Key, live.Key)
	body := getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", params)
	var result struct {
		Value []map[string]json.RawMessage `json:"value"`
	}
	require.NoError(t, json.Unmarshal(body["result"], &result))
	require.JSONEq(t, `["","base64"]`, string(result.Value[0]["data"]))
	for _, config := range []string{"", ",null", `,{"encoding":null,"commitment":null,"minContextSlot":null,"dataSlice":null,"unknown":1}`} {
		body = getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", fmt.Sprintf(`[[%q]%s]`, empty.Key, config))
		require.NotContains(t, body, "error")
	}
	// The single-account method retains its distinct legacy default.
	body = getBlockHistoryHTTPCall(t, endpoint, "getAccountInfo", fmt.Sprintf(`[%q,null]`, empty.Key))
	value := historyResponseObject(t, historyResponseObject(t, body["result"])["value"])
	require.JSONEq(t, `""`, string(value["data"]))
	require.Nil(t, server.transactionHistory)
	wait()
	require.Zero(t, len(server.queryResponses))
}

func TestGetMultipleAccountsEncodingsAndExactConfig(t *testing.T) {
	acct := &accounts.Account{Key: solana.PublicKey{8}, Lamports: 1, Data: bytes.Repeat([]byte{7}, 129)}
	_, endpoint, _ := multipleAccountsFixture(t, acct)
	for _, encoding := range []string{"base64", "base64+zstd", "base58", "binary"} {
		t.Run(encoding, func(t *testing.T) {
			body := getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", fmt.Sprintf(`[[%q],{"encoding":%q,"dataSlice":{"offset":1,"length":128}}]`, acct.Key, encoding))
			require.NotContains(t, body, "error")
			var result struct {
				Value []GetAccountInfoRespValue `json:"value"`
			}
			require.NoError(t, json.Unmarshal(body["result"], &result))
			require.Len(t, result.Value, 1)
			require.Equal(t, uint64(129), result.Value[0].Space)
			var encoded string
			if encoding == "binary" {
				encoded = result.Value[0].Data.(string)
			} else {
				tuple := result.Value[0].Data.([]any)
				require.Equal(t, encoding, tuple[1])
				encoded = tuple[0].(string)
			}
			var decoded []byte
			var err error
			if encoding == "binary" || encoding == "base58" {
				decoded, err = base58.Decode(encoded)
			} else {
				decoded, err = base64.StdEncoding.DecodeString(encoded)
			}
			require.NoError(t, err)
			if encoding == "base64+zstd" {
				decoded, err = zstd.Decompress(nil, decoded)
				require.NoError(t, err)
			}
			require.Equal(t, acct.Data[1:], decoded)
		})
	}
	for _, method := range []string{"getMultipleAccounts", "getAccountInfo"} {
		key := fmt.Sprintf(`%q`, acct.Key)
		if method == "getMultipleAccounts" {
			key = "[" + key + "]"
		}
		for _, encoding := range []string{"base58", "binary"} {
			body := getBlockHistoryHTTPCall(t, endpoint, method, fmt.Sprintf(`[%s,{"encoding":%q}]`, key, encoding))
			requireGetBlockHistoryError(t, body, -32600, (&accountEncodingError{}).Error())
		}
		body := getBlockHistoryHTTPCall(t, endpoint, method, fmt.Sprintf(`[%s,{"encoding":"base64","dataSlice":{"offset":18446744073709551615,"length":18446744073709551615}}]`, key))
		require.NotContains(t, body, "error")
		for _, invalid := range []string{`{"dataSlice":{}}`, `{"dataSlice":{"length":1}}`, `{"dataSlice":{"offset":0,"length":null}}`, `{"dataSlice":{"offset":1.0,"length":2}}`, `{"dataSlice":{"offset":1e0,"length":2}}`, `{"dataSlice":{"offset":-1,"length":2}}`, `{"dataSlice":{"offset":0,"length":18446744073709551616}}`, `{"minContextSlot":0.9999999999999999999}`, `{"minContextSlot":1e0}`, `{"minContextSlot":"42"}`, `{"encoding":"bad"}`, `{"commitment":"bad"}`, `[]`, `true`} {
			body := getBlockHistoryHTTPCall(t, endpoint, method, fmt.Sprintf(`[%s,%s]`, key, invalid))
			require.NotContains(t, body, "result")
			var rpcErr rpcProbeError
			require.NoError(t, json.Unmarshal(body["error"], &rpcErr))
			require.Equal(t, -32602, rpcErr.Code, invalid)
		}
	}
	for _, params := range []string{`[null]`, `[{}]`, `[[null]]`, `[[1]]`, `[[] ,{},1]`, `[]`, fmt.Sprintf(`[[%q]]`, strings.Repeat("1", 45))} {
		body := getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", params)
		require.NotContains(t, body, "result")
	}
	for _, slot := range []string{"43", "9007199254740993", "18446744073709551615"} {
		body := getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", fmt.Sprintf(`[[],{"minContextSlot":%s}]`, slot))
		var rpcErr struct {
			Code int `json:"code"`
			Data struct {
				ContextSlot uint64 `json:"contextSlot"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(body["error"], &rpcErr))
		require.Equal(t, -32016, rpcErr.Code)
		require.Equal(t, uint64(42), rpcErr.Data.ContextSlot)
	}
}

func TestGetMultipleAccountsSDKLookupTableConsumer(t *testing.T) {
	keys := []solana.PublicKey{{31}, {32}}
	addresses := []solana.PublicKeySlice{{{41}, {42}}, {{51}, {52}}}
	var delta []*accounts.Account
	for i, key := range keys {
		state := alt.AddressLookupTableState{TypeIndex: 1, DeactivationSlot: math.MaxUint64, Addresses: addresses[i]}
		var buf bytes.Buffer
		require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&buf)))
		delta = append(delta, &accounts.Account{Key: key, Lamports: 1, Owner: solana.AddressLookupTableProgramID, Data: buf.Bytes()})
	}
	_, _, client := multipleAccountsFixture(t, delta...)
	message := solana.Message{AccountKeys: []solana.PublicKey{{1}}, Header: solana.MessageHeader{NumRequiredSignatures: 1}, AddressTableLookups: []solana.MessageAddressTableLookup{
		{AccountKey: keys[0], WritableIndexes: []uint8{1}, ReadonlyIndexes: []uint8{0}},
		{AccountKey: keys[1], WritableIndexes: []uint8{0}, ReadonlyIndexes: []uint8{1}},
	}}
	message.SetVersion(solana.MessageVersionV0)
	missing := message
	missing.AddressTableLookups = append([]solana.MessageAddressTableLookup(nil), message.AddressTableLookups...)
	missing.AddressTableLookups = append(missing.AddressTableLookups[:1], append([]solana.MessageAddressTableLookup{{AccountKey: solana.PublicKey{99}, ReadonlyIndexes: []uint8{0}}}, missing.AddressTableLookups[1:]...)...)
	opts := &rpc.GetMultipleAccountsOpts{Commitment: rpc.CommitmentFinalized}
	require.NoError(t, alt.ResolveMessageLookupsFromRPCWithOpts(t.Context(), client, &message, opts))
	require.Equal(t, []solana.PublicKey{{1}, {42}, {51}, {41}, {52}}, []solana.PublicKey(message.AccountKeys))
	err := alt.ResolveMessageLookupsFromRPCWithOpts(t.Context(), client, &missing, opts)
	require.ErrorContains(t, err, solana.PublicKey{99}.String()+" not found")
	require.False(t, missing.IsResolved())
}

func TestGetMultipleAccountsBudgetAndRewind(t *testing.T) {
	acct := &accounts.Account{Key: solana.PublicKey{71}, Lamports: 42, Data: make([]byte, 1<<20)}
	server, endpoint, client, wait := multipleAccountsFixtureWithCompletion(t, acct)
	keys := make([]solana.PublicKey, 17)
	for i := range keys {
		keys[i] = acct.Key
	}
	encoded, err := json.Marshal(keys)
	require.NoError(t, err)
	body := getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", fmt.Sprintf(`[%s,{"minContextSlot":43}]`, encoded))
	require.JSONEq(t, `{"code":-32016,"message":"Minimum context slot has not been reached","data":{"contextSlot":42}}`, string(body["error"]))
	wait()
	length, offset := uint64(1), uint64(0)
	_, err = client.GetMultipleAccountsWithOpts(t.Context(), keys, &rpc.GetMultipleAccountsOpts{Encoding: solana.EncodingBase64, DataSlice: &rpc.DataSlice{Offset: &offset, Length: &length}})
	require.ErrorContains(t, err, "16 MiB account data budget")
	wait()
	require.Zero(t, len(server.queryResponses))
	got, err := client.GetMultipleAccountsWithOpts(t.Context(), keys[:1], &rpc.GetMultipleAccountsOpts{Encoding: solana.EncodingBase64, DataSlice: &rpc.DataSlice{Offset: &offset, Length: &length}})
	require.NoError(t, err)
	require.Equal(t, []byte{0}, got.Value[0].Data.GetBinary())
	second := solana.PublicKey{72}
	_, err = server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 43, Delta: []*accounts.Account{{Key: acct.Key, Lamports: 43}, {Key: second, Lamports: 43}}}}, 43, nil, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	remaining := uint64(20)
	_, _, err = server.readRootedAccountsWithBudget(ctx, []solana.PublicKey{acct.Key, second}, 0, &remaining)
	require.ErrorIs(t, err, context.DeadlineExceeded, "folded values must wait for matching root publication")
	require.Equal(t, uint64(20), remaining, "waiting without hydration does not charge")
	server.SetRootedBankState(43, 43, 0)
	got, err = client.GetMultipleAccounts(t.Context(), acct.Key, second)
	require.NoError(t, err)
	require.Equal(t, uint64(43), got.Context.Slot)
	require.Equal(t, uint64(43), got.Value[0].Lamports)
	require.Equal(t, uint64(43), got.Value[1].Lamports)
	broken := filepath.Join(server.acctsDb.AcctsDir, "rewound")
	require.NoError(t, os.WriteFile(broken, nil, 0o600))
	_, err = server.acctsDb.RewindToBatchBoundary(42)
	require.Error(t, err)
	ctx2, cancel2 := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel2()
	_, _, err = server.readRootedAccountsWithBudget(ctx2, []solana.PublicKey{acct.Key, second}, 0, &remaining)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, os.Remove(broken))
	_, err = server.acctsDb.RewindToBatchBoundary(42)
	require.NoError(t, err)
	server.SetRootedBankState(42, 42, 0)
	got, err = client.GetMultipleAccounts(t.Context(), acct.Key, second)
	require.NoError(t, err)
	require.Equal(t, uint64(42), got.Context.Slot)
	require.Equal(t, uint64(42), got.Value[0].Lamports)
	require.Nil(t, got.Value[1])
	_, _, err = server.readRootedAccountsWithBudget(t.Context(), []solana.PublicKey{acct.Key}, 0, &remaining)
	require.ErrorIs(t, err, accountsdb.ErrAccountDataLimit)
}

func TestGetMultipleAccountsResponseAdmission(t *testing.T) {
	server, original, _ := multipleAccountsFixture(t, &accounts.Account{Key: solana.PublicKey{1}, Lamports: 1})
	original.Close()
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Hold") == "yes" {
			w = &queryResponseWriteGate{ResponseWriter: w, ctx: r.Context(), entered: entered, release: release}
		}
		server.ServeHTTP(w, r)
	}))
	t.Cleanup(endpoint.Close)
	endpoint.Client().Timeout = 3 * time.Second
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			req, err := http.NewRequest(http.MethodPost, endpoint.URL, strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getMultipleAccounts","params":[[%q]]}`, solana.PublicKey{1})))
			if err != nil {
				results <- err
				return
			}
			req.Header.Set("X-Hold", "yes")
			got, err := endpoint.Client().Do(req)
			if err == nil {
				var reply struct {
					Result *GetMultipleAccountsResp `json:"result"`
				}
				err = json.NewDecoder(got.Body).Decode(&reply)
				closeErr := got.Body.Close()
				if err == nil {
					err = closeErr
				}
				if err == nil && (reply.Result == nil || len(reply.Result.Value) != 1 || reply.Result.Value[0] == nil || reply.Result.Value[0].Lamports != 1) {
					err = fmt.Errorf("held response did not contain the expected account: %+v", reply.Result)
				}
			}
			results <- err
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("responses did not reach write")
		}
	}
	require.Equal(t, 2, len(server.queryResponses))
	body := getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", `[[]]`)
	requireGetBlockHistoryError(t, body, 1, "query response unavailable")
	body = getBlockHistoryHTTPCall(t, endpoint, "getBalance", fmt.Sprintf(`[%q]`, solana.PublicKey{1}))
	require.NotContains(t, body, "error")
	finish()
	for i := 0; i < 2; i++ {
		require.NoError(t, <-results)
	}
	require.Eventually(t, func() bool { return len(server.queryResponses) == 0 }, time.Second, time.Millisecond)
	_, err := server.GetMultipleAccounts(t.Context(), []byte(`[[]]`))
	require.ErrorIs(t, err, errQueryResponseUnavailable, "no direct/WS payload route without HTTP ownership")
}

func TestGetMultipleAccountsWebSocketRefusalPreservesCheapMethods(t *testing.T) {
	server, endpoint, _ := multipleAccountsFixture(t, &accounts.Account{Key: solana.PublicKey{1}, Lamports: 7})
	_, refused, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(endpoint.URL, "http"), nil)
	require.Error(t, err)
	require.NotNil(t, refused)
	require.Equal(t, http.StatusBadRequest, refused.StatusCode, "outer quiet-probe wrapper already rejects upgrade GETs")
	require.NoError(t, refused.Body.Close())
	probe := newQueryWebSocketProbe(t, server)
	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(probe.URL, "http"), nil)
	require.NoError(t, err)
	defer connection.Close()
	require.NoError(t, connection.SetReadDeadline(time.Now().Add(2*time.Second)))
	require.NoError(t, connection.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"getMultipleAccounts","params":[[]]}`)))
	_, data, err := connection.ReadMessage()
	require.NoError(t, err)
	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &body))
	requireGetBlockHistoryError(t, body, 1, "query response unavailable")
	require.NoError(t, connection.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"getBalance","params":[%q]}`, solana.PublicKey{1}))))
	_, data, err = connection.ReadMessage()
	require.NoError(t, err)
	var reply struct {
		Result *GetBalanceResp `json:"result"`
	}
	require.NoError(t, json.Unmarshal(data, &reply))
	require.NotNil(t, reply.Result)
	require.Equal(t, uint64(7), reply.Result.Value)
	require.Zero(t, len(server.queryResponses))
}
