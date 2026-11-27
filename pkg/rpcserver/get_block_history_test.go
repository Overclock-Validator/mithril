package rpcserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestBlockHistoryRPCServesExporterContract(t *testing.T) {
	store, err := blockhistory.Open(t.TempDir(), 100)
	require.NoError(t, err)
	leader := solana.PublicKey{1}
	require.NoError(t, store.RecordSkipped(40))
	require.NoError(t, store.RecordBlock(&b.Block{
		Slot:          41,
		ParentSlot:    39,
		BlockHeight:   30,
		Blockhash:     solana.Hash{2},
		LastBlockhash: solana.Hash{3},
		Rewards: []rpc.BlockReward{{
			Pubkey: leader, Lamports: 25, PostBalance: 1_025, RewardType: rpc.RewardTypeFee,
		}},
	}))
	require.NoError(t, store.Prepare(41))
	require.NoError(t, store.SetRooted(41))
	server := &RpcServer{blockHistory: store}

	minimum, err := server.MinimumLedgerSlot(t.Context(), jsonrpc.RawParams(`[]`))
	require.NoError(t, err)
	require.Equal(t, uint64(40), minimum)
	first, err := server.GetFirstAvailableBlock(t.Context(), jsonrpc.RawParams(`[]`))
	require.NoError(t, err)
	require.Equal(t, uint64(41), first)
	for _, params := range []jsonrpc.RawParams{nil, jsonrpc.RawParams(`null`)} {
		minimum, err = server.MinimumLedgerSlot(t.Context(), params)
		require.NoError(t, err)
		require.Equal(t, uint64(40), minimum)
		first, err = server.GetFirstAvailableBlock(t.Context(), params)
		require.NoError(t, err)
		require.Equal(t, uint64(41), first)
	}

	response, err := server.GetBlock(t.Context(), jsonrpc.RawParams(`[41,{"commitment":"confirmed","encoding":"json","transactionDetails":"none","rewards":true,"maxSupportedTransactionVersion":0}]`))
	require.NoError(t, err)
	require.Equal(t, uint64(30), response.BlockHeight)
	require.Equal(t, int64(25), response.Rewards[0].Lamports)
	require.Equal(t, leader, response.Rewards[0].Pubkey)
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"commission":null`)

	_, err = server.GetBlock(t.Context(), jsonrpc.RawParams(`[40,{"transactionDetails":"none"}]`))
	var skipped *SlotSkippedError
	require.ErrorAs(t, err, &skipped)
	require.Equal(t, uint64(40), skipped.Slot)

	_, err = server.GetBlock(t.Context(), jsonrpc.RawParams(`[42,{"transactionDetails":"none"}]`))
	var unavailable *BlockNotAvailableError
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, uint64(42), unavailable.Slot)
}

func TestGetBlockRejectsUnretainedTransactionDetails(t *testing.T) {
	store, err := blockhistory.Open(t.TempDir(), 100)
	require.NoError(t, err)
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 1}))
	require.NoError(t, store.Prepare(1))
	require.NoError(t, store.SetRooted(1))
	server := &RpcServer{blockHistory: store}
	_, err = server.GetBlock(t.Context(), jsonrpc.RawParams(`[1,{"transactionDetails":"full"}]`))
	var invalid *InvalidParamsError
	require.ErrorAs(t, err, &invalid)
	require.Contains(t, invalid.Error(), "transactions are not retained")
	_, err = server.GetBlock(t.Context(), jsonrpc.RawParams(`[2,{"transactionDetails":"full"}]`))
	require.ErrorAs(t, err, &invalid)
}

func TestGetBlockRewardsHTTP(t *testing.T) {
	store, err := blockhistory.Open(t.TempDir(), 100)
	require.NoError(t, err)
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 1, BlockHeight: 30, Rewards: []rpc.BlockReward{{
		Pubkey: solana.PublicKey{}, Lamports: 25, PostBalance: 1_025, RewardType: rpc.RewardTypeFee,
	}}}))
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 2, BlockHeight: 30}))
	require.NoError(t, store.Prepare(2))
	require.NoError(t, store.SetRooted(2))
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}, solana.Hash{})
	require.NoError(t, server.listener.Close())
	server.blockHistory = store
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	const rewards = `[{"pubkey":"11111111111111111111111111111111","lamports":25,"postBalance":1025,"rewardType":"Fee","commission":null}]`
	const fullUnavailable = "getBlock transactionDetails: full is not supported because transactions are not retained"
	// Agave 1407e9c6 defaults to full/JSON/rewards=true. None does not encode
	// transactions, but encoding enums and the base58/version constraint still
	// validate. This summary-only server must reject, not downgrade, full.
	for _, test := range []struct {
		name        string
		params      string
		wantRewards string
		wantError   string
		wantInvalid bool
		wantCode    int
	}{
		{name: "default", params: `[1,{"transactionDetails":"none"}]`, wantRewards: rewards},
		{name: "true", params: `[1,{"transactionDetails":"none","rewards":true}]`, wantRewards: rewards},
		{name: "false", params: `[1,{"transactionDetails":"none","rewards":false}]`},
		{name: "empty-default", params: `[2,{"transactionDetails":"none"}]`, wantRewards: `[]`},
		{name: "empty-true", params: `[2,{"transactionDetails":"none","rewards":true}]`, wantRewards: `[]`},
		{name: "empty-false", params: `[2,{"transactionDetails":"none","rewards":false}]`},
		{name: "invalid-rewards", params: `[1,{"transactionDetails":"none","rewards":"false"}]`, wantError: "invalid rewards"},
		{name: "full-without-rewards", params: `[1,{"transactionDetails":"full","rewards":false}]`, wantError: fullUnavailable},
		{name: "exporter-none-json", params: `[1,{"commitment":"confirmed","encoding":"json","transactionDetails":"none","rewards":true,"maxSupportedTransactionVersion":0}]`, wantRewards: rewards},
		{name: "none-finalized", params: `[1,{"commitment":"finalized","transactionDetails":"none"}]`, wantRewards: rewards},
		{name: "none-base64", params: `[1,{"transactionDetails":"none","encoding":"base64"}]`, wantRewards: rewards},
		{name: "none-base58", params: `[1,{"transactionDetails":"none","encoding":"base58","maxSupportedTransactionVersion":0}]`, wantRewards: rewards},
		{name: "none-binary", params: `[1,{"transactionDetails":"none","encoding":"binary"}]`, wantRewards: rewards},
		{name: "none-json-parsed", params: `[1,{"transactionDetails":"none","encoding":"jsonParsed"}]`, wantRewards: rewards},
		{name: "none-base64-without-rewards", params: `[1,{"transactionDetails":"none","encoding":"base64","rewards":false}]`},
		{name: "none-null-options", params: `[1,{"transactionDetails":"none","encoding":null,"commitment":null,"rewards":null,"maxSupportedTransactionVersion":null}]`, wantRewards: rewards},
		{name: "none-json-version-one", params: `[1,{"transactionDetails":"none","maxSupportedTransactionVersion":1}]`, wantRewards: rewards},
		{name: "none-base64-version-255", params: `[1,{"transactionDetails":"none","encoding":"base64","maxSupportedTransactionVersion":255}]`, wantRewards: rewards},
		{name: "omitted-config", params: `[1]`, wantError: fullUnavailable},
		{name: "null-config", params: `[1,null]`, wantError: fullUnavailable},
		{name: "empty-config", params: `[1,{}]`, wantError: fullUnavailable},
		{name: "omitted-details", params: `[1,{"encoding":"json","rewards":false}]`, wantError: fullUnavailable},
		{name: "null-details", params: `[1,{"transactionDetails":null}]`, wantError: fullUnavailable},
		{name: "legacy-encoding-config", params: `[1,"base64"]`, wantError: fullUnavailable},
		{name: "unsupported-signatures", params: `[1,{"transactionDetails":"signatures"}]`, wantInvalid: true},
		{name: "unsupported-accounts", params: `[1,{"transactionDetails":"accounts"}]`, wantInvalid: true},
		{name: "unknown-details", params: `[1,{"transactionDetails":"partial"}]`, wantInvalid: true},
		{name: "invalid-details-type", params: `[1,{"transactionDetails":1}]`, wantInvalid: true},
		{name: "unknown-encoding", params: `[1,{"transactionDetails":"none","encoding":"base64+zstd"}]`, wantInvalid: true},
		{name: "invalid-encoding-type", params: `[1,{"transactionDetails":"none","encoding":false}]`, wantInvalid: true},
		{name: "invalid-config-type", params: `[1,[]]`, wantInvalid: true},
		{name: "processed-commitment", params: `[1,{"transactionDetails":"none","commitment":"processed"}]`, wantInvalid: true},
		{name: "negative-version", params: `[1,{"transactionDetails":"none","maxSupportedTransactionVersion":-1}]`, wantInvalid: true},
		{name: "overflow-version", params: `[1,{"transactionDetails":"none","maxSupportedTransactionVersion":256}]`, wantInvalid: true},
		{name: "fractional-version", params: `[1,{"transactionDetails":"none","maxSupportedTransactionVersion":0.5}]`, wantInvalid: true},
		{name: "invalid-version-type", params: `[1,{"transactionDetails":"none","maxSupportedTransactionVersion":"0"}]`, wantInvalid: true},
		{name: "rounded-fractional-version", params: `[1,{"transactionDetails":"none","maxSupportedTransactionVersion":0.99999999999999999999}]`, wantInvalid: true},
		{name: "decimal-integer-version", params: `[1,{"transactionDetails":"none","maxSupportedTransactionVersion":1.0}]`, wantInvalid: true},
		{name: "exponent-integer-version", params: `[1,{"transactionDetails":"none","maxSupportedTransactionVersion":1e0}]`, wantInvalid: true},
		{name: "rounded-fractional-slot", params: `[0.99999999999999999999,{"transactionDetails":"none"}]`, wantInvalid: true},
		{name: "decimal-integer-slot", params: `[1.0,{"transactionDetails":"none"}]`, wantInvalid: true},
		{name: "exponent-integer-slot", params: `[1e0,{"transactionDetails":"none"}]`, wantInvalid: true},
		{name: "null-slot", params: `[null,{"transactionDetails":"none"}]`, wantInvalid: true},
		{name: "negative-slot", params: `[-1,{"transactionDetails":"none"}]`, wantInvalid: true},
		{name: "overflow-slot", params: `[18446744073709551616,{"transactionDetails":"none"}]`, wantInvalid: true},
		{name: "exact-large-slot", params: `[9007199254740993,{"transactionDetails":"none"}]`, wantCode: -32004, wantError: "Block not available for slot 9007199254740993"},
		{name: "max-uint64-slot", params: `[18446744073709551615,{"transactionDetails":"none"}]`, wantCode: -32004, wantError: "Block not available for slot 18446744073709551615"},
		{name: "base58-version-one", params: `[1,{"transactionDetails":"none","encoding":"base58","maxSupportedTransactionVersion":1}]`, wantError: "base58 encoding is not supported with maxSupportedTransactionVersion >= 1"},
		{name: "binary-version-one", params: `[1,{"transactionDetails":"none","encoding":"binary","maxSupportedTransactionVersion":1}]`, wantError: "base58 encoding is not supported with maxSupportedTransactionVersion >= 1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := `{"jsonrpc":"2.0","id":7,"method":"getBlock","params":` + test.params + `}`
			response, err := httpServer.Client().Post(httpServer.URL, "application/json", strings.NewReader(request))
			require.NoError(t, err)
			defer response.Body.Close()
			var reply struct {
				JSONRPC string                     `json:"jsonrpc"`
				ID      int                        `json:"id"`
				Result  map[string]json.RawMessage `json:"result"`
				Error   *rpcProbeError             `json:"error"`
			}
			require.NoError(t, json.NewDecoder(response.Body).Decode(&reply))
			require.Equal(t, "2.0", reply.JSONRPC)
			require.Equal(t, 7, reply.ID)
			if test.wantError != "" || test.wantInvalid {
				require.NotNil(t, reply.Error)
				wantCode := test.wantCode
				if wantCode == 0 {
					wantCode = -32602
				}
				require.Equal(t, wantCode, reply.Error.Code)
				if test.wantError != "" {
					require.Equal(t, test.wantError, reply.Error.Message)
				}
				require.Nil(t, reply.Result)
				return
			}
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Nil(t, reply.Error)
			if test.wantRewards == "" {
				require.NotContains(t, reply.Result, "rewards")
			} else {
				require.JSONEq(t, test.wantRewards, string(reply.Result["rewards"]))
			}
			delete(reply.Result, "rewards")
			require.Equal(t, map[string]json.RawMessage{
				"blockHeight": json.RawMessage(`30`), "blockTime": json.RawMessage(`null`),
				"blockhash":         json.RawMessage(`"11111111111111111111111111111111"`),
				"previousBlockhash": json.RawMessage(`"11111111111111111111111111111111"`),
				"parentSlot":        json.RawMessage(`0`),
			}, reply.Result)
		})
	}
}
