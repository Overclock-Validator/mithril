package rpcserver

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

// These tests exercise the registered HTTP handler, real retained stores, and
// SDK decoding. The retained transactions are structural fixtures, not replay
// execution or finality proof; no current account/lookup reader is supplied.
func getBlockHistoryHTTPFixture(t *testing.T) (*RpcServer, transactionhistory.Block, *httptest.Server) {
	t.Helper()
	fixture, block := transactionHistoryReadFixture(t)
	server := NewRpcServer(fixture.acctsDb, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	server.blockHistory = fixture.blockHistory
	server.InitTransactionHistory(fixture.transactionHistory)
	server.SetRootedBankState(20, 7, 1)
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	return server, block, endpoint
}

func getBlockHistoryHTTPCall(t *testing.T, endpoint *httptest.Server, method, params string, status ...int) map[string]json.RawMessage {
	t.Helper()
	request := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, method, params)
	response, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(request))
	require.NoError(t, err)
	defer response.Body.Close()
	wantStatus := 200
	if len(status) != 0 {
		wantStatus = status[0]
	}
	require.Equal(t, wantStatus, response.StatusCode)
	var body map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.JSONEq(t, `"2.0"`, string(body["jsonrpc"]))
	require.JSONEq(t, `1`, string(body["id"]))
	return body
}

func requireGetBlockHistoryError(t *testing.T, body map[string]json.RawMessage, code int, message string) {
	t.Helper()
	require.NotContains(t, body, "result", "an error must not leak a partial block")
	var rpcError struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body["error"], &rpcError))
	require.Equal(t, code, rpcError.Code)
	require.Equal(t, message, rpcError.Message)
	require.NotContains(t, historyResponseObject(t, body["error"]), "data")
}

func TestGetBlockTransactionHistoryHTTPAndSDK(t *testing.T) {
	server, block, endpoint := getBlockHistoryHTTPFixture(t)
	block.Records = nil
	var expectedMetadata []*transactionhistory.Metadata
	for _, version := range []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0, solana.MessageVersionV1} {
		record, _, metadata := historyResponseFixture(t, version)
		block.Records = append(block.Records, record)
		expectedMetadata = append(expectedMetadata, metadata)
	}
	require.NoError(t, server.transactionHistory.WriteBlock(block))
	maximum := uint64(1)
	client := rpc.New(endpoint.URL)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	for _, encoding := range []solana.EncodingType{solana.EncodingJSON, solana.EncodingBase64} {
		t.Run(string(encoding), func(t *testing.T) {
			got, err := client.GetBlockWithOpts(t.Context(), 20, &rpc.GetBlockOpts{Encoding: encoding, MaxSupportedTransactionVersion: &maximum})
			require.NoError(t, err)
			require.Equal(t, solana.Hash(block.ID.Blockhash), got.Blockhash)
			require.Equal(t, solana.Hash(block.PreviousBlockhash), got.PreviousBlockhash)
			require.Equal(t, block.ParentSlot, got.ParentSlot)
			require.NotNil(t, got.BlockHeight)
			require.Equal(t, uint64(7), *got.BlockHeight)
			require.NotNil(t, got.BlockTime)
			require.Equal(t, int64(1_700_000_000), int64(*got.BlockTime))
			require.Len(t, got.Transactions, len(block.Records))
			for i, transaction := range got.Transactions {
				decoded, err := transaction.GetTransaction()
				require.NoError(t, err)
				wire, err := decoded.MarshalBinary()
				require.NoError(t, err)
				require.Equal(t, block.Records[i].Transaction, wire, "record %d", i)
				require.NotNil(t, transaction.Meta)
				require.Nil(t, transaction.Meta.Err)
				require.Equal(t, expectedMetadata[i].Fee, transaction.Meta.Fee)
				require.Equal(t, expectedMetadata[i].PreBalances, transaction.Meta.PreBalances)
				require.Equal(t, expectedMetadata[i].PostBalances, transaction.Meta.PostBalances)
				require.NotNil(t, transaction.Meta.ComputeUnitsConsumed)
				require.Zero(t, *transaction.Meta.ComputeUnitsConsumed)
			}
			require.Equal(t, expectedMetadata[1].AccountKeys[2:5], []solana.PublicKey(got.Transactions[1].Meta.LoadedAddresses.Writable))
			require.Equal(t, expectedMetadata[1].AccountKeys[5:], []solana.PublicKey(got.Transactions[1].Meta.LoadedAddresses.ReadOnly))
			body := getBlockHistoryHTTPCall(t, endpoint, "getBlock", fmt.Sprintf(`[20,{"encoding":%q,"maxSupportedTransactionVersion":1}]`, encoding))
			require.NotContains(t, body, "error")
			result := historyResponseObject(t, body["result"])
			var transactions []map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(result["transactions"], &transactions))
			require.Len(t, transactions, 3)
			for i, transaction := range transactions {
				if encoding == solana.EncodingBase64 {
					var tuple []string
					require.NoError(t, json.Unmarshal(transaction["transaction"], &tuple))
					require.Equal(t, []string{base64.StdEncoding.EncodeToString(block.Records[i].Transaction), "base64"}, tuple)
				} else {
					require.Contains(t, historyResponseObject(t, transaction["transaction"]), "message")
				}
			}
			require.JSONEq(t, `"legacy"`, string(transactions[0]["version"]))
			require.JSONEq(t, `0`, string(transactions[1]["version"]))
			require.JSONEq(t, `1`, string(transactions[2]["version"]))
			require.Zero(t, len(server.transactionHistoryReads))
		})
	}
}

func TestGetBlockTransactionHistoryDefaultsAndEmpty(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty=%t", empty), func(t *testing.T) {
			server, block, endpoint := getBlockHistoryHTTPFixture(t)
			if empty {
				block.Records = nil
			}
			require.NoError(t, server.transactionHistory.WriteBlock(block))
			for _, params := range []string{`[20]`, `[20,null]`, `[20,{}]`, `[20,{"transactionDetails":"full","encoding":"json"}]`} {
				body := getBlockHistoryHTTPCall(t, endpoint, "getBlock", params)
				require.NotContains(t, body, "error", params)
				result := historyResponseObject(t, body["result"])
				require.JSONEq(t, `[]`, string(result["rewards"]))
				var transactions []map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(result["transactions"], &transactions))
				require.Len(t, transactions, len(block.Records))
				if empty {
					require.JSONEq(t, `[]`, string(result["transactions"]))
				} else {
					require.NotContains(t, transactions[0], "version")
					require.Contains(t, historyResponseObject(t, transactions[0]["transaction"]), "message")
				}
			}
			body := getBlockHistoryHTTPCall(t, endpoint, "getBlock", `[20,"base64"]`)
			require.NotContains(t, body, "error")
		})
	}
}

func TestGetBlockTransactionHistoryRewardsAndNone(t *testing.T) {
	for _, state := range []string{"available", "missing-payload", "corrupt-metadata", "version-mismatch", "capture-disabled"} {
		t.Run(state, func(t *testing.T) {
			server, block, endpoint := getBlockHistoryHTTPFixture(t)
			switch state {
			case "corrupt-metadata":
				block.Records[0].Metadata = []byte("invalid retained metadata")
			case "version-mismatch":
				block.Records[0], _, _ = historyResponseFixture(t, solana.MessageVersionV1)
			}
			if state != "missing-payload" {
				require.NoError(t, server.transactionHistory.WriteBlock(block))
			}
			if state == "capture-disabled" {
				server.InitTransactionHistory(nil)
			}
			require.NoError(t, server.RecordBlockHistory(&b.Block{
				Slot: 20, ParentSlot: 10, BlockHeight: 7, Blockhash: solana.Hash(block.ID.Blockhash), LastBlockhash: solana.Hash(block.PreviousBlockhash),
				UnixTimestamp: 1_700_000_000, Rewards: []rpc.BlockReward{{Pubkey: solana.PublicKey{99}, Lamports: 12, PostBalance: 34, RewardType: rpc.RewardTypeFee}},
			}))
			require.NoError(t, server.PrepareBlockHistory(20))
			for _, encoding := range []string{"json", "base64", "jsonParsed", "base58", "binary"} {
				body := getBlockHistoryHTTPCall(t, endpoint, "getBlock", fmt.Sprintf(`[20,{"transactionDetails":"none","encoding":%q,"maxSupportedTransactionVersion":0}]`, encoding))
				require.NotContains(t, body, "error")
				result := historyResponseObject(t, body["result"])
				require.NotContains(t, result, "transactions")
				var rewards []map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(result["rewards"], &rewards))
				require.Len(t, rewards, 1)
				require.JSONEq(t, `null`, string(rewards[0]["commission"]))
				require.JSONEq(t, `12`, string(rewards[0]["lamports"]))
			}
			body := getBlockHistoryHTTPCall(t, endpoint, "getBlock", `[20,{"transactionDetails":"none","rewards":false}]`)
			require.NotContains(t, body, "error")
			require.NotContains(t, historyResponseObject(t, body["result"]), "rewards")
			body = getBlockHistoryHTTPCall(t, endpoint, "getBlock", `[20,{"rewards":false,"maxSupportedTransactionVersion":0}]`)
			switch state {
			case "available":
				require.NotContains(t, body, "error")
				result := historyResponseObject(t, body["result"])
				require.NotContains(t, result, "rewards")
				var transactions []map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(result["transactions"], &transactions))
				require.Len(t, transactions, 1)
				require.JSONEq(t, `null`, string(historyResponseObject(t, transactions[0]["meta"])["rewards"]))
			case "version-mismatch":
				requireGetBlockHistoryError(t, body, -32015, (&historyUnsupportedVersionError{Version: 1}).Error())
			case "capture-disabled":
				requireGetBlockHistoryError(t, body, -32602, "getBlock transactionDetails: full is not supported because transactions are not retained")
			default:
				requireGetBlockHistoryError(t, body, -32004, "Block not available for slot 20")
			}
		})
	}
}

func TestGetBlockTransactionHistoryOptionErrors(t *testing.T) {
	_, _, endpoint := getBlockHistoryHTTPFixture(t)
	for _, test := range []struct{ params, message string }{
		{`[20,{"encoding":"base58"}]`, "transaction history supports json and base64 encoding"},
		{`[20,{"encoding":"binary"}]`, "transaction history supports json and base64 encoding"},
		{`[20,{"encoding":"jsonParsed"}]`, "transaction history supports json and base64 encoding"},
		{`[20,{"transactionDetails":"accounts"}]`, "transaction history supports full and none transactionDetails"},
		{`[20,{"transactionDetails":"signatures"}]`, "transaction history supports full and none transactionDetails"},
		{`[20,{"encoding":"base64+zstd"}]`, "invalid encoding"},
		{`[20,{"transactionDetails":"unknown"}]`, "invalid transactionDetails"},
		{`[20,{"rewards":"false"}]`, "invalid rewards"},
		{`[20,{"maxSupportedTransactionVersion":0.99999999999999999999}]`, "invalid maxSupportedTransactionVersion"},
		{`[20,{"maxSupportedTransactionVersion":256}]`, "invalid maxSupportedTransactionVersion"},
		{`[20,{"commitment":"processed"}]`, "getBlock requires confirmed or finalized commitment"},
		{`[20,{"encoding":"base58","transactionDetails":"none","maxSupportedTransactionVersion":1}]`, "base58 encoding is not supported with maxSupportedTransactionVersion >= 1"},
		{`[20,[]]`, "invalid getBlock config"},
		{`[20.0]`, "invalid slot"},
		{`[null]`, "invalid slot"},
		{`[18446744073709551616]`, "invalid slot"},
		{`[]`, "getBlock requires a slot and optional config"},
		{`[20,{},{}]`, "getBlock requires a slot and optional config"},
	} {
		t.Run(test.params, func(t *testing.T) {
			requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlock", test.params), -32602, test.message)
		})
	}
}

func TestGetBlockTransactionHistoryUnavailableHTTP(t *testing.T) {
	for _, state := range []string{"pruned", "root-behind", "durable-rewind", "bankhash-mismatch", "different-selected-fork", "pending", "skipped"} {
		t.Run(state, func(t *testing.T) {
			server, block, endpoint := getBlockHistoryHTTPFixture(t)
			require.NoError(t, server.transactionHistory.WriteBlock(block))
			switch state {
			case "pruned":
				count, err := server.transactionHistory.PruneBefore(21, 1)
				require.NoError(t, err)
				require.Equal(t, uint32(1), count)
			case "root-behind":
				server.SetRootedBankState(19, 6, 0)
			case "durable-rewind":
				_, err := server.acctsDb.RewindToBatchBoundary(10)
				require.NoError(t, err)
			case "bankhash-mismatch":
				require.NoError(t, server.acctsDb.StoreBankHashForSlot(20, make([]byte, 32)))
			case "different-selected-fork":
				block.ID.Blockhash[0]++
				publishTransactionHistorySummary(t, server, block)
			case "pending":
				server.SetHistoryRecoveryPending(true)
			case "skipped":
				require.NoError(t, server.RecordSkippedBlockHistory(20))
				require.NoError(t, server.PrepareBlockHistory(20))
			}
			code, message := -32004, "Block not available for slot 20"
			if state == "skipped" {
				code, message = -32007, "Slot 20 was skipped"
			}
			requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlock", `[20]`), code, message)
			require.Zero(t, len(server.transactionHistoryReads))
		})
	}
}

func TestGetBlockTransactionHistoryVersionErrorHTTP(t *testing.T) {
	for _, version := range []solana.MessageVersion{solana.MessageVersionV0, solana.MessageVersionV1} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			server, block, endpoint := getBlockHistoryHTTPFixture(t)
			record, _, _ := historyResponseFixture(t, version)
			block.Records = append(block.Records, record) // A later failure must discard the earlier rendered record.
			require.NoError(t, server.transactionHistory.WriteBlock(block))
			requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlock", `[20]`), -32015, (&historyUnsupportedVersionError{Version: uint8(version - solana.MessageVersionV0)}).Error())
			if version == solana.MessageVersionV1 {
				requireGetBlockHistoryError(t, getBlockHistoryHTTPCall(t, endpoint, "getBlock", `[20,{"encoding":"base64","maxSupportedTransactionVersion":0}]`), -32015, (&historyUnsupportedVersionError{Version: 1}).Error())
			}
			require.Zero(t, len(server.transactionHistoryReads))
		})
	}
}

func TestGetBlockTransactionHistoryInitializerNotRPC(t *testing.T) {
	server, block, endpoint := getBlockHistoryHTTPFixture(t)
	require.NoError(t, server.transactionHistory.WriteBlock(block))
	store := server.transactionHistory
	body := getBlockHistoryHTTPCall(t, endpoint, "initTransactionHistory", `[null]`, 500)
	requireGetBlockHistoryError(t, body, -32601, "method 'initTransactionHistory' not found")
	require.Same(t, store, server.transactionHistory)
	require.NotContains(t, getBlockHistoryHTTPCall(t, endpoint, "getBlock", `[20]`), "error")
	// Also bypass the quiet unknown-method filter using the existing direct
	// dispatch, mixed-batch, and oversized-request registration checks.
	testInternalMethodNotRPC(t, "initTransactionHistory")
}
