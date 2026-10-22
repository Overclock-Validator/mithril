package rpcserver

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestGetTransactionHistoryHTTPAndSDK(t *testing.T) {
	for _, version := range []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0, solana.MessageVersionV1} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			server, block, endpoint := getBlockHistoryHTTPFixture(t)
			record, _, _ := historyResponseFixture(t, version)
			block.Records = []transactionhistory.Record{record}
			require.NoError(t, server.transactionHistory.WriteBlock(block))
			signature := solana.Signature(record.FirstSignature)
			client := rpc.New(endpoint.URL)
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			maximum := uint64(1)
			for _, encoding := range []solana.EncodingType{solana.EncodingJSON, solana.EncodingBase64} {
				got, err := client.GetTransaction(t.Context(), signature, &rpc.GetTransactionOpts{Encoding: encoding, MaxSupportedTransactionVersion: &maximum})
				require.NoError(t, err)
				require.Equal(t, uint64(20), got.Slot)
				require.NotNil(t, got.BlockTime)
				require.Equal(t, int64(1_700_000_000), int64(*got.BlockTime))
				tx, err := got.Transaction.GetTransaction()
				require.NoError(t, err)
				wire, err := tx.MarshalBinary()
				require.NoError(t, err)
				require.Equal(t, record.Transaction, wire)
				transactionBody := getBlockHistoryHTTPCall(t, endpoint, "getTransaction", fmt.Sprintf(`[%q,{"encoding":%q,"maxSupportedTransactionVersion":1}]`, signature, encoding))
				blockBody := getBlockHistoryHTTPCall(t, endpoint, "getBlock", fmt.Sprintf(`[20,{"encoding":%q,"maxSupportedTransactionVersion":1}]`, encoding))
				require.NotContains(t, transactionBody, "error")
				require.NotContains(t, blockBody, "error")
				transaction := historyResponseObject(t, transactionBody["result"])
				blockResult := historyResponseObject(t, blockBody["result"])
				var records []map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(blockResult["transactions"], &records))
				require.Len(t, records, 1)
				for _, field := range []string{"transaction", "meta", "version"} {
					require.JSONEq(t, string(records[0][field]), string(transaction[field]), field)
				}
			}
			if version != solana.MessageVersionLegacy {
				body := getBlockHistoryHTTPCall(t, endpoint, "getTransaction", fmt.Sprintf(`[%q]`, signature))
				var rpcError struct {
					Code int `json:"code"`
				}
				require.NoError(t, json.Unmarshal(body["error"], &rpcError))
				require.Equal(t, -32015, rpcError.Code)
				require.NotContains(t, body, "result")
			}
		})
	}
}

func TestGetTransactionHistorySelection(t *testing.T) {
	for _, name := range []string{"unknown", "orphan", "unconfirmed", "pruned", "canonical-with-orphan", "pending", "damaged-metadata", "bankhash-mismatch", "duplicate-canonical", "disabled"} {
		t.Run(name, func(t *testing.T) {
			server, block, endpoint := getBlockHistoryHTTPFixture(t)
			signature := solana.Signature(block.Records[0].FirstSignature)
			switch name {
			case "unknown":
				signature[0]++
			case "orphan":
				block.ID.Blockhash[0]++
			case "unconfirmed":
				server.SetRootedBankState(19, 6, 0)
			case "pending":
				server.SetHistoryRecoveryPending(true)
			case "damaged-metadata":
				block.Records[0].Metadata = []byte{1}
			case "bankhash-mismatch":
				block.Bankhash[0]++
			case "duplicate-canonical":
				block.Records = append(block.Records, block.Records[0])
			case "canonical-with-orphan":
				orphan := block
				orphan.ID.Blockhash[0]++
				require.NoError(t, server.transactionHistory.WriteBlock(orphan))
			}
			require.NoError(t, server.transactionHistory.WriteBlock(block))
			if name == "pruned" {
				_, err := server.transactionHistory.PruneBefore(21, 1)
				require.NoError(t, err)
			}
			if name == "disabled" {
				server.InitTransactionHistory(nil)
			}
			body := getBlockHistoryHTTPCall(t, endpoint, "getTransaction", fmt.Sprintf(`[%q]`, signature))
			switch name {
			case "unknown", "orphan", "unconfirmed", "pruned":
				require.NotContains(t, body, "error")
				require.Equal(t, "null", string(body["result"]))
			case "canonical-with-orphan":
				require.NotContains(t, body, "error")
				require.NotEqual(t, "null", string(body["result"]))
			case "disabled":
				requireGetBlockHistoryError(t, body, -32011, "Transaction history is not available from this node")
			default:
				require.Contains(t, body, "error")
				require.NotContains(t, body, "result")
			}
		})
	}
}

func TestGetTransactionHistoryConfig(t *testing.T) {
	server, block, endpoint := getBlockHistoryHTTPFixture(t)
	require.NoError(t, server.transactionHistory.WriteBlock(block))
	signature := solana.Signature(block.Records[0].FirstSignature).String()
	for _, suffix := range []string{"", `,null`, `,{}`, `,"base64"`, `,{"encoding":null,"commitment":null,"maxSupportedTransactionVersion":null}`, `,{"commitment":"confirmed"}`, `,{"commitment":"finalized"}`} {
		body := getBlockHistoryHTTPCall(t, endpoint, "getTransaction", fmt.Sprintf(`[%q%s]`, signature, suffix))
		require.NotContains(t, body, "error", suffix)
		result := historyResponseObject(t, body["result"])
		require.NotContains(t, result, "version", "legacy without max-version omits version")
	}
	for _, config := range []string{`[]`, `1`, `true`, `"base58"`, `{"encoding":"jsonParsed"}`, `{"commitment":"processed"}`, `{"encoding":false}`, `{"maxSupportedTransactionVersion":-1}`, `{"maxSupportedTransactionVersion":256}`, `{"maxSupportedTransactionVersion":0.9}`, `{"maxSupportedTransactionVersion":1e0}`, `{"maxSupportedTransactionVersion":"0"}`} {
		body := getBlockHistoryHTTPCall(t, endpoint, "getTransaction", fmt.Sprintf(`[%q,%s]`, signature, config))
		var rpcError struct {
			Code int `json:"code"`
		}
		require.NoError(t, json.Unmarshal(body["error"], &rpcError))
		require.Equal(t, -32602, rpcError.Code, config)
		require.NotContains(t, body, "result")
	}
	for _, params := range []string{`[]`, `[null]`, `[1]`, `["bad"]`, fmt.Sprintf(`[%q]`, strings.Repeat("1", 89)), fmt.Sprintf(`[%q,{},{}]`, signature)} {
		body := getBlockHistoryHTTPCall(t, endpoint, "getTransaction", params)
		var rpcError struct {
			Code int `json:"code"`
		}
		require.NoError(t, json.Unmarshal(body["error"], &rpcError))
		require.Equal(t, -32602, rpcError.Code, params)
	}
}
