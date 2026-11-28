package rpcserver

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestSignatureStatusesCommitmentContextFloor(t *testing.T) {
	server, _, endpoint, _ := signatureStatusFixture(t)
	sig := solana.Signature{1}
	statusPublish(t, server, 25, true, txstatus.SignatureResult{Signature: sig})
	for _, config := range []string{`,{"minContextSlot":26}`, `,{"searchTransactionHistory":false,"minContextSlot":26}`} {
		reply := statusCall(t, endpoint, []solana.Signature{sig}, config)
		require.JSONEq(t, `{"code":-32016,"message":"Minimum context slot has not been reached","data":{"contextSlot":25}}`, string(reply["error"]))
	}
	for _, commitment := range []string{"confirmed", "finalized"} {
		reply := statusCall(t, endpoint, []solana.Signature{sig},
			fmt.Sprintf(`,{"commitment":%q,"minContextSlot":21}`, commitment))
		require.JSONEq(t, `{"code":-32016,"message":"Minimum context slot has not been reached","data":{"contextSlot":20}}`, string(reply["error"]))
		reply = statusCall(t, endpoint, []solana.Signature{sig},
			fmt.Sprintf(`,{"commitment":%q,"minContextSlot":20}`, commitment))
		require.JSONEq(t, `{"context":{"slot":20},"value":[null]}`, string(reply["result"]))
	}
}

func TestGetTransactionRootedContextFloor(t *testing.T) {
	server, block, endpoint := getBlockHistoryHTTPFixture(t)
	require.NoError(t, server.transactionHistory.WriteBlock(block))
	sig := solana.Signature(block.Records[0].FirstSignature)
	miss := sig
	miss[0]++
	call := func(t *testing.T, signature solana.Signature, floor string) map[string]json.RawMessage {
		return getBlockHistoryHTTPCall(t, endpoint, "getTransaction", fmt.Sprintf(`[%q,{"commitment":"finalized","minContextSlot":%s,"maxSupportedTransactionVersion":1}]`, signature, floor))
	}
	for _, s := range []solana.Signature{sig, miss} {
		t.Run("future", func(t *testing.T) {
			body := call(t, s, "21")
			require.JSONEq(t, `{"code":-32016,"message":"Minimum context slot has not been reached","data":{"contextSlot":20}}`, string(body["error"]))
			require.NotContains(t, body, "result")
		})
	}
	require.NoError(t, historyReselectionCommit(server, 24))
	server.SetRootedBankState(24, 8, 1)
	t.Run("transaction may precede floor", func(t *testing.T) {
		body := call(t, sig, "24")
		require.NotContains(t, body, "error")
		require.JSONEq(t, "20", string(historyResponseObject(t, body["result"])["slot"]))
		require.JSONEq(t, "null", string(call(t, miss, "24")["result"]))
	})
	t.Run("malformed", func(t *testing.T) {
		requireGetBlockHistoryError(t, call(t, miss, `"25"`), -32602, "invalid minContextSlot")
	})
	server.InitTransactionHistory(nil)
	t.Run("history disabled before floor", func(t *testing.T) {
		requireGetBlockHistoryError(t, call(t, sig, "25"), -32011, "Transaction history is not available from this node")
	})
}
