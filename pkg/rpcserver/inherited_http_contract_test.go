package rpcserver

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func TestInheritedRPCParameterHTTPContract(t *testing.T) {
	fixture := transactionOptionsServer(t)
	db := fixture.acctsDb
	server := NewRpcServer(db, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	server.SetSlotCtx(fixture.slotCtx)
	root := fixture.rootedBank.Load()
	server.PublishRootedBankState(root.Slot, root.BlockHeight, root.TransactionCount)
	server.SetRootedExecutionBank(root.executionBank)
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	tx, _ := testLegacyTransaction(t)
	tx.Message.RecentBlockhash = solana.Hash{1}
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	b58, b64 := base58.Encode(wire), base64.StdEncoding.EncodeToString(wire)
	for _, test := range []struct {
		name, method, params string
		code                 int
	}{
		{"default-base58", "simulateTransaction", fmt.Sprintf(`[%q]`, b58), 0},
		{"explicit-base64", "simulateTransaction", fmt.Sprintf(`[%q,{"encoding":"base64"}]`, b64), 0},
		{"simulation-minimum", "simulateTransaction", fmt.Sprintf(`[%q,{"minContextSlot":151}]`, b58), -32016},
		{"send-minimum", "sendTransaction", fmt.Sprintf(`[%q,{"minContextSlot":151}]`, b58), -32016},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := getBlockHistoryHTTPCall(t, endpoint, test.method, test.params)
			if test.code != 0 {
				require.JSONEq(t, `{"code":-32016,"message":"Minimum context slot has not been reached","data":{"contextSlot":150}}`, string(body["error"]))
				return
			}
			require.NotContains(t, body, "error")
			var value struct {
				Context SimulateTransactionRespContext `json:"context"`
				Value   struct{ Err any }              `json:"value"`
			}
			require.NoError(t, json.Unmarshal(body["result"], &value))
			require.Equal(t, uint64(150), value.Context.Slot)
			require.Empty(t, value.Context.ApiVersion)
			require.Equal(t, "BlockhashNotFound", value.Value.Err)
		})
	}
}
