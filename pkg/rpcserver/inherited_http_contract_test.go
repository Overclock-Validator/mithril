package rpcserver

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func TestInheritedRPCParameterHTTPContract(t *testing.T) {
	fixture := transactionOptionsServer(t)
	db := newRPCBankHashDB(t)
	hash := [32]byte{12}
	require.NoError(t, db.StoreBankHashForSlot(9007199254740993, hash[:]))
	server := NewRpcServer(db, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	server.slotCtx = fixture.slotCtx
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	client := endpoint.Client()
	client.Timeout = 5 * time.Second
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
		{"bank-exact-u64", "getBankHash", `[9007199254740993]`, 0},
		{"bank-fraction", "getBankHash", `[10.9]`, -32602},
		{"bank-negative", "getBankHash", `[-1]`, -32602},
		{"bank-overflow", "getBankHash", `[18446744073709551616]`, -32602},
		{"bank-null", "getBankHash", `[null]`, -32602},
		{"bank-extra", "getBankHash", `[10,11]`, -32602},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, test.method, test.params)
			response, err := client.Post(endpoint.URL, "application/json", strings.NewReader(body))
			require.NoError(t, err)
			defer response.Body.Close()
			var got struct {
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Code int `json:"code"`
					Data struct {
						ContextSlot uint64 `json:"contextSlot"`
					} `json:"data"`
				} `json:"error"`
			}
			require.NoError(t, json.NewDecoder(response.Body).Decode(&got))
			if test.code != 0 {
				require.NotNil(t, got.Error)
				require.Equal(t, test.code, got.Error.Code)
				if test.code == -32016 {
					require.Equal(t, uint64(150), got.Error.Data.ContextSlot)
				}
				return
			}
			require.Nil(t, got.Error)
			if test.method == "getBankHash" {
				var value string
				require.NoError(t, json.Unmarshal(got.Result, &value))
				require.Equal(t, base58.Encode(hash[:]), value)
				return
			}
			var value struct {
				Context struct{ Slot uint64 } `json:"context"`
				Value   struct{ Err any }     `json:"value"`
			}
			require.NoError(t, json.Unmarshal(got.Result, &value))
			require.Equal(t, uint64(150), value.Context.Slot)
			require.Equal(t, "BlockhashNotFound", value.Value.Err)
		})
	}
}
