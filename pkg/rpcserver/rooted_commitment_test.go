package rpcserver

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestRootedRPCRejectsProcessedHTTP(t *testing.T) {
	server := NewRpcServer(newRPCAccountsDB(t), 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 1000}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	server.SetRootedBankState(500, 400, 0)
	oldSlot := global.Slot()
	global.SetSlot(600)
	t.Cleanup(func() { global.SetSlot(oldSlot) })
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	key := (solana.PublicKey{8}).String()
	for _, test := range []struct{ method, params string }{
		{"getAccountInfo", `["` + key + `",{"commitment":"processed"}]`},
		{"getBalance", `["` + key + `",{"commitment":"processed"}]`},
		{"getVoteAccounts", `[{"commitment":"processed"}]`},
		{"getLeaderSchedule", `[{"commitment":"processed"}]`},
		{"getLeaderSchedule", `[null,{"commitment":"processed"}]`},
		{"getLeaderSchedule", `[500,{"commitment":"processed"}]`},
		{"getBlockProduction", `[{"commitment":"processed"}]`},
	} {
		t.Run(test.method+test.params, func(t *testing.T) {
			request := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, test.method, test.params)
			response, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(request))
			require.NoError(t, err)
			defer response.Body.Close()
			var reply struct {
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			require.NoError(t, json.NewDecoder(response.Body).Decode(&reply))
			require.NotNil(t, reply.Error, "must not substitute rooted state for processed")
			require.Equal(t, -32602, reply.Error.Code)
			require.Contains(t, reply.Error.Message, "processed commitment is not supported")
			require.Empty(t, reply.Result)
		})
	}
}
