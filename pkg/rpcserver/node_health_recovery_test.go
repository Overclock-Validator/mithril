package rpcserver

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGetHealthHTTPRecoveryPublication(t *testing.T) {
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	server.SetHealthSlotSource(func() (uint64, bool) { return 1128, true })
	bank := &sealevel.SlotCtx{Slot: 1000}
	server.SetSlotCtx(bank)

	check := func(healthy bool, lag *uint64) {
		t.Helper()
		response, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(
			`{"jsonrpc":"2.0","id":1,"method":"getHealth"}`,
		))
		require.NoError(t, err)
		defer response.Body.Close()
		var result struct {
			Result string                `json:"result"`
			Error  *jsonrpc.JSONRPCError `json:"error"`
		}
		require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
		if healthy {
			require.Nil(t, result.Error)
			require.Equal(t, "ok", result.Result)
			return
		}
		require.NotNil(t, result.Error)
		require.Equal(t, jsonrpc.ErrorCode(-32005), result.Error.Code)
		var expected any
		if lag != nil {
			expected = float64(*lag)
		}
		require.Equal(t, map[string]any{"numSlotsBehind": expected}, result.Error.Data)
	}

	check(true, nil)
	server.InvalidateHealth()
	require.Same(t, bank, server.getSlotCtx(), "health invalidation must not clear bank RPC state")
	check(false, nil)
	// A replacement block source in the checkpoint-recovery loop can observe
	// the network before replay publishes any replacement bank.
	server.SetHealthSlotSource(func() (uint64, bool) { return 1128, true })
	check(false, nil)
	server.SetSlotCtx(&sealevel.SlotCtx{Slot: 999})
	check(false, uint64Ptr(129))
	server.SetSlotCtx(&sealevel.SlotCtx{Slot: 1000})
	check(true, nil)
	server.InvalidateHealth()
	check(false, nil)
}
