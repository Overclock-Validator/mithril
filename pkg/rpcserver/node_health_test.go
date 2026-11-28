package rpcserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGetHealthHTTP(t *testing.T) {
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	server.SetSlotCtx(&sealevel.SlotCtx{Slot: 42})
	server.SetHealthSlotSource(func() (uint64, bool) { return 42, true })
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"getHealth"}`)))
	require.Equal(t, http.StatusOK, resp.Code)
	var result struct {
		ID     int                   `json:"id"`
		Result string                `json:"result"`
		Error  *jsonrpc.JSONRPCError `json:"error"`
	}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &result))
	require.Equal(t, 7, result.ID)
	require.Nil(t, result.Error)
	require.Equal(t, "ok", result.Result)
}

func TestNodeHealthBoundaries(t *testing.T) {
	tests := []struct {
		name        string
		ready       bool
		local       uint64
		cluster     uint64
		wantHealthy bool
		wantBehind  *uint64
	}{
		{name: "not ready", cluster: 100},
		{name: "same slot", ready: true, local: 100, cluster: 100, wantHealthy: true},
		{name: "local ahead", ready: true, local: 101, cluster: 100, wantHealthy: true},
		{name: "at threshold", ready: true, local: 100, cluster: 228, wantHealthy: true},
		{name: "past threshold", ready: true, local: 100, cluster: 229, wantBehind: uint64Ptr(129)},
		{name: "large slot without overflow", ready: true, local: ^uint64(0) - 1, cluster: ^uint64(0), wantHealthy: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := nodeHealth(test.ready, test.local, test.cluster)
			if test.wantHealthy {
				require.NoError(t, err)
				require.Equal(t, "ok", got)
				return
			}
			require.Empty(t, got)
			var unhealthy *NodeUnhealthyError
			require.ErrorAs(t, err, &unhealthy)
			require.Equal(t, test.wantBehind, unhealthy.NumSlotsBehind)
		})
	}
}

func TestGetHealthHTTPReportsNotReady(t *testing.T) {
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)

	response, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(
		`{"jsonrpc":"2.0","id":7,"method":"getHealth"}`,
	))
	require.NoError(t, err)
	defer response.Body.Close()

	var result struct {
		Error *jsonrpc.JSONRPCError `json:"error"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
	require.NotNil(t, result.Error)
	require.Equal(t, jsonrpc.ErrorCode(-32005), result.Error.Code)
	require.Equal(t, "Node is unhealthy", result.Error.Message)
	require.Equal(t, map[string]interface{}{"numSlotsBehind": nil}, result.Error.Data)
}

func TestGetHealthRejectsParametersAndCancellation(t *testing.T) {
	server := &RpcServer{}
	_, err := server.GetHealth(t.Context(), jsonrpc.RawParams(`[true]`))
	var invalid *InvalidParamsError
	require.ErrorAs(t, err, &invalid)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = server.GetHealth(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func uint64Ptr(value uint64) *uint64 { return &value }
