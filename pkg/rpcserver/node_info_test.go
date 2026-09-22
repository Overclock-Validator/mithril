package rpcserver

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/version"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestNodeInfoHTTP(t *testing.T) {
	oldVersion := version.Version
	version.Version = "1.2.3-test"
	t.Cleanup(func() { version.Version = oldVersion })

	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{}, solana.Hash(sha256.Sum256([]byte("cluster"))))
	require.NoError(t, server.listener.Close())
	server.SetIdentity("2r1F4iWqVcb8M1DbAjQuFpebkQHY9hcVU4WuW2DJBppN")
	server.SetSlotCtx(&sealevel.SlotCtx{Slot: global.WallClockSlot()})
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)

	for _, test := range []struct {
		method string
		want   string
	}{
		{method: "getHealth", want: `"ok"`},
		{method: "getVersion", want: `{"feature-set":null,"solana-core":"1.2.3-test"}`},
		{method: "getIdentity", want: `{"identity":"2r1F4iWqVcb8M1DbAjQuFpebkQHY9hcVU4WuW2DJBppN"}`},
	} {
		t.Run(test.method, func(t *testing.T) {
			request := fmt.Sprintf(`{"jsonrpc":"2.0","id":7,"method":%q}`, test.method)
			response, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(request))
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusOK, response.StatusCode)

			var result struct {
				JSONRPC string                `json:"jsonrpc"`
				ID      int                   `json:"id"`
				Result  json.RawMessage       `json:"result"`
				Error   *jsonrpc.JSONRPCError `json:"error"`
			}
			require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
			require.Equal(t, "2.0", result.JSONRPC)
			require.Equal(t, 7, result.ID)
			require.Nil(t, result.Error)
			require.JSONEq(t, test.want, string(result.Result))
		})
	}
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

func TestNodeInfoRejectsParametersAndCancellation(t *testing.T) {
	server := &RpcServer{identity: "identity"}
	for _, test := range []struct {
		name string
		call func(context.Context, jsonrpc.RawParams) error
	}{
		{name: "getHealth", call: func(ctx context.Context, params jsonrpc.RawParams) error {
			_, err := server.GetHealth(ctx, params)
			return err
		}},
		{name: "getVersion", call: func(ctx context.Context, params jsonrpc.RawParams) error {
			_, err := server.GetVersion(ctx, params)
			return err
		}},
		{name: "getIdentity", call: func(ctx context.Context, params jsonrpc.RawParams) error {
			_, err := server.GetIdentity(ctx, params)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var invalid *InvalidParamsError
			require.ErrorAs(t, test.call(t.Context(), jsonrpc.RawParams(`[true]`)), &invalid)

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.ErrorIs(t, test.call(ctx, nil), context.Canceled)
		})
	}
}

func TestGetIdentityRequiresConfiguredValidator(t *testing.T) {
	got, err := (&RpcServer{}).GetIdentity(t.Context(), nil)
	require.Error(t, err)
	require.Empty(t, got.Identity)
}

func uint64Ptr(value uint64) *uint64 { return &value }
