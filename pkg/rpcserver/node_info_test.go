package rpcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/version"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestNodeMetadataHTTP(t *testing.T) {
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	server.SetIdentity("test-public-identity")
	for _, tc := range []struct {
		method, params, want string
		code                 jsonrpc.ErrorCode
	}{
		{method: "getIdentity", params: `[]`, want: `{"identity":"test-public-identity"}`},
		{method: "getVersion", params: `[]`, want: fmt.Sprintf(`{"feature-set":null,"solana-core":%q}`, version.Version)},
		{method: "getIdentity", params: `[true]`, code: -32602},
		{method: "getVersion", params: `[true]`, code: -32602},
	} {
		t.Run(tc.method+"/"+tc.params, func(t *testing.T) {
			request := fmt.Sprintf(`{"jsonrpc":"2.0","id":7,"method":%q,"params":%s}`, tc.method, tc.params)
			resp := httptest.NewRecorder()
			server.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(request)))
			var result struct {
				ID     int                   `json:"id"`
				Result json.RawMessage       `json:"result"`
				Error  *jsonrpc.JSONRPCError `json:"error"`
			}
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &result))
			require.Equal(t, 7, result.ID)
			if tc.code != 0 {
				require.NotNil(t, result.Error)
				require.Equal(t, tc.code, result.Error.Code)
				return
			}
			require.Equal(t, http.StatusOK, resp.Code)
			require.Nil(t, result.Error)
			require.JSONEq(t, tc.want, string(result.Result))
		})
	}
}

func TestNodeMetadataCancellation(t *testing.T) {
	server := &RpcServer{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := server.GetIdentity(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
	_, err = server.GetVersion(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)

	got, err := server.GetIdentity(t.Context(), nil)
	require.Error(t, err)
	require.Empty(t, got.Identity)
}

func TestNodeIdentitySettersAreNotRPC(t *testing.T) {
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	for _, method := range []string{"setIdentity", "setIdentitySource"} {
		resp := httptest.NewRecorder()
		request := fmt.Sprintf(`{"jsonrpc":"2.0","id":7,"method":%q}`, method)
		server.rpcService.HandleRequest(t.Context(), strings.NewReader(request), resp)
		var result struct {
			Error *jsonrpc.JSONRPCError `json:"error"`
		}
		require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &result))
		require.NotNil(t, result.Error)
		require.Equal(t, jsonrpc.ErrorCode(-32601), result.Error.Code)
	}
}
