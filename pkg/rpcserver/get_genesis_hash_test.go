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

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestGetGenesisHashHTTP(t *testing.T) {
	for _, cluster := range []string{"cluster-a", "cluster-b"} {
		t.Run(cluster, func(t *testing.T) {
			hash := solana.Hash(sha256.Sum256([]byte(cluster)))
			server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{}, hash)
			// Serve on httptest's loopback listener without starting cluster discovery.
			require.NoError(t, server.listener.Close())
			endpoint := httptest.NewServer(server)
			t.Cleanup(endpoint.Close)

			got, err := solanarpc.New(endpoint.URL).GetGenesisHash(t.Context())
			require.NoError(t, err)
			require.Equal(t, hash, got)

			for _, test := range []struct {
				name, params string
				invalid      bool
			}{
				{name: "omitted"},
				{name: "empty", params: `,"params":[]`},
				{name: "null", params: `,"params":null`},
				{name: "argument", params: `,"params":[true]`, invalid: true},
				{name: "object", params: `,"params":{}`, invalid: true},
				{name: "scalar", params: `,"params":1`, invalid: true},
			} {
				t.Run(test.name, func(t *testing.T) {
					request := fmt.Sprintf(`{"jsonrpc":"2.0","id":7,"method":"getGenesisHash"%s}`, test.params)
					response, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(request))
					require.NoError(t, err)
					defer response.Body.Close()
					var result struct {
						JSONRPC string                `json:"jsonrpc"`
						ID      int                   `json:"id"`
						Result  string                `json:"result"`
						Error   *jsonrpc.JSONRPCError `json:"error"`
					}
					require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
					require.Equal(t, "2.0", result.JSONRPC)
					require.Equal(t, 7, result.ID)
					if test.invalid {
						require.NotNil(t, result.Error)
						require.EqualValues(t, -32602, result.Error.Code)
						require.Empty(t, result.Result)
					} else {
						require.Equal(t, http.StatusOK, response.StatusCode)
						require.Nil(t, result.Error)
						require.Equal(t, hash.String(), result.Result)
					}
				})
			}

			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`[{"jsonrpc":"2.0","id":1,"method":"getGenesisHash"},{"jsonrpc":"2.0","id":2,"method":"getGenesisHash","params":[]}]`))
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			require.Equal(t, http.StatusOK, response.Code)
			require.JSONEq(t, fmt.Sprintf(`[{"jsonrpc":"2.0","id":1,"result":%q},{"jsonrpc":"2.0","id":2,"result":%q}]`, hash.String(), hash.String()), response.Body.String())
		})
	}
}

func TestGetGenesisHashUnavailableAndCanceled(t *testing.T) {
	server := &RpcServer{}
	got, err := server.GetGenesisHash(t.Context(), nil)
	require.Error(t, err)
	require.Empty(t, got)

	server.genesisHash = solana.Hash{1}.String()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, err = server.GetGenesisHash(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, got)
}
