package rpcserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func activationCache(t *testing.T) *txstatus.ResultCache {
	t.Helper()
	cache, err := txstatus.NewResultCache(txstatus.ResultBankID{Slot: 20, Blockhash: solana.Hash{20}})
	require.NoError(t, err)
	return cache
}

func TestSignatureStatusesInitBeforeStart(t *testing.T) {
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}, solana.Hash{1})
	require.NoError(t, server.InitTransactionResults(nil))
	require.False(t, server.requiresHTTPOnly())
	cache := activationCache(t)
	require.NoError(t, server.InitTransactionResults(cache))
	require.NoError(t, server.InitTransactionResults(cache))
	require.True(t, server.requiresHTTPOnly(), "status-only profile must use owned HTTP lifecycle")
	require.Error(t, server.InitTransactionResults(activationCache(t)))
	require.Error(t, server.InitTransactionResults(nil))
	require.Same(t, cache, server.resultCache)
	require.Nil(t, server.transactionHistory, "status-only initialization must not open a history store")
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		joined, err := server.Shutdown(ctx)
		require.True(t, joined)
		require.NoError(t, err)
	})
	require.Error(t, server.InitTransactionResults(cache))
	_, port, err := net.SplitHostPort(server.listener.Addr().String())
	require.NoError(t, err)
	endpoint := "http://" + net.JoinHostPort("127.0.0.1", port)
	for _, name := range []string{"InitTransactionResults", "initTransactionResults", "Mithril.InitTransactionResults"} {
		body, err := historyHTTPPost(endpoint, `{"jsonrpc":"2.0","id":1,"method":"`+name+`","params":[]}`)
		require.NoError(t, err)
		var response struct {
			Error struct{ Code int } `json:"error"`
		}
		require.NoError(t, json.Unmarshal(body, &response))
		require.Equal(t, -32601, response.Error.Code, "initializer must not enter reflected RPC registration")
	}
	joined, err := server.Shutdown(t.Context())
	require.True(t, joined)
	require.NoError(t, err)
	require.Error(t, server.InitTransactionResults(cache))
}

func statusLifecycleServer(t *testing.T, history bool, probe *lifecycleRPCProbe) *RpcServer {
	t.Helper()
	var server *RpcServer
	if history {
		server = newLifecycleRPC(t, probe)
	} else {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		rpc := jsonrpc.NewServer()
		rpc.Register("Lifecycle", probe)
		rpc.AliasMethod("getBankHash", "Lifecycle.GetBankHash")
		server = &RpcServer{listener: listener, rpcService: rpc}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			joined, err := server.Shutdown(ctx)
			require.True(t, joined)
			require.NoError(t, err)
		})
	}
	require.NoError(t, server.InitTransactionResults(activationCache(t)))
	return server
}

func TestSignatureStatusesShutdownJoinsHTTP(t *testing.T) {
	for _, history := range []bool{false, true} {
		t.Run(map[bool]string{false: "status_only", true: "history_and_status"}[history], func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			defer unblock.Do(func() { close(release) })
			probe := &lifecycleRPCProbe{call: func(context.Context) (string, error) {
				close(entered)
				<-release
				return "ok", nil
			}}
			server := statusLifecycleServer(t, history, probe)
			require.NoError(t, server.Start())
			done := make(chan error, 1)
			go func() {
				body, err := historyHTTPPost("http://"+server.listener.Addr().String(), `{"jsonrpc":"2.0","id":1,"method":"getBankHash"}`)
				if err == nil && !strings.Contains(string(body), `"result":"ok"`) {
					err = errors.New("missing response")
				}
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("HTTP handler did not enter")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			joined, err := server.Shutdown(ctx)
			require.False(t, joined, "caller must keep AccountsDB live while this handler is still running")
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Error(t, server.Start())
			select {
			case <-done:
				t.Fatal("shutdown must not force-close the in-flight handler")
			default:
			}
			unblock.Do(func() { close(release) })
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("released request did not finish")
			}
			ctx, cancel = context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			joined, err = server.Shutdown(ctx)
			require.True(t, joined)
			require.NoError(t, err)
			select {
			case <-server.lifecycle.refreshDone:
			default:
				t.Fatal("shutdown must join refresh too")
			}
		})
	}
}

func TestSignatureStatusesShutdownRejectsUpgrade(t *testing.T) {
	probe := &lifecycleRPCProbe{}
	server := statusLifecycleServer(t, false, probe)
	require.NoError(t, server.Start())
	for _, header := range []string{"Upgrade", "keep-alive, xUPGRADEy"} {
		connection, err := net.DialTimeout("tcp", server.listener.Addr().String(), time.Second)
		require.NoError(t, err)
		require.NoError(t, connection.SetDeadline(time.Now().Add(time.Second)))
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+server.listener.Addr().String(),
			strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"getBankHash"}`))
		require.NoError(t, err)
		request.Header.Set("Connection", header)
		request.Header.Set("Upgrade", "websocket")
		request.Header.Set("Sec-WebSocket-Version", "13")
		request.Header.Set("Sec-WebSocket-Key", "MDEyMzQ1Njc4OWFiY2RlZg==")
		require.NoError(t, request.Write(connection))
		response, err := http.ReadResponse(bufio.NewReader(connection), request)
		require.NoError(t, err)
		body, readErr := io.ReadAll(response.Body)
		require.NoError(t, errors.Join(readErr, response.Body.Close(), connection.Close()))
		require.Equal(t, http.StatusBadRequest, response.StatusCode)
		require.Contains(t, string(body), "requires HTTP without upgrades")
	}
	require.Zero(t, probe.work.Load(), "upgrade refusal precedes cheap method dispatch")
}
