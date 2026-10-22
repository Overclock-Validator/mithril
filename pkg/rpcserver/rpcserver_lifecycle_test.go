package rpcserver

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/filecoin-project/go-jsonrpc"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

type lifecycleRPCProbe struct {
	call func(context.Context) (string, error)
	work atomic.Int32
}

func (probe *lifecycleRPCProbe) GetBankHash(ctx context.Context) (string, error) {
	probe.work.Add(1)
	if probe.call != nil {
		return probe.call(ctx)
	}
	return "ok", nil
}

func newLifecycleRPC(t *testing.T, probe *lifecycleRPCProbe, wantShutdownError ...error) *RpcServer {
	t.Helper()
	store, err := transactionhistory.Open(t.TempDir(), [32]byte{1}, transactionhistory.DefaultLimits())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	rpc := jsonrpc.NewServer()
	rpc.Register("Lifecycle", probe)
	rpc.AliasMethod("getBankHash", "Lifecycle.GetBankHash")
	server := &RpcServer{listener: listener, rpcService: rpc}
	server.InitTransactionHistory(store)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		joined, err := server.Shutdown(ctx)
		require.True(t, joined, "test must join handlers before closing its store")
		if len(wantShutdownError) == 0 {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, wantShutdownError[0])
		}
	})
	return server
}

func TestHistoryHTTPShutdownJoinsHandlers(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	probe := &lifecycleRPCProbe{}
	server := newLifecycleRPC(t, probe)
	probe.call = func(context.Context) (string, error) {
		view, err := server.transactionHistory.Snapshot()
		if err != nil {
			return "", err
		}
		close(entered)
		<-release
		return "ok", view.Close()
	}
	require.NoError(t, server.Start())
	lifecycle := server.lifecycle
	require.NoError(t, server.Start())
	require.Same(t, lifecycle, server.lifecycle, "Start must not create another refresh or Serve goroutine")
	endpoint := "http://" + server.listener.Addr().String()
	done := make(chan error, 1)
	go func() {
		body, err := historyHTTPPost(endpoint, `{"jsonrpc":"2.0","id":1,"method":"getBankHash"}`)
		if err == nil && !strings.Contains(string(body), `"result":"ok"`) {
			err = errors.New("missing RPC response")
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
	require.False(t, joined)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Error(t, server.Start(), "shutdown cannot restart the listener")
	require.ErrorIs(t, server.transactionHistory.Close(), transactionhistory.ErrSnapshotsOpen, "blocked handler still owns a live snapshot")
	select {
	case err := <-done:
		t.Fatalf("active handler was not joined or finished: %v", err)
	default:
	}
	_, err = historyHTTPPost(endpoint, `{"jsonrpc":"2.0","id":2,"method":"getBankHash"}`)
	require.Error(t, err, "shutdown must refuse new connections")
	unblock.Do(func() { close(release) })
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("released handler did not finish")
	}
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	joined, err = server.Shutdown(ctx)
	require.True(t, joined)
	require.NoError(t, err)
	joined, err = server.Shutdown(ctx)
	require.True(t, joined)
	require.NoError(t, err)
	view, err := server.transactionHistory.Snapshot()
	require.NoError(t, err, "Shutdown borrows but never closes the history store")
	require.NoError(t, view.Close())
}

func TestHistoryHTTPShutdownJoinsRefresh(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	server := newLifecycleRPC(t, &lifecycleRPCProbe{})
	var calls atomic.Int32
	server.clusterNodesFetcher = func(ctx context.Context) ([]*solanarpc.GetClusterNodesResult, error) {
		calls.Add(1)
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release // A non-cooperative fetcher cannot be called joined by cancellation alone.
		return nil, ctx.Err()
	}
	require.NoError(t, server.Start())
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("refresh did not enter")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	joined, err := server.Shutdown(ctx)
	require.False(t, joined)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-canceled:
	default:
		t.Fatal("refresh context was not canceled")
	}
	select {
	case <-server.lifecycle.refreshDone:
		t.Fatal("refresh reported done while its fetcher was still blocked")
	default:
	}
	unblock.Do(func() { close(release) })
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	joined, err = server.Shutdown(ctx)
	require.True(t, joined)
	require.NoError(t, err)
	require.Equal(t, int32(1), calls.Load())
}

func TestHistoryHTTPShutdownBeforeStart(t *testing.T) {
	server := newLifecycleRPC(t, &lifecycleRPCProbe{})
	joined, err := server.Shutdown(t.Context())
	require.True(t, joined)
	require.NoError(t, err)
	require.Error(t, server.Start())
	_, err = net.DialTimeout("tcp", server.listener.Addr().String(), time.Second)
	require.Error(t, err)
	joined, err = server.Shutdown(t.Context())
	require.True(t, joined)
	require.NoError(t, err)

	var unconfigured RpcServer
	require.Error(t, unconfigured.Start())
	joined, err = unconfigured.Shutdown(t.Context())
	require.False(t, joined)
	require.ErrorContains(t, err, "HTTP-only")
}

func TestHistoryHTTPShutdownRejectsUpgrade(t *testing.T) {
	probe := &lifecycleRPCProbe{}
	server := newLifecycleRPC(t, probe)
	require.NoError(t, server.Start())
	endpoint := "http://" + server.listener.Addr().String()
	for _, connectionHeader := range []string{"Upgrade", "keep-alive, xUPGRADEy"} {
		t.Run(connectionHeader, func(t *testing.T) {
			connection, err := net.DialTimeout("tcp", server.listener.Addr().String(), time.Second)
			require.NoError(t, err)
			defer func() { require.NoError(t, connection.Close()) }()
			require.NoError(t, connection.SetDeadline(time.Now().Add(time.Second)))
			// A valid body crosses the pre-existing quiet-probe filter; without
			// the profile guard the ordinary Upgrade case would be hijacked.
			request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint,
				strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"getBankHash"}`))
			require.NoError(t, err)
			request.Header.Set("Connection", connectionHeader)
			request.Header.Set("Upgrade", "websocket")
			request.Header.Set("Sec-WebSocket-Version", "13")
			request.Header.Set("Sec-WebSocket-Key", "MDEyMzQ1Njc4OWFiY2RlZg==")
			require.NoError(t, request.Write(connection))
			response, err := http.ReadResponse(bufio.NewReader(connection), request)
			require.NoError(t, err)
			body, readErr := io.ReadAll(response.Body)
			require.NoError(t, errors.Join(readErr, response.Body.Close()))
			require.Equal(t, http.StatusBadRequest, response.StatusCode)
			require.Contains(t, string(body), "requires HTTP without upgrades")
		})
	}
	require.Zero(t, probe.work.Load())
	body, err := historyHTTPPost(endpoint, `{"jsonrpc":"2.0","id":1,"method":"getBankHash"}`)
	require.NoError(t, err)
	require.Contains(t, string(body), `"result":"ok"`)
	require.Equal(t, int32(1), probe.work.Load())
}

func TestHistoryHTTPShutdownRefusesDefaultProfile(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &RpcServer{listener: listener, rpcService: newHistoryHTTPRPC(&historyHTTPProbe{})}
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		// Test cleanup has no hijacked connections. This does not strengthen
		// the public default-profile Shutdown contract.
		server.lifecycle.cancelRefresh()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, server.lifecycle.server.Shutdown(ctx))
		<-server.lifecycle.serveDone
		<-server.lifecycle.refreshDone
	})
	joined, err := server.Shutdown(t.Context())
	require.False(t, joined)
	require.ErrorContains(t, err, "HTTP-only")
	require.False(t, server.lifecycleStopped, "unsupported shutdown must not claim to have stopped this profile")
	require.NoError(t, server.Start())
}

func TestHistoryHTTPShutdownPropagatesServeFailure(t *testing.T) {
	server := newLifecycleRPC(t, &lifecycleRPCProbe{}, net.ErrClosed)
	require.NoError(t, server.listener.Close())
	require.NoError(t, server.Start())
	select {
	case <-server.lifecycle.serveDone:
	case <-time.After(time.Second):
		t.Fatal("Serve did not report the closed listener")
	}
	require.ErrorIs(t, server.Start(), net.ErrClosed, "a failed server cannot be restarted silently")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	joined, err := server.Shutdown(ctx)
	require.True(t, joined)
	require.ErrorIs(t, err, net.ErrClosed)
}
