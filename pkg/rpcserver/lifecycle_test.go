package rpcserver

import (
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShutdownJoinsInFlightHandlerBeforeAccountsDBTeardown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	requestDone := make(chan error, 1)
	var releaseOnce sync.Once
	var handlerActive atomic.Bool

	server := &RpcServer{
		listener: listener,
		serveHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			handlerActive.Store(true)
			defer handlerActive.Store(false)
			close(requestStarted)
			<-releaseRequest
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseRequest) })
		_ = server.Shutdown(context.Background())
	})
	server.Start()

	go func() {
		resp, requestErr := http.Get("http://" + listener.Addr().String()) //nolint:gosec // loopback lifecycle test
		if resp != nil {
			_ = resp.Body.Close()
		}
		requestDone <- requestErr
	}()
	select {
	case <-requestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("RPC handler did not start")
	}

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownDone <- server.Shutdown(ctx)
	}()
	require.Eventually(t, func() bool {
		conn, dialErr := net.DialTimeout("tcp", listener.Addr().String(), 10*time.Millisecond)
		if conn != nil {
			_ = conn.Close()
		}
		return dialErr != nil
	}, time.Second, time.Millisecond, "Shutdown did not fence new connections")
	select {
	case shutdownErr := <-shutdownDone:
		t.Fatalf("Shutdown returned while an AccountsDB-using handler remained active: %v", shutdownErr)
	default:
	}
	assert.True(t, handlerActive.Load())

	releaseOnce.Do(func() { close(releaseRequest) })
	require.NoError(t, <-shutdownDone)
	require.NoError(t, <-requestDone)
	assert.False(t, handlerActive.Load(), "AccountsDB teardown may begin only after handler join")
	require.NoError(t, server.Shutdown(context.Background()), "Shutdown must be idempotent")
}

func TestShutdownCancelsAndJoinsClusterNodeRefresh(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	refreshStarted := make(chan struct{})
	refreshExited := make(chan struct{})
	server := &RpcServer{
		listener:     listener,
		serveHandler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		clusterNodesFetcher: func(ctx context.Context) ([]*solanarpc.GetClusterNodesResult, error) {
			close(refreshStarted)
			<-ctx.Done()
			close(refreshExited)
			return nil, ctx.Err()
		},
	}
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	server.Start()
	select {
	case <-refreshStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("cluster-node refresh did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, server.Shutdown(ctx))
	select {
	case <-refreshExited:
	default:
		t.Fatal("Shutdown returned before the cluster-node refresh loop exited")
	}
	require.NoError(t, ctx.Err())
}

func TestShutdownRejectsNilContext(t *testing.T) {
	assert.EqualError(t, (&RpcServer{}).Shutdown(nil), "rpcserver: nil shutdown context")
}
