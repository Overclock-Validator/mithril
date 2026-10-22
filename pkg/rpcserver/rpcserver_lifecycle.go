package rpcserver

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
)

// InitTransactionResults borrows the cache before Start. Configuration is
// immutable while handlers run; replacing the selected lineage needs replay
// owner mutations on this same cache, not a different pointer.
func (rpcServer *RpcServer) InitTransactionResults(cache *txstatus.ResultCache) error {
	rpcServer.lifecycleMu.Lock()
	defer rpcServer.lifecycleMu.Unlock()
	if rpcServer.lifecycle != nil || rpcServer.lifecycleStopped {
		return errors.New("transaction results must be configured before RPC Start")
	}
	if rpcServer.resultCache != nil && rpcServer.resultCache != cache {
		return errors.New("RPC transaction results are already configured")
	}
	rpcServer.resultCache = cache
	return nil
}

func (rpcServer *RpcServer) requiresHTTPOnly() bool {
	return rpcServer.transactionHistory != nil || rpcServer.resultCache != nil
}

type rpcServerLifecycle struct {
	server        *http.Server
	serveDone     chan struct{}
	serveErr      error // Read only after serveDone closes.
	refreshDone   <-chan struct{}
	cancelRefresh context.CancelFunc
	httpOnly      bool
}

// Start serves the configured listener once. Configuration, including optional
// transaction history and results, must remain unchanged after Start. A running
// server may be started again harmlessly, but a shutdown server cannot restart.
func (rpcServer *RpcServer) Start() error {
	rpcServer.lifecycleMu.Lock()
	defer rpcServer.lifecycleMu.Unlock()
	if rpcServer.lifecycleStopped {
		return errors.New("RPC server has been shut down")
	}
	if rpcServer.lifecycle != nil {
		select {
		case <-rpcServer.lifecycle.serveDone:
			return errors.Join(errors.New("RPC server has stopped"), rpcServer.lifecycle.serveErr)
		default:
			return nil
		}
	}
	if rpcServer.listener == nil {
		return errors.New("RPC server has no listener")
	}
	ctx, cancel := context.WithCancel(context.Background())
	lifecycle := &rpcServerLifecycle{
		server:        &http.Server{Handler: rpcServer},
		serveDone:     make(chan struct{}),
		cancelRefresh: cancel,
		httpOnly:      rpcServer.requiresHTTPOnly(),
	}
	lifecycle.refreshDone = rpcServer.startClusterNodesRefreshLoop(ctx)
	rpcServer.lifecycle = lifecycle
	go func() {
		defer close(lifecycle.serveDone)
		if err := lifecycle.server.Serve(rpcServer.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			lifecycle.serveErr = err
			mlog.Log.Errorf("RPC server stopped: %v", err)
		}
	}()
	return nil
}

// Shutdown joins this server's HTTP handlers and cluster refresh loop before its
// caller closes borrowed stores. It is supported only for the optional history
// or signature-status HTTP-only profile; the default profile may own untracked
// WebSocket handlers.
// A false joined result leaves stores live. The caller can retry Shutdown after
// a deadline; it never forces closed sockets to stand in for joined handlers.
// Start and Shutdown own the listener, not external callers of ServeHTTP.
func (rpcServer *RpcServer) Shutdown(ctx context.Context) (joined bool, err error) {
	rpcServer.lifecycleMu.Lock()
	lifecycle := rpcServer.lifecycle
	if (lifecycle == nil && !rpcServer.requiresHTTPOnly()) || (lifecycle != nil && !lifecycle.httpOnly) {
		rpcServer.lifecycleMu.Unlock()
		return false, errors.New("RPC shutdown requires an HTTP-only history or signature-status profile")
	}
	rpcServer.lifecycleStopped = true
	if lifecycle == nil {
		defer rpcServer.lifecycleMu.Unlock()
		if rpcServer.listener == nil {
			return true, nil
		}
		err := rpcServer.listener.Close()
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
		return true, err
	}
	rpcServer.lifecycleMu.Unlock()
	lifecycle.cancelRefresh()
	if err := lifecycle.server.Shutdown(ctx); err != nil {
		return false, err
	}
	for _, done := range []<-chan struct{}{lifecycle.serveDone, lifecycle.refreshDone} {
		select {
		case <-done:
			continue
		default:
		}
		select {
		case <-done:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return true, lifecycle.serveErr
}
