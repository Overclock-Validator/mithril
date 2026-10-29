package rpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newQueryWebSocketProbe bypasses only the pre-existing outer quiet-probe GET
// refusal. It retains real query ownership and the registered JSON-RPC server,
// so WS refusal cannot pass merely because the request has no owner.
func newQueryWebSocketProbe(t *testing.T, server *RpcServer) *httptest.Server {
	t.Helper()
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		owner := newResponseOwner(r.Context(), w, server.queryResponses, time.Now().Add(historyResponseTimeout))
		defer func() { require.NoError(t, finalizeResponseOwners(owner)) }()
		ctx := context.WithValue(r.Context(), queryResponseKey{}, owner)
		server.rpcService.ServeHTTP(w, r.WithContext(ctx))
	}))
	t.Cleanup(endpoint.Close)
	return endpoint
}

type responseDeadlineRecorder struct {
	http.ResponseWriter
	deadlines *[]time.Time
}

func (w *responseDeadlineRecorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *responseDeadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		*w.deadlines = append(*w.deadlines, deadline)
	}
	return http.NewResponseController(w.ResponseWriter).SetWriteDeadline(deadline)
}

func TestSignatureStatusesMixedHistoryDeadlineAndBatchLimit(t *testing.T) {
	server, block, original, _ := signatureStatusFixture(t)
	require.NoError(t, server.transactionHistory.WriteBlock(block))
	original.Close()
	observed := make(chan []time.Time, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var deadlines []time.Time
		server.ServeHTTP(&responseDeadlineRecorder{ResponseWriter: w, deadlines: &deadlines}, r)
		observed <- deadlines
	}))
	t.Cleanup(endpoint.Close)
	data, err := historyHTTPPost(endpoint.URL, `[
		{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[[]]},
		{"jsonrpc":"2.0","id":2,"method":"getBlock","params":[20]},
		{"jsonrpc":"2.0","id":3,"method":"getSignatureStatuses","params":[[]]},
		{"jsonrpc":"2.0","id":4,"method":"getSignatureStatuses","params":[[]]}
	]`)
	require.NoError(t, err)
	var replies []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &replies))
	require.Len(t, replies, 4)
	for _, reply := range replies[:3] {
		require.Contains(t, reply, "result")
		require.NotContains(t, reply, "error")
	}
	requireGetBlockHistoryError(t, replies[3], -32000, "Signature status unavailable")
	deadlines := <-observed
	require.Len(t, deadlines, 2, "each independently admitted response owner sets its deadline once")
	require.True(t, deadlines[0].Equal(deadlines[1]), "switching from query to history must not renew the request deadline")
	require.Zero(t, len(server.queryResponses))
	require.Zero(t, len(server.transactionHistoryResponses))
}

type responseFlushGate struct {
	http.ResponseWriter
	entered chan struct{}
	release <-chan struct{}
	flushes atomic.Int32
	clears  atomic.Int32
}

func (w *responseFlushGate) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *responseFlushGate) SetWriteDeadline(deadline time.Time) error {
	if deadline.IsZero() {
		w.clears.Add(1)
	}
	return http.NewResponseController(w.ResponseWriter).SetWriteDeadline(deadline)
}
func (w *responseFlushGate) FlushError() error {
	if w.flushes.Add(1) == 1 {
		close(w.entered)
	}
	select {
	case <-w.release:
	case <-time.After(2 * time.Second):
		return context.DeadlineExceeded
	}
	return errors.Join(http.NewResponseController(w.ResponseWriter).Flush(), errors.New("injected post-flush failure"))
}

func TestSignatureStatusesMixedHistoryOwnsOneFinalFlush(t *testing.T) {
	server, block, original, _ := signatureStatusFixture(t)
	require.NoError(t, server.transactionHistory.WriteBlock(block))
	original.Close()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	writers := make(chan *responseFlushGate, 1)
	finished := make(chan struct{})
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gate := &responseFlushGate{ResponseWriter: w, entered: make(chan struct{}), release: release}
		writers <- gate
		server.ServeHTTP(gate, r)
		close(finished)
	}))
	t.Cleanup(endpoint.Close)
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		data, err := historyHTTPPost(endpoint.URL, `[
			{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[[]]},
			{"jsonrpc":"2.0","id":2,"method":"getBlock","params":[20]}
		]`)
		done <- result{data, err}
	}()
	gate := <-writers
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("mixed response did not reach final flush")
	}
	require.Equal(t, 1, len(server.queryResponses))
	require.Equal(t, 1, len(server.transactionHistoryResponses))
	require.Zero(t, gate.clears.Load(), "the shared write deadline must remain active while the final flush is blocked")
	unblock()
	got := <-done
	require.NoError(t, got.err)
	<-finished
	var replies []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(got.data, &replies))
	require.Len(t, replies, 2)
	for _, reply := range replies {
		require.Contains(t, reply, "result")
		require.NotContains(t, reply, "error")
	}
	require.Equal(t, int32(1), gate.flushes.Load())
	require.Equal(t, int32(1), gate.clears.Load())
	require.Zero(t, len(server.queryResponses))
	require.Zero(t, len(server.transactionHistoryResponses))
}
