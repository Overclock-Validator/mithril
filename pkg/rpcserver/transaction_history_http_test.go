package rpcserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

type historyHTTPProbe struct {
	payload func(context.Context) any
	work    atomic.Int32
}

func (probe *historyHTTPProbe) Payload(ctx context.Context) (any, error) {
	if err := acquireHistoryResponse(ctx); err != nil {
		return nil, err
	}
	probe.work.Add(1)
	return probe.payload(ctx), nil
}

func (*historyHTTPProbe) Cheap(context.Context) (string, error) { return "cheap", nil }

func newHistoryHTTPRPC(probe *historyHTTPProbe) *jsonrpc.RPCServer {
	rpc := jsonrpc.NewServer()
	rpc.Register("Probe", probe)
	return rpc
}

func newHistoryHTTPServer(t *testing.T, slots chan struct{}, probe *historyHTTPProbe) *httptest.Server {
	t.Helper()
	rpc := newHistoryHTTPRPC(probe)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, owner := historyResponseContext(r.Context(), w, slots)
		defer func() {
			if err := owner.release(); err != nil {
				t.Errorf("release history response: %v", err)
			}
		}()
		rpc.ServeHTTP(w, r.WithContext(ctx))
	}))
	t.Cleanup(server.Close)
	return server
}

func historyHTTPPost(endpoint, body string) ([]byte, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Post(endpoint, "application/json", strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(response.Body)
	return data, errors.Join(readErr, response.Body.Close())
}

type historyHTTPMarshalProbe struct {
	entered chan<- struct{}
	release <-chan struct{}
}

func (probe historyHTTPMarshalProbe) MarshalJSON() ([]byte, error) {
	probe.entered <- struct{}{}
	<-probe.release
	return []byte(`"payload"`), nil
}

func TestHistoryHTTPResponseOwnsSerialization(t *testing.T) {
	slots := make(chan struct{}, 2)
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	probe := &historyHTTPProbe{payload: func(context.Context) any {
		return historyHTTPMarshalProbe{entered: entered, release: release}
	}}
	server := newHistoryHTTPServer(t, slots, probe)
	request := `{"jsonrpc":"2.0","id":1,"method":"Probe.Payload"}`
	results := make(chan error, 2)
	for range 2 {
		go func() {
			data, err := historyHTTPPost(server.URL, request)
			if err == nil && !bytes.Contains(data, []byte(`"result":"payload"`)) {
				err = errors.New("missing payload result")
			}
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("history response did not reach serialization")
		}
	}
	require.Equal(t, 2, len(slots), "permits must survive method return")
	cheap, err := historyHTTPPost(server.URL, `{"jsonrpc":"2.0","id":2,"method":"Probe.Cheap"}`)
	require.NoError(t, err)
	require.Contains(t, string(cheap), `"result":"cheap"`)
	busy, err := historyHTTPPost(server.URL, request)
	require.NoError(t, err)
	require.Contains(t, string(busy), `"error"`)
	require.Equal(t, int32(2), probe.work.Load(), "busy response must not construct a payload")
	unblock.Do(func() { close(release) })
	for range 2 {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("serialized response did not finish")
		}
	}
	require.Eventually(t, func() bool { return len(slots) == 0 }, time.Second, time.Millisecond)
}

func TestHistoryHTTPResponseBatchLimit(t *testing.T) {
	slots := make(chan struct{}, 2)
	var deadline time.Time
	probe := &historyHTTPProbe{payload: func(ctx context.Context) any {
		owner := ctx.Value(historyResponseKey{}).(*historyResponseOwner)
		if deadline.IsZero() {
			deadline = owner.deadline
		} else if !deadline.Equal(owner.deadline) {
			t.Error("batch renewed the write deadline")
		}
		return "payload"
	}}
	server := newHistoryHTTPServer(t, slots, probe)
	body := `[
		{"jsonrpc":"2.0","id":1,"method":"Probe.Payload"},
		{"jsonrpc":"2.0","id":2,"method":"Probe.Cheap"},
		{"jsonrpc":"2.0","id":3,"method":"Probe.Payload"},
		{"jsonrpc":"2.0","id":4,"method":"Probe.Payload"},
		{"jsonrpc":"2.0","method":"Probe.Payload"},
		{"jsonrpc":"2.0","id":6,"method":"Probe.Cheap"}
	]`
	data, err := historyHTTPPost(server.URL, body)
	require.NoError(t, err)
	var responses []struct {
		ID     int             `json:"id"`
		Result string          `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	require.NoError(t, json.Unmarshal(data, &responses))
	require.Len(t, responses, 5)
	require.Equal(t, "payload", responses[0].Result)
	require.Equal(t, "cheap", responses[1].Result)
	require.Equal(t, "payload", responses[2].Result)
	require.NotEmpty(t, responses[3].Error)
	require.Equal(t, "cheap", responses[4].Result)
	require.Equal(t, int32(2), probe.work.Load())
	require.Eventually(t, func() bool { return len(slots) == 0 }, time.Second, time.Millisecond)
}

func TestHistoryHTTPResponseRejectsWebSocketAndUnsupportedWriter(t *testing.T) {
	slots := make(chan struct{}, 2)
	probe := &historyHTTPProbe{payload: func(context.Context) any { return "payload" }}
	server := newHistoryHTTPServer(t, slots, probe)
	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer connection.Close()
	require.NoError(t, connection.SetReadDeadline(time.Now().Add(2*time.Second)))
	require.NoError(t, connection.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"Probe.Payload"}`)))
	_, data, err := connection.ReadMessage()
	require.NoError(t, err)
	require.Contains(t, string(data), `"error"`)
	require.NoError(t, connection.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":2,"method":"Probe.Cheap"}`)))
	_, data, err = connection.ReadMessage()
	require.NoError(t, err)
	require.Contains(t, string(data), `"result":"cheap"`)
	require.Zero(t, probe.work.Load())
	require.Zero(t, len(slots))

	// ResponseRecorder has no write-deadline support. Refuse before payload work.
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"Probe.Payload"}`))
	ctx, owner := historyResponseContext(request.Context(), recorder, slots)
	newHistoryHTTPRPC(probe).ServeHTTP(recorder, request.WithContext(ctx))
	require.Contains(t, recorder.Body.String(), `"error"`)
	require.Zero(t, probe.work.Load())
	require.Zero(t, len(slots))
	require.NoError(t, owner.release())
}

type historyHTTPFlushWriter struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
}

func (*historyHTTPFlushWriter) SetWriteDeadline(time.Time) error { return nil }
func (writer *historyHTTPFlushWriter) FlushError() error {
	close(writer.entered)
	<-writer.release
	return errors.New("flush failed")
}

func TestHistoryHTTPResponseOwnsFinalFlush(t *testing.T) {
	slots := make(chan struct{}, 2)
	writer := &historyHTTPFlushWriter{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	var unblock sync.Once
	defer unblock.Do(func() { close(writer.release) })
	probe := &historyHTTPProbe{payload: func(context.Context) any { return "payload" }}
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"Probe.Payload"}`))
	ctx, owner := historyResponseContext(request.Context(), writer, slots)
	newHistoryHTTPRPC(probe).ServeHTTP(writer, request.WithContext(ctx))
	done := make(chan error, 1)
	go func() { done <- owner.release() }()
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("final flush was not attempted")
	}
	require.Equal(t, 1, len(slots), "final flush still owns admission")
	unblock.Do(func() { close(writer.release) })
	select {
	case err := <-done:
		require.EqualError(t, err, "flush failed")
	case <-time.After(time.Second):
		t.Fatal("final flush did not finish")
	}
	require.Zero(t, len(slots))
	require.NoError(t, owner.release(), "release is idempotent after completion")
}

func TestHistoryHTTPResponseDeadlineAndCancellation(t *testing.T) {
	slots := make(chan struct{}, 2)
	earlier := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(t.Context(), earlier)
	defer cancel()
	_, owner := historyResponseContext(ctx, httptest.NewRecorder(), slots)
	require.Equal(t, earlier, owner.deadline)
	require.NoError(t, owner.release())
	cancel()
	require.ErrorIs(t, acquireHistoryResponse(ctx), context.Canceled)
	require.Zero(t, len(slots))
	ctx, owner = historyResponseContext(t.Context(), httptest.NewRecorder(), nil)
	require.Nil(t, owner)
	require.Error(t, acquireHistoryResponse(ctx), "direct calls without an HTTP owner cannot bypass admission")
	require.NoError(t, owner.release())
}

func TestHistoryHTTPResponseRechecksBatchAdmission(t *testing.T) {
	for _, stop := range []string{"cancel", "deadline"} {
		t.Run(stop, func(t *testing.T) {
			slots := make(chan struct{}, 2)
			probe := &historyHTTPProbe{}
			var cancel context.CancelFunc
			probe.payload = func(ctx context.Context) any {
				if stop == "cancel" {
					cancel()
				} else {
					// Expire admission without sleeping or expiring the network deadline.
					ctx.Value(historyResponseKey{}).(*historyResponseOwner).deadline = time.Now().Add(-time.Second)
				}
				return "payload"
			}
			rpc := newHistoryHTTPRPC(probe)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx, stopRequest := context.WithCancel(r.Context())
				defer stopRequest()
				cancel = stopRequest
				ctx, owner := historyResponseContext(ctx, w, slots)
				defer func() {
					if err := owner.release(); err != nil {
						t.Errorf("release history response: %v", err)
					}
				}()
				rpc.ServeHTTP(w, r.WithContext(ctx))
			}))
			defer server.Close()
			data, err := historyHTTPPost(server.URL, `[{"jsonrpc":"2.0","id":1,"method":"Probe.Payload"},{"jsonrpc":"2.0","id":2,"method":"Probe.Payload"}]`)
			require.NoError(t, err)
			var responses []struct {
				Result string          `json:"result"`
				Error  json.RawMessage `json:"error"`
			}
			require.NoError(t, json.Unmarshal(data, &responses))
			require.Len(t, responses, 2)
			require.Equal(t, "payload", responses[0].Result)
			require.NotEmpty(t, responses[1].Error)
			require.Equal(t, int32(1), probe.work.Load(), "a held permit cannot bypass later admission checks")
			require.Eventually(t, func() bool { return len(slots) == 0 }, time.Second, time.Millisecond)
		})
	}
}

func TestHistoryHTTPResponseSlowWriteExpires(t *testing.T) {
	slots := make(chan struct{}, 2)
	probe := &historyHTTPProbe{payload: func(context.Context) any { return strings.Repeat("x", 2<<20) }}
	rpc := newHistoryHTTPRPC(probe)
	entered, done := make(chan struct{}), make(chan error, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, owner := historyResponseContext(r.Context(), w, slots)
		owner.deadline = time.Now().Add(250 * time.Millisecond)
		defer func() { done <- owner.release() }()
		close(entered)
		rpc.ServeHTTP(w, r.WithContext(ctx))
	}))
	server.Config.ConnState = func(connection net.Conn, state http.ConnState) {
		if state == http.StateNew {
			if tcp, ok := connection.(*net.TCPConn); ok {
				if err := tcp.SetWriteBuffer(1024); err != nil {
					t.Errorf("set small test write buffer: %v", err)
				}
			}
		}
	}
	server.Start()
	defer server.Close()
	connection, err := net.Dial("tcp", server.Listener.Addr().String())
	require.NoError(t, err)
	defer connection.Close()
	require.NoError(t, connection.(*net.TCPConn).SetReadBuffer(1024))
	body := `{"jsonrpc":"2.0","id":1,"method":"Probe.Payload"}`
	_, err = io.WriteString(connection, "POST / HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: "+fmt.Sprint(len(body))+"\r\n\r\n"+body)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("slow-client request did not start")
	}
	require.Eventually(t, func() bool { return len(slots) == 1 }, time.Second, time.Millisecond)
	select {
	case err := <-done:
		require.Error(t, err, "unread large response must hit its write deadline")
	case <-time.After(2 * time.Second):
		t.Fatal("write deadline did not release slow client")
	}
	require.Zero(t, len(slots))
	require.Equal(t, int32(1), probe.work.Load())
}
