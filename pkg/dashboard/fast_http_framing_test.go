package dashboard

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

// fastPostFlushWriter holds a successful body flush before net/http can write
// its trailing chunk, while leaving the real connection deadline observable.
type fastPostFlushWriter struct {
	http.ResponseWriter
	flushed chan error
	expired chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (w *fastPostFlushWriter) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	w.flushed <- err
	<-w.release
	return err
}

func (w *fastPostFlushWriter) SetWriteDeadline(at time.Time) error {
	err := http.NewResponseController(w.ResponseWriter).SetWriteDeadline(at)
	if err == nil && !at.After(time.Now()) {
		w.once.Do(func() { close(w.expired) })
	}
	return err
}

type fastFramingBody struct {
	io.ReadCloser
	arrived     bytes.Buffer
	err         error
	prefixRead  chan struct{}
	releaseRead <-chan struct{}
}

func (b *fastFramingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.arrived.Write(p[:n])
	if n > 0 && b.prefixRead != nil {
		close(b.prefixRead)
		b.prefixRead = nil
		<-b.releaseRead
	}
	if err != nil {
		b.err = err
	}
	return n, err
}

type fastFramingTransport struct {
	*http.Transport
	body        *fastFramingBody
	length      int64
	chunked     bool
	prefixRead  chan struct{}
	releaseRead <-chan struct{}
}

func (t *fastFramingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := t.Transport.RoundTrip(r)
	if err == nil {
		t.length = response.ContentLength
		t.chunked = len(response.TransferEncoding) == 1 && response.TransferEncoding[0] == "chunked"
		t.body = &fastFramingBody{ReadCloser: response.Body, prefixRead: t.prefixRead, releaseRead: t.releaseRead}
		response.Body = t.body
	}
	return response, err
}

func TestFastResponseCompleteBodySurvivesTrailingFramingInvalidation(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { serverConn.Close(); clientConn.Close() })
	listener := &pipeListener{connections: make(chan net.Conn, 1), done: make(chan struct{})}
	listener.connections <- serverConn
	t.Cleanup(func() { listener.Close() })
	flushed, expired, release := make(chan error, 1), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	invalidated, handled := make(chan struct{}), make(chan struct{})
	body := []byte(`{"complete":true}`)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handled)
		held := &fastPostFlushWriter{ResponseWriter: w, flushed: flushed, expired: expired, release: release}
		writeFastResponse(held, r, body, time.Time{}, time.Now().Add(3*time.Second), invalidated, nil, func() bool { return true })
	})}
	t.Cleanup(func() { server.Close() })
	go server.Serve(listener)
	transport := &fastFramingTransport{Transport: &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) { return clientConn, nil },
	}}
	t.Cleanup(transport.CloseIdleConnections)
	type result struct {
		body      []byte
		remaining time.Duration
		reason    string
	}
	fetched := make(chan result, 1)
	go func() {
		value, remaining, _, reason := fetchDetail(context.Background(), &http.Client{Transport: transport}, "recent-executed", 16<<10, nil,
			func(v string) (time.Duration, error) { return parseDetailRemaining(v, StaleAfter) },
			func([]byte) string { return "source_unavailable" })
		fetched <- result{value, remaining, reason}
	}()
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("body did not flush")
	}
	close(invalidated)
	select {
	case <-expired:
	case <-time.After(time.Second):
		t.Fatal("invalidation did not expire the real write deadline")
	}
	unblock()
	select {
	case got := <-fetched:
		if got.reason != "" || !bytes.Equal(got.body, body) || got.remaining <= 0 {
			if transport.body == nil {
				t.Fatalf("fetch failed before response: %s", got.reason)
			}
			t.Fatalf("complete flushed body rejected: reason=%q received=%q read_error=%v length=%d chunked=%t", got.reason, transport.body.arrived.String(), transport.body.err, transport.length, transport.chunked)
		}
		if transport.length != int64(len(body)) || transport.chunked {
			t.Fatal("known complete body did not use exact Content-Length")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fetch did not finish")
	}
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not finish after invalidation")
	}
}

func TestFastResponsePartialBodyRejectedAfterInvalidation(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { serverConn.Close(); clientConn.Close() })
	listener := &pipeListener{connections: make(chan net.Conn, 1), done: make(chan struct{})}
	listener.connections <- serverConn
	t.Cleanup(func() { listener.Close() })
	prefixRead, expired, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	invalidated, handled := make(chan struct{}), make(chan struct{})
	// More than the HTTP buffers, but still within the real 16KiB detail bound:
	// holding the first positive client Read leaves the body writer blocked.
	body := append([]byte(`{"payload":"`), bytes.Repeat([]byte("x"), 12<<10)...)
	body = append(body, []byte(`"}`)...)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handled)
		held := &fastPostFlushWriter{ResponseWriter: w, flushed: make(chan error, 1), expired: expired, release: release}
		writeFastResponse(held, r, body, time.Time{}, time.Now().Add(3*time.Second), invalidated, nil, func() bool { return true })
	})}
	t.Cleanup(func() { server.Close() })
	go server.Serve(listener)
	transport := &fastFramingTransport{Transport: &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) { return clientConn, nil },
	}, prefixRead: prefixRead, releaseRead: release}
	t.Cleanup(transport.CloseIdleConnections)
	type result struct {
		body      []byte
		remaining time.Duration
		reason    string
	}
	fetched := make(chan result, 1)
	go func() {
		value, remaining, _, reason := fetchDetail(context.Background(), &http.Client{Transport: transport}, "recent-executed", 16<<10, nil,
			func(v string) (time.Duration, error) { return parseDetailRemaining(v, StaleAfter) },
			func([]byte) string { return "source_unavailable" })
		fetched <- result{value, remaining, reason}
	}()
	select {
	case <-prefixRead:
	case <-time.After(time.Second):
		t.Fatal("client did not receive a positive body prefix")
	}
	close(invalidated)
	select {
	case <-expired:
	case <-time.After(time.Second):
		t.Fatal("invalidation did not expire the real write deadline")
	}
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("partial body writer did not abort while the client was paused")
	}
	unblock()
	select {
	case got := <-fetched:
		if got.reason != "source_unavailable" || got.body != nil || got.remaining != 0 {
			t.Fatalf("partial body was accepted: reason=%q bytes=%d remaining=%v", got.reason, len(got.body), got.remaining)
		}
		if transport.body == nil {
			t.Fatal("fetch failed before receiving a response body")
		}
		arrived := transport.body.arrived.Bytes()
		if len(arrived) == 0 || len(arrived) >= len(body) || !bytes.Equal(arrived, body[:len(arrived)]) {
			t.Fatalf("expected a positive incomplete prefix: received=%d declared=%d", len(arrived), len(body))
		}
		if transport.length != int64(len(body)) || transport.chunked || !errors.Is(transport.body.err, io.ErrUnexpectedEOF) {
			t.Fatalf("partial exact-length body did not fail its read: length=%d chunked=%t read_error=%v", transport.length, transport.chunked, transport.body.err)
		}
		t.Logf("received=%d declared=%d read_error=%v fetch_reason=%s", len(arrived), len(body), transport.body.err, got.reason)
	case <-time.After(2 * time.Second):
		t.Fatal("partial-body fetch did not finish")
	}
}
