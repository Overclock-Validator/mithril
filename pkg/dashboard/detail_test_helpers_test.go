package dashboard

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func replayDetailHTTP(t *testing.T, client *http.Client, method, url, body string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxSnapshotBytes+1))
	if err != nil || len(data) > MaxSnapshotBytes {
		t.Fatalf("read bounded detail response: bytes=%d error=%v", len(data), err)
	}
	return resp.StatusCode, string(data), resp.Header
}

func replayDetailReaderServer(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	path := socketPath(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close(); <-done })
	return path
}

func replayDetailPublicServer(t *testing.T, path string, options PublicOptions) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveProfile(ctx, path, listener, "https://stats.example", options) }()
	return "http://" + listener.Addr().String(), cancel, done
}

func replayDetailPublicRequest(t *testing.T, address, path, host, origin string) (int, []byte, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, address+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	response, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxSnapshotBytes+1))
	if err != nil || len(body) > MaxSnapshotBytes {
		t.Fatalf("public detail body: bytes=%d error=%v", len(body), err)
	}
	return response.StatusCode, body, response.Header
}

func replayDetailStopPublic(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("dashboard did not join status and detail workers")
	}
}
