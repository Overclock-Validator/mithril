package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagedReaderWaitsForFeed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	socket := socketPath(t)
	if _, err := CheckReaderFromNode(ctx, socket, "127.0.0.1:0", PublicOptions{}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("explicit preflight error = %v, want a missing socket error", err)
	}
	leftover, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	leftover.SetUnlinkOnClose(false)
	if err := leftover.Close(); err != nil {
		t.Fatal(err)
	}
	const want = "https://dashboard.example"
	done := make(chan struct{})
	var origin string
	var waitErr error
	go func() {
		defer close(done)
		origin, waitErr = waitForManagedOrigin(ctx, socket)
	}()
	select {
	case <-done:
		t.Fatalf("managed reader exited before feed startup: %v", waitErr)
	case <-time.After(20 * time.Millisecond):
	}
	feed, err := ListenFeedWithDetails(ctx, socket, "", func() Snapshot {
		return fullTestSnapshot(time.Now().UTC())
	}, DetailSamplers{ReaderOrigin: want})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	select {
	case <-done:
		if waitErr != nil || origin != want {
			t.Fatalf("managed reader origin = %q, error = %v, want %q", origin, waitErr, want)
		}
	case <-ctx.Done():
		t.Fatal("managed reader did not recover after feed startup")
	}
	if err := os.WriteFile(socket+".txt", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := waitForManagedOrigin(ctx, socket+".txt"); err == nil || !strings.Contains(err.Error(), "is not a socket") {
		t.Fatalf("non-socket error = %v, want actionable configuration error", err)
	}
	waitCtx, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	if _, err := waitForManagedOrigin(waitCtx, socket+".missing"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled wait error = %v, want context deadline", err)
	}
}

func TestManagedReaderScopeChangesRequireCurrentStatus(t *testing.T) {
	const oldOrigin = "https://dashboard.example"
	for _, origin := range []string{"https://next-dashboard.example", ""} {
		t.Run(origin, func(t *testing.T) {
			socket := socketPath(t)
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			type source struct {
				snapshot Snapshot
				origin   string
				lease    string
			}
			var next atomic.Pointer[source]
			feed := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				sample := next.Load()
				if sample.origin != "" {
					w.Header().Set(readerOriginHeader, sample.origin)
				}
				w.Header().Set(statusRemainingHeader, sample.lease)
				if err := json.NewEncoder(w).Encode(sample.snapshot); err != nil {
					t.Error(err)
				}
			})}
			go feed.Serve(listener)
			defer feed.Close()
			reader := newCollector(socket)
			defer reader.client.CloseIdleConnections()
			reader.expectedOrigin = oldOrigin
			poll := func(origin, run, lease string, age time.Duration) string {
				snapshot := fullTestSnapshot(time.Now().UTC().Add(-age))
				snapshot.RunID = run
				next.Store(&source{snapshot: snapshot, origin: origin, lease: lease})
				return reader.poll(context.Background(), time.Now().UTC())
			}
			if reason := poll(oldOrigin, "old", "1000", 0); reason != "" || reader.last == nil {
				t.Fatalf("initial sample rejected: %s", reason)
			}
			for _, stale := range []struct {
				lease string
				age   time.Duration
			}{{"1000", 2 * StaleAfter}, {"0", 0}} {
				if reason := poll(origin, "new", stale.lease, stale.age); reason == readerOriginChanged || reader.last == nil || reader.last.RunID != "old" {
					t.Fatalf("expired sample altered the reader origin or accepted source: %q", reason)
				}
			}
			if reason := poll(origin, "new", "1000", 0); reason != readerOriginChanged || reader.last != nil {
				t.Fatalf("current scope change failed to withdraw the old sample: reason %q", reason)
			}
		})
	}
}

func TestManagedOriginBrowserSpelling(t *testing.T) {
	const uppercaseScheme = "HTTPS://dashboard.example"
	if err := ValidatePublicOrigin(uppercaseScheme); err != nil {
		t.Fatalf("explicit public origin compatibility changed: %v", err)
	}
	for _, origin := range []string{
		uppercaseScheme, "https://Dashboard.Example", "https://dashboard.example:443", "https://dashboard.example:08443",
		"https://café.example", "https://127.1", "https://0x7f000001", "https://127.0.0.1.",
		"https://[0:0:0:0:0:0:0:1]:3199", "https://[::ffff:127.0.0.1]", "https://[fe80::1%25eth0]",
		"https://[dashboard.example]", "https://[127.0.0.1]", "https://2001:db8::abcd:123",
	} {
		if err := ValidateManagedOrigin(origin); err == nil {
			t.Fatalf("managed origin accepted a browser-rewritten spelling: %s", origin)
		}
	}
	for _, origin := range []string{
		"https://dashboard.example", "https://dashboard.example:8443", "https://[::1]:8443",
		"https://xn--caf-dma.example", "https://127.0.0.1", "https://[::ffff:7f00:1]", "https://dashboard.example.",
		"https://[2001:db8::abcd]:123",
	} {
		if err := ValidateManagedOrigin(origin); err != nil {
			t.Fatalf("canonical managed origin %s: %v", origin, err)
		}
	}
}

func TestManagedReaderOriginLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	socket := socketPath(t)
	startFeed := func(origin, runID string) *Feed {
		t.Helper()
		feed, err := ListenFeedWithDetails(ctx, socket, "", func() Snapshot {
			snapshot := fullTestSnapshot(time.Now().UTC())
			snapshot.RunID = runID
			return snapshot
		}, DetailSamplers{ReaderOrigin: origin})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(feed.Close)
		return feed
	}

	const firstOrigin = "https://dashboard.example"
	const nextOrigin = "https://next-dashboard.example"
	first := startFeed(firstOrigin, "first-managed-run")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	address := listener.Addr().String()
	origin, err := CheckReaderFromNode(ctx, socket, address, PublicOptions{})
	if err != nil || origin != firstOrigin {
		t.Fatalf("managed preflight got %q, %v, want %q", origin, err, firstOrigin)
	}
	done := make(chan error, 1)
	go func() { done <- serveReader(ctx, socket, listener, origin, PublicOptions{}, true) }()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(3 * time.Second)
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/api/v1/status", nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Host = "dashboard.example"
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr == nil && response.StatusCode == http.StatusOK && strings.Contains(string(body), "first-managed-run") {
			if response.Header.Get(readerOriginHeader) != "" {
				t.Fatal("private discovery header reached the public API")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no fresh status from the managed reader")
		}
		time.Sleep(20 * time.Millisecond)
	}

	first.Close()
	second := startFeed(nextOrigin, "next-managed-run")
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "origin changed") {
			t.Fatalf("managed reader exit error = %v, want origin change", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("managed reader did not exit for an origin change")
	}
	origin, err = CheckReaderFromNode(ctx, socket, address, PublicOptions{})
	if err != nil || origin != nextOrigin {
		t.Fatalf("managed preflight after replacement got %q, %v, want %q", origin, err, nextOrigin)
	}
	second.Close()
	startFeed("", "unmanaged-run")
	if origin, err := CheckReaderFromNode(ctx, socket, address, PublicOptions{}); err == nil || origin != "" {
		t.Fatalf("managed preflight accepted an unmanaged feed: %q, %v", origin, err)
	}
}
