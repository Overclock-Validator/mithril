package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Short paths also work on macOS, whose Unix socket path limit is 104 bytes.
func socketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "mithril-dashboard-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	return filepath.Join(dir, "node.sock")
}

func TestFeedIsPrivateCachedAndReusable(t *testing.T) {
	path := socketPath(t)
	var calls atomic.Int64
	feed, err := ListenFeed(context.Background(), path, func() Snapshot {
		calls.Add(1)
		return testSnapshot(time.Now().UTC())
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode: %v", info.Mode())
	}
	c := newCollector(path)
	t.Cleanup(c.client.CloseIdleConnections)
	before := calls.Load()
	started := time.Now()
	for range 30 {
		response, err := c.client.Get("http://node/snapshot")
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		var s Snapshot
		if response.StatusCode != 200 || json.Unmarshal(b, &s) != nil || s.validate() != nil {
			t.Fatalf("invalid feed: %d %s", response.StatusCode, b)
		}
		if response.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("feed may be cached")
		}
	}
	// Only the independent timer can add calls, never the 30 requests.
	if calls.Load()-before > 1+int64(time.Since(started)/SampleInterval) {
		t.Fatalf("reader requests called sampler: %d", calls.Load()-before)
	}
	feed.Close()
	feed.Close()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("closed socket remains: %v", err)
	}
	reopened, err := ListenFeed(context.Background(), path, func() Snapshot { return testSnapshot(time.Now().UTC()) })
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
}

func TestFeedRefusesUnsafePathsAndDoesNotDeleteExistingFiles(t *testing.T) {
	path := socketPath(t)
	sample := func() Snapshot { return testSnapshot(time.Now().UTC()) }
	if _, err := ListenFeed(context.Background(), "relative.sock", sample); err == nil {
		t.Fatal("relative path accepted")
	}
	if _, err := ListenFeed(context.Background(), path, nil); err == nil {
		t.Fatal("nil sampler accepted")
	}
	if err := os.Chmod(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenFeed(context.Background(), path, sample); err == nil {
		t.Fatal("public directory accepted")
	}
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenFeed(context.Background(), path, sample); err == nil {
		t.Fatal("existing file overwritten")
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "keep" {
		t.Fatalf("existing file changed: %q %v", b, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if _, err := ListenFeed(context.Background(), path, sample); err == nil {
		t.Fatal("live socket replaced")
	}
}

func TestFeedRefusesStaleSocketUntilRemoved(t *testing.T) {
	path := socketPath(t)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Lstat(path)
	if err != nil || before.Mode()&os.ModeSocket == 0 {
		t.Fatalf("stale socket: %v", err)
	}
	sample := func() Snapshot { return testSnapshot(time.Now().UTC()) }
	if feed, err := ListenFeed(context.Background(), path, sample); err == nil {
		feed.Close()
		t.Fatal("stale socket replaced")
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("stale socket changed: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	feed, err := ListenFeed(context.Background(), path, sample)
	if err != nil {
		t.Fatal(err)
	}
	feed.Close()
}

func TestFeedAllowsOnlySnapshotReads(t *testing.T) {
	path := socketPath(t)
	feed, err := ListenFeed(context.Background(), path, func() Snapshot { return testSnapshot(time.Now().UTC()) })
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	c := newCollector(path)
	defer c.client.CloseIdleConnections()
	for _, test := range []struct {
		method, path, body string
		status             int
	}{
		{"POST", "/snapshot", "", 400}, {"GET", "/snapshot?path=/etc/passwd", "", 400},
		{"GET", "/snapshot", "{}", 400}, {"GET", "/admin", "", 404}, {"GET", "/debug/pprof/", "", 404}, {"GET", "/details/network", "", 404},
	} {
		request, err := http.NewRequest(test.method, "http://node"+test.path, bytes.NewBufferString(test.body))
		if err != nil {
			t.Fatal(err)
		}
		response, err := c.client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != test.status {
			t.Errorf("%s %s: got %d, want %d", test.method, test.path, response.StatusCode, test.status)
		}
	}
}

func TestInvalidSamplerDoesNotPublish(t *testing.T) {
	path := socketPath(t)
	feed, err := ListenFeed(context.Background(), path, func() Snapshot { return Snapshot{} })
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	c := newCollector(path)
	defer c.client.CloseIdleConnections()
	c.poll(context.Background(), time.Now().UTC())
	if statusFields(t, c.latest.Load())["source_state"] != "unavailable" {
		t.Fatal("invalid sample published")
	}
}
