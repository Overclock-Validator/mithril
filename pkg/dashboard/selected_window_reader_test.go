package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

type selectedBlockingTransport struct {
	next    http.RoundTripper
	started chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func TestSelectedWindowSlowViewersRemainBounded(t *testing.T) {
	core := replayDetailLiveCore(0)
	now := time.Now()
	window := selectedWindowFixture(1)
	window.Binding, window.CapturedAt, window.ObservedAt = core.Binding, now.UTC(), now.UTC()
	prepared, err := prepareSelectedWindow(window)
	if err != nil {
		t.Fatal(err)
	}
	publication := &replayDetailPrepared{binding: window.Binding, generation: prepared.digest,
		capturedAt: now, pages: prepared.pages, bytes: prepared.bytes}
	for i := range publication.manifests {
		publication.manifests[i] = prepared.manifest
	}
	var gate atomic.Pointer[replayDetailGate]
	g := replayGate(replayDetailLiveStatus(core), now)
	g.revision = 1
	g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	r := &selectedWindowReader{gate: &gate, revision: 1, limit: rate.NewLimiter(16, 32), active: make(chan struct{}, 4)}
	if err := r.cache.publish(publication, g, now); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var viewers sync.WaitGroup
	for range 4 {
		viewers.Go(func() {
			sink := &replayBundleSink{header: make(http.Header), onFirstWrite: func() { entered <- struct{}{}; <-release }}
			r.serveHTTP(sink, httptest.NewRequest(http.MethodGet, "/api/v1/details/selected", nil), allowReplayStatusForTest)
		})
	}
	for range 4 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			viewers.Wait()
			t.Fatal("viewer did not reach the bounded write")
		}
	}
	response := httptest.NewRecorder()
	r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/selected", nil), allowReplayStatusForTest)
	if response.Code != http.StatusTooManyRequests {
		t.Errorf("fifth slow viewer: got %d, want 429", response.Code)
	}
	if !r.mu.TryLock() {
		t.Error("slow viewer held the reader lock")
	} else {
		r.mu.Unlock()
	}
	close(release)
	viewers.Wait()
	if len(r.active) != 0 {
		t.Fatal("viewer admission leaked after completion")
	}
}

func (t *selectedBlockingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.Contains(r.URL.RawQuery, "page=") && t.once.Swap(false) {
		close(t.started)
		select {
		case <-t.release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}
	return t.next.RoundTrip(r)
}

func TestSelectedWindowIsolatedReader(t *testing.T) {
	core := replayDetailLiveCore(0)
	status := replayDetailLiveStatus(core)
	window := selectedWindowFixture(256)
	window.WindowEntries = 512
	window.Binding, window.ObservedAt = core.Binding, time.Now().UTC()
	feed, err := ListenFeedWithDetails(context.Background(), socketPath(t), "", func() Snapshot { return status }, DetailSamplers{
		Replay:         func(at time.Time) (ReplayDetailCore, error) { c := core; c.CapturedAt = at.UTC(); return c, nil },
		SelectedWindow: func(at time.Time) (SelectedWindow, error) { w := window; w.CapturedAt = at.UTC(); return w, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	client := newCollector(feed.listener.Addr().String()).client
	t.Cleanup(client.CloseIdleConnections)
	deadline := time.Now().Add(3 * time.Second)
	for {
		code, _, _ := replayDetailHTTP(t, client, "GET", "http://node/details/selected", "")
		if code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("private selected source not ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	var gate atomic.Pointer[replayDetailGate]
	g := replayGate(status, time.Now())
	g.revision = 1
	g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	r := &selectedWindowReader{client: client, gate: &gate, limit: rate.NewLimiter(16, 40), active: make(chan struct{}, 4)}
	r.collect(context.Background())
	read := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest("GET", path, nil), allowReplayStatusForTest)
		return response
	}
	root := read("/api/v1/details/selected")
	var m selectedWindowManifest
	if root.Code != http.StatusOK || json.Unmarshal(root.Body.Bytes(), &m) != nil || root.Header().Get(replayDetailRemainingHeader) == "" {
		t.Fatalf("isolated reader manifest: %d %s", root.Code, root.Body.String())
	}
	pages := make([][]byte, m.PageCount)
	for i := range pages {
		p := read(fmt.Sprintf("/api/v1/details/selected?generation=%s&page=%d", m.Digest, i))
		if p.Code != http.StatusOK {
			t.Fatalf("page %d: %d %s", i, p.Code, p.Body.String())
		}
		pages[i] = p.Body.Bytes()
	}
	decoded, err := decodeSelectedWindow(root.Body.Bytes(), pages)
	if err != nil || len(decoded.Records) != 256 || decoded.Records[255].Slot != window.Records[255].Slot {
		t.Fatal("isolated reader lost selected records", err)
	}
	firstRemaining, err := strconv.ParseInt(root.Header().Get(replayDetailRemainingHeader), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	r.collect(context.Background())
	repeated := read("/api/v1/details/selected")
	remaining, err := strconv.ParseInt(repeated.Header().Get(replayDetailRemainingHeader), 10, 64)
	if err != nil || repeated.Code != http.StatusOK || remaining > firstRemaining {
		t.Fatal("repeated source capture extended its original lease", err)
	}
	// A different node followed by the original must not revive cached A.
	b := g
	b.RunID, b.revision = "replacement", 2
	b.stamp = statusStamp{nodeEpoch: 2, familyEpoch: 2}
	gate.Store(&b)
	a := g
	a.revision = 3
	a.stamp = statusStamp{nodeEpoch: 3, familyEpoch: 3}
	gate.Store(&a)
	if read("/api/v1/details/selected").Code == http.StatusOK ||
		read(fmt.Sprintf("/api/v1/details/selected?generation=%s&page=0", m.Digest)).Code == http.StatusOK {
		t.Fatal("old selected generation revived after A -> B -> A")
	}
	r.collect(context.Background())
	if read("/api/v1/details/selected").Code != http.StatusOK {
		t.Fatal("reader did not recover on the current generation")
	}
	blocked := &selectedBlockingTransport{next: client.Transport, started: make(chan struct{}), release: make(chan struct{})}
	blocked.once.Store(true)
	r.client = &http.Client{Transport: blocked, Timeout: time.Second}
	done := make(chan struct{})
	go func() { defer close(done); r.collect(context.Background()) }()
	select {
	case <-blocked.started:
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not start a page fetch")
	}
	b2 := b
	b2.revision = 4
	b2.stamp = statusStamp{nodeEpoch: 4, familyEpoch: 4}
	gate.Store(&b2)
	a2 := a
	a2.revision = 5
	a2.stamp = statusStamp{nodeEpoch: 5, familyEpoch: 5}
	gate.Store(&a2)
	close(blocked.release)
	<-done
	if read("/api/v1/details/selected").Code == http.StatusOK {
		t.Fatal("in-flight pre-replacement data became visible")
	}
	r.client = client
	r.collect(context.Background())
	if read("/api/v1/details/selected").Code != http.StatusOK {
		t.Fatal("reader did not recover after interrupted collection")
	}
}

func TestSelectedWindowReaderRouteOptIn(t *testing.T) {
	core := replayDetailLiveCore(0)
	window := selectedWindowFixture(256)
	window.Binding, window.ObservedAt = core.Binding, time.Now().UTC()
	var captures atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot {
		return replayDetailLiveStatus(core)
	}, DetailSamplers{
		Replay: func(at time.Time) (ReplayDetailCore, error) {
			c := core
			c.CapturedAt = at.UTC()
			return c, nil
		},
		SelectedWindow: func(at time.Time) (SelectedWindow, error) {
			captures.Add(1)
			w := window
			w.CapturedAt = at.UTC()
			return w, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: enabled})
			t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
			const route = "/api/v1/details/selected"
			status, body, _ := replayDetailPublicRequest(t, address, route, "stats.example", "")
			if !enabled {
				if status != http.StatusNotFound {
					t.Fatalf("selected route enabled by default: %d", status)
				}
				return
			}
			deadline := time.Now().Add(5 * time.Second)
			for status != http.StatusOK && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
				status, body, _ = replayDetailPublicRequest(t, address, route, "stats.example", "")
			}
			m, err := decodeSelectedWindowManifest(body)
			if status != http.StatusOK || err != nil || m.Count != 256 {
				t.Fatalf("selected manifest: %d %s (%v)", status, body, err)
			}
			pages := make([][]byte, m.PageCount)
			for i := range pages {
				// Stay within the route's rate limit while exercising all pages.
				time.Sleep(70 * time.Millisecond)
				query := fmt.Sprintf("%s?generation=%s&page=%d", route, m.Digest, i)
				status, page, headers := replayDetailPublicRequest(t, address, query, "stats.example", "https://stats.example")
				if status != http.StatusOK || headers.Get("Cache-Control") != "no-store" {
					t.Fatalf("selected page %d: %d %s", i, status, page)
				}
				pages[i] = page
			}
			got, err := decodeSelectedWindow(body, pages)
			if err != nil || len(got.Records) != 256 || got.Records[255].Slot != window.Records[255].Slot {
				t.Fatalf("selected page data: %v", err)
			}
			for _, tc := range []struct{ host, origin string }{
				{"attacker.example", ""}, {"stats.example", "https://attacker.example"},
			} {
				status, _, _ := replayDetailPublicRequest(t, address, route, tc.host, tc.origin)
				if status != http.StatusForbidden {
					t.Fatalf("host/origin boundary: %d", status)
				}
			}
			if captures.Load() != 1 {
				t.Fatal("viewer requests triggered a source capture")
			}
		})
	}
}

func TestSelectedWindowReaderWithOlderPrivateFeed(t *testing.T) {
	core := replayDetailLiveCore(0)
	status := replayDetailLiveStatus(core)
	path := socketPath(t)
	feed, err := ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot { return status },
		func(at time.Time) (ReplayDetailCore, error) { c := core; c.CapturedAt = at.UTC(); return c, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	client := newCollector(path).client
	t.Cleanup(client.CloseIdleConnections)
	var gate atomic.Pointer[replayDetailGate]
	g := replayGate(status, time.Now())
	g.revision = 1
	g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	r := &selectedWindowReader{client: client, gate: &gate, limit: rate.NewLimiter(16, 40), active: make(chan struct{}, 4)}
	r.collect(context.Background())
	response := httptest.NewRecorder()
	r.serveHTTP(response, httptest.NewRequest("GET", "/api/v1/details/selected", nil), allowReplayStatusForTest)
	if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), `"reason":"unsupported_source"`) {
		t.Fatalf("older source was not explicitly unavailable: %d %s", response.Code, response.Body.String())
	}
}

func TestSelectedWindowReaderStoppingStatusFence(t *testing.T) {
	core := replayDetailLiveCore(0)
	status := replayDetailLiveStatus(core)
	c := &collector{last: &status}
	c.observeReplayDetailStatus("fresh", "", time.Now(), 1)
	first := c.detailGate.Load()
	if first == nil || first.stopping || first.revision != 1 {
		t.Fatal("initial selected status fence is invalid")
	}
	c.observeReplayDetailStatus("fresh", "", time.Now(), 1)
	if c.detailGate.Load().revision != first.revision {
		t.Fatal("ordinary status refresh advanced the revision")
	}
	stopping := status
	stopping.Phase = "stopping"
	c.last = &stopping
	c.observeReplayDetailStatus("fresh", "", time.Now(), 1)
	stopped := c.detailGate.Load()
	if !stopped.stopping || stopped.revision != first.revision+1 {
		t.Fatal("stopping status did not invalidate the selected revision")
	}
	r := &selectedWindowReader{gate: &c.detailGate, limit: rate.NewLimiter(16, 40), active: make(chan struct{}, 4)}
	response := httptest.NewRecorder()
	r.serveHTTP(response, httptest.NewRequest("GET", "/api/v1/details/selected", nil), allowReplayStatusForTest)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"reason":"source_unavailable"`) {
		t.Fatalf("stopping status remained available: %d %s", response.Code, response.Body.String())
	}
	c.last = &status
	c.observeReplayDetailStatus("fresh", "", time.Now(), 1)
	if c.detailGate.Load().stopping || c.detailGate.Load().revision != stopped.revision+1 {
		t.Fatal("recovery did not create a new selected revision")
	}
}
