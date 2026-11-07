package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func historyTestService(t *testing.T, resources bool) *historyService {
	t.Helper()
	h, err := newHistoryService(historyTestDir(t), resources)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.store.close() })
	return h
}

func historyPublicFixture(t *testing.T, now time.Time) []byte {
	t.Helper()
	s := fullTestSnapshot(now)
	s.IdentityKey, s.GenesisHash = new("identity"), new("genesis")
	c := newCollector("/unused")
	c.public = true
	b, err := c.encode(&s, "fresh", now)
	if err != nil {
		t.Fatal(err)
	}
	return b.publicJSON
}

func TestHistoryQueueClockAndBinding(t *testing.T) {
	h := historyTestService(t, false)
	now := time.Date(2026, 11, 1, 12, 0, 30, 0, time.UTC)
	h.offer(historyPublicFixture(t, now), now)
	if len(h.queue) != 1 || h.current.Load() == nil {
		t.Fatal("first sample not offered")
	}
	h.offer(historyPublicFixture(t, now.Add(time.Second)), now.Add(time.Second))
	if h.dropped.Load() != 0 {
		t.Fatal("duplicate minute offered")
	}
	start := time.Now()
	h.offer(historyPublicFixture(t, now.Add(time.Minute)), now.Add(time.Minute))
	if time.Since(start) > time.Second || h.dropped.Load() != 1 {
		t.Fatal("full queue blocked or did not record gap")
	}
	h.offer(historyPublicFixture(t, now), now)
	if !h.clockRegressed.Load() || h.dropped.Load() != 1 {
		t.Fatal("regressed clock queued a record")
	}
	var p PublicSnapshot
	if err := json.Unmarshal(historyPublicFixture(t, now.Add(2*time.Minute)), &p); err != nil {
		t.Fatal(err)
	}
	p.Node.Identity = new("new-identity")
	b, _ := json.Marshal(p)
	h.offer(b, now.Add(2*time.Minute))
	if h.current.Load().Identity != "new-identity" || h.clockRegressed.Load() {
		t.Fatal("blocked worker hid identity replacement")
	}
}

func TestHistoryQueueMinuteBoundary(t *testing.T) {
	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, seconds := range []int{0, 58, 59} {
		t.Run(strconv.Itoa(seconds), func(t *testing.T) {
			h := historyTestService(t, false)
			now := base.Add(time.Duration(seconds) * time.Second)
			h.offer(historyPublicFixture(t, now), now)
			h.offer(historyPublicFixture(t, now.Add(time.Second)), now.Add(time.Second))
			wantDropped := uint64(0)
			if seconds == 59 {
				wantDropped = 1
			}
			if len(h.queue) != 1 || h.dropped.Load() != wantDropped {
				t.Fatalf("second%d: queue=%d dropped=%d want%d", seconds, len(h.queue), h.dropped.Load(), wantDropped)
			}
		})
	}
}

func TestHistoryCachedQueryBoundariesAndDisclosure(t *testing.T) {
	h := historyTestService(t, false)
	now := time.Now().UTC()
	r := testHistoryRecord(now, 18446744073709551615)
	h.store.records = []historyRecord{r}
	h.current.Store(&r.Binding)
	h.publish(now, "recording", "")
	allowed := func(r *http.Request) bool {
		return r.Host == "stats.example" && (r.Header.Get("Origin") == "" || r.Header.Get("Origin") == "https://stats.example")
	}
	handler := h.handler(allowed)
	for _, tc := range []struct {
		method, query, host, origin, body string
		status                            int
	}{
		{"GET", "", "stats.example", "", "", 200},
		{"GET", "?metric=completed_slot&range=1h", "stats.example", "", "", 200},
		{"GET", "?metric=cpu_percent&range=1h", "stats.example", "", "", 400},
		{"GET", "?metric=completed_slot&range=1h&extra=x", "stats.example", "", "", 400},
		{"GET", "?metric=completed_slot&metric=durable_slot&range=1h", "stats.example", "", "", 400},
		{"GET", "?metric=completed_slot&range=1h;bad", "stats.example", "", "", 400},
		{"GET", "?metric=completed_slot&range=1y", "stats.example", "", "", 400},
		{"GET", "?metric=completed_slot&range=1h", "bad.example", "", "", 403},
		{"GET", "", "stats.example", "null", "", 403},
		{"POST", "", "stats.example", "", "", 400},
		{"GET", "?", "stats.example", "", "", 400},
		{"GET", "", "stats.example", "", "body", 400},
		{"GET", "?" + strings.Repeat("a", 257), "stats.example", "", "", 400},
	} {
		req := httptest.NewRequest(tc.method, "https://stats.example/api/v1/history"+tc.query, strings.NewReader(tc.body))
		req.Host = tc.host
		if tc.origin != "" {
			req.Header.Set("Origin", tc.origin)
		}
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d %s", tc.method, tc.query, w.Code, w.Body.String())
		}
		if w.Body.Len() > MaxSnapshotBytes {
			t.Fatal("oversized response")
		}
	}
	// Mutating the worker's ring cannot affect an already published cache.
	h.store.records = nil
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest("GET", "https://stats.example/api/v1/history?metric=completed_slot&range=1h", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "18446744073709551615") {
		t.Fatal("handler did not serve immutable cached data")
	}
	if strings.Contains(string(h.cache.Load().health), "cpu_percent") {
		t.Fatal("resource opt-out catalog")
	}
	newBinding := r.Binding
	newBinding.Identity = "replacement"
	h.current.Store(&newBinding)
	w = httptest.NewRecorder()
	handler(w, httptest.NewRequest("GET", "https://stats.example/api/v1/history?metric=completed_slot&range=1h", nil))
	if w.Code != 503 || strings.Contains(w.Body.String(), "18446744073709551615") {
		t.Fatal("old binding escaped during cache rebuild")
	}
}

type blockedHistoryWriter struct {
	header           http.Header
	started, release chan struct{}
}

func (w *blockedHistoryWriter) Header() http.Header { return w.header }
func (w *blockedHistoryWriter) WriteHeader(int)     {}
func (w *blockedHistoryWriter) Write(b []byte) (int, error) {
	w.started <- struct{}{}
	<-w.release
	return len(b), nil
}

func TestHistoryQueryConcurrencyAndRateLimits(t *testing.T) {
	h := historyTestService(t, false)
	handler := h.handler(func(*http.Request) bool { return true })
	started, release := make(chan struct{}, 4), make(chan struct{})
	var group sync.WaitGroup
	for i := 0; i < 4; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			handler(&blockedHistoryWriter{header: make(http.Header), started: started, release: release}, httptest.NewRequest("GET", "http://local/api/v1/history", nil))
		}()
	}
	defer func() { close(release); group.Wait() }()
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("query not admitted")
		}
	}
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest("GET", "http://local/api/v1/history", nil))
	if w.Code != 503 {
		t.Fatalf("fifth query: %d", w.Code)
	}
	rateHandler := h.handler(func(*http.Request) bool { return true })
	rejected := false
	for i := 0; i < 32; i++ {
		w := httptest.NewRecorder()
		rateHandler(w, httptest.NewRequest("GET", "http://local/api/v1/history", nil))
		rejected = rejected || w.Code == 429
	}
	if !rejected {
		t.Fatal("rate limit not enforced")
	}
}

func TestHistoryBlockedWriterDoesNotBlockCollector(t *testing.T) {
	h := historyTestService(t, false)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h.store.write = func(f *os.File, b []byte) (int, error) {
		once.Do(func() { close(started) })
		<-release
		return f.Write(b)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go h.run(ctx)
	defer func() { cancel(); close(release); <-h.done }()
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	c.history = h
	defer c.client.CloseIdleConnections()
	now := time.Now().UTC()
	s := fullTestSnapshot(now)
	setFixture(t, body, s)
	c.poll(ctx, now)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not start")
	}
	for i := 1; i <= 3; i++ {
		at := now.Add(time.Duration(i) * time.Minute)
		s := fullTestSnapshot(at)
		setFixture(t, body, s)
		began := time.Now()
		c.poll(ctx, at)
		if time.Since(began) > time.Second || c.latest.Load() == nil {
			t.Fatal("blocked persistence stalled source polling")
		}
	}
	if h.dropped.Load() == 0 {
		t.Fatal("blocked writer did not bound its queue")
	}
}

func TestHistoryThirtyDayCacheIsBounded(t *testing.T) {
	h := historyTestService(t, true)
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	base := testHistoryRecord(now.Add(-historyRetention), 1)
	h.store.records = make([]historyRecord, 0, 30*1440)
	for i := 0; i < 30*1440; i++ {
		r := base
		r.Version = 2
		r.Chain = &historyChainRecord{Values: [historyChainMetrics]string{"1", "1", "1", "1", "1", "1"}}
		r.At += int64(i * 60)
		r.ObservedAt = r.At * 1000
		r.SourceAt = r.ObservedAt
		r.Chain.At = r.ObservedAt
		for metric := range r.Values {
			text := strconv.Itoa(i + metric)
			r.Values[metric] = &text
			r.GroupAt[metric] = r.ObservedAt
		}
		h.store.records = append(h.store.records, r)
	}
	h.current.Store(&base.Binding)
	h.publish(now, "recording", "")
	cache := h.cache.Load()
	if len(cache.queries) != len(historyMetrics)*len(historyRanges) {
		t.Fatal("missing bounded cache")
	}
	for key, b := range cache.queries {
		if len(b) > MaxSnapshotBytes {
			t.Fatalf("oversized %s", key)
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if after.HeapAlloc > before.HeapAlloc+128<<20 {
		t.Fatalf("history retained too much: %d", after.HeapAlloc-before.HeapAlloc)
	}
	runtime.KeepAlive(h)
}

func TestHistoryPublicProfileIntegration(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			path, body := fixtureFeed(t)
			now := time.Now().UTC()
			s := fullTestSnapshot(now)
			setFixture(t, body, s)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			options := PublicOptions{}
			if enabled {
				options.HistoryDir = historyTestDir(t)
			}
			go func() { done <- serveProfile(ctx, path, listener, "https://stats.example", options) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("reader did not stop")
				}
			}()
			client := &http.Client{Timeout: time.Second}
			request := func(route string) *http.Response {
				r, _ := http.NewRequest("GET", "http://"+listener.Addr().String()+route, nil)
				r.Host = "stats.example"
				resp, err := client.Do(r)
				if err != nil {
					t.Fatal(err)
				}
				return resp
			}
			resp := request("/api/v1/history")
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if !enabled && resp.StatusCode != 404 {
				t.Fatalf("default route exposed: %d", resp.StatusCode)
			}
			if enabled && resp.StatusCode != 200 && resp.StatusCode != 503 {
				t.Fatalf("enabled route: %d %s", resp.StatusCode, data)
			}
			if enabled && (resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("X-Content-Type-Options") != "nosniff") {
				t.Fatal("history bypassed public response policy")
			}
			resp = request("/api/v1/features")
			var features struct {
				History bool `json:"history"`
			}
			err = json.NewDecoder(resp.Body).Decode(&features)
			resp.Body.Close()
			if err != nil || resp.StatusCode != http.StatusOK || features.History != enabled {
				t.Fatalf("history feature flag: status=%d enabled=%v error=%v", resp.StatusCode, features.History, err)
			}
			resp = request("/api/v1/status")
			data, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 || len(data) > MaxSnapshotBytes || strings.Contains(string(data), "history_dir") {
				t.Fatal("legacy status changed")
			}
		})
	}
}

func TestHistoryRestartWithoutFeedAndNewBinding(t *testing.T) {
	path := historyTestDir(t)
	store, err := openHistoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	r := testHistoryRecord(now, 18446744073709551615)
	if _, err := store.append(r, now); err != nil {
		t.Fatal(err)
	}
	if err := store.close(); err != nil {
		t.Fatal(err)
	}
	h, err := newHistoryService(path, false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go h.run(ctx)
	defer func() { cancel(); <-h.done }()
	await := func(predicate func(*historyCache) bool) *historyCache {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			cache := h.cache.Load()
			if cache != nil && predicate(cache) {
				return cache
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("history state did not converge")
		return nil
	}
	cache := await(func(c *historyCache) bool { return strings.Contains(string(c.health), `"state":"waiting_source"`) })
	if !strings.Contains(string(cache.health), `"binding_scope":"last_recorded"`) || !strings.Contains(string(cache.queries["completed_slot/1h"]), "18446744073709551615") {
		t.Fatal("retained history unavailable during source outage")
	}
	handler := h.handler(func(*http.Request) bool { return true })
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest("GET", "http://local/api/v1/history?metric=completed_slot&range=1h", nil))
	if w.Code != 200 {
		t.Fatalf("offline history unavailable over API: %d", w.Code)
	}
	now = time.Now().UTC()
	p := PublicSnapshot{SourceState: "fresh", SampledAt: &now, Node: &PublicNode{RunID: "replacement-run", Identity: new("replacement-identity"), GenesisHash: new("replacement-genesis"), Mode: "verifying"}}
	b, _ := json.Marshal(p)
	h.offer(b, now)
	w = httptest.NewRecorder()
	handler(w, httptest.NewRequest("GET", "http://local/api/v1/history?metric=completed_slot&range=1h", nil))
	if strings.Contains(w.Body.String(), "18446744073709551615") {
		t.Fatal("previous binding exposed after a fresh replacement")
	}
	cache = await(func(c *historyCache) bool {
		return strings.Contains(string(c.health), `"binding_scope":"observed_source"`) && strings.Contains(string(c.health), "replacement-genesis")
	})
	if strings.Contains(string(cache.queries["completed_slot/1h"]), "18446744073709551615") {
		t.Fatal("old identity records joined to replacement")
	}
}
