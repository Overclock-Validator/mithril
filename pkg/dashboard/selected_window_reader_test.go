package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

func TestSelectedWindowCachePressureKeepsOriginalBundle(t *testing.T) {
	for _, tc := range []struct {
		name, remaining, prior              string
		invalid                             bool
		differentGeneration, differentReset bool
		want                                int
		failure                             string
	}{
		{name: "same binding retains original bundle", remaining: "30000", want: 200},
		{name: "prior integrity failure stays closed", remaining: "30000", prior: "source_invalid", want: 503, failure: "source_invalid"},
		{name: "low original budget stays closed", remaining: "13000", want: 503, failure: "generation_not_ready"},
		{name: "invalid source stays closed", remaining: "30000", invalid: true, want: 503, failure: "source_invalid"},
		{name: "new generation replaces rather than falls back", remaining: "30000", differentGeneration: true, want: 200},
		{name: "new generation low budget cannot fall back", remaining: "13000", differentGeneration: true, want: 503, failure: "generation_not_ready"},
		{name: "reset mismatch cannot fall back", remaining: "30000", differentReset: true, want: 503, failure: "source_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			core := replayDetailLiveCore(0)
			g := replayGate(replayDetailLiveStatus(core), now)
			g.revision = 1
			g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
			var gate atomic.Pointer[replayDetailGate]
			gate.Store(&g)
			candidate := selectedWindowFixture(1)
			candidate.Binding = core.Binding
			candidate.CapturedAt = now.UTC()
			candidate.ObservedAt = now.UTC()
			prepared, err := prepareSelectedWindow(candidate)
			if err != nil {
				t.Fatal(err)
			}
			r := &selectedWindowReader{gate: &gate, revision: g.revision, limit: rate.NewLimiter(16, 32), active: make(chan struct{}, 4)}
			for i := 0; i < replayDetailGenerations; i++ {
				captured := now.Add(time.Duration(-20+i*5) * time.Second)
				published := now.Add(time.Duration(-8+i*2) * time.Second)
				w := candidate
				w.CapturedAt = captured.UTC()
				w.ObservedAt = captured.UTC()
				old, err := prepareSelectedWindow(w)
				if err != nil {
					t.Fatal(err)
				}
				bundle, err := selectedWindowBundle(old)
				if err != nil {
					t.Fatal(err)
				}
				item := &replayDetailPrepared{binding: w.Binding, generation: old.digest, capturedAt: captured, pages: old.pages, bytes: old.bytes + len(bundle), selectedBundle: bundle}
				for j := range item.manifests {
					item.manifests[j] = old.manifest
				}
				at := g
				at.ObservedAt = published
				if err := r.cache.publish(item, at, published); err != nil {
					t.Fatal(err)
				}
			}
			original := r.cache.items
			latest := original[len(original)-1]
			if latest == nil || !sameReplayDetailBinding(latest.prepared.binding, candidate.Binding) {
				t.Fatal("fixture is not full/same binding")
			}
			read := func() *httptest.ResponseRecorder {
				out := httptest.NewRecorder()
				r.serveHTTP(out, httptest.NewRequest(http.MethodGet, "/api/v1/details/selected-window", nil), allowReplayStatusForTest)
				return out
			}
			initial := read()
			if initial.Code != 200 {
				t.Fatalf("original bundle not fresh: %d", initial.Code)
			}
			if tc.prior != "" {
				r.failure = &selectedWindowFailure{revision: g.revision, stamp: g.stamp, reason: tc.prior}
			}
			if tc.differentGeneration {
				candidate.Binding.SourceGeneration++
			}
			if tc.differentReset {
				candidate.Binding.ResetAt = candidate.Binding.ResetAt.Add(time.Millisecond)
			}
			prepared, err = prepareSelectedWindow(candidate)
			if err != nil {
				t.Fatal(err)
			}
			requests := 0
			r.client = &http.Client{Transport: selectedEpochTransport(func(req *http.Request) (*http.Response, error) {
				requests++
				body := prepared.manifest
				if req.URL.RawQuery != "" {
					body = prepared.pages[0]
				}
				if tc.invalid {
					body = "{}"
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{replayDetailRemainingHeader: []string{tc.remaining}}, ContentLength: int64(len(body)), Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			r.collect(context.Background())
			out := read()
			if out.Code != tc.want {
				t.Fatalf("capacity pressure hid original bundle: got %d want %d", out.Code, tc.want)
			}
			if tc.failure != "" {
				if r.failure == nil || r.failure.reason != tc.failure {
					t.Fatalf("failure changed: %+v", r.failure)
				}
			} else if r.failure != nil {
				t.Fatalf("capacity became source failure: %+v", r.failure)
			}
			if !tc.differentGeneration && r.cache.items != original {
				t.Fatal("capacity retry replaced/renewed original publication")
			}
			if tc.differentGeneration && tc.want == 200 {
				if r.cache.items[0] == original[0] || !sameReplayDetailBinding(r.cache.items[0].prepared.binding, candidate.Binding) || out.Body.String() == initial.Body.String() {
					t.Fatal("new source generation used old fallback")
				}
				return
			}
			if tc.want == 200 {
				if requests != 2 || out.Body.String() != initial.Body.String() {
					t.Fatal("retained bundle changed or new candidate was published")
				}
				if latest.publishedAt != now.Add(-4*time.Second) || latest.holdUntil != now.Add(26*time.Second) || latest.prepared.capturedAt != now.Add(-10*time.Second) {
					t.Fatal("original clocks renewed")
				}
				expiry := latest.prepared.capturedAt.Add(replayDetailFreshFor)
				future := g
				future.ObservedAt = expiry
				if r.cache.lookupWithBudget("", false, future, expiry, time.Millisecond).status == 200 {
					t.Fatal("original source expiry revived")
				}
				replacement := g
				replacement.stamp.familyEpoch++
				gate.Store(&replacement)
				if read().Code == 200 {
					t.Fatal("retained bundle crossed status stamp")
				}
			}
		})
	}
}

// Only native public-safe failure enums may cross the private reader boundary.
func TestSelectedWindowPrivateFailureReasons(t *testing.T) {
	canonical := func(reason string) string {
		return `{"schema_version":1,"family":"selected_slots","state":"unavailable","reason":"` + reason + `"}`
	}
	cases := []struct {
		name, body, want string
		status           int
		lease            bool
		length           int64
	}{
		{name: "waiting source", body: canonical("waiting_source"), want: "waiting_source", status: 503, length: -1},
		{name: "binding changed stays 503", body: canonical("binding_changed"), want: "binding_changed", status: 503, length: -1},
		{name: "generation not ready", body: canonical("generation_not_ready"), want: "generation_not_ready", status: 503, length: -1},
		{name: "wrong family", body: strings.Replace(canonical("waiting_source"), "selected_slots", "replay", 1), want: "source_invalid", status: 503, length: -1},
		{name: "unknown private text", body: canonical("private-secret-marker"), want: "source_invalid", status: 503, length: -1},
		{name: "extra private key", body: strings.TrimSuffix(canonical("waiting_source"), "}") + `,"private":"private-secret-marker"}`, want: "source_invalid", status: 503, length: -1},
		{name: "duplicate key", body: strings.Replace(canonical("waiting_source"), `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1), want: "source_invalid", status: 503, length: -1},
		{name: "noncanonical body", body: canonical("waiting_source") + " ", want: "source_invalid", status: 503, length: -1},
		{name: "error cannot grant lease", body: canonical("waiting_source"), want: "source_invalid", status: 503, lease: true, length: -1},
		{name: "declared oversize", body: canonical("waiting_source"), want: "response_limit", status: 503, length: 513},
		{name: "chunked oversize", body: strings.Repeat("x", 2048), want: "response_limit", status: 503, length: -1},
		{name: "404 unchanged", body: "", want: "unsupported_source", status: 404, length: -1},
		{name: "409 unchanged", body: "", want: "source_generation_expired", status: 409, length: -1},
		{name: "429 unchanged", body: "", want: "source_unavailable", status: 429, length: -1},
	}
	for _, reason := range []string{"status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "response_limit"} {
		cases = append(cases, struct {
			name, body, want string
			status           int
			lease            bool
			length           int64
		}{name: reason, body: canonical(reason), want: reason, status: 503, length: -1})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			core := replayDetailLiveCore(0)
			g := replayGate(replayDetailLiveStatus(core), time.Now())
			g.revision = 1
			g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
			var gate atomic.Pointer[replayDetailGate]
			gate.Store(&g)
			var source *strings.Reader
			r := &selectedWindowReader{gate: &gate, limit: rate.NewLimiter(16, 32), active: make(chan struct{}, 4)}
			r.client = &http.Client{Transport: selectedEpochTransport(func(*http.Request) (*http.Response, error) {
				source = strings.NewReader(tc.body)
				headers := make(http.Header)
				if tc.lease {
					headers.Set(replayDetailRemainingHeader, "30000")
				}
				return &http.Response{StatusCode: tc.status, Header: headers, ContentLength: tc.length, Body: io.NopCloser(source)}, nil
			})}
			r.collect(context.Background())
			wantInternal := tc.want
			if tc.want == "binding_changed" {
				wantInternal = "private_binding_changed"
			}
			if r.failure == nil || r.failure.reason != wantInternal {
				t.Fatalf("private failure got %+v want %s", r.failure, wantInternal)
			}
			if len(tc.body)-source.Len() > 513 {
				t.Fatal("error body read exceeded fixed cap")
			}
			out := httptest.NewRecorder()
			r.serveHTTP(out, httptest.NewRequest(http.MethodGet, "/api/v1/details/selected-window", nil), allowReplayStatusForTest)
			wantStatus := 503
			if tc.status == 404 || tc.status == 409 {
				wantStatus = tc.status
			}
			var body struct{ Family, State, Reason string }
			if json.Unmarshal(out.Body.Bytes(), &body) != nil || body.Family != "selected_slots" || body.State != "unavailable" || body.Reason != tc.want || out.Code != wantStatus {
				t.Fatalf("public failure %d %s", out.Code, out.Body.String())
			}
			if strings.Contains(out.Body.String(), "private-secret-marker") || out.Header().Get(replayDetailRemainingHeader) != "" || out.Header().Get(statusRemainingHeader) != "" || r.last != nil || r.cache.items != [replayDetailGenerations]*replayDetailPublication{} {
				t.Fatal("error leaked data or admitted/renewed a collection")
			}
			if tc.want == "binding_changed" {
				r.failure.reason = "binding_changed" // Existing local cache.publish conflict classification.
				local := httptest.NewRecorder()
				r.serveHTTP(local, httptest.NewRequest(http.MethodGet, "/api/v1/details/selected-window", nil), allowReplayStatusForTest)
				if local.Code != http.StatusConflict || !strings.Contains(local.Body.String(), `"reason":"binding_changed"`) {
					t.Fatal("local binding conflict no longer preserves 409")
				}
			}
		})
	}
}
