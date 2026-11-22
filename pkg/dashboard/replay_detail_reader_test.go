package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func replayDetailReaderFixture(t *testing.T, core ReplayDetailCore, now time.Time, path string) *replayDetailReader {
	t.Helper()
	var gate atomic.Pointer[replayDetailGate]
	g := replayDetailTestGate(core, now)
	gate.Store(&g)
	r, err := newReplayDetailReader(newCollector(path).client, &gate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.client.CloseIdleConnections)
	return r
}

func TestReplayDetailReaderStartupRetryIsBounded(t *testing.T) {
	calls := make(chan struct{}, 3)
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		calls <- struct{}{}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	now := time.Now()
	reader := replayDetailReaderFixture(t, replayDetailReaderWindow(0, 0, now), now, path)
	ctx, cancel := context.WithCancel(context.Background())
	go reader.run(ctx)
	t.Cleanup(func() { cancel(); <-reader.done })
	for range 2 {
		select {
		case <-calls:
		case <-time.After(3 * time.Second):
			t.Fatal("initial attempt or one early retry was not made")
		}
	}
	select {
	case <-calls:
		t.Fatal("initial failure caused repeated early retries")
	case <-time.After(1200 * time.Millisecond):
	}
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

func replayDetailReaderReply(w http.ResponseWriter, request *http.Request, p *replayDetailPrepared) {
	w.Header().Set("Content-Type", "application/json")
	_, page, pageRequest, ok := parseReplayDetailQuery(request.URL.RawQuery, request.URL.ForceQuery)
	if !ok {
		w.WriteHeader(400)
		return
	}
	if !pageRequest {
		w.Header().Set(replayDetailRemainingHeader, "30000")
		_, _ = io.WriteString(w, p.manifests[0])
	} else if page < len(p.pages) {
		_, _ = io.WriteString(w, p.pages[page])
	} else {
		w.WriteHeader(409)
	}
}

func replayDetailReaderAccept(t *testing.T, r *replayDetailReader, core ReplayDetailCore, now time.Time, remaining time.Duration) *replayDetailCandidate {
	t.Helper()
	gate := replayDetailTestGate(core, now)
	r.gate.Store(&gate)
	generation, err := ReplayDetailGeneration(core)
	if err != nil {
		t.Fatal(err)
	}
	c, reason := r.prepare(core, generation, gate, now, now.Add(remaining), now)
	if reason != "" {
		t.Fatal(reason)
	}
	if reason = r.commit(c, now); reason != "" {
		t.Fatal(reason)
	}
	return c
}

func replayDetailReaderWindow(count int, first uint64, at time.Time) ReplayDetailCore {
	c := replayDetailTestCore(count)
	c.CapturedAt = at.UTC()
	if count > 0 {
		c.FirstSequence, c.LastSequence, c.Evicted = first, first+uint64(count)-1, first-1
		for i := range c.Records {
			c.Records[i].Sequence = first + uint64(i)
			c.Records[i].CompletedIndex = c.Records[i].Sequence
			c.Records[i].Slot = c.Records[i].Sequence
		}
	}
	return c
}

func TestReplayDetailReaderConstructor(t *testing.T) {
	var gate atomic.Pointer[replayDetailGate]
	for _, client := range []*http.Client{nil, {}, {Transport: (*http.Transport)(nil)}} {
		if _, err := newReplayDetailReader(client, &gate); err == nil {
			t.Fatal("invalid client accepted")
		}
	}
	original := newCollector("/unused").client
	if _, err := newReplayDetailReader(original, nil); err == nil {
		t.Fatal("nil gate accepted")
	}
	r, err := newReplayDetailReader(original, &gate)
	if err != nil {
		t.Fatal(err)
	}
	defer r.client.CloseIdleConnections()
	transport := r.client.Transport.(*http.Transport)
	if transport == original.Transport || transport.Proxy != nil || transport.MaxResponseHeaderBytes != 4096 ||
		transport.MaxConnsPerHost != 2 || transport.MaxIdleConnsPerHost != 2 || transport.MaxIdleConns != 2 ||
		!transport.DisableKeepAlives || r.client.Timeout != time.Second || r.client.Jar != nil ||
		r.client.CheckRedirect(nil, nil) != http.ErrUseLastResponse || original.Transport.(*http.Transport).MaxConnsPerHost != 0 {
		t.Fatal("reader did not own the exact bounded transport")
	}
	if r.limit.Limit() != 16 || r.limit.Burst() != 32 || cap(r.active) != 4 {
		t.Fatal("public admission limits changed")
	}
}

func TestReplayDetailReaderRealFeedAllPages(t *testing.T) {
	for _, count := range []int{0, 256} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			core := replayDetailLiveCore(count)
			core.FoldCapacity = &ReplayFoldCapacity{Scope: "replay_fold_capacity", State: "active", Slot: math.MaxUint64, StartedAt: core.CapturedAt}
			if count > 0 {
				core.NearReception = maximumReplayNearReception(core.CapturedAt)
			}
			for i := range core.Records {
				core.Records[i].RetentionLimits = ReplayDetailRetentionLimits{Observed: true, RetainedBytesPauseAt: math.MaxUint64}
			}
			var captures atomic.Int64
			path := socketPath(t)
			feed, err := ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot {
				return replayDetailLiveStatus(core)
			}, func(at time.Time) (ReplayDetailCore, error) {
				captures.Add(1)
				copy := core
				copy.CapturedAt = at.UTC()
				return copy, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(feed.Close)
			client := newCollector(path)
			t.Cleanup(client.client.CloseIdleConnections)
			replayDetailWaitManifest(t, client.client, "http://node/details/replay")
			r := replayDetailReaderFixture(t, core, time.Now(), path)
			r.collect(context.Background())
			got := r.lookup("", false, time.Now())
			replayDetailAssertReply(t, got, 200, "")
			var manifest PublicReplayDetailManifest
			if json.Unmarshal([]byte(got.body), &manifest) != nil || manifest.Count != count || manifest.ReaderMissedRows != 0 || manifest.LastReaderGap != nil ||
				!reflect.DeepEqual(manifest.FoldCapacity, core.FoldCapacity) || !reflect.DeepEqual(manifest.NearReception, core.NearReception) {
				t.Fatal("public manifest changed source or invented coverage")
			}
			rows := make([]ReplayDetailRecord, 0, count)
			for i := range manifest.PageCount {
				query := "generation=" + manifest.Generation + "&page=" + strconv.Itoa(i)
				reply := r.lookup(query, false, time.Now())
				replayDetailAssertReply(t, reply, 200, "")
				var page PublicReplayDetailPage
				if json.Unmarshal([]byte(reply.body), &page) != nil || page.Page != i || !sameReplayDetailBinding(page.Binding, manifest.Binding) {
					t.Fatal("public page changed context")
				}
				rows = append(rows, page.Records...)
			}
			if !reflect.DeepEqual(rows, core.Records) {
				t.Fatal("public pages lost source fields")
			}
			beforeCursor, beforeFamily := r.cursor, r.items[0]
			r.collect(context.Background())
			afterFamily := r.items[0]
			if r.cursor != beforeCursor || afterFamily == nil || afterFamily.generation != beforeFamily.generation ||
				afterFamily.stamp != beforeFamily.stamp || !afterFamily.publishedAt.Equal(beforeFamily.publishedAt) ||
				!afterFamily.holdUntil.Equal(beforeFamily.holdUntil) || afterFamily.sourceFreshUntil.After(beforeFamily.sourceFreshUntil) ||
				afterFamily.manifests != beforeFamily.manifests || !reflect.DeepEqual(afterFamily.pages, beforeFamily.pages) {
				t.Fatal("exact token retry changed or renewed the accepted observation")
			}
			for range 20 {
				response := httptest.NewRecorder()
				r.serveHTTP(response, httptest.NewRequest("GET", "/api/v1/details/replay", nil), allowReplayStatusForTest)
				if response.Code != 200 {
					t.Fatal("cached public manifest unavailable")
				}
			}
			if captures.Load() != 1 {
				t.Fatal("public viewers caused another source capture")
			}
		})
	}
}

func TestReplayDetailReaderRequestStartClock(t *testing.T) {
	core := replayDetailReaderWindow(0, 0, time.Now())
	p := replayDetailTestPrepared(t, core)
	var sent atomic.Pointer[time.Time]
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		time.Sleep(80 * time.Millisecond)
		at := time.Now()
		sent.Store(&at)
		w.Header().Set(replayDetailRemainingHeader, "30000")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(80 * time.Millisecond)
		_, _ = io.WriteString(w, p.manifests[0])
	})
	r := replayDetailReaderFixture(t, core, time.Now(), path)
	before := time.Now()
	r.collect(context.Background())
	if r.items[0] == nil || sent.Load() == nil {
		t.Fatal("delayed real response was not accepted")
	}
	deadline := r.items[0].sourceFreshUntil
	if deadline.Before(before.Add(30*time.Second)) || !deadline.Before(sent.Load().Add(30*time.Second-40*time.Millisecond)) {
		t.Fatal("response delivery was added to the source freshness lease")
	}
	if !r.items[0].publishedAt.After(*sent.Load()) {
		t.Fatal("body transfer was not completed before publication")
	}
	for _, remaining := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond, 13*time.Second - time.Nanosecond, 13 * time.Second, 30 * time.Second} {
		now := time.Now()
		c := replayDetailReaderWindow(0, 0, now)
		reader := replayDetailReaderFixture(t, c, now, "/unused")
		g := *reader.gate.Load()
		generation, err := ReplayDetailGeneration(c)
		if err != nil {
			t.Fatal(err)
		}
		candidate, reason := reader.prepare(c, generation, g, now, now.Add(remaining), now)
		if remaining <= 0 {
			if reason == "" || candidate != nil {
				t.Fatal("nonpositive freshness accepted")
			}
			continue
		}
		if reason != "" || reader.commit(candidate, now) != "" || reader.cursor == nil {
			t.Fatal("positive verified remainder did not advance continuity")
		}
		if (reader.items[0] != nil) != (remaining >= replayDetailMinimum) {
			t.Fatal("unadvertisable positive remainder consumed a family holder")
		}
	}
}

func TestReplayDetailReaderRejectsBadTransfers(t *testing.T) {
	for _, mode := range []string{"malformed", "duplicate", "mixed", "tamper last", "oversize", "oversize header", "bad header", "duplicate header", "redirect", "unsupported", "reset"} {
		t.Run(mode, func(t *testing.T) {
			core := replayDetailReaderWindow(3, 1, time.Now())
			p := replayDetailTestPrepared(t, core)
			var corrupt atomic.Bool
			var reader *replayDetailReader
			path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
				if !corrupt.Load() {
					replayDetailReaderReply(w, request, p)
					return
				}
				_, page, isPage, _ := parseReplayDetailQuery(request.URL.RawQuery, false)
				if !isPage {
					switch mode {
					case "oversize header":
						w.Header().Set("Too-Large", strings.Repeat("x", 5000))
						replayDetailReaderReply(w, request, p)
						return
					case "oversize":
						w.Header().Set(replayDetailRemainingHeader, "30000")
						_, _ = io.WriteString(w, strings.Repeat(" ", MaxSnapshotBytes+1))
						return
					case "bad header", "duplicate header":
						w.Header().Add(replayDetailRemainingHeader, "030000")
						if mode == "duplicate header" {
							w.Header().Set(replayDetailRemainingHeader, "30000")
							w.Header().Add(replayDetailRemainingHeader, "30000")
						}
						_, _ = io.WriteString(w, p.manifests[0])
						return
					case "redirect":
						http.Redirect(w, request, "/snapshot", 302)
						return
					case "unsupported":
						w.WriteHeader(404)
						return
					}
					replayDetailReaderReply(w, request, p)
					return
				}
				switch mode {
				case "malformed":
					_, _ = io.WriteString(w, "private path /secret {")
				case "duplicate":
					_, _ = io.WriteString(w, strings.Replace(p.pages[page], `"family":"replay"`, `"family":"replay","family":"replay"`, 1))
				case "mixed":
					_, _ = io.WriteString(w, p.pages[1-page])
				case "tamper last":
					body := p.pages[page]
					if page == 1 {
						body = strings.Replace(body, `"transactions":"0"`, `"transactions":"1"`, 1)
					}
					_, _ = io.WriteString(w, body)
				case "reset":
					g := replayDetailTestGate(core, time.Now())
					g.Attempt++
					reader.gate.Store(&g)
					replayDetailReaderReply(w, request, p)
				default:
					replayDetailReaderReply(w, request, p)
				}
			})
			reader = replayDetailReaderFixture(t, core, time.Now(), path)
			reader.collect(context.Background())
			cursor, family := reader.cursor, reader.items[0]
			if cursor == nil || family == nil {
				t.Fatal("control collection failed")
			}
			corrupt.Store(true)
			reader.collect(context.Background())
			if reader.cursor != cursor || reader.items[0] != family || reader.failure == nil {
				t.Fatal("failed transfer changed or partly installed a collection")
			}
			reply := reader.lookup("", false, time.Now())
			if strings.Contains(reply.body, "/secret") {
				t.Fatal("raw source error escaped")
			}
			page := reader.lookup("generation="+family.generation+"&page=0", false, time.Now())
			if mode == "reset" {
				replayDetailAssertReply(t, page, 409, "binding_changed")
			} else {
				replayDetailAssertReply(t, page, 200, "")
				if !strings.Contains(reply.body, `"state":"stale"`) {
					t.Fatal("failed collection kept old measurements marked fresh")
				}
			}
		})
	}
}

func TestReplayDetailReaderContinuityAndOwnership(t *testing.T) {
	now := time.Now()
	empty := replayDetailReaderWindow(0, 0, now)
	r := replayDetailReaderFixture(t, empty, now, "/unused")
	replayDetailReaderAccept(t, r, empty, now, time.Second) // Cursor only.
	first := replayDetailReaderWindow(256, 355, now.Add(time.Second))
	accepted := replayDetailReaderAccept(t, r, first, now.Add(time.Second), 30*time.Second)
	if r.cursor.missed != 354 || *r.cursor.gap != (ReplayDetailGap{First: 1, Last: 354}) {
		t.Fatal("known empty cursor lost exact subsequent gap")
	}
	beforeCursor, beforeFamily := r.cursor, r.items[0]
	accepted.cursor.core.Records[0].Transactions++
	accepted.cursor.gap.First++
	accepted.public.manifests[0] = "corrupt"
	accepted.public.pages[0] = "corrupt"
	if r.cursor.core.Records[0].Transactions != 0 || r.cursor.gap.First != 1 || r.items[0].manifests[0] == "corrupt" || r.items[0].pages[0] == "corrupt" {
		t.Fatal("caller-owned candidate mutated installed data")
	}
	gate := replayDetailTestGate(first, now.Add(2*time.Second))
	r.gate.Store(&gate)
	generation, err := ReplayDetailGeneration(first)
	if err != nil {
		t.Fatal(err)
	}
	retry, reason := r.prepare(first, generation, gate, now.Add(2*time.Second), now.Add(32*time.Second), now.Add(2*time.Second))
	if reason != "" || r.commit(retry, now.Add(2*time.Second)) != "" || r.cursor != beforeCursor || r.items[0] != beforeFamily {
		t.Fatal("same-token retry renewed retained state")
	}
	for _, test := range []struct {
		name string
		core ReplayDetailCore
	}{
		{"regression", replayDetailReaderWindow(256, 354, now.Add(3*time.Second))},
		{"overlap changed", replayDetailReaderWindow(256, 400, now.Add(3*time.Second))},
	} {
		if test.name == "overlap changed" {
			test.core.Records[0].Timings.SlotReplay.Count++
		}
		g, err := ReplayDetailGeneration(test.core)
		if err != nil {
			t.Fatal(err)
		}
		if _, reason := r.prepare(test.core, g, gate, now.Add(3*time.Second), now.Add(33*time.Second), now.Add(3*time.Second)); reason != "source_invalid" {
			t.Fatalf("%s accepted: %s", test.name, reason)
		}
	}
	next := replayDetailReaderWindow(256, 400, now.Add(3*time.Second))
	replayDetailReaderAccept(t, r, next, now.Add(3*time.Second), time.Second)
	if r.cursor.missed != 354 {
		t.Fatal("overlapping accepted rows invented a gap")
	}
	gap := replayDetailReaderWindow(256, 700, now.Add(4*time.Second))
	replayDetailReaderAccept(t, r, gap, now.Add(4*time.Second), time.Second)
	if r.cursor.missed != 398 || *r.cursor.gap != (ReplayDetailGap{First: 656, Last: 699}) {
		t.Fatal("successive gap arithmetic changed")
	}
	reset := replayDetailReaderWindow(1, 1, now.Add(5*time.Second))
	reset.Binding.Attempt++
	reset.Binding.SourceGeneration++
	reset.Records[0].Attempt = reset.Binding.Attempt
	replayDetailReaderAccept(t, r, reset, now.Add(5*time.Second), time.Second)
	if r.cursor.missed != 0 || r.cursor.gap != nil || r.items[0] != nil {
		t.Fatal("new context inherited previous gaps or public families")
	}
	// Unknown startup does not invent loss for an already-truncated source ring.
	max := replayDetailReaderWindow(256, math.MaxUint64-255, now)
	r = replayDetailReaderFixture(t, max, now, "/unused")
	replayDetailReaderAccept(t, r, max, now, time.Second)
	if r.cursor.missed != 0 || r.cursor.gap != nil {
		t.Fatal("initial source eviction was counted as reader loss")
	}
	max.CapturedAt = now.Add(time.Second).UTC()
	replayDetailReaderAccept(t, r, max, now.Add(time.Second), time.Second)
	if r.cursor.core.LastSequence != math.MaxUint64 || r.cursor.missed != 0 {
		t.Fatal("MaxUint64 overlap wrapped")
	}
	r = replayDetailReaderFixture(t, empty, now, "/unused")
	replayDetailReaderAccept(t, r, empty, now, time.Second)
	replayDetailReaderAccept(t, r, max, now.Add(time.Second), time.Second)
	if r.cursor.missed != math.MaxUint64-256 || r.cursor.gap.Last != math.MaxUint64-256 {
		t.Fatal("known-empty MaxUint64 gap overflowed")
	}
}

func TestReplayDetailReaderHoldsAndCurrentStatus(t *testing.T) {
	now := time.Now()
	core := replayDetailReaderWindow(1, 1, now)
	r := replayDetailReaderFixture(t, core, now, "/unused")
	for i := range replayDetailReaderGenerations {
		at := now.Add(time.Duration(i) * time.Second)
		core.CapturedAt = at.UTC()
		replayDetailReaderAccept(t, r, core, at, 30*time.Second)
	}
	first := r.items[0]
	core.CapturedAt = now.Add(4 * time.Second).UTC()
	gate := replayDetailTestGate(core, now.Add(4*time.Second))
	r.gate.Store(&gate)
	generation, err := ReplayDetailGeneration(core)
	if err != nil {
		t.Fatal(err)
	}
	c, reason := r.prepare(core, generation, gate, now.Add(4*time.Second), now.Add(34*time.Second), now.Add(4*time.Second))
	if reason != "" || r.commit(c, now.Add(4*time.Second)) != "generation_not_ready" || r.items[0] != first {
		t.Fatal("fifth generation evicted an advertised unexpired holder")
	}
	query := "generation=" + first.generation + "&page=0"
	gate.ObservedAt = now.Add(29 * time.Second)
	r.gate.Store(&gate)
	replayDetailAssertReply(t, r.lookup(query, false, gate.ObservedAt), 200, "")
	gate.ObservedAt = now.Add(30 * time.Second)
	r.gate.Store(&gate)
	replayDetailAssertReply(t, r.lookup(query, false, gate.ObservedAt), 409, "source_generation_expired")
	// A failed collection never suppresses the current authoritative gate reason.
	r.failure = &replayDetailCaptureFailure{gate: gate, reason: "source_unavailable"}
	replayDetailAssertReply(t, r.lookup("", false, gate.ObservedAt.Add(StaleAfter+time.Nanosecond)), 503, "status_stale")
	gate.State, gate.Reason, gate.ObservedAt = "stale", "source_invalid", now.Add(3*time.Second)
	r.gate.Store(&gate)
	got := r.lookup("", false, gate.ObservedAt)
	replayDetailAssertReply(t, got, 200, "source_invalid")
	if !strings.Contains(got.body, `"state":"stale"`) {
		t.Fatal("stale status lost its meaning")
	}
	// A backwards reader clock fails rather than extending a prior lease.
	gate.State, gate.Reason, gate.ObservedAt = "fresh", "", now.Add(-time.Second)
	r.gate.Store(&gate)
	replayDetailAssertReply(t, r.lookup(query, false, gate.ObservedAt), 503, "source_time_invalid")
}

func TestReplayDetailReaderCurrentStatusWithoutFamily(t *testing.T) {
	for _, reason := range []string{"", "status_stale", "source_invalid", "source_unavailable"} {
		t.Run(reason, func(t *testing.T) {
			now := time.Now()
			core := replayDetailReaderWindow(0, 0, now)
			r := replayDetailReaderFixture(t, core, now, "/unused")
			gate := *r.gate.Load()
			r.failure = &replayDetailCaptureFailure{gate: gate, reason: "collection_deadline"}
			gate.State, gate.Reason, gate.ObservedAt = "stale", reason, now.Add(time.Second)
			r.gate.Store(&gate)
			if reason == "" {
				reason = "status_stale"
			}
			replayDetailAssertReply(t, r.lookup("", false, gate.ObservedAt), 503, reason)
			query := "generation=" + strings.Repeat("a", 64) + "&page=0"
			replayDetailAssertReply(t, r.lookup(query, false, gate.ObservedAt), 409, "source_generation_expired")
			gate.State, gate.Reason = "fresh", ""
			r.gate.Store(&gate)
			replayDetailAssertReply(t, r.lookup("", false, gate.ObservedAt), 503, "collection_deadline")
		})
	}
}

func TestReplayDetailReaderTwoWorkersAndCancellation(t *testing.T) {
	core := replayDetailReaderWindow(256, 1, time.Now())
	p := replayDetailTestPrepared(t, core)
	var active, peak, requests atomic.Int64
	entered := make(chan struct{}, 2)
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.URL.RawQuery == "" {
			replayDetailReaderReply(w, request, p)
			return
		}
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		entered <- struct{}{}
		<-request.Context().Done()
	})
	r := replayDetailReaderFixture(t, core, time.Now(), path)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.collect(ctx) }()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("two page workers did not start")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not join workers")
	}
	if peak.Load() != 2 || requests.Load() > 3 || r.cursor != nil || r.items[0] != nil {
		t.Fatal("worker bound, cancellation or no-partial-install failed")
	}
	deadline := time.Now().Add(time.Second)
	for active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if active.Load() != 0 {
		t.Fatal("cancelled source connections remained active")
	}
	// No retry queue or timer survives run cancellation.
	stopped, stop := context.WithCancel(context.Background())
	stop()
	r.run(stopped)
	select {
	case <-r.done:
	default:
		t.Fatal("run did not join on cancellation")
	}
}

type replayDetailBlockingWriter struct {
	header  http.Header
	entered chan struct{}
	release <-chan struct{}
}

func (w *replayDetailBlockingWriter) Header() http.Header { return w.header }
func (w *replayDetailBlockingWriter) WriteHeader(int)     {}
func (w *replayDetailBlockingWriter) Write(body []byte) (int, error) {
	w.entered <- struct{}{}
	<-w.release
	return len(body), nil
}

func TestReplayDetailReaderHTTPAdmission(t *testing.T) {
	now := time.Now()
	core := replayDetailReaderWindow(1, 1, now)
	r := replayDetailReaderFixture(t, core, now, "/unused")
	replayDetailReaderAccept(t, r, core, now, 30*time.Second)
	for _, path := range []string{"/api/v1/details/replay?", "/api/v1/details/replay?path=/secret", "/api/v1/details/%72eplay"} {
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest("GET", path, nil), allowReplayStatusForTest)
		if response.Code != 400 {
			t.Fatal("bad public route/query accepted")
		}
	}
	for _, request := range []*http.Request{
		httptest.NewRequest("POST", "/api/v1/details/replay", nil),
		httptest.NewRequest("GET", "/api/v1/details/replay", strings.NewReader("{}")),
	} {
		response := httptest.NewRecorder()
		r.serveHTTP(response, request, allowReplayStatusForTest)
		if response.Code != 400 {
			t.Fatal("bad public method/body accepted")
		}
	}
	query := "/api/v1/details/replay?generation=" + r.cursor.generation + "&page=0"
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var viewers sync.WaitGroup
	for range 4 {
		viewers.Go(func() {
			w := &replayDetailBlockingWriter{header: make(http.Header), entered: entered, release: release}
			r.serveHTTP(w, httptest.NewRequest("GET", query, nil), allowReplayStatusForTest)
		})
	}
	for range 4 {
		<-entered
	}
	response := httptest.NewRecorder()
	r.serveHTTP(response, httptest.NewRequest("GET", query, nil), allowReplayStatusForTest)
	if response.Code != 429 || !strings.Contains(response.Body.String(), `"reason":"rate_limited"`) {
		t.Fatal("fifth active page was not refused")
	}
	close(release)
	viewers.Wait()
	if len(r.active) != 0 {
		t.Fatal("active-page permit leaked")
	}
	// Deterministically drain the real limiter at a future clock; this test does
	// not add an interval override or change the production rate/burst settings.
	r.limit.AllowN(time.Now().Add(time.Hour), 32)
	response = httptest.NewRecorder()
	r.serveHTTP(response, httptest.NewRequest("GET", "/api/v1/details/replay", nil), allowReplayStatusForTest)
	if response.Code != 429 || response.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatal("rate refusal advertised a collection lease")
	}
}

func TestReplayDetailReaderTimeoutAndRunCadence(t *testing.T) {
	core := replayDetailReaderWindow(0, 0, time.Now())
	p := replayDetailTestPrepared(t, core)
	var stall atomic.Bool
	var requests atomic.Int64
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if stall.Load() {
			w.Header().Set(replayDetailRemainingHeader, "30000")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			<-request.Context().Done()
			return
		}
		replayDetailReaderReply(w, request, p)
	})
	r := replayDetailReaderFixture(t, core, time.Now(), path)
	ctx, cancel := context.WithCancel(context.Background())
	go r.run(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for r.lookup("", false, time.Now()).status != 200 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if r.lookup("", false, time.Now()).status != 200 {
		cancel()
		<-r.done
		t.Fatal("run did not collect immediately")
	}
	time.Sleep(30 * time.Millisecond)
	cancel()
	<-r.done
	if requests.Load() != 1 || replayDetailCaptureInterval != 10*time.Second {
		t.Fatal("run retried before the fixed post-completion cadence")
	}
	stall.Store(true)
	r = replayDetailReaderFixture(t, core, time.Now(), path)
	started := time.Now()
	r.collect(context.Background())
	if elapsed := time.Since(started); elapsed < 900*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("one-second body deadline not enforced: %s", elapsed)
	}
	if r.cursor != nil || r.items[0] != nil || r.failure == nil {
		t.Fatal("body timeout installed partial data")
	}
}

func TestReplayDetailReaderWholeCycleDeadline(t *testing.T) {
	core := replayDetailReaderWindow(256, 1, time.Now())
	p := replayDetailTestPrepared(t, core)
	var requests atomic.Int64
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.URL.RawQuery != "" {
			timer := time.NewTimer(230 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-request.Context().Done():
				return
			}
		}
		replayDetailReaderReply(w, request, p)
	})
	r := replayDetailReaderFixture(t, core, time.Now(), path)
	started := time.Now()
	r.collect(context.Background())
	elapsed := time.Since(started)
	if elapsed < replayDetailCollectionLimit-time.Second || elapsed > replayDetailCollectionLimit+2*time.Second || requests.Load() > 129 || r.cursor != nil ||
		r.failure == nil || r.failure.reason != "collection_deadline" {
		t.Fatalf("whole-cycle bound failed: elapsed=%s requests=%d failure=%+v", elapsed, requests.Load(), r.failure)
	}
}

func TestReplayDetailReaderClockCommitAndFullWidthBudget(t *testing.T) {
	now := time.Now()
	core := replayDetailReaderWindow(256, math.MaxUint64-255, now)
	maximumReplayOptionalMetadata(&core)
	core.Binding.RunID = strings.Repeat("<", 128)
	core.Binding.SourceGeneration, core.Binding.Attempt, core.Binding.Rewinds = math.MaxUint64, math.MaxUint64, math.MaxUint64
	genesis := "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG"
	core.Binding.GenesisHash = &genesis
	var fill func(reflect.Value)
	fill = func(v reflect.Value) {
		if v.Type() == reflect.TypeFor[time.Time]() {
			return
		}
		switch v.Kind() {
		case reflect.Struct:
			for i := range v.NumField() {
				fill(v.Field(i))
			}
		case reflect.Uint64:
			v.SetUint(math.MaxUint64)
		}
	}
	for i := range core.Records {
		fill(reflect.ValueOf(&core.Records[i]).Elem())
		core.Records[i].Sequence, core.Records[i].CompletedIndex = core.FirstSequence+uint64(i), core.FirstSequence+uint64(i)
		core.Records[i].Bankhash = genesis
		core.Records[i].RetentionLimits.Observed = true
	}
	r := replayDetailReaderFixture(t, core, now, "/unused")
	gate := *r.gate.Load()
	generation, err := ReplayDetailGeneration(core)
	if err != nil {
		t.Fatal(err)
	}
	c, reason := r.prepare(core, generation, gate, now.Add(-time.Second), now.Add(30*time.Second), now)
	if reason != "" || c.public == nil {
		t.Fatal("maximum source family did not prepare")
	}
	foldAt, foldDuration := *c.cursor.core.FoldCapacity.ReturnedAt, *c.cursor.core.FoldCapacity.DurationNanoseconds
	*core.FoldCapacity.ReturnedAt, *core.FoldCapacity.DurationNanoseconds = time.Time{}, 0
	if c.cursor.core.FoldCapacity.ReturnedAt.IsZero() || *c.cursor.core.FoldCapacity.DurationNanoseconds == 0 {
		t.Fatal("prepared cursor aliases source fold metadata")
	}
	if r.commit(c, now.Add(-time.Nanosecond)) != "source_time_invalid" || r.cursor != nil {
		t.Fatal("commit before validation finished was accepted")
	}
	if r.commit(c, now.Add(30*time.Second)) != "source_time_invalid" || r.cursor != nil {
		t.Fatal("expired candidate was installed")
	}
	if r.commit(c, now) != "" {
		t.Fatal("valid full-width candidate refused")
	}
	*c.cursor.core.FoldCapacity.ReturnedAt, *c.cursor.core.FoldCapacity.DurationNanoseconds = time.Time{}, 0
	if *r.cursor.core.FoldCapacity.ReturnedAt != foldAt || *r.cursor.core.FoldCapacity.DurationNanoseconds != foldDuration {
		t.Fatal("installed cursor aliases candidate fold metadata")
	}
	total := 0
	for _, body := range c.public.manifests {
		if len(body) > MaxSnapshotBytes {
			t.Fatal("public manifest exceeded 32 KiB")
		}
		total += len(body)
	}
	for _, body := range c.public.pages {
		if len(body) > MaxSnapshotBytes {
			t.Fatal("public page exceeded 32 KiB")
		}
		total += len(body)
	}
	if total > replayDetailFamilyBytes || len(c.public.pages) != 128 {
		t.Fatal("public family exceeded its bound or lost rows")
	}
	// The frozen family includes every exact-u64 loader/timing member.
	if !strings.Contains(c.public.pages[127], `"sysvar_cache_publication_epoch_rejects":"18446744073709551615"`) {
		t.Fatal("full-width final page lost a raw source member")
	}
	t.Logf("full-width public family bytes=%d pages=%d", total, len(c.public.pages))
}
