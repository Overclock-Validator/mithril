package dashboard

import (
	"context"
	"encoding/json"
	"errors"
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

	"golang.org/x/time/rate"
)

type detailCleanupTransport struct {
	done       <-chan struct{}
	closed     bool
	doneBefore bool
}

func TestDetailResponseHeaders(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		w := httptest.NewRecorder()
		writeDetailResponse(w, status, "body", time.Second, 50)
		if w.Code != status || w.Body.String() != "body" || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("response changed")
		}
		if got := w.Header().Get(statusRemainingHeader); (status == http.StatusOK && got != "1000") || (status != http.StatusOK && got != "") {
			t.Fatal("unexpected status lease", got)
		}
		if got := w.Header().Get(replayDetailRemainingHeader); (status == http.StatusOK && got != "50") || (status != http.StatusOK && got != "") {
			t.Fatal("unexpected source lease", got)
		}
	}
	for _, remaining := range []int64{0, -1} {
		w := httptest.NewRecorder()
		writeDetailResponse(w, http.StatusOK, "body", time.Second, remaining)
		if got := w.Header().Get(replayDetailRemainingHeader); got != "" {
			t.Fatal("unexpected nonpositive source lease", got)
		}
	}
}

func TestValidDetailGet(t *testing.T) {
	const path = "/api/v1/details/cache"
	for _, tc := range []struct {
		method, target, body string
		want                 bool
	}{
		{http.MethodGet, path, "", true},
		{http.MethodPost, path, "", false},
		{http.MethodGet, path + "?", "", false},
		{http.MethodGet, path + "?x=1", "", false},
		{http.MethodGet, "/api/v1/details/%63ache", "", false},
		{http.MethodGet, path, "x", false},
	} {
		req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
		if got := validDetailGet(req, path); got != tc.want {
			t.Fatalf("method=%s target=%s body=%q got=%v", tc.method, tc.target, tc.body, got)
		}
	}
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.TransferEncoding = []string{"chunked"}
	if validDetailGet(req, path) {
		t.Fatal("accepted transfer encoding")
	}
}

func (*detailCleanupTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected request")
}

func (t *detailCleanupTransport) CloseIdleConnections() {
	select {
	case <-t.done:
		t.doneBefore = true
	default:
	}
	t.closed = true
}

func TestDetailReaderCancellation(t *testing.T) {
	for _, alreadyCanceled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		if alreadyCanceled {
			cancel()
		}
		done := make(chan struct{})
		transport := &detailCleanupTransport{done: done}
		calls := 0
		runDetailReader(ctx, done, &http.Client{Transport: transport}, time.Hour, func(context.Context) {
			calls++
			cancel()
		})
		if want := 1; alreadyCanceled {
			if calls != 0 {
				t.Fatal("canceled reader collected")
			}
		} else if calls != want {
			t.Fatalf("collections: %d", calls)
		}
		if !transport.closed || transport.doneBefore {
			t.Fatal("connections were not closed before reader completion")
		}
		select {
		case <-done:
		default:
			t.Fatal("reader did not close")
		}
	}
}

func cacheDetailTestReader(t *testing.T, path string, snapshot Snapshot, now time.Time) *cacheDetailReader {
	t.Helper()
	var gate atomic.Pointer[cacheDetailGate]
	g := cacheGate(snapshot, now)
	g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	reader, err := newCacheDetailReader(newCollector(path).client, &gate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.client.CloseIdleConnections)
	return reader
}

func decodePublicCacheDetail(t *testing.T, reply cacheDetailReply) PublicCacheDetail {
	t.Helper()
	var d PublicCacheDetail
	if reply.status != http.StatusOK || len(reply.body) > MaxCacheDetailBytes || json.Unmarshal([]byte(reply.body), &d) != nil {
		t.Fatalf("public cache detail: status=%d body=%s", reply.status, reply.body)
	}
	return d
}

func TestDetailRemainingBounds(t *testing.T) {
	for _, tc := range []struct {
		value string
		limit time.Duration
		want  time.Duration
		valid bool
	}{
		{"1", 2 * time.Millisecond, time.Millisecond, true},
		{"2", 2 * time.Millisecond, 2 * time.Millisecond, true},
		{"3", 2 * time.Millisecond, 0, false},
		{"2", time.Millisecond, 0, false},
		{"0", time.Second, 0, false},
		{"-1", time.Second, 0, false},
		{"01", time.Second, 0, false},
		{"+1", time.Second, 0, false},
		{"9223372036854775808", time.Second, 0, false},
	} {
		got, err := parseDetailRemaining(tc.value, tc.limit)
		if (err == nil) != tc.valid || got != tc.want {
			t.Errorf("parseDetailRemaining(%q, %v) = %v, %v; want %v, valid=%t", tc.value, tc.limit, got, err, tc.want, tc.valid)
		}
	}
}

func TestCacheDetailReaderConstructor(t *testing.T) {
	var gate atomic.Pointer[cacheDetailGate]
	for _, client := range []*http.Client{nil, {}, {Transport: (*http.Transport)(nil)}, {Transport: &http.Transport{}}} {
		if _, err := newCacheDetailReader(client, &gate); err == nil {
			t.Fatal("invalid client accepted")
		}
	}
	original := newCollector("/unused").client
	if _, err := newCacheDetailReader(original, nil); err == nil {
		t.Fatal("nil gate accepted")
	}
	r, err := newCacheDetailReader(original, &gate)
	if err != nil {
		t.Fatal(err)
	}
	defer r.client.CloseIdleConnections()
	transport := r.client.Transport.(*http.Transport)
	if transport == original.Transport || transport.Proxy != nil || transport.MaxResponseHeaderBytes != 4096 ||
		transport.MaxConnsPerHost != 1 || transport.MaxIdleConnsPerHost != 1 || transport.MaxIdleConns != 1 ||
		!transport.DisableKeepAlives || r.client.Timeout != time.Second || r.client.Jar != nil ||
		r.client.CheckRedirect(nil, nil) != http.ErrUseLastResponse || original.Transport.(*http.Transport).MaxConnsPerHost != 0 ||
		r.limit.Limit() != 16 || r.limit.Burst() != 32 || cacheDetailCollectInterval != 10*time.Second {
		t.Fatal("reader did not own the exact bounded transport")
	}
}

func TestDetailReaderTransportLimits(t *testing.T) {
	original := newCollector("/unused").client
	var gate atomic.Pointer[localPeerGate]
	peer, err := newLocalPeerReader(original, &gate)
	if err != nil {
		t.Fatal(err)
	}
	machine, err := newMachineDetailReader(original)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		client      *http.Client
		connections int
		timeout     time.Duration
	}{{peer.client, 1, 3 * time.Second}, {machine.client, 2, time.Second}} {
		defer tc.client.CloseIdleConnections()
		transport := tc.client.Transport.(*http.Transport)
		if transport == original.Transport || transport.Proxy != nil || transport.MaxResponseHeaderBytes != 4096 ||
			transport.MaxConnsPerHost != tc.connections || transport.MaxIdleConnsPerHost != tc.connections ||
			transport.MaxIdleConns != tc.connections || !transport.DisableKeepAlives ||
			tc.client.Timeout != tc.timeout || tc.client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
			t.Fatal("reader did not retain its independent transport limits")
		}
	}
}

func TestCacheDetailStatusStampRecovery(t *testing.T) {
	snapshot, capture := cacheDetailFixture(time.Now())
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		t.Fatal(err)
	}
	var captures atomic.Int64
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/details/cache" {
			http.NotFound(w, request)
			return
		}
		captures.Add(1)
		w.Header().Set(replayDetailRemainingHeader, "30000")
		_, _ = w.Write(body)
	})
	r := cacheDetailTestReader(t, path, snapshot, time.Now())
	r.collect(context.Background())
	original := r.latest.Load()
	if original == nil || !original.stamp.valid() || r.lookup(time.Now()).status != http.StatusOK {
		t.Fatal("initial capture was not published under its gate")
	}
	initialDeadline, initialPublished := original.sourceFreshUntil, original.publishedAt
	rebind := func(epoch uint64, run string) {
		gate := cloneCacheDetailGate(*r.gate.Load())
		gate.stamp = statusStamp{nodeEpoch: epoch, familyEpoch: epoch}
		gate.binding.RunID = run
		gate.observedAt = time.Now()
		r.gate.Store(&gate)
	}
	// A recovered gate alone must not re-stamp a retained capture.
	rebind(2, snapshot.RunID)
	if reply := r.lookup(time.Now()); reply.status != http.StatusServiceUnavailable || r.latest.Load() != original {
		t.Fatal("same-binding recovery exposed an old-epoch capture before a fetch")
	}
	r.collect(context.Background())
	recovered := r.latest.Load()
	if recovered == original || recovered.stamp != (statusStamp{2, 2}) ||
		recovered.sourceFreshUntil.After(initialDeadline) || !recovered.publishedAt.Equal(initialPublished) ||
		recovered.bodies != original.bodies || r.lookup(time.Now()).status != http.StatusOK {
		t.Fatal("a validated identical fetch lost its original facts or renewed its lease")
	}
	// The same A bytes cannot be exposed after A -> B -> A until fetched again.
	rebind(3, "replacement-run")
	if r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("replacement gate accepted the old owner")
	}
	rebind(4, snapshot.RunID)
	if r.lookup(time.Now()).status != http.StatusServiceUnavailable || captures.Load() != 2 {
		t.Fatal("A to B to A revived the old family without a new fetch")
	}
	r.collect(context.Background())
	returned := r.latest.Load()
	if captures.Load() != 3 || returned.stamp != (statusStamp{4, 4}) ||
		returned.sourceFreshUntil.After(initialDeadline) || !returned.publishedAt.Equal(initialPublished) ||
		r.lookup(time.Now()).status != http.StatusOK {
		t.Fatal("A to B to A did not recover from a validated fetch under the original deadline")
	}
	// The same body cannot revive an already expired capture after a gap.
	expired := *returned
	expired.sourceFreshUntil = time.Now().Add(-time.Second)
	r.latest.Store(&expired)
	rebind(5, snapshot.RunID)
	r.collect(context.Background())
	if r.latest.Load() != &expired || r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("re-fetching an expired digest revived its old capture")
	}
}

func TestCacheDetailInFlightStampABAFailsClosed(t *testing.T) {
	snapshot, capture := cacheDetailFixture(time.Now())
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		close(started)
		<-release
		w.Header().Set(replayDetailRemainingHeader, "30000")
		_, _ = w.Write(body)
	})
	r := cacheDetailTestReader(t, path, snapshot, time.Now())
	done := make(chan struct{})
	go func() { r.collect(context.Background()); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("source fetch did not start")
	}
	gate := cloneCacheDetailGate(*r.gate.Load())
	gate.binding.RunID, gate.stamp = "replacement-run", statusStamp{2, 2}
	r.gate.Store(&gate)
	gate = cloneCacheDetailGate(gate)
	gate.binding.RunID, gate.stamp = snapshot.RunID, statusStamp{3, 3}
	r.gate.Store(&gate)
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cache collector did not finish")
	}
	if r.latest.Load() != nil || r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("an in-flight A capture survived an A to B to A gate change")
	}
}

func TestCacheDetailFinalAdmission(t *testing.T) {
	setup := func(t *testing.T) (*collector, *cacheDetailReader) {
		t.Helper()
		now := time.Now()
		snapshot, capture := cacheDetailFixture(now)
		family, err := preparePublicCacheDetail(capture.Detail, now, now.Add(30*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		family.stamp = statusStamp{1, 1}
		c := &collector{clockOrigin: now}
		gate := cacheGate(snapshot, now)
		gate.stamp = family.stamp
		c.cacheGate.Store(&gate)
		r := &cacheDetailReader{gate: &c.cacheGate, limit: rate.NewLimiter(100, 100)}
		r.latest.Store(family)
		frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
		frame.fence.nodeEpoch, frame.fence.families[statusCache] = 1, 1
		c.latest.Store(frame)
		return c, r
	}
	request := func(r *cacheDetailReader, admit func(cacheDetailReply) (time.Duration, bool)) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/cache", nil), admit)
		return response
	}
	t.Run("independent leases", func(t *testing.T) {
		c, r := setup(t)
		response := request(r, func(reply cacheDetailReply) (time.Duration, bool) { return c.admitCacheDetail(r, reply) })
		detail, detailErr := parseCacheDetailRemaining(response.Header().Get(replayDetailRemainingHeader))
		statusMS, statusErr := strconv.ParseInt(response.Header().Get(statusRemainingHeader), 10, 64)
		if response.Code != http.StatusOK || detailErr != nil || statusErr != nil ||
			statusMS <= 0 || detail <= time.Duration(statusMS)*time.Millisecond {
			t.Fatal("detail and status leases were not independently retained")
		}
		if noGate := request(r, nil); noGate.Code != http.StatusServiceUnavailable {
			t.Fatal("missing final admission exposed a cached family")
		}
	})
	t.Run("failure after selection", func(t *testing.T) {
		c, r := setup(t)
		response := request(r, func(reply cacheDetailReply) (time.Duration, bool) {
			gate := *r.gate.Load()
			r.failure.Store(&cacheDetailReaderFailure{gate: gate, reason: "source_unavailable"})
			return c.admitCacheDetail(r, reply)
		})
		if response.Code != http.StatusServiceUnavailable || response.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatal("a source failure after selection exposed the previous fresh result")
		}
		stale := request(r, func(reply cacheDetailReply) (time.Duration, bool) { return c.admitCacheDetail(r, reply) })
		if stale.Code != http.StatusOK || !strings.Contains(stale.Body.String(), `"state":"stale"`) ||
			stale.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatal("an unchanged failure did not retain explicitly stale values")
		}
	})
	t.Run("family replaced after selection", func(t *testing.T) {
		c, r := setup(t)
		response := request(r, func(reply cacheDetailReply) (time.Duration, bool) {
			family := *reply.family
			family.sourceFreshUntil = time.Now().Add(time.Second)
			r.latest.Store(&family)
			return c.admitCacheDetail(r, reply)
		})
		if response.Code != http.StatusServiceUnavailable {
			t.Fatal("replaced family exposed its earlier deadline")
		}
	})
	t.Run("detail expires after selection", func(t *testing.T) {
		c, r := setup(t)
		response := request(r, func(reply cacheDetailReply) (time.Duration, bool) {
			// This callback runs after lookup in the only test goroutine. Move
			// the fixture deadline into the past to model expiry at admission.
			reply.family.sourceFreshUntil = time.Now().Add(-time.Second)
			return c.admitCacheDetail(r, reply)
		})
		if response.Code != http.StatusServiceUnavailable || response.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatal("expired selected detail was still advertised as fresh")
		}
	})
	t.Run("gate changes after selection", func(t *testing.T) {
		c, r := setup(t)
		response := request(r, func(reply cacheDetailReply) (time.Duration, bool) {
			gate := cloneCacheDetailGate(*r.gate.Load())
			gate.stamp = statusStamp{2, 2}
			r.gate.Store(&gate)
			return c.admitCacheDetail(r, reply)
		})
		if response.Code != http.StatusServiceUnavailable {
			t.Fatal("changed gate exposed the old selected family")
		}
	})
	t.Run("parent status expired", func(t *testing.T) {
		c, r := setup(t)
		frame := *c.latest.Load()
		frame.freshUntil = time.Now().Add(-time.Second)
		c.latest.Store(&frame)
		response := request(r, func(reply cacheDetailReply) (time.Duration, bool) { return c.admitCacheDetail(r, reply) })
		if response.Code != http.StatusServiceUnavailable || response.Header().Get(statusRemainingHeader) != "" {
			t.Fatal("expired parent status admitted a fresh detail")
		}
	})
}

func TestCacheDetailReaderRealFeedAndCachedViewer(t *testing.T) {
	snapshot, capture := cacheDetailFixture(time.Now())
	capture.Detail.Binding.CacheGeneration = math.MaxUint64 - 1
	capture.Detail.Program.EstimatedEntries = math.MaxUint64
	lookup := CacheLookupDetail{Hits: 12, Misses: 3}
	delta := CacheLookupDelta{Hits: 7, Misses: 1}
	capture.Detail.Activity = &CacheActivityDetail{Instance: 1, Vote: lookup, Program: lookup, CommonLarge: lookup,
		Window: &CacheActivityWindow{StartedAt: capture.Detail.CapturedAt.Add(-time.Second), EndedAt: capture.Detail.CapturedAt,
			DurationNanoseconds: uint64(time.Second), Vote: delta, Program: delta, CommonLarge: delta}}
	capture.Detail.Activity.InsertionWeights = cacheInsertionWeightsFixture(0, math.MaxUint64)
	capture.Detail.Activity.ProgramSets = cacheProgramSetsFixture(math.MaxUint64 - 1)
	refused := uint64(7)
	capture.Detail.Activity.ProgramRefusals = &CacheProgramSets{Scope: "oversized_program_cache_set_refusals", Count: &refused}
	var samples atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{
		Cache: func(time.Time) (CacheDetailCapture, error) {
			samples.Add(1)
			return capture, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	r := cacheDetailTestReader(t, path, snapshot, time.Now())
	r.collect(context.Background())
	reply := r.lookup(time.Now())
	got := decodePublicCacheDetail(t, reply)
	if got.State != "fresh" || got.Reason != "" || got.CollectorAt.IsZero() || !reflect.DeepEqual(got.Binding, capture.Detail.Binding) ||
		!got.CapturedAt.Equal(capture.Detail.CapturedAt) || got.Vote != capture.Detail.Vote || got.Program != capture.Detail.Program || !reflect.DeepEqual(got.Common, capture.Detail.Common) || !reflect.DeepEqual(got.Activity, capture.Detail.Activity) ||
		reply.remainingMilliseconds < 1 || reply.remainingMilliseconds > 30000 || r.latest.Load().sourceFreshUntil.After(capture.FreshUntil) {
		t.Fatalf("public document changed source values or clocks: %+v", got)
	}
	t.Logf("synthetic actual Feed public fixture: %s", reply.body)
	feed.Close()
	before := samples.Load()
	admit := func(cacheDetailReply) (time.Duration, bool) { return time.Second, true }
	for range 20 {
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/cache", nil), admit)
		remaining, err := parseCacheDetailRemaining(response.Header().Get(replayDetailRemainingHeader))
		if response.Code != 200 || response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Content-Type") != "application/json" || err != nil || remaining <= 0 {
			t.Fatal("cached public response unavailable")
		}
	}
	if samples.Load() != before {
		t.Fatal("public viewers invoked the source sampler")
	}
	for _, request := range []struct{ method, path, body string }{
		{"POST", "/api/v1/details/cache", ""}, {"GET", "/api/v1/details/cache", "x"},
		{"GET", "/api/v1/details/cache?", ""}, {"GET", "/api/v1/details/cache?generation=1", ""},
		{"GET", "/api/v1/details/%63ache", ""}, {"GET", "/api/v1/details/cache/", ""},
	} {
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(request.method, request.path, strings.NewReader(request.body)), admit)
		if response.Code != 400 || response.Body.String() != cacheDetailFailure("invalid_request").body || response.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatalf("strict public request accepted: %s, status=%d", request.path, response.Code)
		}
	}
	r.limit = rate.NewLimiter(0, 0)
	w := httptest.NewRecorder()
	r.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/details/cache", nil), admit)
	if w.Code != 429 || w.Body.String() != cacheDetailFailure("rate_limited").body || w.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatal("public rate limit failed open")
	}
}

func TestCacheDetailPublicCapacityAndDetachedValues(t *testing.T) {
	_, capture := cacheDetailFixture(time.Now())
	d := capture.Detail
	d.Binding.RunID = strings.Repeat("<", 128)
	genesis := "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4S6DYF4wW"
	d.Binding.GenesisHash, d.Binding.CacheGeneration = &genesis, math.MaxUint64-1
	at := time.Date(9999, 12, 31, 23, 59, 59, 123456789, time.UTC)
	d.Binding.NodeStartedAt, d.CapturedAt = at.Add(-time.Minute), at
	d.Vote = CacheVoteDetail{CapacityEntries: math.MaxUint64, EstimatedEntries: math.MaxUint64}
	d.Program = CacheProgramDetail{CapacityCostUnits: math.MaxUint64, CostUnitBytes: 1 << 20, EstimatedEntries: math.MaxUint64}
	d.Common = CacheCommonDetail{Layout: "fixed", ConfiguredBudgetBytes: math.MaxUint64,
		FixedEmptyCapacityEntries: math.MaxUint64, FixedSmallCapacityEntries: math.MaxUint64,
		LargeCapacityBytes: math.MaxUint64 - 1, LargeEstimatedEntries: math.MaxUint64}
	occupied := uint64(math.MaxUint64)
	d.Common.FixedEmptyOccupiedEntries, d.Common.FixedSmallOccupiedEntries = &occupied, &occupied
	lookup := CacheLookupDetail{Hits: math.MaxUint64, Misses: math.MaxUint64, Evictions: math.MaxUint64, EvictionWeight: math.MaxUint64}
	delta := CacheLookupDelta{Hits: math.MaxUint64, Misses: math.MaxUint64}
	d.Activity = &CacheActivityDetail{Instance: math.MaxUint64 - 1, Vote: lookup, Program: lookup, CommonLarge: lookup,
		Fixed: &CacheFixedActivityDetail{EmptyHits: math.MaxUint64, SmallHits: math.MaxUint64, EmptyReplacements: math.MaxUint64, SmallReplacements: math.MaxUint64},
		Window: &CacheActivityWindow{StartedAt: at.Add(-time.Minute), EndedAt: at, DurationNanoseconds: uint64(time.Minute),
			Vote: delta, Program: delta, CommonLarge: delta}}
	d.Activity.InsertionWeights = cacheInsertionWeightsFixture(math.MaxUint64, math.MaxUint64)
	d.Activity.ProgramSets = cacheProgramSetsFixture(math.MaxUint64 - 1)
	for _, layout := range []string{"fixed", "flat"} {
		d.Common.Layout = layout
		if layout == "flat" {
			d.Activity.Fixed = &CacheFixedActivityDetail{}
			occupied = 0
			d.Common.FixedEmptyCapacityEntries, d.Common.FixedSmallCapacityEntries = 0, 0
			d.Common.LargeCapacityBytes = d.Common.ConfiguredBudgetBytes
		}
		family, err := preparePublicCacheDetail(d, at, at.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		total, largest := 0, 0
		for i, body := range family.bodies {
			if len(body) > MaxCacheDetailBytes {
				t.Fatal("public cache variant exceeded the document cap")
			}
			public := decodePublicCacheDetail(t, cacheDetailReply{status: 200, body: body})
			if public.Vote != d.Vote || public.Program != d.Program || !reflect.DeepEqual(public.Common, d.Common) || !reflect.DeepEqual(public.Binding, d.Binding) || !reflect.DeepEqual(public.Activity, d.Activity) ||
				!public.CapturedAt.Equal(d.CapturedAt) || !public.CollectorAt.Equal(at) || public.Reason != cacheDetailPublicReasons[i] ||
				(i == 0 && public.State != "fresh") || (i != 0 && public.State != "stale") {
				t.Fatal("pre-encoded variant changed source values")
			}
			total, largest = total+len(body), max(largest, len(body))
		}
		if total > cacheDetailFamilyBytes {
			t.Fatal("retained public variants exceeded the family cap")
		}
		t.Logf("maximum-width %s: largest=%d/%d, all variants=%d/%d", layout, largest, MaxCacheDetailBytes, total, cacheDetailFamilyBytes)
		t.Logf("synthetic maximum-width %s public fixture: %s", layout, family.bodies[0])
		original := family.bodies
		genesis = "mutated-after-encoding"
		if *family.binding.GenesisHash == genesis || family.bodies != original {
			t.Fatal("publication retained caller-owned values")
		}
		genesis = "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4S6DYF4wW"
	}
}

func TestCacheDetailReaderFailureNoRenewalAndContext(t *testing.T) {
	now := time.Now()
	snapshot, capture := cacheDetailFixture(now)
	capture.Detail.Activity = &CacheActivityDetail{Instance: 1,
		ProgramSets: &CacheProgramSets{Scope: "accepted_program_cache_set_calls", Reason: "counter_saturated"}}
	type sourceReply struct {
		status    int
		body      string
		remaining string
		change    func()
	}
	var current atomic.Pointer[sourceReply]
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/details/cache" || request.URL.RawQuery != "" {
			t.Error("reader selected an unexpected source path")
		}
		reply := current.Load()
		if reply.change != nil {
			reply.change()
		}
		if reply.remaining != "" {
			w.Header().Set(replayDetailRemainingHeader, reply.remaining)
		}
		w.WriteHeader(reply.status)
		_, _ = io.WriteString(w, reply.body)
	})
	body, _ := json.Marshal(capture.Detail)
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "30000"})
	r := cacheDetailTestReader(t, path, snapshot, now)
	r.collect(context.Background())
	first := r.latest.Load()
	if first == nil {
		t.Fatal("control source did not install")
	}
	deadline, collected := first.sourceFreshUntil, first.publishedAt
	failure := cacheDetailFailure("source_unavailable")
	current.Store(&sourceReply{status: failure.status, body: failure.body})
	r.collect(context.Background())
	stale := decodePublicCacheDetail(t, r.lookup(time.Now()))
	if r.latest.Load() != first || stale.State != "stale" || stale.Reason != "source_unavailable" || stale.Vote != capture.Detail.Vote || stale.CapturedAt != capture.Detail.CapturedAt || !reflect.DeepEqual(stale.Activity, capture.Detail.Activity) {
		t.Fatal("failed poll replaced or redated retained cache values")
	}
	current.Store(&sourceReply{status: 404, body: "not found"})
	r.collect(context.Background())
	unsupported := r.lookup(time.Now())
	stale = decodePublicCacheDetail(t, unsupported)
	if r.latest.Load() != first || stale.State != "stale" || stale.Reason != "unsupported_source" || stale.CapturedAt != capture.Detail.CapturedAt || unsupported.remainingMilliseconds != 0 {
		t.Fatal("old producer did not retain explicitly stale dated values")
	}
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "30000"})
	r.collect(context.Background())
	if r.latest.Load() != first || r.failure.Load() != nil || r.latest.Load().sourceFreshUntil != deadline || r.latest.Load().publishedAt != collected {
		t.Fatal("unchanged source after failure renewed its clocks")
	}
	// Even a repeated valid body with a claimed new lease cannot revive expiry.
	expired := *first
	expired.sourceFreshUntil = time.Now().Add(-time.Second)
	r.latest.Store(&expired)
	r.collect(context.Background())
	if r.latest.Load() != &expired || decodePublicCacheDetail(t, r.lookup(time.Now())).State != "stale" {
		t.Fatal("an expired digest was renewed")
	}
	capture.Detail.Binding.CacheGeneration++
	capture.Detail.Vote.EstimatedEntries = 17
	capture.Detail.CapturedAt = time.Now().UTC()
	body, _ = json.Marshal(capture.Detail)
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "30000"})
	r.collect(context.Background())
	replaced := decodePublicCacheDetail(t, r.lookup(time.Now()))
	if replaced.State != "fresh" || replaced.Binding.CacheGeneration != 2 || replaced.Vote.EstimatedEntries != 17 {
		t.Fatal("same-owner cache generation was not replaced")
	}
	for _, mutate := range []func(*cacheDetailGate){
		func(g *cacheDetailGate) { g.binding.RunID = "another-run" },
		func(g *cacheDetailGate) { g.binding.NodeStartedAt = g.binding.NodeStartedAt.Add(time.Second) },
		func(g *cacheDetailGate) {
			genesis := "11111111111111111111111111111111"
			g.binding.GenesisHash = &genesis
		},
	} {
		g := cacheGate(snapshot, time.Now())
		g.stamp = r.gate.Load().stamp
		mutate(&g)
		r.gate.Store(&g)
		reply := r.lookup(time.Now())
		if reply.status != 503 || reply.body != cacheDetailFailure("binding_changed").body || strings.Contains(reply.body, "capacity_entries") {
			t.Fatal("new owner binding inherited retained cache values")
		}
	}
	g := cacheGate(snapshot, time.Now())
	g.stamp = r.gate.Load().stamp
	g.reason = "source_unavailable"
	r.gate.Store(&g)
	if reply := r.lookup(time.Now()); reply.status != 503 || reply.body != cacheDetailFailure(g.reason).body {
		t.Fatal("current status failure did not suppress retained gauges")
	}
	g.reason = ""
	r.gate.Store(&g)
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "30000", change: func() {
		next := cloneCacheDetailGate(*r.gate.Load())
		next.binding.RunID = "mid-transfer-replacement"
		r.gate.Store(&next)
	}})
	before := r.latest.Load()
	r.collect(context.Background())
	if r.latest.Load() != before || r.failure.Load() == nil || r.failure.Load().reason != "binding_changed" {
		t.Fatal("context replacement during transfer partially installed data")
	}
}

func TestCacheDetailReaderDeadlineAndCancellation(t *testing.T) {
	snapshot, capture := cacheDetailFixture(time.Now())
	body, _ := json.Marshal(capture.Detail)
	var headerAt atomic.Pointer[time.Time]
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(80 * time.Millisecond)
		at := time.Now()
		headerAt.Store(&at)
		w.Header().Set(replayDetailRemainingHeader, "1000")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(80 * time.Millisecond)
		_, _ = w.Write(body)
	})
	r := cacheDetailTestReader(t, path, snapshot, time.Now())
	started := time.Now()
	r.collect(context.Background())
	family := r.latest.Load()
	if family == nil || headerAt.Load() == nil || family.sourceFreshUntil.Before(started.Add(time.Second)) ||
		!family.sourceFreshUntil.Before(headerAt.Load().Add(950*time.Millisecond)) || !family.publishedAt.After(*headerAt.Load()) {
		t.Fatal("header or body latency extended freshness")
	}
	reply := r.lookup(family.sourceFreshUntil)
	expired := decodePublicCacheDetail(t, reply)
	if expired.State != "stale" || expired.Reason != "status_stale" || reply.remainingMilliseconds != 0 {
		t.Fatal("exact source deadline remained fresh")
	}
	for _, value := range []string{"", "0", "-1", "+1", "030000", "30001", "1.0", "1 "} {
		if _, err := parseCacheDetailRemaining(value); err == nil {
			t.Fatalf("invalid remaining time accepted: %q", value)
		}
	}
	for _, value := range []string{"1", "30000"} {
		if _, err := parseCacheDetailRemaining(value); err != nil {
			t.Fatal(err)
		}
	}
	latePath := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(replayDetailRemainingHeader, "1")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write(body)
	})
	late := cacheDetailTestReader(t, latePath, snapshot, time.Now())
	late.collect(context.Background())
	if late.latest.Load() != nil || late.failure.Load() == nil || late.failure.Load().reason != "source_time_invalid" {
		t.Fatal("body that outlived its original lease was installed")
	}
	entered := make(chan struct{})
	var once sync.Once
	var requests atomic.Int64
	blockedPath := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		w.Header().Set(replayDetailRemainingHeader, "30000")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		once.Do(func() { close(entered) })
		<-request.Context().Done()
	})
	blocked := cacheDetailTestReader(t, blockedPath, snapshot, time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	go blocked.run(ctx)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocked source was not reached")
	}
	cancel()
	select {
	case <-blocked.done:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not join the cache reader")
	}
	if blocked.latest.Load() != nil || requests.Load() != 1 {
		t.Fatal("cancellation installed or retried a partial capture")
	}
}

func TestCacheDetailReaderRejectsBadTransfers(t *testing.T) {
	snapshot, capture := cacheDetailFixture(time.Now())
	valid, _ := json.Marshal(capture.Detail)
	failure := cacheDetailFailure("source_unavailable")
	for _, mode := range []string{"duplicate", "unknown", "missing", "oversize", "error oversize", "missing header", "bad header", "duplicate header", "redirect", "wrong status", "error header", "raw error", "404"} {
		t.Run(mode, func(t *testing.T) {
			path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
				body, status := valid, 200
				w.Header().Set(replayDetailRemainingHeader, "30000")
				switch mode {
				case "duplicate":
					body = []byte(strings.Replace(string(valid), `"family":"accounts_cache"`, `"family":"accounts_cache","family":"accounts_cache"`, 1))
				case "unknown":
					body = []byte(strings.Replace(string(valid), `"family":"accounts_cache"`, `"family":"accounts_cache","unknown":0`, 1))
				case "missing":
					body = []byte(strings.Replace(string(valid), `"reason":"",`, "", 1))
				case "oversize":
					body = []byte(strings.Repeat(" ", MaxCacheDetailBytes+1))
				case "error oversize":
					status, body = 503, []byte(strings.Repeat(" ", 513))
					w.Header().Del(replayDetailRemainingHeader)
				case "missing header":
					w.Header().Del(replayDetailRemainingHeader)
				case "bad header":
					w.Header().Set(replayDetailRemainingHeader, "030000")
				case "duplicate header":
					w.Header().Add(replayDetailRemainingHeader, "30000")
				case "redirect":
					http.Redirect(w, request, "/snapshot", http.StatusFound)
					return
				case "wrong status":
					status, body = 500, []byte(failure.body)
					w.Header().Del(replayDetailRemainingHeader)
				case "error header":
					status, body = 503, []byte(failure.body)
				case "raw error":
					status, body = 503, []byte(strings.Replace(failure.body, "source_unavailable", "private-path-secret", 1))
					w.Header().Del(replayDetailRemainingHeader)
				case "404":
					status, body = 404, []byte("not found")
					w.Header().Del(replayDetailRemainingHeader)
				}
				w.WriteHeader(status)
				_, _ = w.Write(body)
			})
			r := cacheDetailTestReader(t, path, snapshot, time.Now())
			r.collect(context.Background())
			if r.latest.Load() != nil || r.failure.Load() == nil {
				t.Fatal("bad transfer installed a partial capture")
			}
			reply := r.lookup(time.Now())
			if reply.status != 503 || len(reply.body) > 512 || strings.Contains(reply.body, "private-path") || strings.Contains(reply.body, "/snapshot") {
				t.Fatal("bad transfer exposed raw input")
			}
			if mode == "404" && !strings.Contains(reply.body, `"reason":"unsupported_source"`) {
				t.Fatal("old producer was not identified as unsupported")
			}
		})
	}
	for _, reason := range []string{"invalid_request", "rate_limited", "waiting_source", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "binding_changed", "response_limit", "unsupported_source", "private-secret"} {
		reply := cacheDetailFailure(reason)
		wantStatus := 503
		if reason == "invalid_request" {
			wantStatus = 400
		} else if reason == "rate_limited" {
			wantStatus = 429
		}
		if reply.status != wantStatus || len(reply.body) > 512 || !json.Valid([]byte(reply.body)) || strings.Contains(reply.body, "private-secret") {
			t.Fatalf("unbounded or incorrect failure: %+v", reply)
		}
	}
	// The reader-only 404 reason must not widen the private sampler vocabulary.
	if decodeCacheDetailFailure([]byte(cacheDetailFailure("unsupported_source").body)) != "source_invalid" {
		t.Fatal("public-only reason accepted from the private sampler")
	}
}

func TestCacheDetailCollectorStatusGate(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public, c.publicOptions = true, PublicOptions{Resources: true, CacheDetails: true}
	t.Cleanup(c.client.CloseIdleConnections)
	s, capture := cacheDetailFixture(time.Now())
	genesis := "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4S6DYF4wW"
	s.GenesisHash, capture.Detail.Binding.GenesisHash = &genesis, &genesis
	setFixture(t, body, s)
	c.poll(context.Background(), time.Now().UTC())
	g := c.cacheGate.Load()
	if g == nil || g.failure(time.Now()) != "" || !g.matches(capture.Detail.Binding) || g.observedAt == g.observedAt.Round(0) {
		t.Fatalf("accepted status did not publish detached monotonic context: %+v", g)
	}
	*c.last.GenesisHash = "mutated-after-publication"
	if *g.binding.GenesisHash != genesis {
		t.Fatal("status gate retained a caller-owned genesis pointer")
	}
	setFixture(t, body, s)
	c.poll(context.Background(), time.Now().UTC())
	body.Store(nil)
	c.poll(context.Background(), time.Now().UTC())
	g = c.cacheGate.Load()
	if g.reason != "source_unavailable" || !g.matches(capture.Detail.Binding) {
		t.Fatal("source loss did not retain only dated gate context")
	}
	bad := []byte("{")
	body.Store(&bad)
	c.poll(context.Background(), time.Now().UTC())
	if c.cacheGate.Load().reason != "source_invalid" {
		t.Fatal("malformed status did not block the cache gate")
	}
	s.RunID, s.GenesisHash = "new-run", nil
	setFixture(t, body, s)
	c.poll(context.Background(), time.Now().UTC())
	g = c.cacheGate.Load()
	if g.reason != "" || g.binding.RunID != "new-run" || g.binding.GenesisHash != nil || g.matches(capture.Detail.Binding) {
		t.Fatal("new status run inherited old cache context")
	}
	// The status gate also dominates an earlier cache-poll failure with no family.
	r, err := newCacheDetailReader(c.client, &c.cacheGate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.client.CloseIdleConnections)
	r.failure.Store(&cacheDetailReaderFailure{gate: cloneCacheDetailGate(*g), reason: "source_unavailable"})
	stale := cloneCacheDetailGate(*g)
	stale.reason = "source_invalid"
	c.cacheGate.Store(&stale)
	if reply := r.lookup(time.Now()); reply.status != 503 || reply.body != cacheDetailFailure("source_invalid").body {
		t.Fatal("earlier poll failure hid the current status reason")
	}
	disabled := newCollector(path)
	t.Cleanup(disabled.client.CloseIdleConnections)
	disabled.poll(context.Background(), time.Now().UTC())
	if disabled.cacheGate.Load() != nil {
		t.Fatal("default collector enabled cache gate publication")
	}
}

func TestCacheDetailPublicOptInAndRealRoute(t *testing.T) {
	for _, tc := range []struct {
		origin string
		opts   PublicOptions
	}{
		{"", PublicOptions{CacheDetails: true}},
		{"", PublicOptions{Resources: true, CacheDetails: true}},
		{"https://stats.example", PublicOptions{CacheDetails: true}},
	} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if err := serveProfile(context.Background(), "/unused.sock", listener, tc.origin, tc.opts); err == nil || !strings.Contains(err.Error(), "resource disclosure") {
			t.Fatalf("cache disclosure accepted: origin=%q options=%+v error=%v", tc.origin, tc.opts, err)
		}
	}
	for _, mode := range []string{"verifying", "validator"} {
		t.Run(mode, func(t *testing.T) {
			snapshot, capture := cacheDetailFixture(time.Now())
			snapshot.Mode = mode
			path := socketPath(t)
			feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{
				Cache: func(time.Time) (CacheDetailCapture, error) { return capture, nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(feed.Close)
			for _, enabled := range []bool{false, true} {
				address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, CacheDetails: enabled})
				func() {
					defer replayDetailStopPublic(t, cancel, done)
					code, features, _ := replayDetailPublicRequest(t, address, "/api/v1/features?include=advanced_status", "stats.example", "")
					want := `{"schema_version":1,"history":false,"cluster":false}`
					if enabled {
						want = `{"schema_version":1,"history":false,"cluster":false,"cache_details":true}`
					}
					if code != 200 || string(features) != want {
						t.Fatalf("cache feature changed defaults: %d %s", code, features)
					}
					if !enabled {
						if code, _, _ := replayDetailPublicRequest(t, address, "/api/v1/details/cache", "stats.example", ""); code != 404 {
							t.Fatal("disabled public route was registered")
						}
						return
					}
					deadline := time.Now().Add(3 * time.Second)
					for {
						code, body, h := replayDetailPublicRequest(t, address, "/api/v1/details/cache", "stats.example", "")
						if code == 200 {
							public := decodePublicCacheDetail(t, cacheDetailReply{status: code, body: string(body)})
							if public.Vote != capture.Detail.Vote || public.Program != capture.Detail.Program || !reflect.DeepEqual(public.Common, capture.Detail.Common) ||
								!reflect.DeepEqual(public.Binding, capture.Detail.Binding) || public.CapturedAt != capture.Detail.CapturedAt {
								t.Fatal("public route changed source fields")
							}
							if _, err := parseCacheDetailRemaining(h.Get(replayDetailRemainingHeader)); err != nil || h.Get("X-Content-Type-Options") != "nosniff" {
								t.Fatal("public route lost its freshness or disclosure headers")
							}
							break
						}
						if time.Now().After(deadline) {
							t.Fatalf("public cache reader did not publish: %d %s", code, body)
						}
						time.Sleep(10 * time.Millisecond)
					}
					code, ordinary, _ := replayDetailPublicRequest(t, address, "/api/v1/status", "stats.example", "")
					if code != 200 || strings.Contains(string(ordinary), "cache_generation") || len(ordinary) > MaxSnapshotBytes {
						t.Fatal("cache option changed the ordinary status contract")
					}
					for _, request := range []struct {
						path, host, origin string
						want               int
					}{
						{"/api/v1/details/cache", "attacker.example", "", 403},
						{"/api/v1/details/cache", "stats.example", "https://attacker.example", 403},
						{"/api/v1/details/cache?", "stats.example", "", 400},
						{"/api/v1/details/cache?generation=1", "stats.example", "", 400},
						{"/api/v1/details/%63ache", "stats.example", "", 400},
						{"/details/cache", "stats.example", "", 404},
					} {
						code, _, _ := replayDetailPublicRequest(t, address, request.path, request.host, request.origin)
						if code != request.want {
							t.Fatalf("%s: got %d, want %d", request.path, code, request.want)
						}
					}
				}()
			}
		})
	}
}

func TestCacheDetailPublicStatusFirstAndViewerIndependence(t *testing.T) {
	snapshot, capture := cacheDetailFixture(time.Now())
	var first, cachePolls atomic.Int64
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/snapshot":
			first.CompareAndSwap(0, 1)
			s := snapshot
			s.SampledAt = time.Now().UTC()
			body, _ := json.Marshal(s)
			_, _ = w.Write(body)
		case "/details/cache":
			first.CompareAndSwap(0, 2)
			cachePolls.Add(1)
			body, _ := json.Marshal(capture.Detail)
			w.Header().Set(replayDetailRemainingHeader, "30000")
			_, _ = w.Write(body)
		default:
			t.Error("reader selected an unexpected source route")
		}
	})
	started := time.Now()
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, CacheDetails: true})
	defer replayDetailStopPublic(t, cancel, done)
	deadline := time.Now().Add(3 * time.Second)
	for {
		code, _, _ := replayDetailPublicRequest(t, address, "/api/v1/details/cache", "stats.example", "")
		if code == 200 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("public reader never reached cache source")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for range 20 {
		if code, _, _ := replayDetailPublicRequest(t, address, "/api/v1/details/cache", "stats.example", ""); code != 200 {
			t.Fatal("cached viewer response failed")
		}
	}
	if first.Load() != 1 || cachePolls.Load() > 1+int64(time.Since(started)/cacheDetailCollectInterval) {
		t.Fatal("details ran before status or viewers triggered source work")
	}
}
