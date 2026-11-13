package dashboard

import (
	"context"
	"encoding/json"
	"errors"
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

func storageDetailTestReader(t *testing.T, path string, snapshot Snapshot) *storageDetailReader {
	t.Helper()
	var gate atomic.Pointer[storageDetailGate]
	g := storageGate(snapshot, time.Now())
	g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	reader, err := newStorageDetailReader(newCollector(path).client, &gate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.client.CloseIdleConnections)
	return reader
}

func TestStorageDetailShortenedLease(t *testing.T) {
	t.Run("source", func(t *testing.T) {
		now := time.Now()
		snapshot, capture := storageSourceFixture(now)
		s := &storageDetailSource{sample: func(time.Time) (StorageDetailCapture, error) { return capture, nil }}
		s.update(snapshot, now)
		if p := s.latest.Load(); p == nil || p.reason != "" {
			t.Fatal("initial storage capture was not accepted")
		}
		capture.FreshUntil = now.Add(10 * time.Second)
		s.update(snapshot, time.Now())
		if p := s.latest.Load(); p.reason != "" || !p.freshUntil.Equal(capture.FreshUntil) {
			t.Fatal("unchanged storage capture ignored a shortened source deadline")
		}
		shortened := capture.FreshUntil
		capture.FreshUntil = now.Add(20 * time.Second)
		s.update(snapshot, time.Now())
		if p := s.latest.Load(); p.reason != "" || !p.freshUntil.Equal(shortened) {
			t.Fatal("later observation renewed a shortened storage deadline")
		}
	})
	t.Run("reader", func(t *testing.T) {
		snapshot, capture := storageSourceFixture(time.Now())
		body, _ := json.Marshal(capture.Detail)
		var remaining atomic.Int64
		remaining.Store(20000)
		path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining.Load(), 10))
			_, _ = w.Write(body)
		})
		r := storageDetailTestReader(t, path, snapshot)
		r.collect(context.Background())
		first := r.latest.Load()
		if first == nil {
			t.Fatal("initial storage capture was not accepted")
		}
		remaining.Store(10000)
		r.collect(context.Background())
		if next := r.latest.Load(); next == nil || next == first || !next.sourceFreshUntil.Before(first.sourceFreshUntil) || !next.publishedAt.Equal(first.publishedAt) {
			t.Fatal("unchanged storage capture ignored a shorter lease or changed its collection time")
		}
		shortened := r.latest.Load()
		remaining.Store(20000)
		r.collect(context.Background())
		if r.latest.Load() != shortened {
			t.Fatal("later observation renewed a shortened storage lease")
		}
	})
}

func TestStorageDetailStatusStampRecovery(t *testing.T) {
	snapshot, capture := storageSourceFixture(time.Now())
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		t.Fatal(err)
	}
	var captures atomic.Int64
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		captures.Add(1)
		w.Header().Set(replayDetailRemainingHeader, "20000")
		_, _ = w.Write(body)
	})
	r := storageDetailTestReader(t, path, snapshot)
	r.collect(context.Background())
	original := r.latest.Load()
	if original == nil || !original.stamp.valid() || r.lookup(time.Now()).status != http.StatusOK {
		t.Fatal("initial storage capture was not published")
	}
	initialDeadline, initialPublished := original.sourceFreshUntil, original.publishedAt
	setGate := func(epoch uint64, s Snapshot) {
		gate := storageGate(s, time.Now())
		gate.stamp = statusStamp{epoch, epoch}
		r.gate.Store(&gate)
	}
	setGate(2, snapshot)
	if r.lookup(time.Now()).status != http.StatusServiceUnavailable || r.latest.Load() != original {
		t.Fatal("storage recovery revived an old family without a new fetch")
	}
	r.collect(context.Background())
	recovered := r.latest.Load()
	if recovered == original || recovered.stamp != (statusStamp{2, 2}) || recovered.sourceFreshUntil.After(initialDeadline) ||
		!recovered.publishedAt.Equal(initialPublished) || r.highwaterGen != capture.Detail.Binding.DBGeneration {
		t.Fatal("validated storage recovery renewed capture clocks or generation")
	}
	replacement := snapshot
	replacement.RunID = "replacement-run"
	setGate(3, replacement)
	if r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("replacement node accepted the previous storage family")
	}
	setGate(4, snapshot)
	if r.lookup(time.Now()).status != http.StatusServiceUnavailable || captures.Load() != 2 {
		t.Fatal("A to B to A revived storage without a new fetch")
	}
	r.collect(context.Background())
	returned := r.latest.Load()
	if captures.Load() != 3 || returned.stamp != (statusStamp{4, 4}) || returned.sourceFreshUntil.After(initialDeadline) ||
		!returned.publishedAt.Equal(initialPublished) || r.lookup(time.Now()).status != http.StatusOK {
		t.Fatal("validated storage re-fetch did not retain original clocks")
	}
	expired := *returned
	expired.sourceFreshUntil = time.Now().Add(-time.Second)
	r.latest.Store(&expired)
	r.lastCapture = &expired
	setGate(5, snapshot)
	r.collect(context.Background())
	if r.latest.Load() != &expired || r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("re-fetching an expired storage capture revived it")
	}
}

func TestStorageDetailInFlightStampABAFailsClosed(t *testing.T) {
	snapshot, capture := storageSourceFixture(time.Now())
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.Header().Set(replayDetailRemainingHeader, "20000")
		_, _ = w.Write(body)
	})
	r := storageDetailTestReader(t, path, snapshot)
	done := make(chan struct{})
	go func() { r.collect(context.Background()); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("storage fetch did not start")
	}
	gate := *r.gate.Load()
	gate.runID, gate.stamp = "replacement-run", statusStamp{2, 2}
	r.gate.Store(&gate)
	gate = *r.gate.Load()
	gate.runID, gate.stamp = snapshot.RunID, statusStamp{3, 3}
	r.gate.Store(&gate)
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("storage collector did not finish")
	}
	if r.latest.Load() != nil || r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("in-flight storage survived an A to B to A gate change")
	}
}

func TestStorageDetailFinalAdmission(t *testing.T) {
	setup := func(t *testing.T) (*collector, *storageDetailReader) {
		t.Helper()
		now := time.Now()
		snapshot, capture := storageSourceFixture(now)
		family, err := preparePublicStorageDetail(capture.Detail, now, now.Add(20*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		family.stamp = statusStamp{1, 1}
		c := &collector{clockOrigin: now}
		gate := storageGate(snapshot, now)
		gate.stamp = family.stamp
		c.storageGate.Store(&gate)
		r := &storageDetailReader{gate: &c.storageGate, limit: rate.NewLimiter(100, 100)}
		r.latest.Store(family)
		frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
		frame.fence.nodeEpoch, frame.fence.families[statusStorage] = 1, 1
		c.latest.Store(frame)
		return c, r
	}
	request := func(r *storageDetailReader, admit func(storageDetailReply) (time.Duration, bool)) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/storage", nil), admit)
		return response
	}
	t.Run("independent leases", func(t *testing.T) {
		c, r := setup(t)
		response := request(r, func(reply storageDetailReply) (time.Duration, bool) { return c.admitStorageDetail(r, reply) })
		detail, detailErr := parseStorageDetailRemaining(response.Header().Get(replayDetailRemainingHeader))
		statusMS, statusErr := strconv.ParseInt(response.Header().Get(statusRemainingHeader), 10, 64)
		if response.Code != http.StatusOK || detailErr != nil || statusErr != nil || statusMS <= 0 || detail <= time.Duration(statusMS)*time.Millisecond {
			t.Fatal("storage and status leases were not independently retained")
		}
		if noGate := request(r, nil); noGate.Code != http.StatusServiceUnavailable {
			t.Fatal("missing final admission exposed storage")
		}
	})
	t.Run("failure after selection", func(t *testing.T) {
		c, r := setup(t)
		response := request(r, func(reply storageDetailReply) (time.Duration, bool) {
			gate := *r.gate.Load()
			r.failure.Store(&storageDetailReaderFailure{gate: gate, reason: "source_unavailable"})
			return c.admitStorageDetail(r, reply)
		})
		if response.Code != http.StatusServiceUnavailable || response.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatal("source failure after selection exposed fresh storage")
		}
		stale := request(r, func(reply storageDetailReply) (time.Duration, bool) { return c.admitStorageDetail(r, reply) })
		if stale.Code != http.StatusOK || !strings.Contains(stale.Body.String(), `"state":"stale"`) || stale.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatal("unchanged failure did not retain explicitly stale storage")
		}
	})
	for _, name := range []string{"family replaced", "detail expired", "gate changed", "parent status expired"} {
		t.Run(name, func(t *testing.T) {
			c, r := setup(t)
			response := request(r, func(reply storageDetailReply) (time.Duration, bool) {
				switch name {
				case "family replaced":
					family := *reply.family
					family.sourceFreshUntil = time.Now().Add(time.Second)
					r.latest.Store(&family)
				case "detail expired":
					reply.family.sourceFreshUntil = time.Now().Add(-time.Second)
				case "gate changed":
					gate := *r.gate.Load()
					gate.stamp = statusStamp{2, 2}
					r.gate.Store(&gate)
				case "parent status expired":
					frame := *c.latest.Load()
					frame.freshUntil = time.Now().Add(-time.Second)
					c.latest.Store(&frame)
				}
				return c.admitStorageDetail(r, reply)
			})
			if response.Code != http.StatusServiceUnavailable || response.Header().Get(statusRemainingHeader) != "" ||
				response.Header().Get(replayDetailRemainingHeader) != "" {
				t.Fatal("changed or expired storage admission remained fresh")
			}
		})
	}
}

func TestStorageDetailTransportOwnershipAndExactExpiry(t *testing.T) {
	var gate atomic.Pointer[storageDetailGate]
	for _, client := range []*http.Client{nil, {}, {Transport: (*http.Transport)(nil)}, {Transport: &http.Transport{}}} {
		if _, err := newStorageDetailReader(client, &gate); err == nil {
			t.Fatal("invalid transport accepted")
		}
	}
	original := newCollector("/unused").client
	if _, err := newStorageDetailReader(original, nil); err == nil {
		t.Fatal("nil gate accepted")
	}
	r, err := newStorageDetailReader(original, &gate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.client.CloseIdleConnections)
	transport := r.client.Transport.(*http.Transport)
	if transport == original.Transport || transport.Proxy != nil || transport.MaxResponseHeaderBytes != 4096 ||
		transport.MaxConnsPerHost != 1 || transport.MaxIdleConnsPerHost != 1 || transport.MaxIdleConns != 1 || !transport.DisableKeepAlives ||
		r.client.Timeout != time.Second || r.client.Jar != nil || r.client.CheckRedirect(nil, nil) != http.ErrUseLastResponse ||
		r.limit.Limit() != 16 || r.limit.Burst() != 32 || storageDetailCollectInterval != 10*time.Second {
		t.Fatal("reader does not own its bounded fixed transport")
	}
	now := time.Now()
	snapshot, capture := storageSourceFixture(now)
	g := storageGate(snapshot, now)
	g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	family, err := preparePublicStorageDetail(capture.Detail, now, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	family.stamp = g.stamp
	r.latest.Store(family)
	var got PublicStorageDetail
	before, expired := r.lookup(now), r.lookup(family.sourceFreshUntil)
	if before.remainingMilliseconds != 1000 || json.Unmarshal([]byte(expired.body), &got) != nil ||
		got.State != "stale" || got.Reason != "status_stale" || expired.remainingMilliseconds != 0 {
		t.Fatal("exact original deadline remained fresh")
	}
	r.limit = rate.NewLimiter(0, 1)
	r.limit.Allow()
	w := httptest.NewRecorder()
	r.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/details/storage", nil), nil)
	if w.Code != 429 || w.Header().Get(replayDetailRemainingHeader) != "" || len(w.Body.Bytes()) > 512 {
		t.Fatal("rate limit leaked a fresh lease or unbounded response")
	}
	source := &storageDetailSource{sample: func(time.Time) (StorageDetailCapture, error) { return capture, nil }}
	source.update(snapshot, now)
	p := *source.latest.Load()
	p.freshUntil = time.Now().Add(-time.Second)
	source.latest.Store(&p)
	if response := readStorageSource(source, "/details/storage"); response.Code != 503 || response.Header().Get(replayDetailRemainingHeader) != "" || !strings.Contains(response.Body.String(), `"reason":"status_stale"`) {
		t.Fatal("expired private publication remained fresh")
	}
}

func TestStorageDetailBindingFailuresRetainGeneration(t *testing.T) {
	for _, kind := range []string{"unknown_genesis", "other_node_failure", "other_node_mismatch"} {
		t.Run(kind, func(t *testing.T) {
			snapshot, capture := storageSourceFixture(time.Now())
			capture.Detail.Binding.DBGeneration = 2
			source := &storageDetailSource{sample: func(time.Time) (StorageDetailCapture, error) { return capture, nil }}
			source.update(snapshot, time.Now())
			if source.latest.Load().reason != "" || source.highwaterGen != 2 {
				t.Fatal("control generation not installed")
			}
			uncertain := snapshot
			if kind == "unknown_genesis" {
				uncertain.GenesisHash = nil
			} else {
				uncertain.RunID = "unverified-other-node"
				if kind == "other_node_failure" {
					source.sample = func(time.Time) (StorageDetailCapture, error) {
						return StorageDetailCapture{}, errors.New("source_unavailable")
					}
				}
			}
			source.update(uncertain, time.Now())
			if source.latest.Load().reason == "" {
				t.Fatal("uncertain node was accepted")
			}
			capture.Detail.Binding.DBGeneration = 1
			capture.Detail.CapturedAt = time.Now().UTC()
			source.sample = func(time.Time) (StorageDetailCapture, error) { return capture, nil }
			source.update(snapshot, time.Now())
			if p := source.latest.Load(); p.reason != "binding_changed" || p.body != "" || source.highwaterGen != 2 {
				t.Fatal("source failure erased the accepted generation")
			}
		})
	}
}

func TestStorageDetailReaderGenerationAndContext(t *testing.T) {
	snapshot, capture := storageSourceFixture(time.Now())
	capture.Detail.Binding.DBGeneration = 2
	var body atomic.Pointer[[]byte]
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/details/storage" || request.URL.RawQuery != "" {
			t.Error("reader selected an unexpected storage path")
		}
		w.Header().Set(replayDetailRemainingHeader, "20000")
		_, _ = w.Write(*body.Load())
	})
	setBody := func() {
		b, err := json.Marshal(capture.Detail)
		if err != nil {
			t.Fatal(err)
		}
		body.Store(&b)
	}
	setBody()
	r := storageDetailTestReader(t, path, snapshot)
	r.collect(context.Background())
	accepted := r.latest.Load()
	if accepted == nil || r.highwaterGen != 2 {
		t.Fatal("control generation not installed")
	}
	empty := storageDetailGate{observedAt: time.Now(), reason: "source_unavailable"}
	r.gate.Store(&empty)
	r.collect(context.Background())
	r.latest.Store(nil)
	fresh := storageGate(snapshot, time.Now())
	fresh.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 2}
	r.gate.Store(&fresh)
	capture.Detail.Binding.DBGeneration = 1
	capture.Detail.CapturedAt = time.Now().UTC()
	setBody()
	r.collect(context.Background())
	if r.latest.Load() != nil || r.highwaterGen != 2 || r.failure.Load() == nil || r.failure.Load().reason != "binding_changed" {
		t.Fatal("status loss erased the reader generation fence")
	}
	capture.Detail.Binding.DBGeneration = 2
	capture.Detail.Binding.DBOpenedAt = capture.Detail.Binding.DBOpenedAt.Add(time.Second)
	setBody()
	r.collect(context.Background())
	if r.failure.Load() == nil || r.failure.Load().reason != "binding_changed" {
		t.Fatal("same generation changed its open date")
	}
	capture.Detail.Binding.DBOpenedAt = accepted.binding.DBOpenedAt
	setBody()
	r.collect(context.Background())
	if r.latest.Load() == nil || r.failure.Load() != nil {
		t.Fatal("valid recovery refused")
	}
	next := snapshot
	next.RunID, next.StartedAt = "next-storage-node", snapshot.StartedAt.Add(time.Minute)
	nextGate := storageGate(next, time.Now())
	nextGate.stamp = statusStamp{nodeEpoch: 2, familyEpoch: 3}
	r.gate.Store(&nextGate)
	invalid := []byte("{")
	body.Store(&invalid)
	r.collect(context.Background())
	if r.highwaterGen != 2 {
		t.Fatal("invalid new node reset generation")
	}
	capture.Detail.Binding.RunID, capture.Detail.Binding.NodeStartedAt = next.RunID, next.StartedAt
	capture.Detail.Binding.DBGeneration = 1
	capture.Detail.CapturedAt = time.Now().UTC()
	setBody()
	r.collect(context.Background())
	if r.highwaterGen != 1 || !sameStorageNode(r.highwaterNode, nextGate) || r.failure.Load() != nil {
		t.Fatal("valid replacement node not adopted")
	}
}

func TestStorageDetailReaderTransferBoundsAndExpiry(t *testing.T) {
	snapshot, capture := storageSourceFixture(time.Now())
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"duplicate", "oversize", "missing_header", "duplicate_header", "bad_header", "error_header", "raw_error", "unsupported", "expired_body", "future"} {
		t.Run(mode, func(t *testing.T) {
			path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
				value, status := body, 200
				w.Header().Set(replayDetailRemainingHeader, "20000")
				switch mode {
				case "duplicate":
					value = []byte(strings.Replace(string(body), `"family":"storage"`, `"family":"storage","family":"storage"`, 1))
				case "oversize":
					value = []byte(strings.Repeat(" ", MaxStorageDetailBytes+1))
				case "missing_header":
					w.Header().Del(replayDetailRemainingHeader)
				case "duplicate_header":
					w.Header().Add(replayDetailRemainingHeader, "20000")
				case "bad_header":
					w.Header().Set(replayDetailRemainingHeader, "030000")
				case "error_header":
					status, value = 503, []byte(storageDetailFailure("source_unavailable").body)
				case "raw_error":
					status, value = 503, []byte(strings.Replace(storageDetailFailure("source_unavailable").body, "source_unavailable", "private-secret", 1))
					w.Header().Del(replayDetailRemainingHeader)
				case "unsupported":
					status, value = 404, []byte("not found")
					w.Header().Del(replayDetailRemainingHeader)
				case "expired_body":
					w.Header().Set(replayDetailRemainingHeader, "1")
				case "future":
					future := capture.Detail
					future.CapturedAt = time.Now().Add(2 * time.Second).UTC()
					value, _ = json.Marshal(future)
				}
				w.WriteHeader(status)
				if mode == "expired_body" {
					w.(http.Flusher).Flush()
					time.Sleep(10 * time.Millisecond)
				}
				_, _ = w.Write(value)
			})
			r := storageDetailTestReader(t, path, snapshot)
			r.collect(context.Background())
			reply := r.lookup(time.Now())
			if r.latest.Load() != nil || r.failure.Load() == nil || reply.status != 503 || len(reply.body) > 512 || strings.Contains(reply.body, "private-secret") {
				t.Fatal("invalid transfer installed data or leaked raw input")
			}
			want := "source_invalid"
			if mode == "oversize" {
				want = "response_limit"
			}
			if mode == "unsupported" {
				want = "unsupported_source"
			}
			if mode == "expired_body" || mode == "future" {
				want = "source_time_invalid"
			}
			if r.failure.Load().reason != want {
				t.Fatalf("got %q want %q", r.failure.Load().reason, want)
			}
		})
	}
	for _, value := range []string{"", "0", "-1", "+1", "030000", "30001", "1.0", "1 "} {
		if _, err := parseStorageDetailRemaining(value); err == nil {
			t.Fatalf("invalid remaining lease %q", value)
		}
	}
	for _, value := range []string{"1", "30000"} {
		if _, err := parseStorageDetailRemaining(value); err != nil {
			t.Fatal(err)
		}
	}
	entered := make(chan struct{})
	var once sync.Once
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set(replayDetailRemainingHeader, "30000")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		once.Do(func() { close(entered) })
		<-request.Context().Done()
	})
	r := storageDetailTestReader(t, path, snapshot)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.run(ctx)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reader never reached source")
	}
	cancel()
	select {
	case <-r.done:
	case <-time.After(time.Second):
		t.Fatal("reader did not join on cancellation")
	}
	if r.latest.Load() != nil {
		t.Fatal("cancelled transfer published partial data")
	}
	if decodeStorageDetailFailure([]byte(storageDetailFailure("unsupported_source").body)) != "source_invalid" {
		t.Fatal("public-only failure accepted from node")
	}
}

func storageSourceFixture(now time.Time) (Snapshot, StorageDetailCapture) {
	d := storageDetailFixture()
	d.Binding.RunID = "storage-source-run"
	d.Binding.NodeStartedAt, d.Binding.DBOpenedAt = now.Add(-time.Hour).UTC(), now.Add(-time.Minute).UTC()
	d.CapturedAt = now.UTC()
	s := testSnapshot(now.UTC())
	genesis := d.Binding.GenesisHash
	s.RunID, s.StartedAt, s.GenesisHash = d.Binding.RunID, d.Binding.NodeStartedAt, &genesis
	s.Mode = "verifying"
	return s, StorageDetailCapture{Detail: d, FreshUntil: now.Add(20 * time.Second)}
}

func readStorageSource(s *storageDetailSource, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func TestStorageDetailSourceCaptureAndContextFences(t *testing.T) {
	now := time.Now()
	snapshot, capture := storageSourceFixture(now)
	var calls int
	source := &storageDetailSource{sample: func(time.Time) (StorageDetailCapture, error) { calls++; return capture, nil }}
	source.update(snapshot, now)
	w := readStorageSource(source, "/details/storage")
	if w.Code != 200 || w.Header().Get(replayDetailRemainingHeader) == "" {
		t.Fatalf("source: %d %s", w.Code, w.Body.String())
	}
	original := w.Body.String()
	deadline := source.latest.Load().freshUntil
	for range 20 {
		if got := readStorageSource(source, "/details/storage"); got.Body.String() != original {
			t.Fatal("HTTP changed cached body")
		}
	}
	if calls != 1 {
		t.Fatal("HTTP invoked the source sampler")
	}
	capture.FreshUntil = time.Now().Add(25 * time.Second)
	source.update(snapshot, time.Now())
	if source.latest.Load().freshUntil != deadline {
		t.Fatal("same capture renewed its deadline")
	}
	source.sample = func(time.Time) (StorageDetailCapture, error) {
		return StorageDetailCapture{}, errors.New("private/path")
	}
	source.update(snapshot, time.Now())
	if got := readStorageSource(source, "/details/storage"); got.Code != 503 || strings.Contains(got.Body.String(), "private") {
		t.Fatal("source error leaked")
	}
	source.sample = func(time.Time) (StorageDetailCapture, error) { return capture, nil }
	source.update(snapshot, time.Now())
	if source.latest.Load().freshUntil != deadline {
		t.Fatal("failure renewed unchanged capture")
	}
	capture.Detail.Index.State = "active"
	start := now.Add(-time.Second).UTC()
	capture.Detail.Index.InitialReason, capture.Detail.Index.StartedAt = "memtable", &start
	source.update(snapshot, time.Now())
	if p := source.latest.Load(); p.reason != "source_invalid" || p.body != "" {
		t.Fatal("same-capture content mutation accepted")
	}
	capture.Detail.CapturedAt = time.Now().UTC()
	capture.Detail.Binding.DBGeneration = 2
	source.update(snapshot, time.Now())
	if source.latest.Load().reason != "" {
		t.Fatal("valid later binding refused")
	}
	old := capture
	capture.Detail.Binding.DBGeneration = 1
	capture.Detail.CapturedAt = time.Now().UTC()
	missing := snapshot
	missing.GenesisHash = nil
	source.update(missing, time.Now())
	source.update(snapshot, time.Now())
	if source.latest.Load().reason != "binding_changed" {
		t.Fatal("binding loss cleared the generation fence")
	}
	capture = old
	for _, path := range []string{"/details/storage?", "/details/storage?x=1", "/details/%73torage"} {
		if got := readStorageSource(source, path); got.Code != 400 {
			t.Fatalf("bad path %q: %d", path, got.Code)
		}
	}
}

func TestStorageDetailPublicTypedFamilyBudget(t *testing.T) {
	at := sizingTime(time.Now())
	d := maximumStorageDetail(at)
	d.IO = maximumStorageIO(at)
	family, err := preparePublicStorageDetail(d, at, at.Add(20*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	total, largest := 0, 0
	for i, body := range family.bodies {
		total += len(body)
		if len(body) > largest {
			largest = len(body)
		}
		var got PublicStorageDetail
		if len(body) > MaxStorageDetailBytes || json.Unmarshal([]byte(body), &got) != nil || got.Reason != storageDetailPublicReasons[i] ||
			got.Binding.DBGeneration != math.MaxUint64-1 || got.Index.DurationNanoseconds == nil || *got.Index.DurationNanoseconds != math.MaxInt64-1 ||
			!reflect.DeepEqual(got.Index, d.Index) || !reflect.DeepEqual(got.BankHash, d.BankHash) || !reflect.DeepEqual(got.IO, d.IO) || !reflect.DeepEqual(got.Inventory, d.Inventory) || !reflect.DeepEqual(got.Liveness, d.Liveness) {
			t.Fatal("maximum family lost a field")
		}
	}
	if total > storageDetailFamilyBytes || storageDetailFamilyBytes != 80<<10 || MaxStorageDetailBytes != 8192 {
		t.Fatal("family exceeded or changed its reviewed I/O-inclusive caps")
	}
	t.Logf("storage public largest: %d/%d; complete family: %d/%d bytes", largest, MaxStorageDetailBytes, total, storageDetailFamilyBytes)
}

func TestStorageDetailRealFeedReaderAndCachedFailures(t *testing.T) {
	now := time.Now()
	snapshot, capture := storageSourceFixture(now)
	capture.Detail.Inventory = maximumStorageDetail(capture.Detail.CapturedAt).Inventory
	var samples atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{
		Storage: func(time.Time) (StorageDetailCapture, error) { samples.Add(1); return capture, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	var gate atomic.Pointer[storageDetailGate]
	g := storageGate(snapshot, time.Now())
	g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	reader, err := newStorageDetailReader(newCollector(path).client, &gate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.client.CloseIdleConnections)
	reader.collect(context.Background())
	reply := reader.lookup(time.Now())
	var got PublicStorageDetail
	if reply.status != 200 || json.Unmarshal([]byte(reply.body), &got) != nil || !reflect.DeepEqual(got.Binding, capture.Detail.Binding) ||
		!reflect.DeepEqual(got.Index, capture.Detail.Index) || !reflect.DeepEqual(got.BankHash, capture.Detail.BankHash) ||
		!reflect.DeepEqual(got.Inventory, capture.Detail.Inventory) ||
		reply.remainingMilliseconds < 1 || reply.remainingMilliseconds > 30000 {
		t.Fatalf("real transport projection: %+v", reply)
	}
	previous := reader.latest.Load()
	deadline := previous.sourceFreshUntil
	reader.collect(context.Background())
	current := reader.latest.Load()
	if current == nil || current.sourceFreshUntil.After(deadline) || !current.sourceFreshUntil.After(time.Now()) ||
		!current.publishedAt.Equal(previous.publishedAt) || current.sourceDigest != previous.sourceDigest || current.bodies != previous.bodies {
		t.Fatal("repeated capture changed its projection or renewed its lease")
	}
	deadline = current.sourceFreshUntil
	feed.Close()
	before := samples.Load()
	for range 20 {
		if reader.lookup(time.Now()).body != reply.body {
			t.Fatal("viewer changed cached reply")
		}
	}
	if samples.Load() != before {
		t.Fatal("viewer sampled a stopped node")
	}
	reader.collect(context.Background())
	stale := reader.lookup(time.Now())
	if json.Unmarshal([]byte(stale.body), &got) != nil || got.State != "stale" || got.Reason != "source_unavailable" ||
		!reflect.DeepEqual(got.Binding, capture.Detail.Binding) || !reflect.DeepEqual(got.Inventory, capture.Detail.Inventory) || stale.remainingMilliseconds != 0 {
		t.Fatal("failure relabelled old data fresh or erased its binding")
	}
	if !reader.latest.Load().sourceFreshUntil.Equal(deadline) {
		t.Fatal("failed collect renewed lease")
	}
}

func TestStorageDetailOnlyGateSupportsBothRoles(t *testing.T) {
	path, fixture := fixtureFeed(t)
	c := newCollector(path)
	c.public, c.publicOptions = true, PublicOptions{Resources: true, StorageDetails: true}
	t.Cleanup(c.client.CloseIdleConnections)
	snapshot, _ := storageSourceFixture(time.Now())
	var original statusStamp
	for _, mode := range []string{"verifying", "validator"} {
		snapshot.Mode, snapshot.SampledAt = mode, time.Now().UTC()
		setFixture(t, fixture, snapshot)
		c.poll(context.Background(), time.Now().UTC())
		gate, frame := c.storageGate.Load(), c.latest.Load()
		if gate == nil || frame == nil || gate.failure(time.Now()) != "" || !gate.stamp.valid() ||
			frame.statusStamp(statusStorage) != gate.stamp || c.cacheGate.Load() != nil {
			t.Fatalf("%s did not publish a storage-only status gate", mode)
		}
		if original.valid() && gate.stamp != original {
			t.Fatal("a supported role change unnecessarily invalidated storage")
		}
		original = gate.stamp
	}
}

func TestStorageDetailAllRolesOptInAndRoutes(t *testing.T) {
	for _, tc := range []struct {
		origin string
		opts   PublicOptions
	}{
		{"", PublicOptions{StorageDetails: true}}, {"", PublicOptions{Resources: true, StorageDetails: true}},
		{"https://stats.example", PublicOptions{StorageDetails: true}},
	} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if err := serveProfile(context.Background(), "/unused", listener, tc.origin, tc.opts); err == nil || !strings.Contains(err.Error(), "resource disclosure") {
			t.Fatal("storage disclosure gate missing")
		}
	}
	for _, mode := range []string{"verifying", "validator"} {
		for _, enabled := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "_off", true: "_on"}[enabled], func(t *testing.T) {
				snapshot, capture := storageSourceFixture(time.Now())
				snapshot.Mode = mode
				path := socketPath(t)
				feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{
					Storage: func(time.Time) (StorageDetailCapture, error) { return capture, nil },
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(feed.Close)
				address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, StorageDetails: enabled})
				defer replayDetailStopPublic(t, cancel, done)
				code, features, _ := replayDetailPublicRequest(t, address, "/api/v1/features?include=advanced_status", "stats.example", "")
				want := `{"schema_version":1,"history":false,"cluster":false}`
				if enabled {
					want = `{"schema_version":1,"history":false,"cluster":false,"storage_details":true}`
				}
				if code != 200 || string(features) != want {
					t.Fatalf("feature defaults: %d %s", code, features)
				}
				if !enabled {
					if code, _, _ := replayDetailPublicRequest(t, address, "/api/v1/details/storage", "stats.example", ""); code != 404 {
						t.Fatal("disabled route registered")
					}
					return
				}
				deadline := time.Now().Add(3 * time.Second)
				for {
					code, body, headers := replayDetailPublicRequest(t, address, "/api/v1/details/storage", "stats.example", "")
					if code == 200 {
						var got PublicStorageDetail
						if json.Unmarshal(body, &got) != nil || !reflect.DeepEqual(got.Binding, capture.Detail.Binding) || !reflect.DeepEqual(got.Index, capture.Detail.Index) ||
							headers.Get("X-Content-Type-Options") != "nosniff" || headers.Get(statusRemainingHeader) == "" {
							t.Fatal("public source/headers changed")
						}
						if _, err := parseStorageDetailRemaining(headers.Get(replayDetailRemainingHeader)); err != nil {
							t.Fatal(err)
						}
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("reader did not publish: %d %s", code, body)
					}
					time.Sleep(10 * time.Millisecond)
				}
				for _, req := range []struct {
					path, host, origin string
					code               int
				}{
					{"/api/v1/details/storage", "wrong.example", "", 403}, {"/api/v1/details/storage", "stats.example", "https://wrong.example", 403},
					{"/api/v1/details/storage?", "stats.example", "", 400}, {"/api/v1/details/storage?x=1", "stats.example", "", 400},
					{"/api/v1/details/%73torage", "stats.example", "", 400}, {"/details/storage", "stats.example", "", 404},
				} {
					if code, _, _ := replayDetailPublicRequest(t, address, req.path, req.host, req.origin); code != req.code {
						t.Fatalf("route %q got%d want%d", req.path, code, req.code)
					}
				}
				if code, body, _ := replayDetailPublicRequest(t, address, "/api/v1/status", "stats.example", ""); code != 200 || len(body) > MaxSnapshotBytes || strings.Contains(string(body), "db_opened_at") {
					t.Fatal("storage changed ordinary status contract")
				}
			})
		}
	}
}
