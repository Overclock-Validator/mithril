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

	"golang.org/x/time/rate"
)

func productionDetailTestReader(t *testing.T, path string, snapshot Snapshot, now time.Time) *productionDetailReader {
	t.Helper()
	var gate atomic.Pointer[productionDetailGate]
	g := productionGate(snapshot, now)
	g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	reader, err := newProductionDetailReader(newCollector(path).client, &gate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.client.CloseIdleConnections)
	return reader
}

func decodePublicProductionDetail(t *testing.T, reply productionDetailReply) PublicProductionDetail {
	t.Helper()
	var d PublicProductionDetail
	if reply.status != http.StatusOK || len(reply.body) > MaxProductionDetailBytes || json.Unmarshal([]byte(reply.body), &d) != nil {
		t.Fatalf("public production detail: status=%d body=%s", reply.status, reply.body)
	}
	return d
}

func TestProductionDetailReaderConstructorAndRealFeed(t *testing.T) {
	var gate atomic.Pointer[productionDetailGate]
	for _, client := range []*http.Client{nil, {}, {Transport: (*http.Transport)(nil)}, {Transport: &http.Transport{}}} {
		if _, err := newProductionDetailReader(client, &gate); err == nil {
			t.Fatal("invalid client accepted")
		}
	}
	original := newCollector("/unused").client
	if _, err := newProductionDetailReader(original, nil); err == nil {
		t.Fatal("nil gate accepted")
	}
	r, err := newProductionDetailReader(original, &gate)
	if err != nil {
		t.Fatal(err)
	}
	r.client.CloseIdleConnections()
	transport := r.client.Transport.(*http.Transport)
	if transport == original.Transport || transport.Proxy != nil || transport.MaxResponseHeaderBytes != 4096 ||
		transport.MaxConnsPerHost != 1 || transport.MaxIdleConnsPerHost != 1 || transport.MaxIdleConns != 1 ||
		!transport.DisableKeepAlives || r.client.Timeout != time.Second || r.client.Jar != nil ||
		r.client.CheckRedirect(nil, nil) != http.ErrUseLastResponse || original.Transport.(*http.Transport).MaxConnsPerHost != 0 ||
		r.limit.Limit() != 16 || r.limit.Burst() != 32 || productionDetailCollectInterval != 10*time.Second {
		t.Fatal("reader did not own the exact bounded transport")
	}

	snapshot, capture := productionSourceFixture(time.Now())
	capture.Detail.Binding.ProducerGeneration = math.MaxUint64 - 1
	capture.Detail.LastTerminal.Costs.BlockCost = math.MaxUint64
	capture.Detail.LastTerminal.Finalization = maximumProductionFinalization()
	capture.Detail.LastTerminal.Finalization.WindowCompletion = maximumProductionWindowCompletion(capture.Detail.LastTerminal.Slot, capture.Detail.LastTerminal.ObservedAt)
	capture.Detail.ParentReady = &ProductionParentReadyDetail{WindowStartSlot: 40, ParentSlot: 0,
		ParentBlockID: capture.Detail.Binding.GenesisHash, ReadyAt: capture.Detail.LastTerminal.ObservedAt,
		ObservedAt: capture.Detail.LastTerminal.ObservedAt, Activation: "initial"}
	var samples atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{
		Production: func(time.Time) (ProductionDetailCapture, error) {
			samples.Add(1)
			return capture, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	r = productionDetailTestReader(t, path, snapshot, time.Now())
	r.collect(context.Background())
	reply := r.lookup(time.Now())
	got := decodePublicProductionDetail(t, reply)
	if got.State != "fresh" || got.Reason != "" || got.CollectorAt.IsZero() || !reflect.DeepEqual(got.Binding, capture.Detail.Binding) ||
		!got.CapturedAt.Equal(capture.Detail.CapturedAt) || got.ProducerState != capture.Detail.ProducerState ||
		!reflect.DeepEqual(got.LastTerminal, capture.Detail.LastTerminal) || !reflect.DeepEqual(got.ParentReady, capture.Detail.ParentReady) ||
		reply.remainingMilliseconds < 1 || reply.remainingMilliseconds > 30000 ||
		r.latest.Load().sourceFreshUntil.After(capture.FreshUntil) {
		t.Fatalf("public document changed source values or clocks: %+v", got)
	}
	feed.Close()
	before := samples.Load()
	admit := func(productionDetailReply) (time.Duration, bool) { return time.Second, true }
	for range 20 {
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/production", nil), admit)
		remaining, err := parseProductionDetailRemaining(response.Header().Get(replayDetailRemainingHeader))
		if response.Code != 200 || response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Content-Type") != "application/json" || err != nil || remaining <= 0 {
			t.Fatal("cached public response unavailable")
		}
	}
	if samples.Load() != before {
		t.Fatal("public viewers invoked the source sampler")
	}
	for _, request := range []struct{ method, path, body string }{
		{"POST", "/api/v1/details/production", ""}, {"GET", "/api/v1/details/production", "x"},
		{"GET", "/api/v1/details/production?", ""}, {"GET", "/api/v1/details/production?x=1", ""},
		{"GET", "/api/v1/details/%70roduction", ""}, {"GET", "/api/v1/details/production/", ""},
	} {
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(request.method, request.path, strings.NewReader(request.body)), admit)
		if response.Code != 400 || response.Body.String() != productionDetailFailure("invalid_request").body || response.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatalf("strict public request accepted: %s, status=%d", request.path, response.Code)
		}
	}
	r.limit = rate.NewLimiter(0, 0)
	w := httptest.NewRecorder()
	r.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/details/production", nil), admit)
	if w.Code != 429 || w.Body.String() != productionDetailFailure("rate_limited").body || w.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatal("public rate limit failed open")
	}
}

func TestProductionDetailPublicCapacityAndDetachedValues(t *testing.T) {
	for _, change := range []func(*ProductionDetail){
		func(d *ProductionDetail) { d.LastTerminal = nil },
		func(d *ProductionDetail) { *d.LastTerminal.Costs = ProductionCostDetail{} },
		func(d *ProductionDetail) { d.LastTerminal.Bank, d.LastTerminal.Costs = nil, nil },
	} {
		value := productionDetailFixture()
		change(&value)
		family, err := preparePublicProductionDetail(value, value.CapturedAt, value.CapturedAt.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		public := decodePublicProductionDetail(t, productionDetailReply{status: 200, body: family.bodies[0]})
		if !reflect.DeepEqual(public.LastTerminal, value.LastTerminal) {
			t.Fatal("nullable or known-zero production values changed")
		}
	}
	d := productionDetailFixture()
	d.Binding.RunID = strings.Repeat("<", 128)
	d.Binding.ProducerGeneration = math.MaxUint64 - 1
	at := time.Date(9999, 12, 31, 23, 59, 59, 123456789, time.UTC)
	d.Binding.NodeStartedAt, d.Binding.ProducerStartedAt, d.CapturedAt = at, at, at
	d.LastTerminal.Slot, d.LastTerminal.ObservedAt, d.LastTerminal.Bank.ParentSlot = math.MaxUint64, at, math.MaxUint64
	d.LastTerminal.Costs = &ProductionCostDetail{BlockCost: math.MaxUint64, AllocatedDataSizeDeltaBytes: math.MaxUint64,
		SerializedEntryBytes: math.MaxUint64, Transactions: math.MaxUint64, Limits: ProductionCostLimits{
			BlockCost: math.MaxUint64, WritableAccountCost: math.MaxUint64, AllocatedDataSizeDeltaBytes: math.MaxUint64,
			MaxBatchBytes: math.MaxUint64, MaxEntryBytes: math.MaxUint64}}
	d.ParentReady = &ProductionParentReadyDetail{WindowStartSlot: math.MaxUint64 - 3, ParentSlot: math.MaxUint64 - 4,
		ParentBlockID: d.Binding.GenesisHash, ReadyAt: at, ObservedAt: at, Activation: "handover"}
	family, err := preparePublicProductionDetail(d, at, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	total, largest := 0, 0
	for i, body := range family.bodies {
		public := decodePublicProductionDetail(t, productionDetailReply{status: 200, body: body})
		if len(body) > MaxProductionDetailBytes || !reflect.DeepEqual(public.Binding, d.Binding) || !public.CapturedAt.Equal(d.CapturedAt) ||
			public.ProducerState != d.ProducerState || !reflect.DeepEqual(public.LastTerminal, d.LastTerminal) ||
			!reflect.DeepEqual(public.ParentReady, d.ParentReady) || !public.CollectorAt.Equal(at) ||
			public.Reason != productionDetailPublicReasons[i] || i == 0 && public.State != "fresh" || i != 0 && public.State != "stale" {
			t.Fatal("pre-encoded production variant changed source values")
		}
		total, largest = total+len(body), max(largest, len(body))
	}
	if total > productionDetailFamilyBytes {
		t.Fatal("retained production variants exceeded the family cap")
	}
	t.Logf("maximum-width production: largest=%d/%d, family=%d/%d", largest, MaxProductionDetailBytes, total, productionDetailFamilyBytes)
	original := family.bodies
	d.LastTerminal.Bank.ParentSlot = 1
	d.LastTerminal.Costs.BlockCost = 1
	if family.bodies != original {
		t.Fatal("publication retained caller-owned terminal pointers")
	}
}

func TestProductionDetailReaderNoRenewalGenerationAndContext(t *testing.T) {
	now := time.Now()
	snapshot, capture := productionSourceFixture(now)
	type sourceReply struct {
		status    int
		body      string
		remaining string
		change    func()
	}
	var current atomic.Pointer[sourceReply]
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/details/production" || request.URL.RawQuery != "" {
			t.Error("reader selected an unexpected production source path")
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
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "20000"})
	r := productionDetailTestReader(t, path, snapshot, now)
	r.collect(context.Background())
	first := r.latest.Load()
	if first == nil || r.highwaterGen != 1 {
		t.Fatal("control production source did not install")
	}
	deadline, collected := first.sourceFreshUntil, first.publishedAt
	failure := productionDetailFailure("source_unavailable")
	current.Store(&sourceReply{status: failure.status, body: failure.body})
	r.collect(context.Background())
	stale := decodePublicProductionDetail(t, r.lookup(time.Now()))
	if r.latest.Load() != first || stale.State != "stale" || stale.Reason != "source_unavailable" ||
		stale.LastTerminal.ObservedAt != capture.Detail.LastTerminal.ObservedAt || r.highwaterGen != 1 {
		t.Fatal("failed poll replaced, redated or reset retained production values")
	}
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "20000"})
	r.collect(context.Background())
	if r.latest.Load() != first || r.failure.Load() != nil || r.latest.Load().sourceFreshUntil != deadline || r.latest.Load().publishedAt != collected {
		t.Fatal("unchanged source after failure renewed its clocks")
	}

	conflict := capture.Detail
	conflict.ProducerState = "stopped"
	conflictBody, _ := json.Marshal(conflict)
	current.Store(&sourceReply{status: 200, body: string(conflictBody), remaining: "20000"})
	r.collect(context.Background())
	if r.latest.Load() != first || r.failure.Load() == nil || r.failure.Load().reason != "source_invalid" {
		t.Fatal("same capture with changed content was accepted")
	}

	capture.Detail.Binding.ProducerGeneration = 2
	capture.Detail.Binding.ProducerStartedAt = now.Add(-20 * time.Minute).UTC()
	capture.Detail.CapturedAt = time.Now().UTC()
	capture.Detail.ProducerState = "stopped"
	body, _ = json.Marshal(capture.Detail)
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "20000"})
	r.collect(context.Background())
	second := r.latest.Load()
	if second == first || r.highwaterGen != 2 || decodePublicProductionDetail(t, r.lookup(time.Now())).Binding.ProducerGeneration != 2 {
		t.Fatal("new producer generation was not installed")
	}

	current.Store(&sourceReply{status: failure.status, body: failure.body})
	r.collect(context.Background())
	empty := productionDetailGate{observedAt: time.Now(), reason: "source_unavailable"}
	r.gate.Store(&empty)
	r.collect(context.Background())
	inapplicableStatus := snapshot
	inapplicableStatus.Mode = "verifying"
	inapplicable := productionGate(inapplicableStatus, time.Now())
	inapplicable.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 2}
	r.gate.Store(&inapplicable)
	r.collect(context.Background())
	if r.highwaterGen != 2 {
		t.Fatal("error, empty status or role change reset the generation high-water")
	}
	fresh := productionGate(snapshot, time.Now())
	fresh.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 3}
	r.gate.Store(&fresh)
	capture.Detail.Binding.ProducerGeneration = 1
	capture.Detail.CapturedAt = time.Now().UTC()
	body, _ = json.Marshal(capture.Detail)
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "20000"})
	r.collect(context.Background())
	if r.latest.Load() != second || r.failure.Load() == nil || r.failure.Load().reason != "binding_changed" {
		t.Fatal("older same-node producer generation was accepted after failures")
	}
	capture.Detail.Binding.ProducerGeneration = 2
	capture.Detail.Binding.ProducerStartedAt = capture.Detail.Binding.ProducerStartedAt.Add(time.Second)
	body, _ = json.Marshal(capture.Detail)
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "20000"})
	r.collect(context.Background())
	if r.failure.Load() == nil || r.failure.Load().reason != "binding_changed" {
		t.Fatal("same producer generation changed its start")
	}

	next := snapshot
	next.RunID, next.StartedAt = "replacement-node", snapshot.StartedAt.Add(time.Minute)
	capture.Detail.Binding.RunID, capture.Detail.Binding.NodeStartedAt = next.RunID, next.StartedAt
	capture.Detail.Binding.ProducerGeneration = 1
	capture.Detail.Binding.ProducerStartedAt = time.Now().Add(-time.Minute).UTC()
	capture.Detail.CapturedAt = time.Now().UTC()
	body, _ = json.Marshal(capture.Detail)
	nextGate := productionGate(next, time.Now())
	nextGate.stamp = statusStamp{nodeEpoch: 2, familyEpoch: 4}
	r.gate.Store(&nextGate)
	current.Store(&sourceReply{status: 200, body: "{", remaining: "20000"})
	r.collect(context.Background())
	if r.highwaterGen != 2 {
		t.Fatal("invalid new-node capture replaced the producer high-water")
	}
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "20000"})
	r.collect(context.Background())
	if r.highwaterGen != 1 || !sameProductionNode(r.highwaterNode, nextGate) || decodePublicProductionDetail(t, r.lookup(time.Now())).Binding.RunID != next.RunID {
		t.Fatal("fully valid new node binding did not adopt its producer generation")
	}
}

func TestProductionDetailReaderShortenedLease(t *testing.T) {
	now := time.Now()
	snapshot, capture := productionSourceFixture(now)
	body, _ := json.Marshal(capture.Detail)
	var remaining atomic.Int64
	remaining.Store(20000)
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining.Load(), 10))
		_, _ = w.Write(body)
	})
	r := productionDetailTestReader(t, path, snapshot, now)
	r.collect(context.Background())
	first := r.latest.Load()
	if first == nil {
		t.Fatal("initial capture was not accepted")
	}
	remaining.Store(10000)
	r.collect(context.Background())
	if next := r.latest.Load(); next == first || !next.sourceFreshUntil.Before(first.sourceFreshUntil) || !next.publishedAt.Equal(first.publishedAt) {
		t.Fatal("unchanged capture ignored a shortened source lease or changed its collection time")
	}
	shortened := r.latest.Load()
	remaining.Store(20000)
	r.collect(context.Background())
	if r.latest.Load() != shortened {
		t.Fatal("later observation renewed a shortened production lease")
	}
}

func TestProductionDetailStatusStampRecovery(t *testing.T) {
	snapshot, capture := productionSourceFixture(time.Now())
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
	r := productionDetailTestReader(t, path, snapshot, time.Now())
	r.collect(context.Background())
	original := r.latest.Load()
	if original == nil || !original.stamp.valid() || r.lookup(time.Now()).status != http.StatusOK {
		t.Fatal("initial production capture was not published")
	}
	initialDeadline, initialPublished := original.sourceFreshUntil, original.publishedAt
	setGate := func(epoch uint64, s Snapshot) {
		gate := productionGate(s, time.Now())
		gate.stamp = statusStamp{epoch, epoch}
		r.gate.Store(&gate)
	}
	observer := snapshot
	observer.Mode = "verifying"
	setGate(2, observer)
	if reply := r.lookup(time.Now()); reply.status != http.StatusServiceUnavailable || !strings.Contains(reply.body, `"reason":"not_applicable"`) {
		t.Fatal("non-validator role exposed production data")
	}
	setGate(3, snapshot)
	if r.lookup(time.Now()).status != http.StatusServiceUnavailable || r.latest.Load() != original {
		t.Fatal("role recovery revived an old family without a new fetch")
	}
	r.collect(context.Background())
	recovered := r.latest.Load()
	if recovered == original || recovered.stamp != (statusStamp{3, 3}) || recovered.sourceFreshUntil.After(initialDeadline) ||
		!recovered.publishedAt.Equal(initialPublished) || r.highwaterGen != capture.Detail.Binding.ProducerGeneration {
		t.Fatal("validated role recovery renewed capture clocks or generation")
	}
	replacement := snapshot
	replacement.RunID = "replacement-run"
	setGate(4, replacement)
	if r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("replacement node accepted the previous production family")
	}
	setGate(5, snapshot)
	if r.lookup(time.Now()).status != http.StatusServiceUnavailable || captures.Load() != 2 {
		t.Fatal("A to B to A revived production without a new fetch")
	}
	r.collect(context.Background())
	returned := r.latest.Load()
	if captures.Load() != 3 || returned.stamp != (statusStamp{5, 5}) || returned.sourceFreshUntil.After(initialDeadline) ||
		!returned.publishedAt.Equal(initialPublished) || r.lookup(time.Now()).status != http.StatusOK {
		t.Fatal("validated production re-fetch did not retain original clocks")
	}
	expired := *returned
	expired.sourceFreshUntil = time.Now().Add(-time.Second)
	r.latest.Store(&expired)
	r.lastCapture = &expired
	setGate(6, snapshot)
	r.collect(context.Background())
	if r.latest.Load() != &expired || r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("re-fetching an expired production capture revived it")
	}
}

func TestProductionDetailInFlightStampABAFailsClosed(t *testing.T) {
	snapshot, capture := productionSourceFixture(time.Now())
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
	r := productionDetailTestReader(t, path, snapshot, time.Now())
	done := make(chan struct{})
	go func() { r.collect(context.Background()); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("production fetch did not start")
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
		t.Fatal("production collector did not finish")
	}
	if r.latest.Load() != nil || r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("in-flight production survived an A to B to A gate change")
	}
}

func TestProductionDetailFinalAdmission(t *testing.T) {
	setup := func(t *testing.T) (*collector, *productionDetailReader) {
		t.Helper()
		now := time.Now()
		snapshot, capture := productionSourceFixture(now)
		family, err := preparePublicProductionDetail(capture.Detail, now, now.Add(20*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		family.stamp = statusStamp{1, 1}
		c := &collector{clockOrigin: now}
		gate := productionGate(snapshot, now)
		gate.stamp = family.stamp
		c.productionGate.Store(&gate)
		r := &productionDetailReader{gate: &c.productionGate, limit: rate.NewLimiter(100, 100)}
		r.latest.Store(family)
		frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
		frame.fence.nodeEpoch, frame.fence.families[statusProduction] = 1, 1
		c.latest.Store(frame)
		return c, r
	}
	request := func(r *productionDetailReader, admit func(productionDetailReply) (time.Duration, bool)) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/production", nil), admit)
		return response
	}
	t.Run("independent leases", func(t *testing.T) {
		c, r := setup(t)
		response := request(r, func(reply productionDetailReply) (time.Duration, bool) { return c.admitProductionDetail(r, reply) })
		detail, detailErr := parseProductionDetailRemaining(response.Header().Get(replayDetailRemainingHeader))
		statusMS, statusErr := strconv.ParseInt(response.Header().Get(statusRemainingHeader), 10, 64)
		if response.Code != http.StatusOK || detailErr != nil || statusErr != nil || statusMS <= 0 || detail <= time.Duration(statusMS)*time.Millisecond {
			t.Fatal("production and status leases were not independently retained")
		}
		if noGate := request(r, nil); noGate.Code != http.StatusServiceUnavailable {
			t.Fatal("missing final admission exposed production")
		}
	})
	t.Run("failure after selection", func(t *testing.T) {
		c, r := setup(t)
		response := request(r, func(reply productionDetailReply) (time.Duration, bool) {
			gate := *r.gate.Load()
			r.failure.Store(&productionDetailReaderFailure{gate: gate, reason: "source_unavailable"})
			return c.admitProductionDetail(r, reply)
		})
		if response.Code != http.StatusServiceUnavailable || response.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatal("source failure after selection exposed fresh production")
		}
		stale := request(r, func(reply productionDetailReply) (time.Duration, bool) { return c.admitProductionDetail(r, reply) })
		if stale.Code != http.StatusOK || !strings.Contains(stale.Body.String(), `"state":"stale"`) || stale.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatal("unchanged failure did not retain explicitly stale production")
		}
	})
	for _, name := range []string{"family replaced", "detail expired", "gate changed", "role changed", "parent status expired"} {
		t.Run(name, func(t *testing.T) {
			c, r := setup(t)
			response := request(r, func(reply productionDetailReply) (time.Duration, bool) {
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
				case "role changed":
					gate := *r.gate.Load()
					gate.reason, gate.stamp = "not_applicable", statusStamp{1, 2}
					r.gate.Store(&gate)
				case "parent status expired":
					frame := *c.latest.Load()
					frame.freshUntil = time.Now().Add(-time.Second)
					c.latest.Store(&frame)
				}
				return c.admitProductionDetail(r, reply)
			})
			if response.Code != http.StatusServiceUnavailable || response.Header().Get(statusRemainingHeader) != "" ||
				response.Header().Get(replayDetailRemainingHeader) != "" {
				t.Fatal("changed or expired production admission remained fresh")
			}
		})
	}
}

func TestProductionDetailReaderDeadlineTransfersAndCancellation(t *testing.T) {
	snapshot, capture := productionSourceFixture(time.Now())
	body, _ := json.Marshal(capture.Detail)
	var headerAt atomic.Pointer[time.Time]
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(40 * time.Millisecond)
		at := time.Now()
		headerAt.Store(&at)
		w.Header().Set(replayDetailRemainingHeader, "1000")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(40 * time.Millisecond)
		_, _ = w.Write(body)
	})
	r := productionDetailTestReader(t, path, snapshot, time.Now())
	started := time.Now()
	r.collect(context.Background())
	family := r.latest.Load()
	if family == nil || headerAt.Load() == nil || family.sourceFreshUntil.Before(started.Add(time.Second)) ||
		!family.sourceFreshUntil.Before(headerAt.Load().Add(980*time.Millisecond)) || !family.publishedAt.After(*headerAt.Load()) {
		t.Fatal("header or body latency extended production freshness")
	}
	expired := decodePublicProductionDetail(t, r.lookup(family.sourceFreshUntil))
	if expired.State != "stale" || expired.Reason != "status_stale" {
		t.Fatal("exact production source deadline remained fresh")
	}
	for _, value := range []string{"", "0", "-1", "+1", "030000", "30001", "1.0", "1 "} {
		if _, err := parseProductionDetailRemaining(value); err == nil {
			t.Fatalf("invalid remaining time accepted: %q", value)
		}
	}
	for _, value := range []string{"1", "30000"} {
		if _, err := parseProductionDetailRemaining(value); err != nil {
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
	late := productionDetailTestReader(t, latePath, snapshot, time.Now())
	late.collect(context.Background())
	if late.latest.Load() != nil || late.failure.Load() == nil || late.failure.Load().reason != "source_time_invalid" {
		t.Fatal("body that outlived its original production lease was installed")
	}

	for _, mode := range []string{"duplicate", "oversize", "missing header", "bad header", "error header", "raw error", "404", "future"} {
		t.Run(mode, func(t *testing.T) {
			path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
				value, status := body, 200
				w.Header().Set(replayDetailRemainingHeader, "20000")
				switch mode {
				case "duplicate":
					value = []byte(strings.Replace(string(body), `"family":"production"`, `"family":"production","family":"production"`, 1))
				case "oversize":
					value = []byte(strings.Repeat(" ", MaxProductionDetailBytes+1))
				case "missing header":
					w.Header().Del(replayDetailRemainingHeader)
				case "bad header":
					w.Header().Set(replayDetailRemainingHeader, "030000")
				case "error header":
					status, value = 503, []byte(productionDetailFailure("source_unavailable").body)
				case "raw error":
					status, value = 503, []byte(strings.Replace(productionDetailFailure("source_unavailable").body, "source_unavailable", "private-secret", 1))
					w.Header().Del(replayDetailRemainingHeader)
				case "404":
					status, value = 404, []byte("not found")
					w.Header().Del(replayDetailRemainingHeader)
				case "future":
					future := capture.Detail
					future.CapturedAt = time.Now().Add(2 * time.Second).UTC()
					value, _ = json.Marshal(future)
				}
				w.WriteHeader(status)
				_, _ = w.Write(value)
			})
			r := productionDetailTestReader(t, path, snapshot, time.Now())
			r.collect(context.Background())
			if r.latest.Load() != nil || r.failure.Load() == nil {
				t.Fatal("bad production transfer installed a partial capture")
			}
			reply := r.lookup(time.Now())
			if reply.status != 503 || len(reply.body) > 512 || strings.Contains(reply.body, "private-secret") {
				t.Fatal("bad production transfer exposed raw input")
			}
			if mode == "404" && !strings.Contains(reply.body, `"reason":"unsupported_source"`) {
				t.Fatal("old private producer was not identified as unsupported")
			}
		})
	}
	for _, reason := range []string{"invalid_request", "rate_limited", "waiting_source", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "binding_changed", "response_limit", "unsupported_source", "not_applicable", "private-secret"} {
		reply := productionDetailFailure(reason)
		wantStatus := 503
		if reason == "invalid_request" {
			wantStatus = 400
		} else if reason == "rate_limited" {
			wantStatus = 429
		}
		if reply.status != wantStatus || len(reply.body) > 512 || !json.Valid([]byte(reply.body)) || strings.Contains(reply.body, "private-secret") {
			t.Fatalf("unbounded or incorrect production failure: %+v", reply)
		}
	}
	if decodeProductionDetailFailure([]byte(productionDetailFailure("unsupported_source").body)) != "source_invalid" {
		t.Fatal("public-only reason accepted from the private production source")
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
	blocked := productionDetailTestReader(t, blockedPath, snapshot, time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	go blocked.run(ctx)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocked production source was not reached")
	}
	cancel()
	select {
	case <-blocked.done:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not join the production reader")
	}
	if blocked.latest.Load() != nil || requests.Load() != 1 {
		t.Fatal("cancellation installed or retried a partial production capture")
	}
}

func TestProductionDetailCollectorGateAndPublicOptIn(t *testing.T) {
	path, fixture := fixtureFeed(t)
	c := newCollector(path)
	c.public, c.publicOptions = true, PublicOptions{Resources: true, ProductionDetails: true}
	t.Cleanup(c.client.CloseIdleConnections)
	snapshot, capture := productionSourceFixture(time.Now())
	setFixture(t, fixture, snapshot)
	c.poll(context.Background(), time.Now().UTC())
	g := c.productionGate.Load()
	if g == nil || g.failure(time.Now()) != "" || !g.matches(capture.Detail.Binding) || g.observedAt == g.observedAt.Round(0) ||
		!g.stamp.valid() || c.latest.Load().statusStamp(statusProduction) != g.stamp || c.cacheGate.Load() != nil {
		t.Fatalf("accepted status did not publish detached production context: %+v", g)
	}
	first := g.stamp
	observer := snapshot
	observer.Mode, observer.SampledAt = "verifying", time.Now().UTC()
	setFixture(t, fixture, observer)
	c.poll(context.Background(), time.Now().UTC())
	if g = c.productionGate.Load(); g.reason != "not_applicable" || !g.stamp.valid() || g.stamp == first ||
		c.latest.Load().statusStamp(statusProduction) != g.stamp {
		t.Fatal("non-validator role did not advance the production status gate")
	}
	first = g.stamp
	snapshot.SampledAt = time.Now().UTC()
	setFixture(t, fixture, snapshot)
	c.poll(context.Background(), time.Now().UTC())
	if g = c.productionGate.Load(); g.reason != "" || !g.stamp.valid() || g.stamp == first ||
		c.latest.Load().statusStamp(statusProduction) != g.stamp {
		t.Fatal("validator role recovery did not advance the production status gate")
	}
	fixture.Store(nil)
	c.poll(context.Background(), time.Now().UTC())
	g = c.productionGate.Load()
	if g.reason != "source_unavailable" || !g.matches(capture.Detail.Binding) {
		t.Fatal("source loss did not retain dated production node context")
	}
	bad := []byte("{")
	fixture.Store(&bad)
	c.poll(context.Background(), time.Now().UTC())
	if g = c.productionGate.Load(); g.reason != "source_invalid" || !g.matches(capture.Detail.Binding) {
		t.Fatal("malformed status did not retain only dated production context")
	}
	disabled := newCollector(path)
	t.Cleanup(disabled.client.CloseIdleConnections)
	disabled.poll(context.Background(), time.Now().UTC())
	if disabled.productionGate.Load() != nil {
		t.Fatal("default collector enabled production gate publication")
	}

	for _, tc := range []struct {
		origin string
		opts   PublicOptions
	}{
		{"", PublicOptions{ProductionDetails: true}},
		{"", PublicOptions{Resources: true, ProductionDetails: true}},
		{"https://stats.example", PublicOptions{ProductionDetails: true}},
	} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if err := serveProfile(context.Background(), "/unused.sock", listener, tc.origin, tc.opts); err == nil || !strings.Contains(err.Error(), "resource disclosure") {
			t.Fatalf("production disclosure accepted: origin=%q options=%+v error=%v", tc.origin, tc.opts, err)
		}
	}

	snapshot, capture = productionSourceFixture(time.Now())
	feedPath := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), feedPath, "", func() Snapshot { return snapshot }, DetailSamplers{
		Production: func(time.Time) (ProductionDetailCapture, error) { return capture, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	for _, enabled := range []bool{false, true} {
		address, cancel, done := replayDetailPublicServer(t, feedPath, PublicOptions{Resources: true, ProductionDetails: enabled})
		func() {
			defer replayDetailStopPublic(t, cancel, done)
			code, features, _ := replayDetailPublicRequest(t, address, "/api/v1/features?include=advanced_status", "stats.example", "")
			want := `{"schema_version":1,"history":false,"cluster":false}`
			if enabled {
				want = `{"schema_version":1,"history":false,"cluster":false,"production_details":true}`
			}
			if code != 200 || string(features) != want {
				t.Fatalf("production feature changed defaults: %d %s", code, features)
			}
			if !enabled {
				if code, _, _ := replayDetailPublicRequest(t, address, "/api/v1/details/production", "stats.example", ""); code != 404 {
					t.Fatal("disabled public production route was registered")
				}
				return
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				code, body, headers := replayDetailPublicRequest(t, address, "/api/v1/details/production", "stats.example", "")
				if code == 200 {
					public := decodePublicProductionDetail(t, productionDetailReply{status: code, body: string(body)})
					if !reflect.DeepEqual(public.Binding, capture.Detail.Binding) || public.CapturedAt != capture.Detail.CapturedAt ||
						public.ProducerState != capture.Detail.ProducerState || !reflect.DeepEqual(public.LastTerminal, capture.Detail.LastTerminal) {
						t.Fatal("public route changed production source fields")
					}
					if _, err := parseProductionDetailRemaining(headers.Get(replayDetailRemainingHeader)); err != nil || headers.Get("X-Content-Type-Options") != "nosniff" || headers.Get(statusRemainingHeader) == "" {
						t.Fatal("public route lost freshness or disclosure headers")
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("public production reader did not publish: %d %s", code, body)
				}
				time.Sleep(10 * time.Millisecond)
			}
			code, ordinary, _ := replayDetailPublicRequest(t, address, "/api/v1/status", "stats.example", "")
			if code != 200 || strings.Contains(string(ordinary), "producer_generation") || len(ordinary) > MaxSnapshotBytes {
				t.Fatal("production option changed the ordinary status contract")
			}
			for _, request := range []struct {
				path, host, origin string
				want               int
			}{
				{"/api/v1/details/production", "attacker.example", "", 403},
				{"/api/v1/details/production", "stats.example", "https://attacker.example", 403},
				{"/api/v1/details/production?", "stats.example", "", 400},
				{"/api/v1/details/production?x=1", "stats.example", "", 400},
				{"/api/v1/details/%70roduction", "stats.example", "", 400},
				{"/details/production", "stats.example", "", 404},
			} {
				code, _, _ := replayDetailPublicRequest(t, address, request.path, request.host, request.origin)
				if code != request.want {
					t.Fatalf("%s: got %d, want %d", request.path, code, request.want)
				}
			}
		}()
	}
}
