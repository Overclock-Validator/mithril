package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func maximumConsensusClock(at time.Time) *ConsensusClockDetail {
	return &ConsensusClockDetail{Scope: "consensus_bounded_live_clock", Generation: math.MaxUint64, SampledAt: at, AnchorAt: at,
		AnchorSlot: math.MaxUint64 - 4, AnchorAgeNanoseconds: math.MaxInt64 - 1, EstimatedSlot: math.MaxUint64, AddedSlots: 4}
}

func TestNodeNetworkLiveClockContract(t *testing.T) {
	at := time.Now().UTC()
	d := maximumNodeNetwork(at)
	d.LiveClock = maximumConsensusClock(at)
	body, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeNodeNetworkDetail(body)
	if err != nil || !reflect.DeepEqual(got.LiveClock, d.LiveClock) {
		t.Fatalf("clock changed: %v", err)
	}
	for _, mutate := range []func(*ConsensusClockDetail){
		func(c *ConsensusClockDetail) { c.Scope = "certified_tip" }, func(c *ConsensusClockDetail) { c.Generation = 0 },
		func(c *ConsensusClockDetail) { c.SampledAt = time.Time{} }, func(c *ConsensusClockDetail) { c.AnchorAt = time.Time{} },
		func(c *ConsensusClockDetail) { c.AnchorSlot = 0 }, func(c *ConsensusClockDetail) { c.AnchorAgeNanoseconds = math.MaxInt64 },
		func(c *ConsensusClockDetail) { c.AddedSlots = 5 }, func(c *ConsensusClockDetail) { c.EstimatedSlot-- },
		func(c *ConsensusClockDetail) { c.SampledAt = at.Add(-NetworkDetailFreshFor) }, func(c *ConsensusClockDetail) { c.AnchorAt = at.Add(2 * time.Second) },
	} {
		bad := d
		c := *d.LiveClock
		mutate(&c)
		bad.LiveClock = &c
		if validateNodeNetworkDetail(bad) == nil {
			t.Fatal("invalid clock accepted")
		}
	}
	c := d.LiveClock
	c.AnchorSlot, c.EstimatedSlot, c.AddedSlots, c.AnchorAgeNanoseconds = 100, 100, 0, 0
	c.AnchorAt = at.Add(time.Second) // Wall dates do not reconstruct monotonic age.
	if validateNodeNetworkDetail(d) != nil {
		t.Fatal("known zero or independent original dates rejected")
	}
	c.AnchorSlot, c.EstimatedSlot, c.AddedSlots, c.AnchorAgeNanoseconds = math.MaxUint64, math.MaxUint64, 0, 1000000000
	if validateNodeNetworkDetail(d) != nil {
		t.Fatal("actual uint64-saturated delta rejected")
	}
	if _, err := preparePublicNodeNetwork(d, at, at.Add(NetworkDetailFreshFor+time.Nanosecond)); err == nil {
		t.Fatal("clock lease extended")
	}
	c.SampledAt = at.Add(-20 * time.Second)
	if _, err := preparePublicNodeNetwork(d, at, at.Add(10*time.Second)); err != nil {
		t.Fatal("original shorter lease refused")
	}
	if _, err := preparePublicNodeNetwork(d, at, at.Add(10*time.Second+time.Nanosecond)); err == nil {
		t.Fatal("old clock sample acquired a new lease")
	}
}

func TestNodeNetworkClockOnlyAndReceiverLoss(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	capture.Detail.Interfaces, capture.Detail.InterfacesReason = nil, "not_provided"
	capture.Detail.LiveClock = maximumConsensusClock(now.UTC())
	source := &networkDetailSource{nodeSample: func(time.Time) (NodeNetworkCapture, error) { return capture, nil }}
	source.update(snapshot, now)
	lost := snapshot
	lost.BlockSource = nil
	g := networkGate(lost, time.Now())
	source.gate.Store(&g)
	w := httptest.NewRecorder()
	source.serveHTTP(w, httptest.NewRequest("GET", "/details/network", nil))
	got, err := decodeNodeNetworkDetail(w.Body.Bytes())
	if w.Code != 200 || err != nil || got.Receiver != nil || !reflect.DeepEqual(got.LiveClock, capture.Detail.LiveClock) {
		t.Fatal("receiver loss cleared or redated independent clock")
	}
	path := socketPath(t)
	var samples atomic.Int64
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{NodeNetwork: func(time.Time) (NodeNetworkCapture, error) { samples.Add(1); return capture, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	reader := networkDetailTestReader(t, path, snapshot, now)
	reader.collect(context.Background())
	initial := reader.lookup(time.Now())
	g = networkDetailTestObserveStatus(t, reader, lost)
	reply := reader.lookup(time.Now())
	var public PublicNodeNetworkDetail
	if initial.status != 200 || reply.status != 200 || json.Unmarshal([]byte(reply.body+reply.tail), &public) != nil || public.Receiver != nil || public.Interfaces != nil ||
		!reflect.DeepEqual(public.LiveClock, capture.Detail.LiveClock) || reply.remainingMilliseconds <= 0 || reply.remainingMilliseconds > initial.remainingMilliseconds {
		t.Fatal("clock-only Unix reader depended on receiver or renewed its lease")
	}
	previous := reader.latest.Load()
	deadline := previous.sourceFreshUntil
	reader.collect(context.Background())
	current := reader.latest.Load()
	if current == nil || current.sourceFreshUntil.After(deadline) || !current.sourceFreshUntil.After(time.Now()) ||
		!current.publishedAt.Equal(previous.publishedAt) || current.sourceDigest != previous.sourceDigest {
		t.Fatal("same clock capture changed its source timing or renewed its lease")
	}
	deadline = current.sourceFreshUntil
	refreshed := reader.lookup(time.Now())
	if refreshed.status != 200 || json.Unmarshal([]byte(refreshed.body+refreshed.tail), &public) != nil ||
		public.Receiver != nil || !reflect.DeepEqual(public.LiveClock, capture.Detail.LiveClock) {
		t.Fatal("receiver-loss refetch hid the independent clock")
	}
	feed.Close()
	before := samples.Load()
	for range 20 {
		reader.lookup(time.Now())
	}
	if samples.Load() != before || !reader.latest.Load().sourceFreshUntil.Equal(deadline) {
		t.Fatal("viewer requests sampled clock or renewed its lease")
	}
}

func TestNodeNetworkPublicReceiverLossPreservesClock(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	capture.Detail.LiveClock = maximumConsensusClock(now.UTC())
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		t.Fatal(err)
	}
	var current atomic.Pointer[Snapshot]
	current.Store(&snapshot)
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/snapshot":
			w.Header().Set(statusRemainingHeader, "5000")
			_ = json.NewEncoder(w).Encode(current.Load())
		case "/details/network":
			w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(time.Until(capture.FreshUntil).Milliseconds(), 10))
			_, _ = w.Write(body)
		default:
			http.NotFound(w, request)
		}
	})
	c := newCollector(path)
	t.Cleanup(c.client.CloseIdleConnections)
	c.public, c.publicOptions = true, PublicOptions{Resources: true, NetworkDetails: true}
	c.poll(context.Background(), time.Now().UTC())
	before := c.networkGate.Load()
	if before == nil || !before.stamp.valid() || !before.nodeStamp.valid() {
		t.Fatal("initial status did not publish both network stamps")
	}
	r, err := newNetworkDetailReader(c.client, &c.networkGate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.client.CloseIdleConnections)
	r.admit = func(reply networkDetailReply) (time.Duration, bool) {
		return c.admitNetworkDetail(r, reply, statusNetworkNode)
	}
	r.collect(context.Background())
	request := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/details/network", nil))
		return w
	}
	first := request()
	var original PublicNodeNetworkDetail
	firstLease, firstErr := strconv.ParseInt(first.Header().Get(replayDetailRemainingHeader), 10, 64)
	if first.Code != http.StatusOK || firstErr != nil || firstLease < 1 || json.Unmarshal(first.Body.Bytes(), &original) != nil || original.Receiver == nil {
		t.Fatalf("initial public network data: status=%d lease=%q body=%s", first.Code, first.Header().Get(replayDetailRemainingHeader), first.Body.String())
	}
	lost := snapshot
	lost.BlockSource = nil
	lost.SampledAt = time.Now().UTC()
	if !lost.SampledAt.After(snapshot.SampledAt) {
		lost.SampledAt = snapshot.SampledAt.Add(time.Nanosecond)
	}
	current.Store(&lost)
	c.poll(context.Background(), time.Now().UTC())
	after := c.networkGate.Load()
	if after == nil || after.nodeStamp != before.nodeStamp || !after.stamp.valid() || after.stamp == before.stamp {
		t.Fatal("actual status poll did not isolate the receiver transition")
	}
	response := request()
	var independent PublicNodeNetworkDetail
	lease, leaseErr := strconv.ParseInt(response.Header().Get(replayDetailRemainingHeader), 10, 64)
	statusLease, statusErr := strconv.ParseInt(response.Header().Get(statusRemainingHeader), 10, 64)
	if response.Code != http.StatusOK || leaseErr != nil || lease < 1 || lease > firstLease || statusErr != nil || statusLease < 1 ||
		json.Unmarshal(response.Body.Bytes(), &independent) != nil || independent.Receiver != nil ||
		!reflect.DeepEqual(independent.LiveClock, capture.Detail.LiveClock) || !independent.CollectorAt.Equal(original.CollectorAt) {
		t.Fatal("public receiver loss hid or renewed independent clock data")
	}
}
