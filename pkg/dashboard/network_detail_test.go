package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func networkDetailFixture(now time.Time) (Snapshot, NetworkDetailCapture) {
	start := now.Add(-time.Hour).UTC()
	at := now.UTC()
	m := SocketMemory{ReceiveAllocatedBytes: math.MaxUint32, ReceiveLimitBytes: 1, SendAllocatedBytes: 2, SendLimitBytes: 3,
		ForwardAllocatedBytes: 4, SendQueuedBytes: 5, OptionsAllocatedBytes: 6, BacklogBytes: 7, Drops: 8}
	w := SocketDropWindow{StartedAt: now.Add(-time.Second).UTC(), SampledAt: at, DurationMilliseconds: 1000, Drops: 8}
	d := NetworkDetail{SchemaVersion: 1, Family: "network", Scope: "turbine_receiver_session", State: "fresh",
		Binding: NetworkDetailBinding{RunID: "network-run", NodeStartedAt: start, SourceStartedAt: start, ReceiverStartedAt: start,
			ReceiverSession: math.MaxUint64, SocketGeneration: math.MaxUint64}, UpdatedAt: at,
		Live:   SocketDetail{Enabled: true, State: "available", StartedAt: &at, SampledAt: &at, Memory: &m, DropWindow: &w},
		Repair: SocketDetail{Enabled: true, State: "available", StartedAt: &at, SampledAt: &at, Memory: &SocketMemory{}}}
	s := testSnapshot(at)
	s.RunID, s.StartedAt, s.GenesisHash = d.Binding.RunID, start, nil
	s.BlockSource = &BlockSource{SampledAt: at, StartedAt: start, ConfiguredSource: "turbine", LiveConnected: true,
		Turbine: &Turbine{SampledAt: at, StartedAt: start, Session: math.MaxUint64}}
	return s, NetworkDetailCapture{Detail: d, FreshUntil: now.Add(NetworkDetailFreshFor)}
}

func TestNetworkDetailRealFeed(t *testing.T) {
	now := time.Now()
	snapshot, capture := networkDetailFixture(now)
	capture.Detail.ArrivalTiming = &ArrivalTimingDetail{Scope: "retained_adjacent_tvu_arrivals", SampledAt: now.UTC(),
		LatestReceiptAt: now.Add(-time.Hour).UTC(), RetainedSlots: 2, AdjacentPairs: 1, TotalIntervalNanoseconds: 400000000}
	var calls, statusCalls atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot {
		statusCalls.Add(1)
		return snapshot
	}, DetailSamplers{
		Network: func(at time.Time) (NetworkDetailCapture, error) {
			if at == at.UTC() {
				t.Error("feed stripped monotonic clock")
			}
			calls.Add(1)
			return capture, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	client := newCollector(path).client
	t.Cleanup(client.CloseIdleConnections)
	status, body, headers := replayDetailHTTP(t, client, "GET", "http://node/details/network", "")
	d, err := decodeNetworkDetail([]byte(body))
	if status != 200 || err != nil || !reflect.DeepEqual(d, capture.Detail) || len(body) > MaxNetworkDetailBytes {
		t.Fatalf("complete document: status=%d error=%v body=%s", status, err, body)
	}
	remaining, err := strconv.Atoi(headers.Get(replayDetailRemainingHeader))
	if err != nil || remaining <= 0 || remaining > 30000 || headers.Get("Cache-Control") != "no-store" {
		t.Fatal("invalid remaining-time/cache header")
	}
	for range 20 {
		status, _, _ := replayDetailHTTP(t, client, "GET", "http://node/details/network", "")
		if status != 200 {
			t.Fatal("cached request failed")
		}
	}
	status, ordinary, _ := replayDetailHTTP(t, client, "GET", "http://node/snapshot", "")
	var decoded Snapshot
	if status != 200 || len(ordinary) > MaxSnapshotBytes || json.Unmarshal([]byte(ordinary), &decoded) != nil || decoded.validate() != nil || strings.Contains(ordinary, "socket_generation") {
		t.Fatal("ordinary status contract changed")
	}
	for _, request := range []struct{ method, suffix, body string }{
		{"POST", "", ""}, {"GET", "?", ""}, {"GET", "?page=0", ""}, {"GET", "", "x"},
	} {
		status, body, h := replayDetailHTTP(t, client, request.method, "http://node/details/network"+request.suffix, request.body)
		if status != 400 || !strings.Contains(body, `"reason":"invalid_request"`) || len(body) > 512 || h.Get(replayDetailRemainingHeader) != "" {
			t.Fatalf("strict request accepted: %d %s", status, body)
		}
	}
	if feed.details != nil || feed.network == nil || feed.server.MaxHeaderBytes != 4096 || feed.server.ReadTimeout != 2*time.Second || feed.server.WriteTimeout != 2*time.Second {
		t.Fatal("network option changed worker/server bounds")
	}
	feed.Close()
	// Joining the owner makes the counts stable even if slow CI spans ticks.
	if calls.Load() != statusCalls.Load() {
		t.Fatal("viewers invoked a callback outside accepted status cycles")
	}
	for _, constructor := range []func(string) (*Feed, error){
		func(path string) (*Feed, error) {
			return ListenFeed(context.Background(), path, func() Snapshot { return snapshot })
		},
		func(path string) (*Feed, error) {
			return ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{})
		},
		func(path string) (*Feed, error) {
			return ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot { return snapshot }, nil)
		},
	} {
		path := socketPath(t)
		f, err := constructor(path)
		if err != nil {
			t.Fatal(err)
		}
		client := newCollector(path).client
		status, _, _ := replayDetailHTTP(t, client, "GET", "http://node/details/network", "")
		client.CloseIdleConnections()
		f.Close()
		if status != 404 || f.network != nil {
			t.Fatal("default enabled network details")
		}
	}
}

func maximumReceiverNetworkDetail(at time.Time) NetworkDetail {
	_, c := networkDetailFixture(at)
	wide := c.Detail
	wide.Binding.RunID = strings.Repeat("<", 128)
	genesis := "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4S6DYF4wW"
	wide.Binding.GenesisHash = &genesis
	wide.Binding.NodeStartedAt, wide.Binding.SourceStartedAt, wide.Binding.ReceiverStartedAt, wide.UpdatedAt = at, at, at, at
	m := SocketMemory{math.MaxUint32, math.MaxUint32, math.MaxUint32, math.MaxUint32, math.MaxUint32, math.MaxUint32, math.MaxUint32, math.MaxUint32, math.MaxUint32}
	w := SocketDropWindow{StartedAt: at, SampledAt: at, DurationMilliseconds: 30000, Drops: math.MaxUint32}
	wide.Live = SocketDetail{Enabled: true, State: "available", StartedAt: &at, SampledAt: &at, Memory: &m, DropWindow: &w}
	wide.Live.ReadBytes = &SocketReadBytes{StartedAt: at, SampledAt: at, Bytes: math.MaxUint64,
		Window: &SocketReadInterval{StartedAt: at, SampledAt: at, DurationNanoseconds: uint64(NetworkDetailFreshFor), Bytes: math.MaxUint64}}
	wide.Repair = wide.Live
	wide.Reconstruction = &ReconstructionDetail{Scope: "assembler_session_high_water", Slot: math.MaxUint64, ObservedAt: at}
	wide.ArrivalTiming = &ArrivalTimingDetail{Scope: "retained_adjacent_tvu_arrivals", SampledAt: at, LatestReceiptAt: at,
		RetainedSlots: 750, AdjacentPairs: 749, ReorderedPairs: 748, TotalIntervalNanoseconds: math.MaxInt64}
	return wide
}

func TestNetworkDetailCanonicalAndBounds(t *testing.T) {
	_, c := networkDetailFixture(time.Now())
	body, err := json.Marshal(c.Detail)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeNetworkDetail(body); err != nil {
		t.Fatal(err)
	}
	// All permitted strings, dates and both sets of counters at maximum width
	// still fit without removing any ordinary status or detail fields.
	at := time.Date(9999, 12, 31, 23, 59, 59, 123456789, time.UTC)
	wide := maximumReceiverNetworkDetail(at)
	encoded, err := json.Marshal(wide)
	if err != nil || len(encoded) > MaxNetworkDetailBytes {
		t.Fatalf("maximum-width body: %d %v", len(encoded), err)
	}
	if _, err := decodeNetworkDetail(encoded); err != nil {
		t.Fatal(err)
	}
	t.Logf("all-field maximum-width private body: %d/%d bytes", len(encoded), MaxNetworkDetailBytes)
	family, err := preparePublicNetworkDetail(wide, at, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var public PublicNetworkDetail
	if err := json.Unmarshal([]byte(family.bodies[0]), &public); err != nil || !reflect.DeepEqual(public.Reconstruction, wide.Reconstruction) || !reflect.DeepEqual(public.ArrivalTiming, wide.ArrivalTiming) {
		t.Fatal("public projection changed the original high-water event")
	}
	total := 0
	for _, body := range family.bodies {
		total += len(body)
	}
	if total > networkDetailFamilyBytes {
		t.Fatal("network family exceeded its existing bound")
	}
	t.Logf("all-field maximum-width network public family: %d/%d bytes", total, networkDetailFamilyBytes)
	for _, bad := range [][]byte{
		append([]byte(" "), body...), append(append([]byte{}, body...), []byte("{}")...),
		bytes.Replace(body, []byte(`"schema_version":1`), []byte(`"schema_version":1,"schema_version":1`), 1),
		bytes.Replace(body, []byte(`"family":"network"`), []byte(`"unknown":0,"family":"network"`), 1),
		bytes.Replace(body, []byte(`"reason":"",`), nil, 1),
		bytes.Replace(body, []byte(`"drops":"0"`), []byte(`"drops":null`), 1),
		bytes.Replace(body, []byte(`"drops":"0"`), []byte(`"drops":"00"`), 1),
		bytes.Replace(body, []byte(`"drops":"0"`), []byte(`"drops":"4294967296"`), 1),
		bytes.Replace(body, []byte(`"memory":{`), []byte(`"memory":{"extra":0,`), 1),
		bytes.Repeat([]byte(" "), MaxNetworkDetailBytes+1),
	} {
		if _, err := decodeNetworkDetail(bad); err == nil {
			t.Fatalf("invalid private JSON accepted: %s", bad[:min(len(bad), 100)])
		}
	}
	for _, mutate := range []func(*NetworkDetail){
		func(d *NetworkDetail) { d.Binding.ReceiverSession = 0 },
		func(d *NetworkDetail) { d.Binding.SocketGeneration = 0 },
		func(d *NetworkDetail) { d.Binding.RunID = strings.Repeat("a", 129) },
		func(d *NetworkDetail) { d.Live.Memory = nil },
		func(d *NetworkDetail) { d.Live.State = "read_failed" },
		func(d *NetworkDetail) { d.Repair.Enabled = false },
		func(d *NetworkDetail) { d.Live.DropWindow.DurationMilliseconds = 30001 },
	} {
		_, copy := networkDetailFixture(time.Now())
		mutate(&copy.Detail)
		if validateNetworkDetail(copy.Detail) == nil {
			t.Fatal("invalid source accepted")
		}
	}
}

func TestNetworkDetailPostStartOwnerPublication(t *testing.T) {
	started := time.Now()
	status, original := networkDetailFixture(started)
	var deadline atomic.Pointer[time.Time]
	sampler := func(time.Time) (NetworkDetailCapture, error) {
		// This delay models a bounded cache read racing the separate owner.
		time.Sleep(10 * time.Millisecond)
		_, current := networkDetailFixture(time.Now())
		current.Detail.Binding = original.Detail.Binding
		until := current.FreshUntil
		deadline.Store(&until)
		return current, nil
	}
	source := networkDetailSource{sample: sampler}
	source.update(status, started)
	p := source.latest.Load()
	if p == nil || p.reason != "" || p.body == "" || p.freshUntil != *deadline.Load() {
		t.Fatalf("post-start owner capture rejected or redated: %+v", p)
	}
	path := socketPath(t)
	f, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return status }, DetailSamplers{Network: sampler})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.Close)
	client := newCollector(path).client
	t.Cleanup(client.CloseIdleConnections)
	code, body, _ := replayDetailHTTP(t, client, "GET", "http://node/details/network", "")
	if code != 200 {
		t.Fatalf("real Feed rejected concurrent owner publication: %d %s", code, body)
	}
	f.Close()
	if f.network.latest.Load().freshUntil != *deadline.Load() {
		t.Fatal("Feed renewed source deadline")
	}
}

func TestNetworkDetailSourceReplacementClockAndFailure(t *testing.T) {
	now := time.Now()
	snapshot, capture := networkDetailFixture(now)
	s := &networkDetailSource{sample: func(time.Time) (NetworkDetailCapture, error) { return capture, nil }}
	read := func() (int, string, string) {
		w := httptest.NewRecorder()
		s.serveHTTP(w, httptest.NewRequest("GET", "/details/network", nil))
		return w.Code, w.Body.String(), w.Header().Get(replayDetailRemainingHeader)
	}
	s.update(snapshot, now)
	_, original, _ := read()
	// Caller containers are not retained after publication.
	capture.Detail.Live.Memory.Drops = 99
	_, unchanged, _ := read()
	if unchanged != original {
		t.Fatal("caller mutated published body")
	}
	deadline := s.latest.Load().freshUntil
	s.update(snapshot, time.Now())
	if s.latest.Load().freshUntil != deadline {
		t.Fatal("cache copy renewed owner freshness")
	}
	capture.Detail.Live.State, capture.Detail.Live.Memory, capture.Detail.Live.DropWindow = "read_failed", nil, nil
	s.update(snapshot, time.Now())
	_, body, _ := read()
	if !strings.Contains(body, `"state":"read_failed"`) || strings.Contains(body, `"drops":"99"`) {
		t.Fatal("failure retained old known values")
	}
	capture.Detail.Binding.SocketGeneration--
	s.update(snapshot, time.Now())
	_, body, _ = read()
	if !strings.Contains(body, `"socket_generation":"18446744073709551614"`) {
		t.Fatal("new socket generation not installed")
	}
	changed := snapshot
	changed.RunID = "another-run"
	g := networkGate(changed, time.Now())
	s.gate.Store(&g)
	status, body, _ := read()
	if status != 503 || !strings.Contains(body, "binding_changed") {
		t.Fatal("new status did not fence cached body")
	}
	s.sample = func(time.Time) (NetworkDetailCapture, error) {
		return NetworkDetailCapture{}, errors.New("private path and error")
	}
	s.update(snapshot, time.Now())
	status, body, _ = read()
	if status != 503 || strings.Contains(body, "private") || !strings.Contains(body, "source_unavailable") {
		t.Fatal("raw source error leaked")
	}
	g = networkGate(snapshot, time.Now().Add(-StaleAfter-time.Second))
	s.gate.Store(&g)
	_, body, _ = read()
	if !strings.Contains(body, "status_stale") {
		t.Fatal("older capture failure hid current stale status")
	}
	for _, delta := range []time.Duration{0, -time.Second, 31 * time.Second} {
		s.sample = func(at time.Time) (NetworkDetailCapture, error) {
			_, c := networkDetailFixture(at)
			c.FreshUntil = at.Add(delta)
			return c, nil
		}
		s.update(snapshot, now)
		if s.latest.Load().reason != "source_time_invalid" {
			t.Fatal("invalid monotonic deadline accepted")
		}
	}
}
