package dashboard

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestTPUWorkerPassCanonicalProjection(t *testing.T) {
	now := time.Now()
	_, capture := nodeNetworkSourceFixture(now)
	d := capture.Detail
	d.Receiver, d.Interfaces = nil, nil
	d.ReceiverReason, d.InterfacesReason = "waiting_source", "not_provided"
	d.TPUQueues = &TPUQueuesDetail{Scope: "tpu_pipeline_queue_samples", Generation: 7, StartedAt: now.Add(-10 * time.Second).UTC(), SampledAt: now.UTC(), Ingress: TPUQueueGauge{Capacity: 2}, DedupOutput: TPUQueueGauge{Capacity: 2}, Verified: TPUQueueGauge{Capacity: 2}, SigverifyWorkers: &TPUSigverifyWorkers{Configured: 2}}
	for i := range 2 {
		d.TPUQueues.SigverifyWorkers.Passes = append(d.TPUQueues.SigverifyWorkers.Passes, TPUSigverifyWorkerPass{Index: uint64(i), SampledAt: now.UTC(), ActiveNanoseconds: uint64(i) * uint64(time.Second), ElapsedNanoseconds: uint64(10 * time.Second)})
	}
	body, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeNodeNetworkDetail(body)
	if err != nil || got.TPUQueues.SigverifyWorkers.Passes[0].ActiveNanoseconds != 0 {
		t.Fatal("canonical exact/zero timing rejected", err)
	}
	family, err := preparePublicNodeNetwork(d, now, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var public PublicNodeNetworkDetail
	if json.Unmarshal([]byte(family.bodies[0]+family.tail), &public) != nil || len(public.TPUQueues.SigverifyWorkers.Passes) != 2 {
		t.Fatal("public projection lost passes")
	}
	for name, change := range map[string]func(*TPUSigverifyWorkers){
		"missing_worker":      func(w *TPUSigverifyWorkers) { w.Passes = w.Passes[:1] },
		"index":               func(w *TPUSigverifyWorkers) { w.Passes[1].Index = 0 },
		"sentinel":            func(w *TPUSigverifyWorkers) { w.Passes[0].ElapsedNanoseconds = math.MaxInt64 },
		"zero_elapsed":        func(w *TPUSigverifyWorkers) { w.Passes[0].ElapsedNanoseconds = 0 },
		"active_over_elapsed": func(w *TPUSigverifyWorkers) { w.Passes[0].ActiveNanoseconds = w.Passes[0].ElapsedNanoseconds + 1 },
		"wall_mismatch":       func(w *TPUSigverifyWorkers) { w.Passes[0].ElapsedNanoseconds = uint64(time.Second) },
		"old_child": func(w *TPUSigverifyWorkers) {
			w.Passes[0].SampledAt = now.Add(-2 * time.Second).UTC()
			w.Passes[0].ElapsedNanoseconds = uint64(8 * time.Second)
		},
	} {
		t.Run(name, func(t *testing.T) {
			q := *d.TPUQueues
			w := *q.SigverifyWorkers
			w.Passes = append([]TPUSigverifyWorkerPass(nil), w.Passes...)
			change(&w)
			q.SigverifyWorkers = &w
			bad := d
			bad.TPUQueues = &q
			if validateNodeNetworkDetail(bad) == nil {
				t.Fatal("invalid optional timing admitted")
			}
		})
	}
	w := d.TPUQueues.SigverifyWorkers
	w.Passes = nil
	off, _ := json.Marshal(d)
	if strings.Contains(string(off), `"passes"`) {
		t.Fatal("default-off wire changed")
	}
	if _, err := decodeNodeNetworkDetail(off); err != nil {
		t.Fatal("old absent field rejected", err)
	}
}

func workerPassBudgetFixture(now time.Time) NodeNetworkDetail {
	d := maximumNodeNetwork(now)
	d.Gossip = &GossipOperationDetail{Scope: "gossip_client_totals", ClientGeneration: math.MaxUint64, StartedAt: now.UTC(), SampledAt: now.UTC(),
		RxPings: math.MaxUint64, RxPongs: math.MaxUint64, RxContacts: math.MaxUint64, RxPullRequests: math.MaxUint64,
		TxPushMessages: math.MaxUint64, TxPingMessages: math.MaxUint64, TxPongMessages: math.MaxUint64, TxPullResponses: math.MaxUint64,
		Socket: &GossipSocketDetail{Scope: "gossip_udp_socket", Generation: math.MaxUint64, Observation: d.Receiver.Live}}
	d.Gossip.Socket.Observation.ReadBytes = nil
	d.LiveClock = maximumConsensusClock(now.UTC())
	d.PrewarmHandover = maximumPrewarmHandover(now.UTC())
	return d
}

func TestTPUWorkerPassBudgetPreservesNetwork(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	d := workerPassBudgetFixture(now)
	genesis := d.Binding.GenesisHash
	d.Binding = capture.Detail.Binding
	d.Receiver.Binding = capture.Detail.Receiver.Binding
	d.Binding.GenesisHash, d.Receiver.Binding.GenesisHash, snapshot.GenesisHash = genesis, genesis, genesis
	d.Binding.RunID = strings.Repeat("<", 128)
	d.Receiver.Binding.RunID = d.Binding.RunID
	snapshot.RunID = d.Binding.RunID
	d.TPUQueues = &TPUQueuesDetail{Scope: "tpu_pipeline_queue_samples", Generation: 7, StartedAt: now.Add(-10 * time.Second).UTC(), SampledAt: now.UTC(), Ingress: TPUQueueGauge{Capacity: 2}, DedupOutput: TPUQueueGauge{Capacity: 2}, Verified: TPUQueueGauge{Capacity: 2}, SigverifyWorkers: &TPUSigverifyWorkers{Configured: 64}}
	for i := range 64 {
		d.TPUQueues.SigverifyWorkers.Passes = append(d.TPUQueues.SigverifyWorkers.Passes, TPUSigverifyWorkerPass{Index: uint64(i), SampledAt: now.UTC(), ActiveNanoseconds: uint64(9 * time.Second), ElapsedNanoseconds: uint64(10 * time.Second)})
	}
	if err := validateNodeNetworkDetail(d); err != nil {
		t.Fatal("invalid maximum source fixture", err)
	}
	full, _ := json.Marshal(d)
	trimmed, changed := withoutTPUWorkerPasses(d)
	old, _ := json.Marshal(trimmed)
	if !changed || len(full) <= MaxNetworkDetailBytes || len(old) > MaxNetworkDetailBytes {
		t.Fatalf("fixture does not exercise size boundary: full=%d old=%d", len(full), len(old))
	}
	if len(d.TPUQueues.SigverifyWorkers.Passes) != 64 {
		t.Fatal("fallback mutated original source")
	}
	capture.Detail = d
	capture.FreshUntil = now.Add(time.Second)
	source := &networkDetailSource{nodeSample: func(time.Time) (NodeNetworkCapture, error) { return capture, nil }}
	source.update(snapshot, now)
	publication := source.latest.Load()
	if publication == nil || publication.reason != "" {
		t.Fatal("optional rows suppressed whole private source", publication)
	}
	for _, body := range []string{publication.body, publication.extendedBody, publication.latestBody, publication.gateBody} {
		got, err := decodeNodeNetworkDetail([]byte(body))
		if err != nil || got.Receiver == nil || got.Interfaces == nil || got.TPUQueues == nil || got.TPUQueues.SigverifyWorkers == nil || len(got.TPUQueues.SigverifyWorkers.Passes) != 0 {
			t.Fatal("private variant lost old facts or exceeded cap", err)
		}
	}
	family, err := preparePublicNodeNetwork(d, now, now.Add(time.Second))
	if err != nil {
		t.Fatal("optional rows suppressed whole public family", err)
	}
	for _, prefix := range family.bodies {
		var got PublicNodeNetworkDetail
		if json.Unmarshal([]byte(prefix+family.tail), &got) != nil || got.Receiver == nil || got.Interfaces == nil || got.TPUQueues == nil || len(got.TPUQueues.SigverifyWorkers.Passes) != 0 || len(prefix)+len(family.tail) > MaxNetworkDetailBytes {
			t.Fatal("public variant lost old facts or exceeded cap")
		}
	}
	if len(d.TPUQueues.SigverifyWorkers.Passes) != 64 {
		t.Fatal("public fallback mutated original source")
	}
}

func TestTPUWorkerPassGateBudgetPreservesOldVariants(t *testing.T) {
	fixture := func(now time.Time, workers int) (Snapshot, NodeNetworkDetail) {
		snapshot, capture := nodeNetworkSourceFixture(now)
		d := workerPassBudgetFixture(now)
		genesis := d.Binding.GenesisHash
		d.Binding = capture.Detail.Binding
		d.Receiver.Binding = capture.Detail.Receiver.Binding
		d.Binding.GenesisHash, d.Receiver.Binding.GenesisHash, snapshot.GenesisHash = genesis, genesis, genesis
		d.RepairGate = &RepairGateDetail{SampledAt: now.UTC(), Armed: true}
		d.TPUQueues = &TPUQueuesDetail{Scope: "tpu_pipeline_queue_samples", Generation: 7, StartedAt: now.Add(-10 * time.Second).UTC(), SampledAt: now.UTC(), Ingress: TPUQueueGauge{Capacity: 2}, DedupOutput: TPUQueueGauge{Capacity: 2}, Verified: TPUQueueGauge{Capacity: 2}, SigverifyWorkers: &TPUSigverifyWorkers{Configured: uint64(workers)}}
		for i := range workers {
			d.TPUQueues.SigverifyWorkers.Passes = append(d.TPUQueues.SigverifyWorkers.Passes, TPUSigverifyWorkerPass{Index: uint64(i), SampledAt: now.UTC(), ActiveNanoseconds: uint64(9 * time.Second), ElapsedNanoseconds: uint64(10 * time.Second)})
		}
		return snapshot, d
	}
	setRunID := func(d *NodeNetworkDetail, length int) {
		d.Binding.RunID = strings.Repeat("<", length)
		d.Receiver.Binding.RunID = d.Binding.RunID
	}
	encodedLen := func(d NodeNetworkDetail) int {
		t.Helper()
		b, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		return len(b)
	}
	// Whole-second timestamps give the search and the fresh sample the same
	// encoding size. The search itself does not consume the sample's lease.
	shapeTime := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	workers, runIDLength := 1, 0
	for ; workers <= 64; workers++ {
		_, d := fixture(shapeTime, workers)
		length := 1 + sort.Search(128, func(i int) bool {
			setRunID(&d, i+1)
			return encodedLen(d) > MaxNetworkDetailBytes
		})
		if length > 128 {
			continue
		}
		setRunID(&d, length)
		d.RepairGate = nil
		if encodedLen(d) <= MaxNetworkDetailBytes {
			runIDLength = length
			break
		}
	}
	if runIDLength == 0 {
		t.Fatal("no valid gate-only size boundary found")
	}
	now := time.Now().Truncate(time.Second)
	snapshot, d := fixture(now, workers)
	setRunID(&d, runIDLength)
	withoutGate := d
	withoutGate.RepairGate = nil
	if encodedLen(d) <= MaxNetworkDetailBytes || encodedLen(withoutGate) > MaxNetworkDetailBytes {
		t.Fatal("fresh fixture does not exercise the gate-only size boundary")
	}
	if err := validateNodeNetworkDetail(d); err != nil {
		t.Fatal("invalid gate boundary", err)
	}
	snapshot.RunID = d.Binding.RunID
	capture := NodeNetworkCapture{Detail: d, FreshUntil: now.Add(5 * time.Second)}
	source := &networkDetailSource{nodeSample: func(time.Time) (NodeNetworkCapture, error) { return capture, nil }}
	source.update(snapshot, now)
	publication := source.latest.Load()
	if publication == nil || publication.reason != "" {
		t.Fatal("private boundary unavailable", publication)
	}
	gate, err := decodeNodeNetworkDetail([]byte(publication.gateBody))
	if err != nil || gate.RepairGate == nil || len(gate.TPUQueues.SigverifyWorkers.Passes) != 0 {
		t.Fatal("worker list displaced existing repair gate", err)
	}
	noPass, _ := withoutTPUWorkerPasses(d)
	base, err := preparePublicNodeNetwork(noPass, now, capture.FreshUntil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := preparePublicNodeNetwork(d, now, capture.FreshUntil)
	if err != nil || base.gateTail == "" || got.gateTail != base.gateTail || got.tail != base.tail || got.latestTail != base.latestTail || got.extendedTail != base.extendedTail {
		t.Fatal("public worker list displaced a baseline variant", err)
	}
}
