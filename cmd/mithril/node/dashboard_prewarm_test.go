package node

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/gagliardetto/solana-go"
)

func prewarmNodeFixture() *nodeDashboard {
	genesis := (solana.Hash{8}).String()
	return &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: "prewarm-run",
		StartedAt: time.Now().Add(-time.Hour).UTC(), GenesisHash: &genesis, Phase: "bootstrap", Mode: "verifying"}}
}

func prewarmNodeEvent() blockstream.TurbinePrewarmHandoverObservation {
	return blockstream.TurbinePrewarmHandoverObservation{HandedOverAt: time.Now().Add(-48 * time.Hour),
		FloorSlot: 7, Capacity: 2, ReturnedBlocks: 2, OverflowEvictions: 9, FirstSlot: 7, LastSlot: 10,
		RepairKnown: true, Repair: blockstream.TurbinePrewarmRepairObservation{SampledAt: time.Now().Add(-47 * time.Hour),
			Requests: 3, Responses: 2, LateResponses: 1, Timeouts: 2}}
}

func TestDashboardPrewarmHistoricalIndependentOwner(t *testing.T) {
	d := prewarmNodeFixture()
	observe := d.prewarmHandoverObserver()
	if observe == nil || d.prewarmHandover.capture(d.prewarmHandover.binding) != nil {
		t.Fatal("valid pending prewarm was absent or fabricated a handover")
	}
	o := prewarmNodeEvent()
	want := o
	observe(o)
	o.FirstSlot, o.Repair.Requests = 9, 99
	before := time.Now()
	first, err := d.sampleNodeNetwork(time.Time{})
	if err != nil || first.Detail.Receiver != nil || first.Detail.PrewarmHandover == nil ||
		first.Detail.PrewarmHandover.ReturnedBlocks != 2 || *first.Detail.PrewarmHandover.FirstSlot != 7 ||
		first.Detail.PrewarmHandover.Repair.Requests != 3 || !first.Detail.PrewarmHandover.HandedOverAt.Equal(want.HandedOverAt) ||
		!first.Detail.PrewarmHandover.Repair.SampledAt.Equal(want.Repair.SampledAt) ||
		first.FreshUntil.Before(before) || first.FreshUntil.After(time.Now().Add(dashboard.NetworkDetailFreshFor)) {
		t.Fatalf("historical standalone handover changed: %+v, %v", first, err)
	}
	copy := *first.Detail.PrewarmHandover
	first.Detail.PrewarmHandover.Repair.Requests = 500
	*first.Detail.PrewarmHandover.FirstSlot = 100
	observe(o)
	second, err := d.sampleNodeNetwork(time.Now().Add(24 * time.Hour))
	if err != nil || second.Detail.PrewarmHandover.Repair.Requests != 3 || *second.Detail.PrewarmHandover.FirstSlot != 7 ||
		second.Detail.PrewarmHandover.HandedOverAt != copy.HandedOverAt {
		t.Fatal("caller, repeated callback, or supplied Feed clock mutated the source fact")
	}
	retired := d.prewarmHandoverObserver()
	replacement := d.prewarmHandoverObserver()
	retired(want)
	if d.prewarmHandover.capture(d.prewarmHandover.binding) != nil {
		t.Fatal("retired callback populated a replacement")
	}
	want.ReturnedBlocks, want.FirstSlot, want.LastSlot = 0, 0, 0
	want.RepairKnown, want.Repair = false, blockstream.TurbinePrewarmRepairObservation{}
	replacement(want)
	zero, err := d.sampleNodeNetwork(time.Now())
	if err != nil || zero.Detail.PrewarmHandover.ReturnedBlocks != 0 || zero.Detail.PrewarmHandover.FirstSlot != nil ||
		zero.Detail.PrewarmHandover.LastSlot != nil || zero.Detail.PrewarmHandover.Repair != nil {
		t.Fatal("observed empty input invented bounds or repair counters")
	}
}

func TestDashboardPrewarmAttachmentAndValidation(t *testing.T) {
	var absent *nodeDashboard
	if absent.prewarmHandoverObserver() != nil {
		t.Fatal("nil dashboard attached prewarm")
	}
	var m *prewarmHandoverMonitor
	m.observe(prewarmNodeEvent())
	if m.capture(dashboard.NodeNetworkBinding{}) != nil {
		t.Fatal("nil monitor returned data")
	}
	for _, change := range []func(*nodeDashboard){
		func(d *nodeDashboard) { d.networkDetails = false }, func(d *nodeDashboard) { d.networkRetired = true },
		func(d *nodeDashboard) { d.state.Phase = "stopping" }, func(d *nodeDashboard) { d.state.RunID = "" },
		func(d *nodeDashboard) { d.state.StartedAt = time.Time{} }, func(d *nodeDashboard) { d.state.GenesisHash = nil },
		func(d *nodeDashboard) { hash := (solana.Hash{}).String(); d.state.GenesisHash = &hash },
		func(d *nodeDashboard) { d.prewarmGeneration = math.MaxUint64 - 1 },
		func(d *nodeDashboard) { d.prewarmGeneration = math.MaxUint64 },
	} {
		d := prewarmNodeFixture()
		change(d)
		if d.prewarmHandoverObserver() != nil || d.prewarmHandover != nil {
			t.Fatal("disabled, retired, exhausted or unbound owner attached")
		}
	}
	for _, mutate := range []func(*blockstream.TurbinePrewarmHandoverObservation){
		func(o *blockstream.TurbinePrewarmHandoverObservation) { o.HandedOverAt = time.Time{} },
		func(o *blockstream.TurbinePrewarmHandoverObservation) { o.Capacity = 0 },
		func(o *blockstream.TurbinePrewarmHandoverObservation) { o.ReturnedBlocks = 3 },
		func(o *blockstream.TurbinePrewarmHandoverObservation) { o.ReturnedBlocks = 1 },
		func(o *blockstream.TurbinePrewarmHandoverObservation) { o.FirstSlot = 6 },
		func(o *blockstream.TurbinePrewarmHandoverObservation) { o.LastSlot = 7 },
		func(o *blockstream.TurbinePrewarmHandoverObservation) { o.Repair.SampledAt = time.Time{} },
		func(o *blockstream.TurbinePrewarmHandoverObservation) { o.RepairKnown = false },
	} {
		d := prewarmNodeFixture()
		observe := d.prewarmHandoverObserver()
		o := prewarmNodeEvent()
		mutate(&o)
		observe(o)
		if d.prewarmHandover.latest.Load() != nil {
			t.Fatal("malformed input became a known handover")
		}
	}
	d := prewarmNodeFixture()
	observe := d.prewarmHandoverObserver()
	observe(prewarmNodeEvent())
	binding := d.prewarmHandover.binding
	for _, mutate := range []func(*dashboard.NodeNetworkBinding){
		func(b *dashboard.NodeNetworkBinding) { b.RunID = "other" },
		func(b *dashboard.NodeNetworkBinding) { b.NodeStartedAt = b.NodeStartedAt.Add(time.Second) },
		func(b *dashboard.NodeNetworkBinding) { b.GenesisHash = nil },
	} {
		wrong := binding
		mutate(&wrong)
		if d.prewarmHandover.capture(wrong) != nil || !reflect.DeepEqual(binding, d.prewarmHandover.binding) {
			t.Fatal("wrong context exposed or mutated historical input")
		}
	}
}
