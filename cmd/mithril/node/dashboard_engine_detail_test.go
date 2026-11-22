package node

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/harmonic"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
)

func TestEngineDetailMapping(t *testing.T) {
	now := time.Now()
	o := harmonic.ServiceObservation{StartedAt: now.Add(-time.Minute), CapturedAt: now, RequestedStrategy: "fifo", RelayerConfigured: true,
		HasLastSlot: true, LastSlot: harmonic.SlotObservation{ObservedAt: now.Add(-time.Hour), Slot: math.MaxUint64, Mode: "fallback",
			Timeout: true, Failed: true, Reverted: true, Unavailable: true, Aborted: true, Executed: 11, RevertedBundles: 12,
			Rejected: 13, MaxParallel: math.MaxUint32, Crank: "failed", HasFirstArrival: true}}
	o.BlockEngine.Packets = math.MaxUint64
	d := engineDetailMeasurement(4, o)
	if d.BlockEngine.Packets != math.MaxUint64 {
		t.Fatal("block-engine packet counter changed")
	}
	s := d.LastSlot
	if d.ServiceGeneration != 4 || !d.SampledAt.Equal(o.CapturedAt) || s == nil || !s.ObservedAt.Equal(o.LastSlot.ObservedAt) ||
		s.ExecutedBundles != 11 || s.RevertedBundles != 12 || s.RejectedBundles != 13 || s.FirstArrivalNanoseconds == nil || *s.FirstArrivalNanoseconds != 0 ||
		!s.Timeout || !s.Failed || !s.Reverted || !s.Unavailable || !s.Aborted || s.MaxParallel != math.MaxUint32 {
		t.Fatal("last-slot native report changed")
	}
	o.RelayerConfigured, o.HasLastSlot = false, false
	d = engineDetailMeasurement(4, o)
	if d.Relayer != nil || d.LastSlot != nil {
		t.Fatal("absent owners fabricated zeros")
	}
	o.RequestedStrategy = "https://private.invalid/credential"
	if engineDetailMeasurement(4, o) != nil {
		t.Fatal("unbounded strategy entered an engine detail")
	}
}

func TestEngineDetailGenerationAndLease(t *testing.T) {
	now := time.Now()
	genesis := "CktRuQ2mttgRGkXJtyksdKHjUdc2C4TgDzyB98oEzy8"
	d := &nodeDashboard{productionDetails: true, state: dashboard.Snapshot{RunID: "engine-run", StartedAt: now.Add(-time.Hour).UTC(),
		Mode: "validator", Alpenglow: true, GenesisHash: &genesis}}
	m := d.attachProduction()
	m.running()
	callback := d.configureHarmonic(true, blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FIFO)
	if e, _ := d.harmonic.detail(now, d.state.StartedAt); e != nil {
		t.Fatal("waiting source fabricated counters")
	}
	o := harmonic.ServiceObservation{StartedAt: now.Add(-time.Minute), CapturedAt: now.Add(-time.Second), RequestedStrategy: "fifo"}
	callback(o)
	first, err := d.sampleProductionDetail(now)
	if err != nil || first.Detail.Engine == nil || first.Detail.Engine.ServiceGeneration != 1 || first.FreshUntil != o.CapturedAt.Add(dashboard.ProductionDetailFreshFor) {
		t.Fatalf("service lease/binding: %+v %v", first, err)
	}
	first.Detail.Engine.BlockEngine.Packets = math.MaxUint64
	second, err := d.sampleProductionDetail(now.Add(time.Hour))
	if err != nil || second.Detail.Engine.BlockEngine.Packets != 0 || !second.Detail.Engine.SampledAt.Equal(o.CapturedAt) || second.FreshUntil != first.FreshUntil {
		t.Fatal("copy mutated owner or renewed its lease")
	}
	replacement := d.configureHarmonic(true, blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FIFO)
	callback(o)
	if e, _ := d.harmonic.detail(now, d.state.StartedAt); e != nil {
		t.Fatal("retired callback labelled a new service")
	}
	replacement(o)
	if e, _ := d.harmonic.detail(now, d.state.StartedAt); e == nil || e.ServiceGeneration != 2 {
		t.Fatal("replacement reused the old service generation")
	}
	o.CapturedAt = now.Add(-31 * time.Second)
	replacement(o)
	if e, _ := d.harmonic.detail(now, d.state.StartedAt); e != nil {
		t.Fatal("expired service sample remained current")
	}
	if c, err := d.sampleProductionDetail(now); err != nil || c.Detail.Engine != nil {
		t.Fatal("unavailable engine removed independent producer data")
	}
}
