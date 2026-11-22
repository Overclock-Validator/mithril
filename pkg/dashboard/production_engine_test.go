package dashboard

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func maximumEngineDetail(at time.Time) *ProductionEngineDetail {
	d := &ProductionEngineDetail{Scope: "harmonic_service_instance", ServiceGeneration: math.MaxUint64 - 1, StartedAt: at, SampledAt: at,
		Strategy: "mrev", Relayer: &EngineRelayerDetail{}, LastSlot: &EngineSlotDetail{ObservedAt: at, Slot: math.MaxUint64,
			Mode: "undecided", Timeout: true, Failed: true, Reverted: true, Unavailable: true, Aborted: true,
			ExecutedBundles: math.MaxUint64, RevertedBundles: math.MaxUint64, RejectedBundles: math.MaxUint64, MaxParallel: math.MaxUint32, Crank: "not_needed"}}
	for _, counts := range []any{&d.BlockEngine, d.Relayer, &d.BundlePipeline} {
		v := reflect.ValueOf(counts).Elem()
		for i := 0; i < v.NumField(); i++ {
			v.Field(i).SetUint(math.MaxUint64)
		}
	}
	arrival := uint64(math.MaxInt64 - 1)
	d.LastSlot.FirstArrivalNanoseconds = &arrival
	return d
}

func TestProductionEngineContractAndBounds(t *testing.T) {
	d := productionDetailFixture()
	d.Binding.RunID = strings.Repeat("<", 128)
	d.Binding.ProducerGeneration = math.MaxUint64 - 1
	at := time.Date(9999, 12, 31, 23, 59, 59, 123456789, time.UTC)
	d.Binding.NodeStartedAt, d.Binding.ProducerStartedAt, d.CapturedAt, d.LastTerminal.ObservedAt = at, at, at, at
	d.LastTerminal.Slot, d.LastTerminal.Bank.ParentSlot = math.MaxUint64, math.MaxUint64
	for _, counts := range []any{d.LastTerminal.Costs, &d.LastTerminal.Costs.Limits} {
		v := reflect.ValueOf(counts).Elem()
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).Kind() == reflect.Uint64 {
				v.Field(i).SetUint(math.MaxUint64)
			}
		}
	}
	d.Engine = maximumEngineDetail(d.CapturedAt)
	d.LastTerminal.Finalization = maximumProductionFinalization()
	d.LastTerminal.Finalization.WindowCompletion = maximumProductionWindowCompletion(d.LastTerminal.Slot, at)
	d.SchedulerBudget = &ProductionSchedulerDetail{Scope: "attached_scheduler_snapshot", SampledAt: d.CapturedAt,
		BlockCost: math.MaxUint64, AccountCost: math.MaxUint64, AllocatedData: math.MaxUint64, EntryBytes: math.MaxUint64}
	d.ParentReady = &ProductionParentReadyDetail{WindowStartSlot: math.MaxUint64 - 3, ParentSlot: math.MaxUint64 - 4,
		ParentBlockID: d.Binding.GenesisHash, ReadyAt: at, ObservedAt: at, Activation: "handover"}
	// False has the longer JSON representation for all unconstrained flags.
	d.Engine.LastSlot.Timeout, d.Engine.LastSlot.Failed, d.Engine.LastSlot.Reverted = false, false, false
	d.Engine.LastSlot.Unavailable, d.Engine.LastSlot.Aborted = false, false
	body, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeProductionDetail(body)
	if err != nil || !reflect.DeepEqual(got, d) {
		t.Fatalf("engine detail round trip: %v", err)
	}
	family, err := preparePublicProductionDetail(d, d.CapturedAt, d.CapturedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var public PublicProductionDetail
	if json.Unmarshal([]byte(family.bodies[0]), &public) != nil || !reflect.DeepEqual(public.Engine, d.Engine) || !reflect.DeepEqual(public.SchedulerBudget, d.SchedulerBudget) ||
		!reflect.DeepEqual(public.LastTerminal.Finalization, d.LastTerminal.Finalization) || !reflect.DeepEqual(public.ParentReady, d.ParentReady) {
		t.Fatal("public projection changed native service facts")
	}
	var total int
	for _, b := range family.bodies {
		total += len(b)
		if len(b) > MaxProductionDetailBytes {
			t.Fatal("complete public timing variant exceeded cap")
		}
		var projected PublicProductionDetail
		if json.Unmarshal([]byte(b), &projected) != nil || !reflect.DeepEqual(projected.LastTerminal.Finalization, d.LastTerminal.Finalization) {
			t.Fatal("stale variant lost timing fields")
		}
	}
	if MaxProductionDetailBytes != 6144 || productionDetailFamilyBytes != 60<<10 {
		t.Fatal("production profile bounds changed without paired tests")
	}
	t.Logf("production with engine and scheduler: %d/%d bytes private; %d/%d cached public bytes", len(body), MaxProductionDetailBytes, total, productionDetailFamilyBytes)
	for _, mutate := range []func(*ProductionEngineDetail){
		func(e *ProductionEngineDetail) { e.ServiceGeneration = 0 }, func(e *ProductionEngineDetail) { e.ServiceGeneration = math.MaxUint64 },
		func(e *ProductionEngineDetail) { e.Strategy = "private-url" }, func(e *ProductionEngineDetail) { e.LastSlot.Mode = "done" },
		func(e *ProductionEngineDetail) { e.LastSlot.Crank = "arbitrary-reason" }, func(e *ProductionEngineDetail) { e.SampledAt = e.SampledAt.Add(-31 * time.Second) },
		func(e *ProductionEngineDetail) { e.StartedAt = e.StartedAt.Add(2 * time.Second) },
	} {
		e := maximumEngineDetail(d.CapturedAt)
		mutate(e)
		if validProductionEngine(e, d.CapturedAt) {
			t.Fatal("invalid engine shape admitted")
		}
	}
}

func TestProductionEngineOriginalLease(t *testing.T) {
	d := productionDetailFixture()
	d.Engine = maximumEngineDetail(d.CapturedAt)
	d.Engine.SampledAt = d.CapturedAt.Add(-20 * time.Second)
	c := ProductionDetailCapture{Detail: d, FreshUntil: d.CapturedAt.Add(10 * time.Second)}
	if productionDetailTimeInvalid(c, d.CapturedAt) {
		t.Fatal("original service lease rejected")
	}
	c.FreshUntil = c.FreshUntil.Add(time.Nanosecond)
	if !productionDetailTimeInvalid(c, d.CapturedAt) {
		t.Fatal("document refresh extended the engine sample lease")
	}
	c.FreshUntil = d.CapturedAt.Add(time.Second)
	c.Detail.Engine.SampledAt = d.CapturedAt.Add(-30 * time.Second)
	if !productionDetailTimeInvalid(c, d.CapturedAt) {
		t.Fatal("expired service counters accepted")
	}
}
