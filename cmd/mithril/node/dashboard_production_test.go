package node

import (
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/gagliardetto/solana-go"
)

func productionNodeFixture() *nodeDashboard {
	genesis := (solana.Hash{9}).String()
	now := time.Now().UTC()
	return &nodeDashboard{productionDetails: true, state: dashboard.Snapshot{
		SchemaVersion: dashboard.SchemaVersion, RunID: "production-owner-run", SampledAt: now, StartedAt: now.Add(-time.Hour), GenesisHash: &genesis,
		Mode: "validator", Alpenglow: true, Phase: "replaying", Version: "test", Commit: "test", Network: "isolated",
	}}
}

func productionNodeEvent(slot uint64) blockprod.LeaderTerminalObservation {
	return blockprod.LeaderTerminalObservation{Slot: slot, SampledAt: time.Now(),
		Outcome: "broadcast", Reason: "complete", Terminal: "complete", Cause: "complete",
		BankOpened: true, BankLeader: solana.PublicKey{7}, ParentSlot: slot - 1, ParentBankhash: solana.Hash{8},
		CostsKnown: true, Costs: blockprod.LeaderBankCosts{BlockCost: slot, Transactions: slot, Limits: costmodel.DefaultLimits()},
	}
}

func requireProductionSourceError(t *testing.T, d *nodeDashboard, reason string) {
	t.Helper()
	capture, err := d.sampleProductionDetail(time.Now())
	if err == nil || err.Error() != reason || !reflect.DeepEqual(capture, dashboard.ProductionDetailCapture{}) {
		t.Fatalf("want empty %s refusal, got %+v, %v", reason, capture, err)
	}
}

func TestDashboardProductionAttachmentAndOptIn(t *testing.T) {
	var absent *nodeDashboard
	if absent.attachProduction() != nil {
		t.Fatal("nil dashboard attached production")
	}
	requireProductionSourceError(t, absent, "source_unavailable")
	var absentMonitor *productionDashboardMonitor
	absentMonitor.running()
	absentMonitor.observe(productionNodeEvent(1))
	absentMonitor.stopped()
	for name, change := range map[string]func(*nodeDashboard){
		"disabled":       func(d *nodeDashboard) { d.productionDetails = false },
		"verifying":      func(d *nodeDashboard) { d.state.Mode = "verifying" },
		"classic":        func(d *nodeDashboard) { d.state.Alpenglow = false },
		"no_run":         func(d *nodeDashboard) { d.state.RunID = "" },
		"no_start":       func(d *nodeDashboard) { d.state.StartedAt = time.Time{} },
		"unbound_chain":  func(d *nodeDashboard) { d.state.GenesisHash = nil },
		"zero_genesis":   func(d *nodeDashboard) { *d.state.GenesisHash = strings.Repeat("1", 32) },
		"bad_genesis":    func(d *nodeDashboard) { *d.state.GenesisHash = "private error" },
		"wide_genesis":   func(d *nodeDashboard) { *d.state.GenesisHash = strings.Repeat("z", 44) },
		"generation_end": func(d *nodeDashboard) { d.productionGeneration = math.MaxUint64 },
	} {
		t.Run(name, func(t *testing.T) {
			d := productionNodeFixture()
			change(d)
			if d.attachProduction() != nil || d.production != nil {
				t.Fatal("inapplicable or unbound node attached a producer")
			}
			want := "waiting_source"
			if !d.productionDetails {
				want = "source_unavailable"
			} else if d.state.Mode != "validator" || !d.state.Alpenglow {
				want = "not_applicable"
			}
			requireProductionSourceError(t, d, want)
		})
	}
	d := productionNodeFixture()
	d.productionGeneration = math.MaxUint64 - 2
	last := d.attachProduction()
	if last == nil || last.binding.ProducerGeneration != math.MaxUint64-1 {
		t.Fatal("last nonexhausted generation was not preserved")
	}
	if d.attachProduction() != nil || d.production != nil || d.productionGeneration != math.MaxUint64 || d.attachProduction() != nil {
		t.Fatal("exhausted generation wrapped or retained the retired producer")
	}
	if validateDashboardProductionDetails(false, "") != nil || validateDashboardProductionDetails(true, "/private/node.sock") != nil {
		t.Fatal("valid production detail option rejected")
	}
	for _, socket := range []string{"", " \t\n"} {
		if validateDashboardProductionDetails(true, socket) == nil {
			t.Fatal("production details enabled without a private socket")
		}
	}
	flag := Run.Flags().Lookup("dashboard-production-details")
	if flag == nil || flag.DefValue != "false" {
		t.Fatal("production detail flag is not default off")
	}
}

func TestDashboardProductionLifecycleAndCaptureClock(t *testing.T) {
	d := productionNodeFixture()
	m := d.attachProduction()
	if m == nil {
		t.Fatal("valid producer not attached")
	}
	requireProductionSourceError(t, d, "waiting_source")
	m.observe(productionNodeEvent(1))
	if m.view.Load() != nil {
		t.Fatal("a callback before Run fabricated a running producer")
	}
	beforeStart := time.Now()
	m.running()
	afterStart := time.Now()
	view := m.view.Load()
	if view == nil || view.startedAt.Before(beforeStart) || view.startedAt.After(afterStart) || view.startedAt == view.startedAt.Round(0) {
		t.Fatal("Run wrapper start was not the original monotonic observation")
	}
	empty, err := d.sampleProductionDetail(time.Time{})
	if err != nil || empty.Detail.ProducerState != "running" || empty.Detail.LastTerminal != nil ||
		!empty.Detail.Binding.ProducerStartedAt.Equal(view.startedAt) {
		t.Fatalf("running without a terminal is not a failure: %+v, %v", empty, err)
	}
	o := productionNodeEvent(42)
	o.SampledAt = time.Now().Add(-48 * time.Hour)
	wantEventAt := o.SampledAt.UTC()
	m.observe(o)
	o.SampledAt, o.Costs.BlockCost, o.Costs.Limits.BlockCost = time.Time{}, 0, 0
	stored := m.view.Load()
	var previousCapture time.Time
	for _, supplied := range []time.Time{time.Time{}, time.Now().Add(-time.Hour), time.Now().Add(24 * time.Hour)} {
		before := time.Now()
		capture, err := d.sampleProductionDetail(supplied)
		after := time.Now()
		if err != nil {
			t.Fatal(err)
		}
		at := capture.Detail.CapturedAt
		leaseBase := capture.FreshUntil.Add(-dashboard.ProductionDetailFreshFor)
		// Projection completion can follow its conservative monotonic budget
		// start. Neither may use the supplied Feed clock or extend the budget.
		if at.Before(before.UTC()) || at.After(after.UTC()) || at.Before(previousCapture) ||
			leaseBase.Before(before) || leaseBase.After(after) || leaseBase.After(at) || !capture.FreshUntil.After(after) ||
			capture.FreshUntil == capture.FreshUntil.Round(0) {
			t.Fatalf("capture/lease clock contract: before=%s captured=%s lease_base=%s after=%s previous=%s monotonic_lease=%t", before.UTC(), at,
				capture.FreshUntil.Add(-dashboard.ProductionDetailFreshFor).UTC(), after.UTC(), previousCapture, capture.FreshUntil != capture.FreshUntil.Round(0))
		}
		previousCapture = at
		if capture.Detail.LastTerminal == nil || capture.Detail.LastTerminal.ObservedAt != wantEventAt ||
			capture.Detail.LastTerminal.Costs.BlockCost != 42 || m.view.Load() != stored {
			t.Fatal("copy redated a historical terminal, called the producer, or changed its owner cache")
		}
	}
	m.stopped()
	stopped := m.view.Load()
	m.observe(productionNodeEvent(43))
	m.running()
	capture, err := d.sampleProductionDetail(time.Now())
	if err != nil || capture.Detail.ProducerState != "stopped" || capture.Detail.LastTerminal.Slot != 42 ||
		capture.Detail.LastTerminal.ObservedAt != wantEventAt || m.view.Load() != stopped {
		t.Fatal("stopped producer lost history or a late callback/restart revived it")
	}
}

func TestDashboardProductionParentReadyLifetime(t *testing.T) {
	d := productionNodeFixture()
	m := d.attachProduction()
	var absent *productionDashboardMonitor
	absent.observeParentReady(blockprod.LeaderParentReadyObservation{})
	o := blockprod.LeaderParentReadyObservation{WindowStartSlot: 40, ParentSlot: 39, ParentBlockID: solana.Hash{4},
		ReadyAt: time.Now().Add(-48 * time.Hour), ObservedAt: time.Now().Add(-49 * time.Hour), Activation: "initial"}
	m.observeParentReady(o)
	if m.view.Load() != nil {
		t.Fatal("pre-Run notification fabricated a producer")
	}
	m.running()
	m.observeParentReady(o)
	want := *m.view.Load().parentReady
	o.ParentSlot, o.Activation = 1, "handover"
	first, err := d.sampleProductionDetail(time.Now())
	if err != nil || first.Detail.LastTerminal != nil || first.Detail.ParentReady == nil ||
		first.Detail.ParentReady.ParentSlot != want.ParentSlot || first.Detail.ParentReady.Activation != want.Activation ||
		!first.Detail.ParentReady.ObservedAt.Equal(want.ObservedAt) || !first.Detail.ParentReady.ReadyAt.Equal(want.ReadyAt) {
		t.Fatalf("detached original parent notification changed: %+v, %v", first, err)
	}
	first.Detail.ParentReady.ParentSlot = 0
	m.observe(productionNodeEvent(42))
	m.stopped()
	m.observeParentReady(o)
	stopped, err := d.sampleProductionDetail(time.Now())
	if err != nil || stopped.Detail.ProducerState != "stopped" || stopped.Detail.LastTerminal.Slot != 42 ||
		stopped.Detail.ParentReady.ParentSlot != want.ParentSlot {
		t.Fatal("terminal/caller/late callback changed retained parent notification")
	}
	replacement := d.attachProduction()
	replacement.running()
	m.observeParentReady(o)
	reset, err := d.sampleProductionDetail(time.Now())
	if err != nil || reset.Detail.ParentReady != nil || reset.Detail.LastTerminal != nil {
		t.Fatal("replacement inherited a retired producer's notification")
	}
}

func TestDashboardProductionReplacementAndContext(t *testing.T) {
	d := productionNodeFixture()
	old := d.attachProduction()
	old.running()
	old.observe(productionNodeEvent(1))
	replacement := d.attachProduction()
	if replacement == nil || replacement == old || replacement.binding.ProducerGeneration != old.binding.ProducerGeneration+1 || replacement.view.Load() != nil {
		t.Fatal("replacement reused producer history or generation")
	}
	old.observe(productionNodeEvent(2))
	requireProductionSourceError(t, d, "waiting_source")
	replacement.running()
	old.stopped()
	capture, err := d.sampleProductionDetail(time.Now())
	if err != nil || capture.Detail.LastTerminal != nil || capture.Detail.ProducerState != "running" ||
		capture.Detail.Binding.ProducerGeneration != replacement.binding.ProducerGeneration {
		t.Fatal("retired callback populated the replacement")
	}
	for _, field := range []string{"run", "start", "genesis", "missing_genesis", "mode", "classic", "disabled"} {
		d.mu.Lock()
		previous, enabled := d.state, d.productionDetails
		want := "binding_changed"
		switch field {
		case "run":
			d.state.RunID = "replacement-run"
		case "start":
			d.state.StartedAt = d.state.StartedAt.Add(time.Second)
		case "genesis":
			genesis := (solana.Hash{10}).String()
			d.state.GenesisHash = &genesis
		case "missing_genesis":
			d.state.GenesisHash = nil
		case "mode":
			d.state.Mode, want = "verifying", "not_applicable"
		case "classic":
			d.state.Alpenglow, want = false, "not_applicable"
		case "disabled":
			d.productionDetails, want = false, "source_unavailable"
		}
		d.mu.Unlock()
		requireProductionSourceError(t, d, want)
		d.mu.Lock()
		d.state, d.productionDetails = previous, enabled
		d.mu.Unlock()
	}
}

func TestDashboardProductionConcurrentSingleWriterCopies(t *testing.T) {
	d := productionNodeFixture()
	m := d.attachProduction()
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		m.running()
		for slot := uint64(1); slot <= 100; slot++ {
			m.observe(productionNodeEvent(slot))
			runtime.Gosched()
		}
		m.stopped()
	}()
	defer writer.Wait()
	for range 100 {
		capture, err := d.sampleProductionDetail(time.Now().Add(-time.Hour))
		if err != nil {
			if err.Error() != "waiting_source" {
				t.Fatal(err)
			}
			continue
		}
		if event := capture.Detail.LastTerminal; event != nil {
			if event.ObservedAt.After(capture.Detail.CapturedAt) || event.Bank.ParentSlot != event.Slot-1 ||
				event.Costs.BlockCost != event.Slot || event.Costs.Transactions != event.Slot {
				t.Fatal("concurrent atomic copy mixed events or used the earlier Feed time")
			}
			event.Costs.Limits.BlockCost = 0
		}
		runtime.Gosched()
	}
	writer.Wait()
	final, err := d.sampleProductionDetail(time.Now())
	if err != nil || final.Detail.ProducerState != "stopped" || final.Detail.LastTerminal.Slot != 100 || final.Detail.LastTerminal.Costs.Limits.BlockCost == 0 {
		t.Fatal("joined single producer lost its final immutable event")
	}
}
