package node

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod/scheduler"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/tpu"
)

func TestSchedulerBudgetOwnerAndOriginalLease(t *testing.T) {
	now := time.Now()
	genesis := "CktRuQ2mttgRGkXJtyksdKHjUdc2C4TgDzyB98oEzy8"
	d := &nodeDashboard{productionDetails: true, state: dashboard.Snapshot{RunID: "budget-run", StartedAt: now.Add(-time.Hour).UTC(),
		Mode: "validator", Alpenglow: true, GenesisHash: &genesis}}
	m := d.attachProduction()
	m.running()
	service, owner := new(tpu.TPU), new(scheduler.Scheduler)
	d.pipelineStarted(service, owner)
	stats := scheduler.Stats{DroppedBlockCost: math.MaxUint64, DroppedAccountCost: 2, DroppedAllocCost: 3, DroppedBatchBytes: 4}
	stamp := now.Add(-time.Second)
	d.observeSchedulerBudget(service, owner, d.state, stats, stamp)
	first, err := d.sampleProductionDetail(now)
	if err != nil || first.Detail.SchedulerBudget == nil {
		t.Fatalf("missing scheduler budget: %v", err)
	}
	v := first.Detail.SchedulerBudget
	if v.BlockCost != math.MaxUint64 || v.AccountCost != 2 || v.AllocatedData != 3 || v.EntryBytes != 4 ||
		!v.SampledAt.Equal(stamp) || first.FreshUntil != stamp.Add(dashboard.ProductionDetailFreshFor) {
		t.Fatal("mapping or original lease changed")
	}
	v.AccountCost = 900
	second, _ := d.sampleProductionDetail(now)
	if second.Detail.SchedulerBudget.AccountCost != 2 || second.FreshUntil != first.FreshUntil {
		t.Fatal("copy mutated owner or extended lease")
	}
	d.observeSchedulerBudget(service, owner, d.state, scheduler.Stats{}, stamp.Add(-time.Second))
	second, _ = d.sampleProductionDetail(now)
	if second.Detail.SchedulerBudget.AccountCost != 2 || !second.Detail.SchedulerBudget.SampledAt.Equal(stamp) {
		t.Fatal("an older concurrent copy replaced the current sample")
	}
	// A new attached scheduler can legitimately expose lower raw counts.
	replacement := new(scheduler.Scheduler)
	d.pipelineStarted(service, replacement)
	d.observeSchedulerBudget(service, owner, d.state, stats, stamp)
	second, _ = d.sampleProductionDetail(now)
	if second.Detail.SchedulerBudget != nil {
		t.Fatal("retired scheduler relabelled as current")
	}
	d.observeSchedulerBudget(service, replacement, d.state, scheduler.Stats{}, stamp)
	second, _ = d.sampleProductionDetail(now)
	if second.Detail.SchedulerBudget == nil || second.Detail.SchedulerBudget.BlockCost != 0 {
		t.Fatal("replacement's known zero rejected")
	}
	d.pipelineStarted(service, replacement)
	d.observeSchedulerBudget(service, replacement, d.state, stats, now.Add(-31*time.Second))
	second, _ = d.sampleProductionDetail(now)
	if second.Detail.SchedulerBudget != nil {
		t.Fatal("fresh production redated stale scheduler data")
	}
	d.observeSchedulerBudget(service, replacement, d.state, stats, stamp)
	m.stopped()
	d.pipelineStopped()
	second, err = d.sampleProductionDetail(now)
	if err != nil || second.Detail.ProducerState != "stopped" || second.Detail.SchedulerBudget != nil {
		t.Fatal("missing scheduler erased independent stopped producer")
	}
	d.pipelineStarted(service, replacement)
	d.productionDetails = false
	d.observeSchedulerBudget(service, replacement, d.state, stats, stamp)
	if d.schedulerBudget != nil {
		t.Fatal("disabled feature allocated a recorder")
	}
}
