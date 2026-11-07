package node

import (
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/gagliardetto/solana-go"
)

func validateDashboardProductionDetails(enabled bool, socket string) error {
	if enabled && strings.TrimSpace(socket) == "" {
		return errors.New("dashboard.production_details requires dashboard.socket")
	}
	return nil
}

type productionDashboardView struct {
	startedAt   time.Time
	state       string
	last        *blockprod.LeaderTerminalObservation
	parentReady *blockprod.LeaderParentReadyObservation
}

// One producer Run goroutine owns running, observe and stopped in that order.
// A retired callback writes only its own monitor, never a replacement instance.
type productionDashboardMonitor struct {
	binding dashboard.ProductionDetailBinding
	view    atomic.Pointer[productionDashboardView]
}

func (d *nodeDashboard) attachProduction() *productionDashboardMonitor {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.production = nil
	if !d.productionDetails || d.state.Mode != "validator" || !d.state.Alpenglow ||
		d.state.RunID == "" || d.state.StartedAt.IsZero() || d.state.GenesisHash == nil ||
		d.productionGeneration == ^uint64(0) {
		return nil
	}
	if len(*d.state.GenesisHash) < 32 || len(*d.state.GenesisHash) > 44 {
		return nil
	}
	genesis, err := solana.HashFromBase58(*d.state.GenesisHash)
	if err != nil || genesis == (solana.Hash{}) || genesis.String() != *d.state.GenesisHash {
		return nil
	}
	d.productionGeneration++
	if d.productionGeneration == ^uint64(0) {
		return nil
	}
	m := &productionDashboardMonitor{binding: dashboard.ProductionDetailBinding{
		RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt.UTC(), GenesisHash: *d.state.GenesisHash,
		ProducerGeneration: d.productionGeneration,
	}}
	d.production = m
	return m
}

func (m *productionDashboardMonitor) running() {
	if m == nil || m.view.Load() != nil {
		return
	}
	m.view.Store(&productionDashboardView{startedAt: time.Now(), state: "running"})
}

func (m *productionDashboardMonitor) observe(o blockprod.LeaderTerminalObservation) {
	if m == nil {
		return
	}
	previous := m.view.Load()
	if previous == nil || previous.state != "running" {
		return
	}
	m.view.Store(&productionDashboardView{startedAt: previous.startedAt, state: "running", last: &o, parentReady: previous.parentReady})
}

func (m *productionDashboardMonitor) observeParentReady(o blockprod.LeaderParentReadyObservation) {
	if m == nil {
		return
	}
	previous := m.view.Load()
	if previous == nil || previous.state != "running" {
		return
	}
	m.view.Store(&productionDashboardView{startedAt: previous.startedAt, state: "running", last: previous.last, parentReady: &o})
}

func (m *productionDashboardMonitor) stopped() {
	if m == nil {
		return
	}
	previous := m.view.Load()
	if previous != nil {
		m.view.Store(&productionDashboardView{startedAt: previous.startedAt, state: "stopped", last: previous.last, parentReady: previous.parentReady})
	}
}

func productionMatchesNode(s dashboard.Snapshot, b dashboard.ProductionDetailBinding) bool {
	return s.Mode == "validator" && s.Alpenglow && s.RunID == b.RunID && s.StartedAt.Equal(b.NodeStartedAt) &&
		s.GenesisHash != nil && *s.GenesisHash == b.GenesisHash
}

func (d *nodeDashboard) sampleProductionDetail(_ time.Time) (dashboard.ProductionDetailCapture, error) {
	if d == nil {
		return dashboard.ProductionDetailCapture{}, errors.New("source_unavailable")
	}
	d.mu.Lock()
	applicable, enabled := d.state.Mode == "validator" && d.state.Alpenglow, d.productionDetails
	m := d.production
	harmonicMonitor := d.harmonic
	matched := m != nil && productionMatchesNode(d.state, m.binding)
	d.mu.Unlock()
	if !enabled {
		return dashboard.ProductionDetailCapture{}, errors.New("source_unavailable")
	}
	if !applicable {
		return dashboard.ProductionDetailCapture{}, errors.New("not_applicable")
	}
	if m == nil {
		return dashboard.ProductionDetailCapture{}, errors.New("waiting_source")
	}
	if !matched {
		return dashboard.ProductionDetailCapture{}, errors.New("binding_changed")
	}
	view := m.view.Load()
	if view == nil {
		return dashboard.ProductionDetailCapture{}, errors.New("waiting_source")
	}
	// Completion follows the atomic copy; the earlier Feed tick is not its date.
	capturedAt := time.Now()
	binding := m.binding
	binding.ProducerStartedAt = view.startedAt.UTC()
	detail := dashboard.ProductionDetail{SchemaVersion: 1, Family: "production", Scope: "producer_session_latest_terminal",
		State: "fresh", Binding: binding, CapturedAt: capturedAt.UTC(), ProducerState: view.state}
	if p := view.parentReady; p != nil {
		detail.ParentReady = &dashboard.ProductionParentReadyDetail{WindowStartSlot: p.WindowStartSlot,
			ParentSlot: p.ParentSlot, ParentBlockID: p.ParentBlockID.String(), ReadyAt: p.ReadyAt.UTC(),
			ObservedAt: p.ObservedAt.UTC(), Activation: p.Activation}
	}
	if view.last != nil {
		o := *view.last
		terminal := &dashboard.ProductionTerminalDetail{Slot: o.Slot, ObservedAt: o.SampledAt.UTC(),
			Outcome: o.Outcome, Reason: o.Reason, Terminal: o.Terminal, Cause: o.Cause}
		if o.BankOpened {
			terminal.Bank = &dashboard.ProductionBankDetail{Leader: o.BankLeader.String(),
				ParentSlot: o.ParentSlot, ParentBankhash: o.ParentBankhash.String()}
		}
		if o.CostsKnown {
			if !o.BankOpened || o.Outcome != "broadcast" {
				return dashboard.ProductionDetailCapture{}, errors.New("source_invalid")
			}
			c := o.Costs
			terminal.Costs = &dashboard.ProductionCostDetail{BlockCost: c.BlockCost, AllocatedDataSizeDeltaBytes: c.AllocatedDataSizeDelta,
				SerializedEntryBytes: c.EntryBytes, Transactions: c.Transactions, Limits: dashboard.ProductionCostLimits{
					BlockCost: c.Limits.BlockCost, WritableAccountCost: c.Limits.WritableAccountCost, AllocatedDataSizeDeltaBytes: c.Limits.AllocatedDataSizeDelta,
					MaxBatchBytes: c.Limits.MaxBatchBytes, MaxEntryBytes: c.Limits.MaxEntryBytes,
				}}
		}
		if o.FinalizationKnown {
			var err error
			terminal.Finalization, err = productionFinalizationMeasurement(o.Finalization)
			if err != nil || !o.CostsKnown || !o.BankOpened || o.Outcome != "broadcast" {
				return dashboard.ProductionDetailCapture{}, errors.New("source_invalid")
			}
		}
		detail.LastTerminal = terminal
	}
	freshUntil := capturedAt.Add(dashboard.ProductionDetailFreshFor)
	if engine, deadline := harmonicMonitor.detail(capturedAt, binding.NodeStartedAt); engine != nil {
		detail.Engine = engine
		if deadline.Before(freshUntil) {
			freshUntil = deadline
		}
	}
	d.mu.Lock()
	matched = d.production == m && d.harmonic == harmonicMonitor && d.productionDetails && productionMatchesNode(d.state, binding)
	if matched {
		if o := d.schedulerBudgetFor(binding, capturedAt); o != nil {
			value := o.value
			detail.SchedulerBudget = &value
			if deadline := o.sampledAt.Add(dashboard.ProductionDetailFreshFor); deadline.Before(freshUntil) {
				freshUntil = deadline
			}
		}
	}
	d.mu.Unlock()
	detail.CapturedAt = time.Now().UTC()
	if !matched {
		return dashboard.ProductionDetailCapture{}, errors.New("binding_changed")
	}
	return dashboard.ProductionDetailCapture{Detail: detail, FreshUntil: freshUntil}, nil
}
