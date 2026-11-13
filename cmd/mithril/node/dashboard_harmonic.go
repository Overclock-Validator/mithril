package node

import (
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/harmonic"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
)

// harmonicDashboardMonitor belongs to one configured service generation. An
// older service's callback cannot replace the current generation's sample.
type harmonicDashboardMonitor struct {
	enabled    bool
	generation uint64
	strategy   string
	latest     atomic.Pointer[harmonic.ServiceObservation]
}

// configureHarmonic records startup configuration and returns an atomic-only
// callback. Reconfiguration creates a separate generation instead of reusing it.
func (d *nodeDashboard) configureHarmonic(enabled bool, strategy blockenginepb.SchedulingStrategy) func(harmonic.ServiceObservation) {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.harmonic = nil
	if d.state.Mode != "validator" || !d.state.Alpenglow {
		return nil
	}
	m := &harmonicDashboardMonitor{enabled: enabled}
	if enabled {
		switch strategy {
		case blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FIFO:
			m.strategy = "fifo"
		case blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FBA:
			m.strategy = "fba"
		case blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_MREV:
			m.strategy = "mrev"
		default:
			return nil
		}
		if d.harmonicGeneration >= ^uint64(0)-1 {
			return nil
		}
		d.harmonicGeneration++
		m.generation = d.harmonicGeneration
	}
	d.harmonic = m
	if !enabled {
		return nil
	}
	return func(o harmonic.ServiceObservation) { m.latest.Store(&o) }
}

func (m *harmonicDashboardMonitor) snapshot() *dashboard.Harmonic {
	if m == nil {
		return nil
	}
	h := &dashboard.Harmonic{Enabled: m.enabled, Strategy: m.strategy}
	if o := m.latest.Load(); o != nil {
		switch o.RequestedStrategy {
		case "fifo", "fba", "mrev":
		default:
			return nil
		}
		h.Strategy = o.RequestedStrategy
		h.Observation = &dashboard.HarmonicObservation{
			StartedAt: o.StartedAt.UTC(), SampledAt: o.CapturedAt.UTC(),
			BlockEngineHealthy: o.BlockEngineHealthy, RelayerConfigured: o.RelayerConfigured,
			RelayerUsable: o.RelayerUsable, RelayerTPUAdvertised: o.RelayerTPUAdvertised,
			HarmonicClientIDAdvertised: o.HarmonicClientIDAdvertised,
		}
	}
	return h
}
