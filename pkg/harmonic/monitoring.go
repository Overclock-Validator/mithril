package harmonic

import (
	"math"
	"time"

	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
)

// ServiceObservation contains cumulative local measurements for one constructed
// service. It excludes endpoints, credentials, identities and tip configuration.
// The source reads are a bounded pass, not a transaction across the owners.
type ServiceObservation struct {
	StartedAt, CapturedAt                                time.Time
	RequestedStrategy                                    string
	BlockEngineHealthy, RelayerConfigured, RelayerUsable bool
	RelayerTPUAdvertised, HarmonicClientIDAdvertised     bool
	BlockEngine                                          BlockEngineStats
	Relayer                                              RelayerStats
	Pipeline                                             BlockPipelineStats
	HasLastSlot                                          bool
	LastSlot                                             SlotObservation
}

// SlotObservation is the last completed local executor report, not finalized
// output. It omits the report's free-form abort reason.
type SlotObservation struct {
	ObservedAt                             time.Time
	Slot                                   uint64
	Mode                                   string
	Timeout, Failed, Reverted, Unavailable bool
	Aborted                                bool
	Executed, RevertedBundles, Rejected    uint64
	MaxParallel                            uint32
	Crank                                  string
	HasFirstArrival                        bool
	FirstArrival                           time.Duration
}

func (s *Service) observeStats(stats statsSnapshot, healthy, usable, relayed, advertised bool) {
	if s.cfg.Observe == nil {
		return
	}
	var strategy string
	switch s.blockEngine.Strategy() {
	case blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FIFO:
		strategy = "fifo"
	case blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FBA:
		strategy = "fba"
	case blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_MREV:
		strategy = "mrev"
	default:
		return
	}
	o := ServiceObservation{
		StartedAt: s.observationStartedAt, RequestedStrategy: strategy,
		BlockEngineHealthy: healthy, RelayerConfigured: s.relayer != nil,
		RelayerUsable: usable, RelayerTPUAdvertised: relayed,
		HarmonicClientIDAdvertised: advertised,
		BlockEngine:                stats.blockEngine, Relayer: stats.relayer, Pipeline: stats.pipeline,
	}
	if slot := s.lastSlot.Load(); slot != nil {
		o.HasLastSlot, o.LastSlot = true, *slot
	}
	o.CapturedAt = time.Now()
	s.cfg.Observe(o)
}

func (s *Service) observeSlotReport(r SlotReport) {
	reportSlot(r)
	// Invalid internal values are unknown, never unsigned wraps or labels for
	// another enum. Clear an older report instead of presenting it as this one.
	if r.Mode < ModeUndecided || r.Mode > ModeFallback || r.Crank < CrankNotRun || r.Crank > CrankFailed ||
		r.MaxParallel < 0 || uint64(r.MaxParallel) > math.MaxUint32 || r.Arrived && r.FirstArrival < 0 {
		s.lastSlot.Store(nil)
		return
	}
	o := SlotObservation{
		ObservedAt: time.Now(), Slot: r.Slot, Mode: r.Mode.String(),
		Timeout: r.Flags&FlagTimeout != 0, Failed: r.Flags&FlagFailed != 0,
		Reverted: r.Flags&FlagReverted != 0, Unavailable: r.Flags&FlagUnavailable != 0,
		Aborted: r.Aborted, Executed: r.Executed, RevertedBundles: r.Reverted, Rejected: r.Rejected,
		MaxParallel: uint32(r.MaxParallel), Crank: r.Crank.String(),
		HasFirstArrival: r.Arrived,
	}
	if r.Arrived {
		o.FirstArrival = r.FirstArrival
	}
	s.lastSlot.Store(&o)
}
