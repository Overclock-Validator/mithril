package node

import (
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/harmonic"
)

func (m *harmonicDashboardMonitor) detail(now, nodeStartedAt time.Time) (*dashboard.ProductionEngineDetail, time.Time) {
	if m == nil || !m.enabled || m.generation == 0 || m.generation == math.MaxUint64 {
		return nil, time.Time{}
	}
	o := m.latest.Load()
	if o == nil || o.StartedAt.IsZero() || o.StartedAt.Before(nodeStartedAt) || o.CapturedAt.Before(o.StartedAt) ||
		now.Before(o.CapturedAt) || !o.CapturedAt.Add(dashboard.ProductionDetailFreshFor).After(now) {
		return nil, time.Time{}
	}
	d := engineDetailMeasurement(m.generation, *o)
	return d, o.CapturedAt.Add(dashboard.ProductionDetailFreshFor)
}

// engineDetailMeasurement copies only fixed counters and known classifications.
// Endpoint/authentication data and the executor's free-form reason stay private.
func engineDetailMeasurement(generation uint64, o harmonic.ServiceObservation) *dashboard.ProductionEngineDetail {
	if generation == 0 || generation == math.MaxUint64 || o.RequestedStrategy != "fifo" && o.RequestedStrategy != "fba" && o.RequestedStrategy != "mrev" {
		return nil
	}
	if o.HasLastSlot && (o.LastSlot.Mode != "undecided" && o.LastSlot.Mode != "harmonic" && o.LastSlot.Mode != "fallback" ||
		o.LastSlot.Crank != "not_run" && o.LastSlot.Crank != "not_needed" && o.LastSlot.Crank != "landed" && o.LastSlot.Crank != "failed") {
		return nil
	}
	be, p := o.BlockEngine, o.Pipeline
	d := &dashboard.ProductionEngineDetail{Scope: "harmonic_service_instance", ServiceGeneration: generation,
		StartedAt: o.StartedAt.UTC(), SampledAt: o.CapturedAt.UTC(), Strategy: o.RequestedStrategy,
		BlockEngine: dashboard.EngineSessionDetail{Sessions: be.Sessions, Resets: be.Resets, Packets: be.Packets,
			DroppedSize: be.DroppedSize, DroppedFull: be.DroppedFull, Blocks: be.Blocks,
			NoticesSent: be.NoticesSent, NoticesExpired: be.NoticesExpired, NoticeErrors: be.NoticeErrors,
			StreamsLost: be.StreamsLost, StrategyErrors: be.StrategyErrors, InvalidFeeInfo: be.InvalidFeeInfo},
		BundlePipeline: dashboard.EngineBundleDetail{Messages: p.Messages, WrongSlot: p.WrongSlot,
			LostSlot: p.LostSlot, Bundles: p.Bundles, InvalidBundles: p.InvalidBundles, Transactions: p.Transactions}}
	if o.RelayerConfigured {
		r := o.Relayer
		d.Relayer = &dashboard.EngineRelayerDetail{Sessions: r.Sessions, Resets: r.Resets, Heartbeats: r.Heartbeats,
			Batches: r.Batches, Packets: r.Packets, DroppedSize: r.DroppedSize, DroppedFull: r.DroppedFull, InvalidTPUConfigs: r.InvalidTPUConfigs}
	}
	if o.HasLastSlot {
		s := o.LastSlot
		d.LastSlot = &dashboard.EngineSlotDetail{ObservedAt: s.ObservedAt.UTC(), Slot: s.Slot, Mode: s.Mode,
			Timeout: s.Timeout, Failed: s.Failed, Reverted: s.Reverted, Unavailable: s.Unavailable, Aborted: s.Aborted,
			ExecutedBundles: s.Executed, RevertedBundles: s.RevertedBundles, RejectedBundles: s.Rejected,
			MaxParallel: s.MaxParallel, Crank: s.Crank}
		if s.HasFirstArrival && s.FirstArrival >= 0 && s.FirstArrival < time.Duration(math.MaxInt64) {
			value := uint64(s.FirstArrival)
			d.LastSlot.FirstArrivalNanoseconds = &value
		}
	}
	return d
}
