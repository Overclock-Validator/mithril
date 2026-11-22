package node

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

// repairGateObservation is the sampled local catch-up gate, not evidence that
// repair packets are flowing, that a range is complete, or that replay is ready.
type repairGateObservation struct {
	SampledAt time.Time
	Armed     bool
}

func repairGateMeasurement(s blockstream.NetworkSnapshot, binding dashboard.NetworkDetailBinding, now time.Time) (repairGateObservation, bool) {
	if s.ConfiguredSource != "turbine" || !s.LiveConnected || s.Turbine == nil || s.Turbine.Session == 0 ||
		!s.SourceStartedAt.Equal(binding.SourceStartedAt) || !s.Turbine.StartedAt.Equal(binding.ReceiverStartedAt) ||
		s.Turbine.Session != binding.ReceiverSession || s.SourceStartedAt.IsZero() || s.Turbine.StartedAt.IsZero() || s.SampledAt.IsZero() ||
		s.SampledAt.Before(s.SourceStartedAt) || s.SampledAt.Before(s.Turbine.StartedAt) || now.Before(s.SampledAt) || now.Sub(s.SampledAt) >= dashboard.NetworkDetailFreshFor {
		return repairGateObservation{}, false
	}
	return repairGateObservation{SampledAt: s.SampledAt.UTC(), Armed: s.RepairCatchupFrom != 0}, true
}
