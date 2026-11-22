package replay

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// ReplayDiagnosticNearReception is the first new successful row observed near
// recent authenticated TVU input in one replay context. It is historical input
// proximity, not current readiness, fork agreement or certified catch-up.
type ReplayDiagnosticNearReception struct {
	Slot, Epoch, ReceptionSlot, ReceiverSession uint64
	Bankhash                                    solana.Hash
	CompletedAt, ObservedAt                     time.Time
	SourceStartedAt, ReceiverStartedAt          time.Time
	ReceptionSampledAt, ReceptionObservedAt     time.Time
	RetainedSlots                               int
}

// recordNearReceptionLocked requires the diagnostics lock and a newly accepted
// completed row. Receipt eligibility uses original monotonic clocks; displayed
// wall dates are retained without inventing an elapsed-time ordering.
func (d *replayDiagnostics) recordNearReceptionLocked(b CompletedBlock, sourceStarted time.Time, r blockstream.ReceptionObservation, now time.Time) bool {
	p := r.Progress
	if !d.nearReception.ObservedAt.IsZero() || b.At.IsZero() || b.Bankhash.IsZero() || sourceStarted.IsZero() || r.StartedAt.IsZero() ||
		r.Session == 0 || r.WindowMilliseconds != uint64(turbine.ReceptionWindow/time.Millisecond) || !p.HasSlot || p.CoverageLimited ||
		p.RetainedSlots < 1 || p.RetainedSlots > 64 || p.SampledAt.IsZero() || p.ObservedAt.IsZero() ||
		now == now.Round(0) || p.ValidUntil == p.ValidUntil.Round(0) || !now.Before(p.ValidUntil) {
		return false
	}
	var distance uint64
	if b.Slot >= p.Slot {
		distance = b.Slot - p.Slot
	} else {
		distance = p.Slot - b.Slot
	}
	if distance > 4 {
		return false
	}
	d.nearReception = ReplayDiagnosticNearReception{Slot: b.Slot, Epoch: b.Epoch, Bankhash: b.Bankhash,
		CompletedAt: b.At.UTC(), ObservedAt: now.UTC(), SourceStartedAt: sourceStarted.UTC(), ReceiverStartedAt: r.StartedAt.UTC(),
		ReceiverSession: r.Session, ReceptionSlot: p.Slot, ReceptionSampledAt: p.SampledAt.UTC(), ReceptionObservedAt: p.ObservedAt.UTC(),
		RetainedSlots: p.RetainedSlots}
	return true
}
