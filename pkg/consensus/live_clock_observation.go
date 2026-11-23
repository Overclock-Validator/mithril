package consensus

import (
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
)

// LiveClockObservation is a monitoring-only copy of the existing bounded local
// clock. It is not a cluster tip, replay/finality frontier or readiness signal.
// AddedSlots is the actual difference after both the four-slot and uint64 caps.
type LiveClockObservation struct {
	AnchorSlot, EstimatedSlot, AddedSlots uint64
	AnchorAt, SampledAt                   time.Time
	AnchorAge                             time.Duration
}

// ObserveLiveClock copies the cached anchor without changing the protocol
// getter, anchor or clock. Call only after Start; ordinary viewers use cached API
// bytes, never this method. Disabled monitoring performs no observation work.
func (e *AlpenglowObserverEngine) ObserveLiveClock() (LiveClockObservation, bool) {
	if e == nil || !e.monitoringEnabled {
		return LiveClockObservation{}, false
	}
	e.liveClockMu.Lock()
	slot, at := e.liveClockSlot, e.liveClockAt
	e.liveClockMu.Unlock()
	if slot == 0 || at.IsZero() {
		return LiveClockObservation{}, false
	}
	now := time.Now()
	age := now.Sub(at)
	if age < 0 || age == time.Duration(math.MaxInt64) {
		return LiveClockObservation{}, false
	}
	added := min(uint64(age/alpenglowLiveClockSlotDuration), alpenglow.LeaderWindowSlots, math.MaxUint64-slot)
	return LiveClockObservation{AnchorSlot: slot, EstimatedSlot: slot + added, AddedSlots: added, AnchorAt: at, SampledAt: now, AnchorAge: age}, true
}
