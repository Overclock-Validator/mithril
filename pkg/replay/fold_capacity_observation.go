package replay

import (
	"math"
	"time"
)

// ReplayDiagnosticFoldCapacity is the latest capacity-branch entry/return, not
// a disk-health or finality verdict. It is independent of completed-row cadence.
type ReplayDiagnosticFoldCapacity struct {
	State, Outcome        string
	Slot                  uint64
	StartedAt, ReturnedAt time.Time
	HadInFlightFold       bool
	DurationKnown         bool
	DurationNanoseconds   uint64
}

type foldCapacityObservation struct {
	owner                *replayDiagnostics
	generation, sequence uint64
	startedAt            time.Time
}

// beginFoldCapacityObservation reuses the replay branch's existing clock.
// Disabled diagnostics add neither a clock read nor an allocation.
func (p *Progress) beginFoldCapacityObservation(slot uint64, hadInFlight bool, startedAt time.Time) foldCapacityObservation {
	if p == nil {
		return foldCapacityObservation{}
	}
	d := p.diagnostics.Load()
	if d == nil {
		return foldCapacityObservation{}
	}
	s, ok := p.Snapshot()
	if !ok || slot == 0 || startedAt.IsZero() {
		return foldCapacityObservation{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.exhausted || d.generation == 0 || d.attempt != s.Attempt || d.rewinds != s.Rewinds {
		return foldCapacityObservation{}
	}
	if d.foldCapacitySequence == math.MaxUint64 {
		d.foldCapacity = ReplayDiagnosticFoldCapacity{}
		return foldCapacityObservation{}
	}
	d.foldCapacitySequence++
	d.foldCapacity = ReplayDiagnosticFoldCapacity{State: "active", Slot: slot, StartedAt: startedAt, HadInFlightFold: hadInFlight}
	return foldCapacityObservation{owner: d, generation: d.generation, sequence: d.foldCapacitySequence, startedAt: startedAt}
}

func (o foldCapacityObservation) finish(stillOverCap, failed bool) {
	if o.owner == nil {
		return
	}
	returnedAt := time.Now()
	d := o.owner
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.exhausted || d.generation != o.generation || d.foldCapacitySequence != o.sequence || d.foldCapacity.State != "active" {
		return
	}
	v := d.foldCapacity
	v.State, v.ReturnedAt, v.Outcome = "returned", returnedAt, "relieved"
	if failed {
		v.Outcome = "failed"
	} else if stillOverCap {
		v.Outcome = "still_over_capacity"
	}
	if elapsed := returnedAt.Sub(o.startedAt); elapsed >= 0 && elapsed < math.MaxInt64 {
		v.DurationKnown, v.DurationNanoseconds = true, uint64(elapsed)
	}
	d.foldCapacity = v
}
