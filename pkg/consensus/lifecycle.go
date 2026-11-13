package consensus

import (
	"math"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
)

const (
	// LifecycleCapacity bounds retained exact block identities, including forks.
	LifecycleCapacity = 128
	// LifecycleVisibleRows bounds each copied monitoring snapshot.
	LifecycleVisibleRows = 4
)

// LifecycleInterval is a measured local interval. Unknown is not zero elapsed.
type LifecycleInterval struct {
	Duration time.Duration
	Known    bool
}

// LifecycleRecord contains first retained events for one exact block during
// this monitoring session. It does not assert selected-fork status or durability.
// Assembly times describe the completed assembly generation, not exact-block
// first receipt. Zero times and an empty finality kind mean unobserved.
type LifecycleRecord struct {
	Block                   alpenglow.BlockID
	FirstObservedAt         time.Time
	LastObservedAt          time.Time
	HasIngress              bool
	AssemblyFirstAcceptedAt time.Time
	AssemblyFullAt          time.Time
	ReadyAt                 time.Time
	ReplayAcceptedAt        time.Time
	DirectFinalizedAt       time.Time
	FinalityKind            alpenglow.CertificateType
	AssemblyFirstToFull     LifecycleInterval
	FullToReady             LifecycleInterval
	ReadyToReplay           LifecycleInterval
	ReplayToFinality        LifecycleInterval
	FinalityToReplay        LifecycleInterval
	// FinalityBeforeReplay is meaningful only when both event times are known.
	FinalityBeforeReplay bool
	WallOrderConflict    bool
}

// LifecycleSnapshot is a detached, newest-insertion-first bounded history.
// Retained counts all entries, not just Rows[:RowCount]. Eviction means missing
// local history, not absence of a block or certificate. Event times remain
// historical when the reader's current transport becomes stale.
type LifecycleSnapshot struct {
	StartedAt time.Time
	UpdatedAt time.Time
	Retained  int
	Evicted   uint64
	RowCount  int
	Rows      [LifecycleVisibleRows]LifecycleRecord
}

// lifecycleRecorder accepts already validated owner events; it never verifies
// certificates or makes consensus decisions. The owner replaces it on a new
// monitoring session. Nil disables recording without allocation or locking.
type lifecycleRecorder struct {
	// ponytail: one mutex covers at most 128 entries and four copied rows;
	// split publication from writes only if contention is measured.
	mu        sync.Mutex
	startedAt time.Time
	updatedAt time.Time
	entries   [LifecycleCapacity]LifecycleRecord
	index     map[alpenglow.BlockID]int
	next      int
	evicted   uint64
}

func newLifecycleRecorder(startedAt time.Time) *lifecycleRecorder {
	if startedAt.IsZero() {
		return nil
	}
	return &lifecycleRecorder{startedAt: startedAt, index: make(map[alpenglow.BlockID]int, LifecycleCapacity)}
}

// observeIngress retains the first complete trusted timing envelope. Once the
// first replay is recorded, a missing envelope stays missing: a later reassembly
// must not supply timing for an earlier execution of the same block.
func (r *lifecycleRecorder) observeIngress(id alpenglow.BlockID, timing block.TurbineIngressTimings, at time.Time) bool {
	if r == nil || !id.HasHash() || at.IsZero() || timing.AssemblyFirstAcceptedAt.IsZero() || timing.AssemblyFullAt.IsZero() || timing.ReadyAt.IsZero() {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepts(at) || timing.AssemblyFirstAcceptedAt.Before(r.startedAt) || timing.AssemblyFullAt.Before(r.startedAt) || timing.ReadyAt.Before(r.startedAt) ||
		timing.AssemblyFirstAcceptedAt.After(at) || timing.AssemblyFullAt.After(at) || timing.ReadyAt.After(at) {
		return false
	}
	if i, ok := r.index[id]; ok && (r.entries[i].HasIngress || !r.entries[i].ReplayAcceptedAt.IsZero()) {
		return false
	}
	e := r.entry(id, at)
	e.HasIngress = true
	e.AssemblyFirstAcceptedAt = timing.AssemblyFirstAcceptedAt
	e.AssemblyFullAt = timing.AssemblyFullAt
	e.ReadyAt = timing.ReadyAt
	r.touch(e, at)
	return true
}

// observeReplay records a successful callback only when its caller has matched
// the exact block identity. It is independent of the direct-finality arrival order.
func (r *lifecycleRecorder) observeReplay(id alpenglow.BlockID, at time.Time) bool {
	if r == nil || !id.HasHash() || at.IsZero() {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepts(at) {
		return false
	}
	if i, ok := r.index[id]; ok && !r.entries[i].ReplayAcceptedAt.IsZero() {
		return false
	}
	e := r.entry(id, at)
	e.ReplayAcceptedAt = at
	r.touch(e, at)
	return true
}

// observeFinality records a direct decision already accepted by the consensus
// owner. Indirect ancestry and non-finalizing certificate types are not accepted.
func (r *lifecycleRecorder) observeFinality(id alpenglow.BlockID, kind alpenglow.CertificateType, at time.Time) bool {
	if r == nil || !id.HasHash() || at.IsZero() {
		return false
	}
	switch kind {
	case alpenglow.CertificateFinalize, alpenglow.CertificateFinalizeFast, alpenglow.CertificateGenesis:
	default:
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepts(at) {
		return false
	}
	if i, ok := r.index[id]; ok && !r.entries[i].DirectFinalizedAt.IsZero() {
		return false
	}
	e := r.entry(id, at)
	e.DirectFinalizedAt, e.FinalityKind = at, kind
	r.touch(e, at)
	return true
}

// snapshot computes durations before converting only the returned copies to UTC.
func (r *lifecycleRecorder) snapshot() LifecycleSnapshot {
	if r == nil {
		return LifecycleSnapshot{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := LifecycleSnapshot{StartedAt: r.startedAt.UTC(), UpdatedAt: r.updatedAt.UTC(), Retained: len(r.index), Evicted: r.evicted}
	s.RowCount = min(s.Retained, LifecycleVisibleRows)
	for i := 0; i < s.RowCount; i++ {
		e := r.entries[(r.next-1-i+LifecycleCapacity)%LifecycleCapacity]
		e.AssemblyFirstToFull = lifecycleInterval(e.AssemblyFirstAcceptedAt, e.AssemblyFullAt)
		e.FullToReady = lifecycleInterval(e.AssemblyFullAt, e.ReadyAt)
		e.ReadyToReplay = lifecycleInterval(e.ReadyAt, e.ReplayAcceptedAt)
		e.ReplayToFinality = lifecycleInterval(e.ReplayAcceptedAt, e.DirectFinalizedAt)
		if !e.ReplayAcceptedAt.IsZero() && !e.DirectFinalizedAt.IsZero() && e.DirectFinalizedAt.Before(e.ReplayAcceptedAt) {
			e.FinalityBeforeReplay = true
			e.FinalityToReplay = lifecycleInterval(e.DirectFinalizedAt, e.ReplayAcceptedAt)
		}
		e.WallOrderConflict = lifecycleWallReversed(e.AssemblyFirstAcceptedAt, e.AssemblyFullAt) ||
			lifecycleWallReversed(e.AssemblyFullAt, e.ReadyAt) || lifecycleWallReversed(e.ReadyAt, e.ReplayAcceptedAt)
		if !e.ReplayAcceptedAt.IsZero() && !e.DirectFinalizedAt.IsZero() {
			e.WallOrderConflict = e.WallOrderConflict || e.FinalityBeforeReplay != e.DirectFinalizedAt.UTC().Before(e.ReplayAcceptedAt.UTC()) ||
				e.DirectFinalizedAt.Equal(e.ReplayAcceptedAt) != e.DirectFinalizedAt.UTC().Equal(e.ReplayAcceptedAt.UTC())
		}
		e.FirstObservedAt, e.LastObservedAt = e.FirstObservedAt.UTC(), e.LastObservedAt.UTC()
		e.AssemblyFirstAcceptedAt, e.AssemblyFullAt = e.AssemblyFirstAcceptedAt.UTC(), e.AssemblyFullAt.UTC()
		e.ReadyAt, e.ReplayAcceptedAt = e.ReadyAt.UTC(), e.ReplayAcceptedAt.UTC()
		e.DirectFinalizedAt = e.DirectFinalizedAt.UTC()
		s.Rows[i] = e
	}
	return s
}

// accepts requires this recorder's nonzero session boundary. Source event times
// may arrive out of order across owner goroutines; insertion order is separate.
func (r *lifecycleRecorder) accepts(at time.Time) bool {
	return !r.startedAt.IsZero() && !at.Before(r.startedAt)
}

// entry is called with mu held and returns only an internal pointer.
func (r *lifecycleRecorder) entry(id alpenglow.BlockID, at time.Time) *LifecycleRecord {
	if i, ok := r.index[id]; ok {
		return &r.entries[i]
	}
	if len(r.index) == LifecycleCapacity {
		delete(r.index, r.entries[r.next].Block)
		if r.evicted != math.MaxUint64 {
			r.evicted++
		}
	}
	i := r.next
	r.next = (r.next + 1) % LifecycleCapacity
	r.index[id] = i
	r.entries[i] = LifecycleRecord{Block: id, FirstObservedAt: at, LastObservedAt: at}
	return &r.entries[i]
}

func (r *lifecycleRecorder) touch(e *LifecycleRecord, at time.Time) {
	if at.After(e.LastObservedAt) {
		e.LastObservedAt = at
	}
	if at.After(r.updatedAt) {
		r.updatedAt = at
	}
}

func lifecycleInterval(start, end time.Time) LifecycleInterval {
	if start.IsZero() || end.IsZero() {
		return LifecycleInterval{}
	}
	elapsed := end.Sub(start)
	if elapsed < 0 || elapsed == time.Duration(math.MaxInt64) {
		return LifecycleInterval{}
	}
	return LifecycleInterval{Duration: elapsed, Known: true}
}

func lifecycleWallReversed(start, end time.Time) bool {
	return !start.IsZero() && !end.IsZero() && end.UTC().Before(start.UTC())
}
