package accountsdb

import (
	"math"
	"sync/atomic"
	"time"
)

// WriteStallObservation describes the latest intentional Pebble write delay.
// InitialReason is memtable, l0 or other; conditions may change during a stall.
// An ended stall is release from pressure, not a successful-write receipt.
type WriteStallObservation struct {
	HasEvent      bool
	Active        bool
	InitialReason string
	BeganAt       time.Time
	EndedAt       time.Time
	Duration      time.Duration
	DurationKnown bool
}

// StorageObservation belongs to the two stores from one successful runtime
// open. It excludes snapshot ingest/format-upgrade handles and cache lifetime.
// The two store copies are coherent individually, not a database transaction.
type StorageObservation struct {
	OpenedAt time.Time
	Index    WriteStallObservation
	BankHash WriteStallObservation
}

type writeStallRecord struct {
	beganAt, endedAt time.Time
	duration         time.Duration
	reason           uint8
	state            uint8
	durationKnown    bool
}

const (
	writeStallIdle uint8 = iota
	writeStallActive
	writeStallEnded
	writeStallRetired
)

var retiredWriteStall = &writeStallRecord{state: writeStallRetired}

// writeStallRecorder has one serialized Pebble callback writer. Retirement may
// race a callback; a single CAS refuses that obsolete publication without retry.
type writeStallRecorder struct {
	latest atomic.Pointer[writeStallRecord]
}

func newWriteStallRecorder() *writeStallRecorder {
	r := &writeStallRecorder{}
	r.latest.Store(&writeStallRecord{})
	return r
}

func (r *writeStallRecorder) begin(at time.Time, reason string) {
	if r == nil || at.IsZero() {
		return
	}
	old := r.latest.Load()
	if old == nil || old.state == writeStallActive || old.state == writeStallRetired {
		return
	}
	code := uint8(3)
	switch reason {
	case "memtable count limit reached":
		code = 1
	case "L0 file count limit exceeded":
		code = 2
	}
	r.latest.CompareAndSwap(old, &writeStallRecord{state: writeStallActive, reason: code, beganAt: at})
}

func (r *writeStallRecorder) end(at time.Time) {
	if r == nil || at.IsZero() {
		return
	}
	old := r.latest.Load()
	if old == nil || old.state != writeStallActive {
		return
	}
	next := *old
	next.state, next.endedAt = writeStallEnded, at
	duration := at.Sub(old.beganAt)
	if duration >= 0 && duration < time.Duration(math.MaxInt64) {
		next.duration, next.durationKnown = duration, true
	}
	r.latest.CompareAndSwap(old, &next)
}

func (r *writeStallRecorder) snapshot() (WriteStallObservation, bool) {
	if r == nil {
		return WriteStallObservation{}, false
	}
	v := r.latest.Load()
	if v == nil || v.state == writeStallRetired {
		return WriteStallObservation{}, false
	}
	o := WriteStallObservation{HasEvent: v.state != writeStallIdle, Active: v.state == writeStallActive,
		BeganAt: v.beganAt, EndedAt: v.endedAt, Duration: v.duration, DurationKnown: v.durationKnown}
	if v.state != writeStallIdle {
		o.InitialReason = [...]string{"", "memtable", "l0", "other"}[v.reason]
	}
	return o, true
}

func (r *writeStallRecorder) retire() {
	if r != nil {
		r.latest.Store(retiredWriteStall)
	}
}

// StorageObservation copies fixed scalar records without clocks, database calls,
// scans, locks or resetting counters. Source recording uses existing always-on
// stall callbacks; disclosure is separately opt-in. Closing owners are unknown.
func (db *AccountsDb) StorageObservation() (StorageObservation, bool) {
	if db == nil || db.storageOpenedAt.IsZero() || db.storageClosing.Load() {
		return StorageObservation{}, false
	}
	index, indexOK := db.indexWriteStalls.snapshot()
	bankHash, bankHashOK := db.bankHashWriteStalls.snapshot()
	if !indexOK || !bankHashOK || db.storageClosing.Load() {
		return StorageObservation{}, false
	}
	return StorageObservation{OpenedAt: db.storageOpenedAt, Index: index, BankHash: bankHash}, true
}
