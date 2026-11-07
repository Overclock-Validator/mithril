package turbine

import (
	"sync"
	"time"
)

// ReceptionWindow bounds how long a socket observation remains recent.
const ReceptionWindow = 4800 * time.Millisecond

// ReceptionSnapshot describes recent leader-authenticated TVU-socket observations
// after the existing admission path. Receipt time is processPacket entry time,
// not kernel arrival time. It does not establish reconstruction or finality.
type ReceptionSnapshot struct {
	SampledAt       time.Time
	Slot            uint64
	ObservedAt      time.Time
	HasSlot         bool
	CoverageLimited bool
	RetainedSlots   int
	// ValidUntil is an original same-process monotonic eligibility deadline.
	// It is never serialized or reconstructed from the public UTC dates.
	ValidUntil time.Time `json:"-"`
	// ArrivalTiming is a retained adjacent-slot arrival aggregate, not a tip or
	// block-duration estimate. Its clocks are original process readings.
	ArrivalTiming ArrivalTimingSnapshot `json:"-"`
}

// ArrivalTimingSnapshot aggregates first accepted TVU receipts in 750 tagged
// buckets. Numeric gaps are excluded; reordered adjacent intervals count as
// zero. Zero AdjacentPairs means unavailable, not a measured zero interval.
type ArrivalTimingSnapshot struct {
	SampledAt       time.Time
	LatestReceiptAt time.Time
	RetainedSlots   int
	AdjacentPairs   int
	ReorderedPairs  int
	TotalInterval   time.Duration
}

type receptionObservation struct {
	slot uint64
	at   time.Time
}

type receptionMonitoring struct {
	// ponytail: one lock and at most 64-entry scans; index expiry only if profiling warrants it.
	mu           sync.Mutex
	slots        [64]receptionObservation
	count        int
	watermark    time.Time
	limitedUntil time.Time
	arrivals     [750]receptionObservation
}

// EnableReceptionMonitoring enables the bounded recent-reception observer.
// It is idempotent and must be called before Run or other receiver operations.
func (r *UDPReceiver) EnableReceptionMonitoring() {
	if r != nil && r.reception == nil {
		r.reception = new(receptionMonitoring)
	}
}

// ReceptionSnapshot samples recent reception, expiring observations after 4.8 s
// even when no packets arrive. The boolean is false when monitoring is disabled.
// HasSlot is false while capacity loss prevents a complete recent-slot maximum.
func (r *UDPReceiver) ReceptionSnapshot() (ReceptionSnapshot, bool) {
	if r == nil || r.reception == nil {
		return ReceptionSnapshot{}, false
	}
	m := r.reception
	m.mu.Lock()
	now := time.Now()
	s := m.snapshotLocked(now)
	arrivals := m.arrivals
	clockValid := !now.Before(m.watermark)
	m.mu.Unlock()
	if clockValid {
		s.ArrivalTiming = arrivalTiming(&arrivals, now)
	}
	return s, true
}

func (m *receptionMonitoring) observe(slot uint64, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.advanceLocked(at)
	if !at.After(m.watermark.Add(-ReceptionWindow)) {
		return
	}
	entry := &m.arrivals[slot%uint64(len(m.arrivals))]
	if entry.at.IsZero() || entry.slot != slot {
		*entry = receptionObservation{slot: slot, at: at}
	}
	for i := 0; i < m.count; i++ {
		if m.slots[i].slot == slot {
			if at.After(m.slots[i].at) {
				m.slots[i].at = at
			}
			return
		}
	}
	if m.count == len(m.slots) {
		if until := at.Add(ReceptionWindow); until.After(m.limitedUntil) {
			m.limitedUntil = until
		}
		return
	}
	m.slots[m.count] = receptionObservation{slot: slot, at: at}
	m.count++
}

// advanceLocked keeps a monotonic watermark so delayed processing cannot revive
// evidence already expired by a newer packet or sample. Preserve monotonic clock
// readings internally; only exported timestamps are converted to UTC.
func (m *receptionMonitoring) advanceLocked(at time.Time) {
	if !at.After(m.watermark) {
		return
	}
	m.watermark = at
	cutoff := at.Add(-ReceptionWindow)
	n := 0
	for i := 0; i < m.count; i++ {
		if m.slots[i].at.After(cutoff) {
			m.slots[n] = m.slots[i]
			n++
		}
	}
	m.count = n
}

// snapshotLocked requires m.mu. A reversed sample is unknown, not a reset.
func (m *receptionMonitoring) snapshotLocked(now time.Time) ReceptionSnapshot {
	s := ReceptionSnapshot{SampledAt: now.UTC(), CoverageLimited: now.Before(m.limitedUntil)}
	if now.Before(m.watermark) {
		s.RetainedSlots = m.count
		return s
	}
	m.advanceLocked(now)
	s.RetainedSlots = m.count
	if s.CoverageLimited {
		return s
	}
	var observed time.Time
	for i := 0; i < m.count; i++ {
		if !s.HasSlot || m.slots[i].slot > s.Slot {
			s.HasSlot = true
			s.Slot = m.slots[i].slot
			s.ObservedAt = m.slots[i].at.UTC()
			observed = m.slots[i].at
		}
	}
	if s.HasSlot && now != now.Round(0) && observed != observed.Round(0) {
		s.ValidUntil = now.Add(3 * time.Second)
		if expiry := observed.Add(ReceptionWindow); expiry.Before(s.ValidUntil) {
			s.ValidUntil = expiry
		}
	}
	return s
}

// arrivalTiming runs on the existing sampler, never in the packet critical
// section. The retained tags can contain gaps and different forks.
func arrivalTiming(entries *[750]receptionObservation, now time.Time) ArrivalTimingSnapshot {
	var s ArrivalTimingSnapshot
	if now.IsZero() || now == now.Round(0) {
		return s
	}
	for _, entry := range entries {
		if entry.at.IsZero() {
			continue
		}
		if entry.at == entry.at.Round(0) || entry.at.After(now) {
			return ArrivalTimingSnapshot{}
		}
		s.RetainedSlots++
		if entry.at.After(s.LatestReceiptAt) {
			s.LatestReceiptAt = entry.at
		}
		if entry.slot == ^uint64(0) {
			continue
		}
		next := entries[(entry.slot+1)%uint64(len(entries))]
		if next.at.IsZero() || next.slot != entry.slot+1 {
			continue
		}
		interval := next.at.Sub(entry.at)
		if interval < 0 {
			s.ReorderedPairs++
			interval = 0
		}
		if interval > time.Duration(1<<63-1)-s.TotalInterval {
			return ArrivalTimingSnapshot{}
		}
		s.TotalInterval += interval
		s.AdjacentPairs++
	}
	if s.AdjacentPairs == 0 {
		return ArrivalTimingSnapshot{}
	}
	s.SampledAt = now
	return s
}
