package dashboard

import "time"

// Reception is a bounded recent leader-authenticated TVU-socket observation,
// independently sampled from receiver counters. It is not a certified tip.
type Reception struct {
	SampledAt          time.Time  `json:"sampled_at"`
	StartedAt          time.Time  `json:"started_at"`
	Session            uint64     `json:"session,string"`
	WindowMilliseconds uint64     `json:"window_milliseconds"`
	Slot               *uint64    `json:"slot,string"`
	ObservedAt         *time.Time `json:"observed_at"`
	CoverageLimited    bool       `json:"coverage_limited"`
	RetainedSlots      int        `json:"retained_slots"`
}

// PublicReception distinguishes a recent local input estimate from permanent
// assembler maxima. Receipt time is userspace processing entry, not kernel
// arrival. Missing, stale or capacity-limited evidence never yields a tip.
type PublicReception struct {
	PublicObservation
	StartedAt          time.Time  `json:"started_at"`
	Session            uint64     `json:"session,string"`
	WindowMilliseconds uint64     `json:"window_milliseconds"`
	Slot               *uint64    `json:"slot,string"`
	ObservedAt         *time.Time `json:"observed_at"`
	CoverageLimited    bool       `json:"coverage_limited"`
	RetainedSlots      int        `json:"retained_slots"`
}

func publicReception(s *Snapshot, b *BlockSource, state string, now time.Time) *PublicReception {
	r := b.Reception
	if r == nil || !b.LiveConnected || r.Session == 0 || r.WindowMilliseconds == 0 || r.WindowMilliseconds > 60000 ||
		r.RetainedSlots < 0 || r.RetainedSlots > 64 || b.StartedAt.IsZero() || b.StartedAt.Before(s.StartedAt) ||
		b.SampledAt.Before(b.StartedAt) || b.SampledAt.After(s.SampledAt.Add(time.Second)) ||
		r.StartedAt.Before(b.StartedAt) || r.SampledAt.Before(r.StartedAt) ||
		r.SampledAt.After(b.SampledAt.Add(time.Second)) || !validPublicTime(r.SampledAt, now) ||
		(r.Slot == nil) != (r.ObservedAt == nil) {
		return nil
	}
	window := time.Duration(r.WindowMilliseconds) * time.Millisecond
	if r.Slot != nil && (r.CoverageLimited || r.RetainedSlots == 0 || r.ObservedAt.Before(r.StartedAt) ||
		r.ObservedAt.After(r.SampledAt) || r.SampledAt.Sub(*r.ObservedAt) >= window) {
		return nil
	}
	p := &PublicReception{PublicObservation: publicObservation(r.SampledAt.UTC(), state, now), StartedAt: r.StartedAt.UTC(),
		Session: r.Session, WindowMilliseconds: r.WindowMilliseconds, CoverageLimited: r.CoverageLimited, RetainedSlots: r.RetainedSlots}
	if p.State == "fresh" && now.Sub(r.SampledAt) > 3*time.Second {
		p.State = "stale"
	}
	if p.State == "fresh" && r.Slot != nil && !now.Before(*r.ObservedAt) && now.Sub(*r.ObservedAt) < window {
		slot, at := *r.Slot, r.ObservedAt.UTC()
		p.Slot, p.ObservedAt = &slot, &at
	}
	return p
}
