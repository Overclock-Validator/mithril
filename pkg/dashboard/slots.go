package dashboard

import "time"

// SlotHistory is an attempt-local summary of selected-and-folded slots.
// Outcomes is chronological, with '?' unknown, 'p' produced and 's' skipped.
type SlotHistory struct {
	SampledAt          time.Time `json:"sampled_at"`
	WindowStart        uint64    `json:"window_start,string"`
	WindowEnd          uint64    `json:"window_end,string"`
	Outcomes           string    `json:"outcomes"`
	CoverageIncomplete bool      `json:"coverage_incomplete"`
}

// PublicSlotHistory describes at most 32 consecutive settled slots. A skip
// means an exact parent gap on the selected chain, not a local leader failure.
type PublicSlotHistory struct {
	PublicObservation
	Provenance         string       `json:"provenance"`
	WindowStart        uint64       `json:"window_start,string"`
	WindowEnd          uint64       `json:"window_end,string"`
	Slots              []PublicSlot `json:"slots"`
	CoverageIncomplete bool         `json:"coverage_incomplete"`
}

// PublicSlot preserves unknown separately from produced and selected-fork skipped.
type PublicSlot struct {
	Slot    uint64 `json:"slot,string"`
	Outcome string `json:"outcome"`
}

func publicSlotHistory(s *Snapshot, state string, now time.Time) *PublicSlotHistory {
	r := s.Replay
	if !s.Alpenglow || r == nil || r.SlotHistory == nil {
		return nil
	}
	h := r.SlotHistory
	if !validPublicTime(r.StartedAt, now) || r.StartedAt.Before(s.StartedAt) || !validPublicTime(h.SampledAt, now) || h.SampledAt.Before(r.StartedAt) || h.SampledAt.After(s.SampledAt.Add(time.Second)) ||
		r.DurableSlot == nil || h.WindowEnd > *r.DurableSlot || h.WindowEnd < h.WindowStart || h.WindowEnd-h.WindowStart >= 32 ||
		len(h.Outcomes) != int(h.WindowEnd-h.WindowStart+1) {
		return nil
	}
	p := &PublicSlotHistory{PublicObservation: publicObservation(h.SampledAt, state, now), Provenance: "selected_and_folded",
		WindowStart: h.WindowStart, WindowEnd: h.WindowEnd, CoverageIncomplete: h.CoverageIncomplete,
		Slots: make([]PublicSlot, len(h.Outcomes))}
	for i := range len(h.Outcomes) {
		var outcome string
		switch h.Outcomes[i] {
		case '?':
			outcome, p.CoverageIncomplete = "unknown", true
		case 'p':
			outcome = "produced"
		case 's':
			outcome = "selected_fork_skipped"
		default:
			return nil
		}
		p.Slots[i] = PublicSlot{Slot: h.WindowStart + uint64(i), Outcome: outcome}
	}
	return p
}
