package replay

import "time"

const slotHistoryLimit = 32

// SlotHistorySummary is the latest bounded selected-and-folded slot window.
// Outcomes is chronological: '?' is unknown, 'p' produced, 's' a proven parent
// gap on that chain. It is not direct certificate finality or local production.
// An immutable string keeps ProgressSnapshot copies detached without a slice.
type SlotHistorySummary struct {
	SampledAt          time.Time
	WindowStart        uint64
	WindowEnd          uint64
	Outcomes           string
	CoverageIncomplete bool
}

func slotHistory(through uint64, carriers []LeaderOutcomeCarrier, predecessor *CertificateFoldEvidence) SlotHistorySummary {
	first, outcomes, incomplete := computeSelectedSlots(through, carriers, predecessor)
	start := first
	if through >= slotHistoryLimit-1 {
		start = through - (slotHistoryLimit - 1)
	}
	var recent [slotHistoryLimit]byte
	length := int(through - start + 1)
	for i := range length {
		outcome := outcomes[start-first+uint64(i)]
		recent[i] = "?ps"[outcome]
		incomplete = incomplete || outcome == 0
	}
	return SlotHistorySummary{WindowStart: start, WindowEnd: through, Outcomes: string(recent[:length]), CoverageIncomplete: incomplete}
}

func (r *CertificateOutcomes) slotSnapshot(through uint64) SlotHistorySummary {
	if r == nil || r.summary.SampledAt.IsZero() {
		return SlotHistorySummary{}
	}
	carriers, predecessor := r.foldedCarriers()
	s := slotHistory(through, carriers, predecessor)
	s.SampledAt = r.summary.SampledAt
	s.CoverageIncomplete = s.CoverageIncomplete || r.coverageLost
	return s
}
