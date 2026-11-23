package replay

import (
	"sort"
	"time"

	"github.com/gagliardetto/solana-go"
)

const leaderOutcomeWindowSlots = 512

// LeaderOutcomeCarrier is an actual selected-and-folded produced block, not a
// received candidate or provisional skip. CertificateFoldEvidence must identify
// its exact committed layer. ParentSlot is the explicit source claim (the
// producer's ParentSlot for local blocks); ExecutionParent* describes the bank
// actually used to execute/adopt it. Missing parent evidence stays unknown.
type LeaderOutcomeCarrier struct {
	CertificateFoldEvidence
	ParentSlot, ExecutionParentSlot uint64
	ParentBlockID                   solana.Hash
	HasParentBlockID                bool
	ExecutionParentBankhash         solana.Hash
}

// LeaderOutcomeSummary describes at most the last 512 slots through a known
// durable slot. Counts concern assignments of Identity, not local forging or
// globally certified skips. CoveredStart/End is the envelope of slots with both
// known schedule and ancestry; holes are reported by the unknown counters.
type LeaderOutcomeSummary struct {
	SampledAt                                       time.Time
	Provenance                                      string
	Identity                                        solana.PublicKey
	HasWindow                                       bool
	WindowStart, WindowEnd                          uint64
	HasCoveredRange                                 bool
	CoveredStart, CoveredEnd                        uint64
	Produced, SelectedForkSkipped, ScheduledUnknown uint64
	UnknownScheduleSlots, UnknownAncestrySlots      uint64
	CoverageIncomplete                              bool
}

type leaderOutcomeRecord struct {
	carrier     LeaderOutcomeCarrier
	hasAncestry bool
	conflict    bool
}

// ComputeLeaderOutcomes consumes at most 512 retained committed records and an
// optional exact preceding committed anchor. The caller must omit failed,
// rejected and rewound speculative records and supply the actual durable end.
// Include every retained folded produced layer, even with missing identity or
// parent metadata: such a layer is an unknown barrier, not an omitted skip.
// Records need not be ordered. Duplicate identical records are harmless;
// conflicting records and oversized input fail closed to unknown coverage.
//
// schedule must use a single captured immutable schedule view for each epoch
// covering the window, with false for unavailable slots. Do not pass a mutable
// per-call global lookup or fetch/build schedules here. The function performs
// at most 512 lookups, independent of the size of any parent-child gap.
func ComputeLeaderOutcomes(identity solana.PublicKey, through uint64, carriers []LeaderOutcomeCarrier, predecessor *CertificateFoldEvidence, schedule func(uint64) (solana.PublicKey, bool)) LeaderOutcomeSummary {
	first, outcomes, incomplete := computeSelectedSlots(through, carriers, predecessor)
	s := LeaderOutcomeSummary{Identity: identity, HasWindow: true, WindowStart: first, WindowEnd: through,
		CoverageIncomplete: incomplete || identity.IsZero()}
	length := int(through - first + 1)
	for offset := range length {
		slot := first + uint64(offset)
		outcome := outcomes[offset]
		if outcome == 0 {
			s.UnknownAncestrySlots++
		}
		var leader solana.PublicKey
		known := false
		if schedule != nil {
			leader, known = schedule(slot)
		}
		if !known || leader.IsZero() {
			s.UnknownScheduleSlots++
			continue
		}
		if outcome != 0 {
			if !s.HasCoveredRange {
				s.CoveredStart, s.HasCoveredRange = slot, true
			}
			s.CoveredEnd = slot
		}
		if leader != identity || identity.IsZero() {
			continue
		}
		switch outcome {
		case 1:
			s.Produced++
		case 2:
			s.SelectedForkSkipped++
		default:
			s.ScheduledUnknown++
		}
	}
	s.CoverageIncomplete = s.CoverageIncomplete || s.UnknownScheduleSlots != 0 || s.UnknownAncestrySlots != 0
	return s
}

// computeSelectedSlots is shared by the timeline and leader accounting. Values
// are 0 unknown, 1 produced and 2 skipped on the selected-and-folded chain.
func computeSelectedSlots(through uint64, carriers []LeaderOutcomeCarrier, predecessor *CertificateFoldEvidence) (first uint64, outcomes [leaderOutcomeWindowSlots]uint8, incomplete bool) {
	if through >= leaderOutcomeWindowSlots-1 {
		first = through - (leaderOutcomeWindowSlots - 1)
	}
	if len(carriers) > leaderOutcomeWindowSlots {
		incomplete = true
	} else {
		records := make(map[uint64]leaderOutcomeRecord, len(carriers)+1)
		if predecessor != nil {
			if predecessor.Slot > through {
				incomplete = true
			} else {
				records[predecessor.Slot] = leaderOutcomeRecord{carrier: LeaderOutcomeCarrier{CertificateFoldEvidence: *predecessor}}
			}
		}
		for _, c := range carriers {
			if c.Slot > through {
				incomplete = true
				continue // A future child cannot establish an already-folded gap.
			}
			if old, ok := records[c.Slot]; ok {
				if old.carrier.CertificateFoldEvidence != c.CertificateFoldEvidence || old.hasAncestry && old.carrier != c {
					old.conflict = true
				}
				old.carrier, old.hasAncestry = c, true
				records[c.Slot] = old
			} else {
				records[c.Slot] = leaderOutcomeRecord{carrier: c, hasAncestry: true}
			}
		}
		slots := make([]uint64, 0, len(records))
		for slot := range records {
			slots = append(slots, slot)
		}
		sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
		for i, slot := range slots {
			r := records[slot]
			if !r.known() {
				continue
			}
			if slot >= first {
				outcomes[slot-first] = 1
			}
			if !r.hasAncestry || i == 0 || !r.carrier.HasParentBlockID || r.carrier.ParentSlot != r.carrier.ExecutionParentSlot || r.carrier.ParentSlot >= slot {
				continue
			}
			// The immediate retained produced predecessor must be the claimed
			// parent. Any intervening produced layer contradicts a skip span.
			p := records[slots[i-1]]
			if !p.known() || p.carrier.Slot != r.carrier.ParentSlot || p.carrier.BlockID != r.carrier.ParentBlockID || p.carrier.Bankhash != r.carrier.ExecutionParentBankhash {
				continue
			}
			gapFirst, last := r.carrier.ParentSlot+1, slot-1
			if gapFirst < first {
				gapFirst = first
			}
			// Clip before iteration: even a near-uint64-sized gap costs at
			// most 511 iterations inside this fixed slot window.
			if gapFirst <= last {
				for offset := gapFirst - first; offset <= last-first; offset++ {
					outcomes[offset] = 2
				}
			}
		}
	}
	return first, outcomes, incomplete
}

func (r leaderOutcomeRecord) known() bool {
	return !r.conflict && r.carrier.HasBlockID && r.carrier.BlockID != (solana.Hash{}) && r.carrier.Bankhash != (solana.Hash{})
}
