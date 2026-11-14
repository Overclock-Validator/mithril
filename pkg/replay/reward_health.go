package replay

import (
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

const rewardHealthWindowSlots = 128

// RewardHealthState describes ordinary reward membership in a settled target
// window, not current voting, local signing, payment or readiness.
type RewardHealthState string

const (
	RewardHealthUnknown          RewardHealthState = "unknown"
	RewardHealthIncludedRecently RewardHealthState = "included_recently"
	RewardHealthNoInclusion128   RewardHealthState = "no_inclusion_128"
	RewardHealthIneligible       RewardHealthState = "ineligible"
)

// RewardTargetOutcome classifies one target for the fixed identity/vote account.
// Unknown admission or a different target-epoch identity is never exclusion.
type RewardTargetOutcome string

const (
	RewardTargetUnknown    RewardTargetOutcome = "unknown"
	RewardTargetIncluded   RewardTargetOutcome = "included"
	RewardTargetExcluded   RewardTargetOutcome = "excluded"
	RewardTargetIneligible RewardTargetOutcome = "ineligible"
)

// RewardTargetObservation is retained ordinary reward membership, not a vote
// cast by this process. Latest references can disappear at ring eviction.
type RewardTargetObservation struct {
	TargetSlot, TargetEpoch uint64
	Outcome                 RewardTargetOutcome
}

// RewardHealthSummary partitions at most 128 mature targets ending eight slots
// before Through. This differs from FD's replay-frontier-minus-128 policy.
// Source time is the successful fold observation, never a viewer heartbeat.
type RewardHealthSummary struct {
	SampledAt                                           time.Time
	Provenance                                          string
	Identity, VoteAccount                               solana.PublicKey
	Through                                             uint64
	HasWindow                                           bool
	WindowFirst, WindowLast                             uint64
	Included, Excluded, Ineligible, Unknown, Unobserved uint64
	CoverageComplete                                    bool
	Health                                              RewardHealthState
	HasLatestResolved, HasLatestIncluded                bool
	LatestResolved, LatestIncluded                      RewardTargetObservation
}

// rewardHealthSnapshot runs on the replay owner after settlement. It scans the
// existing ring once, with fixed target storage and no material/cache lookups.
func (r *CertificateOutcomes) rewardHealthSnapshot(identity solana.PublicKey, through uint64, schedule *sealevel.SysvarEpochSchedule) RewardHealthSummary {
	if r == nil || identity.IsZero() || r.subject.IsZero() || r.summary.SampledAt.IsZero() || schedule == nil || schedule.SlotsPerEpoch == 0 {
		return RewardHealthSummary{}
	}
	// Use the same slot-derived epoch bounds as epochSnapshot, avoiding unchecked
	// first-slot multiplication. Also reject a wrapped normal epoch number.
	epochAt := func(slot uint64) (uint64, bool) {
		epoch, index := schedule.GetEpochAndSlotIndex(slot)
		length := schedule.SlotsInEpoch(epoch)
		return epoch, index <= slot && length != 0 && index < length && length-1 <= math.MaxUint64-(slot-index) &&
			(slot < schedule.FirstNormalSlot || epoch >= schedule.FirstNormalEpoch)
	}
	if _, ok := epochAt(through); !ok {
		return RewardHealthSummary{}
	}
	s := RewardHealthSummary{SampledAt: r.summary.SampledAt, Provenance: "selected_and_folded", Identity: identity, VoteAccount: r.subject,
		Through: through, Health: RewardHealthUnknown}
	if through < rewardcerts.SlotsForReward {
		return s
	}
	s.HasWindow, s.WindowLast = true, through-rewardcerts.SlotsForReward
	if s.WindowLast >= rewardHealthWindowSlots-1 {
		s.WindowFirst = s.WindowLast - (rewardHealthWindowSlots - 1)
	}
	// Empty entries are unobserved targets, not exclusions or skipped votes.
	var outcomes [rewardHealthWindowSlots]RewardTargetOutcome
	for i := 0; i < r.count; i++ {
		e := &r.window[(r.start+i)%len(r.window)]
		if e.slot < rewardcerts.SlotsForReward || e.slot > through {
			continue
		}
		target := e.slot - rewardcerts.SlotsForReward
		epoch, ok := epochAt(target)
		if !ok {
			return RewardHealthSummary{}
		}
		c, m := &e.carrier, e.carrier.Observation.Reward
		outcome := RewardTargetUnknown
		if !e.gap && c.Slot == e.slot && e.fold.Slot == e.slot && e.fold.HasBlockID && !c.BlockID.IsZero() &&
			c.BlockID == e.fold.BlockID && c.Bankhash == e.fold.Bankhash && c.Observation.VoteAccount == r.subject &&
			m.TargetSlot == target && m.TargetEpoch == epoch && m.AdmissionKnown && m.MaterialGeneration != 0 &&
			!(m.Included && (!m.Present || !m.Eligible)) {
			switch {
			case !m.Eligible:
				outcome = RewardTargetIneligible
			case m.EpochIdentity != identity:
				// A vote account's prior identity must not label this node.
			case m.Included:
				outcome = RewardTargetIncluded
			default:
				outcome = RewardTargetExcluded
			}
		}
		if target >= s.WindowFirst {
			outcomes[target-s.WindowFirst] = outcome
		}
		if outcome != RewardTargetUnknown {
			observation := RewardTargetObservation{TargetSlot: target, TargetEpoch: epoch, Outcome: outcome}
			if !s.HasLatestResolved || target > s.LatestResolved.TargetSlot {
				s.LatestResolved, s.HasLatestResolved = observation, true
			}
			if outcome == RewardTargetIncluded && (!s.HasLatestIncluded || target > s.LatestIncluded.TargetSlot) {
				s.LatestIncluded, s.HasLatestIncluded = observation, true
			}
		}
	}
	for offset := uint64(0); offset <= s.WindowLast-s.WindowFirst; offset++ {
		switch outcomes[offset] {
		case RewardTargetIncluded:
			s.Included++
		case RewardTargetExcluded:
			s.Excluded++
		case RewardTargetIneligible:
			s.Ineligible++
		case RewardTargetUnknown:
			s.Unknown++
		default:
			s.Unobserved++
		}
	}
	s.CoverageComplete = !r.coverageLost && s.Unknown == 0 && s.Unobserved == 0
	switch outcomes[s.WindowLast-s.WindowFirst] {
	case RewardTargetIneligible:
		s.Health = RewardHealthIneligible
	case RewardTargetIncluded, RewardTargetExcluded:
		if s.Included != 0 {
			s.Health = RewardHealthIncludedRecently
		} else if s.CoverageComplete && s.Excluded == rewardHealthWindowSlots {
			s.Health = RewardHealthNoInclusion128
		}
	}
	return s
}
