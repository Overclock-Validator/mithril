package replay

import (
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// EpochOutcomeSummary splits recent settled observations by the epoch at the
// durable slot and its predecessor. It is not a persisted epoch score.
type EpochOutcomeSummary struct {
	SampledAt             time.Time
	Identity, VoteAccount solana.PublicKey
	Through               uint64
	Count                 int
	Epochs                [2]EpochOutcome
}

// EpochOutcome retains explicit slot coverage and uncertainty for one epoch.
// Reward ranges count target slots, eight slots before their carriers, rather
// than assigning a boundary carrier's rewards to its own epoch.
type EpochOutcome struct {
	Epoch, FirstSlot, LastSlot                          uint64
	HasLeaders                                          bool
	LeaderFirst, LeaderLast                             uint64
	Produced, Skipped, ScheduledUnknown                 uint64
	UnknownSchedule, UnknownAncestry                    uint64
	LeaderEpochComplete                                 bool
	HasRewards                                          bool
	RewardFirst, RewardLast                             uint64
	Included, Excluded, Ineligible, Unknown, Unobserved uint64
	RewardEpochComplete                                 bool
}

func (r *CertificateOutcomes) epochSnapshot(identity solana.PublicKey, through uint64, schedule *sealevel.SysvarEpochSchedule) EpochOutcomeSummary {
	if r == nil || identity.IsZero() || r.subject.IsZero() || r.summary.SampledAt.IsZero() || schedule == nil || schedule.SlotsPerEpoch == 0 {
		return EpochOutcomeSummary{}
	}
	s := EpochOutcomeSummary{SampledAt: r.summary.SampledAt, Identity: identity, VoteAccount: r.subject, Through: through}
	// Derive bounds from an actual slot in each epoch, avoiding unchecked
	// multiplication in a first-slot calculation and overflow at uint64's end.
	point := through
	var epochs []uint64
	for i := range s.Epochs {
		epoch, index := schedule.GetEpochAndSlotIndex(point)
		length := schedule.SlotsInEpoch(epoch)
		if index > point || length == 0 || index >= length || length-1 > math.MaxUint64-(point-index) {
			return EpochOutcomeSummary{}
		}
		first := point - index
		s.Epochs[i] = EpochOutcome{Epoch: epoch, FirstSlot: first, LastSlot: first + length - 1}
		epochs = append(epochs, epoch)
		s.Count++
		if first == 0 {
			break
		}
		point = first - 1
	}
	schedules := global.LeaderSchedulesForEpochs(epochs)
	carriers, predecessor := r.foldedCarriers()
	first, outcomes, incomplete := computeSelectedSlots(through, carriers, predecessor)
	for offset := uint64(0); offset <= through-first; offset++ {
		slot := first + offset
		for i := 0; i < s.Count; i++ {
			e := &s.Epochs[i]
			if slot < e.FirstSlot || slot > e.LastSlot {
				continue
			}
			if !e.HasLeaders {
				e.LeaderFirst, e.HasLeaders = slot, true
			}
			e.LeaderLast = slot
			outcome := outcomes[offset]
			if outcome == 0 {
				e.UnknownAncestry++
			}
			leader, known := solana.PublicKey{}, false
			if schedules[i] != nil {
				leader, known = schedules[i].LeaderForSlot(slot)
			}
			if !known || leader.IsZero() {
				e.UnknownSchedule++
				break
			}
			if leader == identity {
				switch outcome {
				case 1:
					e.Produced++
				case 2:
					e.Skipped++
				default:
					e.ScheduledUnknown++
				}
			}
			break
		}
	}
	// Missing carriers are not exclusions: the target may have no retained
	// reward evidence at all. Keep that denominator visible as unobserved.
	byTarget := make(map[uint64]certificateWindowEntry, r.count)
	for i := 0; i < r.count; i++ {
		e := r.window[(r.start+i)%len(r.window)]
		if e.slot >= rewardcerts.SlotsForReward && e.slot <= through {
			byTarget[e.slot-rewardcerts.SlotsForReward] = e
		}
	}
	if through >= rewardcerts.SlotsForReward {
		last := through - rewardcerts.SlotsForReward
		start := uint64(0)
		if last >= certificateWindowLimit-1 {
			start = last - (certificateWindowLimit - 1)
		}
		for offset := uint64(0); offset <= last-start; offset++ {
			target := start + offset
			for i := 0; i < s.Count; i++ {
				e := &s.Epochs[i]
				if target < e.FirstSlot || target > e.LastSlot {
					continue
				}
				if !e.HasRewards {
					e.RewardFirst, e.HasRewards = target, true
				}
				e.RewardLast = target
				carrier, found := byTarget[target]
				m := carrier.carrier.Observation.Reward
				switch {
				case !found:
					e.Unobserved++
				case carrier.gap || m.TargetSlot != target || m.TargetEpoch != e.Epoch || !m.AdmissionKnown || m.Included && (!m.Present || !m.Eligible):
					e.Unknown++
				case !m.Eligible:
					e.Ineligible++
				case m.Included:
					e.Included++
				default:
					e.Excluded++
				}
				break
			}
		}
	}
	for i := 0; i < s.Count; i++ {
		e := &s.Epochs[i]
		e.LeaderEpochComplete = !incomplete && !r.coverageLost && e.HasLeaders && e.LeaderFirst == e.FirstSlot && e.LeaderLast == e.LastSlot && e.UnknownAncestry == 0 && e.UnknownSchedule == 0
		e.RewardEpochComplete = !r.coverageLost && e.HasRewards && e.RewardFirst == e.FirstSlot && e.RewardLast == e.LastSlot && e.Unknown == 0 && e.Unobserved == 0
	}
	return s
}
