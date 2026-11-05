package dashboard

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
)

// EpochOutcomes carries at most two epoch partitions of bounded local evidence.
type EpochOutcomes struct {
	SampledAt   time.Time      `json:"sampled_at"`
	Identity    string         `json:"identity"`
	VoteAccount string         `json:"vote_account"`
	Through     uint64         `json:"through_slot,string"`
	Epochs      []EpochOutcome `json:"epochs"`
}

// PublicEpochOutcome contains the allowlisted selected-and-folded epoch data.
type PublicEpochOutcome struct {
	Epoch     uint64                   `json:"epoch,string"`
	FirstSlot uint64                   `json:"first_slot,string"`
	LastSlot  uint64                   `json:"last_slot,string"`
	Leaders   *PublicEpochLeaderWindow `json:"leaders"`
	Rewards   *PublicEpochRewardWindow `json:"rewards"`
}

// PublicEpochLeaderWindow counts selected-fork leader outcomes.
type PublicEpochLeaderWindow struct {
	First            uint64 `json:"first_slot,string"`
	Last             uint64 `json:"last_slot,string"`
	Produced         uint64 `json:"produced,string"`
	Skipped          uint64 `json:"selected_fork_skipped,string"`
	ScheduledUnknown uint64 `json:"scheduled_unknown,string"`
	UnknownSchedule  uint64 `json:"unknown_schedule_slots,string"`
	UnknownAncestry  uint64 `json:"unknown_ancestry_slots,string"`
	CompleteEpoch    bool   `json:"complete_epoch"`
}

// PublicEpochRewardWindow counts retained reward-target outcomes.
type PublicEpochRewardWindow struct {
	First         uint64 `json:"first_slot,string"`
	Last          uint64 `json:"last_slot,string"`
	Included      uint64 `json:"included,string"`
	Excluded      uint64 `json:"excluded,string"`
	Ineligible    uint64 `json:"ineligible,string"`
	Unknown       uint64 `json:"unknown,string"`
	Unobserved    uint64 `json:"unobserved,string"`
	CompleteEpoch bool   `json:"complete_epoch"`
}

// EpochOutcome keeps leader-slot and reward-target ranges separate.
type EpochOutcome struct {
	Epoch     uint64             `json:"epoch,string"`
	FirstSlot uint64             `json:"first_slot,string"`
	LastSlot  uint64             `json:"last_slot,string"`
	Leaders   *EpochLeaderWindow `json:"leaders"`
	Rewards   *EpochRewardWindow `json:"rewards"`
}

// EpochLeaderWindow counts assignments of the bound node identity. Unknown
// schedule slots are not included in the assignment denominator.
type EpochLeaderWindow struct {
	First            uint64 `json:"first_slot,string"`
	Last             uint64 `json:"last_slot,string"`
	Produced         uint64 `json:"produced,string"`
	Skipped          uint64 `json:"selected_fork_skipped,string"`
	ScheduledUnknown uint64 `json:"scheduled_unknown,string"`
	UnknownSchedule  uint64 `json:"unknown_schedule_slots,string"`
	UnknownAncestry  uint64 `json:"unknown_ancestry_slots,string"`
	CompleteEpoch    bool   `json:"complete_epoch"`
}

// EpochRewardWindow partitions target slots, not payments. Included+Excluded is
// the known eligible-carrier denominator. Unobserved slots have no retained
// carrier; Unknown has a carrier but insufficient valid admission evidence.
type EpochRewardWindow struct {
	First         uint64 `json:"first_slot,string"`
	Last          uint64 `json:"last_slot,string"`
	Included      uint64 `json:"included,string"`
	Excluded      uint64 `json:"excluded,string"`
	Ineligible    uint64 `json:"ineligible,string"`
	Unknown       uint64 `json:"unknown,string"`
	Unobserved    uint64 `json:"unobserved,string"`
	CompleteEpoch bool   `json:"complete_epoch"`
}

// PublicEpochOutcomes dates an attempt-local, selected-and-folded observation.
type PublicEpochOutcomes struct {
	PublicObservation
	Provenance  string               `json:"provenance"`
	Identity    string               `json:"identity"`
	VoteAccount string               `json:"vote_account"`
	Through     uint64               `json:"through_slot,string"`
	Epochs      []PublicEpochOutcome `json:"epochs"`
}

func publicEpochOutcomes(s *Snapshot, voting *PublicVoting, state string, now time.Time) *PublicEpochOutcomes {
	if !s.Alpenglow || s.Mode != "validator" || voting == nil || s.Replay == nil || s.Replay.EpochOutcomes == nil {
		return nil
	}
	r, e := s.Replay, s.Replay.EpochOutcomes
	if e.Identity != voting.Identity || e.VoteAccount != voting.VoteAccount || len(e.Epochs) == 0 || len(e.Epochs) > 2 ||
		r.DurableSlot == nil || e.Through > *r.DurableSlot || !validPublicTime(r.StartedAt, now) || r.StartedAt.Before(s.StartedAt) ||
		!validPublicTime(e.SampledAt, now) || e.SampledAt.Before(r.StartedAt) || e.SampledAt.After(s.SampledAt.Add(time.Second)) {
		return nil
	}
	p := &PublicEpochOutcomes{PublicObservation: publicObservation(e.SampledAt, state, now), Provenance: "selected_and_folded", Identity: e.Identity, VoteAccount: e.VoteAccount, Through: e.Through, Epochs: make([]PublicEpochOutcome, len(e.Epochs))}
	if voting.State != "fresh" {
		p.State = voting.State
	}
	for i, row := range e.Epochs {
		if row.FirstSlot > row.LastSlot {
			return nil
		}
		if i == 0 {
			if e.Through < row.FirstSlot || e.Through > row.LastSlot {
				return nil
			}
		} else {
			prev := e.Epochs[i-1]
			if prev.Epoch == 0 || row.Epoch != prev.Epoch-1 || prev.FirstSlot == 0 || row.LastSlot != prev.FirstSlot-1 {
				return nil
			}
		}
		p.Epochs[i] = PublicEpochOutcome{Epoch: row.Epoch, FirstSlot: row.FirstSlot, LastSlot: row.LastSlot}
		if l := row.Leaders; l != nil {
			if !validEpochRange(l.First, l.Last, row, e.Through) {
				return nil
			}
			n := l.Last - l.First + 1
			if l.Produced > n || l.Skipped > n || l.ScheduledUnknown > n || l.UnknownSchedule > n || l.UnknownAncestry > n ||
				l.Produced+l.Skipped+l.ScheduledUnknown > n-l.UnknownSchedule || l.ScheduledUnknown > l.UnknownAncestry || l.Produced+l.Skipped > n-l.UnknownAncestry ||
				l.CompleteEpoch && (l.First != row.FirstSlot || l.Last != row.LastSlot || l.UnknownSchedule != 0 || l.UnknownAncestry != 0) {
				return nil
			}
			p.Epochs[i].Leaders = &PublicEpochLeaderWindow{First: l.First, Last: l.Last, Produced: l.Produced, Skipped: l.Skipped,
				ScheduledUnknown: l.ScheduledUnknown, UnknownSchedule: l.UnknownSchedule, UnknownAncestry: l.UnknownAncestry, CompleteEpoch: l.CompleteEpoch}
		}
		if w := row.Rewards; w != nil {
			if e.Through < rewardcerts.SlotsForReward || !validEpochRange(w.First, w.Last, row, e.Through-rewardcerts.SlotsForReward) {
				return nil
			}
			n := w.Last - w.First + 1
			if w.Included > n || w.Excluded > n || w.Ineligible > n || w.Unknown > n || w.Unobserved > n ||
				w.Included+w.Excluded+w.Ineligible+w.Unknown+w.Unobserved != n ||
				w.CompleteEpoch && (w.First != row.FirstSlot || w.Last != row.LastSlot || w.Unknown != 0 || w.Unobserved != 0) {
				return nil
			}
			p.Epochs[i].Rewards = &PublicEpochRewardWindow{First: w.First, Last: w.Last, Included: w.Included, Excluded: w.Excluded,
				Ineligible: w.Ineligible, Unknown: w.Unknown, Unobserved: w.Unobserved, CompleteEpoch: w.CompleteEpoch}
		}
	}
	return p
}

func validEpochRange(first, last uint64, epoch EpochOutcome, through uint64) bool {
	return first >= epoch.FirstSlot && first <= last && last <= epoch.LastSlot && last <= through && last-first < 512 && through-first < 512
}
