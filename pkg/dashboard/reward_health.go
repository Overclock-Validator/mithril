package dashboard

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
)

// RewardTarget is a retained ordinary reward result, not a vote cast or payment.
type RewardTarget struct {
	Slot    uint64 `json:"target_slot,string"`
	Epoch   uint64 `json:"target_epoch,string"`
	Outcome string `json:"outcome"`
}

// RewardHealth partitions at most 128 mature targets for one identity/vote pair.
// Missing newest evidence stays unknown even when older inclusion is retained.
type RewardHealth struct {
	SampledAt        time.Time     `json:"sampled_at"`
	Identity         string        `json:"identity"`
	VoteAccount      string        `json:"vote_account"`
	Through          uint64        `json:"through_slot,string"`
	WindowFirst      *uint64       `json:"window_first,string"`
	WindowLast       *uint64       `json:"window_last,string"`
	Included         uint64        `json:"included,string"`
	Excluded         uint64        `json:"excluded,string"`
	Ineligible       uint64        `json:"ineligible,string"`
	Unknown          uint64        `json:"unknown,string"`
	Unobserved       uint64        `json:"unobserved,string"`
	CoverageComplete bool          `json:"coverage_complete"`
	Health           string        `json:"health"`
	LatestResolved   *RewardTarget `json:"latest_resolved"`
	LatestIncluded   *RewardTarget `json:"latest_included"`
}

// PublicRewardHealth is identity-bound settled evidence, not node currentness.
// The mature-target window differs from FD's replay-frontier-minus-128 policy.
type PublicRewardHealth struct {
	PublicObservation
	Provenance       string              `json:"provenance"`
	Identity         string              `json:"identity"`
	VoteAccount      string              `json:"vote_account"`
	Through          uint64              `json:"through_slot,string"`
	WindowFirst      *uint64             `json:"window_first,string"`
	WindowLast       *uint64             `json:"window_last,string"`
	Included         uint64              `json:"included,string"`
	Excluded         uint64              `json:"excluded,string"`
	Ineligible       uint64              `json:"ineligible,string"`
	Unknown          uint64              `json:"unknown,string"`
	Unobserved       uint64              `json:"unobserved,string"`
	CoverageComplete bool                `json:"coverage_complete"`
	Health           string              `json:"health"`
	LatestResolved   *PublicRewardTarget `json:"latest_resolved"`
	LatestIncluded   *PublicRewardTarget `json:"latest_included"`
}

// PublicRewardTarget contains the allowlisted reward-target outcome.
type PublicRewardTarget struct {
	Slot    uint64 `json:"target_slot,string"`
	Epoch   uint64 `json:"target_epoch,string"`
	Outcome string `json:"outcome"`
}

func publicRewardHealth(s *Snapshot, voting *PublicVoting, state string, now time.Time) *PublicRewardHealth {
	if !s.Alpenglow || s.Mode != "validator" || voting == nil || s.Replay == nil || s.Replay.RewardHealth == nil {
		return nil
	}
	r, h := s.Replay, s.Replay.RewardHealth
	if r.Attempt == 0 || h.Identity != voting.Identity || h.VoteAccount != voting.VoteAccount || r.DurableSlot == nil ||
		h.Through > *r.DurableSlot || !validPublicTime(r.StartedAt, now) || r.StartedAt.Before(s.StartedAt) ||
		!validPublicTime(h.SampledAt, now) || h.SampledAt.Before(r.StartedAt) || h.SampledAt.After(r.SampledAt) ||
		h.SampledAt.After(s.SampledAt.Add(time.Second)) || !validRewardHealth(h) {
		return nil
	}
	p := &PublicRewardHealth{PublicObservation: publicObservation(h.SampledAt, state, now), Provenance: "selected_and_folded",
		Identity: h.Identity, VoteAccount: h.VoteAccount, Through: h.Through, Included: h.Included, Excluded: h.Excluded,
		Ineligible: h.Ineligible, Unknown: h.Unknown, Unobserved: h.Unobserved, CoverageComplete: h.CoverageComplete, Health: h.Health}
	if h.WindowFirst != nil {
		first, last := *h.WindowFirst, *h.WindowLast
		p.WindowFirst, p.WindowLast = &first, &last
	}
	if h.LatestResolved != nil {
		v := h.LatestResolved
		p.LatestResolved = &PublicRewardTarget{Slot: v.Slot, Epoch: v.Epoch, Outcome: v.Outcome}
	}
	if h.LatestIncluded != nil {
		v := h.LatestIncluded
		p.LatestIncluded = &PublicRewardTarget{Slot: v.Slot, Epoch: v.Epoch, Outcome: v.Outcome}
	}
	if voting.State != "fresh" {
		p.State = voting.State
	}
	if p.State != "fresh" || r.Phase != "running" || s.Phase != "replaying" {
		p.Health = "unknown"
	}
	return p
}

func validRewardHealth(h *RewardHealth) bool {
	if (h.WindowFirst == nil) != (h.WindowLast == nil) {
		return false
	}
	if h.Through < rewardcerts.SlotsForReward {
		return h.WindowFirst == nil && h.LatestResolved == nil && h.LatestIncluded == nil && h.Included == 0 &&
			h.Excluded == 0 && h.Ineligible == 0 && h.Unknown == 0 && h.Unobserved == 0 && !h.CoverageComplete && h.Health == "unknown"
	}
	last := h.Through - rewardcerts.SlotsForReward
	first := uint64(0)
	if last >= 127 {
		first = last - 127
	}
	if h.WindowFirst == nil || *h.WindowFirst != first || *h.WindowLast != last {
		return false
	}
	n := last - first + 1
	if h.Included > n || h.Excluded > n || h.Ineligible > n || h.Unknown > n || h.Unobserved > n ||
		h.Included+h.Excluded+h.Ineligible+h.Unknown+h.Unobserved != n || h.CoverageComplete && (h.Unknown != 0 || h.Unobserved != 0) {
		return false
	}
	resolved, included := h.LatestResolved, h.LatestIncluded
	if resolved != nil {
		if resolved.Slot > last || resolved.Outcome != "included" && resolved.Outcome != "excluded" && resolved.Outcome != "ineligible" ||
			resolved.Slot >= first && (resolved.Outcome == "included" && h.Included == 0 || resolved.Outcome == "excluded" && h.Excluded == 0 || resolved.Outcome == "ineligible" && h.Ineligible == 0) {
			return false
		}
	}
	if included != nil && (resolved == nil || included.Outcome != "included" || included.Slot > resolved.Slot || included.Epoch > resolved.Epoch ||
		included.Slot == resolved.Slot && *included != *resolved) {
		return false
	}
	if (h.Included != 0) != (included != nil && included.Slot >= first) ||
		h.Included+h.Excluded+h.Ineligible != 0 && (resolved == nil || resolved.Slot < first) ||
		resolved != nil && resolved.Slot >= first && h.Included+h.Excluded+h.Ineligible > resolved.Slot-first+1 ||
		included != nil && included.Slot >= first && h.Included > included.Slot-first+1 ||
		resolved != nil && resolved.Outcome == "included" && (included == nil || *resolved != *included) {
		return false
	}
	expected := "unknown"
	if resolved != nil && resolved.Slot == last {
		switch {
		case resolved.Outcome == "ineligible":
			expected = "ineligible"
		case h.Included != 0:
			expected = "included_recently"
		case h.CoverageComplete && h.Excluded == 128:
			expected = "no_inclusion_128"
		}
	}
	return h.Health == expected
}
