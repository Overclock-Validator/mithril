package dashboard

import (
	"math"
	"time"
)

// ReplayLatestSelected describes the newest applied fold, not the newest replay
// row or cluster head. Gap facts and missing identifiers remain unknown.
type ReplayLatestSelected struct {
	Scope      string               `json:"scope"`
	ObservedAt time.Time            `json:"observed_at"`
	Slot       uint64               `json:"slot,string"`
	Bankhash   *string              `json:"bankhash"`
	BlockID    *string              `json:"block_id"`
	Gap        bool                 `json:"gap"`
	Facts      *ReplaySelectedFacts `json:"facts"`
}

// ReplaySelectedFacts preserves successful replay facts matched to that fold.
// Counts may be saturated; they are not transaction rates or unique work.
type ReplaySelectedFacts struct {
	ReplayResultAt      time.Time            `json:"replay_result_at"`
	Epoch               uint64               `json:"epoch,string"`
	ExecutionLeader     *string              `json:"execution_leader"`
	FromLocalProduction bool                 `json:"from_local_production"`
	Transactions        ReplaySelectedCounts `json:"transactions"`
	Fees                *ReplaySelectedFees  `json:"fees"`
	LocalCost           *ReplaySelectedCost  `json:"local_cost"`
}

// ReplaySelectedFees are charged transaction fees, not tips or net revenue.
type ReplaySelectedFees struct {
	ExecutionLamports uint64  `json:"execution_lamports,string"`
	PriorityLamports  uint64  `json:"priority_lamports,string"`
	TotalLamports     uint64  `json:"total_lamports,string"`
	BurnedLamports    *uint64 `json:"burned_lamports,string,omitempty"`
}

// ReplaySelectedCost is accepted native cost frozen by the matching local
// producer. It is not recorded transaction compute units or the cost of rejected work.
type ReplaySelectedCost struct {
	BlockCost      uint64 `json:"block_cost,string"`
	BlockCostLimit uint64 `json:"block_cost_limit,string"`
}

// ReplaySelectedCounts uses the execution owner's independent raw counters.
// ComputeUnits is absent for older sources. If saturated, all counters are a
// partial recorded prefix, not a complete bank total.
type ReplaySelectedCounts struct {
	Total         uint64  `json:"total,string"`
	NonVote       uint64  `json:"non_vote,string"`
	Failed        uint64  `json:"failed,string"`
	NonVoteFailed uint64  `json:"non_vote_failed,string"`
	Saturated     bool    `json:"saturated"`
	ComputeUnits  *uint64 `json:"compute_units,string,omitempty"`
}

func validReplayLatestSelected(s *ReplayLatestSelected) bool {
	if s == nil {
		return true
	}
	if s.Scope != "latest_selected_and_folded" || !validReplayDetailTime(s.ObservedAt) ||
		s.Bankhash != nil && !validReplayDetailHash(*s.Bankhash, true) ||
		s.BlockID != nil && !validReplayDetailHash(*s.BlockID, true) {
		return false
	}
	if s.Gap {
		return s.Facts == nil
	}
	f := s.Facts
	if s.Bankhash == nil || s.BlockID == nil || f == nil || !validReplayDetailTime(f.ReplayResultAt) ||
		f.ExecutionLeader != nil && !validReplayDetailHash(*f.ExecutionLeader, true) {
		return false
	}
	if fees := f.Fees; fees != nil && (fees.ExecutionLamports > math.MaxUint64-fees.PriorityLamports ||
		fees.ExecutionLamports+fees.PriorityLamports != fees.TotalLamports || fees.BurnedLamports != nil && *fees.BurnedLamports > fees.TotalLamports) {
		return false
	}
	if f.LocalCost != nil && !f.FromLocalProduction {
		return false
	}
	c := f.Transactions
	return c.NonVote <= c.Total && c.Failed <= c.Total && c.NonVoteFailed <= c.NonVote &&
		c.NonVoteFailed <= c.Failed && c.Failed-c.NonVoteFailed <= c.Total-c.NonVote
}

func cloneReplayLatestSelected(s *ReplayLatestSelected) *ReplayLatestSelected {
	if s == nil {
		return nil
	}
	copy := *s
	if s.Bankhash != nil {
		value := *s.Bankhash
		copy.Bankhash = &value
	}
	if s.BlockID != nil {
		value := *s.BlockID
		copy.BlockID = &value
	}
	if s.Facts != nil {
		facts := *s.Facts
		if facts.Transactions.ComputeUnits != nil {
			units := *facts.Transactions.ComputeUnits
			facts.Transactions.ComputeUnits = &units
		}
		if facts.Fees != nil {
			fees := *facts.Fees
			if fees.BurnedLamports != nil {
				burned := *fees.BurnedLamports
				fees.BurnedLamports = &burned
			}
			facts.Fees = &fees
		}
		if facts.LocalCost != nil {
			cost := *facts.LocalCost
			facts.LocalCost = &cost
		}
		if facts.ExecutionLeader != nil {
			value := *facts.ExecutionLeader
			facts.ExecutionLeader = &value
		}
		copy.Facts = &facts
	}
	return &copy
}
