package dashboard

import "time"

// ChainActivity is a value copy of replay's bounded, settled chain interval.
type ChainActivity struct {
	SampledAt           time.Time `json:"sampled_at"`
	Available           bool      `json:"available"`
	AnchorSlot          uint64    `json:"anchor_slot,string"`
	LastSlot            uint64    `json:"last_slot,string"`
	StartTime           int64     `json:"start_time,string"`
	EndTime             int64     `json:"end_time,string"`
	Blocks              uint64    `json:"blocks,string"`
	Transactions        uint64    `json:"transactions,string"`
	NonVoteTransactions uint64    `json:"non_vote_transactions,string"`
	FailedTransactions  uint64    `json:"failed_transactions,string"`
	FailedNonVote       uint64    `json:"failed_non_vote,string"`
	ComputeUnits        uint64    `json:"compute_units,string"`
}

// PublicChainActivity is present when the source supports settled activity.
// A nil interval means no continuous, positive-time interval was established.
type PublicChainActivity struct {
	PublicObservation
	Provenance string                  `json:"provenance"`
	TimeSource string                  `json:"time_source"`
	Interval   *PublicActivityInterval `json:"interval"`
}

// PublicActivityInterval counts (anchor_slot, last_slot], excluding the anchor
// block. Rates use the exact bank-clock interval; they are not replay speed or
// a promise that this range is at the cluster tip.
type PublicActivityInterval struct {
	AnchorSlot          uint64  `json:"anchor_slot,string"`
	LastSlot            uint64  `json:"last_slot,string"`
	StartTime           int64   `json:"start_time,string"`
	EndTime             int64   `json:"end_time,string"`
	Blocks              uint64  `json:"blocks,string"`
	Transactions        uint64  `json:"transactions,string"`
	NonVoteTransactions uint64  `json:"non_vote_transactions,string"`
	FailedTransactions  uint64  `json:"failed_transactions,string"`
	FailedNonVote       uint64  `json:"failed_non_vote,string"`
	ComputeUnits        uint64  `json:"compute_units,string"`
	TransactionsPerSec  float64 `json:"transactions_per_second"`
	NonVotePerSec       float64 `json:"non_vote_per_second"`
	FailedPerSec        float64 `json:"failed_per_second"`
	ComputeUnitsPerSec  float64 `json:"compute_units_per_second"`
}

func publicChainActivity(s *Snapshot, state string, now time.Time) *PublicChainActivity {
	r := s.Replay
	if !s.Alpenglow || r == nil || r.ChainActivity == nil {
		return nil
	}
	a := r.ChainActivity
	if !validPublicTime(r.StartedAt, now) || r.StartedAt.Before(s.StartedAt) || !validPublicTime(a.SampledAt, now) ||
		a.SampledAt.Before(r.StartedAt) || a.SampledAt.After(s.SampledAt.Add(time.Second)) {
		return nil
	}
	p := &PublicChainActivity{PublicObservation: publicObservation(a.SampledAt, state, now), Provenance: "selected_and_folded", TimeSource: "bank_clock"}
	if !a.Available {
		return p
	}
	if r.DurableSlot == nil || a.LastSlot > *r.DurableSlot || a.LastSlot <= a.AnchorSlot || a.StartTime <= 0 || a.EndTime <= a.StartTime ||
		a.Blocks == 0 || a.Blocks > 511 || a.Blocks > a.LastSlot-a.AnchorSlot || a.NonVoteTransactions > a.Transactions || a.FailedTransactions > a.Transactions ||
		a.FailedNonVote > a.FailedTransactions || a.FailedNonVote > a.NonVoteTransactions || a.FailedTransactions-a.FailedNonVote > a.Transactions-a.NonVoteTransactions {
		return nil
	}
	seconds := float64(a.EndTime - a.StartTime)
	p.Interval = &PublicActivityInterval{AnchorSlot: a.AnchorSlot, LastSlot: a.LastSlot, StartTime: a.StartTime, EndTime: a.EndTime, Blocks: a.Blocks,
		Transactions: a.Transactions, NonVoteTransactions: a.NonVoteTransactions, FailedTransactions: a.FailedTransactions, FailedNonVote: a.FailedNonVote, ComputeUnits: a.ComputeUnits,
		TransactionsPerSec: float64(a.Transactions) / seconds, NonVotePerSec: float64(a.NonVoteTransactions) / seconds,
		FailedPerSec: float64(a.FailedTransactions) / seconds, ComputeUnitsPerSec: float64(a.ComputeUnits) / seconds}
	return p
}
