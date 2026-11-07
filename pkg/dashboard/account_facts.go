package dashboard

import (
	"time"

	"github.com/gagliardetto/solana-go"
)

// AccountFacts retains the successful local bank where balances and optional
// stored commissions were sampled. Nil balances mean unavailable.
type AccountFacts struct {
	SampledAt               time.Time          `json:"sampled_at"`
	Slot                    uint64             `json:"slot,string"`
	Bankhash                string             `json:"bankhash"`
	Epoch                   uint64             `json:"epoch,string"`
	Identity                string             `json:"identity"`
	IdentityBalanceLamports *uint64            `json:"identity_balance_lamports,string"`
	VoteAccount             *string            `json:"vote_account"`
	VoteBalanceLamports     *uint64            `json:"vote_balance_lamports,string"`
	StoredCommissions       *StoredCommissions `json:"stored_commissions,omitempty"`
	StoredVoteHeader        *StoredVoteHeader  `json:"stored_vote_header,omitempty"`
	FundingRules            *FundingRules      `json:"funding_rules,omitempty"`
	EpochStakeSets          *EpochStakeSets    `json:"epoch_stake_sets,omitempty"`
}

// StoredCommissions contains raw vote-account header fields, not full state
// validity or an effective payout rate. Nil fields are unknown; legacy states
// do not store a separate block-revenue commission. Values above 10000 are valid.
type StoredCommissions struct {
	InflationRewardsBPS *uint16 `json:"inflation_rewards_bps"`
	BlockRevenueBPS     *uint16 `json:"block_revenue_bps"`
}

// PublicStoredCommissions contains the explicitly exposed commission fields.
type PublicStoredCommissions struct {
	InflationRewardsBPS *uint16 `json:"inflation_rewards_bps"`
	BlockRevenueBPS     *uint16 `json:"block_revenue_bps"`
}

// PublicAccountFacts is neither a durable balance nor a voting-readiness claim.
type PublicAccountFacts struct {
	PublicObservation
	Provenance              string                   `json:"provenance"`
	Slot                    uint64                   `json:"slot,string"`
	Bankhash                string                   `json:"bankhash"`
	Epoch                   uint64                   `json:"epoch,string"`
	Identity                string                   `json:"identity"`
	IdentityBalanceLamports *uint64                  `json:"identity_balance_lamports,string"`
	VoteAccount             *string                  `json:"vote_account"`
	VoteBalanceLamports     *uint64                  `json:"vote_balance_lamports,string"`
	StoredCommissions       *PublicStoredCommissions `json:"stored_commissions,omitempty"`
	StoredVoteHeader        *PublicStoredVoteHeader  `json:"stored_vote_header,omitempty"`
	FundingRules            *PublicFundingRules      `json:"funding_rules,omitempty"`
	EpochStakeSets          *PublicEpochStakeSets    `json:"epoch_stake_sets,omitempty"`
	TicketCapacityNoIncome  *VoteTicketCapacity      `json:"ticket_capacity_no_income,omitempty"`
}

func publicAccountFacts(s *Snapshot, state string, now time.Time) *PublicAccountFacts {
	if s.Replay == nil || s.Replay.AccountFacts == nil || s.Replay.Completed == nil || s.IdentityKey == nil {
		return nil
	}
	r, f, b := s.Replay, s.Replay.AccountFacts, s.Replay.Completed
	if s.RunID == "" || r.Attempt == 0 || f.Identity != *s.IdentityKey ||
		!validPublicTime(s.StartedAt, now) || !validPublicTime(r.StartedAt, now) || r.StartedAt.Before(s.StartedAt) ||
		!validPublicTime(f.SampledAt, now) || f.SampledAt.Before(r.StartedAt) || f.SampledAt.After(r.SampledAt) ||
		f.SampledAt.After(s.SampledAt.Add(time.Second)) || !validPublicTime(b.At, now) || b.At.Before(r.StartedAt) || b.At.After(r.SampledAt) ||
		f.Slot > b.Slot || f.Epoch > b.Epoch || f.Slot == b.Slot && (f.Bankhash != b.Bankhash || f.Epoch != b.Epoch) ||
		f.VoteAccount == nil && (f.VoteBalanceLamports != nil || f.StoredCommissions != nil || f.StoredVoteHeader != nil || f.FundingRules != nil || f.EpochStakeSets != nil) {
		return nil
	}
	identity, err := solana.PublicKeyFromBase58(f.Identity)
	if err != nil || identity.IsZero() {
		return nil
	}
	if _, err := solana.HashFromBase58(f.Bankhash); err != nil {
		return nil
	}
	if f.VoteAccount != nil {
		vote, err := solana.PublicKeyFromBase58(*f.VoteAccount)
		if err != nil || vote.IsZero() {
			return nil
		}
	}
	p := &PublicAccountFacts{PublicObservation: publicObservation(f.SampledAt, state, now), Provenance: "successful_local_bank",
		Slot: f.Slot, Bankhash: f.Bankhash, Epoch: f.Epoch, Identity: f.Identity}
	if f.IdentityBalanceLamports != nil {
		v := *f.IdentityBalanceLamports
		p.IdentityBalanceLamports = &v
	}
	// Funding is useful before admission and on full nodes. A current voter is
	// not required, but its conflicting subject must never relabel old funds.
	if c := s.Consensus; c != nil && c.Voting != nil {
		v := c.Voting
		if validPublicTime(v.SampledAt, now) && !v.SampledAt.Before(s.StartedAt) &&
			(v.Identity != f.Identity || f.VoteAccount != nil && v.VoteAccount != *f.VoteAccount) {
			return p
		}
	}
	if f.VoteAccount != nil {
		v := *f.VoteAccount
		p.VoteAccount = &v
	}
	if f.VoteBalanceLamports != nil {
		v := *f.VoteBalanceLamports
		p.VoteBalanceLamports = &v
	}
	if c := f.StoredCommissions; c != nil {
		p.StoredCommissions = &PublicStoredCommissions{}
		if c.InflationRewardsBPS != nil {
			v := *c.InflationRewardsBPS
			p.StoredCommissions.InflationRewardsBPS = &v
		}
		if c.BlockRevenueBPS != nil {
			v := *c.BlockRevenueBPS
			p.StoredCommissions.BlockRevenueBPS = &v
		}
	}
	p.StoredVoteHeader = publicStoredVoteHeader(f.StoredVoteHeader)
	p.FundingRules = publicFundingRules(f.FundingRules, f.Slot, f.VoteBalanceLamports)
	p.EpochStakeSets = publicEpochStakeSets(f.EpochStakeSets, f.Epoch, f.Slot, f.Identity)
	if s.Mode == "validator" && p.VoteAccount != nil {
		p.TicketCapacityNoIncome = publicVoteTicketCapacity(p)
	}
	return p
}
