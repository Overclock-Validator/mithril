package dashboard

import "time"

// PaymentTotals are gross bank-application receipts over known retained
// carriers. They exclude credit accrual, transfers, expenses and balance deltas.
type PaymentTotals struct {
	Fees                   uint64 `json:"fees_lamports,string"`
	FeeBurned              uint64 `json:"fee_burned_lamports,string"`
	IncineratorDeposits    uint64 `json:"incinerator_deposits_lamports,string"`
	IdentityFeeDeposits    uint64 `json:"identity_fee_deposits_lamports,string"`
	VoteFeeDeposits        uint64 `json:"vote_fee_deposits_lamports,string"`
	OtherLeaderFeeDeposits uint64 `json:"other_leader_fee_deposits_lamports,string"`
	VotingRewardsPaid      uint64 `json:"vote_account_rewards_paid_lamports,string"`
}

// PaymentOutcomes is an attempt-local value copy, never a balance history.
type PaymentOutcomes struct {
	SampledAt          time.Time      `json:"sampled_at"`
	Identity           string         `json:"identity"`
	VoteAccount        string         `json:"vote_account"`
	FirstCarrier       uint64         `json:"first_carrier,string"`
	LastCarrier        uint64         `json:"last_carrier,string"`
	Carriers           uint64         `json:"carriers,string"`
	Known              uint64         `json:"known,string"`
	Unknown            uint64         `json:"unknown,string"`
	CoverageIncomplete bool           `json:"coverage_incomplete"`
	Totals             *PaymentTotals `json:"totals"`
}

// PublicPaymentOutcomes exposes only bounded receipts for the bound voter.
type PublicPaymentOutcomes struct {
	PublicObservation
	Provenance         string               `json:"provenance"`
	Identity           string               `json:"identity"`
	VoteAccount        string               `json:"vote_account"`
	FirstCarrier       uint64               `json:"first_carrier,string"`
	LastCarrier        uint64               `json:"last_carrier,string"`
	Carriers           uint64               `json:"carriers,string"`
	Known              uint64               `json:"known,string"`
	Unknown            uint64               `json:"unknown,string"`
	CoverageIncomplete bool                 `json:"coverage_incomplete"`
	Totals             *PublicPaymentTotals `json:"totals"`
}

// PublicPaymentTotals is the allowlisted payment summary returned to viewers.
type PublicPaymentTotals struct {
	Fees                   uint64 `json:"fees_lamports,string"`
	FeeBurned              uint64 `json:"fee_burned_lamports,string"`
	IncineratorDeposits    uint64 `json:"incinerator_deposits_lamports,string"`
	IdentityFeeDeposits    uint64 `json:"identity_fee_deposits_lamports,string"`
	VoteFeeDeposits        uint64 `json:"vote_fee_deposits_lamports,string"`
	OtherLeaderFeeDeposits uint64 `json:"other_leader_fee_deposits_lamports,string"`
	VotingRewardsPaid      uint64 `json:"vote_account_rewards_paid_lamports,string"`
}

func publicPaymentOutcomes(s *Snapshot, voting *PublicVoting, state string, now time.Time) *PublicPaymentOutcomes {
	if !s.Alpenglow || s.Mode != "validator" || voting == nil || s.Replay == nil || s.Replay.PaymentOutcomes == nil {
		return nil
	}
	r, v := s.Replay, s.Replay.PaymentOutcomes
	if v.Identity != voting.Identity || v.VoteAccount != voting.VoteAccount || r.DurableSlot == nil || v.LastCarrier > *r.DurableSlot ||
		!validPublicTime(r.StartedAt, now) || r.StartedAt.Before(s.StartedAt) || !validPublicTime(v.SampledAt, now) ||
		v.SampledAt.Before(r.StartedAt) || v.SampledAt.After(s.SampledAt.Add(time.Second)) || v.FirstCarrier > v.LastCarrier ||
		v.Carriers == 0 || v.Carriers > 512 || v.Carriers-1 > v.LastCarrier-v.FirstCarrier ||
		v.Known > v.Carriers || v.Unknown > v.Carriers || v.Known+v.Unknown != v.Carriers ||
		!v.CoverageIncomplete && (v.Unknown != 0 || v.LastCarrier-v.FirstCarrier != v.Carriers-1) {
		return nil
	}
	p := &PublicPaymentOutcomes{PublicObservation: publicObservation(v.SampledAt, state, now), Provenance: "selected_and_folded",
		Identity: v.Identity, VoteAccount: v.VoteAccount, FirstCarrier: v.FirstCarrier, LastCarrier: v.LastCarrier,
		Carriers: v.Carriers, Known: v.Known, Unknown: v.Unknown, CoverageIncomplete: v.CoverageIncomplete}
	if voting.State != "fresh" {
		p.State = voting.State
	}
	if t := v.Totals; t != nil {
		if v.Known == 0 || t.FeeBurned > t.Fees {
			return nil
		}
		remaining := t.Fees - t.FeeBurned
		for _, deposit := range []uint64{t.IncineratorDeposits, t.IdentityFeeDeposits, t.VoteFeeDeposits, t.OtherLeaderFeeDeposits} {
			if deposit > remaining {
				return nil
			}
			remaining -= deposit
		}
		p.Totals = &PublicPaymentTotals{Fees: t.Fees, FeeBurned: t.FeeBurned,
			IncineratorDeposits: t.IncineratorDeposits, IdentityFeeDeposits: t.IdentityFeeDeposits,
			VoteFeeDeposits: t.VoteFeeDeposits, OtherLeaderFeeDeposits: t.OtherLeaderFeeDeposits,
			VotingRewardsPaid: t.VotingRewardsPaid}
	} else if v.Known != 0 && !v.CoverageIncomplete {
		return nil
	}
	return p
}
