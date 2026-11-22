package block

import (
	"math"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// PaymentObservation contains actual bank-application deposits, not net income
// or Alpenglow credit accrual. Only the execution owner writes it; publication
// still requires accepted replay and an exact committed fold.
type PaymentObservation struct {
	Identity, VoteAccount         solana.PublicKey
	Leader, FeeCollector          solana.PublicKey
	FeesKnown, RewardsKnown       bool
	Fees, FeeBurned, FeeDeposited uint64
	VotingRewardPaid              uint64
}

// EnablePaymentObservation starts a fresh private request before epoch reward
// processing. Replay must not call this on an adopted locally produced block.
func (b *Block) EnablePaymentObservation(identity, vote solana.PublicKey) {
	if b == nil {
		return
	}
	if identity.IsZero() || vote.IsZero() {
		b.paymentObservation = nil
		return
	}
	b.paymentObservation = &PaymentObservation{Identity: identity, VoteAccount: vote, RewardsKnown: true}
}

// RecordFeePayment records the fee-specific burn and collector deposit only
// after successful distribution. Invalid saturated arithmetic stays unknown.
func (b *Block) RecordFeePayment(total, burn uint64, collector solana.PublicKey, valid bool) {
	if b == nil || b.paymentObservation == nil {
		return
	}
	p := b.paymentObservation
	p.Leader, p.FeeCollector = b.Leader, collector
	p.FeesKnown = valid && burn <= total && (burn == total || !collector.IsZero())
	p.Fees, p.FeeBurned, p.FeeDeposited = 0, 0, 0
	if p.FeesKnown {
		p.Fees, p.FeeBurned, p.FeeDeposited = total, burn, total-burn
	}
}

// RecordAppliedVotingRewards replaces this block's observed reward phase with
// locally applied receipts. Callers must never pass RPC-supplied Block.Rewards.
// A redirected inflation collector does not count as a vote-account deposit.
func (b *Block) RecordAppliedVotingRewards(records []rpc.BlockReward) {
	if b == nil || b.paymentObservation == nil {
		return
	}
	p := b.paymentObservation
	var amount uint64
	for _, r := range records {
		if r.Pubkey != p.VoteAccount || r.RewardType != rpc.RewardTypeVoting {
			continue
		}
		if r.Lamports < 0 || uint64(r.Lamports) > math.MaxUint64-amount {
			p.RewardsKnown = false
			p.VotingRewardPaid = 0
			return
		}
		amount += uint64(r.Lamports)
	}
	if p.RewardsKnown {
		p.VotingRewardPaid = amount
	}
}

// PaymentObservation returns detached values; a missing request is unknown.
func (b *Block) PaymentObservation() (PaymentObservation, bool) {
	if b == nil || b.paymentObservation == nil {
		return PaymentObservation{}, false
	}
	return *b.paymentObservation, true
}
