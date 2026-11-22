package replay

import (
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
)

// PaymentOutcomeSummary covers only retained selected-and-folded carriers.
// Totals are gross observed deposits, never account balance changes, lifetime
// income, projected rewards or Alpenglow credit accrual.
type PaymentOutcomeSummary struct {
	SampledAt                                                    time.Time
	Identity, VoteAccount                                        solana.PublicKey
	HasWindow                                                    bool
	FirstCarrier, LastCarrier, Carriers, Known, Unknown          uint64
	CoverageIncomplete, TotalsAvailable                          bool
	Fees, FeeBurned, IncineratorDeposits                         uint64
	IdentityFeeDeposits, VoteFeeDeposits, OtherLeaderFeeDeposits uint64
	VotingRewardsPaid                                            uint64
}

func (r *CertificateOutcomes) paymentSnapshot(identity solana.PublicKey) PaymentOutcomeSummary {
	if r == nil || identity.IsZero() || r.subject.IsZero() || r.summary.SampledAt.IsZero() {
		return PaymentOutcomeSummary{}
	}
	s := PaymentOutcomeSummary{SampledAt: r.summary.SampledAt, Identity: identity, VoteAccount: r.subject,
		HasWindow: r.summary.HasWindow, FirstCarrier: r.summary.FirstCarrier, LastCarrier: r.summary.LastCarrier, Carriers: r.summary.Carriers,
		CoverageIncomplete: r.coverageLost || r.summary.SlotsWithoutCarrier != 0}
	overflow := false
	add := func(sum *uint64, n uint64) {
		if n > math.MaxUint64-*sum {
			overflow = true
			return
		}
		*sum += n
	}
	for i := 0; i < r.count; i++ {
		e := r.window[(r.start+i)%len(r.window)]
		p := e.carrier.Payments
		if e.gap || !p.FeesKnown || !p.RewardsKnown || p.Identity != identity || p.VoteAccount != r.subject ||
			p.FeeBurned > p.Fees || p.FeeDeposited != p.Fees-p.FeeBurned || p.FeeDeposited != 0 && p.FeeCollector.IsZero() {
			s.Unknown++
			continue
		}
		s.Known++
		add(&s.Fees, p.Fees)
		add(&s.FeeBurned, p.FeeBurned)
		add(&s.VotingRewardsPaid, p.VotingRewardPaid)
		switch p.FeeCollector {
		case addresses.IncineratorAddr:
			add(&s.IncineratorDeposits, p.FeeDeposited)
		case identity:
			add(&s.IdentityFeeDeposits, p.FeeDeposited)
		case r.subject:
			add(&s.VoteFeeDeposits, p.FeeDeposited)
		default:
			if p.Leader == identity {
				add(&s.OtherLeaderFeeDeposits, p.FeeDeposited)
			}
		}
	}
	s.CoverageIncomplete = s.CoverageIncomplete || s.Unknown != 0 || overflow
	s.TotalsAvailable = s.Known != 0 && !overflow
	if !s.TotalsAvailable {
		s.Fees, s.FeeBurned, s.IncineratorDeposits = 0, 0, 0
		s.IdentityFeeDeposits, s.VoteFeeDeposits, s.OtherLeaderFeeDeposits, s.VotingRewardsPaid = 0, 0, 0, 0
	}
	return s
}
