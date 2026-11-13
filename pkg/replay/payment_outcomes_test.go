package replay

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func paymentCarrier(slot uint64, identity, vote solana.PublicKey) CertificateCarrier {
	c := outcomeCarrier(slot, 1, vote)
	c.Payments = block.PaymentObservation{Identity: identity, VoteAccount: vote, Leader: identity, FeeCollector: identity,
		FeesKnown: true, RewardsKnown: true, Fees: 10, FeeBurned: 3, FeeDeposited: 7, VotingRewardPaid: 11}
	return c
}

func TestPaymentOutcomesExactFoldAndRewind(t *testing.T) {
	identity, vote := solana.PublicKey{1}, solana.PublicKey{2}
	r := NewCertificateOutcomes(vote)
	a, abandoned := paymentCarrier(10, identity, vote), paymentCarrier(11, identity, vote)
	abandoned.Payments.VotingRewardPaid = 999
	require.True(t, r.Observe(a))
	require.True(t, r.Observe(abandoned))
	require.Equal(t, PaymentOutcomeSummary{}, r.paymentSnapshot(identity), "pending execution is not a payment outcome")
	at := time.Unix(100, 0)
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(a)}, at)
	first := r.paymentSnapshot(identity)
	require.Equal(t, uint64(1), first.Known)
	require.Equal(t, uint64(10), first.Fees)
	require.Equal(t, uint64(11), first.VotingRewardsPaid)
	require.True(t, first.TotalsAvailable)
	require.False(t, first.CoverageIncomplete)
	r.RewindFrom(11)
	replacement := paymentCarrier(11, identity, vote)
	replacement.BlockID = outcomeHash(9)
	replacement.Payments.FeeCollector = vote
	replacement.Payments.VotingRewardPaid = 17
	require.True(t, r.Observe(replacement))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(replacement)}, at)
	s := r.paymentSnapshot(identity)
	require.Equal(t, uint64(2), s.Carriers)
	require.Equal(t, uint64(20), s.Fees)
	require.Equal(t, uint64(28), s.VotingRewardsPaid)
	require.Equal(t, uint64(7), s.IdentityFeeDeposits)
	require.Equal(t, uint64(7), s.VoteFeeDeposits)
	require.Equal(t, uint64(11), first.VotingRewardsPaid, "prior summaries are detached")
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(replacement)}, at)
	require.Equal(t, s, r.paymentSnapshot(identity), "duplicate folds cannot double count")
	conflict := outcomeEvidence(replacement)
	conflict.Bankhash = outcomeHash(99)
	r.Settle([]CertificateFoldEvidence{conflict}, at)
	conflicted := r.paymentSnapshot(identity)
	require.Equal(t, uint64(1), conflicted.Unknown)
	require.Equal(t, uint64(1), conflicted.Known)
	require.Equal(t, uint64(11), conflicted.VotingRewardsPaid)
	require.True(t, conflicted.CoverageIncomplete)
}

func TestPaymentOutcomesMissingBindingAndInvalidArithmetic(t *testing.T) {
	identity, vote := solana.PublicKey{1}, solana.PublicKey{2}
	for name, change := range map[string]func(*block.PaymentObservation){
		"missing observation": func(p *block.PaymentObservation) { *p = block.PaymentObservation{} },
		"wrong identity":      func(p *block.PaymentObservation) { p.Identity = solana.PublicKey{9} },
		"wrong vote":          func(p *block.PaymentObservation) { p.VoteAccount = solana.PublicKey{9} },
		"unknown fees":        func(p *block.PaymentObservation) { p.FeesKnown = false },
		"unknown rewards":     func(p *block.PaymentObservation) { p.RewardsKnown = false },
		"excess burn":         func(p *block.PaymentObservation) { p.FeeBurned = p.Fees + 1 },
		"wrong deposit":       func(p *block.PaymentObservation) { p.FeeDeposited++ },
		"missing collector":   func(p *block.PaymentObservation) { p.FeeCollector = solana.PublicKey{} },
	} {
		t.Run(name, func(t *testing.T) {
			r := NewCertificateOutcomes(vote)
			c := paymentCarrier(10, identity, vote)
			change(&c.Payments)
			require.True(t, r.Observe(c))
			r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, time.Unix(100, 0))
			s := r.paymentSnapshot(identity)
			require.Equal(t, uint64(1), s.Unknown)
			require.Zero(t, s.Known)
			require.False(t, s.TotalsAvailable)
			require.True(t, s.CoverageIncomplete)
			require.Zero(t, s.Fees)
			require.Zero(t, s.VotingRewardsPaid)
		})
	}
	r := NewCertificateOutcomes(vote)
	c := paymentCarrier(10, identity, vote)
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, time.Unix(100, 0))
	require.Equal(t, uint64(1), r.paymentSnapshot(identity).Unknown, "a fold without its matching observed carrier is unknown")
	var absent *CertificateOutcomes
	require.Equal(t, PaymentOutcomeSummary{}, absent.paymentSnapshot(identity))
	require.Equal(t, PaymentOutcomeSummary{}, r.paymentSnapshot(solana.PublicKey{}))
	require.Equal(t, PaymentOutcomeSummary{}, NewCertificateOutcomes(solana.PublicKey{}).paymentSnapshot(identity))
}

func TestPaymentOutcomesCheckedTotals(t *testing.T) {
	identity, vote := solana.PublicKey{1}, solana.PublicKey{2}
	for _, second := range []uint64{0, 1} {
		r := NewCertificateOutcomes(vote)
		for i, amount := range []uint64{math.MaxUint64, second} {
			c := paymentCarrier(uint64(10+i), identity, vote)
			c.Payments.Fees, c.Payments.FeeBurned, c.Payments.FeeDeposited = amount, 0, amount
			c.Payments.VotingRewardPaid = amount
			require.True(t, r.Observe(c))
			r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, time.Unix(100, 0))
		}
		s := r.paymentSnapshot(identity)
		require.Equal(t, uint64(2), s.Known)
		require.Equal(t, second == 0, s.TotalsAvailable)
		require.Equal(t, second != 0, s.CoverageIncomplete)
		if second == 0 {
			require.Equal(t, uint64(math.MaxUint64), s.Fees)
			require.Equal(t, uint64(math.MaxUint64), s.IdentityFeeDeposits)
			require.Equal(t, uint64(math.MaxUint64), s.VotingRewardsPaid)
		} else {
			require.Zero(t, s.Fees)
			require.Zero(t, s.FeeBurned)
			require.Zero(t, s.IncineratorDeposits)
			require.Zero(t, s.IdentityFeeDeposits)
			require.Zero(t, s.VoteFeeDeposits)
			require.Zero(t, s.OtherLeaderFeeDeposits)
			require.Zero(t, s.VotingRewardsPaid)
		}
	}
}

func TestPaymentOutcomesKnownZero(t *testing.T) {
	identity, vote := solana.PublicKey{1}, solana.PublicKey{2}
	r := NewCertificateOutcomes(vote)
	c := paymentCarrier(10, identity, vote)
	c.Payments = block.PaymentObservation{Identity: identity, VoteAccount: vote, FeesKnown: true, RewardsKnown: true}
	require.True(t, r.Observe(c))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, time.Unix(100, 0))
	s := r.paymentSnapshot(identity)
	require.True(t, s.TotalsAvailable)
	require.False(t, s.CoverageIncomplete)
	require.Equal(t, uint64(1), s.Known)
	require.Zero(t, s.Unknown)
	require.Zero(t, s.Fees)
	require.Zero(t, s.VotingRewardsPaid)
}

func TestPaymentOutcomesRetentionCoverageAndAttemptReset(t *testing.T) {
	identity, vote := solana.PublicKey{1}, solana.PublicKey{2}
	r := NewCertificateOutcomes(vote)
	for slot := uint64(10); slot <= 10+certificateWindowLimit; slot++ {
		c := paymentCarrier(slot, identity, vote)
		if slot == 10 {
			c.Payments.VotingRewardPaid = 1_000_000
		}
		require.True(t, r.Observe(c))
		r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, time.Unix(100, 0))
	}
	s := r.paymentSnapshot(identity)
	require.Equal(t, uint64(certificateWindowLimit), s.Carriers)
	require.Equal(t, uint64(11), s.FirstCarrier)
	require.Equal(t, uint64(10+certificateWindowLimit), s.LastCarrier)
	require.Equal(t, uint64(11*certificateWindowLimit), s.VotingRewardsPaid)
	require.False(t, s.CoverageIncomplete)
	c := paymentCarrier(s.LastCarrier+2, identity, vote)
	require.True(t, r.Observe(c))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, time.Unix(101, 0))
	require.True(t, r.paymentSnapshot(identity).CoverageIncomplete, "unrepresented slots cannot be complete coverage")
	r.loseCoverage(time.Unix(102, 0))
	require.True(t, r.paymentSnapshot(identity).CoverageIncomplete)
	require.True(t, r.paymentSnapshot(identity).TotalsAvailable, "known retained subtotal remains available with lost coverage")
	var progress Progress
	progress.latest.Store(&ProgressSnapshot{Attempt: 1, PaymentOutcomes: s})
	old, ok := progress.Snapshot()
	require.True(t, ok)
	progress.begin(1000, nil)
	reset, ok := progress.Snapshot()
	require.True(t, ok)
	require.Equal(t, PaymentOutcomeSummary{}, reset.PaymentOutcomes)
	require.Equal(t, uint64(2), reset.Attempt)
	require.Equal(t, s, old.PaymentOutcomes)
}
