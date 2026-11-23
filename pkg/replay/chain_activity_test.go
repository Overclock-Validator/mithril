package replay

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func activityCarriers(n int) []CertificateCarrier {
	carriers := make([]CertificateCarrier, n)
	parent := leaderOutcomeEvidence(9, 1)
	for i := range carriers {
		c := leaderOutcomeChild(10+uint64(i)*2, byte(i%200+2), parent)
		carriers[i] = leaderSettlementCarrier(solana.PublicKey{}, c)
		carriers[i].ActivityKnown = true
		carriers[i].BankTime = 1000 + int64(i)*3
		carriers[i].Transactions = sealevel.BankTransactionCounts{Total: 4, NonVote: 3, Failed: 2, NonVoteFailed: 1, ComputeUnits: 600}
		parent = c.CertificateFoldEvidence
	}
	return carriers
}

func settleActivity(t *testing.T, carriers []CertificateCarrier) *CertificateOutcomes {
	t.Helper()
	r := NewCertificateOutcomes(solana.PublicKey{})
	for _, c := range carriers {
		require.True(t, r.Observe(c))
		r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, time.Unix(2000, 0))
	}
	return r
}

func TestChainActivityCountsExactIntervalNotReplaySpeed(t *testing.T) {
	carriers := activityCarriers(7)
	r := settleActivity(t, carriers)
	s := r.activitySnapshot(22)
	require.Equal(t, ChainActivitySummary{SampledAt: time.Unix(2000, 0).UTC(), Available: true,
		AnchorSlot: 14, LastSlot: 22, StartTime: 1006, EndTime: 1018, Blocks: 4,
		Transactions: sealevel.BankTransactionCounts{Total: 16, NonVote: 12, Failed: 8, NonVoteFailed: 4, ComputeUnits: 2400}}, s)
	// Replaying the same bank range at a different wall-clock speed cannot
	// change its counts or chain-time denominator.
	r.summary.SampledAt = time.Unix(200000, 0).UTC()
	want := s
	want.SampledAt = r.summary.SampledAt
	require.Equal(t, want, r.activitySnapshot(22))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(carriers[6])}, s.SampledAt)
	require.Equal(t, s, r.activitySnapshot(22), "duplicate fold cannot double-count activity")

	p := &Progress{}
	p.begin(10, nil)
	p.settledOutcomes(22, r.Snapshot(), LeaderOutcomeSummary{}, r.slotSnapshot(22), s, EpochOutcomeSummary{}, RewardHealthSummary{})
	p.rewound()
	got, _ := p.Snapshot()
	require.Equal(t, s, got.ChainActivity, "speculative rewind preserves committed activity")
	p.begin(24, nil)
	got, _ = p.Snapshot()
	require.Zero(t, got.ChainActivity, "a new attempt cannot borrow an old interval")
}

func TestChainActivityMissingConflictingAndSaturatedEvidence(t *testing.T) {
	cases := map[string]func([]CertificateCarrier){
		"missing clock":  func(c []CertificateCarrier) { c[2].ActivityKnown = false },
		"zero clock":     func(c []CertificateCarrier) { c[2].BankTime = 0 },
		"clock reversal": func(c []CertificateCarrier) { c[2].BankTime = 1010 },
		"no elapsed time": func(c []CertificateCarrier) {
			for i := range c {
				c[i].BankTime = 1000
			}
		},
		"parent slot":           func(c []CertificateCarrier) { c[2].ParentSlot++ },
		"parent ID":             func(c []CertificateCarrier) { c[2].ParentBlockID[0]++ },
		"execution parent":      func(c []CertificateCarrier) { c[2].ExecutionParentBankhash[0]++ },
		"missing ancestry":      func(c []CertificateCarrier) { c[2].HasParentBlockID = false },
		"saturated bank":        func(c []CertificateCarrier) { c[2].Transactions.Saturated = true },
		"invalid counts":        func(c []CertificateCarrier) { c[2].Transactions.NonVoteFailed = 3 },
		"invalid vote failures": func(c []CertificateCarrier) { c[2].Transactions.NonVoteFailed = 0 },
		"sum overflow":          func(c []CertificateCarrier) { c[2].Transactions.ComputeUnits = math.MaxUint64 },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			c := activityCarriers(4)
			change(c)
			r := settleActivity(t, c)
			require.False(t, r.activitySnapshot(16).Available)
		})
	}
	c := activityCarriers(4)
	r := settleActivity(t, c)
	require.False(t, r.activitySnapshot(15).Available, "future folded block cannot leak into an earlier interval")
	conflict := outcomeEvidence(c[2])
	conflict.Bankhash[0]++
	r.Settle([]CertificateFoldEvidence{conflict}, time.Unix(2001, 0))
	require.False(t, r.activitySnapshot(16).Available)
	missing := NewCertificateOutcomes(solana.PublicKey{})
	missing.Settle([]CertificateFoldEvidence{outcomeEvidence(c[0])}, time.Unix(2000, 0))
	require.True(t, missing.Observe(c[1]))
	missing.Settle([]CertificateFoldEvidence{outcomeEvidence(c[1])}, time.Unix(2000, 0))
	require.False(t, missing.activitySnapshot(12).Available)
}
