package replay

import (
	"strings"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestSlotHistorySharesExactParentRules(t *testing.T) {
	anchor := leaderOutcomeEvidence(9, 1)
	a := leaderOutcomeChild(10, 2, anchor)
	b := leaderOutcomeChild(41, 3, a.CertificateFoldEvidence)
	s := slotHistory(41, []LeaderOutcomeCarrier{b, a, a}, &anchor)
	require.Equal(t, uint64(10), s.WindowStart)
	require.Equal(t, uint64(41), s.WindowEnd)
	require.Equal(t, "p"+strings.Repeat("s", 30)+"p", s.Outcomes)
	require.False(t, s.CoverageIncomplete)

	b.ExecutionParentBankhash[0]++
	s = slotHistory(41, []LeaderOutcomeCarrier{a, b}, &anchor)
	require.Equal(t, "p"+strings.Repeat("?", 30)+"p", s.Outcomes)
	require.True(t, s.CoverageIncomplete)
	conflict := b
	conflict.BlockID[0]++
	s = slotHistory(41, []LeaderOutcomeCarrier{a, b, conflict}, &anchor)
	require.Equal(t, "p"+strings.Repeat("?", 31), s.Outcomes)
}

func TestSlotHistoryRetainsOnlyBoundedSettledWindow(t *testing.T) {
	r := NewCertificateOutcomes(solana.PublicKey{})
	parent := leaderOutcomeEvidence(0, 1)
	at := time.Unix(100, 0).UTC()
	for slot := uint64(1); slot <= certificateWindowLimit+100; slot++ {
		child := leaderOutcomeChild(slot, byte(slot%200+2), parent)
		require.True(t, r.Observe(leaderSettlementCarrier(solana.PublicKey{}, child)))
		r.Settle([]CertificateFoldEvidence{child.CertificateFoldEvidence}, at)
		parent = child.CertificateFoldEvidence
	}
	s := r.slotSnapshot(parent.Slot)
	require.Equal(t, strings.Repeat("p", slotHistoryLimit), s.Outcomes)
	require.Equal(t, parent.Slot-31, s.WindowStart)
	require.False(t, s.CoverageIncomplete)
	require.Equal(t, certificateWindowLimit, r.count)
	r.loseCoverage(at.Add(time.Second))
	require.True(t, r.slotSnapshot(parent.Slot).CoverageIncomplete)
	var absent *CertificateOutcomes
	require.Empty(t, absent.slotSnapshot(0))
}
