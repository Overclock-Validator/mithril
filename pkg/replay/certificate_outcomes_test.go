package replay

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func outcomeHash(n byte) (h solana.Hash) { h[0] = n; return }

func outcomeCarrier(slot uint64, id byte, subject solana.PublicKey) CertificateCarrier {
	return CertificateCarrier{Slot: slot, BlockID: outcomeHash(id), Bankhash: outcomeHash(id + 1),
		ParentSlot: slot - 1, ParentBlockID: outcomeHash(id + 2),
		Observation: block.CertificateObservation{VoteAccount: subject,
			Reward: block.CertificateMembership{TargetSlot: slot - 8, TargetEpoch: 4, AdmissionKnown: true, Eligible: true, Present: true, Included: true}}}
}

func outcomeEvidence(c CertificateCarrier) CertificateFoldEvidence {
	return CertificateFoldEvidence{Slot: c.Slot, Bankhash: c.Bankhash, BlockID: c.BlockID, HasBlockID: true}
}

func TestCertificateOutcomesExactFoldAndRewind(t *testing.T) {
	var subject solana.PublicKey
	subject[0] = 1
	r := NewCertificateOutcomes(subject)
	a := outcomeCarrier(10, 1, subject)
	b := outcomeCarrier(11, 2, subject)
	require.True(t, r.Observe(a))
	require.True(t, r.Observe(b))
	at := time.Unix(100, 0)
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(a)}, at)
	s := r.Snapshot()
	require.Equal(t, uint64(1), s.RewardIncluded)
	require.Equal(t, uint64(1), s.Pending)
	require.Equal(t, "selected_and_folded", s.Provenance)
	require.Equal(t, at.UTC(), s.SampledAt)
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(a)}, at)
	require.Equal(t, uint64(1), r.Snapshot().Carriers)
	r.RewindFrom(11)
	replacement := outcomeCarrier(11, 4, subject)
	require.True(t, r.Observe(replacement))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(replacement)}, at)
	require.Equal(t, uint64(2), r.Snapshot().RewardIncluded)
	bad := outcomeEvidence(replacement)
	bad.BlockID = outcomeHash(9)
	r.Settle([]CertificateFoldEvidence{bad}, at)
	require.True(t, r.Snapshot().CoverageIncomplete)
	require.Equal(t, uint64(1), r.Snapshot().Gaps)
}

func TestCertificateOutcomesMissingConflictAndOverflow(t *testing.T) {
	var subject solana.PublicKey
	subject[0] = 1
	r := NewCertificateOutcomes(subject)
	c := outcomeCarrier(20, 1, subject)
	require.True(t, r.Observe(c))
	changed := c
	changed.Bankhash = outcomeHash(99)
	require.False(t, r.Observe(changed))
	require.False(t, r.Observe(c)) // a later copy cannot erase the conflict
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, time.Now())
	require.Equal(t, uint64(1), r.Snapshot().Gaps)
	require.Equal(t, uint64(1), r.Snapshot().RewardUnknown)
	for i := uint64(100); i < 100+certificatePendingLimit; i++ {
		require.True(t, r.Observe(outcomeCarrier(i, 1, subject)))
	}
	require.False(t, r.Observe(outcomeCarrier(2000, 1, subject)))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(outcomeCarrier(2000, 1, subject))}, time.Now())
	require.Equal(t, uint64(2), r.Snapshot().Gaps)
	r.Settle(make([]CertificateFoldEvidence, certificatePendingLimit+1), time.Now())
	require.True(t, r.Snapshot().CoverageIncomplete)
	require.Zero(t, r.Snapshot().Pending)
}

func TestCertificateOutcomesWindowFinalDedupeAndEpoch(t *testing.T) {
	var subject solana.PublicKey
	subject[0] = 1
	r := NewCertificateOutcomes(subject)
	first := outcomeCarrier(20, 1, subject)
	first.Observation.Final = block.CertificateMembership{TargetSlot: 2, TargetEpoch: 3, Present: true, AdmissionKnown: true, Eligible: true, Included: true}
	first.Observation.FinalBlockID = outcomeHash(8)
	first.Observation.FinalFast = true
	second := outcomeCarrier(21, 2, subject)
	second.Observation.Final = first.Observation.Final
	second.Observation.FinalBlockID = first.Observation.FinalBlockID
	second.Observation.FinalFast = false
	require.True(t, r.Observe(first))
	require.True(t, r.Observe(second))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(second), outcomeEvidence(first)}, time.Now())
	s := r.Snapshot()
	require.Equal(t, uint64(1), s.FinalFast)
	require.Equal(t, uint64(1), s.FastMember)
	require.Equal(t, uint64(1), s.FinalDuplicates)
	require.Equal(t, uint64(1), s.FinalVariants)
	third := outcomeCarrier(22, 3, subject)
	third.Observation.Final = first.Observation.Final
	third.Observation.FinalBlockID = outcomeHash(9)
	require.True(t, r.Observe(third))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(third)}, time.Now())
	require.Equal(t, uint64(1), r.Snapshot().FinalConflict)
	require.Zero(t, r.Snapshot().FinalFast)
	for i := uint64(23); i < 23+certificateWindowLimit; i++ {
		c := outcomeCarrier(i, 1, subject)
		require.True(t, r.Observe(c))
		r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, time.Now())
	}
	last := outcomeCarrier(23+certificateWindowLimit, 1, subject)
	last.Observation.Final = first.Observation.Final
	last.Observation.FinalBlockID = first.Observation.FinalBlockID
	last.Observation.FinalFast = true
	require.True(t, r.Observe(last))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(last)}, time.Now())
	s = r.Snapshot()
	require.Equal(t, uint64(certificateWindowLimit), s.Carriers)
	require.Equal(t, uint64(1), s.FinalFast)
	require.Zero(t, s.FinalDuplicates)
	require.Zero(t, s.FinalConflict)
}

func TestCertificateOutcomesUnknownAdmissionAndSlowFirst(t *testing.T) {
	var subject solana.PublicKey
	subject[0] = 1
	r := NewCertificateOutcomes(subject)
	a := outcomeCarrier(40, 1, subject)
	a.Observation.Reward.AdmissionKnown = false
	a.Observation.Final = block.CertificateMembership{TargetSlot: 31, TargetEpoch: 6, Present: true, AdmissionKnown: true, Eligible: true}
	a.Observation.FinalBlockID = outcomeHash(8)
	b := outcomeCarrier(41, 2, subject)
	b.Observation.Reward.Present = false
	b.Observation.Reward.Included = false
	b.Observation.Final = a.Observation.Final
	b.Observation.Final.TargetEpoch = 7
	b.Observation.FinalBlockID = a.Observation.FinalBlockID
	b.Observation.FinalFast = true
	c := outcomeCarrier(42, 3, subject)
	c.Observation.Final = a.Observation.Final
	c.Observation.FinalBlockID = a.Observation.FinalBlockID
	c.Observation.FinalFast = true
	require.True(t, r.Observe(a))
	require.True(t, r.Observe(b))
	require.True(t, r.Observe(c))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(a), outcomeEvidence(b), outcomeEvidence(c)}, time.Now())
	s := r.Snapshot()
	require.Equal(t, uint64(1), s.RewardUnknown)
	require.True(t, s.CoverageIncomplete)
	require.Equal(t, uint64(1), s.RewardExcluded)
	require.Equal(t, uint64(1), s.FinalSlow)
	require.Equal(t, uint64(1), s.FinalFast)
	require.Equal(t, uint64(1), s.FastNonmember)
	require.Equal(t, uint64(1), s.FinalDuplicates)
	require.Equal(t, uint64(1), s.FinalVariants)
}

func TestCertificateOutcomesSameBatchDuplicatesAndMissingSlots(t *testing.T) {
	var subject solana.PublicKey
	subject[0] = 1
	r := NewCertificateOutcomes(subject)
	a, b := outcomeCarrier(100, 1, subject), outcomeCarrier(110, 2, subject)
	require.True(t, r.Observe(a))
	require.True(t, r.Observe(b))
	ea, eb := outcomeEvidence(a), outcomeEvidence(b)
	r.Settle([]CertificateFoldEvidence{eb, ea, ea}, time.Now())
	s := r.Snapshot()
	require.Zero(t, s.Gaps)
	require.Equal(t, uint64(9), s.SlotsWithoutCarrier)
	require.True(t, s.CoverageIncomplete)
	require.Equal(t, uint64(2), s.RewardIncluded)
	conflict := ea
	conflict.Bankhash = outcomeHash(99)
	r.Settle([]CertificateFoldEvidence{ea, conflict}, time.Now())
	require.Equal(t, uint64(1), r.Snapshot().Gaps)
}
