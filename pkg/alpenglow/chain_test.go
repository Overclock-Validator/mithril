package alpenglow

import (
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestChainTrackerRequiresVerifiedCertificatesByDefault(t *testing.T) {
	tracker := NewChainTracker()

	_, err := tracker.ObserveCertificate(Certificate{Type: CertificateSkip, Slot: 11})
	if err != nil {
		t.Fatalf("observe unverified skip certificate: %v", err)
	}

	if decision, ok := tracker.NextDecision(10); ok {
		t.Fatalf("expected unverified certificate not to drive a decision, got %+v", decision)
	}

	snap := tracker.Snapshot()
	if snap.CertificatesObserved != 1 || snap.CertificatesAccepted != 0 || snap.CertificatesIgnoredUntrusted != 1 {
		t.Fatalf("unexpected snapshot after unverified cert: %+v", snap)
	}

	_, err = tracker.ObserveCertificate(Certificate{Type: CertificateSkip, Slot: 12, SignatureVerified: true})
	if err != nil {
		t.Fatalf("observe verified skip certificate: %v", err)
	}

	decision, ok := tracker.NextDecision(11)
	if !ok {
		t.Fatalf("expected verified skip certificate to drive a decision")
	}
	if decision.Kind != ChainDecisionKindSkip || decision.Slot != 12 || decision.CertificateType != CertificateSkip {
		t.Fatalf("unexpected skip decision: %+v", decision)
	}
}

func TestChainTrackerCanAllowUnverifiedCertificatesForDiagnostics(t *testing.T) {
	tracker := NewChainTrackerWithConfig(ChainConfig{RequireVerifiedCertificates: false})

	_, err := tracker.ObserveCertificate(Certificate{Type: CertificateSkip, Slot: 11})
	if err != nil {
		t.Fatalf("observe skip certificate: %v", err)
	}

	decision, ok := tracker.NextDecision(10)
	if !ok {
		t.Fatalf("expected diagnostic tracker to accept unverified skip certificate")
	}
	if decision.Kind != ChainDecisionKindSkip || decision.Slot != 11 {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

func TestChainTrackerCanRequireStakeVerifiedCertificates(t *testing.T) {
	tracker := NewChainTrackerWithConfig(ChainConfig{
		RequireVerifiedCertificates:      false,
		RequireStakeVerifiedCertificates: true,
	})

	_, err := tracker.ObserveCertificate(Certificate{Type: CertificateSkip, Slot: 11})
	if err != nil {
		t.Fatalf("observe untrusted skip certificate: %v", err)
	}
	if decision, ok := tracker.NextDecision(10); ok {
		t.Fatalf("expected stake-unverified certificate not to drive a decision, got %+v", decision)
	}

	_, err = tracker.ObserveCertificate(Certificate{Type: CertificateSkip, Slot: 12, StakeVerified: true})
	if err != nil {
		t.Fatalf("observe stake-verified skip certificate: %v", err)
	}
	decision, ok := tracker.NextDecision(11)
	if !ok {
		t.Fatalf("expected stake-verified certificate to drive a decision")
	}
	if decision.Kind != ChainDecisionKindSkip || decision.Slot != 12 {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

func TestChainTrackerResolvesCertifiedBlock(t *testing.T) {
	tracker := NewChainTracker()
	blockID := BlockID{Slot: 11, Hash: chainTestHash(1)}

	_, err := tracker.ObserveCertificate(Certificate{
		Type:              CertificateNotarize,
		Slot:              blockID.Slot,
		BlockHash:         blockID.Hash,
		SignatureVerified: true,
	})
	if err != nil {
		t.Fatalf("observe notarize certificate: %v", err)
	}

	decision, ok := tracker.NextDecision(10)
	if !ok {
		t.Fatalf("expected certified block decision")
	}
	if decision.Kind != ChainDecisionKindBlock || decision.Block != blockID || decision.Observed {
		t.Fatalf("unexpected decision before replay observation: %+v", decision)
	}

	parentHash := chainTestHash(9)
	tracker.ObserveReplayBlock(ReplayBlockObservation{
		Block:      blockID,
		ParentSlot: 10,
		ParentHash: parentHash,
	})

	decision, ok = tracker.NextDecision(10)
	if !ok {
		t.Fatalf("expected certified block decision after replay observation")
	}
	if decision.Kind != ChainDecisionKindBlock || !decision.Observed || decision.ParentSlot != 10 || decision.ParentHash != parentHash {
		t.Fatalf("unexpected decision after replay observation: %+v", decision)
	}
}

func TestChainTrackerDetectsCertifiedBlockConflict(t *testing.T) {
	tracker := NewChainTracker()

	for _, hash := range []solana.Hash{chainTestHash(1), chainTestHash(2)} {
		_, err := tracker.ObserveCertificate(Certificate{
			Type:              CertificateNotarize,
			Slot:              11,
			BlockHash:         hash,
			SignatureVerified: true,
		})
		if err != nil {
			t.Fatalf("observe notarize certificate: %v", err)
		}
	}

	decision, ok := tracker.NextDecision(10)
	if !ok {
		t.Fatalf("expected conflict decision")
	}
	if decision.Kind != ChainDecisionKindConflict || len(decision.Candidates) != 2 {
		t.Fatalf("unexpected conflict decision: %+v", decision)
	}
	if snap := tracker.Snapshot(); snap.ConflictingSlots != 1 {
		t.Fatalf("expected one conflicting slot, got snapshot %+v", snap)
	}
}

// Notarize + skip certificates legally coexist (voters may notarize a block and
// later skip-fallback; whitepaper Lemmas 21/26 reserve exclusivity for finalized
// blocks). The slot resolves to the certified skip — a conflict here would false-halt
// on normal fallback traffic.
func TestChainTrackerNotarizePlusSkipResolvesToSkip(t *testing.T) {
	tracker := NewChainTracker()

	_, err := tracker.ObserveCertificate(Certificate{
		Type:              CertificateNotarize,
		Slot:              11,
		BlockHash:         chainTestHash(1),
		SignatureVerified: true,
	})
	if err != nil {
		t.Fatalf("observe notarize certificate: %v", err)
	}
	_, err = tracker.ObserveCertificate(Certificate{
		Type:              CertificateSkip,
		Slot:              11,
		SignatureVerified: true,
	})
	if err != nil {
		t.Fatalf("observe skip certificate: %v", err)
	}

	decision, ok := tracker.NextDecision(10)
	if !ok || decision.Kind != ChainDecisionKindSkip {
		t.Fatalf("notarize+skip must resolve to skip, got %+v (ok=%v)", decision, ok)
	}
}

func TestChainTrackerFastFinalizationDerivesOmittedSkips(t *testing.T) {
	tracker := NewChainTracker()
	blockID := BlockID{Slot: 15, Hash: chainTestHash(15)}

	_, err := tracker.ObserveCertificate(Certificate{
		Type:              CertificateFinalizeFast,
		Slot:              blockID.Slot,
		BlockHash:         blockID.Hash,
		SignatureVerified: true,
	})
	if err != nil {
		t.Fatalf("observe fast finalization certificate: %v", err)
	}
	tracker.ObserveReplayBlock(ReplayBlockObservation{
		Block:      blockID,
		ParentSlot: 12,
		ParentHash: chainTestHash(12),
	})

	path := tracker.ResolvePath(12, 4)
	if len(path.Decisions) != 3 {
		t.Fatalf("expected two skips and one block decision, got %+v", path)
	}
	for i, wantSlot := range []uint64{13, 14} {
		decision := path.Decisions[i]
		if decision.Kind != ChainDecisionKindSkip || decision.Slot != wantSlot || !decision.Indirect || decision.ViaFinalized != blockID {
			t.Fatalf("unexpected indirect skip decision %d: %+v", i, decision)
		}
	}
	blockDecision := path.Decisions[2]
	if blockDecision.Kind != ChainDecisionKindBlock || blockDecision.Block != blockID || !blockDecision.Observed {
		t.Fatalf("unexpected finalized block decision: %+v", blockDecision)
	}
}

// The decision version advances on cert acceptance AND on a replay observation
// that derives new decisiveness (here: the parent link that produces the
// omitted indirect skips) — the exact case the switch sweep would otherwise
// miss when gated on certificate count alone.
func TestChainTrackerDecisionVersionAdvancesOnReplayDerivation(t *testing.T) {
	tracker := NewChainTracker()
	blockID := BlockID{Slot: 15, Hash: chainTestHash(15)}

	v0 := tracker.DecisionVersion()
	if _, err := tracker.ObserveCertificate(Certificate{
		Type: CertificateFinalizeFast, Slot: blockID.Slot, BlockHash: blockID.Hash, SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe cert: %v", err)
	}
	v1 := tracker.DecisionVersion()
	if v1 <= v0 {
		t.Fatalf("cert acceptance must advance the decision version (%d -> %d)", v0, v1)
	}

	// The replay observation supplies the parent link that derives the omitted
	// skips (13, 14) — a decisiveness change with NO new certificate.
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: blockID, ParentSlot: 12, ParentHash: chainTestHash(12)})
	v2 := tracker.DecisionVersion()
	if v2 <= v1 {
		t.Fatalf("replay-derived indirect skips must advance the decision version (%d -> %d)", v1, v2)
	}
}

func TestChainTrackerSlowFinalizationRequiresNotarizationCertificate(t *testing.T) {
	tracker := NewChainTracker()
	blockID := BlockID{Slot: 15, Hash: chainTestHash(15)}

	_, err := tracker.ObserveCertificate(Certificate{
		Type:              CertificateFinalize,
		Slot:              blockID.Slot,
		SignatureVerified: true,
	})
	if err != nil {
		t.Fatalf("observe finalization certificate: %v", err)
	}
	tracker.ObserveReplayBlock(ReplayBlockObservation{
		Block:      blockID,
		ParentSlot: 12,
		ParentHash: chainTestHash(12),
	})

	if snap := tracker.Snapshot(); snap.DirectFinalizedBlocks != 0 || snap.IndirectSkips != 0 {
		t.Fatalf("finalization without notarization should not identify a block, got snapshot %+v", snap)
	}

	_, err = tracker.ObserveCertificate(Certificate{
		Type:              CertificateNotarize,
		Slot:              blockID.Slot,
		BlockHash:         blockID.Hash,
		SignatureVerified: true,
	})
	if err != nil {
		t.Fatalf("observe notarization certificate: %v", err)
	}

	path := tracker.ResolvePath(12, 4)
	if len(path.Decisions) != 3 {
		t.Fatalf("expected slow-finalized path, got %+v", path)
	}
	if path.Decisions[0].Kind != ChainDecisionKindSkip || path.Decisions[0].Slot != 13 {
		t.Fatalf("unexpected first decision: %+v", path.Decisions[0])
	}
	if path.Decisions[2].Kind != ChainDecisionKindBlock || path.Decisions[2].Block != blockID {
		t.Fatalf("unexpected block decision: %+v", path.Decisions[2])
	}
}

func TestChainTrackerRejectsBlockCertificateWithEmptyHash(t *testing.T) {
	tracker := NewChainTracker()

	if _, err := tracker.ObserveCertificate(Certificate{
		Type:              CertificateNotarize,
		Slot:              11,
		SignatureVerified: true,
	}); err == nil {
		t.Fatalf("expected empty block hash to be rejected")
	}
}

func chainTestHash(seed byte) solana.Hash {
	var hash solana.Hash
	hash[0] = seed
	hash[31] = seed
	return hash
}
