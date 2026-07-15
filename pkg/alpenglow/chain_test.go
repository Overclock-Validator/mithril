package alpenglow

import (
	"testing"

	"github.com/gagliardetto/solana-go"
)

func requireChainFinalized(t *testing.T, tracker *ChainTracker, block BlockID, certType CertificateType) {
	t.Helper()
	if err := tracker.ObserveFinalized(block, certType); err != nil {
		t.Fatalf("observe %s finalization for %s: %v", certType, block, err)
	}
}

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

func TestChainTrackerAppliesCertificateAfterDeferredVerification(t *testing.T) {
	tracker := NewChainTrackerWithConfig(ChainConfig{
		RequireVerifiedCertificates:      true,
		RequireStakeVerifiedCertificates: true,
	})
	cert := Certificate{Type: CertificateSkip, Slot: 11}
	if _, err := tracker.ObserveCertificate(cert); err != nil {
		t.Fatalf("observe pending certificate: %v", err)
	}
	cert.SignatureVerified = true
	cert.StakeVerified = true
	update, err := tracker.ObserveCertificate(cert)
	if err != nil {
		t.Fatalf("observe verified certificate: %v", err)
	}
	if update.New || !update.Trusted {
		t.Fatalf("verified retry update = %+v", update)
	}
	decision, ok := tracker.NextDecision(10)
	if !ok || decision.Kind != ChainDecisionKindSkip {
		t.Fatalf("verified retry was not applied: %+v (ok=%v)", decision, ok)
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

func TestChainTrackerNotarizeAndSkipResolvesToSkip(t *testing.T) {
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
	if !ok {
		t.Fatalf("expected skip decision")
	}
	if decision.Kind != ChainDecisionKindSkip {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if snap := tracker.Snapshot(); snap.ConflictingSlots != 0 {
		t.Fatalf("pre-finality notarize+skip is legal, got snapshot %+v", snap)
	}
}

func TestChainTrackerFinalizedBlockAndSkipConflicts(t *testing.T) {
	tracker := NewChainTracker()
	block := BlockID{Slot: 11, Hash: chainTestHash(1)}
	fast := Certificate{Type: CertificateFinalizeFast, Slot: block.Slot, BlockHash: block.Hash, SignatureVerified: true}
	if _, err := tracker.ObserveCertificate(fast); err != nil {
		t.Fatalf("observe %s: %v", fast.Type, err)
	}
	requireChainFinalized(t, tracker, block, CertificateFinalizeFast)
	if _, err := tracker.ObserveCertificate(Certificate{Type: CertificateSkip, Slot: block.Slot, SignatureVerified: true}); err != nil {
		t.Fatalf("observe skip: %v", err)
	}

	decision, ok := tracker.NextDecision(10)
	if !ok || decision.Kind != ChainDecisionKindConflict {
		t.Fatalf("expected finalized/skip conflict, got %+v (ok=%v)", decision, ok)
	}
	if tracker.SkipCertifiedAt(11) {
		t.Fatalf("conflicted skip must fail closed")
	}
	if _, _, ok := tracker.CertifiedBlockAt(11); ok {
		t.Fatalf("conflicted block must fail closed")
	}
}

func TestChainTrackerFallbackSiblingsAreLegalAndNonDecisive(t *testing.T) {
	tracker := NewChainTracker()
	for seed := byte(1); seed <= 2; seed++ {
		if _, err := tracker.ObserveCertificate(Certificate{
			Type: CertificateNotarizeFallback, Slot: 11, BlockHash: chainTestHash(seed), SignatureVerified: true,
		}); err != nil {
			t.Fatalf("observe fallback %d: %v", seed, err)
		}
	}
	if decision, ok := tracker.NextDecision(10); ok {
		t.Fatalf("fallback-only siblings must wait, got %+v", decision)
	}
	if _, ok := tracker.KnownBlockAtSlot(11); ok {
		t.Fatalf("fallback-only parent identity must remain ambiguous")
	}
	if tracker.FinalityConflictAt(11) {
		t.Fatalf("legal fallback siblings were marked conflicted")
	}
}

func TestChainTrackerEnforcesCertifiedBlockProtocolBound(t *testing.T) {
	tracker := NewChainTracker()
	for seed := byte(1); seed <= maxCertifiedBlocksPerSlot; seed++ {
		if _, err := tracker.ObserveCertificate(Certificate{
			Type: CertificateNotarizeFallback, Slot: 11, BlockHash: chainTestHash(seed), SignatureVerified: true,
		}); err != nil {
			t.Fatalf("observe legal fallback %d: %v", seed, err)
		}
	}
	if tracker.FinalityConflictAt(11) {
		t.Fatalf("protocol permits %d fallback blocks", maxCertifiedBlocksPerSlot)
	}
	if _, err := tracker.ObserveCertificate(Certificate{
		Type: CertificateNotarizeFallback, Slot: 11, BlockHash: chainTestHash(8), SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe over-bound fallback: %v", err)
	}
	if !tracker.FinalityConflictAt(11) {
		t.Fatalf("expected over-bound fallback set to fail closed")
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
	requireChainFinalized(t, tracker, blockID, CertificateFinalizeFast)
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
	if snap := tracker.Snapshot(); snap.DirectFinalizedBlocks != 0 {
		t.Fatalf("certificate pair bypassed pool finalization: %+v", snap)
	}
	requireChainFinalized(t, tracker, blockID, CertificateFinalize)

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
	if path.Decisions[2].CertificateType != CertificateFinalize {
		t.Fatalf("slow-finalized block decision cert type = %q, want %q", path.Decisions[2].CertificateType, CertificateFinalize)
	}
}

// TestChainTrackerFinalizedAncestryWalkDerivesDeepSkips models the
// bootstrap-catchup stall: slots 13-16 were skipped before the Votor listener
// connected (no certs for them, nor for blocks 17-19), but block 20 is fast-
// finalized live. Observing the ancestor chain 20 -> 19 -> 18 -> 17 (parent
// 12) must derive indirect skips for 13-16 even though only block 20 has a
// certificate.
func TestChainTrackerFinalizedAncestryWalkDerivesDeepSkips(t *testing.T) {
	tracker := NewChainTracker()

	block17 := BlockID{Slot: 17, Hash: chainTestHash(17)}
	block18 := BlockID{Slot: 18, Hash: chainTestHash(18)}
	block19 := BlockID{Slot: 19, Hash: chainTestHash(19)}
	block20 := BlockID{Slot: 20, Hash: chainTestHash(20)}

	if _, err := tracker.ObserveCertificate(Certificate{
		Type:              CertificateFinalizeFast,
		Slot:              block20.Slot,
		BlockHash:         block20.Hash,
		SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe fast finalization certificate: %v", err)
	}
	requireChainFinalized(t, tracker, block20, CertificateFinalizeFast)

	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: block20, ParentSlot: 19, ParentHash: block19.Hash})
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: block19, ParentSlot: 18, ParentHash: block18.Hash})
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: block18, ParentSlot: 17, ParentHash: block17.Hash})

	// Chain not linked back past 17 yet: no skips derivable below it.
	if decision, ok := tracker.NextDecision(12); ok {
		t.Fatalf("expected no decision before ancestry reaches slot 17, got %+v", decision)
	}

	// Observing 17 (parent 12) closes the chain; 13-16 must become skips.
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: block17, ParentSlot: 12, ParentHash: chainTestHash(12)})

	for slot := uint64(13); slot <= 16; slot++ {
		decision, ok := tracker.NextDecision(slot - 1)
		if !ok {
			t.Fatalf("expected indirect skip decision for slot %d", slot)
		}
		if decision.Kind != ChainDecisionKindSkip || decision.Slot != slot || !decision.Indirect || decision.ViaFinalized != block17 {
			t.Fatalf("unexpected decision for slot %d: %+v", slot, decision)
		}
	}

	snap := tracker.Snapshot()
	if snap.IndirectSkips != 4 {
		t.Fatalf("expected 4 indirect skips, got snapshot %+v", snap)
	}
	if snap.FinalizedAncestorBlocks != 4 {
		t.Fatalf("expected 4 finalized ancestors (19, 18, 17, 12), got snapshot %+v", snap)
	}
}

// TestChainTrackerFinalizedAncestryWalkHandlesInOrderObservation covers the
// repair path delivering blocks oldest-first: ancestors observed before the
// finalize certificate arrives must still produce the deep skips.
func TestChainTrackerFinalizedAncestryWalkHandlesInOrderObservation(t *testing.T) {
	tracker := NewChainTracker()

	block17 := BlockID{Slot: 17, Hash: chainTestHash(17)}
	block18 := BlockID{Slot: 18, Hash: chainTestHash(18)}
	block20 := BlockID{Slot: 20, Hash: chainTestHash(20)}

	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: block17, ParentSlot: 12, ParentHash: chainTestHash(12)})
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: block18, ParentSlot: 17, ParentHash: block17.Hash})
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: block20, ParentSlot: 18, ParentHash: block18.Hash})

	if _, err := tracker.ObserveCertificate(Certificate{
		Type:              CertificateFinalizeFast,
		Slot:              block20.Slot,
		BlockHash:         block20.Hash,
		SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe fast finalization certificate: %v", err)
	}
	requireChainFinalized(t, tracker, block20, CertificateFinalizeFast)

	// Gap 13-16 below ancestor 17, plus gap 19 between 18 and 20.
	for _, slot := range []uint64{13, 14, 15, 16, 19} {
		decision, ok := tracker.NextDecision(slot - 1)
		if !ok {
			t.Fatalf("expected indirect skip decision for slot %d", slot)
		}
		if decision.Kind != ChainDecisionKindSkip || decision.Slot != slot || !decision.Indirect {
			t.Fatalf("unexpected decision for slot %d: %+v", slot, decision)
		}
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

func TestChainTrackerFinalizedAncestorUsesFinalizationCertType(t *testing.T) {
	tracker := NewChainTracker()

	type link struct {
		slot       uint64
		parentSlot uint64
		seed       byte
	}
	links := []link{
		{slot: 11, parentSlot: 10, seed: 11},
		{slot: 12, parentSlot: 11, seed: 12},
		{slot: 13, parentSlot: 12, seed: 13},
	}
	for _, entry := range links {
		blockID := BlockID{Slot: entry.slot, Hash: chainTestHash(entry.seed)}
		if _, err := tracker.ObserveCertificate(Certificate{
			Type:              CertificateNotarize,
			Slot:              blockID.Slot,
			BlockHash:         blockID.Hash,
			SignatureVerified: true,
		}); err != nil {
			t.Fatalf("observe notarize certificate for slot %d: %v", entry.slot, err)
		}
		parentHash := chainTestHash(10)
		if entry.parentSlot != 10 {
			parentHash = chainTestHash(byte(entry.parentSlot))
		}
		tracker.ObserveReplayBlock(ReplayBlockObservation{
			Block:      blockID,
			ParentSlot: entry.parentSlot,
			ParentHash: parentHash,
		})
	}

	tip := BlockID{Slot: 13, Hash: chainTestHash(13)}
	if _, err := tracker.ObserveCertificate(Certificate{
		Type:              CertificateFinalizeFast,
		Slot:              tip.Slot,
		BlockHash:         tip.Hash,
		SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe fast finalization certificate: %v", err)
	}
	requireChainFinalized(t, tracker, tip, CertificateFinalizeFast)

	for _, slot := range []uint64{11, 12, 13} {
		decision, ok := tracker.NextDecision(slot - 1)
		if !ok {
			t.Fatalf("expected decision for slot %d", slot)
		}
		if decision.Kind != ChainDecisionKindBlock || decision.Block.Slot != slot {
			t.Fatalf("unexpected decision for slot %d: %+v", slot, decision)
		}
		if decision.CertificateType != CertificateFinalizeFast {
			t.Fatalf("slot %d cert type = %q, want %q", slot, decision.CertificateType, CertificateFinalizeFast)
		}
	}
}

func TestChainTrackerFinalizedAncestorWithoutNotarizeCert(t *testing.T) {
	tracker := NewChainTracker()

	block11 := BlockID{Slot: 11, Hash: chainTestHash(11)}
	block12 := BlockID{Slot: 12, Hash: chainTestHash(12)}
	block13 := BlockID{Slot: 13, Hash: chainTestHash(13)}

	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: block11, ParentSlot: 10, ParentHash: chainTestHash(10)})
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: block12, ParentSlot: 11, ParentHash: block11.Hash})
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: block13, ParentSlot: 12, ParentHash: block12.Hash})

	if _, err := tracker.ObserveCertificate(Certificate{
		Type:              CertificateFinalizeFast,
		Slot:              block13.Slot,
		BlockHash:         block13.Hash,
		SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe fast finalization certificate: %v", err)
	}
	requireChainFinalized(t, tracker, block13, CertificateFinalizeFast)

	for _, slot := range []uint64{11, 12} {
		decision, ok := tracker.NextDecision(slot - 1)
		if !ok {
			t.Fatalf("expected finalized-ancestor decision for slot %d", slot)
		}
		if decision.Kind != ChainDecisionKindBlock || decision.Block.Slot != slot || !decision.Observed {
			t.Fatalf("unexpected decision for slot %d: %+v", slot, decision)
		}
		if decision.CertificateType != CertificateFinalizeFast {
			t.Fatalf("slot %d cert type = %q, want %q", slot, decision.CertificateType, CertificateFinalizeFast)
		}
	}
}

func TestChainTrackerDoesNotInferHashlessParentFromFallback(t *testing.T) {
	tracker := NewChainTracker()
	parent := BlockID{Slot: 11, Hash: chainTestHash(11)}
	child := BlockID{Slot: 12, Hash: chainTestHash(12)}

	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: child, ParentSlot: parent.Slot})
	if _, err := tracker.ObserveCertificate(Certificate{
		Type: CertificateNotarize, Slot: child.Slot, BlockHash: child.Hash, SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe child notarize: %v", err)
	}
	if _, err := tracker.ObserveCertificate(Certificate{
		Type: CertificateNotarizeFallback, Slot: parent.Slot, BlockHash: parent.Hash, SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe parent fallback: %v", err)
	}
	tracker.RefreshParentLinkagesFromSlot(parent.Slot, parent.Hash)

	decision, ok := tracker.NextDecision(parent.Slot)
	if !ok || decision.Block != child {
		t.Fatalf("expected child decision, got %+v (ok=%v)", decision, ok)
	}
	if decision.ParentHash != (solana.Hash{}) {
		t.Fatalf("fallback-only parent was unsafely inferred: %s", decision.ParentHash)
	}

	if _, err := tracker.ObserveCertificate(Certificate{
		Type: CertificateNotarize, Slot: parent.Slot, BlockHash: parent.Hash, SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe decisive parent notarize: %v", err)
	}
	tracker.RefreshParentLinkagesFromSlot(parent.Slot, parent.Hash)
	decision, ok = tracker.NextDecision(parent.Slot)
	if !ok || decision.ParentHash != parent.Hash {
		t.Fatalf("decisive parent was not linked: %+v (ok=%v)", decision, ok)
	}
}

func TestChainTrackerFinalizedAncestryCannotOverwriteTwin(t *testing.T) {
	tracker := NewChainTracker()
	finalizedTwin := BlockID{Slot: 11, Hash: chainTestHash(1)}
	ancestryTwin := BlockID{Slot: 11, Hash: chainTestHash(2)}
	tip := BlockID{Slot: 12, Hash: chainTestHash(12)}

	if _, err := tracker.ObserveCertificate(Certificate{
		Type: CertificateFinalizeFast, Slot: finalizedTwin.Slot, BlockHash: finalizedTwin.Hash, SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe finalized twin: %v", err)
	}
	requireChainFinalized(t, tracker, finalizedTwin, CertificateFinalizeFast)
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: ancestryTwin, ParentSlot: 10, ParentHash: chainTestHash(10)})
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: tip, ParentSlot: ancestryTwin.Slot, ParentHash: ancestryTwin.Hash})
	if _, err := tracker.ObserveCertificate(Certificate{
		Type: CertificateFinalizeFast, Slot: tip.Slot, BlockHash: tip.Hash, SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe finalized tip: %v", err)
	}
	if err := tracker.ObserveFinalized(tip, CertificateFinalizeFast); err == nil {
		t.Fatal("conflicting finalized ancestry was accepted")
	}

	decision, ok := tracker.NextDecision(10)
	if !ok || decision.Kind != ChainDecisionKindConflict {
		t.Fatalf("expected finalized ancestry conflict, got %+v (ok=%v)", decision, ok)
	}
	if _, ok := tracker.FinalizedBlockAt(11); ok {
		t.Fatalf("ambiguous finalized slot must fail closed")
	}

	tracker.PruneBeforeSlot(12)
	decision, ok = tracker.NextDecision(10)
	if !ok || decision.Kind != ChainDecisionKindConflict {
		t.Fatalf("pruning erased conflict evidence: %+v (ok=%v)", decision, ok)
	}
}

func TestChainTrackerRejectsConflictingParentLinkForBlockIdentity(t *testing.T) {
	tracker := NewChainTracker()
	block := BlockID{Slot: 12, Hash: chainTestHash(12)}
	if _, err := tracker.ObserveCertificate(Certificate{
		Type: CertificateNotarize, Slot: block.Slot, BlockHash: block.Hash, SignatureVerified: true,
	}); err != nil {
		t.Fatal(err)
	}
	firstParent := chainTestHash(11)
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: block, ParentSlot: 11, ParentHash: firstParent})

	update := tracker.ObserveReplayBlock(ReplayBlockObservation{
		Block: block, ParentSlot: 11, ParentHash: chainTestHash(10),
	})
	if !update.Conflict || update.ConflictSlot != block.Slot {
		t.Fatalf("conflicting parent update = %+v", update)
	}
	state := tracker.blocks[block]
	if state == nil || state.parentSlot != 11 || state.parentHash != firstParent {
		t.Fatalf("conflicting observation rewrote parent linkage: %+v", state)
	}
	decision, ok := tracker.NextDecision(11)
	if !ok || decision.Kind != ChainDecisionKindConflict {
		t.Fatalf("parent-link conflict did not fail closed: %+v (ok=%v)", decision, ok)
	}
}

func TestChainTrackerRejectsFinalizedAncestryAcrossPrunedRoot(t *testing.T) {
	tracker := NewChainTracker()
	tracker.PruneBeforeSlot(20)
	tip := BlockID{Slot: 21, Hash: chainTestHash(21)}
	tracker.ObserveReplayBlock(ReplayBlockObservation{
		Block: tip, ParentSlot: 19, ParentHash: chainTestHash(19),
	})
	if _, err := tracker.ObserveCertificate(Certificate{
		Type: CertificateFinalizeFast, Slot: tip.Slot, BlockHash: tip.Hash, SignatureVerified: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tracker.ObserveFinalized(tip, CertificateFinalizeFast); err == nil {
		t.Fatal("finalized branch crossing the durable root was accepted")
	}
	if !tracker.FinalityConflictAt(20) {
		t.Fatal("durable-root crossing did not latch a conflict")
	}
	for slot := range tracker.finalizedAncestors {
		if slot < 20 {
			t.Fatalf("finalized ancestry regrew below prune boundary at slot %d", slot)
		}
	}
	for slot := range tracker.indirectSkips {
		if slot < 20 {
			t.Fatalf("indirect skip regrew below prune boundary at slot %d", slot)
		}
	}
}

func TestChainTrackerDoesNotRegrowPrunedHistory(t *testing.T) {
	tracker := NewChainTracker()
	tracker.PruneBeforeSlot(20)
	update, err := tracker.ObserveCertificate(Certificate{
		Type: CertificateSkip, Slot: 10, SignatureVerified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if update.New || update.Trusted {
		t.Fatalf("old certificate update = %+v", update)
	}
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: BlockID{Slot: 10, Hash: chainTestHash(10)}})
	if snapshot := tracker.Snapshot(); snapshot.CertificatesObserved != 0 || snapshot.ReplayBlocksObserved != 0 || snapshot.CertifiedSkips != 0 {
		t.Fatalf("pruned history regrew: %+v", snapshot)
	}
}

func chainTestHash(seed byte) solana.Hash {
	var hash solana.Hash
	hash[0] = seed
	hash[31] = seed
	return hash
}
