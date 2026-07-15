package alpenglow

import (
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestChainTrackerRefreshParentLinkagesFromSlot(t *testing.T) {
	tracker := NewChainTracker()

	block102 := BlockID{Slot: 102, Hash: chainTestHash(102)}
	block103 := BlockID{Slot: 103, Hash: chainTestHash(103)}

	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: block103, ParentSlot: 102})
	tracker.RefreshParentLinkagesFromSlot(100, chainTestHash(100))

	state := tracker.blocks[block103]
	if state == nil || state.parentHash != (solana.Hash{}) {
		t.Fatalf("expected missing parent hash before slot 102 refresh, got %+v", state)
	}

	tracker.RefreshParentLinkagesFromSlot(102, block102.Hash)
	state = tracker.blocks[block103]
	if state == nil || state.parentHash != (solana.Hash{}) {
		t.Fatalf("ambiguous parent identity must not be backfilled, got %+v", state)
	}
	if _, err := tracker.ObserveCertificate(Certificate{
		Type: CertificateNotarize, Slot: block102.Slot, BlockHash: block102.Hash, SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe decisive parent certificate: %v", err)
	}
	tracker.RefreshParentLinkagesFromSlot(102, block102.Hash)
	state = tracker.blocks[block103]
	if state == nil || state.parentHash != block102.Hash {
		t.Fatalf("expected decisive parent hash backfill for slot 102, got %+v", state)
	}
}

func TestChainTrackerRetryWalkAfterParentAlreadyFinalizedAncestor(t *testing.T) {
	tracker := NewChainTracker()

	anchor := BlockID{Slot: 100, Hash: chainTestHash(100)}
	block102 := BlockID{Slot: 102, Hash: chainTestHash(102)}
	block103 := BlockID{Slot: 103, Hash: chainTestHash(103)}

	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: anchor, ParentSlot: 99, ParentHash: chainTestHash(99)})
	tracker.ObserveReplayBlock(ReplayBlockObservation{
		Block:      block103,
		ParentSlot: block102.Slot,
		ParentHash: block102.Hash,
	})

	if _, err := tracker.ObserveCertificate(Certificate{
		Type:              CertificateFinalizeFast,
		Slot:              block103.Slot,
		BlockHash:         block103.Hash,
		SignatureVerified: true,
	}); err != nil {
		t.Fatalf("observe fast finalization certificate: %v", err)
	}
	requireChainFinalized(t, tracker, block103, CertificateFinalizeFast)

	if _, ok := tracker.NextDecision(100); ok {
		t.Fatalf("expected no indirect skip before parent linkage")
	}

	tracker.ObserveReplayBlock(ReplayBlockObservation{
		Block:      block102,
		ParentSlot: anchor.Slot,
		ParentHash: anchor.Hash,
	})

	decision, ok := tracker.NextDecision(100)
	if !ok {
		t.Fatalf("expected indirect skip for slot 101 after parent linkage")
	}
	if decision.Kind != ChainDecisionKindSkip || decision.Slot != 101 || !decision.Indirect {
		t.Fatalf("unexpected decision for slot 101: %+v", decision)
	}
}
