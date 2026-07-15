package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

func TestActiveExecutedIdentityDoesNotUseObservedCandidateFallback(t *testing.T) {
	const slot = uint64(9_000_000_001)
	global.SetAlpenglowBlockID(slot, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(slot, solana.Hash{2})

	sr := NewSpeculativeReplay()
	sr.Enable()
	cleanup := publishActiveSpeculativeReplay(sr)
	defer cleanup()

	if _, _, ok := ResolveActiveAlpenglowIdentity(slot); ok {
		t.Fatal("active executed identity resolved an observed but unexecuted candidate")
	}
}

func TestSeedFromManifestUsesPoHLastBlockhash(t *testing.T) {
	lastHash := solana.Hash{7}
	sr := NewSpeculativeReplay()
	sr.Enable()
	sr.SeedFromManifest(&state.MithrilState{
		ManifestParentSlot:    41,
		ManifestLastBlockhash: base58.Encode(lastHash[:]),
		SnapshotEpoch:         7,
	}, &ReplayCtx{})

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.headSnapshot == nil || solana.Hash(sr.headSnapshot.Blockhash) != lastHash {
		t.Fatalf("manifest PoH blockhash was not installed: %+v", sr.headSnapshot)
	}
	if sr.headSnapshot.Epoch != 7 {
		t.Fatalf("manifest parent epoch = %d, want 7", sr.headSnapshot.Epoch)
	}
}

func TestSeedFromResumeRetainsDurableParentEpoch(t *testing.T) {
	sr := NewSpeculativeReplay()
	sr.Enable()
	sr.SeedFromResume(&ResumeState{ParentSlot: 41, ParentEpoch: 7}, &ReplayCtx{})

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.headSnapshot == nil || sr.headSnapshot.Epoch != 7 {
		t.Fatalf("resume parent epoch was not retained: %+v", sr.headSnapshot)
	}
}

func TestAlpenglowCertConfirmsPersist(t *testing.T) {
	if !alpenglowCertConfirmsPersist(alpenglow.CertificateFinalizeFast) {
		t.Fatalf("expected finalize-fast to confirm persist")
	}
	if !alpenglowCertConfirmsPersist(alpenglow.CertificateGenesis) {
		t.Fatalf("expected genesis to confirm persist")
	}
	if alpenglowCertConfirmsPersist(alpenglow.CertificateNotarize) {
		t.Fatalf("expected notarize alone to defer persist")
	}
}

func TestCaptureHeadSnapshotRoundTrip(t *testing.T) {
	replayCtx := &ReplayCtx{Capitalization: 123}
	slotCtx := &sealevel.SlotCtx{
		Slot:            6404034,
		ParentSlot:      6404033,
		FinalBankhash:   []byte{1, 2, 3},
		Blockhash:       [32]byte{9},
		NumSignatures:   42,
		AcctsLtHash:     &lthash.LtHash{},
		FeeRateGovernor: &sealevel.FeeRateGovernor{LamportsPerSignature: 5000},
	}
	clock := sealevel.SysvarClock{Slot: 6404034}
	sealevel.SysvarCache.Clock.Sysvar = &clock

	snapshot := CaptureHeadSnapshot(slotCtx, replayCtx, 6404034)
	if snapshot == nil || snapshot.Slot != 6404034 || snapshot.Capitalization != 123 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}

	restored := slotCtxFromSnapshot(snapshot)
	if restored.Slot != 6404034 || restored.NumSignatures != 42 {
		t.Fatalf("unexpected restored slot ctx: %+v", restored)
	}
}

func TestTryCommitPendingRequiresFinalizeCert(t *testing.T) {
	sr := NewSpeculativeReplay()
	sr.Enable()

	blockHash := solana.Hash{7}
	sr.mu.Lock()
	sr.committedSlot = 6404034
	sr.pending[6404035] = &DeferredBlockCommit{
		BlockSlot: 6404035,
		Bankhash:  []byte{1},
		SlotCtx:   &sealevel.SlotCtx{Slot: 6404035},
	}
	sr.mu.Unlock()

	block := &b.Block{
		Slot:                6404035,
		FromLightbringer:    true,
		HasAlpenglowBlockID: true,
		AlpenglowBlockID:    blockHash,
	}
	decisionSource := func(anchor uint64) (alpenglow.ChainDecision, bool) {
		if anchor != 6404034 {
			return alpenglow.ChainDecision{}, false
		}
		return alpenglow.ChainDecision{
			Kind:            alpenglow.ChainDecisionKindBlock,
			Block:           alpenglow.BlockID{Slot: 6404035, Hash: blockHash},
			CertificateType: alpenglow.CertificateNotarize,
		}, true
	}

	if sr.TryCommitPending(nil, &persistedTracker{}, block, 6404035, &ReplayCtx{}, decisionSource) {
		t.Fatalf("expected notarize-only decision to defer persist")
	}
}

func TestTryFlushPendingDoesNotCommitOnNotarizeOnly(t *testing.T) {
	sr := NewSpeculativeReplay()
	sr.Enable()

	sr.mu.Lock()
	sr.committedSlot = 10
	sr.pending[11] = &DeferredBlockCommit{
		BlockSlot: 11,
		SlotCtx:   &sealevel.SlotCtx{Slot: 11},
	}
	sr.mu.Unlock()

	decisionSource := func(anchor uint64) (alpenglow.ChainDecision, bool) {
		if anchor != 10 {
			return alpenglow.ChainDecision{}, false
		}
		return alpenglow.ChainDecision{
			Kind:            alpenglow.ChainDecisionKindBlock,
			Block:           alpenglow.BlockID{Slot: 11, Hash: speculativeTestHash(11)},
			CertificateType: alpenglow.CertificateNotarize,
		}, true
	}

	if flushed := sr.TryFlushPending(nil, &persistedTracker{}, &ReplayCtx{}, decisionSource); flushed != 0 {
		t.Fatalf("flushed = %d, want 0 for notarize-only decision", flushed)
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.committedSlot != 10 || len(sr.pending) != 1 {
		t.Fatalf("committedSlot=%d pending=%d, want committed=10 pending=1", sr.committedSlot, len(sr.pending))
	}
}

func speculativeTestHash(seed byte) solana.Hash {
	var hash solana.Hash
	hash[0] = seed
	hash[31] = seed
	return hash
}

func TestTryCommitPendingStopsWhenEarlierPendingMissing(t *testing.T) {
	sr := NewSpeculativeReplay()
	sr.Enable()

	blockHash := func(slot byte) solana.Hash {
		return solana.Hash{slot}
	}

	sr.mu.Lock()
	sr.committedSlot = 100
	sr.pending[102] = &DeferredBlockCommit{
		BlockSlot:           102,
		HasAlpenglowBlockID: true,
		AlpenglowBlockID:    blockHash(102),
		SlotCtx:             &sealevel.SlotCtx{Slot: 102},
	}
	sr.mu.Unlock()

	decisionSource := func(anchor uint64) (alpenglow.ChainDecision, bool) {
		slot := anchor + 1
		if slot > 102 {
			return alpenglow.ChainDecision{}, false
		}
		return alpenglow.ChainDecision{
			Kind:            alpenglow.ChainDecisionKindBlock,
			Block:           alpenglow.BlockID{Slot: slot, Hash: blockHash(byte(slot))},
			CertificateType: alpenglow.CertificateFinalize,
		}, true
	}

	block := &b.Block{
		Slot:                102,
		HasAlpenglowBlockID: true,
		AlpenglowBlockID:    blockHash(102),
	}
	if sr.TryCommitPending(nil, &persistedTracker{}, block, 102, &ReplayCtx{}, decisionSource) {
		t.Fatalf("expected missing pending slot 101 to block commit")
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.committedSlot != 100 {
		t.Fatalf("committedSlot = %d, want 100", sr.committedSlot)
	}
	if len(sr.pending) != 1 {
		t.Fatalf("expected one pending entry, got %d", len(sr.pending))
	}
}
