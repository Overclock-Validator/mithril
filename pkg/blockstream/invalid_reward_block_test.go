package blockstream

import (
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

// Slot 2000727 carried an invalid reward certificate. The finalized chain
// instead continued from 2000726 to 2000728. Once replay rejects the candidate,
// the source must retain that parent and allow the connected successor through.
func TestInvalidRewardCandidateQuarantineAllowsConnectedSuccessor(t *testing.T) {
	const parentSlot = uint64(2000726)
	const invalidSlot = parentSlot + 1
	parentID := solana.Hash{0x26}
	invalidID := solana.MustHashFromBase58("7uu3EcXpMRFJnfDpj9WrmDL8xiqFa6FjPCsiekfHodfz")
	descendantID := solana.Hash{0x27, 0x28}
	successorID := solana.Hash{0x28}
	parent := invalidTestLiveBlock(parentSlot, parentSlot-1, parentID, solana.Hash{0x25})
	invalid := invalidTestLiveBlock(invalidSlot, parentSlot, invalidID, parentID)
	descendant := invalidTestLiveBlock(invalidSlot+1, invalidSlot, descendantID, invalidID)
	successor := invalidTestLiveBlock(invalidSlot+1, parentSlot, successorID, parentID)
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    invalidSlot,
		EndSlot:                      invalidSlot + 10,
	})
	bs.isNearTip.Store(true)
	bs.liveHandoffSlot.Store(invalidSlot)
	// Replay has consumed the parent and is validating the invalid candidate;
	// its already-emitted descendant and duplicate delivery remain queued.
	for _, blk := range []*b.Block{parent, invalid, descendant} {
		bs.lastEmittedBlockSlot = blk.Slot
		bs.recordEmittedAlpenglowBlockIDLocked(blk)
	}
	bs.nextSlotToSend = descendant.Slot + 1
	bs.streamChan <- invalid
	bs.streamChan <- descendant
	bs.reorderBuffer[invalidSlot+2] = invalidTestLiveBlock(invalidSlot+2, descendant.Slot, solana.Hash{0x29}, descendantID)
	bs.liveStagingBuffer[invalidSlot+3] = invalidTestLiveBlock(invalidSlot+3, descendant.Slot, solana.Hash{0x30}, descendantID)

	if err := bs.QuarantineInvalidAlpenglowBlock(invalid); err != nil {
		t.Fatalf("quarantine invalid reward candidate: %v", err)
	}
	if bs.BufferDepth() != 0 || len(bs.reorderBuffer) != 0 || len(bs.liveStagingBuffer) != 0 {
		t.Fatalf("invalid suffix remains queued: replay=%d reorder=%d staging=%d", bs.BufferDepth(), len(bs.reorderBuffer), len(bs.liveStagingBuffer))
	}
	if bs.nextSlotToSend != invalidSlot || bs.lastEmittedBlockSlot != parentSlot ||
		!bs.hasLastEmittedAlpenglowBlockID || bs.lastEmittedAlpenglowBlockID != parentID {
		t.Fatalf("quarantine lost parent: next=%d anchor=%d/%s", bs.nextSlotToSend, bs.lastEmittedBlockSlot, bs.lastEmittedAlpenglowBlockID)
	}
	if got := bs.emittedAlpenglowBlockIDs[parentSlot]; got != parentID {
		t.Fatalf("retained parent identity = %s, want %s", got, parentID)
	}
	for _, blk := range []*b.Block{invalid, descendant} {
		if !bs.IsObjectivelyInvalidAlpenglowBlock(blk) {
			t.Fatalf("emitted invalid suffix identity at slot %d was not quarantined", blk.Slot)
		}
		if bs.skippedSlots[blk.Slot] || bs.alpenglowCertifiedSkips[blk.Slot] || bs.liveSynthesizedSkips[blk.Slot] {
			t.Fatalf("quarantine invented a skip for slot %d", blk.Slot)
		}
		if _, emitted := bs.emittedAlpenglowBlockIDs[blk.Slot]; emitted {
			t.Fatalf("invalid identity at slot %d remains in selected ancestry", blk.Slot)
		}
	}
	bs.bufferLiveStreamBlock(invalid)
	bs.bufferLiveStreamBlock(descendant)
	if len(bs.liveStagingBuffer) != 0 {
		t.Fatal("a duplicate invalid candidate or descendant was restaged")
	}

	emitterDone := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(emitterDone)
	}()
	defer func() {
		close(bs.resultQueue)
		waitInvalidTestDone(t, emitterDone, "reward candidate recovery emitter")
	}()
	// Delayed duplicate results must also be rejected at the emitter boundary.
	generation := bs.liveResultGeneration.Load()
	bs.resultQueue <- fetchResult{slot: invalid.Slot, block: invalid, liveStreamGeneration: generation}
	bs.resultQueue <- fetchResult{slot: descendant.Slot, block: descendant, liveStreamGeneration: generation}
	bs.resultQueue <- fetchResult{slot: successor.Slot, block: successor, liveStreamGeneration: generation}

	for i, want := range []*b.Block{{Slot: invalidSlot, IsSkipped: true}, successor} {
		select {
		case got := <-bs.streamChan:
			if got == nil || got.Slot != want.Slot || got.IsSkipped != want.IsSkipped || (i == 1 && got != successor) {
				t.Fatalf("recovery emission %d = %+v, want slot %d skipped=%t", i, got, want.Slot, want.IsSkipped)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for recovery emission %d", i)
		}
	}
	bs.reorderMu.Lock()
	certifiedSkip := bs.alpenglowCertifiedSkips[invalidSlot]
	bs.reorderMu.Unlock()
	if certifiedSkip {
		t.Fatal("parent-linked successor falsely established a certified skip")
	}
	if !bs.IsObjectivelyInvalidAlpenglowBlock(invalid) || !bs.IsObjectivelyInvalidAlpenglowBlock(descendant) {
		t.Fatal("accepting the successor cleared an invalid identity tombstone")
	}
}
