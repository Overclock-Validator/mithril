package blockstream

import (
	"errors"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

func parentRecoveryFixture() *AlpenglowParentSwitchRecovery {
	return &AlpenglowParentSwitchRecovery{
		Selection: AlpenglowParentSwitch{SwitchSlot: 276, ParentSlot: 275, ParentID: solana.Hash{1}, ChildSlot: 280, ChildID: solana.Hash{2}},
		Child: &b.Block{Slot: 280, SourceParentSlot: 275, FromLiveStream: true,
			HasAlpenglowBlockID: true, AlpenglowBlockID: solana.Hash{2},
			HasAlpenglowParentBlockID: true, AlpenglowParentBlockID: solana.Hash{1}},
	}
}

func recoverySource(r *AlpenglowParentSwitchRecovery, change func(*BlockSourceOpts)) *BlockSource {
	opts := &BlockSourceOpts{SourceType: BlockSourceTurbine, TurbineBindAddr: "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true, StartSlot: 276, EndSlot: 300,
		HasInitialAlpenglowBlockID: true, InitialAlpenglowBlockID: solana.Hash{1}, ParentSwitchRecovery: r}
	if change != nil {
		change(opts)
	}
	return NewBlockSource(opts)
}

func TestRootedParentRecoveryReplaysSelectedBranch(t *testing.T) {
	r := parentRecoveryFixture()
	old := recoverySource(nil, nil)
	old.lastEmittedBlockSlot = 276
	old.lastEmittedAlpenglowBlockID = solana.Hash{3}
	old.emittedAlpenglowBlockIDs[276] = solana.Hash{3}
	old.nextSlotToSend = 277
	old.reorderBuffer[280] = r.Child
	if !old.queueAlpenglowParentSwitchLocked(r.Child) {
		t.Fatal("no parent switch")
	}
	recovery := old.ParentSwitchRecovery(r.Selection)
	if recovery == nil {
		t.Fatal("lost pending child")
	}
	// This is the node-level rooted retry: a fresh source starts on the exact
	// root and receives the old losing block before any network replacement.
	bs := recoverySource(recovery, nil)
	bs.isNearTip.Store(true)
	bs.liveStreamActive.Store(true)
	bs.liveHandoffSlot.Store(276)
	wrong := &b.Block{Slot: 276, SourceParentSlot: 275, FromLiveStream: true,
		HasAlpenglowBlockID: true, AlpenglowBlockID: solana.Hash{3},
		HasAlpenglowParentBlockID: true, AlpenglowParentBlockID: solana.Hash{1}}
	done := make(chan struct{})
	go func() { bs.emitOrderedBlocks(); close(done) }()
	defer func() { close(bs.resultQueue); <-done }()
	bs.resultQueue <- fetchResult{slot: 276, block: wrong, liveStreamGeneration: bs.liveResultGeneration.Load()}
	for slot := uint64(276); slot <= 280; slot++ {
		select {
		case got := <-bs.streamChan:
			if got.Slot != slot || got.IsSkipped != (slot < 280) {
				t.Fatalf("unexpected emission: %+v", got)
			}
			if slot == 280 && got != recovery.Child {
				t.Fatal("emitted a different child")
			}
		case <-time.After(time.Second):
			t.Fatalf("no emission at %d", slot)
		}
	}
	if !bs.isRejectedAlpenglowBlock(wrong) {
		t.Fatal("discarded branch can resurrect")
	}
	bs.SetKnownAlpenglowBlockID(276, solana.Hash{3})
	if bs.isRejectedAlpenglowBlock(wrong) {
		t.Fatal("speculation overrode certificate")
	}
}

func TestRootedParentRecoveryGuards(t *testing.T) {
	for _, name := range []string{"parent-id", "child-id", "child-parent", "too-long", "classic", "invalid", "certificate-block", "certificate-child-skip", "no-pending"} {
		t.Run(name, func(t *testing.T) {
			r := parentRecoveryFixture()
			bs := recoverySource(r, func(o *BlockSourceOpts) {
				switch name {
				case "parent-id":
					o.InitialAlpenglowBlockID = solana.Hash{9}
				case "child-id":
					r.Child.AlpenglowBlockID = solana.Hash{9}
				case "child-parent":
					r.Child.SourceParentSlot--
				case "too-long":
					r.Selection.ChildSlot += alpenglowMaxParentLinkedSkipRun
					r.Child.Slot = r.Selection.ChildSlot
				case "classic":
					o.TurbineAlpenglowBlockIDHints = false
				case "invalid":
					o.AlpenglowCandidateValidator = func(*b.Block) error { return errors.New("invalid transaction") }
				case "certificate-block":
					o.AlpenglowDecisionSource = func(anchor uint64) (alpenglow.ChainDecision, bool) {
						return alpenglow.ChainDecision{Slot: anchor + 1, Kind: alpenglow.ChainDecisionKindBlock, Block: alpenglow.BlockID{Slot: anchor + 1, Hash: solana.Hash{9}}}, true
					}
				case "certificate-child-skip":
					o.AlpenglowDecisionSource = func(anchor uint64) (alpenglow.ChainDecision, bool) {
						return alpenglow.ChainDecision{Slot: anchor + 1, Kind: alpenglow.ChainDecisionKindSkip}, anchor+1 == 280
					}
				case "no-pending":
					o.ParentSwitchRecovery = nil
				}
			})
			bs.restoreAlpenglowParentSelectionLocked()
			if bs.reorderBuffer[280] != nil || len(bs.rejectedAlpenglowSkipRanges) != 0 {
				t.Fatal("invalid recovery changed selection")
			}
			if bs.ParentSwitchRecovery(r.Selection) != nil {
				t.Fatal("captured an event that is not pending")
			}
		})
	}
}

func TestRootedParentRecoveryWaitsForExactParent(t *testing.T) {
	r := parentRecoveryFixture()
	bs := recoverySource(r, func(o *BlockSourceOpts) { o.StartSlot = 270 })
	bs.restoreAlpenglowParentSelectionLocked()
	if bs.parentSwitchRecovery == nil || bs.reorderBuffer[280] != nil {
		t.Fatal("recovery did not wait for parent")
	}
	bs.nextSlotToSend = 276
	bs.lastEmittedBlockSlot = 275
	bs.lastEmittedAlpenglowBlockID = solana.Hash{1}
	bs.restoreAlpenglowParentSelectionLocked()
	if bs.reorderBuffer[280] != r.Child || !bs.synthesizeAlpenglowParentLinkedSkipsLocked() {
		t.Fatal("exact replayed parent did not restore branch")
	}
}
