package blockstream

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func recoveryTestBlock(slot, parent uint64, id, parentID solana.Hash) *b.Block {
	return &b.Block{
		Slot: slot, SourceParentSlot: parent, FromLiveStream: true,
		AlpenglowBlockID: id, HasAlpenglowBlockID: true,
		AlpenglowParentBlockID: parentID, HasAlpenglowParentBlockID: true,
	}
}

// Mainnet 2026-10-30: 060 linked to rooted 056, replacing executed 057-059.
// The source accepted that branch, but rebuilding replay's vote/stake caches
// replaced the source too. The spool then delivered discarded 057 first and
// recovery stopped on the same switch a second time.
func TestAlpenglowRecoveryDoesNotReplayDiscardedSpoolBranch(t *testing.T) {
	const parent = uint64(463100056)
	parentID, wrongID, childID := solana.Hash{0x56}, solana.Hash{0x57}, solana.Hash{0x60}
	old := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceTurbine, TurbineAlpenglowBlockIDHints: true,
		StartSlot: parent + 1, InitialAlpenglowBlockID: parentID, HasInitialAlpenglowBlockID: true,
	})
	wrong := recoveryTestBlock(parent+1, parent, wrongID, parentID)
	child := recoveryTestBlock(parent+4, parent, childID, parentID)
	old.recordEmittedAlpenglowBlockIDLocked(wrong)
	old.lastEmittedBlockSlot = wrong.Slot
	old.nextSlotToSend = wrong.Slot + 1
	old.reorderBuffer[child.Slot] = child
	require.True(t, old.queueAlpenglowParentSwitchLocked(child))
	event := <-old.alpenglowParentSwitchCh
	require.True(t, old.RewindForAlpenglowParentSwitch(event))
	old.Stop()
	recovery := old.CaptureAlpenglowRecoveryState()

	for _, earlierRoot := range []bool{false, true} {
		t.Run(map[bool]string{false: "root_is_parent", true: "root_precedes_parent"}[earlierRoot], func(t *testing.T) {
			start, anchor := parent+1, parentID
			if earlierRoot {
				start, anchor = parent, solana.Hash{0x55}
			}
			retry := NewBlockSource(&BlockSourceOpts{
				SourceType: BlockSourceTurbine, TurbineAlpenglowBlockIDHints: true,
				StartSlot: start, InitialAlpenglowBlockID: anchor, HasInitialAlpenglowBlockID: true,
				AlpenglowRecovery: recovery,
			})
			retry.isNearTip.Store(true)
			retry.liveStreamActive.Store(true)
			retry.liveHandoffSlot.Store(start)
			done := make(chan struct{})
			go func() {
				retry.emitOrderedBlocks()
				close(done)
			}()
			t.Cleanup(func() { close(retry.resultQueue); <-done })
			feed := func(blk *b.Block) { retry.resultQueue <- fetchResult{slot: blk.Slot, block: blk} }
			next := func() *b.Block {
				select {
				case blk := <-retry.streamChan:
					return blk
				case <-time.After(time.Second):
					t.Fatal("recovered source did not emit selected branch")
					return nil
				}
			}
			if earlierRoot {
				prefix := recoveryTestBlock(parent, parent-1, parentID, anchor)
				feed(prefix)
				require.Same(t, prefix, next())
			}
			// Old fork arrives BEFORE the selected child, just as it did in the
			// failed retry. Also reject an unseen sibling within the skip run.
			feed(wrong)
			feed(recoveryTestBlock(parent+2, parent, solana.Hash{0x58}, parentID))
			feed(child)
			for slot := parent + 1; slot < child.Slot; slot++ {
				blk := next()
				require.Equal(t, slot, blk.Slot)
				require.True(t, blk.IsSkipped, "discarded block at %d executed again", slot)
			}
			require.Same(t, child, next())
			select {
			case repeated := <-retry.alpenglowParentSwitchCh:
				t.Fatalf("repeated the recovered switch: %+v", repeated)
			default:
			}
		})
	}
}

func TestAlpenglowRecoveryPreservesCertificateAuthorityAndOwnsItsMaps(t *testing.T) {
	opts := &BlockSourceOpts{SourceType: BlockSourceTurbine, TurbineAlpenglowBlockIDHints: true, StartSlot: 101}
	old := NewBlockSource(opts)
	wrong := recoveryTestBlock(101, 100, solana.Hash{1}, solana.Hash{100})
	allowed := recoveryTestBlock(102, 100, solana.Hash{2}, solana.Hash{100})
	old.rejectAlpenglowBlockID(101, solana.Hash(wrong.AlpenglowBlockID))
	old.rejectAlpenglowSkipRange(101, 105)
	old.SetKnownAlpenglowBlockID(102, solana.Hash(allowed.AlpenglowBlockID))
	old.Stop()
	opts.AlpenglowRecovery = old.CaptureAlpenglowRecoveryState()
	retry := NewBlockSource(opts)
	require.True(t, retry.isRejectedAlpenglowBlock(wrong))
	require.False(t, retry.isRejectedAlpenglowBlock(allowed), "lost an existing certificate override")
	require.Empty(t, retry.skippedSlots, "an exclusion alone must not manufacture skip decisions")

	// A NEW decisive certificate must override the restored speculative range.
	retry.alpenglowDecisionSource = func(anchor uint64) (alpenglow.ChainDecision, bool) {
		return alpenglow.ChainDecision{Slot: 101, Kind: alpenglow.ChainDecisionKindBlock,
			Block: alpenglow.BlockID{Slot: 101, Hash: solana.Hash(wrong.AlpenglowBlockID)}}, anchor == 100
	}
	retry.applyAlpenglowDecisionLocked()
	require.False(t, retry.isRejectedAlpenglowBlock(wrong))
	require.True(t, old.isRejectedAlpenglowBlock(wrong), "retry mutated the abandoned source")
	require.True(t, NewBlockSource(opts).isRejectedAlpenglowBlock(wrong), "retry mutated the handoff")

	// Advancing the durable checkpoint retires old exclusions; the snapshot
	// still belongs to its caller and remains usable by an earlier retry.
	opts.StartSlot = 103
	advanced := NewBlockSource(opts)
	require.Empty(t, advanced.rejectedAlpenglowBlockIDs)
	require.Empty(t, advanced.authoritativeAlpenglowBlockIDs)
	require.Equal(t, []alpenglowSlotRange{{from: 103, to: 105}}, advanced.rejectedAlpenglowSkipRanges)
	opts.StartSlot = 105
	require.Empty(t, NewBlockSource(opts).rejectedAlpenglowSkipRanges)
	opts.SourceType = BlockSourceRpc
	require.Empty(t, NewBlockSource(opts).rejectedAlpenglowBlockIDs)
}

func TestAlpenglowRecoveryKeepsIdentityExclusionsDistinctFromInvalidity(t *testing.T) {
	opts := &BlockSourceOpts{SourceType: BlockSourceTurbine, TurbineAlpenglowBlockIDHints: true, StartSlot: 101}
	old := NewBlockSource(opts)
	wrong := recoveryTestBlock(101, 100, solana.Hash{1}, solana.Hash{100})
	sibling := recoveryTestBlock(101, 100, solana.Hash{2}, solana.Hash{100})
	invalid := recoveryTestBlock(102, 101, solana.Hash{3}, solana.Hash{2})
	old.rejectAlpenglowBlockID(101, solana.Hash(wrong.AlpenglowBlockID))
	old.markInvalidAlpenglowBlockID(102, solana.Hash(invalid.AlpenglowBlockID))
	old.Stop()
	opts.AlpenglowRecovery = old.CaptureAlpenglowRecoveryState()
	retry := NewBlockSource(opts)
	require.True(t, retry.isRejectedAlpenglowBlock(wrong))
	require.False(t, retry.isRejectedAlpenglowBlock(sibling), "same-slot replacement was excluded")
	retry.SetKnownAlpenglowBlockID(101, solana.Hash(wrong.AlpenglowBlockID))
	require.False(t, retry.isRejectedAlpenglowBlock(wrong))
	retry.SetKnownAlpenglowBlockID(102, solana.Hash(invalid.AlpenglowBlockID))
	require.True(t, retry.isRejectedAlpenglowBlock(invalid), "certificate hint revived an objectively invalid block")
	require.True(t, NewBlockSource(opts).isRejectedAlpenglowBlock(wrong), "inner identity map was shared across attempts")
}
