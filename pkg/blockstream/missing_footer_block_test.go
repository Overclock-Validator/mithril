package blockstream

import (
	"context"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// On mainnet, 461653844-845 were replayed before footerless candidate 461653846
// arrived. The next leader's 461653848 links back to rooted slot 461653843.
// Rejecting 846 must leave that earlier ancestor available for the subsequent
// parent switch, whether the alternate child arrived before or after rejection.
func TestMissingFooterQuarantineAllowsAncestorForkRecovery(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		name := "child_after_quarantine"
		if buffered {
			name = "child_before_quarantine"
		}
		t.Run(name, func(t *testing.T) {
			const rootSlot = uint64(461653843)
			rootID := solana.MustHashFromBase58("2EU3LVQ7qJGHUYb7S5AhMR2nGGFdyNUNGQwkrfmiY6kd")
			id844 := solana.MustHashFromBase58("5rFJD22afPd2CaNyMytF1nFWJ18969zevPZqZBuut6aB")
			id845 := solana.MustHashFromBase58("9fpQZwBJcbpS624CB9YLwnq86Y2bNBrbdrCn8ikTFVVS")
			invalidID := solana.MustHashFromBase58("GZKU7fvTgAV9gjjaUnwPsZnfxWYaeoMUtdU7mvYBPdu6")
			childID := solana.MustHashFromBase58("7V7NFU9sq2gNqjW1UWRs67AWmo3n3sAhDX2sJVFpKRP")
			root := invalidTestLiveBlock(rootSlot, rootSlot-1, rootID, solana.Hash{1})
			block844 := invalidTestLiveBlock(rootSlot+1, rootSlot, id844, rootID)
			block845 := invalidTestLiveBlock(rootSlot+2, rootSlot+1, id845, id844)
			invalid := invalidTestLiveBlock(rootSlot+3, rootSlot+2, invalidID, id845)
			child := invalidTestLiveBlock(rootSlot+5, rootSlot, childID, rootID)
			for _, blk := range []*b.Block{root, block844, block845, child} {
				blk.HasAlpenglowFooter = true
			}
			bs := NewBlockSource(&BlockSourceOpts{
				SourceType:                   BlockSourceTurbine,
				TurbineAlpenglowBlockIDHints: true,
				StartSlot:                    rootSlot + 1,
				EndSlot:                      rootSlot + 10,
			})
			bs.isNearTip.Store(true)
			bs.liveHandoffSlot.Store(rootSlot + 1)
			for _, blk := range []*b.Block{root, block844, block845, invalid} {
				bs.lastEmittedBlockSlot = blk.Slot
				bs.recordEmittedAlpenglowBlockIDLocked(blk)
			}
			bs.nextSlotToSend = invalid.Slot + 1
			bs.streamChan <- invalid // a duplicate delivery waiting behind replay
			if buffered {
				bs.reorderBuffer[child.Slot] = child
				bs.slotState[child.Slot] = slotDone
				bs.reorderMu.Lock()
				queued := bs.queueBufferedAlpenglowParentSwitchLocked()
				bs.reorderMu.Unlock()
				require.True(t, queued)
			}

			require.NoError(t, bs.QuarantineInvalidAlpenglowBlock(invalid))
			require.Equal(t, invalid.Slot, bs.nextSlotToSend)
			require.Equal(t, block845.Slot, bs.lastEmittedBlockSlot)
			require.Equal(t, rootID, bs.emittedAlpenglowBlockIDs[rootSlot])
			require.True(t, bs.IsObjectivelyInvalidAlpenglowBlock(invalid))
			require.False(t, bs.IsObjectivelyInvalidAlpenglowBlock(child))
			require.False(t, bs.skippedSlots[invalid.Slot], "rejection alone must not invent a skip")
			require.Zero(t, bs.BufferDepth(), "drain duplicate deliveries")
			require.Empty(t, bs.alpenglowParentSwitchCh, "drain superseded switch notifications")

			done := make(chan struct{})
			go func() { bs.emitOrderedBlocks(); close(done) }()
			t.Cleanup(func() {
				close(bs.resultQueue)
				waitInvalidTestDone(t, done, "missing-footer recovery emitter")
			})
			generation := bs.liveResultGeneration.Load()
			bs.resultQueue <- fetchResult{slot: invalid.Slot, block: invalid, liveStreamGeneration: generation}
			bs.resultQueue <- fetchResult{slot: child.Slot, block: child, liveStreamGeneration: generation}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			input := bs.NextReplayInput(ctx, nil, nil, nil)
			require.Nil(t, input.Block)
			require.NotNil(t, input.ParentSwitch, "the earlier ancestor must remain selectable")
			require.Equal(t, rootSlot+1, input.ParentSwitch.SwitchSlot)
			require.Equal(t, rootSlot, input.ParentSwitch.ParentSlot)
			require.Equal(t, child.Slot, input.ParentSwitch.ChildSlot)
			require.True(t, bs.RewindForAlpenglowParentSwitch(*input.ParentSwitch))
			for slot := rootSlot + 1; slot < child.Slot; slot++ {
				input = bs.NextReplayInput(ctx, nil, nil, nil)
				require.NotNil(t, input.Block)
				require.Equal(t, slot, input.Block.Slot)
				require.True(t, input.Block.IsSkipped)
			}
			input = bs.NextReplayInput(ctx, nil, nil, nil)
			require.Same(t, child, input.Block)
			require.True(t, bs.IsObjectivelyInvalidAlpenglowBlock(invalid), "fork selection must not clear the invalid identity")
			bs.reorderMu.Lock()
			certifiedSkip := bs.alpenglowCertifiedSkips[invalid.Slot]
			bs.reorderMu.Unlock()
			require.False(t, certifiedSkip, "a parent-linked skip is provisional")
		})
	}
}
