package blockstream

import (
	"encoding/binary"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func pausedTestBlockID(slot uint64) solana.Hash {
	var id solana.Hash
	binary.LittleEndian.PutUint64(id[:8], slot)
	return id
}

func TestPausedReceptionCapsDecodedBlocksAndResumesEarliest(t *testing.T) {
	const first, last, capacity = uint64(101), uint64(140), 8
	opts := &BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: first, EndSlot: 200,
		DisableRPCBlockFetch: true, TurbineAlpenglowBlockIDHints: true,
		InitialAlpenglowBlockID: pausedTestBlockID(first - 1), HasInitialAlpenglowBlockID: true}
	bs := NewBlockSource(opts)
	bs.maxPending = capacity
	bs.liveHandoffSlot.Store(first)
	bs.isNearTip.Store(true)
	bs.pauseReplay()
	done := make(chan struct{})
	go func() { defer close(done); bs.emitOrderedBlocks() }()
	t.Cleanup(func() {
		bs.Stop()
		close(bs.resultQueue)
		waitInvalidTestDone(t, done, "paused-cap emitter")
	})
	blockAt := func(slot uint64) *b.Block {
		return invalidTestLiveBlock(slot, slot-1, pausedTestBlockID(slot), pausedTestBlockID(slot-1))
	}
	// Reverse order forces eviction of earlier arrivals, not just rejection of
	// incoming far-future blocks. Reception remains live while replay is absent.
	for slot := last; slot >= first; slot-- {
		require.True(t, bs.ingestLiveShredBlock(blockAt(slot)))
	}
	waitForBlockSourceCondition(t, func() bool {
		bs.slotStateMu.Lock()
		defer bs.slotStateMu.Unlock()
		return bs.slotState[first] == slotDone
	})
	bs.reorderMu.Lock()
	require.Len(t, bs.reorderBuffer, capacity)
	for slot := first; slot < first+capacity; slot++ {
		require.Contains(t, bs.reorderBuffer, slot)
	}
	bs.reorderMu.Unlock()
	bs.slotStateMu.Lock()
	for slot := first + capacity; slot <= last; slot++ {
		require.NotContains(t, bs.slotState, slot, "evicted completion must permit repair delivery")
		require.NotContains(t, bs.inflightStart, slot)
	}
	bs.slotStateMu.Unlock()
	require.Empty(t, bs.streamChan)
	require.NoError(t, bs.resetReplayAnchor(first, pausedTestBlockID(first-1), true))
	bs.replayPaused.Store(false)
	bs.wakeReplayEmitter()
	for slot := first; slot < first+capacity; slot++ {
		select {
		case got := <-bs.streamChan:
			require.Equal(t, slot, got.Slot)
		case <-time.After(time.Second):
			t.Fatal("earliest buffered block lost during resume")
		}
	}
	// Model the preserved spool rehydrating evicted blocks. No stale slotDone
	// entry may make the repaired block disappear on its second delivery.
	for slot := first + capacity; slot <= last; slot++ {
		require.True(t, bs.ingestLiveShredBlock(blockAt(slot)))
		select {
		case got := <-bs.streamChan:
			require.Equal(t, slot, got.Slot)
		case <-time.After(time.Second):
			t.Fatal("evicted block could not be delivered again")
		}
	}
}

func TestPausedCollectionRetainsQueuedHeadOverFutureBuffer(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 101, EndSlot: 200})
	bs.maxPending = 2
	bs.reorderBuffer[103] = &b.Block{Slot: 103}
	bs.reorderBuffer[104] = &b.Block{Slot: 104}
	bs.slotState[103], bs.slotState[104] = slotDone, slotDone
	bs.streamChan <- &b.Block{Slot: 101}
	bs.streamChan <- &b.Block{Slot: 102}
	bs.pauseReplay()
	require.Len(t, bs.reorderBuffer, 2)
	require.Contains(t, bs.reorderBuffer, uint64(101))
	require.Contains(t, bs.reorderBuffer, uint64(102))
	require.NotContains(t, bs.slotState, uint64(103))
	require.NotContains(t, bs.slotState, uint64(104))
	bs.Stop()
}

func TestPausedReceptionDoesNotTripStallWatchdog(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 101, EndSlot: 200, DisableRPCBlockFetch: true})
	bs.stallTimeout = time.Millisecond
	bs.lastProgress.Store(1)
	bs.pauseReplay()
	done := make(chan struct{})
	go func() { defer close(done); bs.scheduler() }()
	t.Cleanup(func() { bs.Stop(); waitInvalidTestDone(t, done, "paused scheduler") })
	select {
	case <-done:
		t.Fatalf("paused scheduler stopped: %s", bs.StopReason())
	case <-time.After(30 * time.Millisecond):
	}
	require.Equal(t, blockSourceStopReasonNone, bs.stopReasonEnum())
	bs.Stop()
	waitInvalidTestDone(t, done, "explicitly stopped paused scheduler")
}
