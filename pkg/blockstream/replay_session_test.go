package blockstream

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestReplaySessionKeepsReceptionAndQueuedBranch(t *testing.T) {
	parentID := solana.Hash{1}
	opts := &BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 101, EndSlot: 200,
		TurbineBindAddr: "127.0.0.1:0", DisableRPCBlockFetch: true,
		TurbineAlpenglowBlockIDHints: true, InitialAlpenglowBlockID: parentID, HasInitialAlpenglowBlockID: true}
	bs := NewBlockSource(opts)
	bs.liveHandoffSlot.Store(101)
	bs.isNearTip.Store(true)
	first := invalidTestLiveBlock(101, 100, solana.Hash{2}, parentID)
	second := invalidTestLiveBlock(102, 101, solana.Hash{3}, solana.Hash{2})
	third := invalidTestLiveBlock(103, 102, solana.Hash{4}, solana.Hash{3})
	var oldHook, newHook atomic.Int32
	bs.SetReplayEmitHook(func(*b.Block) { oldHook.Add(1) })
	done := make(chan struct{})
	go func() { defer close(done); bs.emitOrderedBlocks() }()
	t.Cleanup(func() { bs.Stop(); close(bs.resultQueue); waitInvalidTestDone(t, done, "session emitter") })
	require.True(t, bs.ingestLiveShredBlock(first))
	require.True(t, bs.ingestLiveShredBlock(second))
	waitForBlockSourceCondition(t, func() bool { return len(bs.streamChan) == 2 })
	// Replay consumed 101, then needs to reconstruct from that exact parent.
	require.Same(t, first, <-bs.streamChan)
	session := &ReplaySession{source: bs}
	session.Release()
	require.False(t, bs.stopped.Load())
	require.Nil(t, bs.replayEmitHook.Load())
	require.True(t, bs.ingestLiveShredBlock(third))
	waitForBlockSourceCondition(t, func() bool {
		bs.reorderMu.Lock()
		defer bs.reorderMu.Unlock()
		return bs.reorderBuffer[third.Slot] == third
	})
	require.Empty(t, bs.streamChan, "received blocks must not enter a detached replay attempt")
	opts.StartSlot, opts.InitialAlpenglowBlockID = 102, solana.Hash{2}
	reused, wasReused, err := session.Acquire(opts)
	require.NoError(t, err)
	require.True(t, wasReused)
	require.Same(t, bs, reused)
	require.Equal(t, uint64(101), bs.lastExecutedSlot.Load())
	bs.SetReplayEmitHook(func(*b.Block) { newHook.Add(1) })
	bs.replayPaused.Store(false)
	bs.wakeReplayEmitter()
	for _, want := range []*b.Block{second, third} {
		select {
		case got := <-bs.streamChan:
			require.Same(t, want, got)
		case <-time.After(time.Second):
			t.Fatal("buffered branch was lost across replay attempts")
		}
	}
	require.Equal(t, int32(2), oldHook.Load())
	require.Equal(t, int32(2), newHook.Load())
}

func TestReplaySessionPauseJoinsInFlightEmitHook(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 11, EndSlot: 100, DisableRPCBlockFetch: true})
	blk := &b.Block{Slot: 11, ParentSlot: 10}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	bs.SetReplayEmitHook(func(*b.Block) { close(entered); <-release })
	done := make(chan struct{})
	go func() { defer close(done); bs.emitOrderedBlocks() }()
	paused := make(chan struct{})
	t.Cleanup(func() {
		unblock()
		bs.Stop()
		close(bs.resultQueue)
		waitInvalidTestDone(t, done, "pause emitter")
	})
	bs.resultQueue <- fetchResult{slot: blk.Slot, block: blk}
	waitInvalidTestDone(t, entered, "emit hook")
	go func() { bs.pauseReplay(); close(paused) }()
	waitForBlockSourceCondition(t, bs.replayPaused.Load)
	select {
	case <-paused:
		t.Fatal("pause returned while the old prefetch hook was still running")
	default:
	}
	unblock()
	waitInvalidTestDone(t, paused, "pause barrier")
	require.Empty(t, bs.streamChan)
	bs.reorderMu.Lock()
	defer bs.reorderMu.Unlock()
	require.Same(t, blk, bs.reorderBuffer[blk.Slot])
}

func TestReplayAnchorResetPreservesRejectedIdentity(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 11, EndSlot: 100, TurbineAlpenglowBlockIDHints: true})
	bad := invalidTestLiveBlock(12, 11, solana.Hash{3}, solana.Hash{2})
	bs.rejectedAlpenglowBlockIDs[12] = map[solana.Hash]struct{}{solana.Hash{3}: {}}
	bs.emittedAlpenglowBlockIDs[11] = solana.Hash{2}
	bs.emittedAlpenglowBlockIDOrder = []uint64{11}
	bs.lastEmittedBlockSlot = 11
	bs.pauseReplay()
	require.NoError(t, bs.resetReplayAnchor(11, solana.Hash{1}, true))
	require.True(t, bs.isRejectedAlpenglowBlock(bad))
	require.Equal(t, uint64(10), bs.lastEmittedBlockSlot)
	require.Equal(t, solana.Hash{1}, bs.lastEmittedAlpenglowBlockID)
	require.NotContains(t, bs.emittedAlpenglowBlockIDs, uint64(11))
	bs.Stop()
}
