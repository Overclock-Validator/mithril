package blockstream

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gagliardetto/solana-go"
)

// ReplaySession owns reception across successive serialized replay attempts.
// It keeps the socket, repair peers, bounded receive buffers and shred spool
// alive while an attempt rebuilds bank state. Close belongs to the outer node
// lifecycle, not to an individual ReplayBlocks call.
type ReplaySession struct {
	source *BlockSource
	done   chan struct{}
}

// Acquire must be called by the serialized replay owner. The returned source
// is paused until Run, allowing the new attempt to install its prefetch hook.
func (s *ReplaySession) Acquire(opts *BlockSourceOpts) (*BlockSource, bool, error) {
	if opts.SourceType != BlockSourceTurbine {
		return nil, false, fmt.Errorf("persistent reception requires a turbine source")
	}
	if s.source != nil && !s.source.stopped.Load() {
		if err := s.source.resetReplayAnchor(opts.StartSlot, opts.InitialAlpenglowBlockID, opts.HasInitialAlpenglowBlockID); err != nil {
			return nil, false, err
		}
		return s.source, true, nil
	}
	if s.source != nil {
		s.Close()
	}
	s.source = NewBlockSource(opts)
	s.source.replayPaused.Store(true)
	s.done = nil
	return s.source, false, nil
}

func (s *ReplaySession) Run() {
	if s.done == nil {
		s.done = make(chan struct{})
		source, done := s.source, s.done
		go func() {
			defer close(done)
			source.Start()
		}()
	}
	s.source.certifiedRewindMu.Lock()
	if !s.source.stopped.Load() {
		s.source.replayPaused.Store(false)
		s.source.wakeReplayEmitter()
	}
	s.source.certifiedRewindMu.Unlock()
}

// Release joins the old attempt's emission hook without stopping reception.
func (s *ReplaySession) Release() {
	if s.source != nil {
		s.source.pauseReplay()
	}
}

func (s *ReplaySession) Close() {
	if s.source != nil {
		s.source.Stop()
		if s.done != nil {
			<-s.done
		}
		s.source, s.done = nil, nil
	}
}

func (bs *BlockSource) wakeReplayEmitter() {
	select {
	case bs.resultQueue <- fetchResult{wakeEmitter: true}:
	default:
	}
}

func (bs *BlockSource) pauseReplay() {
	bs.certifiedRewindMu.Lock()
	defer bs.certifiedRewindMu.Unlock()
	bs.replayPaused.Store(true)
	// Add to replayEmissionWg happens under reorderMu. Crossing the lock
	// after closing the gate ensures no new sends can start during Wait.
	bs.reorderMu.Lock()
	dropped := bs.trimPausedReplayBufferLocked()
	bs.reorderMu.Unlock()
	bs.resetPausedReplayDrops(dropped)
	// Release a send blocked on a full channel, join it, then collect its
	// late result. Keep unconsumed blocks in the existing bounded buffer.
	bs.collectPausedBlocks()
	bs.replayEmissionWg.Wait()
	bs.collectPausedBlocks()
	bs.SetReplayEmitHook(nil)
}

func (bs *BlockSource) collectPausedBlocks() {
	for {
		select {
		case blk, ok := <-bs.streamChan:
			if !ok {
				return
			}
			if blk == nil || blk.IsSkipped || bs.isRejectedAlpenglowBlock(blk) {
				continue
			}
			bs.reorderMu.Lock()
			if bs.reorderBuffer[blk.Slot] == nil {
				bs.reorderBuffer[blk.Slot] = blk
			}
			dropped := bs.trimPausedReplayBufferLocked()
			bs.reorderMu.Unlock()
			bs.resetPausedReplayDrops(dropped)
		default:
			return
		}
	}
}

// resetReplayAnchor changes execution delivery only. Branch exclusions and
// certificate hints remain authoritative, and future received blocks remain
// buffered. Previously consumed blocks can rehydrate from the same shred
// spool; resetting their completion markers does not discard their packets.
func (bs *BlockSource) resetReplayAnchor(next uint64, parentID solana.Hash, hasParentID bool) error {
	if next == 0 || !bs.replayPaused.Load() {
		return fmt.Errorf("replay source must be paused with a nonzero resume slot")
	}
	bs.certifiedRewindMu.Lock()
	defer bs.certifiedRewindMu.Unlock()
	if bs.stopped.Load() {
		return fmt.Errorf("cannot resume a stopped block source")
	}
	reset := make(map[uint64]struct{})
	bs.reorderMu.Lock()
	for slot := range bs.emittedAlpenglowBlockIDs {
		if slot >= next && bs.reorderBuffer[slot] == nil {
			reset[slot] = struct{}{}
		}
	}
	bs.rewindAlpenglowEmissionAnchorLocked(next)
	bs.nextSlotToSend = next
	bs.lastEmittedBlockSlot = next - 1
	bs.lastEmittedAlpenglowBlockID = parentID
	bs.hasLastEmittedAlpenglowBlockID = hasParentID
	if hasParentID {
		if _, exists := bs.emittedAlpenglowBlockIDs[next-1]; !exists {
			bs.emittedAlpenglowBlockIDOrder = append(bs.emittedAlpenglowBlockIDOrder, next-1)
		}
		bs.emittedAlpenglowBlockIDs[next-1] = parentID
	}
	for slot := range bs.reorderBuffer {
		if slot < next {
			delete(bs.reorderBuffer, slot)
		}
	}
	// Re-derive skips from retained certificates and exact child ancestry.
	clear(bs.skippedSlots)
	clear(bs.liveSynthesizedSkips)
	clear(bs.alpenglowCertifiedSkips)
	bs.pendingAlpenglowParentSwitch = nil
	bs.alpenglowRewindWait.clear()
	bs.slotStateMu.Lock()
	for slot := range bs.slotState {
		if bs.reorderBuffer[slot] == nil {
			delete(bs.slotState, slot)
			delete(bs.inflightStart, slot)
		}
	}
	bs.slotStateMu.Unlock()
	bs.reorderMu.Unlock()
	bs.drainAlpenglowParentSwitchNotifications()
	for slot := range reset {
		bs.resetTurbineSlotState(slot)
	}
	for _, frontier := range []*atomic.Uint64{&bs.liveHandoffSlot, &bs.repairCatchupFrom} {
		for old := frontier.Load(); old > next; old = frontier.Load() {
			if frontier.CompareAndSwap(old, next) {
				break
			}
		}
	}
	bs.lastExecutedSlot.Store(next - 1)
	bs.liveNeedRPCResume.Store(true)
	bs.lastProgress.Store(time.Now().Unix())
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver != nil {
		receiver.SetRetentionFloor(next)
		receiver.SetHydrationWindow(next, next+repairCatchupLiveDeliverWindow)
	}
	return nil
}
