package blockstream

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

var ErrBlockNotQuarantinable = errors.New("block is not a quarantinable live Alpenglow emission")

// QuarantineInvalidAlpenglowBlock rejects an objectively invalid block that
// this source already emitted. It hard-tombstones the exact emitted suffix,
// rewinds the ancestry/frontier, drains queued descendants, invalidates live
// results, and discards poisoned assembler/spool state. It deliberately does
// not infer a skip: an alternate block or a certified skip must advance slot.
//
// Objective-invalid tombstones differ from speculative fork tombstones:
// SetKnownAlpenglowBlockID cannot clear them, even if a certificate names the
// same identity. Such a certificate is a safety contradiction and the source
// fails closed rather than re-authorizing invalid transaction contents.
//
// Only replay's serialized recovery path may call this method. It must not
// overlap RewindForAlpenglowSwitch: replay owns the shared emission gate, and
// the certified rewind's shutdown mutex does not serialize quarantine calls.
func (bs *BlockSource) QuarantineInvalidAlpenglowBlock(blk *b.Block) error {
	if bs == nil || blk == nil {
		return fmt.Errorf("%w: nil source or block", ErrBlockNotQuarantinable)
	}
	if bs.stopped.Load() {
		return fmt.Errorf("%w: block source is stopped", ErrBlockNotQuarantinable)
	}
	if bs.sourceType != BlockSourceTurbine || !bs.turbineAlpenglowBlockIDHints {
		return fmt.Errorf("%w: source is not Alpenglow Turbine", ErrBlockNotQuarantinable)
	}
	if !blk.FromLiveStream || blk.FromLocalProduction {
		return fmt.Errorf("%w: slot %d was not emitted from the live network stream", ErrBlockNotQuarantinable, blk.Slot)
	}
	if blk.Slot == 0 || !blk.HasAlpenglowBlockID {
		return fmt.Errorf("%w: slot or block ID is missing", ErrBlockNotQuarantinable)
	}

	from := blk.Slot
	invalidID := solana.Hash(blk.AlpenglowBlockID)
	if invalidID == (solana.Hash{}) {
		return fmt.Errorf("%w: block ID is zero", ErrBlockNotQuarantinable)
	}
	// CAS rejects an already-owned gate, but cannot protect against a concurrent
	// certified rewind's Store. Replay must serialize both recovery methods.
	if !bs.alpenglowQuarantineFrom.CompareAndSwap(0, from) {
		return fmt.Errorf("%w: another Alpenglow quarantine is in progress", ErrBlockNotQuarantinable)
	}
	quarantineComplete := false
	defer func() {
		if !quarantineComplete {
			bs.alpenglowQuarantineFrom.CompareAndSwap(from, 0)
		}
	}()

	poisonedSlots := make(map[uint64]solana.Hash)
	resetSlots := make(map[uint64]struct{})

	bs.reorderMu.Lock()
	emittedID, emitted := bs.emittedAlpenglowBlockIDs[from]
	if !emitted || emittedID != invalidID {
		bs.reorderMu.Unlock()
		return fmt.Errorf("%w: block %s at slot %d is not the exact emitted identity", ErrBlockNotQuarantinable, invalidID, from)
	}
	bs.alpenglowRewindWait.clear()

	// Every identity already emitted after this block is its selected suffix
	// and therefore descends from objectively invalid contents. These hard
	// tombstones are checked before certificate overrides.
	bs.rejectedAlpenglowMu.Lock()
	if bs.invalidAlpenglowBlockIDs == nil {
		bs.invalidAlpenglowBlockIDs = make(map[uint64]map[solana.Hash]struct{})
	}
	for slot, id := range bs.emittedAlpenglowBlockIDs {
		if slot < from || id == (solana.Hash{}) {
			continue
		}
		ids := bs.invalidAlpenglowBlockIDs[slot]
		if ids == nil {
			ids = make(map[solana.Hash]struct{})
			bs.invalidAlpenglowBlockIDs[slot] = ids
		}
		ids[id] = struct{}{}
		poisonedSlots[slot] = id
	}
	bs.pruneRejectedAlpenglowLocked(bs.lastEmittedBlockSlot)
	bs.rejectedAlpenglowMu.Unlock()

	bs.nextSlotToSend = from
	bs.rewindAlpenglowEmissionAnchorLocked(from)
	bs.clearAlpenglowBufferedSuffixLocked(from, resetSlots)
	for slot := range bs.skippedSlots {
		if slot < from || bs.alpenglowCertifiedSkips[slot] {
			continue
		}
		delete(bs.skippedSlots, slot)
		delete(bs.liveSynthesizedSkips, slot)
		delete(bs.alpenglowCertifiedSkips, slot)
	}
	bs.pendingAlpenglowParentSwitch = nil
	bs.reorderMu.Unlock()

	var invalidSinkErr error
	if bs.alpenglowInvalidBlockSink != nil {
		if err := bs.alpenglowInvalidBlockSink(
			alpenglow.BlockID{Slot: from, Hash: invalidID},
			"replay validation rejected emitted block"); err != nil {
			invalidSinkErr = err
		}
		// Consensus-side parent/repair state also needs every already-emitted
		// descendant identity tombstoned. Notify all of them even after the
		// first safety conflict; physical source cleanup must still complete.
		descendantSlots := make([]uint64, 0, len(poisonedSlots))
		for slot := range poisonedSlots {
			if slot > from {
				descendantSlots = append(descendantSlots, slot)
			}
		}
		sort.Slice(descendantSlots, func(i, j int) bool { return descendantSlots[i] < descendantSlots[j] })
		for _, slot := range descendantSlots {
			err := bs.alpenglowInvalidBlockSink(
				alpenglow.BlockID{Slot: slot, Hash: poisonedSlots[slot]},
				fmt.Sprintf("emitted descendant of replay-invalid block %s at slot %d", invalidID, from))
			if err != nil && invalidSinkErr == nil {
				invalidSinkErr = err
			}
		}
	}
	for slot, id := range poisonedSlots {
		bs.markInvalidAlpenglowBlockID(slot, id)
	}

	// Invalidate every already-queued/in-flight live result before clearing its
	// bookkeeping. Newly arriving results receive the new generation and wait
	// behind alpenglowQuarantineFrom until the drain barrier completes.
	resultGeneration := bs.invalidateLiveStreamResults()

	bs.clearAlpenglowTrackedSuffix(from, resetSlots)
	bs.clearAlpenglowRetrySuffix(from)
	bs.clearAlpenglowStagedSuffix(from, resetSlots)

	bs.drainAlpenglowParentSwitchNotifications()
	bs.waitForAlpenglowEmissionBarrier(from, resetSlots)

	// A result that passed its generation check just before invalidation may
	// have inserted stale suffix state after the first clear. The single emitter
	// is now past every pre-gate send; clear that late state and reassert the
	// frontier before the final drain.
	bs.reorderMu.Lock()
	bs.nextSlotToSend = from
	bs.rewindAlpenglowEmissionAnchorLocked(from)
	bs.clearAlpenglowBufferedSuffixLocked(from, resetSlots)
	for slot := range bs.skippedSlots {
		if slot < from || bs.alpenglowCertifiedSkips[slot] {
			continue
		}
		delete(bs.skippedSlots, slot)
		delete(bs.liveSynthesizedSkips, slot)
		delete(bs.alpenglowCertifiedSkips, slot)
	}
	bs.pendingAlpenglowParentSwitch = nil
	bs.reorderMu.Unlock()
	bs.clearAlpenglowTrackedSuffix(from, resetSlots)
	bs.drainAlpenglowParentSwitchNotifications()
	bs.drainAlpenglowEmissionSuffixForReset(from, resetSlots)

	allSlots := make([]uint64, 0, len(resetSlots)+len(poisonedSlots))
	for slot := range resetSlots {
		allSlots = append(allSlots, slot)
	}
	for slot := range poisonedSlots {
		if _, exists := resetSlots[slot]; !exists {
			allSlots = append(allSlots, slot)
		}
	}
	sort.Slice(allSlots, func(i, j int) bool { return allSlots[i] < allSlots[j] })
	for _, slot := range allSlots {
		bs.clearSlotErrors(slot)
		bs.clearLiveRepairSlot(slot)
		if poisonedID, poisoned := poisonedSlots[slot]; poisoned {
			bs.rejectInvalidTurbineBlockState(slot, poisonedID)
		} else {
			// This candidate was buffered but not proven to descend from the
			// invalid identity. Reset its completed marker without destroying
			// spool data so a valid alternate can be rehydrated cheaply.
			bs.resetTurbineSlotState(slot)
		}
	}

	bs.alpenglowQuarantineFrom.CompareAndSwap(from, 0)
	quarantineComplete = true
	bs.lastProgress.Store(time.Now().Unix())
	select {
	case bs.resultQueue <- fetchResult{wakeEmitter: true}:
	default:
		// A pending result already guarantees an emitter pass.
	}
	mlog.Log.Warnf("ALPENGLOW invalid block quarantined: block %s at slot %d and %d emitted descendant identities rejected; frontier rewound without inferring a skip (live generation %d)",
		invalidID, from, len(poisonedSlots)-1, resultGeneration)
	if invalidSinkErr != nil {
		bs.setStopReason(blockSourceStopReasonInvalidAlpenglowCertificate, from)
		return fmt.Errorf("invalid block %s at slot %d conflicts with consensus: %w", invalidID, from, invalidSinkErr)
	}
	return nil
}

func (bs *BlockSource) drainAlpenglowParentSwitchNotifications() {
	for {
		select {
		case <-bs.alpenglowParentSwitchCh:
		default:
			return
		}
	}
}

func (bs *BlockSource) drainEmittedAlpenglowSuffix(from uint64) []uint64 {
	var retained []*b.Block
	var drained []uint64
drain:
	for {
		select {
		case blk, open := <-bs.streamChan:
			if !open {
				break drain
			}
			if blk == nil || blk.Slot >= from {
				if blk != nil {
					drained = append(drained, blk.Slot)
				}
				continue
			}
			retained = append(retained, blk)
		default:
			break drain
		}
	}
	for _, blk := range retained {
		select {
		case bs.streamChan <- blk:
		case <-bs.stopChan:
			return drained
		}
	}
	return drained
}
