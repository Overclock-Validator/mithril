// live_gap: Live-stream gap detection and healing: a missing slot is repair's job on turbine and a reconnect/RPC question on lightbringer.
package blockstream

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	alpenglowEmittedBlockIDHistory  = 65_536
	alpenglowMaxParentLinkedSkipRun = 65_536
)

type alpenglowSlotRange struct {
	from uint64 // inclusive
	to   uint64 // exclusive
}

func (bs *BlockSource) clearLiveGapWatch() {
	bs.lightbringerGapSlot.Store(0)
	bs.liveGapSinceUnix.Store(0)
	bs.liveGapLastLogUnix.Store(0)
	bs.lightbringerGapReconnectSlot.Store(0)
}

func (bs *BlockSource) invalidateLiveStreamResults() uint64 {
	return bs.liveResultGeneration.Add(1)
}

func (bs *BlockSource) setLiveStreamCancel(cancel context.CancelFunc) {
	bs.liveCancelMu.Lock()
	bs.liveCancel = cancel
	bs.liveCancelMu.Unlock()
}

func (bs *BlockSource) clearLiveStreamCancel() {
	bs.liveCancelMu.Lock()
	bs.liveCancel = nil
	bs.liveCancelMu.Unlock()
}

func (bs *BlockSource) requestLiveStreamReconnect(reason string) bool {
	if !bs.liveStreamConnected.Load() {
		return false
	}
	if !bs.liveReconnectRequested.CompareAndSwap(false, true) {
		return false
	}

	bs.reorderMu.Lock()
	waitingSlot := bs.nextSlotToSend
	lastEmitted := bs.lastEmittedBlockSlot
	bs.reorderMu.Unlock()
	latestStreamed := bs.liveLastStreamSlot.Load()

	action := "stream reconnect requested"
	if bs.sourceType == BlockSourceTurbine {
		// Not a remote stream: this restarts our LOCAL UDP receiver.
		action = "local receiver restart requested"
	}
	mlog.Log.Warnf("%s %s: %s | waiting_slot=%d | last_emitted=%d | latest_streamed=%d",
		bs.liveShredStreamName(), action, reason, waitingSlot, lastEmitted, latestStreamed)

	bs.liveCancelMu.Lock()
	cancel := bs.liveCancel
	bs.liveCancelMu.Unlock()
	if cancel == nil {
		bs.liveReconnectRequested.Store(false)
		return false
	}
	cancel()
	return true
}

func isLiveStreamReconnectCancel(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return status.Code(err) == codes.Canceled
}

func (bs *BlockSource) maybeReconnectActiveLiveStreamForNoProgress(stallDuration time.Duration) {
	if !bs.usesLiveShredStream() {
		return
	}
	if !bs.liveStreamActive.Load() || !bs.isNearTip.Load() {
		return
	}
	if stallDuration < liveNoEmitReconnect {
		return
	}

	bs.reorderMu.Lock()
	waitingSlot := bs.nextSlotToSend
	waitingReady := bs.reorderBuffer[waitingSlot] != nil || bs.skippedSlots[waitingSlot]
	lastEmitted := bs.lastEmittedBlockSlot
	firstBufferedSlot := uint64(0)
	bufferedLightbringer := 0
	foundBuffered := false
	for slot, blk := range bs.reorderBuffer {
		if blk == nil || !blk.FromLiveStream || slot <= waitingSlot {
			continue
		}
		bufferedLightbringer++
		if !foundBuffered || slot < firstBufferedSlot {
			firstBufferedSlot = slot
			foundBuffered = true
		}
	}
	bs.reorderMu.Unlock()

	if waitingReady || len(bs.streamChan) > 0 {
		return
	}

	reason := fmt.Sprintf("no block emitted for %s while %s is active and replay is waiting on slot %d",
		stallDuration.Round(time.Second), bs.liveShredStreamName(), waitingSlot)
	if foundBuffered {
		reason = fmt.Sprintf("no block emitted for %s while waiting on slot %d (first_buffered=%d buffered_live_stream=%d last_emitted=%d)",
			stallDuration.Round(time.Second), waitingSlot, firstBufferedSlot, bufferedLightbringer, lastEmitted)
	}

	bs.requestLiveStreamReconnect(reason)
}

func (bs *BlockSource) isLiveRepairSlot(slot uint64) bool {
	return slot != 0 && bs.liveRepairSlot.Load() == slot
}

func (bs *BlockSource) clearLiveRepairSlot(slot uint64) {
	if slot == 0 {
		return
	}
	bs.liveRepairSlot.CompareAndSwap(slot, 0)
}

func (bs *BlockSource) resetLiveRepairSlot() {
	bs.liveRepairSlot.Store(0)
}

func (bs *BlockSource) liveBlockConnectsLocked(blk *b.Block) bool {
	if blk == nil || !blk.FromLiveStream {
		return true
	}
	if bs.lastEmittedBlockSlot == 0 {
		return true
	}
	if blk.SourceParentSlot == 0 {
		return false
	}
	if blk.SourceParentSlot != bs.lastEmittedBlockSlot {
		return false
	}
	if bs.isRejectedAlpenglowBlock(blk) {
		return false
	}
	// Classic turbine/lightbringer blocks have only slot ancestry. In
	// Alpenglow, use the exact parent identity whenever both sides provide it;
	// accepting a same-slot/different-ID parent would immediately put replay
	// back onto the suffix it just unwound.
	if bs.turbineAlpenglowBlockIDHints && blk.HasAlpenglowParentBlockID && bs.hasLastEmittedAlpenglowBlockID {
		return solana.Hash(blk.AlpenglowParentBlockID) == bs.lastEmittedAlpenglowBlockID
	}
	return true
}

func (bs *BlockSource) recordEmittedAlpenglowBlockIDLocked(blk *b.Block) {
	if blk == nil || !blk.HasAlpenglowBlockID {
		bs.lastEmittedAlpenglowBlockID = solana.Hash{}
		bs.hasLastEmittedAlpenglowBlockID = false
		return
	}
	id := solana.Hash(blk.AlpenglowBlockID)
	bs.lastEmittedAlpenglowBlockID = id
	bs.hasLastEmittedAlpenglowBlockID = true
	if _, exists := bs.emittedAlpenglowBlockIDs[blk.Slot]; !exists {
		bs.emittedAlpenglowBlockIDOrder = append(bs.emittedAlpenglowBlockIDOrder, blk.Slot)
	}
	bs.emittedAlpenglowBlockIDs[blk.Slot] = id
	if len(bs.emittedAlpenglowBlockIDOrder) > alpenglowEmittedBlockIDHistory {
		oldest := bs.emittedAlpenglowBlockIDOrder[0]
		bs.emittedAlpenglowBlockIDOrder = bs.emittedAlpenglowBlockIDOrder[1:]
		delete(bs.emittedAlpenglowBlockIDs, oldest)
	}
}

// rejectAlpenglowEmissionSuffixLocked tombstones exact identities from a
// discarded speculative suffix. Without this, a delayed assembler result can
// link to the same retained ancestor and reverse replay straight back onto the
// branch it just unwound. Caller holds reorderMu.
func (bs *BlockSource) rejectAlpenglowEmissionSuffixLocked(from uint64) {
	bs.rejectedAlpenglowMu.Lock()
	defer bs.rejectedAlpenglowMu.Unlock()
	for slot, id := range bs.emittedAlpenglowBlockIDs {
		if slot < from || id == (solana.Hash{}) {
			continue
		}
		ids := bs.rejectedAlpenglowBlockIDs[slot]
		if ids == nil {
			ids = make(map[solana.Hash]struct{})
			bs.rejectedAlpenglowBlockIDs[slot] = ids
		}
		ids[id] = struct{}{}
	}
	// Keep tombstones for the same bounded ancestry window as emitted IDs.
	// They exist only when a switch occurs, so pruning here avoids per-block
	// scans on the hot emission path.
	bs.pruneRejectedAlpenglowLocked(bs.lastEmittedBlockSlot)
}

// rejectAlpenglowSkipRange records the slots a selected parent-linked child
// proved absent on that speculative branch. This is stronger than tombstoning
// only blocks already emitted: an unseen/delayed block in the chosen skip range
// must not resurrect another branch and make replay oscillate. Certificates can
// still authorize an exact identity through allowRejectedAlpenglowBlockID.
func (bs *BlockSource) rejectAlpenglowSkipRange(from, to uint64) {
	if from == 0 || to <= from {
		return
	}
	bs.rejectedAlpenglowMu.Lock()
	bs.rejectedAlpenglowSkipRanges = append(bs.rejectedAlpenglowSkipRanges, alpenglowSlotRange{from: from, to: to})
	sort.Slice(bs.rejectedAlpenglowSkipRanges, func(i, j int) bool {
		return bs.rejectedAlpenglowSkipRanges[i].from < bs.rejectedAlpenglowSkipRanges[j].from
	})
	merged := bs.rejectedAlpenglowSkipRanges[:0]
	for _, r := range bs.rejectedAlpenglowSkipRanges {
		if len(merged) == 0 || r.from > merged[len(merged)-1].to {
			merged = append(merged, r)
			continue
		}
		if r.to > merged[len(merged)-1].to {
			merged[len(merged)-1].to = r.to
		}
	}
	bs.rejectedAlpenglowSkipRanges = merged
	bs.pruneRejectedAlpenglowLocked(bs.lastEmittedBlockSlot)
	bs.rejectedAlpenglowMu.Unlock()
}

func (bs *BlockSource) pruneRejectedAlpenglowLocked(tip uint64) {
	if tip <= alpenglowEmittedBlockIDHistory {
		return
	}
	floor := tip - alpenglowEmittedBlockIDHistory
	for slot := range bs.rejectedAlpenglowBlockIDs {
		if slot < floor {
			delete(bs.rejectedAlpenglowBlockIDs, slot)
		}
	}
	for slot := range bs.invalidAlpenglowBlockIDs {
		if slot < floor {
			delete(bs.invalidAlpenglowBlockIDs, slot)
		}
	}
	for slot := range bs.authoritativeAlpenglowBlockIDs {
		if slot < floor {
			delete(bs.authoritativeAlpenglowBlockIDs, slot)
		}
	}
	kept := bs.rejectedAlpenglowSkipRanges[:0]
	for _, r := range bs.rejectedAlpenglowSkipRanges {
		if r.to <= floor {
			continue
		}
		if r.from < floor {
			r.from = floor
		}
		kept = append(kept, r)
	}
	bs.rejectedAlpenglowSkipRanges = kept
}

func (bs *BlockSource) isRejectedAlpenglowBlock(blk *b.Block) bool {
	if blk == nil || !bs.turbineAlpenglowBlockIDHints || !blk.HasAlpenglowBlockID {
		return false
	}
	id := solana.Hash(blk.AlpenglowBlockID)
	bs.rejectedAlpenglowMu.RLock()
	if _, invalid := bs.invalidAlpenglowBlockIDs[blk.Slot][id]; invalid {
		bs.rejectedAlpenglowMu.RUnlock()
		return true
	}
	if allowed, ok := bs.authoritativeAlpenglowBlockIDs[blk.Slot]; ok && allowed == id {
		bs.rejectedAlpenglowMu.RUnlock()
		return false
	}
	_, rejected := bs.rejectedAlpenglowBlockIDs[blk.Slot][id]
	if !rejected {
		for _, r := range bs.rejectedAlpenglowSkipRanges {
			if blk.Slot >= r.from && blk.Slot < r.to {
				rejected = true
				break
			}
		}
	}
	bs.rejectedAlpenglowMu.RUnlock()
	return rejected
}

func (bs *BlockSource) isInvalidAlpenglowBlockID(slot uint64, id solana.Hash) bool {
	if slot == 0 || id == (solana.Hash{}) {
		return false
	}
	bs.rejectedAlpenglowMu.RLock()
	_, invalid := bs.invalidAlpenglowBlockIDs[slot][id]
	bs.rejectedAlpenglowMu.RUnlock()
	return invalid
}
func (bs *BlockSource) invalidAlpenglowCandidateReason(blk *b.Block) (string, bool) {
	if blk == nil || !bs.turbineAlpenglowBlockIDHints || !blk.FromLiveStream || !blk.HasAlpenglowBlockID {
		return "", false
	}
	id := solana.Hash(blk.AlpenglowBlockID)
	parentSlot := blk.SourceParentSlot
	if parentSlot == 0 {
		parentSlot = blk.ParentSlot
	}

	bs.rejectedAlpenglowMu.RLock()
	_, selfInvalid := bs.invalidAlpenglowBlockIDs[blk.Slot][id]
	parentInvalid := false
	if blk.HasAlpenglowParentBlockID {
		_, parentInvalid = bs.invalidAlpenglowBlockIDs[parentSlot][solana.Hash(blk.AlpenglowParentBlockID)]
	}
	bs.rejectedAlpenglowMu.RUnlock()

	if selfInvalid {
		return fmt.Sprintf("block %s at slot %d is objectively invalid", id, blk.Slot), true
	}
	if parentInvalid {
		return fmt.Sprintf("exact parent %s at slot %d is objectively invalid", solana.Hash(blk.AlpenglowParentBlockID), parentSlot), true
	}
	return "", false
}

func (bs *BlockSource) rejectInvalidAlpenglowCandidate(blk *b.Block, reason string) {
	if blk == nil || !blk.HasAlpenglowBlockID {
		return
	}
	id := solana.Hash(blk.AlpenglowBlockID)
	bs.markInvalidAlpenglowBlockID(blk.Slot, id)
	bs.rejectInvalidTurbineBlockState(blk.Slot, id)
	if bs.alpenglowInvalidBlockSink != nil {
		if err := bs.alpenglowInvalidBlockSink(alpenglow.BlockID{Slot: blk.Slot, Hash: id}, reason); err != nil {
			bs.setStopReason(blockSourceStopReasonInvalidAlpenglowCertificate, blk.Slot)
			mlog.Log.Errorf("ALPENGLOW SAFETY: invalid descendant at slot %d conflicts with consensus: %v", blk.Slot, err)
		}
	}
	mlog.Log.Warnf("ALPENGLOW invalid candidate rejected at commit: block %s at slot %d (%s)", id, blk.Slot, reason)
}

// IsObjectivelyInvalidAlpenglowBlock reports whether deterministic replay
// validation quarantined this exact block identity. Replay checks this after
// every source receive as a backstop against an in-flight send racing a
// stream-channel drain.
func (bs *BlockSource) IsObjectivelyInvalidAlpenglowBlock(blk *b.Block) bool {
	if blk == nil || !blk.HasAlpenglowBlockID {
		return false
	}
	return bs.isInvalidAlpenglowBlockID(blk.Slot, solana.Hash(blk.AlpenglowBlockID))
}

func (bs *BlockSource) markInvalidAlpenglowBlockID(slot uint64, id solana.Hash) bool {
	changed := bs.markInvalidAlpenglowBlockIDOnly(slot, id)
	for _, descendant := range bs.removeBufferedInvalidAlpenglowDescendants(alpenglow.BlockID{Slot: slot, Hash: id}) {
		bs.markInvalidAlpenglowBlockIDOnly(descendant.Slot, descendant.Hash)
		bs.rejectInvalidTurbineBlockState(descendant.Slot, descendant.Hash)
		bs.slotStateMu.Lock()
		delete(bs.slotState, descendant.Slot)
		delete(bs.inflightStart, descendant.Slot)
		bs.slotStateMu.Unlock()
		bs.clearSlotErrors(descendant.Slot)
		if bs.alpenglowInvalidBlockSink != nil {
			reason := fmt.Sprintf("descends from objectively invalid block %s at slot %d", id, slot)
			if err := bs.alpenglowInvalidBlockSink(descendant, reason); err != nil {
				bs.setStopReason(blockSourceStopReasonInvalidAlpenglowCertificate, descendant.Slot)
				mlog.Log.Errorf("ALPENGLOW SAFETY: buffered invalid descendant %s conflicts with consensus: %v", descendant, err)
			}
		}
	}
	return changed
}

func (bs *BlockSource) markInvalidAlpenglowBlockIDOnly(slot uint64, id solana.Hash) bool {
	if slot == 0 || id == (solana.Hash{}) {
		return false
	}
	bs.reorderMu.Lock()
	emittedTip := bs.lastEmittedBlockSlot
	bs.reorderMu.Unlock()
	bs.rejectedAlpenglowMu.Lock()
	if bs.invalidAlpenglowBlockIDs == nil {
		bs.invalidAlpenglowBlockIDs = make(map[uint64]map[solana.Hash]struct{})
	}
	ids := bs.invalidAlpenglowBlockIDs[slot]
	if ids == nil {
		ids = make(map[solana.Hash]struct{})
		bs.invalidAlpenglowBlockIDs[slot] = ids
	}
	_, existed := ids[id]
	ids[id] = struct{}{}
	if bs.authoritativeAlpenglowBlockIDs[slot] == id {
		delete(bs.authoritativeAlpenglowBlockIDs, slot)
	}
	bs.pruneRejectedAlpenglowLocked(emittedTip)
	bs.rejectedAlpenglowMu.Unlock()

	// Ordinary assembly learns block IDs into the same hint cache used for
	// receiver restarts. Unpin only this invalid identity; a distinct certified
	// sibling remains authoritative.
	bs.alpenglowMu.Lock()
	if bs.knownAlpenglowBlockIDs[slot] == id {
		delete(bs.knownAlpenglowBlockIDs, slot)
		kept := bs.knownAlpenglowBlockIDOrder[:0]
		for _, knownSlot := range bs.knownAlpenglowBlockIDOrder {
			if knownSlot != slot {
				kept = append(kept, knownSlot)
			}
		}
		bs.knownAlpenglowBlockIDOrder = kept
	}
	bs.alpenglowMu.Unlock()
	return !existed
}
func (bs *BlockSource) removeBufferedInvalidAlpenglowDescendants(root alpenglow.BlockID) []alpenglow.BlockID {
	if root.IsZero() || !root.HasHash() {
		return nil
	}

	var candidates []*b.Block
	bs.reorderMu.Lock()
	for _, blk := range bs.reorderBuffer {
		candidates = append(candidates, blk)
	}
	bs.reorderMu.Unlock()
	bs.liveStagingMu.Lock()
	for _, blk := range bs.liveStagingBuffer {
		candidates = append(candidates, blk)
	}
	bs.liveStagingMu.Unlock()

	invalid := map[alpenglow.BlockID]struct{}{root: {}}
	descendants := make(map[alpenglow.BlockID]struct{})
	for {
		advanced := false
		for _, blk := range candidates {
			if blk == nil || !blk.FromLiveStream || !blk.HasAlpenglowBlockID || !blk.HasAlpenglowParentBlockID {
				continue
			}
			child := alpenglow.BlockID{Slot: blk.Slot, Hash: solana.Hash(blk.AlpenglowBlockID)}
			if _, known := invalid[child]; known {
				continue
			}
			parentSlot := blk.SourceParentSlot
			if parentSlot == 0 {
				parentSlot = blk.ParentSlot
			}
			parent := alpenglow.BlockID{Slot: parentSlot, Hash: solana.Hash(blk.AlpenglowParentBlockID)}
			if _, parentInvalid := invalid[parent]; !parentInvalid {
				continue
			}
			invalid[child] = struct{}{}
			descendants[child] = struct{}{}
			advanced = true
		}
		if !advanced {
			break
		}
	}
	if len(descendants) == 0 {
		return nil
	}

	bs.reorderMu.Lock()
	for slot, blk := range bs.reorderBuffer {
		if blk == nil || !blk.HasAlpenglowBlockID {
			continue
		}
		id := alpenglow.BlockID{Slot: slot, Hash: solana.Hash(blk.AlpenglowBlockID)}
		if _, invalid := descendants[id]; invalid {
			delete(bs.reorderBuffer, slot)
		}
	}
	bs.reorderMu.Unlock()

	bs.liveStagingMu.Lock()
	for slot, blk := range bs.liveStagingBuffer {
		if blk == nil || !blk.HasAlpenglowBlockID {
			continue
		}
		id := alpenglow.BlockID{Slot: slot, Hash: solana.Hash(blk.AlpenglowBlockID)}
		if _, invalid := descendants[id]; invalid {
			delete(bs.liveStagingBuffer, slot)
		}
	}
	order := bs.liveStagingOrder[:0]
	for _, slot := range bs.liveStagingOrder {
		if _, retained := bs.liveStagingBuffer[slot]; retained {
			order = append(order, slot)
		}
	}
	bs.liveStagingOrder = order
	bs.liveStagingMu.Unlock()

	out := make([]alpenglow.BlockID, 0, len(descendants))
	for id := range descendants {
		out = append(out, id)
	}
	return out
}

func (bs *BlockSource) allowRejectedAlpenglowBlockID(slot uint64, id solana.Hash) {
	bs.rejectedAlpenglowMu.Lock()
	bs.authoritativeAlpenglowBlockIDs[slot] = id
	if ids := bs.rejectedAlpenglowBlockIDs[slot]; ids != nil {
		delete(ids, id)
		if len(ids) == 0 {
			delete(bs.rejectedAlpenglowBlockIDs, slot)
		}
	}
	bs.rejectedAlpenglowMu.Unlock()
}

func (bs *BlockSource) rejectAlpenglowBlockID(slot uint64, id solana.Hash) {
	if slot == 0 || id == (solana.Hash{}) {
		return
	}
	bs.rejectedAlpenglowMu.Lock()
	ids := bs.rejectedAlpenglowBlockIDs[slot]
	if ids == nil {
		ids = make(map[solana.Hash]struct{})
		bs.rejectedAlpenglowBlockIDs[slot] = ids
	}
	ids[id] = struct{}{}
	bs.pruneRejectedAlpenglowLocked(bs.lastEmittedBlockSlot)
	bs.rejectedAlpenglowMu.Unlock()
}

// synthesizeAlpenglowParentLinkedSkipsLocked advances over a missing slot run
// only when a later turbine block carries an exact Alpenglow parent block ID
// matching the last emitted block. The inference is deliberately provisional:
// replay records the skipped outcome and the certificate switch sweep unwinds
// it if Votor later names a real block in the range.
func (bs *BlockSource) synthesizeAlpenglowParentLinkedSkipsLocked() bool {
	if bs.sourceType != BlockSourceTurbine || !bs.turbineAlpenglowBlockIDHints || !bs.hasLastEmittedAlpenglowBlockID {
		return false
	}
	waitingSlot := bs.nextSlotToSend
	if waitingSlot == 0 || bs.reorderBuffer[waitingSlot] != nil || bs.skippedSlots[waitingSlot] {
		return false
	}

	childSlot := uint64(0)
	var child *b.Block
	for slot, candidate := range bs.reorderBuffer {
		if slot <= waitingSlot || candidate == nil || !candidate.HasAlpenglowBlockID || !candidate.HasAlpenglowParentBlockID {
			continue
		}
		if bs.isRejectedAlpenglowBlock(candidate) {
			continue
		}
		if candidate.SourceParentSlot != bs.lastEmittedBlockSlot || solana.Hash(candidate.AlpenglowParentBlockID) != bs.lastEmittedAlpenglowBlockID {
			continue
		}
		if child == nil || slot < childSlot {
			childSlot = slot
			child = candidate
		}
	}
	if child == nil {
		return false
	}
	if childSlot-waitingSlot > alpenglowMaxParentLinkedSkipRun {
		return false
	}
	// Never skip over an observed candidate, even if a later branch reconnects
	// to the anchor. A certificate must adjudicate that fork explicitly.
	for slot, candidate := range bs.reorderBuffer {
		if candidate != nil && slot >= waitingSlot && slot < childSlot {
			return false
		}
	}

	newSkips := uint64(0)
	for slot := waitingSlot; slot < childSlot; slot++ {
		if !bs.skippedSlots[slot] {
			newSkips++
			bs.stats.FetchSkipped.Add(1)
		}
		bs.skippedSlots[slot] = true
		bs.liveSynthesizedSkips[slot] = true
		delete(bs.alpenglowCertifiedSkips, slot)
		bs.slotStateMu.Lock()
		bs.slotState[slot] = slotDone
		delete(bs.inflightStart, slot)
		bs.slotStateMu.Unlock()
		bs.clearSlotErrors(slot)
	}
	if newSkips == 0 {
		return false
	}
	mlog.Log.Infof("ALPENGLOW speculative skip: slots %d-%d are absent on parent-linked branch %s -> %s at slot %d; certificate decisions remain authoritative",
		waitingSlot, childSlot-1, bs.lastEmittedAlpenglowBlockID, solana.Hash(child.AlpenglowBlockID), childSlot)
	return true
}

func (bs *BlockSource) shouldPreferIncomingLiveBlockLocked(existing, incoming *b.Block) bool {
	if existing == nil || incoming == nil {
		return false
	}
	if !existing.FromLiveStream || !incoming.FromLiveStream {
		return false
	}
	if existing.Slot != incoming.Slot {
		return false
	}
	return !bs.liveBlockConnectsLocked(existing) && bs.liveBlockConnectsLocked(incoming)
}

func (bs *BlockSource) waitingLiveParentMismatchLocked() (waitingSlot uint64, observedParentSlot uint64, expectedParentSlot uint64, mismatch bool) {
	blk := bs.reorderBuffer[bs.nextSlotToSend]
	if blk == nil || !blk.FromLiveStream {
		return 0, 0, 0, false
	}
	if bs.liveBlockConnectsLocked(blk) {
		return 0, 0, 0, false
	}
	return blk.Slot, blk.SourceParentSlot, bs.lastEmittedBlockSlot, true
}

// queueAlpenglowParentSwitchLocked bridges a post-emission fork observation to
// replay. The child must carry an exact parent block-ID link to an emitted
// historical ancestor; slot numbers alone are not enough to rewind state.
//
// The event remains pending until replay acknowledges it by rewinding the
// source. Keeping one pending event prevents a busy repair stream from flooding
// replay with the same transition while the source deliberately holds child.
func (bs *BlockSource) queueAlpenglowParentSwitchLocked(blk *b.Block) bool {
	if bs.sourceType != BlockSourceTurbine || !bs.turbineAlpenglowBlockIDHints || blk == nil ||
		!blk.FromLiveStream || !blk.HasAlpenglowBlockID || !blk.HasAlpenglowParentBlockID {
		return false
	}
	if blk.SourceParentSlot == 0 || blk.SourceParentSlot >= bs.lastEmittedBlockSlot || blk.SourceParentSlot >= blk.Slot {
		return false
	}
	// Finite replay uses an exclusive end frontier. A far-ahead live child may
	// expose a fork wholly outside that requested range while the scheduler is
	// already shutting down; it is irrelevant to this run and must not race the
	// source channel closure.
	if bs.endSlot != 0 && blk.SourceParentSlot+1 >= bs.endSlot {
		return false
	}
	if bs.isRejectedAlpenglowBlock(blk) {
		return false
	}
	parentID, ok := bs.emittedAlpenglowBlockIDs[blk.SourceParentSlot]
	if !ok || parentID != solana.Hash(blk.AlpenglowParentBlockID) {
		return false
	}
	childID := solana.Hash(blk.AlpenglowBlockID)
	if emittedID, emitted := bs.emittedAlpenglowBlockIDs[blk.Slot]; emitted && emittedID == childID {
		// A delayed duplicate from our current branch is not a fork.
		return false
	}

	event := AlpenglowParentSwitch{
		SwitchSlot: blk.SourceParentSlot + 1,
		ParentSlot: blk.SourceParentSlot,
		ParentID:   parentID,
		ChildSlot:  blk.Slot,
		ChildID:    childID,
	}
	if conflicts, reason := bs.alpenglowParentSwitchConflictsWithDecision(event); conflicts {
		// Exact parent links are speculative evidence only. Never let a late
		// assembled sibling override an already-decisive block/skip path; doing
		// so invalidates queued live results and can strand the next certified
		// block behind a stale completed-slot marker. The exact tombstone keeps
		// spool hydration from proposing the same losing identity repeatedly.
		bs.rejectAlpenglowBlockID(event.ChildSlot, event.ChildID)
		mlog.Log.Warnf("ALPENGLOW certified branch retained: dropping speculative child %s at slot %d linking to ancestor %s at slot %d (%s)",
			event.ChildID, event.ChildSlot, event.ParentID, event.ParentSlot, reason)
		return false
	}
	if event.ChildSlot < bs.nextSlotToSend && !bs.alpenglowParentSwitchSelectedByDecision(event) {
		// Exact parent linkage proves that this is a coherent alternate branch,
		// not that it won fork choice. Once the source has already emitted past
		// the child, replacing and tombstoning that progressed suffix on arrival
		// order alone is destructive: a delayed earlier-slot sibling could erase
		// the branch whose later finalized child proves the intervening skips.
		// Keep first-emitted selection until consensus decisively names the late
		// child; SetKnownAlpenglowBlockID then clears this soft tombstone.
		bs.rejectAlpenglowBlockID(event.ChildSlot, event.ChildID)
		mlog.Log.Warnf("ALPENGLOW progressed branch retained: dropping late speculative child %s at slot %d behind emitted frontier %d; exact parent link alone cannot replace the emitted suffix",
			event.ChildID, event.ChildSlot, bs.nextSlotToSend-1)
		return false
	}
	if pending := bs.pendingAlpenglowParentSwitch; pending != nil {
		return *pending == event
	}
	bs.pendingAlpenglowParentSwitch = &event
	select {
	case bs.alpenglowParentSwitchCh <- event:
		mlog.Log.Warnf("ALPENGLOW speculative fork: assembled block %s at slot %d links back to emitted ancestor %s at slot %d; requesting replay branch selection from slot %d",
			event.ChildID, event.ChildSlot, event.ParentID, event.ParentSlot, event.SwitchSlot)
	default:
		// A stale notification can only exist while its matching pending event
		// is set. Do not replace it and risk reordering two replay rewinds.
	}
	return true
}

func (bs *BlockSource) alpenglowParentSwitchSelectedByDecision(event AlpenglowParentSwitch) bool {
	if bs.alpenglowDecisionSource == nil {
		return false
	}
	decision, ok := bs.alpenglowDecisionSource(event.ChildSlot - 1)
	return ok && decision.Slot == event.ChildSlot && decision.Kind == alpenglow.ChainDecisionKindBlock && decision.Block.Hash == event.ChildID
}

// alpenglowParentSwitchConflictsWithDecision checks the entire direct-link
// span against decisions already known to consensus. Slots omitted by the
// proposed child may be certified skips; a decisive block inside that range,
// a skip at the child slot, a different decisive child, or any conflict makes
// the speculative transition ineligible. Unknown slots remain speculative and
// may still use the ordinary rewind path.
func (bs *BlockSource) alpenglowParentSwitchConflictsWithDecision(event AlpenglowParentSwitch) (bool, string) {
	if bs.alpenglowDecisionSource == nil {
		return false, ""
	}
	for slot := event.SwitchSlot; slot <= event.ChildSlot; slot++ {
		decision, ok := bs.alpenglowDecisionSource(slot - 1)
		if !ok || decision.Slot != slot {
			continue
		}
		switch decision.Kind {
		case alpenglow.ChainDecisionKindConflict:
			return true, fmt.Sprintf("consensus conflict at slot %d", slot)
		case alpenglow.ChainDecisionKindSkip:
			if slot == event.ChildSlot {
				return true, fmt.Sprintf("slot %d is certified-skipped", slot)
			}
		case alpenglow.ChainDecisionKindBlock:
			if slot < event.ChildSlot {
				return true, fmt.Sprintf("direct link omits decisive block %s at slot %d", decision.Block.Hash, slot)
			}
			if decision.Block.Hash != event.ChildID {
				return true, fmt.Sprintf("decisive child at slot %d is %s", slot, decision.Block.Hash)
			}
		}
		if slot == ^uint64(0) {
			break
		}
	}
	return false, ""
}

// queueBufferedAlpenglowParentSwitchLocked finds an alternate child even when
// replay is currently waiting on an earlier absent slot. This is the common
// fork shape: wrong suffix A has already been emitted (and may have executed),
// canonical child B jumps back to A's parent, and the slots before B do not
// exist on B's branch. If we inspect only reorderBuffer[nextSlotToSend], B can
// sit buffered forever behind those absent slots and replay never receives the
// branch-selection signal.
func (bs *BlockSource) queueBufferedAlpenglowParentSwitchLocked() bool {
	if bs.pendingAlpenglowParentSwitch != nil {
		return true
	}
	var earliest *b.Block
	for _, candidate := range bs.reorderBuffer {
		if candidate == nil || candidate.Slot < bs.nextSlotToSend {
			continue
		}
		if earliest != nil && candidate.Slot >= earliest.Slot {
			continue
		}
		// Test eligibility without publishing a later event merely because Go
		// map iteration happened to encounter it first.
		if bs.sourceType != BlockSourceTurbine || !bs.turbineAlpenglowBlockIDHints ||
			!candidate.FromLiveStream || !candidate.HasAlpenglowBlockID || !candidate.HasAlpenglowParentBlockID ||
			candidate.SourceParentSlot == 0 || candidate.SourceParentSlot >= bs.lastEmittedBlockSlot ||
			candidate.SourceParentSlot >= candidate.Slot {
			continue
		}
		if bs.isRejectedAlpenglowBlock(candidate) {
			continue
		}
		parentID, ok := bs.emittedAlpenglowBlockIDs[candidate.SourceParentSlot]
		if !ok || parentID != solana.Hash(candidate.AlpenglowParentBlockID) {
			continue
		}
		childID := solana.Hash(candidate.AlpenglowBlockID)
		if emittedID, emitted := bs.emittedAlpenglowBlockIDs[candidate.Slot]; emitted && emittedID == childID {
			continue
		}
		earliest = candidate
	}
	return earliest != nil && bs.queueAlpenglowParentSwitchLocked(earliest)
}

func (bs *BlockSource) repairLiveStreamGap(waitingSlot, firstBufferedSlot, firstBufferedParentSlot uint64, bufferedCount int) {
	if !bs.usesLiveShredStream() {
		return
	}
	if !bs.liveStreamActive.Load() {
		bs.forceRPCForLiveGap(waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount)
		return
	}

	gapSinceUnix := bs.liveGapSinceUnix.Load()
	gapAge := time.Duration(0)
	if gapSinceUnix != 0 {
		gapAge = time.Since(time.Unix(0, gapSinceUnix))
	}

	streamName := bs.liveShredStreamName()
	waitReason := fmt.Sprintf("first buffered %s block %d still depends on parent slot %d", streamName, firstBufferedSlot, firstBufferedParentSlot)
	switch {
	case firstBufferedParentSlot == waitingSlot:
		waitReason = fmt.Sprintf("waiting on live %s slot %d; later buffered block %d still depends on it", streamName, waitingSlot, firstBufferedSlot)
	case firstBufferedParentSlot > waitingSlot:
		waitReason = fmt.Sprintf("waiting on missing %s ancestry range %d-%d; later buffered block %d still depends on slot %d",
			streamName, waitingSlot, firstBufferedParentSlot, firstBufferedSlot, firstBufferedParentSlot)
	case firstBufferedParentSlot == bs.lastEmittedBlockSlot:
		waitReason = fmt.Sprintf("later buffered block %d points back to anchor %d, but that only proves an observed alternate branch and is not treated as a canonical skipped run", firstBufferedSlot, bs.lastEmittedBlockSlot)
	}

	now := time.Now()
	lastLog := time.Unix(0, bs.liveGapLastLogUnix.Load())
	if lastLog.IsZero() || now.Sub(lastLog) >= reorderGapWarnInterval {
		bs.liveGapLastLogUnix.Store(now.UnixNano())
		// Expected catchup rhythm, not an operator problem: repair owns the
		// hole and the per-slot lines show progress. The full picture —
		// including the repair/hydration counters that explain WHY the head
		// is waiting — goes to catchup.log in the run directory.
		mlog.NamedFilef("catchup", "waiting for missing %s slot %d (keeping %s active) | first_buffered=%d | first_parent_slot=%d | buffered_live_stream=%d | reason=%s | mode=%s%s",
			streamName, waitingSlot, streamName, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, waitReason, bs.currentModeString(), bs.catchupDiagSuffix())
	}

	// Native turbine is NOT a connection: a missing live slot is repair's
	// job, so pin repair on the hole and return — no reconnect reasoning at
	// all. "Reconnecting" would tear down our LOCAL receiver/repair/gossip
	// state (minutes of warmup) and re-stream nothing. Observed live: one
	// missing slot ended the first shred-native replay 15s in. The idle
	// watchdog still restarts a genuinely dead socket.
	if bs.sourceType == BlockSourceTurbine {
		bs.prioritizeTurbineRepairForLiveGap(waitingSlot, firstBufferedParentSlot)
		return
	}

	shouldReconnect := false
	reconnectReason := ""
	switch {
	case firstBufferedParentSlot != waitingSlot:
		if firstBufferedParentSlot <= waitingSlot {
			// A reconnect only helps when later buffered live-stream blocks still
			// depend on an unseen ancestor beyond the current anchor.
			break
		}
		switch {
		case bufferedCount >= reorderGapWarnThreshold && gapAge >= lightbringerDeepGapReconnect:
			shouldReconnect = true
			reconnectReason = fmt.Sprintf("waiting %s for %s ancestry range %d-%d while later buffered block %d still depends on slot %d",
				gapAge.Round(time.Second), streamName, waitingSlot, firstBufferedParentSlot, firstBufferedSlot, firstBufferedParentSlot)
		case gapAge >= lightbringerGapReconnectAfter:
			shouldReconnect = true
			reconnectReason = fmt.Sprintf("waiting %s for %s ancestry range %d-%d while later buffered block %d still depends on slot %d",
				gapAge.Round(time.Second), streamName, waitingSlot, firstBufferedParentSlot, firstBufferedSlot, firstBufferedParentSlot)
		}
	case bufferedCount >= reorderGapWarnThreshold && gapAge >= lightbringerDeepGapReconnect:
		shouldReconnect = true
		reconnectReason = fmt.Sprintf("waiting %s for live %s slot %d while %d later buffered blocks still depend on it",
			gapAge.Round(time.Second), streamName, waitingSlot, bufferedCount)
	case gapAge >= lightbringerGapReconnectAfter:
		shouldReconnect = true
		reconnectReason = fmt.Sprintf("waiting %s for live %s slot %d while later buffered blocks still depend on it",
			gapAge.Round(time.Second), streamName, waitingSlot)
	}

	if shouldReconnect && bs.lightbringerGapReconnectSlot.CompareAndSwap(0, waitingSlot) {
		bs.requestLiveStreamReconnect(reconnectReason)
	}
}

func (bs *BlockSource) forceRPCForLiveGap(waitingSlot, firstBufferedSlot, firstBufferedParentSlot uint64, bufferedCount int) {
	if !bs.usesLiveShredStream() {
		return
	}
	// Shreds-only mode: there is no RPC resume, so setting the force flags
	// would wedge the decode gates waiting for blocks that never come. Push
	// turbine repair at the missing range instead and leave the live path
	// fully open.
	if !bs.rpcFallbackEnabled {
		bs.prioritizeTurbineRepairForLiveGap(waitingSlot, firstBufferedParentSlot)
		now := time.Now()
		if last := time.Unix(bs.noRPCFallbackLogUnix.Load(), 0); now.Sub(last) >= 30*time.Second {
			bs.noRPCFallbackLogUnix.Store(now.Unix())
			mlog.Log.Warnf("live stream gap at slot %d: RPC block fetch is disabled (block.rpc_fallback=false); pushing turbine repair instead | first_buffered=%d | buffered_live_stream=%d",
				waitingSlot, firstBufferedSlot, bufferedCount)
		}
		return
	}

	recoveryUntil := waitingSlot + liveRecoverySlots
	if recoveryUntil < waitingSlot {
		recoveryUntil = ^uint64(0)
	}

	bs.liveForceRPCUntil.Store(waitingSlot)
	bs.liveCooldownUntil.Store(recoveryUntil)
	oldHandoff := bs.liveHandoffSlot.Swap(0)
	wasActive := bs.liveStreamActive.Swap(false)
	resultGeneration := bs.invalidateLiveStreamResults()
	bs.liveNeedRPCResume.Store(true)
	bs.clearLiveGapWatch()
	bs.resetLiveRepairSlot()
	clearedPrefetched := bs.clearBufferedLiveStreamBlocks()

	bs.reorderMu.Lock()
	removedSlots := make([]uint64, 0)
	for slot, blk := range bs.reorderBuffer {
		if blk != nil && blk.FromLiveStream && slot >= waitingSlot {
			delete(bs.reorderBuffer, slot)
			removedSlots = append(removedSlots, slot)
		}
	}
	bs.reorderMu.Unlock()

	if len(removedSlots) > 0 {
		bs.slotStateMu.Lock()
		for _, slot := range removedSlots {
			delete(bs.slotState, slot)
			delete(bs.inflightStart, slot)
		}
		bs.slotStateMu.Unlock()
	}

	if wasActive || oldHandoff != 0 {
		mlog.Log.Warnf("BLOCK SOURCE SWITCH: %s -> RPC at slot %d | reason=missing_streamed_slot | first_buffered=%d | first_parent_slot=%d | buffered_live_stream=%d | cleared_prefetched_live_stream=%d | rpc_forced_until=%d | rpc_cooldown_until=%d | live_generation=%d | mode=%s",
			bs.liveShredStreamName(), waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, clearedPrefetched, waitingSlot, recoveryUntil, resultGeneration, bs.currentModeString())
		return
	}

	mlog.Log.Warnf("BLOCK SOURCE STATUS: forcing RPC because %s skipped waiting slot %d | first_buffered=%d | first_parent_slot=%d | buffered_live_stream=%d | cleared_prefetched_live_stream=%d | rpc_forced_until=%d | rpc_cooldown_until=%d | live_generation=%d | mode=%s",
		bs.liveShredStreamName(), waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, clearedPrefetched, waitingSlot, recoveryUntil, resultGeneration, bs.currentModeString())
}

func (bs *BlockSource) handleDetectedLiveGap(waitingSlot, firstBufferedSlot, firstBufferedParentSlot uint64, bufferedCount int) {
	if bs.liveStreamActive.Load() {
		bs.repairLiveStreamGap(waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount)
		return
	}
	bs.forceRPCForLiveGap(waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount)
}

func (bs *BlockSource) forceRPCForLiveParentMismatch(waitingSlot, observedParentSlot, expectedParentSlot uint64) {
	if !bs.usesLiveShredStream() {
		return
	}

	recoveryUntil := waitingSlot + liveRecoverySlots
	if recoveryUntil < waitingSlot {
		recoveryUntil = ^uint64(0)
	}

	bs.liveForceRPCUntil.Store(waitingSlot)
	bs.liveCooldownUntil.Store(recoveryUntil)
	oldHandoff := bs.liveHandoffSlot.Swap(0)
	wasActive := bs.liveStreamActive.Swap(false)
	resultGeneration := bs.invalidateLiveStreamResults()
	bs.liveNeedRPCResume.Store(true)
	bs.clearLiveGapWatch()
	bs.resetLiveRepairSlot()
	clearedPrefetched := bs.clearBufferedLiveStreamBlocks()

	bs.reorderMu.Lock()
	removedSlots := make([]uint64, 0)
	for slot, blk := range bs.reorderBuffer {
		if blk != nil && blk.FromLiveStream && slot >= waitingSlot {
			delete(bs.reorderBuffer, slot)
			removedSlots = append(removedSlots, slot)
		}
	}
	bs.reorderMu.Unlock()

	if len(removedSlots) > 0 {
		bs.slotStateMu.Lock()
		for _, slot := range removedSlots {
			delete(bs.slotState, slot)
			delete(bs.inflightStart, slot)
		}
		bs.slotStateMu.Unlock()
	}

	reason := "parent_slot_mismatch"
	if observedParentSlot == 0 {
		reason = "missing_parent_slot"
	}

	if wasActive || oldHandoff != 0 {
		mlog.Log.Warnf("BLOCK SOURCE SWITCH: %s -> RPC at slot %d | reason=%s | observed_parent_slot=%d | expected_parent_slot=%d | cleared_prefetched_live_stream=%d | rpc_forced_until=%d | rpc_cooldown_until=%d | live_generation=%d | mode=%s",
			bs.liveShredStreamName(), waitingSlot, reason, observedParentSlot, expectedParentSlot, clearedPrefetched, waitingSlot, recoveryUntil, resultGeneration, bs.currentModeString())
		return
	}

	mlog.Log.Warnf("BLOCK SOURCE STATUS: rejecting %s handoff at slot %d | reason=%s | observed_parent_slot=%d | expected_parent_slot=%d | cleared_prefetched_live_stream=%d | rpc_forced_until=%d | rpc_cooldown_until=%d | live_generation=%d | mode=%s",
		bs.liveShredStreamName(), waitingSlot, reason, observedParentSlot, expectedParentSlot, clearedPrefetched, waitingSlot, recoveryUntil, resultGeneration, bs.currentModeString())
}

func (bs *BlockSource) logLiveStreamModeState(mode string, gap uint64) {
	if !bs.usesLiveShredStream() {
		return
	}

	handoffSlot := bs.liveHandoffSlot.Load()
	active := bs.liveStreamActive.Load()
	cooldownUntil := bs.liveCooldownUntil.Load()
	connected := bs.liveStreamConnected.Load()
	lastSlot := bs.liveLastStreamSlot.Load()
	started := bs.liveStreamStarted.Load()

	bs.reorderMu.Lock()
	waitingSlot := bs.nextSlotToSend
	bs.reorderMu.Unlock()

	switch mode {
	case "near-tip":
		if active {
			mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained and blocks are already arriving from %s | waiting_slot=%d | handoff_slot=%d | gap=%d",
				bs.liveShredStreamName(), waitingSlot, handoffSlot, gap)
			return
		}
		if cooldownUntil != 0 {
			if connected && lastSlot != 0 {
				mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; staying on RPC until slot %d after a %s gap (latest streamed slot %d) | waiting_slot=%d | gap=%d",
					cooldownUntil, bs.liveShredStreamName(), lastSlot, waitingSlot, gap)
				return
			}
			mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; staying on RPC until slot %d after a %s gap | waiting_slot=%d | gap=%d",
				cooldownUntil, bs.liveShredStreamName(), waitingSlot, gap)
			return
		}
		if handoffSlot != 0 {
			mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; waiting to switch block receipt from RPC to %s at handoff slot %d | waiting_slot=%d | gap=%d",
				bs.liveShredStreamName(), handoffSlot, waitingSlot, gap)
			return
		}
		if connected {
			if lastSlot != 0 {
				mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; %s stream is connected (latest streamed slot %d) and waiting to arm handoff | waiting_slot=%d | gap=%d",
					bs.liveShredStreamName(), lastSlot, waitingSlot, gap)
				return
			}
			mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; %s stream is connected and waiting for its first streamed slot | waiting_slot=%d | gap=%d",
				bs.liveShredStreamName(), waitingSlot, gap)
			return
		}
		if started {
			mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; waiting for %s stream connection before handoff | waiting_slot=%d | gap=%d",
				bs.liveShredStreamName(), waitingSlot, gap)
			return
		}
		mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; preparing to switch block receipt from RPC to %s | waiting_slot=%d | gap=%d",
			bs.liveShredStreamName(), waitingSlot, gap)
	}
}

func (bs *BlockSource) maybeLogReorderGapLocked() {
	waitingSlot := bs.nextSlotToSend
	if len(bs.reorderBuffer) < reorderGapWarnThreshold {
		return
	}
	if bs.reorderBuffer[waitingSlot] != nil || bs.skippedSlots[waitingSlot] {
		return
	}

	now := time.Now()
	lastLogUnix := bs.lastReorderGapLog.Load()
	interval := reorderGapWarnInterval
	if bs.lastReorderGapSlot.Load() == waitingSlot {
		// Same head-of-line slot as the previous warn: the situation is known
		// (and the rescue may already be pulling it) — back off 3x instead of
		// repainting the same message every few seconds.
		interval *= 3
	}
	if lastLogUnix != 0 && now.Sub(time.Unix(lastLogUnix, 0)) < interval {
		return
	}
	bs.lastReorderGapLog.Store(now.Unix())
	bs.lastReorderGapSlot.Store(waitingSlot)

	var minSlot uint64
	var maxSlot uint64
	var liveStreamCount int
	first := true
	for slot, blk := range bs.reorderBuffer {
		if first || slot < minSlot {
			minSlot = slot
		}
		if first || slot > maxSlot {
			maxSlot = slot
		}
		if blk != nil && blk.FromLiveStream {
			liveStreamCount++
		}
		first = false
	}
	if first {
		return
	}

	waitingState := "missing"
	bs.slotStateMu.Lock()
	if state, exists := bs.slotState[waitingSlot]; exists {
		switch state {
		case slotInflight:
			waitingState = "inflight"
		case slotDone:
			waitingState = "done"
		case slotPending:
			waitingState = "pending"
		}
	}
	bs.slotStateMu.Unlock()

	var gapToFirst uint64
	if minSlot > waitingSlot {
		gapToFirst = minSlot - waitingSlot
	}

	firstLightbringerSlot, firstLightbringerParentSlot, _, firstConnectedSlot, firstConnectedParentSlot, foundConnected := bs.inspectLaterLiveBlocksLocked(waitingSlot)

	firstLightbringerDesc := "none"
	if firstLightbringerSlot != 0 {
		firstLightbringerDesc = fmt.Sprintf("%d(parent=%d)", firstLightbringerSlot, firstLightbringerParentSlot)
	}

	firstConnectedDesc := "none"
	if foundConnected {
		firstConnectedDesc = fmt.Sprintf("%d(parent=%d gap_span=%d)", firstConnectedSlot, firstConnectedParentSlot, firstConnectedSlot-waitingSlot)
	}

	mlog.NamedFilef("catchup", "reorder buffer: waiting on missing slot %d | waiting_state=%s | buffered=%d slots (%d from live stream) | buffered_range=%d-%d | gap_to_first_buffered=%d (buffered blocks are the live edge, not the missing range — repair owns the hole) | first_live=%s | first_connected_to_anchor=%s | mode=%s%s",
		waitingSlot, waitingState, len(bs.reorderBuffer), liveStreamCount, minSlot, maxSlot, gapToFirst, firstLightbringerDesc, firstConnectedDesc, bs.currentModeString(), bs.catchupDiagSuffix())
}

func (bs *BlockSource) detectLiveGapLocked() (waitingSlot uint64, firstBufferedSlot uint64, firstBufferedParentSlot uint64, bufferedCount int, shouldFallback bool) {
	if !bs.usesLiveShredStream() {
		return 0, 0, 0, 0, false
	}
	if bs.liveForceRPCUntil.Load() != 0 {
		bs.clearLiveGapWatch()
		return 0, 0, 0, 0, false
	}
	handoffSlot := bs.liveHandoffSlot.Load()
	liveStreamActive := bs.liveStreamActive.Load()
	if handoffSlot == 0 && !liveStreamActive {
		bs.clearLiveGapWatch()
		return 0, 0, 0, 0, false
	}
	waitingSlot = bs.nextSlotToSend
	if !liveStreamActive && handoffSlot != 0 && waitingSlot < handoffSlot {
		// RPC still owns slots before the pending handoff boundary. Buffered
		// Lightbringer blocks beyond that boundary are expected and must not be
		// treated as evidence that the current RPC-owned waiting slot is missing.
		bs.clearLiveGapWatch()
		return 0, 0, 0, 0, false
	}
	if bs.reorderBuffer[waitingSlot] != nil || bs.skippedSlots[waitingSlot] {
		bs.clearLiveGapWatch()
		return 0, 0, 0, 0, false
	}
	if liveStreamActive && bs.isLiveRepairSlot(waitingSlot) {
		bs.clearLiveGapWatch()
		return 0, 0, 0, 0, false
	}

	first := true
	for slot, blk := range bs.reorderBuffer {
		if blk == nil || !blk.FromLiveStream || slot <= waitingSlot {
			continue
		}
		bufferedCount++
		if first || slot < firstBufferedSlot {
			firstBufferedSlot = slot
			firstBufferedParentSlot = blk.SourceParentSlot
			first = false
		}
	}

	if bufferedCount == 0 || first {
		bs.clearLiveGapWatch()
		return 0, 0, 0, 0, false
	}

	now := time.Now()
	gapSlot := bs.lightbringerGapSlot.Load()
	gapSinceUnix := bs.liveGapSinceUnix.Load()
	if gapSlot != waitingSlot || gapSinceUnix == 0 {
		bs.lightbringerGapSlot.Store(waitingSlot)
		bs.liveGapSinceUnix.Store(now.UnixNano())
		bs.liveGapLastLogUnix.Store(0)
		bs.lightbringerGapReconnectSlot.Store(0)
		return waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, false
	}

	if bufferedCount >= liveGapBufferDepth {
		return waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, true
	}
	if now.Sub(time.Unix(0, gapSinceUnix)) < liveGapFallbackWait {
		return waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, false
	}

	return waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, true
}
