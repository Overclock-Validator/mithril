package blockstream

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

const (
	// maxFrontierRepairAhead caps turbine repair to a window starting at the
	// replay waiting slot instead of the live tip.
	maxFrontierRepairAhead = uint64(64)
)

func frontierRepairEndSlot(waitingSlot, gapParentSlot uint64) uint64 {
	if waitingSlot == 0 {
		return 0
	}
	end := waitingSlot + maxFrontierRepairAhead
	if gapParentSlot > waitingSlot && gapParentSlot < end {
		end = gapParentSlot
	}
	return end
}

func (bs *BlockSource) SetKnownAlpenglowBlockID(slot uint64, blockID solana.Hash) {
	if !bs.turbineAlpenglowBlockIDHints || slot == 0 || blockID == (solana.Hash{}) {
		return
	}

	bs.alpenglowMu.Lock()
	if existing, ok := bs.knownAlpenglowBlockIDs[slot]; !ok {
		bs.knownAlpenglowBlockIDOrder = append(bs.knownAlpenglowBlockIDOrder, slot)
	} else if existing == blockID {
		receiver := bs.activeTurbineReceiver
		bs.alpenglowMu.Unlock()
		if receiver != nil {
			receiver.SetKnownAlpenglowBlockID(slot, blockID)
		}
		return
	}
	bs.knownAlpenglowBlockIDs[slot] = blockID
	for len(bs.knownAlpenglowBlockIDOrder) > maxKnownAlpenglowBlockIDs {
		old := bs.knownAlpenglowBlockIDOrder[0]
		bs.knownAlpenglowBlockIDOrder = bs.knownAlpenglowBlockIDOrder[1:]
		delete(bs.knownAlpenglowBlockIDs, old)
	}
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()

	if receiver != nil {
		receiver.SetKnownAlpenglowBlockID(slot, blockID)
	}
}

func (bs *BlockSource) resetTurbineSlotForAlpenglowBlock(slot uint64, blockID solana.Hash) {
	if !bs.turbineAlpenglowBlockIDHints || slot == 0 || blockID == (solana.Hash{}) {
		return
	}
	bs.SetKnownAlpenglowBlockID(slot, blockID)

	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver != nil {
		receiver.ResetSlot(slot)
		receiver.PrioritizeRepairSlot(slot)
	}
}

func (bs *BlockSource) prioritizeTurbineRepairRange(start, end uint64) {
	if bs.sourceType != BlockSourceTurbine || start == 0 {
		return
	}
	if !bs.turbineRepairOnly && !bs.turbineAlpenglowBlockIDHints {
		return
	}
	if end < start {
		end = start
	}

	bs.seedTurbineRepairObservedSlot(bs.repairObservedSeedSlot())

	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver != nil {
		// start is the replay frontier (waiting slot): pin it as the repair
		// floor so the assembler never evicts it while the tip races ahead.
		receiver.SetRepairFloor(start)
		receiver.PrioritizeRepairRange(start, end)
	}
}

func (bs *BlockSource) PrioritizeTurbineRepairSlot(slot uint64) {
	if bs.sourceType != BlockSourceTurbine || slot == 0 {
		return
	}
	if !bs.turbineRepairOnly && !bs.turbineAlpenglowBlockIDHints {
		return
	}
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver != nil {
		receiver.PrioritizeRepairSlot(slot)
	}
}

func (bs *BlockSource) prioritizeTurbineRepairForLiveGap(waitingSlot, firstBufferedParentSlot uint64) {
	end := frontierRepairEndSlot(waitingSlot, firstBufferedParentSlot)
	bs.prioritizeTurbineRepairRange(waitingSlot, end)
}

func (bs *BlockSource) registerActiveTurbineReceiver(receiver *turbine.UDPReceiver) {
	bs.alpenglowMu.Lock()
	bs.activeTurbineReceiver = receiver
	bs.alpenglowMu.Unlock()
	bs.seedTurbineRepairObservedSlot(bs.repairObservedSeedSlot())

	if receiver == nil || !bs.turbineRepairOnlyMode() {
		return
	}
	bs.reorderMu.Lock()
	waitingSlot := bs.waitingSlotLocked()
	bs.reorderMu.Unlock()
	if waitingSlot == 0 {
		return
	}
	end := frontierRepairEndSlot(waitingSlot, 0)
	receiver.SetRepairFloor(waitingSlot)
	receiver.PrioritizeRepairRange(waitingSlot, end)
}

func (bs *BlockSource) repairObservedSeedSlot() uint64 {
	const repairObservedSlotLag = uint64(1)

	bs.reorderMu.Lock()
	waitingSlot := bs.waitingSlotLocked()
	bs.reorderMu.Unlock()

	seed := waitingSlot + repairObservedSlotLag
	if frontierEnd := frontierRepairEndSlot(waitingSlot, 0); frontierEnd != 0 && seed > frontierEnd {
		seed = frontierEnd
	}
	if bs.startSlot > seed {
		seed = bs.startSlot
	}
	if lastExecuted := bs.lastExecutedSlot.Load(); lastExecuted != 0 {
		if candidate := lastExecuted + repairObservedSlotLag; candidate > seed {
			seed = candidate
		}
	} else if bs.startSlot > 0 && bs.startSlot > seed {
		seed = bs.startSlot
	}
	if frontierEnd := frontierRepairEndSlot(waitingSlot, 0); frontierEnd != 0 && seed > frontierEnd {
		seed = frontierEnd
	}
	return seed
}

func (bs *BlockSource) seedTurbineRepairObservedSlot(slot uint64) {
	if slot == 0 {
		return
	}
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver != nil {
		receiver.SeedRepairObservedSlot(slot)
	}
}

func (bs *BlockSource) attachAlpenglowBlockIDHintsToReceiver(receiver *turbine.UDPReceiver) {
	bs.registerActiveTurbineReceiver(receiver)
	if !bs.turbineAlpenglowBlockIDHints || receiver == nil {
		return
	}

	bs.alpenglowMu.Lock()
	known := make([]struct {
		slot    uint64
		blockID solana.Hash
	}, 0, len(bs.knownAlpenglowBlockIDs))
	for slot, blockID := range bs.knownAlpenglowBlockIDs {
		known = append(known, struct {
			slot    uint64
			blockID solana.Hash
		}{slot: slot, blockID: blockID})
	}
	bs.alpenglowMu.Unlock()

	for _, entry := range known {
		receiver.SetKnownAlpenglowBlockID(entry.slot, entry.blockID)
	}
}

func (bs *BlockSource) observeAlpenglowCandidateBlock(blk *b.Block) {
	if bs.alpenglowCandidateBlockSink == nil || !bs.turbineAlpenglowBlockIDHints {
		return
	}
	if blk == nil || !blk.HasAlpenglowBlockID {
		return
	}
	parentSlot := blk.SourceParentSlot
	if parentSlot == 0 {
		parentSlot = blk.ParentSlot
	}
	// ParentHash is the parent's Alpenglow block id (zero when unknown) so the
	// chain tracker can link observed blocks into finalized ancestor chains.
	var parentBlockID solana.Hash
	if blk.HasAlpenglowParentBlockID {
		parentBlockID = solana.Hash(blk.AlpenglowParentBlockID)
	}
	bs.alpenglowCandidateBlockSink(alpenglow.ReplayBlockObservation{
		Block: alpenglow.BlockID{
			Slot: blk.Slot,
			Hash: solana.Hash(blk.AlpenglowBlockID),
		},
		ParentSlot: parentSlot,
		ParentHash: parentBlockID,
		Source:     bs.liveShredStreamName(),
		At:         time.Now(),
	})
}

func (bs *BlockSource) applyAlpenglowDecisionLocked() bool {
	if bs.alpenglowDecisionSource == nil || bs.sourceType != BlockSourceTurbine || !bs.turbineAlpenglowBlockIDHints {
		return false
	}
	waitingSlot := bs.nextSlotToSend
	if waitingSlot == 0 {
		return false
	}
	decision, ok := bs.alpenglowDecisionSource(waitingSlot - 1)
	if !ok || decision.Slot != waitingSlot {
		return false
	}

	switch decision.Kind {
	case alpenglow.ChainDecisionKindSkip:
		if !bs.skippedSlots[waitingSlot] {
			delete(bs.reorderBuffer, waitingSlot)
			bs.skippedSlots[waitingSlot] = true
			bs.alpenglowCertifiedSkips[waitingSlot] = true
			delete(bs.lightbringerSynthesizedSkips, waitingSlot)
			bs.slotStateMu.Lock()
			bs.slotState[waitingSlot] = slotDone
			delete(bs.inflightStart, waitingSlot)
			bs.slotStateMu.Unlock()
			bs.clearSlotErrors(waitingSlot)
			bs.stats.FetchSkipped.Add(1)
			mlog.Log.FileOnlyf("ALPENGLOW consensus decision: slot %d is skipped (%s)", waitingSlot, decision.Reason)
		}
		return false
	case alpenglow.ChainDecisionKindBlock:
		if bs.skippedSlots[waitingSlot] {
			delete(bs.skippedSlots, waitingSlot)
			delete(bs.lightbringerSynthesizedSkips, waitingSlot)
			delete(bs.alpenglowCertifiedSkips, waitingSlot)
		}
		blk := bs.reorderBuffer[waitingSlot]
		if blk == nil || !blk.HasAlpenglowBlockID {
			return false
		}
		if solana.Hash(blk.AlpenglowBlockID) == decision.Block.Hash {
			return false
		}
		delete(bs.reorderBuffer, waitingSlot)
		bs.slotStateMu.Lock()
		delete(bs.slotState, waitingSlot)
		delete(bs.inflightStart, waitingSlot)
		bs.slotStateMu.Unlock()
		bs.clearSlotErrors(waitingSlot)
		bs.resetTurbineSlotForAlpenglowBlock(waitingSlot, decision.Block.Hash)
		mlog.Log.Warnf("ALPENGLOW consensus decision: discarded non-canonical turbine block for slot %d (got=%s want=%s)",
			waitingSlot, solana.Hash(blk.AlpenglowBlockID), decision.Block.Hash)
		return true
	case alpenglow.ChainDecisionKindConflict:
		mlog.Log.FileOnlyf("ALPENGLOW consensus decision: conflict at slot %d (%s)", waitingSlot, decision.Reason)
		return false
	default:
		return false
	}
}
