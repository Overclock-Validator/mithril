package blockprod

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/replay"
)

// pocLeaderSlotReplayReady reports whether replay has advanced far enough to start
// forging leaderSlot.
//
// POC block-production policy: blockprod must stay aligned with replay. We only
// attempt a leader slot once its immediate parent (leaderSlot-1) is available as
// chain tip input. When that parent was led by another validator, replay must have
// executed slot leaderSlot-1 before we try. When the parent was our own leader
// slot, we may proceed once that slot is finished locally (consecutive leader
// windows) even if replay is still catching up via local-leader skip synthesis.
func (l *LeaderLoop) pocLeaderSlotReplayReady(leaderSlot uint64) (bool, error) {
	if leaderSlot == 0 {
		return true, nil
	}
	parent, err := l.resolveProductionParent(leaderSlot)
	if err != nil {
		return false, err
	}
	parentSlot := parent.Slot
	if l.epochSchedule != nil && l.epochSchedule.GetEpoch(parentSlot) != l.epochSchedule.GetEpoch(leaderSlot) {
		return false, fmt.Errorf("%w: local production for slot %d crosses an epoch boundary from parent %d; replay epoch-transition preparation is required before forging",
			errParentNotReady, leaderSlot, parentSlot)
	}
	replaySlot := global.Slot()
	if replaySlot != parentSlot && !l.isLeaderSlotFinished(parentSlot) {
		return false, fmt.Errorf("%w: replay head %d does not match Alpenglow parent %d for leader slot %d",
			errParentNotReady, replaySlot, parentSlot, leaderSlot)
	}
	return l.parentAlpenglowInputsReady(parent)
}

func (l *LeaderLoop) resolveProductionParent(leaderSlot uint64) (alpenglow.BlockID, error) {
	if leaderSlot == 0 {
		return alpenglow.BlockID{}, nil
	}
	if leaderSlot%alpenglow.LeaderWindowSlots == 0 && l.productionParent != nil {
		parent := l.productionParent(leaderSlot)
		switch parent.Kind {
		case alpenglow.BlockProductionParentReady:
			if parent.Parent.IsZero() || !parent.Parent.HasHash() || parent.Parent.Slot >= leaderSlot {
				return alpenglow.BlockID{}, fmt.Errorf("%w: invalid ParentReady value for leader slot %d", errParentNotReady, leaderSlot)
			}
			return parent.Parent, nil
		case alpenglow.BlockProductionParentMissedWindow:
			return alpenglow.BlockID{}, fmt.Errorf("%w: ParentReady arrived after leader window %d", errParentNotReady, leaderSlot)
		default:
			return alpenglow.BlockID{}, fmt.Errorf("%w: no verified ParentReady for leader window %d", errParentNotReady, leaderSlot)
		}
	}

	parentSlot := leaderSlot - 1
	parentHash, _, ok := replay.ResolveActiveAlpenglowIdentity(parentSlot)
	if !ok && l.parentBlockID != nil {
		parentHash, ok = l.parentBlockID(leaderSlot)
	}
	if !ok {
		return alpenglow.BlockID{}, fmt.Errorf("%w: alpenglow block id missing for parent slot %d",
			errParentNotReady, parentSlot)
	}
	return alpenglow.BlockID{Slot: parentSlot, Hash: parentHash}, nil
}

func (l *LeaderLoop) parentAlpenglowInputsReady(parent alpenglow.BlockID) (bool, error) {
	blockID, _, ok := replay.ResolveActiveAlpenglowIdentity(parent.Slot)
	if !ok {
		return false, fmt.Errorf("%w: executed Alpenglow identity missing for parent slot %d",
			errParentNotReady, parent.Slot)
	}
	if blockID != parent.Hash {
		return false, fmt.Errorf("%w: executed block %s does not match selected parent %s at slot %d",
			errParentNotReady, blockID, parent.Hash, parent.Slot)
	}
	return true, nil
}
