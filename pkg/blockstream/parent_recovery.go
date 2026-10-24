package blockstream

import (
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

// AlpenglowParentSwitchRecovery retains one unexecuted, immutable assembly
// across an in-process rooted retry. Slot numbers alone must never authorize
// skips: the child and the replayed parent must still match both exact IDs.
type AlpenglowParentSwitchRecovery struct {
	Selection AlpenglowParentSwitch
	Child     *b.Block
}

func (r *AlpenglowParentSwitchRecovery) valid() bool {
	e, child := r.Selection, r.Child
	return e.SwitchSlot != 0 && e.ParentSlot+1 == e.SwitchSlot &&
		e.ChildSlot >= e.SwitchSlot && e.ChildSlot-e.SwitchSlot <= alpenglowMaxParentLinkedSkipRun &&
		e.ParentID != (solana.Hash{}) && e.ChildID != (solana.Hash{}) &&
		child != nil && child.FromLiveStream && child.Slot == e.ChildSlot &&
		child.SourceParentSlot == e.ParentSlot && child.HasAlpenglowBlockID &&
		child.HasAlpenglowParentBlockID && solana.Hash(child.AlpenglowBlockID) == e.ChildID &&
		solana.Hash(child.AlpenglowParentBlockID) == e.ParentID
}

// ParentSwitchRecovery snapshots only the source's pending selection. The
// buffered child has not been emitted or executed; its assembly data is
// immutable and may outlive the abandoned source like a prewarmed block.
func (bs *BlockSource) ParentSwitchRecovery(event AlpenglowParentSwitch) *AlpenglowParentSwitchRecovery {
	bs.reorderMu.Lock()
	defer bs.reorderMu.Unlock()
	if bs.pendingAlpenglowParentSwitch == nil || *bs.pendingAlpenglowParentSwitch != event {
		return nil
	}
	r := &AlpenglowParentSwitchRecovery{Selection: event, Child: bs.reorderBuffer[event.ChildSlot]}
	if !r.valid() {
		return nil
	}
	return r
}

// restoreAlpenglowParentSelectionLocked runs before ordinary emission. Delay
// until the retry has replayed the parent; abandon the hint if that parent or
// a decisive certificate now selects another branch. Never promote the hint
// to a known/certified block identity.
func (bs *BlockSource) restoreAlpenglowParentSelectionLocked() {
	r := bs.parentSwitchRecovery
	if r == nil || bs.nextSlotToSend < r.Selection.SwitchSlot {
		return
	}
	bs.parentSwitchRecovery = nil
	e := r.Selection
	if bs.nextSlotToSend != e.SwitchSlot || bs.lastEmittedBlockSlot != e.ParentSlot ||
		!bs.hasLastEmittedAlpenglowBlockID || bs.lastEmittedAlpenglowBlockID != e.ParentID ||
		bs.isRejectedAlpenglowBlock(r.Child) {
		return
	}
	if conflicts, _ := bs.alpenglowParentSwitchConflictsWithDecision(e); conflicts {
		return
	}
	// As in RewindForAlpenglowParentSwitch, discard the selected skip range,
	// but leave its classification provisional so certificates can override it.
	bs.rejectAlpenglowSkipRange(e.SwitchSlot, e.ChildSlot)
	for slot := e.SwitchSlot; slot <= e.ChildSlot; slot++ {
		delete(bs.reorderBuffer, slot)
		delete(bs.skippedSlots, slot)
		delete(bs.liveSynthesizedSkips, slot)
		delete(bs.alpenglowCertifiedSkips, slot)
		if slot == e.ChildSlot {
			break
		}
	}
	bs.reorderBuffer[e.ChildSlot] = r.Child
	mlog.Log.Warnf("ALPENGLOW rooted recovery: restored speculative child %s at slot %d linking to parent %s at slot %d; certificate decisions remain authoritative",
		e.ChildID, e.ChildSlot, e.ParentID, e.ParentSlot)
}
