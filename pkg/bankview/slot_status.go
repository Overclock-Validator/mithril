package bankview

import (
	"github.com/gagliardetto/solana-go"
)

// SlotStatus is how far one slot has progressed on the chain replay follows.
type SlotStatus struct {
	Slot uint64
	// Status is one of the Level names, "skipped" (no block on this chain),
	// or "unknown" (ahead of replay, or older than retained history).
	Status     string
	BlockID    solana.Hash
	HasBlockID bool
	// FromHistory marks a slot older than the retained views. Status holds
	// the level it has if it was a block; the caller must check the
	// finalized bank's SlotHistory and report "skipped" or "unknown"
	// otherwise.
	FromHistory bool
}

// SlotStatuses reports each slot's status against the current views. It also
// returns the processed slot it was evaluated at and whether the notarized
// level is backed by a notarization certificate newer than finality.
func (p *Publisher) SlotStatuses(slots []uint64) (processedSlot uint64, certificateNotarized bool, out []SlotStatus, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	snap := p.cur.Load()
	if snap == nil || snap.views[Processed] == nil {
		return 0, false, nil, ErrNotReady
	}
	proc := snap.views[Processed]
	certificateNotarized = selectLevel(snap, Notarized) == Notarized
	out = make([]SlotStatus, len(slots))
	for i, slot := range slots {
		out[i] = p.slotStatusLocked(snap, slot)
	}
	return proc.Slot, certificateNotarized, out, nil
}

func (p *Publisher) slotStatusLocked(snap *snapshot, slot uint64) SlotStatus {
	st := SlotStatus{Slot: slot, Status: "unknown"}
	if slot > snap.views[Processed].Slot {
		return st
	}
	if len(p.recent) > 0 && slot >= p.recent[0].Slot {
		for i := len(p.recent) - 1; i >= 0; i-- {
			v := p.recent[i]
			if v.Slot < slot {
				break
			}
			if v.Slot == slot {
				st.Status = levelAt(snap, slot).String()
				st.BlockID, st.HasBlockID = v.BlockID, v.HasBlockID
				return st
			}
		}
		st.Status = "skipped"
		return st
	}
	// Older than the retained views, so at least finalized if it was a
	// block. The caller confirms that from the finalized bank's SlotHistory
	// sysvar (see FromHistory).
	st.Status = levelAt(snap, slot).String()
	st.FromHistory = true
	return st
}

// levelAt is the highest level whose view is at or above slot. Ancestors of a
// notarized, finalized or durable block share its level.
func levelAt(snap *snapshot, slot uint64) Level {
	for level := Durable; level > Processed; level-- {
		if v := snap.views[level]; v != nil && v.Slot >= slot {
			if level == Notarized && selectLevel(snap, Notarized) != Notarized {
				continue
			}
			return level
		}
	}
	return Processed
}
