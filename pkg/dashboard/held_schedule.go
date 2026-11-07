package dashboard

import (
	"math"
	"time"
)

// HeldScheduleCapacity is a local lookup window, not complete epoch coverage.
const HeldScheduleCapacity = 32

// HeldScheduleWindow anchors locally held assignments to a completed bank. An
// assignment is not evidence that this leader produced a block or was finalized.
// The parent replay capture supplies run/attempt binding and capture time.
type HeldScheduleWindow struct {
	Scope                string              `json:"scope"`
	ReferenceSlot        uint64              `json:"reference_slot,string"`
	ReferenceBankhash    string              `json:"reference_bankhash"`
	ReferenceCompletedAt time.Time           `json:"reference_completed_at"`
	Epoch                uint64              `json:"epoch,string"`
	SlotIndex            uint64              `json:"slot_index,string"`
	SlotsInEpoch         uint64              `json:"slots_in_epoch,string"`
	Slots                []HeldScheduleSlot  `json:"slots"`
	OwnAssignments       *HeldOwnAssignments `json:"own_assignments,omitempty"`
}

// HeldOwnAssignments counts locally held individual slots for one sampled
// replay identity. Upcoming excludes the parent window's completed slot.
type HeldOwnAssignments struct {
	Identity  string        `json:"identity"`
	Total     uint64        `json:"total,string"`
	Remaining uint64        `json:"remaining,string"`
	Upcoming  []HeldOwnSlot `json:"upcoming"`
}

// HeldOwnSlot keeps slot numbers exact on the JSON wire.
type HeldOwnSlot struct {
	Slot uint64 `json:"slot,string"`
}

// HeldScheduleSlot preserves unknown assignments as null, never a zero identity.
type HeldScheduleSlot struct {
	Slot   uint64  `json:"slot,string"`
	Leader *string `json:"leader"`
}

// HeldScheduleCount clips at the reference epoch and uint64 boundary, with no
// overflowing endpoint calculation. Zero denotes invalid epoch geometry.
func HeldScheduleCount(slot, index, length uint64) int {
	if length == 0 || index >= length || index > slot {
		return 0
	}
	count := min(uint64(HeldScheduleCapacity), length-index)
	if math.MaxUint64-slot < count-1 {
		count = math.MaxUint64 - slot + 1
	}
	return int(count)
}

func validHeldSchedule(w *HeldScheduleWindow) bool {
	if w == nil {
		return true
	}
	count := HeldScheduleCount(w.ReferenceSlot, w.SlotIndex, w.SlotsInEpoch)
	if w.Scope != "held_schedule_at_capture" || count == 0 || len(w.Slots) != count ||
		!validReplayDetailHash(w.ReferenceBankhash, true) || !validReplayDetailTime(w.ReferenceCompletedAt) {
		return false
	}
	for i, entry := range w.Slots {
		if entry.Slot != w.ReferenceSlot+uint64(i) || entry.Leader != nil && !validReplayDetailHash(*entry.Leader, true) {
			return false
		}
	}
	if own := w.OwnAssignments; own != nil {
		end := w.ReferenceSlot + min(w.SlotsInEpoch-w.SlotIndex-1, math.MaxUint64-w.ReferenceSlot)
		var referenceOwn uint64
		if w.Slots[0].Leader != nil && *w.Slots[0].Leader == own.Identity {
			referenceOwn = 1
		}
		if !validReplayDetailHash(own.Identity, true) || own.Total > end-(w.ReferenceSlot-w.SlotIndex)+1 || own.Remaining > own.Total ||
			own.Remaining > end-w.ReferenceSlot || own.Total-own.Remaining < referenceOwn || own.Total-own.Remaining > w.SlotIndex+referenceOwn ||
			own.Upcoming == nil || len(own.Upcoming) != int(min(own.Remaining, 16)) {
			return false
		}
		previous := w.ReferenceSlot
		for _, entry := range own.Upcoming {
			if entry.Slot <= previous || entry.Slot > end {
				return false
			}
			previous = entry.Slot
			if offset := entry.Slot - w.ReferenceSlot; offset < uint64(len(w.Slots)) &&
				(w.Slots[offset].Leader == nil || *w.Slots[offset].Leader != own.Identity) {
				return false
			}
		}
		var known, upcoming uint64
		for i, entry := range w.Slots {
			if entry.Leader == nil || *entry.Leader != own.Identity {
				continue
			}
			known++
			if i > 0 {
				upcoming++
				if upcoming <= uint64(len(own.Upcoming)) && own.Upcoming[upcoming-1].Slot != entry.Slot {
					return false
				}
			}
		}
		if own.Total < known || own.Remaining < upcoming || own.Remaining > upcoming+(end-w.Slots[len(w.Slots)-1].Slot) {
			return false
		}
	}
	return true
}

func cloneHeldSchedule(w *HeldScheduleWindow) *HeldScheduleWindow {
	if w == nil {
		return nil
	}
	c := *w
	c.Slots = make([]HeldScheduleSlot, len(w.Slots))
	for i, slot := range w.Slots {
		c.Slots[i] = slot
		if slot.Leader != nil {
			value := *slot.Leader
			c.Slots[i].Leader = &value
		}
	}
	if w.OwnAssignments != nil {
		own := *w.OwnAssignments
		own.Upcoming = append(make([]HeldOwnSlot, 0, len(own.Upcoming)), own.Upcoming...)
		c.OwnAssignments = &own
	}
	return &c
}
