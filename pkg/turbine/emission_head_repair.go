package turbine

// Emission-head repair pin.
//
// A slot is normally pinned for priority repair when replay opens its stream,
// which needs the block header decoded from the slot's first FEC set. When
// that first set is what turbine lost, the stream cannot open, so the slot was
// pinned only once a later slot completed (gap detection) or freshness repair
// reached it (after the next slot's shreds arrived). Live, such blocks first
// asked for their missing header shreds 100 to 260 ms after the leader's
// footer and completed 350 to 450 ms after it, against about 75 ms normally.
//
// The block source therefore pins the slot it will emit next as soon as that
// slot becomes the emission head, provided turbine has delivered at least one
// of its shreds. That is the pin the slot would receive the moment its header
// arrived, so repair is no more aggressive than for any other block; a slot
// with no shreds (a leader that never produced) is not pinned, so nothing is
// probed for blocks that may never exist.

// Blocks stream back to back, so the next slot's first shred often arrives a
// few milliseconds after its parent is emitted. A head with no shreds yet is
// therefore remembered, and its first accepted shred installs the pin.

// prioritizeObservedRepairSlot pins slot for priority repair when it holds at
// least one accepted shred and is not complete, or arranges for its first
// shred to pin it. It reports whether a new pin was installed.
func (a *SlotAssembler) prioritizeObservedRepairSlot(slot uint64) bool {
	if slot == 0 {
		return false
	}
	a.mu.Lock()
	state := a.slots[slot]
	_, completed := a.completedSlots[slot]
	observed := state != nil && !state.firstShredAt.IsZero()
	completing := state != nil && state.completing
	if !observed && !completed {
		a.pendingHeadRepairPin = slot
	} else if a.pendingHeadRepairPin == slot {
		a.pendingHeadRepairPin = 0
	}
	a.mu.Unlock()
	if !observed || completed || completing {
		return false
	}
	return a.prioritizeRepairRange(slot, slot)
}

// notePendingHeadRepairPinLocked installs the pending emission-head pin when
// that slot accepts its first shred. Called with a.mu held; the repair wake is
// the same nonblocking, coalesced send the child lookahead uses.
func (a *SlotAssembler) notePendingHeadRepairPinLocked(state *slotState) {
	if a.pendingHeadRepairPin == 0 || state == nil || state.slot != a.pendingHeadRepairPin {
		return
	}
	a.pendingHeadRepairPin = 0
	if _, exists := a.priorityRepairSlots[state.slot]; exists {
		return
	}
	a.priorityRepairSlots[state.slot] = struct{}{}
	a.priorityRepairOrder = append(a.priorityRepairOrder, state.slot)
	a.prunePriorityRepairSlotsLocked()
	select {
	case a.streamRepairWake <- struct{}{}:
	default:
	}
}

// PrioritizeObservedRepairSlot pins the next slot to emit for repair once
// turbine has delivered any of its shreds (see prioritizeObservedRepairSlot).
func (r *UDPReceiver) PrioritizeObservedRepairSlot(slot uint64) {
	if r == nil || r.assembler == nil {
		return
	}
	if r.assembler.prioritizeObservedRepairSlot(slot) && r.repairClient != nil {
		r.repairClient.wakePriority()
	}
}
