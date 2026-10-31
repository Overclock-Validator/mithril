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

// prioritizeObservedRepairSlot pins slot for priority repair when it holds at
// least one accepted shred and is not complete. It reports whether a new pin
// was installed.
func (a *SlotAssembler) prioritizeObservedRepairSlot(slot uint64) bool {
	if slot == 0 {
		return false
	}
	a.mu.Lock()
	state := a.slots[slot]
	_, completed := a.completedSlots[slot]
	observed := state != nil && !state.firstShredAt.IsZero() && !state.completing
	a.mu.Unlock()
	if !observed || completed {
		return false
	}
	return a.prioritizeRepairRange(slot, slot)
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
