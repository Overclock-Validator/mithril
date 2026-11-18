package blockstream

// trimPausedReplayBufferLocked bounds decoded blocks while bank reconstruction
// has paused their consumer. Keep the earliest slots, since later blocks can
// rehydrate from the shred spool when replay reaches them. Caller holds
// reorderMu and must reset the returned slots outside that lock.
func (bs *BlockSource) trimPausedReplayBufferLocked() []uint64 {
	limit := bs.maxPending
	if limit <= 0 {
		limit = defaultMaxPending
	}
	var dropped []uint64
	for len(bs.reorderBuffer) > limit {
		var highest uint64
		for slot := range bs.reorderBuffer {
			if slot > highest {
				highest = slot
			}
		}
		delete(bs.reorderBuffer, highest)
		bs.slotStateMu.Lock()
		delete(bs.slotState, highest)
		delete(bs.inflightStart, highest)
		bs.slotStateMu.Unlock()
		dropped = append(dropped, highest)
	}
	return dropped
}

func (bs *BlockSource) resetPausedReplayDrops(slots []uint64) {
	for _, slot := range slots {
		bs.clearSlotErrors(slot)
		// This preserves verified packets on disk while reopening assembly;
		// leaving a completed marker would make an evicted block unfetchable.
		bs.resetTurbineSlotState(slot)
	}
}
