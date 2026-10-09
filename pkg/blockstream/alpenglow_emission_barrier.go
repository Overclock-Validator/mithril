package blockstream

// waitForAlpenglowEmissionBarrier is used only by replay's serialized recovery
// path. The caller must own alpenglowQuarantineFrom, have crossed reorderMu
// after raising that gate, and have invalidated old live-result generations.
//
// Emitters add to replayEmissionWg under reorderMu before unlocking to send.
// Drain first so a sender blocked on a full channel can finish, then wait for
// both its send and frontier update. The caller must clear late suffix state
// and drain once more before releasing the gate.
func (bs *BlockSource) waitForAlpenglowEmissionBarrier(from uint64, resetSlots map[uint64]struct{}) {
	bs.drainAlpenglowEmissionSuffixForReset(from, resetSlots)
	bs.replayEmissionWg.Wait()
}

func (bs *BlockSource) drainAlpenglowEmissionSuffixForReset(from uint64, resetSlots map[uint64]struct{}) {
	for _, slot := range bs.drainEmittedAlpenglowSuffix(from) {
		resetSlots[slot] = struct{}{}
	}
}

// clearAlpenglowBufferedSuffixLocked requires reorderMu. Skip certificates,
// tombstones, parent-switch state, and the emission frontier remain the
// caller's responsibility because certified rewinds and quarantine differ.
func (bs *BlockSource) clearAlpenglowBufferedSuffixLocked(from uint64, resetSlots map[uint64]struct{}) {
	for slot := range bs.reorderBuffer {
		if slot >= from {
			delete(bs.reorderBuffer, slot)
			resetSlots[slot] = struct{}{}
		}
	}
}

func (bs *BlockSource) clearAlpenglowTrackedSuffix(from uint64, resetSlots map[uint64]struct{}) {
	bs.slotStateMu.Lock()
	defer bs.slotStateMu.Unlock()
	for slot := range bs.slotState {
		if slot >= from {
			delete(bs.slotState, slot)
			delete(bs.inflightStart, slot)
			resetSlots[slot] = struct{}{}
		}
	}
}

func (bs *BlockSource) clearAlpenglowRetrySuffix(from uint64) {
	bs.retryMu.Lock()
	defer bs.retryMu.Unlock()
	kept := bs.retrySlots[:0]
	for _, slot := range bs.retrySlots {
		if slot < from {
			kept = append(kept, slot)
		}
	}
	bs.retrySlots = kept
}

func (bs *BlockSource) clearAlpenglowStagedSuffix(from uint64, resetSlots map[uint64]struct{}) {
	bs.liveStagingMu.Lock()
	defer bs.liveStagingMu.Unlock()
	for slot := range bs.liveStagingBuffer {
		if slot >= from {
			delete(bs.liveStagingBuffer, slot)
			resetSlots[slot] = struct{}{}
		}
	}
	kept := bs.liveStagingOrder[:0]
	for _, slot := range bs.liveStagingOrder {
		if slot < from {
			kept = append(kept, slot)
		}
	}
	bs.liveStagingOrder = kept
}
