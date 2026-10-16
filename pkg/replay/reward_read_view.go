package replay

// executedSlotAtOrBefore selects a complete retained bank at the verified
// prefix. Skipped slots and unwound banks have no context and cannot advance an
// RPC epoch. The scan is bounded by the existing unrooted-tail halt cap.
func (t *unrootedTail) executedSlotAtOrBefore(through uint64) uint64 {
	var slot uint64
	for held, ctx := range t.contexts {
		if ctx != nil && ctx.Slot == held && held <= through && held > slot {
			slot = held
		}
	}
	return slot
}
