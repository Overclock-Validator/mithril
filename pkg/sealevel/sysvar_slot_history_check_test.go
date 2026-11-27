package sealevel

import "testing"

func TestSlotHistoryCheck(t *testing.T) {
	sh := SysvarSlotHistory{
		Bits:     SlotHistoryBitvec{Bits: SlotHistoryInner{BlocksLen: slotHistoryMaxEntries / bitsPerBlock, Blocks: make([]uint64, slotHistoryMaxEntries/bitsPerBlock)}},
		NextSlot: 0,
	}
	for _, slot := range []uint64{5, 7, 2_000_000} {
		sh.Add(slot)
		sh.SetNextSlot(slot + 1)
	}
	for slot, want := range map[uint64]SlotHistoryCheck{
		2_000_000: SlotHistoryFound,
		1_999_999: SlotHistoryNotFound,
		2_000_001: SlotHistoryFuture,
		7:         SlotHistoryTooOld,
	} {
		if got := sh.Check(slot); got != want {
			t.Fatalf("Check(%d) = %d, want %d", slot, got, want)
		}
	}
}
