package turbine

import (
	"testing"
	"time"
)

// The emission head is pinned once turbine delivered any of its shreds, so a
// slot missing its header FEC set heads repair even at the live edge, where
// freshness repair would otherwise wait for the next slot.
func TestPrioritizeObservedRepairSlotPinsObservedHeaderlessSlot(t *testing.T) {
	assembler := NewSlotAssembler()
	assembler.maxObservedSlot = 220
	assembler.slots[220] = &slotState{
		slot:         220,
		shreds:       map[uint32]*Shred{40: {Slot: 220, Type: ShredTypeData, Index: 40}},
		fecSets:      make(map[uint32]*fecState),
		firstShredAt: time.Now(),
	}

	if !assembler.prioritizeObservedRepairSlot(220) {
		t.Fatal("observed incomplete slot was not pinned")
	}
	if assembler.prioritizeObservedRepairSlot(220) {
		t.Fatal("a repeated pin reported a new install (it would wake repair every emitter pass)")
	}
	priority, _ := assembler.RepairRequestsTiered(32, 64)
	if len(priority) != 1 || priority[0].Slot != 220 {
		t.Fatalf("priority = %+v, want the observed emission head 220", priority)
	}
	missing := priority[0].MissingDataShreds
	if len(missing) == 0 || missing[0] != 0 {
		t.Fatalf("head listing = %v, want the lost header shreds from index 0", missing)
	}
}

// A slot with no shreds may never be produced (skipped leader): no pin, so
// repair does not probe it.
func TestPrioritizeObservedRepairSlotSkipsUnobservedAndCompletedSlots(t *testing.T) {
	assembler := NewSlotAssembler()
	assembler.maxObservedSlot = 220
	if assembler.prioritizeObservedRepairSlot(221) {
		t.Fatal("pinned a slot with no shreds")
	}
	assembler.slots[219] = &slotState{slot: 219, shreds: map[uint32]*Shred{}, fecSets: map[uint32]*fecState{}, firstShredAt: time.Now()}
	assembler.completedSlots[219] = struct{}{}
	if assembler.prioritizeObservedRepairSlot(219) {
		t.Fatal("pinned a completed slot")
	}
	if assembler.prioritizeObservedRepairSlot(0) {
		t.Fatal("pinned slot 0")
	}
	if priority, _ := assembler.RepairRequestsTiered(32, 8); len(priority) != 0 {
		t.Fatalf("priority = %+v, want none", priority)
	}
}
