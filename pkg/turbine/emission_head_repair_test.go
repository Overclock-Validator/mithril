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

// The next slot's first shred usually lands just after its parent is emitted:
// the head is remembered and that first shred installs the pin and wakes
// repair, without waiting for another emitter pass.
func TestPendingHeadRepairPinInstallsOnFirstShred(t *testing.T) {
	assembler := NewSlotAssembler()
	wake := make(chan struct{}, 1)
	assembler.streamRepairWake = wake
	if assembler.prioritizeObservedRepairSlot(301) {
		t.Fatal("pinned a slot before any shred arrived")
	}
	if _, err := assembler.AddShred(&Shred{Variant: legacyDataVariant, Type: ShredTypeData, Slot: 301, Index: 40, Version: 1, ParentOffset: 1, Data: []byte{1}}); err != nil {
		t.Fatalf("AddShred: %v", err)
	}
	select {
	case <-wake:
	default:
		t.Fatal("first shred of the pending head did not wake repair")
	}
	if _, pinned := assembler.priorityRepairSlots[301]; !pinned {
		t.Fatal("first shred of the pending head did not install its pin")
	}
	if assembler.pendingHeadRepairPin != 0 {
		t.Fatalf("pending pin not cleared: %d", assembler.pendingHeadRepairPin)
	}
	// Another slot's first shred does not consume a newer pending head.
	assembler.prioritizeObservedRepairSlot(305)
	if _, err := assembler.AddShred(&Shred{Variant: legacyDataVariant, Type: ShredTypeData, Slot: 304, Index: 0, Version: 1, ParentOffset: 1, Data: []byte{1}}); err != nil {
		t.Fatalf("AddShred: %v", err)
	}
	if _, pinned := assembler.priorityRepairSlots[304]; pinned || assembler.pendingHeadRepairPin != 305 {
		t.Fatalf("pin went to slot 304 or the pending head was lost: pending=%d", assembler.pendingHeadRepairPin)
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
