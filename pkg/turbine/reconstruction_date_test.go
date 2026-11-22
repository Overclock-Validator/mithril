package turbine

import (
	"math"
	"testing"
)

func TestReconstructionDatePreservesHighWaterAcrossReset(t *testing.T) {
	for _, slot := range []uint64{0, math.MaxUint64} {
		a := NewSlotAssembler()
		shred := &Shred{Variant: legacyDataVariant, Type: ShredTypeData, Slot: slot, Flags: shredFlagLastShredInSlot, Data: []byte{1}}
		work, err := a.addShredFrom(shred, true)
		if err != nil || work == nil {
			t.Fatalf("first all-data-full slot %d: %v", slot, err)
		}
		first := a.ShredFrontiers()
		if !first.HasReconstructed || first.ReconstructedSlot != slot || first.ReconstructedAt.IsZero() || !first.ReconstructedAt.Equal(work.state.fullAt) {
			t.Fatalf("completion date not retained: %+v", first)
		}
		// A completion still being decoded ignores another copy of its shreds.
		_, _ = a.addShredFrom(shred, false)
		if got := a.ShredFrontiers(); got != first {
			t.Fatal("duplicate refreshed the event")
		}
		a.ResetSlot(slot)
		if _, err := a.addShredFrom(shred, false); err != nil {
			t.Fatal(err)
		}
		if got := a.ShredFrontiers(); got != first {
			t.Fatal("same-slot reconstruction after reset refreshed the historical high-water")
		}
		if got := NewSlotAssembler().ShredFrontiers(); !got.ReconstructedAt.IsZero() || got.HasReconstructed {
			t.Fatal("replacement inherited a prior assembler event")
		}
	}
}
