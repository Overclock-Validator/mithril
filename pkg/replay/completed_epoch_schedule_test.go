package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

func TestCompletedEpochScheduleOwnedGeometry(t *testing.T) {
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128, Warmup: true, FirstNormalEpoch: 2, FirstNormalSlot: 96}
	for _, slot := range []uint64{0, 31, 32, 95, 96, 223, 224} {
		epoch, index := schedule.GetEpochAndSlotIndex(slot)
		bank := CompletedBlock{Slot: slot, Epoch: epoch, SlotIndex: index, SlotsInEpoch: schedule.SlotsInEpoch(epoch), EpochSchedule: schedule}
		copy, ok := bank.CheckedEpochSchedule()
		if !ok || copy != schedule {
			t.Fatalf("slot%d rejected owned geometry", slot)
		}
		copy.SlotsPerEpoch = 1
		if bank.EpochSchedule != schedule {
			t.Fatal("returned schedule was not detached")
		}
		if slot == 31 && schedule.SlotsInEpoch(epoch+1) != 64 {
			t.Fatal("warmup next epoch lost its different length")
		}
		for _, change := range []func(*CompletedBlock){
			func(b *CompletedBlock) { b.Epoch++ }, func(b *CompletedBlock) { b.SlotIndex++ },
			func(b *CompletedBlock) { b.SlotsInEpoch++ }, func(b *CompletedBlock) { b.EpochSchedule.SlotsPerEpoch = 0 },
			func(b *CompletedBlock) { b.EpochSchedule.FirstNormalSlot++ },
		} {
			bad := bank
			change(&bad)
			if _, ok := bad.CheckedEpochSchedule(); ok {
				t.Fatal("incoherent bank geometry accepted")
			}
		}
	}
	if _, ok := (CompletedBlock{}).CheckedEpochSchedule(); ok {
		t.Fatal("legacy omission invented geometry")
	}
}
