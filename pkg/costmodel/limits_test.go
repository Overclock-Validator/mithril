package costmodel

import "testing"

func TestDefaultTargetBatchBytesMatchesTwoTypicalFECSets(t *testing.T) {
	const want = 61_632
	if DefaultTargetBatchBytes != want {
		t.Fatalf("DefaultTargetBatchBytes = %d, want %d", DefaultTargetBatchBytes, want)
	}
	if DefaultTargetBatchBytes != 2*DataShredsPerFECBlock*TypicalDataShredPayloadBytes {
		t.Fatal("default batch target must remain two complete typical FEC payloads")
	}
}

// A batch spanning multiple FEC sets must consume each set from the slot budget.
func TestPackEntryBytesMaxFitsAvailableFECs(t *testing.T) {
	for fecSets := uint64(0); fecSets < 100; fecSets++ {
		got := PackEntryBytesMax(fecSets*DataShredsPerFECSet, MaxMicroblockBytes)
		capacity := fecSets * TypicalFECSetPayloadBytes
		if got > capacity {
			t.Fatalf("%d FEC sets: entry budget %d exceeds raw payload capacity %d", fecSets, got, capacity)
		}
	}
}
