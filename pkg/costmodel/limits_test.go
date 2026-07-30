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
