package block

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTransactionSignaturesVerifiedMarkerIsNotSerialized(t *testing.T) {
	original := &Block{Slot: 42}
	original.MarkTransactionSignaturesVerified()
	admissionStart := time.Now()
	original.MarkTurbineReplayAdmissionStart(admissionStart)
	ingress := TurbineIngressTimings{
		ShredCollection: 12 * time.Millisecond, TransactionSigverify: 34 * time.Millisecond,
		EarlyTransactionParse: 2 * time.Millisecond, EarlyTransactionSigverify: 56 * time.Millisecond,
		EarlyPreparationWait:      3 * time.Millisecond,
		EarlyVerifiedTransactions: 80, FullToReady: 35 * time.Millisecond,
	}
	original.MarkTurbineIngressTimings(ingress)
	if got, ok := original.TurbineIngressTimings(); !ok || got != ingress {
		t.Fatalf("ingress timings = %+v, %t; want %+v, true", got, ok, ingress)
	}
	if got, ok := original.TurbineReplayAdmissionStart(); !ok || got != admissionStart {
		t.Fatalf("admission clock = %v, %t; want %v, true", got, ok, admissionStart)
	}
	if !original.TransactionSignaturesVerified() {
		t.Fatal("verification marker was not set")
	}

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal block: %v", err)
	}

	var decoded Block
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal block: %v", err)
	}
	if decoded.TransactionSignaturesVerified() {
		t.Fatal("verification marker crossed a serialization boundary")
	}
	if got, ok := decoded.TurbineReplayAdmissionStart(); ok || !got.IsZero() {
		t.Fatalf("admission clock crossed a serialization boundary: %v, %t", got, ok)
	}
	if got, ok := decoded.TurbineIngressTimings(); ok || got != (TurbineIngressTimings{}) {
		t.Fatalf("ingress timings crossed a serialization boundary: %+v, %t", got, ok)
	}
}

func TestCompleteTurbineReplayAdmissionIsMonotonicAndOneShot(t *testing.T) {
	start := time.Now()
	blk := &Block{}
	blk.MarkTurbineIngressTimings(TurbineIngressTimings{BlockDecode: time.Millisecond})
	blk.MarkTurbineReplayAdmissionStart(start)

	got, ok := blk.CompleteTurbineReplayAdmission(start.Add(25 * time.Millisecond))
	if !ok || got.ReplayAdmission != 25*time.Millisecond || got.BlockDecode != time.Millisecond {
		t.Fatalf("completed ingress timings = %+v, %t", got, ok)
	}
	if _, ok := blk.CompleteTurbineReplayAdmission(start.Add(30 * time.Millisecond)); ok {
		t.Fatal("replay admission completed more than once")
	}
	if _, ok := blk.TurbineReplayAdmissionStart(); ok {
		t.Fatal("completed replay admission retained a live start clock")
	}
}
