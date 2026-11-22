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
		AssemblyFirstAcceptedAt: admissionStart.Add(-50 * time.Millisecond),
		AssemblyFullAt:          admissionStart.Add(-35 * time.Millisecond), ReadyAt: admissionStart,
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
	blk.MarkTurbineIngressTimings(TurbineIngressTimings{BlockDecode: time.Millisecond, ReadyAt: start})
	blk.MarkTurbineReplayAdmissionStart(start)

	got, ok := blk.CompleteTurbineReplayAdmission(start.Add(25 * time.Millisecond))
	if !ok || got.ReplayAdmission != 25*time.Millisecond || got.BlockDecode != time.Millisecond || got.ReadyAt != start {
		t.Fatalf("completed ingress timings = %+v, %t", got, ok)
	}
	if _, ok := blk.CompleteTurbineReplayAdmission(start.Add(30 * time.Millisecond)); ok {
		t.Fatal("replay admission completed more than once")
	}
	if _, ok := blk.TurbineReplayAdmissionStart(); ok {
		t.Fatal("completed replay admission retained a live start clock")
	}
	if retained, ok := blk.TurbineIngressTimings(); !ok || retained.ReadyAt != start {
		t.Fatal("completed replay admission lost the original ready timestamp")
	}
}

func TestTurbineIngressTimingsAreDetachedValues(t *testing.T) {
	first := time.Now()
	want := TurbineIngressTimings{AssemblyFirstAcceptedAt: first,
		AssemblyFullAt: first.Add(time.Millisecond), ReadyAt: first.Add(2 * time.Millisecond)}
	blk := &Block{}
	blk.MarkTurbineIngressTimings(want)
	got, ok := blk.TurbineIngressTimings()
	if !ok || got != want {
		t.Fatalf("timings changed original time.Time values: %+v, %t", got, ok)
	}
	got.AssemblyFirstAcceptedAt, got.AssemblyFullAt, got.ReadyAt = time.Time{}, time.Time{}, time.Time{}
	if again, ok := blk.TurbineIngressTimings(); !ok || again != want {
		t.Fatal("returned timing values alias the block")
	}
	for _, unknown := range []*Block{nil, {ShredFirstNanos: first.UnixNano(), ShredFullNanos: first.UnixNano()}} {
		if value, ok := unknown.TurbineIngressTimings(); ok || value != (TurbineIngressTimings{}) {
			t.Fatal("wall-clock fields fabricated trusted ingress timing")
		}
	}
}
