package turbine

import (
	"errors"
	"testing"
)

func TestShredFrontiersDistinguishUnknownFromSlotZero(t *testing.T) {
	r := NewUDPReceiver("127.0.0.1:0")
	a := r.assembler
	if got := r.ShredFrontiers(); got != (ShredFrontiers{}) {
		t.Fatalf("new receiver frontiers = %+v, want unknown", got)
	}
	if _, err := a.AddShred(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AddPacket([]byte{1}); !errors.Is(err, ErrShortShred) {
		t.Fatalf("malformed packet error = %v, want ErrShortShred", err)
	}
	if got := a.ShredFrontiers(); got != (ShredFrontiers{}) {
		t.Fatalf("invalid input frontiers = %+v, want unknown", got)
	}

	if _, err := a.AddShred(&Shred{Variant: legacyDataVariant, Type: ShredTypeData, Data: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	want := ShredFrontiers{HasObserved: true}
	if got := r.ShredFrontiers(); got != want {
		t.Fatalf("slot zero frontiers = %+v, want %+v", got, want)
	}
	if observed, full := r.ShredEdges(); observed != 0 || full != 0 {
		t.Fatalf("legacy slot zero edges = (%d, %d), want (0, 0)", observed, full)
	}
}

func TestShredFrontiersAreDetachedSessionMaxima(t *testing.T) {
	r := NewUDPReceiver("127.0.0.1:0")
	work, err := r.assembler.addShredFrom(&Shred{
		Variant: legacyDataVariant,
		Type:    ShredTypeData,
		Slot:    9,
		Flags:   shredFlagLastShredInSlot,
		Data:    []byte{1},
	}, false)
	if err != nil || work == nil {
		t.Fatalf("reconstruction = %+v, error %v", work, err)
	}
	want := ShredFrontiers{ObservedSlot: 9, ReconstructedSlot: 9, HasObserved: true, HasReconstructed: true, ReconstructedAt: work.state.fullAt}
	snapshot := r.ShredFrontiers()
	snapshot.ObservedSlot, snapshot.ReconstructedSlot = 100, 100
	snapshot.HasObserved, snapshot.HasReconstructed = false, false
	if got := r.ShredFrontiers(); got != want {
		t.Fatalf("mutating snapshot changed receiver: %+v, want %+v", got, want)
	}
	r.ResetSlot(9)
	if got := r.ShredFrontiers(); got != want {
		t.Fatalf("slot reset cleared session maxima: %+v, want %+v", got, want)
	}
	if got := NewUDPReceiver("127.0.0.1:0").ShredFrontiers(); got != (ShredFrontiers{}) {
		t.Fatalf("replacement receiver inherited frontiers: %+v", got)
	}
}

func TestShredFrontiersConcurrentReads(t *testing.T) {
	a := NewSlotAssembler()
	done := make(chan struct{})
	go func() {
		defer close(done)
		var previous uint64
		for range 256 {
			got := a.ShredFrontiers()
			if got.ObservedSlot < previous || got.HasReconstructed || got.ReconstructedSlot != 0 {
				t.Errorf("concurrent snapshot = %+v, previous observed slot %d", got, previous)
				return
			}
			previous = got.ObservedSlot
		}
	}()
	defer func() { <-done }()
	for slot := uint64(1); slot <= 256; slot++ {
		if _, err := a.AddShred(&Shred{Variant: legacyDataVariant, Type: ShredTypeData, Slot: slot, Data: []byte{1}}); err != nil {
			t.Fatalf("slot %d: %v", slot, err)
		}
	}
}
