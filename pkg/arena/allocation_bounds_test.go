package arena

import "testing"

func TestAllocNRestrictsCapacityToReservation(t *testing.T) {
	a := New[byte](16)
	first, ok := a.AllocN(4)
	if !ok || len(first) != 4 || cap(first) != 4 {
		t.Fatalf("allocation len=%d cap=%d arena=%v", len(first), cap(first), ok)
	}
	// Appending cannot write into a neighboring allocation or an unused suffix
	// that Reset would not clear. Growing beyond the reservation uses the heap.
	grown := append(first, 0xa5)
	second, ok := a.AllocN(4)
	if !ok || second[0] != 0 {
		t.Fatal("append escaped the first reservation")
	}
	second[0] = 0x42
	if grown[4] != 0xa5 {
		t.Fatal("neighbor allocation aliases appended data")
	}
	if a.Used() != 8 {
		t.Fatalf("used=%d", a.Used())
	}
	a.Reset()
	all, _ := a.AllocN(16)
	for i, v := range all {
		if v != 0 {
			t.Fatalf("stale byte at %d: %d", i, v)
		}
	}
	if grown[4] != 0xa5 {
		t.Fatal("reset affected heap append")
	}
}

func TestAllocNOverflowDoesNotAdvanceReservation(t *testing.T) {
	a := New[byte](4)
	first, _ := a.AllocN(1)
	first[0] = 7
	func() {
		defer func() {
			if recover() == nil {
				t.Error("impossibly large allocation did not panic")
			}
		}()
		a.AllocN(^uint64(0))
	}()
	if a.Used() != 1 {
		t.Fatalf("failed allocation advanced used to %d", a.Used())
	}
	next, ok := a.AllocN(3)
	if !ok || cap(next) != 3 {
		t.Fatal("failed allocation corrupted remaining capacity")
	}
	if first[0] != 7 {
		t.Fatal("failed allocation changed earlier data")
	}
}

func TestAllocNZeroAndFallbackBounds(t *testing.T) {
	a := New[byte](2)
	zero, ok := a.AllocN(0)
	if !ok || zero != nil || a.Used() != 0 {
		t.Fatal("zero allocation consumed capacity")
	}
	full, ok := a.AllocN(2)
	if !ok || cap(full) != 2 {
		t.Fatal("full allocation bounds")
	}
	fallback, ok := a.AllocN(3)
	if ok || len(fallback) != 3 || cap(fallback) != 3 {
		t.Fatal("fallback bounds")
	}
	fallback[0] = 9
	a.Reset()
	if a.Used() != 0 || fallback[0] != 9 {
		t.Fatal("reset changed fallback or retained usage")
	}
}
