package arena

import (
	"sync"
	"testing"
)

type resetFixture struct {
	ref   *int
	value uint64
}

func TestResetClearsOnlyUsedPrefix(t *testing.T) {
	a := New[resetFixture](4)
	value := 7

	first, ok := a.Alloc()
	if !ok {
		t.Fatal("first allocation unexpectedly fell back to the heap")
	}
	second, ok := a.Alloc()
	if !ok {
		t.Fatal("second allocation unexpectedly fell back to the heap")
	}
	*first = resetFixture{ref: &value, value: 11}
	*second = resetFixture{ref: &value, value: 13}

	// This slot was not handed out by the arena. Reset must not spend time
	// clearing capacity that was never used.
	unused := resetFixture{ref: &value, value: 17}
	a.pool[3] = unused

	a.Reset()

	if a.index != 0 {
		t.Fatalf("index after reset = %d, want 0", a.index)
	}
	if got := a.pool[0]; got != (resetFixture{}) {
		t.Fatalf("first used slot was not cleared: %+v", got)
	}
	if got := a.pool[1]; got != (resetFixture{}) {
		t.Fatalf("second used slot was not cleared: %+v", got)
	}
	if got := a.pool[3]; got != unused {
		t.Fatalf("unused capacity was cleared: got %+v, want %+v", got, unused)
	}

	recycled, ok := a.Alloc()
	if !ok {
		t.Fatal("allocation after reset unexpectedly fell back to the heap")
	}
	if recycled != first {
		t.Fatal("reset did not recycle the arena from its first slot")
	}
	if *recycled != (resetFixture{}) {
		t.Fatalf("recycled slot contains stale state: %+v", *recycled)
	}
}

func TestResetClearsFullUsedArena(t *testing.T) {
	a := New[resetFixture](3)
	value := 23
	for i := range 3 {
		item, ok := a.Alloc()
		if !ok {
			t.Fatalf("allocation %d unexpectedly fell back to the heap", i)
		}
		*item = resetFixture{ref: &value, value: uint64(i + 1)}
	}

	a.Reset()

	for i, item := range a.pool {
		if item != (resetFixture{}) {
			t.Fatalf("used slot %d was not cleared: %+v", i, item)
		}
	}
}

func TestResetDoesNotAffectHeapFallback(t *testing.T) {
	a := New[resetFixture](1)
	used, ok := a.Alloc()
	if !ok {
		t.Fatal("arena allocation unexpectedly fell back to the heap")
	}
	used.value = 29

	fallback, ok := a.Alloc()
	if ok {
		t.Fatal("allocation beyond capacity unexpectedly came from the arena")
	}
	fallback.value = 31

	a.Reset()

	if used.value != 0 {
		t.Fatalf("arena-backed value after reset = %d, want 0", used.value)
	}
	if fallback.value != 31 {
		t.Fatalf("heap fallback was modified by reset: got %d, want 31", fallback.value)
	}
}

func TestConcurrentOverflowAccountingAndReset(t *testing.T) {
	a := New[byte](10)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				buf, _ := a.AllocN(3)
				buf[0] = 17
			}
		})
	}
	wg.Wait()
	elements, allocations := a.Overflow()
	if a.Used() != 9 || elements != 3*797 || allocations != 797 {
		t.Fatalf("used=%d overflow=%d allocations=%d", a.Used(), elements, allocations)
	}
	// Zero-size requests consume neither space nor an overflow allocation.
	a.AllocN(0)
	// A single-element allocation can still fit after the larger fallbacks.
	a.Alloc()
	a.Alloc()
	elements, allocations = a.Overflow()
	if elements != 3*797+1 || allocations != 798 {
		t.Fatalf("mixed allocation accounting: elements=%d allocations=%d", elements, allocations)
	}
	a.ResetPaced(4, func() {})
	elements, allocations = a.Overflow()
	if a.Used() != 0 || elements != 0 || allocations != 0 {
		t.Fatal("paced reset did not reset allocation accounting")
	}
	a.AllocN(11)
	a.Reset()
	elements, allocations = a.Overflow()
	if elements != 0 || allocations != 0 {
		t.Fatal("reset did not reset overflow accounting")
	}
}

func BenchmarkResetSmallUsedPrefix(b *testing.B) {
	const (
		capacity = 64 << 20
		used     = 4 << 10
	)
	a := New[byte](capacity)
	b.ReportAllocs()
	b.SetBytes(used)

	for b.Loop() {
		buf, ok := a.AllocN(used)
		if !ok {
			b.Fatal("allocation unexpectedly fell back to the heap")
		}
		buf[0] = 1
		buf[len(buf)-1] = 1
		a.Reset()
	}
}
