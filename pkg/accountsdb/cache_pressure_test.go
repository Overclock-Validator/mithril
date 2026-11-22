package accountsdb

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/maypok86/otter/v2"
)

func insertionWeightTestCache[K comparable, V any](t *testing.T, capacity uint64, weigh func(K, V) uint32) *WeightedCache[K, V] {
	t.Helper()
	c := newWeightedCache(capacity, weigh, true)
	t.Cleanup(func() { c.cache.StopAllGoroutines() })
	return c
}

// Maintenance and enumeration belong only to this independent, quiescent test
// oracle. In particular, a mutable payload's current size is not the oracle.
func checkInsertionWeight[K comparable, V any](t *testing.T, c *WeightedCache[K, V]) cacheInsertionWeightSnapshot {
	t.Helper()
	c.cache.CleanUp()
	var total uint64
	for key := range c.cache.Keys() {
		entry, ok := c.cache.GetEntryQuietly(key)
		if !ok || uint64(entry.Weight) > math.MaxUint64-total {
			t.Fatal("invalid quiescent native entry")
		}
		total += uint64(entry.Weight)
	}
	s := c.insertionWeight()
	if !s.KnownApproximate || s.Faulted || s.ApproximateWeight != total {
		t.Fatalf("insertion weights %+v; native entry sum %d", s, total)
	}
	return s
}

func TestCacheInsertionWeightOverflowAndClear(t *testing.T) {
	c := insertionWeightTestCache(t, 100, func(int, int) uint32 { return 1 })
	for key := range 1000 {
		if !c.Set(key, key) {
			t.Fatal("small entry refused")
		}
	}
	before := checkInsertionWeight(t, c)
	activity := c.lookupActivity()
	if activity.Evictions == 0 || before.PreparedInsertionWeight != 1000 || before.ApproximateWeight > 100 {
		t.Fatalf("real policy overflow not exercised: %+v, %+v", before, activity)
	}
	c.Clear()
	after := checkInsertionWeight(t, c)
	if after.PreparedInsertionWeight != before.PreparedInsertionWeight || after.AnnouncedRetirementWeight != after.PreparedInsertionWeight || c.lookupActivity() != activity {
		t.Fatalf("Clear changed lifetime or eviction statistics: %+v", after)
	}
}

func TestCacheInsertionWeightFaults(t *testing.T) {
	for _, field := range []string{"prepared", "retired"} {
		t.Run(field+"_overflow", func(t *testing.T) {
			p := &cacheInsertionWeight{}
			c := &WeightedCache[int, int]{pressure: p}
			p.prepared.Store(math.MaxUint64)
			if field == "prepared" {
				p.addPrepared(1)
			} else {
				p.retired.Store(math.MaxUint64)
				p.addRetired(1)
			}
			if s := c.insertionWeight(); !s.Faulted || s.KnownApproximate || p.prepared.Load() != math.MaxUint64 || (field == "retired" && p.retired.Load() != math.MaxUint64) {
				t.Fatalf("overflow did not saturate and latch: %+v", s)
			}
			p.addPrepared(0)
			p.addRetired(0)
			if !c.insertionWeight().Faulted {
				t.Fatal("fault cleared")
			}
		})
	}
	t.Run("retired_exceeds_prepared", func(t *testing.T) {
		c := &WeightedCache[int, int]{pressure: &cacheInsertionWeight{}}
		c.pressure.addRetired(1)
		if s := c.insertionWeight(); !s.Faulted || s.KnownApproximate {
			t.Fatal("negative difference was clamped to known zero")
		}
		c.pressure.addPrepared(1)
		if !c.insertionWeight().Faulted {
			t.Fatal("later preparation cleared fault")
		}
	})
	t.Run("missing_entry", func(t *testing.T) {
		c := insertionWeightTestCache(t, 100, func(int, int) uint32 { return 1 })
		c.recordRetiredInsertionWeight(404)
		c.Set(1, 1)
		c.Clear()
		if s := c.insertionWeight(); !s.Faulted || s.KnownApproximate {
			t.Fatal("missing retirement lookup did not latch")
		}
	})
}

func TestCacheInsertionWeightCallbackWindowAndOldInstance(t *testing.T) {
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	old := &WeightedCache[int, uint32]{weigh: func(_ int, weight uint32) uint32 { return weight }, maxWeight: 1000, pressure: &cacheInsertionWeight{}}
	// A test-only listener chain pauses the permitted native callback after the
	// same production retirement helper. No production test hook is installed.
	old.cache = otter.Must(&otter.Options[int, uint32]{
		MaximumWeight: old.maxWeight,
		Weigher: func(key int, value uint32) uint32 {
			weight := old.weigh(key, value)
			old.pressure.addPrepared(uint64(weight))
			return weight
		},
		OnAtomicDeletion: func(event otter.DeletionEvent[int, uint32]) {
			old.recordRetiredInsertionWeight(event.Key)
			close(entered)
			<-release
		},
	})
	t.Cleanup(func() { old.cache.StopAllGoroutines() })
	old.Set(1, 10)
	go func() { old.Delete(1); close(done) }()
	defer func() {
		unblock()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("old deletion remained active during cleanup")
		}
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("retirement callback not reached")
	}
	s := old.insertionWeight()
	entry, ok := old.cache.GetEntryQuietly(1)
	if !s.KnownApproximate || s.ApproximateWeight != 0 || !ok || entry.Weight != 10 {
		t.Fatalf("pre-removal approximate interval missing: %+v, entry %+v, %v", s, entry, ok)
	}
	replacement := insertionWeightTestCache(t, 1000, func(_ int, value uint32) uint32 { return value })
	replacement.Set(1, 7)
	want := checkInsertionWeight(t, replacement)
	unblock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("old deletion did not join")
	}
	if checkInsertionWeight(t, old).ApproximateWeight != 0 || checkInsertionWeight(t, replacement) != want {
		t.Fatal("old callback changed replacement cache")
	}
}
