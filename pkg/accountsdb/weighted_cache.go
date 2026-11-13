package accountsdb

import (
	"github.com/maypok86/otter/v2"
	"github.com/maypok86/otter/v2/stats"
)

// WeightedCache is a weight-bounded Otter v2 cache with the semantics the
// account and program caches rely on:
//   - Set reports whether the value is now cached. A value heavier than a tenth
//     of the budget is refused, and any prior value for the key is discarded,
//     so one giant entry never evicts most of the working set and a refused
//     refresh never leaves a stale value behind (the per-entry ceiling Otter v1
//     applied through its small-queue budget).
//   - Has does not count as an access for the eviction policy.
type WeightedCache[K comparable, V any] struct {
	cache     *otter.Cache[K, V]
	weigh     func(K, V) uint32
	maxWeight uint64
	pressure  *cacheInsertionWeight
}

// NewWeightedCache returns a cache holding at most maxWeight total weight.
func NewWeightedCache[K comparable, V any](maxWeight uint64, weigh func(K, V) uint32) *WeightedCache[K, V] {
	return newWeightedCache(maxWeight, weigh, false)
}

func newWeightedCache[K comparable, V any](maxWeight uint64, weigh func(K, V) uint32, recordActivity bool) *WeightedCache[K, V] {
	if maxWeight == 0 {
		maxWeight = 1
	}
	options := &otter.Options[K, V]{MaximumWeight: maxWeight, Weigher: weigh}
	c := &WeightedCache[K, V]{weigh: weigh, maxWeight: maxWeight}
	if recordActivity {
		options.StatsRecorder = stats.NewCounter()
	}
	if recordActivity && weigh != nil {
		c.pressure = &cacheInsertionWeight{}
		p := c.pressure
		options.Weigher = func(key K, value V) uint32 {
			weight := weigh(key, value)
			p.addPrepared(uint64(weight))
			return weight
		}
		options.OnAtomicDeletion = func(event otter.DeletionEvent[K, V]) {
			c.recordRetiredInsertionWeight(event.Key)
		}
	}
	c.cache = otter.Must(options)
	return c
}

func (c *WeightedCache[K, V]) Get(key K) (V, bool) {
	return c.cache.GetIfPresent(key)
}

func (c *WeightedCache[K, V]) Set(key K, value V) bool {
	if w := uint64(c.weigh(key, value)); w > c.maxWeight/10 && w > 1 {
		c.cache.Invalidate(key)
		return false
	}
	c.cache.Set(key, value)
	return true
}

func (c *WeightedCache[K, V]) Has(key K) bool {
	_, ok := c.cache.GetEntryQuietly(key)
	return ok
}

func (c *WeightedCache[K, V]) Delete(key K) {
	c.cache.Invalidate(key)
}

func (c *WeightedCache[K, V]) Clear() {
	c.cache.InvalidateAll()
}

// cacheGauge reads the configured weight limit and approximate entry count.
// It does not acquire Otter's eviction-policy lock or run maintenance.
func (c *WeightedCache[K, V]) cacheGauge() (capacity, estimatedEntries uint64) {
	return c.maxWeight, uint64(c.cache.EstimatedSize())
}
