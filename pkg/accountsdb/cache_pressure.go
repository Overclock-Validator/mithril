package accountsdb

import (
	"math"
	"sync/atomic"
)

// cacheInsertionWeight observes Otter v2.4.0's non-expiring weighted-cache
// profile. The internal weigher prepares a weight before table publication;
// the atomic deletion callback announces retirement before removal or an
// in-place replacement. Their difference is approximate during mutations and
// exact at quiescence. It is neither policy weighted size nor resident memory.
// Adding expiry, refresh, loaders or fallible write calculators requires a new
// admission review: those paths need not share this factory's write lifecycle.
type cacheInsertionWeight struct {
	prepared atomic.Uint64
	retired  atomic.Uint64
	faulted  atomic.Bool
}

type cacheInsertionWeightSnapshot struct {
	PreparedInsertionWeight   uint64
	AnnouncedRetirementWeight uint64
	ApproximateWeight         uint64
	KnownApproximate          bool
	Faulted                   bool
}

// CacheInsertionWeightObservation is an approximate sum of stored insertion
// weights, not current mutable payload size, eviction-policy weight or RAM.
// Faulted observations have no known value for the lifetime of that cache.
type CacheInsertionWeightObservation struct {
	ApproximateWeight uint64
	KnownApproximate  bool
	Faulted           bool
}

func (c *WeightedCache[K, V]) insertionWeightObservation() CacheInsertionWeightObservation {
	s := c.insertionWeight()
	return CacheInsertionWeightObservation{ApproximateWeight: s.ApproximateWeight, KnownApproximate: s.KnownApproximate, Faulted: s.Faulted}
}

func (p *cacheInsertionWeight) addPrepared(weight uint64) {
	p.add(&p.prepared, weight)
}

func (p *cacheInsertionWeight) addRetired(weight uint64) {
	p.add(&p.retired, weight)
}

func (p *cacheInsertionWeight) add(counter *atomic.Uint64, weight uint64) {
	if weight == 0 {
		return
	}
	for {
		old := counter.Load()
		if weight > math.MaxUint64-old {
			p.faulted.Store(true)
			if counter.CompareAndSwap(old, math.MaxUint64) {
				return
			}
			continue
		}
		if counter.CompareAndSwap(old, old+weight) {
			return
		}
	}
}

// recordRetiredInsertionWeight is called only by OnAtomicDeletion, while the
// old table entry is still present. Otter expressly permits this quiet read
// under its bucket lock. Read the stored insertion weight, never the possibly
// mutated Value or the eviction-policy size; a missing entry invalidates this
// observer for its entire cache lifetime.
func (c *WeightedCache[K, V]) recordRetiredInsertionWeight(key K) {
	entry, ok := c.cache.GetEntryQuietly(key)
	if !ok {
		c.pressure.faulted.Store(true)
		return
	}
	c.pressure.addRetired(uint64(entry.Weight))
}

// insertionWeight returns detached scalars without policy maintenance, a scan,
// value reweighing, or a sampling clock. Clear balances callbacks but does not
// reset the lifetime totals. Only constructing a new cache creates a new ledger.
func (c *WeightedCache[K, V]) insertionWeight() cacheInsertionWeightSnapshot {
	if c == nil || c.pressure == nil {
		return cacheInsertionWeightSnapshot{}
	}
	p := c.pressure
	// Each retirement follows its preparation. Retired-first sampling avoids a
	// false underflow when another goroutine prepares and retires between loads.
	retired := p.retired.Load()
	prepared := p.prepared.Load()
	if retired > prepared {
		p.faulted.Store(true)
	}
	s := cacheInsertionWeightSnapshot{
		PreparedInsertionWeight:   prepared,
		AnnouncedRetirementWeight: retired,
		Faulted:                   p.faulted.Load(),
	}
	if !s.Faulted {
		s.ApproximateWeight = prepared - retired
		s.KnownApproximate = true
	}
	return s
}
