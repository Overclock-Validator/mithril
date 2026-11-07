package accountsdb

import (
	"math"
	"sync/atomic"
)

// CacheLookupActivity contains cumulative lookup and policy-removal counts from
// one cache recorder. Reads are approximate under concurrent operations. Manual
// invalidation, oversized-value refusal and Clear are not policy evictions.
type CacheLookupActivity struct {
	Hits, Misses, Evictions, EvictionWeight uint64
}

// CacheActivityObservation separates recorder lifetime from cache validity.
// Clear changes Generation but retains Instance and cumulative activity; cache
// reconstruction changes both. Program hits are key lookups, not verified
// executable reuse, and vote lookups can probe keys that are not vote accounts.
// Fixed-tier hits are separate from the large tier's fallback lookups.
type CacheActivityObservation struct {
	Generation, Instance                               uint64
	Vote, Program, CommonLarge                         CacheLookupActivity
	FixedKnown                                         bool
	FixedEmptyHits, FixedSmallHits                     uint64
	FixedEmptyReplacements, FixedSmallReplacements     uint64
	InsertionWeightsRecorded                           bool
	ProgramInsertionWeight, CommonLargeInsertionWeight CacheInsertionWeightObservation
	// ProgramAcceptedSets counts AddProgramToCache calls accepted by Set,
	// including updates and repeated values, not unique programs or residency.
	// Saturation makes only this counter unknown until cache reconstruction.
	ProgramAcceptedSets      uint64
	ProgramAcceptedSetsKnown bool
	// ProgramRefusedSets counts oversized program-cache Set calls, including
	// refused updates.
	ProgramRefusedSets      uint64
	ProgramRefusedSetsKnown bool
}

// recordProgramSet runs while programCacheMu pins the cache instance.
// MaxUint64 is terminal unknown, never a wrapped zero.
func recordProgramSet(counter *atomic.Uint64) {
	for {
		old := counter.Load()
		if old == math.MaxUint64 || counter.CompareAndSwap(old, old+1) {
			return
		}
	}
}

func (c *WeightedCache[K, V]) lookupActivity() CacheLookupActivity {
	s := c.cache.Stats()
	return CacheLookupActivity{Hits: s.Hits, Misses: s.Misses, Evictions: s.Evictions, EvictionWeight: s.EvictionWeight}
}

// CacheActivity returns detached scalars without scanning keys or running cache
// maintenance. The caller must pin the initialized owner, as for CacheObservation.
// Recording is default off. A busy or saturated fixed shard makes only its
// aggregate unknown. Otter counters retain that library's overflow semantics.
func (db *AccountsDb) CacheActivity() (CacheActivityObservation, bool) {
	if db == nil {
		return CacheActivityObservation{}, false
	}
	db.readCacheEpochMu.RLock()
	defer db.readCacheEpochMu.RUnlock()
	db.programCacheMu.RLock()
	defer db.programCacheMu.RUnlock()
	generation := db.CacheObservationGeneration()
	if generation == 0 || !db.cacheActivityEnabled || db.cacheActivityInstance == 0 ||
		db.VoteAcctCache == nil || db.ProgramCache == nil || db.CommonAcctsCache == nil ||
		db.VoteAcctCache.cache == nil || db.ProgramCache.cache == nil || db.CommonAcctsCache.large == nil ||
		db.CommonAcctsCache.large.cache == nil || !db.VoteAcctCache.cache.IsRecordingStats() ||
		!db.ProgramCache.cache.IsRecordingStats() || !db.CommonAcctsCache.recordActivity ||
		!db.CommonAcctsCache.large.cache.IsRecordingStats() {
		return CacheActivityObservation{}, false
	}
	o := CacheActivityObservation{
		Generation: generation, Instance: db.cacheActivityInstance,
		Vote: db.VoteAcctCache.lookupActivity(), Program: db.ProgramCache.lookupActivity(),
		CommonLarge: db.CommonAcctsCache.large.lookupActivity(),
	}
	if count := db.programCacheAcceptedSets.Load(); count != math.MaxUint64 {
		o.ProgramAcceptedSets, o.ProgramAcceptedSetsKnown = count, true
	}
	if count := db.programCacheRefusedSets.Load(); count != math.MaxUint64 {
		o.ProgramRefusedSets, o.ProgramRefusedSetsKnown = count, true
	}
	if db.ProgramCache.pressure != nil && db.CommonAcctsCache.large.pressure != nil {
		o.InsertionWeightsRecorded = true
		o.ProgramInsertionWeight = db.ProgramCache.insertionWeightObservation()
		o.CommonLargeInsertionWeight = db.CommonAcctsCache.large.insertionWeightObservation()
	}
	fixed, known := db.CommonAcctsCache.fixedActivity()
	o.FixedKnown = known
	o.FixedEmptyHits, o.FixedSmallHits = fixed[0], fixed[1]
	o.FixedEmptyReplacements, o.FixedSmallReplacements = fixed[2], fixed[3]
	return o, true
}

func (c *commonAccountCache) fixedActivity() (totals [4]uint64, known bool) {
	if !c.enabled {
		return totals, true
	}
	for i := range c.shards {
		s := &c.shards[i]
		if !s.mu.TryLock() {
			return [4]uint64{}, false
		}
		values, saturated := s.activity, s.activitySaturated
		s.mu.Unlock()
		if saturated {
			return [4]uint64{}, false
		}
		for j, value := range values {
			if value > math.MaxUint64-totals[j] {
				return [4]uint64{}, false
			}
			totals[j] += value
		}
	}
	return totals, true
}
