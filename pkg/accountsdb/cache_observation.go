package accountsdb

// CacheObservation contains only scalar read-cache gauges. Entry counts are
// approximate under concurrent updates; capacities retain their native units.
// It does not report cache hit rates, weighted occupancy, RSS or database health.
type CacheObservation struct {
	Generation                      uint64
	VoteCapacityEntries             uint64
	VoteEstimatedEntries            uint64
	ProgramCapacityCostUnits        uint64
	ProgramCostUnitBytes            uint64
	ProgramEstimatedEntries         uint64
	CommonLayout                    string
	CommonConfiguredBudgetBytes     uint64
	CommonFixedEmptyCapacityEntries uint64
	CommonFixedSmallCapacityEntries uint64
	CommonFixedOccupancyKnown       bool
	CommonFixedEmptyOccupiedEntries uint64
	CommonFixedSmallOccupiedEntries uint64
	CommonLargeCapacityBytes        uint64
	CommonLargeEstimatedEntries     uint64
}

// CacheObservationGeneration is a cheap lifetime fence, not an occupancy read.
// Zero means uninitialized, being initialized, or an exhausted generation.
func (db *AccountsDb) CacheObservationGeneration() uint64 {
	if db == nil {
		return 0
	}
	generation := db.cacheObservationGeneration.Load()
	if generation == ^uint64(0) {
		return 0
	}
	return generation
}

// CacheObservation returns a detached value without I/O, key iteration or cache
// policy maintenance. The caller must pin the initialized cache lifetime and
// stop observations before closing its AccountsDb owner.
func (db *AccountsDb) CacheObservation() (CacheObservation, bool) {
	if db == nil {
		return CacheObservation{}, false
	}
	db.readCacheEpochMu.RLock()
	defer db.readCacheEpochMu.RUnlock()
	db.programCacheMu.RLock()
	defer db.programCacheMu.RUnlock()
	generation := db.CacheObservationGeneration()
	if generation == 0 || db.VoteAcctCache == nil || db.ProgramCache == nil || db.CommonAcctsCache == nil ||
		db.VoteAcctCache.cache == nil || db.ProgramCache.cache == nil || db.CommonAcctsCache.large == nil || db.CommonAcctsCache.large.cache == nil {
		return CacheObservation{}, false
	}
	voteCapacity, voteEntries := db.VoteAcctCache.cacheGauge()
	programCapacity, programEntries := db.ProgramCache.cacheGauge()
	largeCapacity, largeEntries := db.CommonAcctsCache.large.cacheGauge()
	emptyUsed, smallUsed, fixedKnown := db.CommonAcctsCache.fixedOccupancy()
	layout := "flat"
	if db.CommonAcctsCache.enabled {
		layout = "fixed"
	}
	return CacheObservation{
		Generation: generation, VoteCapacityEntries: voteCapacity, VoteEstimatedEntries: voteEntries,
		ProgramCapacityCostUnits: programCapacity, ProgramCostUnitBytes: programCacheCostUnitBytes,
		ProgramEstimatedEntries: programEntries, CommonLayout: layout,
		CommonConfiguredBudgetBytes:     db.CommonAcctsCache.budgetBytes,
		CommonFixedEmptyCapacityEntries: db.CommonAcctsCache.emptyCapacity,
		CommonFixedSmallCapacityEntries: db.CommonAcctsCache.smallCapacity,
		CommonFixedOccupancyKnown:       fixedKnown,
		CommonFixedEmptyOccupiedEntries: emptyUsed, CommonFixedSmallOccupiedEntries: smallUsed,
		CommonLargeCapacityBytes: largeCapacity, CommonLargeEstimatedEntries: largeEntries,
	}, true
}

func nextCacheObservationGeneration(previous uint64) uint64 {
	if previous == ^uint64(0) {
		return previous
	}
	return previous + 1
}
