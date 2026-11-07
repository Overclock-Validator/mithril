package accountsdb

import (
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
)

func observationTestDB(t testing.TB, fixed bool) *AccountsDb {
	t.Helper()
	db := &AccountsDb{
		VoteAcctCache:    NewWeightedCache(32, func(solana.PublicKey, *accounts.Account) uint32 { return 1 }),
		ProgramCache:     NewWeightedCache(16, func(_ solana.PublicKey, entry *ProgramCacheEntry) uint32 { return entry.CostUnits() }),
		CommonAcctsCache: newCommonAccountCache(2<<20, fixed),
	}
	db.cacheObservationGeneration.Store(1)
	stopObservationCaches(t, db)
	return db
}

func stopObservationCaches(t testing.TB, db *AccountsDb) {
	t.Helper()
	vote, program, large := db.VoteAcctCache.cache, db.ProgramCache.cache, db.CommonAcctsCache.large.cache
	t.Cleanup(func() {
		vote.StopAllGoroutines()
		program.StopAllGoroutines()
		large.StopAllGoroutines()
	})
}

func TestCacheObservationAvailability(t *testing.T) {
	for _, db := range []*AccountsDb{nil, {}} {
		if got, ok := db.CacheObservation(); ok || got != (CacheObservation{}) || db.CacheObservationGeneration() != 0 {
			t.Fatalf("uninitialized observation: got %+v, %v", got, ok)
		}
	}
	db := observationTestDB(t, false)
	cache := db.VoteAcctCache
	db.VoteAcctCache = nil
	if got, ok := db.CacheObservation(); ok || got != (CacheObservation{}) {
		t.Fatalf("partial cache set was published: %+v, %v", got, ok)
	}
	db.VoteAcctCache = cache
}

func TestCacheObservationCountsUnitsAndReset(t *testing.T) {
	for _, fixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "flat", true: "fixed"}[fixed], func(t *testing.T) {
			db := observationTestDB(t, fixed)
			before, ok := db.CacheObservation()
			if !ok || before.Generation != 1 || before.VoteCapacityEntries != 32 || before.ProgramCapacityCostUnits != 16 ||
				before.ProgramCostUnitBytes != 1<<20 || before.CommonConfiguredBudgetBytes != 2<<20 ||
				before.VoteEstimatedEntries != 0 || before.ProgramEstimatedEntries != 0 || before.CommonLargeEstimatedEntries != 0 ||
				!before.CommonFixedOccupancyKnown || before.CommonFixedEmptyOccupiedEntries != 0 || before.CommonFixedSmallOccupiedEntries != 0 {
				t.Fatalf("initial gauges: %+v, %v", before, ok)
			}
			var empty, small uint64
			for i := range db.CommonAcctsCache.shards {
				empty += uint64(len(db.CommonAcctsCache.shards[i].empty))
				small += uint64(len(db.CommonAcctsCache.shards[i].small))
			}
			wantLarge := uint64(2 << 20)
			if fixed {
				wantLarge = 5 * wantLarge / 8
			}
			if before.CommonLayout != map[bool]string{false: "flat", true: "fixed"}[fixed] ||
				before.CommonFixedEmptyCapacityEntries != empty || before.CommonFixedSmallCapacityEntries != small ||
				before.CommonLargeCapacityBytes != wantLarge || fixed && (empty == 0 || small == 0) {
				t.Fatalf("capacity units/layout: %+v", before)
			}
			voteKey, programKey, largeKey := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
			if !db.VoteAcctCache.Set(voteKey, &accounts.Account{Key: voteKey}) {
				t.Fatal("vote fixture not cached")
			}
			db.AddProgramToCache(programKey, &ProgramCacheEntry{})
			if !db.CommonAcctsCache.Set(largeKey, &accounts.Account{Key: largeKey, Data: make([]byte, 201)}) {
				t.Fatal("large fixture not cached")
			}
			if fixed {
				for i, size := range []int{0, 200} {
					key := solana.PublicKey{byte(i + 4)}
					if !db.CommonAcctsCache.Set(key, &accounts.Account{Key: key, Data: make([]byte, size)}) {
						t.Fatal("fixed fixture not cached")
					}
				}
			}
			populated, ok := db.CacheObservation()
			if !ok || populated.VoteEstimatedEntries != 1 || populated.ProgramEstimatedEntries != 1 || populated.CommonLargeEstimatedEntries != 1 {
				t.Fatalf("approximate entries mix counts with weights/fixed slots: %+v, %v", populated, ok)
			}
			wantFixed := uint64(0)
			if fixed {
				wantFixed = 1
			}
			if !populated.CommonFixedOccupancyKnown || populated.CommonFixedEmptyOccupiedEntries != wantFixed || populated.CommonFixedSmallOccupiedEntries != wantFixed {
				t.Fatal("fixed-tier occupancy was lost or mixed with large entries")
			}
			db.readCacheEpochMu.Lock()
			db.resetReadCachesLocked()
			db.readCacheEpochMu.Unlock()
			after, ok := db.CacheObservation()
			before.Generation = 2
			if !ok || after != before || db.CacheObservationGeneration() != 2 {
				t.Fatalf("reset: got %+v, %v; want %+v", after, ok, before)
			}
			if populated.Generation != 1 || populated.VoteEstimatedEntries != 1 || populated.ProgramEstimatedEntries != 1 || populated.CommonLargeEstimatedEntries != 1 {
				t.Fatal("detached earlier observation changed after reset")
			}
		})
	}
}

func TestCacheObservationUsesSavedCapacities(t *testing.T) {
	db := observationTestDB(t, false)
	// Deliberately separate the saved scalars from Otter's policy limit. The
	// observer must not call the locking GetMaximum/WeightedSize APIs.
	db.VoteAcctCache.maxWeight = ^uint64(0)
	db.ProgramCache.maxWeight = ^uint64(0) - 1
	db.CommonAcctsCache.large.maxWeight = ^uint64(0) - 2
	db.CommonAcctsCache.budgetBytes = ^uint64(0) - 3
	got, ok := db.CacheObservation()
	if !ok || got.VoteCapacityEntries != ^uint64(0) || got.ProgramCapacityCostUnits != ^uint64(0)-1 ||
		got.CommonLargeCapacityBytes != ^uint64(0)-2 || got.CommonConfiguredBudgetBytes != ^uint64(0)-3 {
		t.Fatalf("saved full-width capacities were lost: %+v, %v", got, ok)
	}
}

func TestCacheObservationInitializationAndExhaustion(t *testing.T) {
	oldProgram, oldCommon, oldLayout := ProgramCacheMaxMB, CommonAccountCacheMaxMB, CommonAccountCacheLayout
	ProgramCacheMaxMB, CommonAccountCacheMaxMB, CommonAccountCacheLayout = 1, 1, "flat"
	defer func() {
		ProgramCacheMaxMB, CommonAccountCacheMaxMB, CommonAccountCacheLayout = oldProgram, oldCommon, oldLayout
	}()
	db := &AccountsDb{}
	for want := uint64(1); want <= 2; want++ {
		db.InitCaches()
		stopObservationCaches(t, db)
		got, ok := db.CacheObservation()
		if !ok || got.Generation != want || got.VoteCapacityEntries != 2500 || got.ProgramCapacityCostUnits != 1 || got.CommonLargeCapacityBytes != 1<<20 {
			t.Fatalf("initialized caches: got %+v, %v; want generation %d", got, ok, want)
		}
	}
	db.cacheObservationGeneration.Store(^uint64(0) - 1)
	for i := 0; i < 2; i++ {
		db.readCacheEpochMu.Lock()
		db.resetReadCachesLocked()
		db.readCacheEpochMu.Unlock()
		if got, ok := db.CacheObservation(); ok || got != (CacheObservation{}) || db.CacheObservationGeneration() != 0 {
			t.Fatalf("exhausted generation was reused: %+v, %v", got, ok)
		}
	}
	db.InitCaches()
	stopObservationCaches(t, db)
	if _, ok := db.CacheObservation(); ok || db.CacheObservationGeneration() != 0 {
		t.Fatal("reinitialization revived an exhausted generation")
	}
	minimum := newCommonAccountCache(0, true)
	defer minimum.large.cache.StopAllGoroutines()
	if minimum.budgetBytes != 1 || minimum.large.maxWeight != 1 || minimum.enabled {
		t.Fatal("effective zero-budget fallback differs from WeightedCache")
	}
}

func TestCacheObservationConcurrentReadsAndReset(t *testing.T) {
	db := observationTestDB(t, true)
	var group sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			key := solana.PublicKey{byte(worker + 1)}
			account := &accounts.Account{Key: key, Data: make([]byte, 201)}
			for i := 0; i < 200; i++ {
				db.readCacheEpochMu.RLock()
				db.VoteAcctCache.Set(key, account)
				db.CommonAcctsCache.Set(key, account)
				db.VoteAcctCache.Get(key)
				db.CommonAcctsCache.Get(key)
				if i%2 == 0 {
					db.VoteAcctCache.Delete(key)
					db.CommonAcctsCache.Delete(key)
				}
				db.readCacheEpochMu.RUnlock()
				db.AddProgramToCache(key, &ProgramCacheEntry{})
				db.MaybeGetProgramFromCache(key)
				db.RemoveProgramFromCache(key)
			}
		}(worker)
	}
	group.Add(1)
	go func() {
		defer group.Done()
		for i := 0; i < 30; i++ {
			db.readCacheEpochMu.Lock()
			db.resetReadCachesLocked()
			db.readCacheEpochMu.Unlock()
		}
	}()
	for i, previous := 0, uint64(0); i < 500; i++ {
		got, ok := db.CacheObservation()
		if !ok || got.Generation < previous || got.VoteCapacityEntries != 32 || got.ProgramCapacityCostUnits != 16 || got.CommonLayout != "fixed" {
			t.Errorf("concurrent capture: %+v, %v; previous generation %d", got, ok, previous)
		}
		previous = got.Generation
	}
	group.Wait()
}

var cacheObservationSink CacheObservation

func TestCacheObservationDoesNotAllocate(t *testing.T) {
	db := observationTestDB(t, true)
	allocations := testing.AllocsPerRun(1000, func() {
		var ok bool
		cacheObservationSink, ok = db.CacheObservation()
		if !ok {
			panic("initialized cache observation unavailable")
		}
	})
	if allocations != 0 {
		t.Fatalf("cache observation allocated: %g", allocations)
	}
}

func TestCacheObservationBusyFixedTierPreservesOtherGauges(t *testing.T) {
	db := observationTestDB(t, true)
	for _, index := range []int{0, commonTierShards - 1} {
		db.CommonAcctsCache.shards[index].mu.Lock()
		got, ok := db.CacheObservation()
		db.CommonAcctsCache.shards[index].mu.Unlock()
		if !ok || got.Generation != 1 || got.VoteCapacityEntries != 32 || got.ProgramCapacityCostUnits != 16 || got.CommonLargeCapacityBytes != 5*(2<<20)/8 ||
			got.CommonFixedOccupancyKnown || got.CommonFixedEmptyOccupiedEntries != 0 || got.CommonFixedSmallOccupiedEntries != 0 {
			t.Fatalf("busy shard lost established gauges or leaked partial values: %+v, %v", got, ok)
		}
	}
}
