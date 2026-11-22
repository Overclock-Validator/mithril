package accountsdb

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
)

func activityTestDB(t *testing.T, fixed bool) *AccountsDb {
	t.Helper()
	db := &AccountsDb{
		VoteAcctCache:        newWeightedCache(32, func(solana.PublicKey, *accounts.Account) uint32 { return 1 }, true),
		ProgramCache:         newWeightedCache(32, func(solana.PublicKey, *ProgramCacheEntry) uint32 { return 1 }, true),
		CommonAcctsCache:     newCommonAccountCacheWithActivity(2<<20, fixed, true),
		cacheActivityEnabled: true, cacheActivityInstance: 1,
	}
	db.cacheObservationGeneration.Store(1)
	stopObservationCaches(t, db)
	return db
}

func TestCacheActivityRecordingAndClear(t *testing.T) {
	for _, fixed := range []bool{false, true} {
		db := activityTestDB(t, fixed)
		key := solana.PublicKey{1}
		db.VoteAcctCache.Get(key)
		db.VoteAcctCache.Set(key, &accounts.Account{Key: key})
		db.VoteAcctCache.Get(key)
		db.VoteAcctCache.Has(key)
		db.ProgramCache.Set(key, &ProgramCacheEntry{})
		db.ProgramCache.Get(key)
		for _, size := range []int{0, 200, 201} {
			k := solana.PublicKey{byte(size + 2)}
			db.CommonAcctsCache.Set(k, &accounts.Account{Key: k, Data: make([]byte, size)})
			db.CommonAcctsCache.Get(k)
			db.CommonAcctsCache.Has(k)
		}
		db.CommonAcctsCache.Get(solana.PublicKey{255})
		before, ok := db.CacheActivity()
		wantLargeHits := uint64(3)
		if fixed {
			wantLargeHits = 1
		}
		if !ok || before.Instance != 1 || before.Generation != 1 || !before.FixedKnown ||
			before.Vote.Hits != 1 || before.Vote.Misses != 1 || before.Vote.Evictions != 0 ||
			before.Program.Hits != 1 || before.CommonLarge.Hits != wantLargeHits || before.CommonLarge.Misses != 1 {
			t.Fatalf("lookup counts: fixed=%v, %+v, known=%v", fixed, before, ok)
		}
		if fixed && (before.FixedEmptyHits != 1 || before.FixedSmallHits != 1) {
			t.Fatalf("fixed hits lost: %+v", before)
		}
		db.readCacheEpochMu.Lock()
		db.resetReadCachesLocked()
		db.readCacheEpochMu.Unlock()
		after, ok := db.CacheActivity()
		before.Generation = 2
		before.ProgramInsertionWeight.ApproximateWeight = 0
		before.CommonLargeInsertionWeight.ApproximateWeight = 0
		if !ok || after != before {
			t.Fatalf("Clear reset activity: got %+v, want %+v", after, before)
		}
	}
}

func TestCacheActivityDefaultAndLifetime(t *testing.T) {
	oldProgram, oldCommon, oldLayout := ProgramCacheMaxMB, CommonAccountCacheMaxMB, CommonAccountCacheLayout
	ProgramCacheMaxMB, CommonAccountCacheMaxMB, CommonAccountCacheLayout = 1, 1, "flat"
	defer func() {
		ProgramCacheMaxMB, CommonAccountCacheMaxMB, CommonAccountCacheLayout = oldProgram, oldCommon, oldLayout
	}()
	db := &AccountsDb{}
	db.InitCaches()
	stopObservationCaches(t, db)
	if _, ok := db.CacheActivity(); ok || db.VoteAcctCache.cache.IsRecordingStats() {
		t.Fatal("default enabled recording")
	}
	db.InitCachesWithActivity(true)
	stopObservationCaches(t, db)
	first, ok := db.CacheActivity()
	if !ok || first.Instance != 2 || first.Generation != 2 || !first.InsertionWeightsRecorded ||
		!first.ProgramInsertionWeight.KnownApproximate || first.ProgramInsertionWeight.ApproximateWeight != 0 ||
		!first.CommonLargeInsertionWeight.KnownApproximate || first.CommonLargeInsertionWeight.ApproximateWeight != 0 {
		t.Fatalf("enabled: %+v, %v", first, ok)
	}
	oldPressure := db.ProgramCache.pressure
	oldPressure.faulted.Store(true)
	db.InitCaches()
	stopObservationCaches(t, db)
	oldPressure.addPrepared(99)
	second, ok := db.CacheActivity()
	if !ok || second.Instance != 3 || second.Generation != 3 || second.ProgramInsertionWeight.Faulted ||
		!second.ProgramInsertionWeight.KnownApproximate || second.ProgramInsertionWeight.ApproximateWeight != 0 {
		t.Fatalf("reconstruction: %+v, %v", second, ok)
	}
	db.InitCachesWithActivity(false)
	stopObservationCaches(t, db)
	if _, ok := db.CacheActivity(); ok {
		t.Fatal("disabled activity still available")
	}
}

func TestCacheActivityFixedLimits(t *testing.T) {
	db := activityTestDB(t, true)
	c := db.CommonAcctsCache
	// Use one set so replacing the fifth distinct entry has one known victim.
	c.shards[0].empty = make([]commonTierHeader, commonTierWays)
	var keys []solana.PublicKey
	for n := uint64(0); len(keys) < 5; n++ {
		key := solana.PublicKey{byte(n), byte(n >> 8)}
		s, _ := c.locate(key)
		if s == &c.shards[0] {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		c.Set(key, &accounts.Account{Key: key})
	}
	c.Set(keys[4], &accounts.Account{Key: keys[4]})
	c.Delete(keys[4])
	o, ok := db.CacheActivity()
	if !ok || !o.FixedKnown || o.FixedEmptyReplacements != 1 {
		t.Fatalf("capacity replacements: %+v", o)
	}
	c.shards[0].mu.Lock()
	blocked, ok := db.CacheActivity()
	c.shards[0].mu.Unlock()
	if !ok || blocked.FixedKnown || blocked.FixedEmptyReplacements != 0 {
		t.Fatal("busy fixed shard fabricated a total")
	}
	c.shards[0].activity[0], c.shards[1].activity[0] = math.MaxUint64, 1
	if o, ok := db.CacheActivity(); !ok || o.FixedKnown {
		t.Fatal("aggregate overflow was published")
	}
	c.shards[0].recordActivity(0)
	if !c.shards[0].activitySaturated || c.shards[0].activity[0] != math.MaxUint64 {
		t.Fatal("counter wrapped")
	}
}
