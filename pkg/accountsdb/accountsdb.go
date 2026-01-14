package accountsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/maypok86/otter"
)

type AccountsDb struct {
	Index            *pebble.DB
	BankHashStore    *pebble.DB
	AcctsDir         string
	LargestFileId    atomic.Uint64
	VoteAcctCache    otter.Cache[solana.PublicKey, *accounts.Account] // Vote accounts (frequently accessed)
	StakeAcctCache   otter.Cache[solana.PublicKey, *accounts.Account] // Stake accounts (small 2k cache)
	SmallAcctCache   otter.Cache[solana.PublicKey, *accounts.Account] // Small accounts ≤512 bytes (50k entries)
	MediumAcctCache  otter.Cache[solana.PublicKey, *accounts.Account] // Medium accounts 512-64KB (20k entries)
	HugeAcctCache    otter.Cache[solana.PublicKey, *accounts.Account] // Huge accounts >64KB (500 entries, mostly programs)
	ProgramCache     otter.Cache[solana.PublicKey, *ProgramCacheEntry]
	InRewardsWindow  bool // When true, only update existing stake cache entries (don't add new ones)

	// Admit-on-second-hit LRU filter for common accounts (small/medium/huge)
	// Only accounts seen twice within the filter window get cached, filtering one-shot reads.
	// nil = disabled (cache everything immediately)
	SeenOnceFilter *otter.Cache[solana.PublicKey, struct{}]
}

// silentLogger implements pebble.Logger but discards all messages.
// This suppresses verbose WAL recovery messages on startup.
type silentLogger struct{}

func (silentLogger) Infof(format string, args ...interface{})  {}
func (silentLogger) Fatalf(format string, args ...interface{}) { log.Fatalf(format, args...) }

var (
	ErrNoAccount = errors.New("ErrNoAccount")
)

// Cache hit/miss counters for profiling
var (
	// Cache hits per cache type
	SmallCacheHits   atomic.Uint64 // Small accounts ≤512 bytes
	MediumCacheHits  atomic.Uint64 // Medium accounts 512-64KB
	HugeCacheHits    atomic.Uint64 // Huge accounts >64KB
	StakeCacheHits   atomic.Uint64
	VoteCacheHits    atomic.Uint64
	ProgramCacheHits atomic.Uint64 // Compiled BPF programs

	// Cache misses per cache type
	SmallCacheMisses   atomic.Uint64 // ≤512 bytes
	MediumCacheMisses  atomic.Uint64 // 512-64KB
	HugeCacheMisses    atomic.Uint64 // >64KB (total)
	StakeCacheMisses   atomic.Uint64
	VoteCacheMisses    atomic.Uint64
	ProgramCacheMisses atomic.Uint64 // Compiled BPF programs

	// Granular miss breakdown within huge range (>64KB)
	HugeMiss64Kto256K atomic.Uint64 // 64KB-256KB
	HugeMiss256Kto1M  atomic.Uint64 // 256KB-1MB
	HugeMissOver1M    atomic.Uint64 // >1MB

	// Program cache miss size breakdown
	ProgramMissUnder1M atomic.Uint64 // <1MB programs
	ProgramMissOver1M  atomic.Uint64 // ≥1MB programs

	// Admit-on-second-hit filter stats
	SeenOnceFiltered atomic.Uint64 // First hit, added to seen-once tracking
	SeenOnceAdmitted atomic.Uint64 // Second hit, admitted to cache
)

// CacheStats holds cache hit/miss counts for reporting
type CacheStats struct {
	SmallHits, MediumHits, HugeHits, StakeHits, VoteHits uint64
	SmallMisses, MediumMisses, HugeMisses                uint64
	StakeMisses, VoteMisses                              uint64
	// Program cache stats
	ProgramHits, ProgramMisses     uint64
	ProgramMissUnder1M             uint64 // <1MB programs
	ProgramMissOver1M              uint64 // ≥1MB programs
	// Granular breakdown within huge range
	HugeMiss64Kto256K uint64 // 64KB-256KB
	HugeMiss256Kto1M  uint64 // 256KB-1MB
	HugeMissOver1M    uint64 // >1MB
	// Admit-on-second-hit stats
	SeenOnceFiltered uint64 // First hits (tracked but not cached)
	SeenOnceAdmitted uint64 // Second hits (admitted to cache)
}

// GetAndResetCacheStats returns current cache hit/miss counts and resets them
func GetAndResetCacheStats() CacheStats {
	return CacheStats{
		SmallHits:          SmallCacheHits.Swap(0),
		MediumHits:         MediumCacheHits.Swap(0),
		HugeHits:           HugeCacheHits.Swap(0),
		StakeHits:          StakeCacheHits.Swap(0),
		VoteHits:           VoteCacheHits.Swap(0),
		ProgramHits:        ProgramCacheHits.Swap(0),
		SmallMisses:        SmallCacheMisses.Swap(0),
		MediumMisses:       MediumCacheMisses.Swap(0),
		HugeMisses:         HugeCacheMisses.Swap(0),
		StakeMisses:        StakeCacheMisses.Swap(0),
		VoteMisses:         VoteCacheMisses.Swap(0),
		ProgramMisses:      ProgramCacheMisses.Swap(0),
		ProgramMissUnder1M: ProgramMissUnder1M.Swap(0),
		ProgramMissOver1M:  ProgramMissOver1M.Swap(0),
		HugeMiss64Kto256K:  HugeMiss64Kto256K.Swap(0),
		HugeMiss256Kto1M:   HugeMiss256Kto1M.Swap(0),
		HugeMissOver1M:     HugeMissOver1M.Swap(0),
		SeenOnceFiltered:   SeenOnceFiltered.Swap(0),
		SeenOnceAdmitted:   SeenOnceAdmitted.Swap(0),
	}
}

// CacheFillStats holds current cache fill levels
type CacheFillStats struct {
	SmallSize, SmallCap     int
	MediumSize, MediumCap   int
	HugeSize, HugeCap       int
	StakeSize, StakeCap     int
	VoteSize, VoteCap       int
	ProgramSize, ProgramCap int
}

// GetCacheFillStats returns current cache fill levels (size/capacity)
func (accountsDb *AccountsDb) GetCacheFillStats() CacheFillStats {
	return CacheFillStats{
		SmallSize:   accountsDb.SmallAcctCache.Size(),
		SmallCap:    accountsDb.SmallAcctCache.Capacity(),
		MediumSize:  accountsDb.MediumAcctCache.Size(),
		MediumCap:   accountsDb.MediumAcctCache.Capacity(),
		HugeSize:    accountsDb.HugeAcctCache.Size(),
		HugeCap:     accountsDb.HugeAcctCache.Capacity(),
		StakeSize:   accountsDb.StakeAcctCache.Size(),
		StakeCap:    accountsDb.StakeAcctCache.Capacity(),
		VoteSize:    accountsDb.VoteAcctCache.Size(),
		VoteCap:     accountsDb.VoteAcctCache.Capacity(),
		ProgramSize: accountsDb.ProgramCache.Size(),
		ProgramCap:  accountsDb.ProgramCache.Capacity(),
	}
}

// recordCacheMiss increments the appropriate cache miss counter based on owner and size
func recordCacheMiss(owner solana.PublicKey, dataLen uint64) {
	if owner == addresses.VoteProgramAddr {
		VoteCacheMisses.Add(1)
	} else if owner == addresses.StakeProgramAddr {
		StakeCacheMisses.Add(1)
	} else if dataLen <= 512 {
		SmallCacheMisses.Add(1)
	} else if dataLen <= 65536 {
		MediumCacheMisses.Add(1)
	} else {
		// Huge: >64KB - track total and granular breakdown
		HugeCacheMisses.Add(1)
		if dataLen <= 262144 { // 64KB-256KB
			HugeMiss64Kto256K.Add(1)
		} else if dataLen <= 1048576 { // 256KB-1MB
			HugeMiss256Kto1M.Add(1)
		} else { // >1MB
			HugeMissOver1M.Add(1)
		}
	}
}

func OpenDb(accountsDbDir string) (*AccountsDb, error) {
	// check for existence of the 'accounts' directory, which holds the appendvecs
	appendVecsDir := fmt.Sprintf("%s/accounts", accountsDbDir)
	_, err := os.Stat(appendVecsDir)
	if err != nil {
		return nil, err
	}

	// attempt to open largest_file_id file
	largestFileIdFn := fmt.Sprintf("%s/largest_file_id", accountsDbDir)
	lfi, err := os.Open(largestFileIdFn)
	if err != nil {
		mlog.Log.Infof("failed to open %s\n", largestFileIdFn)
		return nil, err
	}

	largestFileIdBytes := make([]byte, 8)
	bytesRead, err := lfi.Read(largestFileIdBytes)
	if err != nil {
		mlog.Log.Infof("error reading %s: %s\n", largestFileIdFn, err)
		return nil, err
	} else if bytesRead != 8 {
		mlog.Log.Infof("error reading %s: expected 8 bytes, got %d\n", largestFileIdFn, bytesRead)
		return nil, fmt.Errorf("only got %d bytes", bytesRead)
	}

	largestFileId := binary.LittleEndian.Uint64(largestFileIdBytes)

	indexDir := filepath.Join(accountsDbDir, "mithril_db")
	db, err := pebble.Open(indexDir, &pebble.Options{Logger: silentLogger{}})
	if err != nil {
		return nil, fmt.Errorf("opening indexDir=%s: %w", indexDir, err)
	}

	bankhashDir := filepath.Join(accountsDbDir, "bankhash_db")
	bankhashDb, err := pebble.Open(bankhashDir, &pebble.Options{Logger: silentLogger{}})
	if err != nil {
		return nil, fmt.Errorf("opening bankhashDir=%s: %w", bankhashDir, err)
	}

	accountsDb := &AccountsDb{Index: db, BankHashStore: bankhashDb, AcctsDir: appendVecsDir}
	accountsDb.LargestFileId.Store(largestFileId)

	return accountsDb, nil
}

func (accountsDb *AccountsDb) CloseDb() {
	mlog.Log.Infof("CloseDb: syncing and closing Index...")
	if err := accountsDb.Index.Close(); err != nil {
		mlog.Log.Errorf("CloseDb: Index.Close() error: %v", err)
	}
	mlog.Log.Infof("CloseDb: syncing and closing BankHashStore...")
	if err := accountsDb.BankHashStore.Close(); err != nil {
		mlog.Log.Errorf("CloseDb: BankHashStore.Close() error: %v", err)
	}
	mlog.Log.Infof("CloseDb: done\n") // extra newline for spacing after close
}

// InitCaches initializes the LRU caches with the given sizes.
// Pass 0 for any size to use a reasonable builtin value.
// seenOnceFilterSize controls the admit-on-second-hit LRU filter for common accounts:
//   - 0 = disabled (cache everything immediately, no filtering)
//   - >0 = enable filtering with LRU of this capacity
func (accountsDb *AccountsDb) InitCaches(voteSize, stakeSize, smallSize, mediumSize, hugeSize, programSize int, seenOnceFilterSize int) {
	// Apply builtin values when config not set
	if voteSize <= 0 {
		voteSize = 5000
	}
	if stakeSize <= 0 {
		stakeSize = 2000
	}
	if smallSize <= 0 {
		smallSize = 50000 // 50k small accounts ≤512 bytes
	}
	if mediumSize <= 0 {
		mediumSize = 20000 // 20k medium accounts 512-64KB
	}
	if hugeSize <= 0 {
		hugeSize = 500 // 500 huge accounts >64KB (mostly programs)
	}
	if programSize <= 0 {
		programSize = 5000
	}

	var err error
	accountsDb.VoteAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](voteSize).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.ProgramCache, err = otter.MustBuilder[solana.PublicKey, *ProgramCacheEntry](programSize).
		Cost(func(key solana.PublicKey, progEntry *ProgramCacheEntry) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.SmallAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](smallSize).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.MediumAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](mediumSize).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.HugeAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](hugeSize).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.StakeAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](stakeSize).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	// Initialize admit-on-second-hit LRU filter for common accounts
	if seenOnceFilterSize > 0 {
		// Otter v1 requires capacity >= 10
		if seenOnceFilterSize < 10 {
			seenOnceFilterSize = 10
		}
		filter, err := otter.MustBuilder[solana.PublicKey, struct{}](seenOnceFilterSize).
			Cost(func(key solana.PublicKey, val struct{}) uint32 { return 1 }).
			Build()
		if err != nil {
			panic(err)
		}
		accountsDb.SeenOnceFilter = &filter
		mlog.Log.Infof("AccountsDB caches initialized: vote=%d stake=%d small=%d medium=%d huge=%d program=%d seenOnceFilter=%d",
			voteSize, stakeSize, smallSize, mediumSize, hugeSize, programSize, seenOnceFilterSize)
	} else {
		// SeenOnceFilter stays nil (disabled)
		mlog.Log.Infof("AccountsDB caches initialized: vote=%d stake=%d small=%d medium=%d huge=%d program=%d (seen-once filter disabled)",
			voteSize, stakeSize, smallSize, mediumSize, hugeSize, programSize)
	}
}

type ProgramCacheEntry struct {
	Program        *sbpf.Program
	DeploymentSlot uint64
}

func (accountsDb *AccountsDb) MaybeGetProgramFromCache(pubkey solana.PublicKey) (*ProgramCacheEntry, bool) {
	entry, found := accountsDb.ProgramCache.Get(pubkey)
	if found {
		ProgramCacheHits.Add(1)
	} else {
		ProgramCacheMisses.Add(1)
	}
	return entry, found
}

// RecordProgramCacheMissSize records the size breakdown for a program cache miss.
// Call this after loading the program to track <1MB vs ≥1MB breakdown.
func RecordProgramCacheMissSize(programBytes uint64) {
	if programBytes >= 1024*1024 {
		ProgramMissOver1M.Add(1)
	} else {
		ProgramMissUnder1M.Add(1)
	}
}

func (accountsDb *AccountsDb) AddProgramToCache(pubkey solana.PublicKey, programEntry *ProgramCacheEntry) {
	accountsDb.ProgramCache.Set(pubkey, programEntry)
}

func (accountsDb *AccountsDb) RemoveProgramFromCache(pubkey solana.PublicKey) {
	accountsDb.ProgramCache.Delete(pubkey)
}

// cacheAccount evicts stale entries from other caches, then inserts into the correct
// cache based on owner and size. This prevents stale data when an account changes owner
// (e.g., stake account closed becomes system-owned).
//
// During rewards window (InRewardsWindow=true), stake accounts are only updated if
// already cached - new entries are not added. This prevents cache thrash from the
// ~1.25M one-shot stake account accesses while preserving hot entries.
//
// For common accounts (small/medium/huge), if SeenOnceFilter != nil, uses
// admit-on-second-hit: only caches accounts seen twice within the filter window.
// This filters one-shot reads that would pollute the cache.
func (accountsDb *AccountsDb) cacheAccount(acct *accounts.Account) {
	owner := solana.PublicKeyFromBytes(acct.Owner[:])

	// OSCILLATION FIX: Check if already in a common cache BEFORE deleting
	// This preserves "was hot" information so writes don't oscillate
	wasCommonCached := false
	if owner != addresses.VoteProgramAddr && owner != addresses.StakeProgramAddr {
		// Use GetEntryQuietly to avoid inflating hit stats
		_, wasCommonCached = accountsDb.SmallAcctCache.Extension().GetEntryQuietly(acct.Key)
		if !wasCommonCached {
			_, wasCommonCached = accountsDb.MediumAcctCache.Extension().GetEntryQuietly(acct.Key)
		}
		if !wasCommonCached {
			_, wasCommonCached = accountsDb.HugeAcctCache.Extension().GetEntryQuietly(acct.Key)
		}
	}

	// Delete from all caches (prevents stale data if size tier changes)
	accountsDb.VoteAcctCache.Delete(acct.Key)
	accountsDb.StakeAcctCache.Delete(acct.Key)
	accountsDb.SmallAcctCache.Delete(acct.Key)
	accountsDb.MediumAcctCache.Delete(acct.Key)
	accountsDb.HugeAcctCache.Delete(acct.Key)

	if owner == addresses.VoteProgramAddr {
		accountsDb.VoteAcctCache.Set(acct.Key, acct)
	} else if owner == addresses.StakeProgramAddr {
		// During rewards: only update existing entries, don't add new ones
		if accountsDb.InRewardsWindow {
			if _, exists := accountsDb.StakeAcctCache.Get(acct.Key); exists {
				accountsDb.StakeAcctCache.Set(acct.Key, acct)
			}
		} else {
			accountsDb.StakeAcctCache.Set(acct.Key, acct)
		}
	} else {
		// Common accounts - pass wasCommonCached to bypass filter if already hot
		accountsDb.cacheCommonAccount(acct, wasCommonCached)
	}
}

// cacheCommonAccount handles caching for non-vote, non-stake accounts.
// If force=true or filter disabled, caches immediately.
// Otherwise applies admit-on-second-hit filter.
func (accountsDb *AccountsDb) cacheCommonAccount(acct *accounts.Account, force bool) {
	// Direct cache if filter disabled or account was already cached (force=true)
	if accountsDb.SeenOnceFilter == nil || force {
		accountsDb.cacheCommonDirect(acct)
		// Cleanup from filter if present (account is now hot)
		if accountsDb.SeenOnceFilter != nil {
			accountsDb.SeenOnceFilter.Delete(acct.Key)
		}
		return
	}

	// Admit-on-second-hit: only cache if seen before
	if _, seenBefore := accountsDb.SeenOnceFilter.Get(acct.Key); seenBefore {
		// Second hit - admit to cache
		SeenOnceAdmitted.Add(1)
		accountsDb.SeenOnceFilter.Delete(acct.Key) // Delete AFTER checking
		accountsDb.cacheCommonDirect(acct)
	} else {
		// First hit - track but don't cache
		SeenOnceFiltered.Add(1)
		accountsDb.SeenOnceFilter.Set(acct.Key, struct{}{})
	}
}

// cacheCommonDirect inserts into the appropriate size-tiered cache
func (accountsDb *AccountsDb) cacheCommonDirect(acct *accounts.Account) {
	if len(acct.Data) <= 512 {
		accountsDb.SmallAcctCache.Set(acct.Key, acct)
	} else if len(acct.Data) <= 65536 {
		accountsDb.MediumAcctCache.Set(acct.Key, acct)
	} else {
		accountsDb.HugeAcctCache.Set(acct.Key, acct)
	}
}

func (accountsDb *AccountsDb) GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	cachedAcct, hasAcct := accountsDb.VoteAcctCache.Get(pubkey)
	if hasAcct {
		VoteCacheHits.Add(1)
		return cachedAcct, nil
	}

	cachedAcct, hasAcct = accountsDb.StakeAcctCache.Get(pubkey)
	if hasAcct {
		StakeCacheHits.Add(1)
		return cachedAcct, nil
	}

	cachedAcct, hasAcct = accountsDb.SmallAcctCache.Get(pubkey)
	if hasAcct {
		SmallCacheHits.Add(1)
		return cachedAcct, nil
	}

	cachedAcct, hasAcct = accountsDb.MediumAcctCache.Get(pubkey)
	if hasAcct {
		MediumCacheHits.Add(1)
		return cachedAcct, nil
	}

	cachedAcct, hasAcct = accountsDb.HugeAcctCache.Get(pubkey)
	if hasAcct {
		HugeCacheHits.Add(1)
		return cachedAcct, nil
	}

	acctIdxEntryBytes, c, err := accountsDb.Index.Get(pubkey[:])
	if err != nil {
		//mlog.Log.Debugf("no account found in accountsdb for pubkey %s: %s", pubkey, err)
		return nil, ErrNoAccount
	}

	acctIdxEntry, err := unmarshalAcctIdxEntry(acctIdxEntryBytes)
	if err != nil {
		panic("failed to unmarshal AccountIndexEntry from index kv database")
	}
	c.Close()

	appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, acctIdxEntry.Slot, acctIdxEntry.FileId)

	appendVecFile, err := os.Open(appendVecFileName)
	if err != nil {
		//mlog.Log.Debugf("failed to open appendvec file %s")
		return nil, err
	}
	defer appendVecFile.Close()

	offset, err := appendVecFile.Seek(int64(acctIdxEntry.Offset), 0)
	if err != nil {
		panic(fmt.Sprintf("file seek failed: %s\n", err))
	}
	if offset != int64(acctIdxEntry.Offset) {
		panic(fmt.Sprintf("file seek gave wrong idx (%d)\n", offset))
	}

	acct, err := unmarshalAcctFromAppendVecAcctHeader(appendVecFile)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal account from appendvec file %s: %s", appendVecFileName, err))
	}

	if acct.Key != pubkey {
		panic(fmt.Sprintf("account unmarshaled from appendvec file %s has the wrong pubkey", appendVecFileName))
	}

	acct.Slot = acctIdxEntry.Slot

	// Record cache miss by owner type and size bucket (for profiling)
	recordCacheMiss(solana.PublicKeyFromBytes(acct.Owner[:]), uint64(len(acct.Data)))

	accountsDb.cacheAccount(acct)

	return acct, err
}

func (accountsDb *AccountsDb) StoreAccounts(accts []*accounts.Account, slot uint64) error {
	for _, acct := range accts {
		if acct == nil {
			continue
		}
		acct.Slot = slot
	}

	accountsDb.storeAccountsInternal(accts, slot)

	for _, acct := range accts {
		if acct == nil {
			continue
		}
		accountsDb.cacheAccount(acct)
	}

	return nil
}

func (accountsDb *AccountsDb) storeAccountsInternal(accts []*accounts.Account, slot uint64) {
	fileId := accountsDb.LargestFileId.Add(1)
	appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, slot, fileId)
	appendVecFile, err := os.OpenFile(appendVecFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		//mlog.Log.Debugf("unable to open appendvec file %s for writing to accountsdb", appendVecFileName)
		panic(err)
	}
	defer appendVecFile.Close()

	appendVecAcctsBuf := new(bytes.Buffer)
	writer := new(bytes.Buffer)
	var acctIdxEntryBuf [24]byte

	for _, acct := range accts {
		if acct == nil {
			continue
		}

		// create index entry, encode it and write it to the index kv store
		// offset field is specified as the current num of bytes written to the appendvec buffer.
		writer.Reset()

		indexEntry := AccountIndexEntry{Slot: slot, FileId: fileId, Offset: uint64(appendVecAcctsBuf.Len())}
		indexEntry.Marshal(&acctIdxEntryBuf)

		// if an entry already existed in the index for this account, very often we can simply make the state update
		// in-place, i.e. into the account's existing appendvec blob.
		// we can make the account state update in-place iff the existing version's data length is the same as the
		// new version's data length, which is the case about 98% of the time.
		// if not, then we write out a new appendvec.
		existingacctIdxEntryBuf, c, err := accountsDb.Index.Get(acct.Key[:])
		if err == nil {
			acctIdxEntry, err := unmarshalAcctIdxEntry(existingacctIdxEntryBuf)
			if err != nil {
				panic("failed to unmarshal AccountIndexEntry from index kv database")
			}
			c.Close()

			existingAppendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, acctIdxEntry.Slot, acctIdxEntry.FileId)
			existingAppendVecFile, err := os.OpenFile(existingAppendVecFileName, os.O_RDWR, 0666)
			if err != nil {
				panic(err)
			}

			_, err = existingAppendVecFile.Seek(int64(acctIdxEntry.Offset), 0)
			if err != nil {
				panic(err)
			}

			existingAcct, err := unmarshalAcctFromAppendVecAcctHeader(existingAppendVecFile)
			if err != nil {
				panic(fmt.Sprintf("failed to unmarshal account from appendvec file %s: %s", existingAppendVecFileName, err))
			}

			if len(acct.Data) == len(existingAcct.Data) {
				newAppendVecAcct := AppendVecAccount{DataLen: uint64(len(acct.Data)), Pubkey: acct.Key, Lamports: acct.Lamports,
					RentEpoch: acct.RentEpoch, Owner: acct.Owner, Executable: acct.Executable, Data: acct.Data}

				_, err = existingAppendVecFile.Seek(int64(acctIdxEntry.Offset), 0)
				if err != nil {
					panic(err)
				}

				err = newAppendVecAcct.Marshal(existingAppendVecFile)
				if err != nil {
					panic(fmt.Sprintf("error marshaling appendvec for storage: %s", err))
				}

				existingAppendVecFile.Close()
				continue
			}
		}

		err = accountsDb.Index.Set(acct.Key[:], acctIdxEntryBuf[:], &pebble.WriteOptions{})
		if err != nil {
			panic(fmt.Sprintf("unable to add acct for %s to acctsdb: %v", acct.Key, err))
		}

		// marshal up the account as an appendvec style account and write it to the buffer
		appendVecAcct := AppendVecAccount{DataLen: uint64(len(acct.Data)), Pubkey: acct.Key, Lamports: acct.Lamports,
			RentEpoch: acct.RentEpoch, Owner: acct.Owner, Executable: acct.Executable, Data: acct.Data}

		err = appendVecAcct.Marshal(appendVecAcctsBuf)
		if err != nil {
			panic(fmt.Sprintf("unable to add acct for %s to acctsdb: %v", acct.Key, err))
		}
	}

	// write the appendvecs data into the file
	_, err = appendVecFile.Write(appendVecAcctsBuf.Bytes())
	if err != nil {
		panic(err)
	}
}

func (accountsDb *AccountsDb) GetBankHashForSlot(slot uint64) ([]byte, error) {
	var slotBytes [8]byte
	binary.LittleEndian.PutUint64(slotBytes[:], slot)
	bh, c, err := accountsDb.BankHashStore.Get(slotBytes[:])
	if err != nil {
		return nil, fmt.Errorf("GetBankHashForSlot slot=%d: %w", slot, err)
	}
	out := make([]byte, len(bh))
	copy(out, bh)
	c.Close()
	return out, nil
}

func (accountsDb *AccountsDb) StoreBankHashForSlot(slot uint64, bankHash []byte) error {
	var slotBytes [8]byte
	binary.LittleEndian.PutUint64(slotBytes[:], slot)
	return accountsDb.BankHashStore.Set(slotBytes[:], bankHash, &pebble.WriteOptions{})
}

func (accountsDb *AccountsDb) KeysBetweenPrefixes(startPrefix uint64, endPrefix uint64) []solana.PublicKey {
	return nil
	/*keys := accountsDb.IndexDb.KeysBetweenPrefixes(startPrefix, endPrefix)

	keyObjs := make([]solana.PublicKey, 0)
	for _, key := range keys {
		keyObject := solana.PublicKeyFromBytes(key)
		keyObjs = append(keyObjs, keyObject)
	}

	return keyObjs*/
}

func (accountsDb *AccountsDb) AllKeys() [][]byte {
	return nil
}
