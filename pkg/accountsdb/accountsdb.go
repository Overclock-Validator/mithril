package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
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
	VoteAcctCache    otter.Cache[solana.PublicKey, *accounts.Account]
	CommonAcctsCache otter.Cache[solana.PublicKey, *accounts.Account]
	ProgramCache     otter.Cache[solana.PublicKey, *ProgramCacheEntry]
	// Otter permits concurrent ordinary operations but not Clear. Rewind takes
	// the write side; hot-path program cache operations take the read side.
	programCacheMu sync.RWMutex

	// readCacheEpochMu protects the cache epoch and the pending fold view.
	// CommitBatch publishes its immutable newest-wins union here before the
	// Pebble index commit. Readers use that view for changed keys while the
	// index and caches catch up, so the expensive durable commit does not hold
	// this mutex and old snapshot reads still cannot publish stale cache bytes.
	readCacheEpochMu sync.RWMutex
	readCacheEpoch   uint64
	pendingFold      map[[32]byte]dedupedVersion
	commonAdmission  *commonCacheAdmission
	batchHooks       batchReadTestHooks

	// appendVecReadMu pins appendvec paths from before an index snapshot until
	// its file reads complete. Compaction/rewind take the write side across an
	// index move plus source unlink, so a batch never falls through to a
	// different logical epoch.
	appendVecReadMu sync.RWMutex

	// MinFreeBytes leaves operational headroom before admitting a fold. Set
	// before replay starts; zero disables the admission check for offline tools.
	MinFreeBytes uint64

	// IndexWALDisabled runs the Pebble index without a WAL; the fold manifests
	// are the index redo log (recovery replays the contiguous manifest run
	// above the committed fold meta). Off by default until soaked.
	IndexWALDisabled bool

	// Batch-fold state (segment.go/fold.go/recovery.go). foldMu serializes
	// CommitBatch, recovery, rewind, and compaction.
	foldMu           sync.Mutex
	lastBatchSeq     uint64        // guarded by foldMu; seeded by RecoverFoldState
	durableThrough   atomic.Uint64 // observability: highest durably folded slot
	foldHooks        foldTestHooks // test-only crash injection
	compactCursor    string        // guarded by foldMu; scan resume point across CompactOnce cycles
	sparseCursor     string        // independent cursor so bootstrap scanning cannot starve large segments
	resparseNext     bool          // alternate new and previously sparse segments, even with one-file cycles
	compactScanSeq   uint64
	compactScanned   map[uint64]struct{} // successful sparse scans at compactScanSeq; reset when the head changes
	compactScanClock uint64
	compactLastScan  map[uint64]uint64 // oldest scan first; a growing directory must not postpone cursor wrap forever
	compactCycles    uint64            // periodically serve bootstrap and mostly-empty sparse files too
	compactHooks     compactionTestHooks
}

// silentLogger implements pebble.Logger but discards all messages.
// This suppresses verbose WAL recovery messages on startup.
type silentLogger struct{}

func (silentLogger) Infof(format string, args ...any)  {}
func (silentLogger) Fatalf(format string, args ...any) { log.Fatalf(format, args...) }

var (
	ErrNoAccount = errors.New("ErrNoAccount")

	ProgramCacheMaxMB       = DefaultProgramCacheMaxMB
	CommonAccountCacheMaxMB = DefaultCommonAccountCacheMaxMB
)

const (
	indexPebbleMemTableSize                = 64 << 20
	indexPebbleMemTableStopWritesThreshold = 4
	DefaultCommonAccountCacheMaxMB         = 256
	DefaultProgramCacheMaxMB               = 1024
	programCacheCostUnitBytes              = 1 << 20
	commonAccountCacheEntryOverheadBytes   = 256
)

// DisableIndexWAL (storage.index_wal=false) runs the account index without a
// Pebble WAL: the fold manifests are the index redo log, and recovery replays
// the contiguous manifest run above the committed fold meta. Set before
// OpenDb. Default false (WAL on) until soaked.
var DisableIndexWAL bool

func NewAccountsIndexPebbleOptions(logger pebble.Logger) *pebble.Options {
	return &pebble.Options{
		Logger:                      logger,
		MemTableSize:                indexPebbleMemTableSize,
		MemTableStopWritesThreshold: indexPebbleMemTableStopWritesThreshold,
		DisableWAL:                  DisableIndexWAL,
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
	db, err := pebble.Open(indexDir, NewAccountsIndexPebbleOptions(silentLogger{}))
	if err != nil {
		return nil, fmt.Errorf("opening indexDir=%s: %w", indexDir, err)
	}

	bankhashDir := filepath.Join(accountsDbDir, "bankhash_db")
	bankhashDb, err := pebble.Open(bankhashDir, &pebble.Options{Logger: silentLogger{}})
	if err != nil {
		return nil, fmt.Errorf("opening bankhashDir=%s: %w", bankhashDir, err)
	}

	accountsDb := &AccountsDb{
		IndexWALDisabled: DisableIndexWAL, Index: db, BankHashStore: bankhashDb, AcctsDir: appendVecsDir}
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

func (accountsDb *AccountsDb) InitCaches() {
	var err error
	accountsDb.commonAdmission = newCommonCacheAdmission()
	accountsDb.VoteAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](2500).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.ProgramCache, err = otter.MustBuilder[solana.PublicKey, *ProgramCacheEntry](programCacheCapacityUnits()).
		Cost(func(key solana.PublicKey, progEntry *ProgramCacheEntry) uint32 {
			return progEntry.CostUnits()
		}).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.CommonAcctsCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](commonAccountCacheCapacityBytes()).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 {
			return commonAccountCacheCost(acct)
		}).
		Build()
	if err != nil {
		panic(err)
	}
}

type ProgramCacheEntry struct {
	Program        *sbpf.Program
	DeploymentSlot uint64
	// Executables are reusable across banks only for identical source and loader
	// features. Slot alone is not a version: competing forks can deploy at the
	// same slot. Fields are immutable after cache publication.
	sourceBytes    []byte
	sourceBound    bool
	sourceFeatures features.Features
}

// BindSource must be called before publishing the entry; published bindings
// must never be mutated, including when another bank replaces the cache key.
func (entry *ProgramCacheEntry) BindSource(source []byte, f *features.Features) {
	entry.sourceBytes = bytes.Clone(source)
	entry.sourceBound = true
	entry.sourceFeatures = *f.Clone()
}

func (entry *ProgramCacheEntry) MatchesSource(source []byte, f *features.Features) bool {
	return entry != nil && entry.sourceBound && bytes.Equal(entry.sourceBytes, source) && maps.Equal(entry.sourceFeatures, *f)
}

func programCacheCapacityUnits() int {
	if ProgramCacheMaxMB <= 0 {
		return DefaultProgramCacheMaxMB
	}
	return ProgramCacheMaxMB
}

func commonAccountCacheCapacityBytes() int {
	maxMB := CommonAccountCacheMaxMB
	if maxMB <= 0 {
		maxMB = DefaultCommonAccountCacheMaxMB
	}
	maxInt := int(^uint(0) >> 1)
	if maxMB > maxInt/(1<<20) {
		return maxInt
	}
	return maxMB << 20
}

func commonAccountCacheCost(acct *accounts.Account) uint32 {
	bytes := uint64(commonAccountCacheEntryOverheadBytes)
	if acct != nil {
		bytes += uint64(len(acct.Data))
	}
	if bytes > uint64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(bytes)
}

func (entry *ProgramCacheEntry) CostUnits() uint32 {
	if entry == nil || entry.Program == nil {
		return 1
	}
	bytes := entry.Program.MemoryBytes() + uint64(len(entry.sourceBytes))
	units := (bytes + programCacheCostUnitBytes - 1) / programCacheCostUnitBytes
	if units == 0 {
		return 1
	}
	max := uint64(^uint32(0))
	if units > max {
		return ^uint32(0)
	}
	return uint32(units)
}

func (accountsDb *AccountsDb) MaybeGetProgramFromCache(pubkey solana.PublicKey) (*ProgramCacheEntry, bool) {
	if accountsDb == nil {
		return nil, false
	}
	accountsDb.programCacheMu.RLock()
	defer accountsDb.programCacheMu.RUnlock()
	return accountsDb.ProgramCache.Get(pubkey)
}

func (accountsDb *AccountsDb) AddProgramToCache(pubkey solana.PublicKey, programEntry *ProgramCacheEntry) {
	accountsDb.programCacheMu.RLock()
	defer accountsDb.programCacheMu.RUnlock()
	accountsDb.ProgramCache.Set(pubkey, programEntry)
}

func (accountsDb *AccountsDb) RemoveProgramFromCache(pubkey solana.PublicKey) {
	accountsDb.programCacheMu.RLock()
	defer accountsDb.programCacheMu.RUnlock()
	accountsDb.ProgramCache.Delete(pubkey)
}

// AccountReadStats is one single-key read's exact wall-time decomposition.
// The sysvar loader records these separately from its decode/update work so a
// fold publication wait cannot hide inside the broad SysvarUpdates timer.
type AccountReadStats struct {
	WorkingSetLookupNanoseconds      uint64
	CloneNanoseconds                 uint64
	AppendVecPinWaitNanoseconds      uint64
	ReadCacheEpochWaitNanoseconds    uint64
	CacheLookupNanoseconds           uint64
	IndexAndAppendVecReadNanoseconds uint64
	CachePublicationWaitNanoseconds  uint64
	CachePublicationNanoseconds      uint64
	WorkingSetHit                    bool
	PendingFoldHit                   bool
	CacheHit                         bool
	DurableRead                      bool
	CachePublicationEpochRejected    bool
}

func (accountsDb *AccountsDb) GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if accountsDb == nil {
		return nil, ErrNoAccount
	}
	accountsDb.appendVecReadMu.RLock()
	defer accountsDb.appendVecReadMu.RUnlock()
	return accountsDb.getStoredAccountPinned(slot, pubkey)
}

func (accountsDb *AccountsDb) GetAccountWithStats(slot uint64, pubkey solana.PublicKey) (*accounts.Account, AccountReadStats, error) {
	var stats AccountReadStats
	if accountsDb == nil {
		return nil, stats, ErrNoAccount
	}
	start := time.Now()
	accountsDb.appendVecReadMu.RLock()
	stats.AppendVecPinWaitNanoseconds = uint64(time.Since(start).Nanoseconds())
	defer accountsDb.appendVecReadMu.RUnlock()
	acct, err := accountsDb.getStoredAccountPinnedWithStats(slot, pubkey, &stats)
	return acct, stats, err
}

func (accountsDb *AccountsDb) getStoredAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	accountsDb.appendVecReadMu.RLock()
	defer accountsDb.appendVecReadMu.RUnlock()
	return accountsDb.getStoredAccountPinned(slot, pubkey)
}

// getStoredAccountPinned requires appendVecReadMu to be held for reading.
func (accountsDb *AccountsDb) getStoredAccountPinned(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if accountsDb.Index == nil {
		return nil, ErrNoAccount
	}
	r := trace.StartRegion(context.Background(), "GetStoredAccountCache")
	cachedAcct, hasAcct, cacheEpoch := accountsDb.getCachedAccountAndEpoch(pubkey)
	if hasAcct {
		r.End()
		return cachedAcct, nil
	}
	r.End()

	defer trace.StartRegion(context.Background(), "GetStoredAccountDisk").End()
	acct, err := accountsDb.readIndexedAccount(pubkey)
	if err == ErrNoAccount {
		return nil, ErrNoAccount
	}
	if err != nil {
		if acct, err = accountsDb.readIndexedAccount(pubkey); err != nil {
			if err == ErrNoAccount {
				return nil, ErrNoAccount
			}
			return nil, fmt.Errorf("accountsdb: read %s failed after retry: %w", pubkey, err)
		}
	}
	accountsDb.cacheReadAccount(pubkey, acct, cacheEpoch)
	return acct, nil
}

func (accountsDb *AccountsDb) getStoredAccountPinnedWithStats(slot uint64, pubkey solana.PublicKey, stats *AccountReadStats) (*accounts.Account, error) {
	if accountsDb.Index == nil {
		return nil, ErrNoAccount
	}
	r := trace.StartRegion(context.Background(), "GetStoredAccountCache")
	waitStart := time.Now()
	accountsDb.readCacheEpochMu.RLock()
	stats.ReadCacheEpochWaitNanoseconds += uint64(time.Since(waitStart).Nanoseconds())
	cacheStart := time.Now()
	cachedAcct, hasAcct, pending := accountsDb.getCachedAccountLocked(pubkey)
	cacheEpoch := accountsDb.readCacheEpoch
	accountsDb.readCacheEpochMu.RUnlock()
	stats.CacheLookupNanoseconds += uint64(time.Since(cacheStart).Nanoseconds())
	if hasAcct {
		stats.PendingFoldHit = pending
		stats.CacheHit = !pending
		r.End()
		return cachedAcct, nil
	}
	r.End()

	defer trace.StartRegion(context.Background(), "GetStoredAccountDisk").End()
	stats.DurableRead = true
	readStart := time.Now()

	// One-shot retry preserves the single-account path's existing tolerance for
	// an externally removed or stale index location. The appendvec reader pin
	// prevents in-process compaction and legacy stores from racing this read.
	acct, err := accountsDb.readIndexedAccount(pubkey)
	if err == ErrNoAccount {
		return nil, ErrNoAccount
	}
	if err != nil {
		if acct, err = accountsDb.readIndexedAccount(pubkey); err != nil {
			if err == ErrNoAccount {
				return nil, ErrNoAccount
			}
			return nil, fmt.Errorf("accountsdb: read %s failed after retry: %w", pubkey, err)
		}
	}
	stats.IndexAndAppendVecReadNanoseconds += uint64(time.Since(readStart).Nanoseconds())

	publicationStart := time.Now()
	waitNanoseconds, rejected := accountsDb.cacheReadAccountWithStats(pubkey, acct, cacheEpoch)
	stats.CachePublicationNanoseconds += uint64(time.Since(publicationStart).Nanoseconds())
	stats.CachePublicationWaitNanoseconds += waitNanoseconds
	stats.CachePublicationEpochRejected = rejected

	return acct, nil
}

func (accountsDb *AccountsDb) getCachedAccount(pubkey solana.PublicKey) (*accounts.Account, bool) {
	accountsDb.readCacheEpochMu.RLock()
	defer accountsDb.readCacheEpochMu.RUnlock()
	acct, ok, _ := accountsDb.getCachedAccountLocked(pubkey)
	return acct, ok
}

func (accountsDb *AccountsDb) getCachedAccountAndEpoch(pubkey solana.PublicKey) (*accounts.Account, bool, uint64) {
	accountsDb.readCacheEpochMu.RLock()
	defer accountsDb.readCacheEpochMu.RUnlock()
	acct, ok, _ := accountsDb.getCachedAccountLocked(pubkey)
	return acct, ok, accountsDb.readCacheEpoch
}

// getCachedAccountLocked requires readCacheEpochMu to be held for reading or
// writing. Keeping a whole batch's cache probes under one epoch makes its
// cache results coherent with the Pebble snapshot created at that boundary.
func (accountsDb *AccountsDb) getCachedAccountLocked(pubkey solana.PublicKey) (*accounts.Account, bool, bool) {
	if version, ok := accountsDb.pendingFold[[32]byte(pubkey)]; ok {
		return version.acct, true, true
	}
	if acct, ok := accountsDb.VoteAcctCache.Get(pubkey); ok {
		return acct, true, false
	}
	acct, ok := accountsDb.CommonAcctsCache.Get(pubkey)
	return acct, ok, false
}

func (accountsDb *AccountsDb) cacheReadAccount(pubkey solana.PublicKey, acct *accounts.Account, expectedEpoch uint64) {
	accountsDb.readCacheEpochMu.RLock()
	defer accountsDb.readCacheEpochMu.RUnlock()
	accountsDb.cacheReadAccountLocked(pubkey, acct, expectedEpoch)
}

func (accountsDb *AccountsDb) cacheReadAccountWithStats(pubkey solana.PublicKey, acct *accounts.Account, expectedEpoch uint64) (uint64, bool) {
	waitStart := time.Now()
	accountsDb.readCacheEpochMu.RLock()
	waitNanoseconds := uint64(time.Since(waitStart).Nanoseconds())
	defer accountsDb.readCacheEpochMu.RUnlock()
	return waitNanoseconds, !accountsDb.cacheReadAccountLocked(pubkey, acct, expectedEpoch)
}

func (accountsDb *AccountsDb) cacheReadAccountLocked(pubkey solana.PublicKey, acct *accounts.Account, expectedEpoch uint64) bool {
	if expectedEpoch != accountsDb.readCacheEpoch || accountsDb.pendingFoldContainsLocked(pubkey) {
		return false
	}
	if solana.PublicKeyFromBytes(acct.Owner[:]) == addresses.VoteProgramAddr {
		accountsDb.VoteAcctCache.Set(pubkey, acct)
	} else {
		accountsDb.CommonAcctsCache.Set(pubkey, acct)
	}
	return true
}

type batchCacheAdmission uint8

const (
	batchCacheNone batchCacheAdmission = iota
	batchCacheCommonSkipped
	batchCacheVoteSkipped
	batchCacheEpochRejected
	batchCacheCommon
	batchCacheVote
)

// cacheBatchReadAccount publishes a decoded value only if the authoritative
// index/cache epoch captured with its batch snapshot is still current. Large
// common-account scans are filtered by commonCacheAdmission before this call;
// vote accounts retain their dedicated unconditional policy.
func (accountsDb *AccountsDb) cacheBatchReadAccount(
	pubkey solana.PublicKey,
	acct *accounts.Account,
	admitCommon bool,
	expectedEpoch uint64,
) (batchCacheAdmission, uint64) {
	if hook := accountsDb.batchHooks.beforeCacheAdmission; hook != nil {
		hook(pubkey)
	}
	waitStart := time.Now()
	accountsDb.readCacheEpochMu.RLock()
	waitNanoseconds := uint64(time.Since(waitStart).Nanoseconds())
	defer accountsDb.readCacheEpochMu.RUnlock()
	if expectedEpoch != accountsDb.readCacheEpoch || accountsDb.pendingFoldContainsLocked(pubkey) {
		return batchCacheEpochRejected, waitNanoseconds
	}
	if solana.PublicKeyFromBytes(acct.Owner[:]) == addresses.VoteProgramAddr {
		if accountsDb.VoteAcctCache.Set(pubkey, acct) {
			return batchCacheVote, waitNanoseconds
		}
		return batchCacheVoteSkipped, waitNanoseconds
	}
	if admitCommon && accountsDb.CommonAcctsCache.Set(pubkey, acct) {
		return batchCacheCommon, waitNanoseconds
	}
	return batchCacheCommonSkipped, waitNanoseconds
}

func (accountsDb *AccountsDb) pendingFoldContainsLocked(pubkey solana.PublicKey) bool {
	_, ok := accountsDb.pendingFold[[32]byte(pubkey)]
	return ok
}

// readIndexedAccount performs one index-fetch + file-read attempt.
func (accountsDb *AccountsDb) readIndexedAccount(pubkey solana.PublicKey) (*accounts.Account, error) {
	acctIdxEntryBytes, c, err := accountsDb.Index.Get(pubkey[:])
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNoAccount
		}
		return nil, fmt.Errorf("index get: %w", err)
	}

	acctIdxEntry, err := UnmarshalAcctIdxEntry(acctIdxEntryBytes)
	c.Close()
	if err != nil {
		return nil, fmt.Errorf("unmarshal index entry: %w", err)
	}

	appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, acctIdxEntry.Slot, acctIdxEntry.FileId)

	appendVecFile, err := os.Open(appendVecFileName)
	if err != nil {
		return nil, err
	}
	defer appendVecFile.Close()

	if _, err := appendVecFile.Seek(int64(acctIdxEntry.Offset), 0); err != nil {
		return nil, fmt.Errorf("seek %s@%d: %w", appendVecFileName, acctIdxEntry.Offset, err)
	}

	acct, err := unmarshalAcctFromAppendVecAcctHeader(appendVecFile)
	if err != nil {
		return nil, fmt.Errorf("unmarshal account at %s@%d: %w", appendVecFileName, acctIdxEntry.Offset, err)
	}
	if acct.Key != pubkey {
		return nil, fmt.Errorf("record at %s@%d holds %s (stale index entry)", appendVecFileName, acctIdxEntry.Offset, acct.Key)
	}

	acct.Slot = acctIdxEntry.Slot
	return acct, nil
}

// refreshReadCacheEntries keeps already-hot common entries coherent after a
// fold, but does not admit every account in a large fold: doing so turns the
// retained byte budget into an expensive write-only churn loop. Vote accounts
// retain their dedicated cache. Deleted and owner-transitioned entries are
// evicted from the cache that can no longer serve them.
//
// It uses only concurrent-safe ordinary Otter operations.
// CommitBatch may call it without readCacheEpochMu while pendingFold masks every
// changed key; Clear remains confined to resetReadCachesLocked.
func (accountsDb *AccountsDb) refreshReadCacheEntries(accts []*accounts.Account) {
	for _, acct := range accts {
		if acct == nil {
			continue
		}
		if acct.Lamports == 0 {
			accountsDb.CommonAcctsCache.Delete(acct.Key)
			accountsDb.VoteAcctCache.Delete(acct.Key)
		} else if solana.PublicKeyFromBytes(acct.Owner[:]) == addresses.VoteProgramAddr {
			accountsDb.CommonAcctsCache.Delete(acct.Key)
			accountsDb.VoteAcctCache.Set(acct.Key, acct)
		} else {
			accountsDb.VoteAcctCache.Delete(acct.Key)
			if accountsDb.CommonAcctsCache.Has(acct.Key) {
				if !accountsDb.CommonAcctsCache.Set(acct.Key, acct) {
					// A weighted entry may outgrow Otter's per-item ceiling.
					// Never retain the prior value when replacement is rejected.
					accountsDb.CommonAcctsCache.Delete(acct.Key)
				}
			}
		}
	}
}

// resetReadCachesLocked requires readCacheEpochMu for writing.
func (accountsDb *AccountsDb) resetReadCachesLocked() {
	accountsDb.CommonAcctsCache.Clear()
	accountsDb.VoteAcctCache.Clear()
	accountsDb.programCacheMu.Lock()
	accountsDb.ProgramCache.Clear()
	accountsDb.programCacheMu.Unlock()
	accountsDb.commonAdmission = newCommonCacheAdmission()
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
