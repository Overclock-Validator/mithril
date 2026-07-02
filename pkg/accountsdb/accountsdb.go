package accountsdb

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/maypok86/otter"
	"golang.org/x/sync/errgroup"
)

type AccountsDb struct {
	Index            *pebble.DB
	BankHashStore    *pebble.DB
	AcctsDir         string // primary shard's accounts dir; its parent is the metadata dir
	Shards           *Shards
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
	// index move plus source unlink, and the legacy mutable store takes it while
	// writing, so a batch never falls through to a different logical epoch.
	appendVecReadMu sync.RWMutex

	// RootedDurable keeps the canonical store rooted-only: replayed slots buffer
	// in an in-RAM working set (pkg/accounts) and fold to disk via CommitBatch
	// once finalized+verified. The Alpenglow node forces this on at startup.
	RootedDurable bool

	// IndexWALDisabled runs the Pebble index without a WAL; the fold manifests
	// are the index redo log (recovery replays the contiguous manifest run
	// above the committed fold meta). Off by default until soaked.
	IndexWALDisabled bool

	// Batch-fold state (segment.go/fold.go/recovery.go). foldMu serializes
	// CommitBatch, recovery, rewind, and compaction.
	foldMu         sync.Mutex
	lastBatchSeq   uint64        // guarded by foldMu; seeded by RecoverFoldState
	durableThrough atomic.Uint64 // observability: highest durably folded slot
	foldHooks      foldTestHooks // test-only crash injection
	compactCursor  string        // guarded by foldMu; scan resume point across CompactOnce cycles

	// A list of store requests. They are added to the back as they arrive and
	// removed from the front as they are persisted.
	inProgressStoreRequestsMu sync.Mutex
	inProgressStoreRequests   *list.List
	storeRequestChan          chan *list.Element
	storeWorkerDone           chan struct{}
}

type storeRequest struct {
	accts []*accounts.Account
	slot  uint64
	m     map[solana.PublicKey]*accounts.Account
	cb    func()
}

func (accountsDb *AccountsDb) StoreQueueLen() int {
	if accountsDb.inProgressStoreRequests == nil {
		return 0
	}
	accountsDb.inProgressStoreRequestsMu.Lock()
	defer accountsDb.inProgressStoreRequestsMu.Unlock()
	return accountsDb.inProgressStoreRequests.Len()
}

// silentLogger implements pebble.Logger but discards all messages.
// This suppresses verbose WAL recovery messages on startup.
type silentLogger struct{}

func (silentLogger) Infof(format string, args ...any)  {}
func (silentLogger) Fatalf(format string, args ...any) { log.Fatalf(format, args...) }

var (
	ErrNoAccount = errors.New("ErrNoAccount")

	StoreAccountsWorkers    = 128
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
	return OpenDbPaths([]string{accountsDbDir})
}

func OpenDbPaths(accountsPaths []string) (*AccountsDb, error) {
	if len(accountsPaths) == 0 {
		return nil, fmt.Errorf("OpenDb: no accounts paths configured")
	}
	// The first path holds all metadata (index, manifest, state); every path
	// holds an "accounts" dir with that disk's shard data.
	accountsDbDir := accountsPaths[0]

	// num_shards records the shard count the DB was built with.
	b, err := os.ReadFile(filepath.Join(accountsDbDir, "num_shards"))
	legacy := os.IsNotExist(err) && len(accountsPaths) == 1
	if legacy {
		b = make([]byte, 8)
		binary.LittleEndian.PutUint64(b, 1)
		err = nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading num_shards: %w", err)
	}
	if len(b) != 8 {
		return nil, fmt.Errorf("num_shards: expected 8 bytes, got %d", len(b))
	}
	numShards := int(binary.LittleEndian.Uint64(b))
	if numShards != len(accountsPaths) {
		return nil, fmt.Errorf("configured %d accounts dir(s) but AccountsDB was built with %d shard(s); rebuild required", len(accountsPaths), numShards)
	}

	shardDirs := make([]string, len(accountsPaths))
	for i, p := range accountsPaths {
		shardDirs[i] = filepath.Join(p, "accounts")
	}

	// check for existence of the primary 'accounts' directory, which holds the appendvecs
	appendVecsDir := shardDirs[0]
	if _, err := os.Stat(appendVecsDir); err != nil {
		return nil, err
	}

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
	if !legacy {
		accountsDb.Shards = newShards(shardDirs)
	}
	largestBytes, err := os.ReadFile(filepath.Join(accountsDbDir, "largest_file_id"))
	if err != nil || len(largestBytes) != 8 {
		bankhashDb.Close()
		db.Close()
		return nil, fmt.Errorf("reading largest_file_id: expected 8 bytes: %v", err)
	}
	accountsDb.LargestFileId.Store(binary.LittleEndian.Uint64(largestBytes))

	accountsDb.inProgressStoreRequests = list.New()
	accountsDb.storeRequestChan = make(chan *list.Element)
	accountsDb.storeWorkerDone = make(chan struct{})
	go accountsDb.storeWorker()

	return accountsDb, nil
}

// Turns down the store worker. AccountsDb cannot accept writes after this.
// Should not be called concurrently.
func (accountsDb *AccountsDb) WaitForStoreWorker() {
	if accountsDb.storeWorkerDone == nil {
		mlog.Log.Infof("AccountsDb: async store worker already done.")
		return
	}
	mlog.Log.Infof("AccountsDb: waiting for async store worker...")
	close(accountsDb.storeRequestChan)
	<-accountsDb.storeWorkerDone
	accountsDb.storeWorkerDone = nil
}

func (accountsDb *AccountsDb) CloseDb() {
	accountsDb.WaitForStoreWorker()
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
	if accountsDb.inProgressStoreRequests == nil {
		accountsDb.inProgressStoreRequests = list.New()
	}
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
	bytes := entry.Program.MemoryBytes()
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
	InProgressNanoseconds            uint64
	ReadCacheEpochWaitNanoseconds    uint64
	CacheLookupNanoseconds           uint64
	IndexAndAppendVecReadNanoseconds uint64
	CachePublicationWaitNanoseconds  uint64
	CachePublicationNanoseconds      uint64
	WorkingSetHit                    bool
	InProgressHit                    bool
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
	accts := accountsDb.getStoreInProgressAccounts([]solana.PublicKey{pubkey})
	if accts[0] != nil {
		return accts[0], nil
	}
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
	start = time.Now()
	accts := accountsDb.getStoreInProgressAccounts([]solana.PublicKey{pubkey})
	stats.InProgressNanoseconds = uint64(time.Since(start).Nanoseconds())
	if accts[0] != nil {
		stats.InProgressHit = true
		return accts[0], stats, nil
	}
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

	appendVecFileName := accountsDb.appendVecPath(acctIdxEntry.Slot, acctIdxEntry.FileId)

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

// Returns a slice of the same length as the input with results matching indexes, nil if not found.
// Returns clones to avoid data races with the store worker.
func (accountsDb *AccountsDb) getStoreInProgressAccounts(pks []solana.PublicKey) []*accounts.Account {
	defer trace.StartRegion(context.Background(), "getStoreInProgressAccounts").End()
	out := make([]*accounts.Account, len(pks))
	accountsDb.inProgressStoreRequestsMu.Lock()
	defer accountsDb.inProgressStoreRequestsMu.Unlock()
	// Start with newest first.
	for e := accountsDb.inProgressStoreRequests.Back(); e != nil; e = e.Prev() {
		sr := e.Value.(storeRequest)
		for i := range len(pks) {
			if out[i] != nil {
				continue // Already found.
			}
			if acct := sr.m[pks[i]]; acct != nil {
				out[i] = acct.Clone()
			}
		}
	}
	return out
}

func (accountsDb *AccountsDb) StoreAccounts(
	accts []*accounts.Account,
	slot uint64,
	cb func(),
) error {
	// Rooted-durable (the only mode of the Alpenglow-only build): direct stores
	// are no-ops. Every write reaches disk exclusively via the fold path
	// (CommitBatch) once finalized+verified — epoch-boundary code that still
	// calls StoreAccounts directly is redundant with the slot delta it also
	// feeds (block.EpochUpdatedAccts), and writing here would violate
	// "durable state is rooted-only".
	if accountsDb.RootedDurable {
		if cb != nil {
			cb()
		}
		return nil
	}
	for _, acct := range accts {
		if acct == nil {
			continue
		}
		acct.Slot = slot
	}

	m := make(map[solana.PublicKey]*accounts.Account, len(accts))
	for _, a := range accts {
		if a == nil {
			continue
		}
		m[a.Key] = a
	}
	// Must not hold lock during channel send to avoid deadlock with storeWorker.
	accountsDb.appendVecReadMu.Lock()
	accountsDb.inProgressStoreRequestsMu.Lock()
	element := accountsDb.inProgressStoreRequests.PushBack(storeRequest{accts: accts, slot: slot, m: m, cb: cb})
	accountsDb.inProgressStoreRequestsMu.Unlock()
	accountsDb.appendVecReadMu.Unlock()
	accountsDb.storeRequestChan <- element
	return nil
}

func (accountsDb *AccountsDb) storeAccountsSync(accts []*accounts.Account, slot uint64) {
	defer trace.StartRegion(context.Background(), "StoreAccounts").End()
	// The request is still present in the in-progress overlay, so publish its
	// cache epoch before disk I/O. New readers see the overlay; older readers
	// cannot publish stale values after this bump.
	accountsDb.refreshReadCaches(accts)
	accountsDb.appendVecReadMu.Lock()
	defer accountsDb.appendVecReadMu.Unlock()
	if StoreAccountsWorkers == 1 {
		accountsDb.storeAccountsInternal(accts, slot)
	} else {
		accountsDb.parallelStoreAccounts(StoreAccountsWorkers, accts, slot)
	}
}

// refreshReadCaches keeps already-hot common entries coherent after a store,
// but does not admit every account in a large fold: doing so turns the retained
// byte budget into an expensive write-only churn loop. Vote accounts retain their
// dedicated cache. Deleted and owner-transitioned entries are evicted from the
// cache that can no longer serve them.
func (accountsDb *AccountsDb) refreshReadCaches(accts []*accounts.Account) {
	accountsDb.readCacheEpochMu.Lock()
	defer accountsDb.readCacheEpochMu.Unlock()
	accountsDb.readCacheEpoch++
	accountsDb.refreshReadCacheEntries(accts)
}

// refreshReadCacheEntries uses only concurrent-safe ordinary Otter operations.
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

func (accountsDb *AccountsDb) storeWorker() {
	defer close(accountsDb.storeWorkerDone)
	for elt := range accountsDb.storeRequestChan {
		sr := elt.Value.(storeRequest)
		accountsDb.storeAccountsSync(sr.accts, sr.slot)
		if sr.cb != nil {
			sr.cb()
		}
		// Remove after callback so DrainStoreQueue waits for callbacks (e.g. index flush) to complete
		accountsDb.inProgressStoreRequestsMu.Lock()
		accountsDb.inProgressStoreRequests.Remove(elt)
		accountsDb.inProgressStoreRequestsMu.Unlock()
	}
}

func (accountsDb *AccountsDb) storeAccountsInternal(accts []*accounts.Account, slot uint64) {
	fileId := accountsDb.nextFileId(accountsDb.chooseShard())
	appendVecFileName := accountsDb.appendVecPath(slot, fileId)
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
			acctIdxEntry, err := UnmarshalAcctIdxEntry(existingacctIdxEntryBuf)
			if err != nil {
				panic("failed to unmarshal AccountIndexEntry from index kv database")
			}
			c.Close()

			existingAppendVecFileName := accountsDb.appendVecPath(acctIdxEntry.Slot, acctIdxEntry.FileId)
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

// parallelStoreAccounts makes n workers which process a list of
// accounts in parallel. One worker receives accounts to add to a new
// appendvec file. The remaining workers do the following:
// for each account they receive:
// 1. Check the existing accounts length
// 2. If the length of the new account data is the same, overwrite the existing account
// 3. Otherwise, pass it on to be added to a new appendvec.
func (accountsDb *AccountsDb) parallelStoreAccounts(n int, accts []*accounts.Account, slot uint64) {
	if n < 2 {
		panic(fmt.Sprintf("AccountsDb.parallelStoreAccounts: n=%d must be >= 2", n))
	}

	acctsChan := make(chan *accounts.Account, len(accts))
	for i := range len(accts) {
		if accts[i] == nil {
			continue
		}
		acctsChan <- accts[i]
	}
	close(acctsChan)

	// Assumes that none of the accounts overlap in the same appendvec file.
	lengthChangedAccounts := make(chan *accounts.Account)
	overwriteOrPassGroup, ctx := errgroup.WithContext(context.Background())
	for range n - 1 {
		overwriteOrPassGroup.Go(func() error {
			for acct := range acctsChan {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				err := func(a *accounts.Account) error {
					existingacctIdxEntryBuf, c, err := accountsDb.Index.Get(a.Key[:])
					if errors.Is(err, pebble.ErrNotFound) {
						lengthChangedAccounts <- a
						return nil
					}
					if err != nil {
						return fmt.Errorf("reading from index: %w", err)
					}
					existingIdxEntry, err := UnmarshalAcctIdxEntry(existingacctIdxEntryBuf)
					c.Close()
					if err != nil {
						return fmt.Errorf("unmarshaling index entry: %w", err)
					}

					existingAppendVecFileName := accountsDb.appendVecPath(existingIdxEntry.Slot, existingIdxEntry.FileId)
					existingAppendVecFile, err := os.OpenFile(existingAppendVecFileName, os.O_RDWR, 0666)
					if err != nil {
						return fmt.Errorf("open %s: %w", existingAppendVecFileName, err)
					}
					defer existingAppendVecFile.Close()

					existingDataLen, err := GetAppendVecDataLen(existingAppendVecFile, existingIdxEntry.Offset)
					if err != nil {
						return fmt.Errorf("GetAppendVecDataLen %s: %w", existingAppendVecFileName, err)
					}

					if uint64(len(a.Data)) != existingDataLen {
						lengthChangedAccounts <- a
						return nil
					}

					_, err = existingAppendVecFile.Seek(int64(existingIdxEntry.Offset), 0)
					if err != nil {
						return fmt.Errorf("seek %s %d: %w", existingAppendVecFileName, existingIdxEntry.Offset, err)
					}
					newAppendVecAcct := AppendVecAccount{
						DataLen:    uint64(len(a.Data)),
						Pubkey:     a.Key,
						Lamports:   a.Lamports,
						RentEpoch:  a.RentEpoch,
						Owner:      a.Owner,
						Executable: a.Executable,
						Data:       a.Data,
					}
					err = newAppendVecAcct.Marshal(existingAppendVecFile)
					if err != nil {
						return fmt.Errorf("marshaling appendvec: %w", err)
					}
					return nil
				}(acct)
				if err != nil {
					return fmt.Errorf("reading account key=%s: %w", acct.Key.String(), err)
				}
			}
			return nil
		})
	}
	newAppendVecGroup := errgroup.Group{}
	newAppendVecGroup.Go(func() error {
		fileId := accountsDb.nextFileId(accountsDb.chooseShard())
		appendVecFileName := accountsDb.appendVecPath(slot, fileId)
		appendVecFile, err := os.OpenFile(appendVecFileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		defer appendVecFile.Close()
		appendVecWriter := bufio.NewWriter(appendVecFile)
		defer appendVecWriter.Flush()

		appendVecFileOffset := uint64(0)
		var acctIdxEntryBuf [24]byte

		for acct := range lengthChangedAccounts {
			indexEntry := AccountIndexEntry{Slot: slot, FileId: fileId, Offset: appendVecFileOffset}
			indexEntry.Marshal(&acctIdxEntryBuf)
			err = accountsDb.Index.Set(acct.Key[:], acctIdxEntryBuf[:], &pebble.WriteOptions{})
			if err != nil {
				return fmt.Errorf("unable to add acct for %s to acctsdb: %v", acct.Key, err)
			}

			appendVecAcct := AppendVecAccount{
				DataLen:    uint64(len(acct.Data)),
				Pubkey:     acct.Key,
				Lamports:   acct.Lamports,
				RentEpoch:  acct.RentEpoch,
				Owner:      acct.Owner,
				Executable: acct.Executable,
				Data:       acct.Data,
			}
			l, err := appendVecAcct.MarshalReturningLength(appendVecWriter)
			if err != nil {
				return fmt.Errorf("unable to add acct for %s to acctsdb: %v", acct.Key, err)
			}
			appendVecFileOffset += uint64(l)
		}

		return nil
	})

	e1 := overwriteOrPassGroup.Wait()
	close(lengthChangedAccounts)
	e2 := newAppendVecGroup.Wait()
	if err := errors.Join(e1, e2); err != nil {
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
