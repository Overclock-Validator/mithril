package accountsdb

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime/trace"
	"sort"
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
	// ProductionIndex is the V2 sharded StreamHash index used by snapshot-built
	// and resumed nodes. Index/BaseIndex remain only as the transitional V1
	// in-process representation used by older fixtures and offline tooling.
	ProductionIndex *ProductionAccountIndex
	// Index is the exact journaled RAM head. BaseIndex is the immutable
	// StreamHash snapshot index. The head always takes precedence over the
	// base; its on-disk journal is never consulted by a lookup.
	Index         *MutableAccountIndex
	BaseIndex     *StreamAccountIndex
	BankHashStore *pebble.DB
	AcctsDir      string
	LargestFileId atomic.Uint64
	// fileIDMu serializes the global durable allocator. fileIDFatal fences all
	// later allocations after a selector rename with an uncertain durability
	// outcome; only restart/reconciliation may resolve that state.
	fileIDMu             sync.Mutex
	fileIDFatal          error
	publishLargestFileID largestFileIDPublisher
	VoteAcctCache        otter.Cache[solana.PublicKey, *accounts.Account]
	CommonAcctsCache     otter.Cache[solana.PublicKey, *accounts.Account]
	ProgramCache         otter.Cache[solana.PublicKey, *ProgramCacheEntry]
	// Otter permits concurrent ordinary operations but not Clear. Rewind takes
	// the write side; hot-path program cache operations take the read side.
	programCacheMu sync.RWMutex

	// readCacheEpochMu protects the cache epoch and the pending fold view.
	// CommitBatch publishes its immutable newest-wins union here before the
	// journaled index commit. Readers use that view for changed keys while the
	// index and caches catch up, so the expensive durable commit does not hold
	// this mutex and old snapshot reads still cannot publish stale cache bytes.
	readCacheEpochMu sync.RWMutex
	readCacheEpoch   uint64
	pendingFold      map[[32]byte]dedupedVersion
	commonAdmission  *commonCacheAdmission
	batchHooks       batchReadTestHooks
	// Test-only scheduling hook between candidate lookup and file verification.
	afterAccountIndexLookup func()

	// appendVecReadMu pins appendvec paths from before an index snapshot until
	// its file reads complete. Compaction/rewind take the write side across an
	// index move plus source unlink, and the legacy mutable store takes it while
	// writing, so a batch never falls through to a different logical epoch.
	appendVecReadMu sync.RWMutex
	// accountIndexWriteMu keeps a logical multi-frame recovery/rewind/fold
	// transaction contiguous with respect to every other index writer. Point
	// readers remain lock-free; live oversized folds publish pendingFold first.
	accountIndexWriteMu sync.Mutex

	// RootedDurable keeps the canonical store rooted-only: replayed slots buffer
	// in an in-RAM working set (pkg/accounts) and fold to disk via CommitBatch
	// once finalized+verified. The Alpenglow node forces this on at startup.
	RootedDurable bool

	// Batch-fold state (segment.go/fold.go/recovery.go). foldMu serializes
	// CommitBatch, recovery, rewind, and compaction.
	foldMu         sync.Mutex
	lastBatchSeq   uint64        // guarded by foldMu; seeded by RecoverFoldState
	durableThrough atomic.Uint64 // observability: highest durably folded slot
	foldCommits    atomic.Uint64 // successful live logical fold commits
	foldWALFrames  atomic.Uint64 // physical account-index frames in those commits
	foldOversized  atomic.Uint64 // live folds requiring more than one frame
	foldMaxKeys    atomic.Uint64 // high-water union size for a live fold
	foldHooks      foldTestHooks // test-only crash injection
	compactCursor  string        // guarded by foldMu; scan resume point across CompactOnce cycles
	// foldDiskAdmission is installed once, before replay starts. It reserves
	// enough filesystem headroom for one complete fold before CommitBatch takes
	// foldMu or performs any durable side effect. The returned release function
	// keeps concurrent admissions and pressure compaction serialized through the
	// end of the fold.
	foldDiskAdmissionMu sync.RWMutex
	foldDiskAdmission   FoldDiskAdmission
	// diskSpaceProbe is overridden only by deterministic package tests.
	diskSpaceProbe func(string) (AppendVecFilesystemSpace, error)
	// foldFatal is set only after a fold manifest may have crossed its durable
	// commit point without completing. The process must restart so recovery can
	// resolve the on-disk decision before another BatchSeq is allocated.
	foldFatal error // guarded by foldMu

	// A list of store requests. They are added to the back as they arrive and
	// removed from the front as they are persisted.
	inProgressStoreRequestsMu sync.Mutex
	inProgressStoreRequests   *list.List
	storeRequestChan          chan *list.Element
	storeWorkerDone           chan struct{}

	// shutdownMu makes teardown retryable without double-closing resources.
	// In particular, a timed-out V2 generation drain leaves the production
	// index and its exclusive store lock live for a later Shutdown call.
	shutdownMu              sync.Mutex
	shutdownErr             error
	productionIndexClosed   bool
	transitionalIndexClosed bool
	baseIndexClosed         bool
	bankHashStoreClosed     bool
	// storeLock is populated only for a successfully opened transitional V1
	// store. V2 ownership lives in ProductionIndex. Either way, AccountsDb
	// keeps the store-wide exclusion until every sidecar has closed.
	storeLock       *productionAccountIndexStoreLock
	storeLockClosed bool
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

type accountIndexSource uint8

const (
	accountIndexSourceNone accountIndexSource = iota
	accountIndexSourceDelta
	accountIndexSourceBase
)

const (
	DefaultCommonAccountCacheMaxMB       = 256
	DefaultProgramCacheMaxMB             = 1024
	programCacheCostUnitBytes            = 1 << 20
	commonAccountCacheEntryOverheadBytes = 256
)

func OpenDb(accountsDbDir string) (*AccountsDb, error) {
	config, err := CurrentProductionAccountIndexConfig()
	if err != nil {
		return nil, err
	}
	return OpenDbWithProductionAccountIndexConfig(accountsDbDir, config)
}

// OpenDbWithStoreGuard is the guarded counterpart used by snapshot
// bootstrap. It resolves the current production configuration and transfers
// the already-held exclusive guard into the opened V2 index.
func OpenDbWithStoreGuard(
	accountsDbDir string,
	guard *ProductionAccountIndexStoreGuard,
) (*AccountsDb, error) {
	config, err := CurrentProductionAccountIndexConfig()
	if err != nil {
		return nil, err
	}
	return OpenDbWithProductionAccountIndexConfigAndStoreGuard(accountsDbDir, config, guard)
}

// OpenDbWithPreparedSnapshotAccountIndexAndStoreGuard adopts the immutable
// generation retained by snapshot verification while continuously holding the
// same exclusive store guard. It performs every ordinary AccountsDB preflight
// and opens all sidecars, but does not rehash/revalidate the immutable payload
// bytes a second time.
func OpenDbWithPreparedSnapshotAccountIndexAndStoreGuard(
	accountsDbDir string,
	prepared *PreparedSnapshotAccountIndex,
	guard *ProductionAccountIndexStoreGuard,
) (*AccountsDb, error) {
	if guard == nil {
		return nil, errors.New("accountsdb: nil production account-index store guard")
	}
	config, err := prepared.productionConfig()
	if err != nil {
		return nil, err
	}
	var accountsDB *AccountsDb
	err = guard.transferProductionAccountIndexStoreLockOnSuccess(
		accountsDbDir,
		func(storeLock *productionAccountIndexStoreLock) error {
			return prepared.adopt(
				accountsDbDir,
				config,
				storeLock,
				func(catalog *RootIndexCatalog, immutable *ShardedImmutableIndex) (bool, error) {
					consumed := false
					var openErr error
					accountsDB, openErr = openDbWithProductionAccountIndexConfigUsingPrepared(
						accountsDbDir,
						config,
						storeLock,
						true,
						catalog,
						immutable,
						&consumed,
					)
					return consumed, openErr
				},
			)
		},
	)
	return accountsDB, err
}

// OpenDbWithProductionAccountIndexConfig is the explicit-config form used by
// tests, embedded callers and tooling. Production node startup normally uses
// OpenDb after binding its flags into CurrentProductionAccountIndexConfig.
func OpenDbWithProductionAccountIndexConfig(
	accountsDbDir string,
	productionConfig ProductionAccountIndexConfig,
) (_ *AccountsDb, retErr error) {
	// The store lock must precede every store-derived read, including the
	// appendvec high-water reconciliation. Otherwise an opener can observe a
	// stale high-water mark while a prior owner is still publishing or closing.
	guard, err := AcquireExclusiveProductionAccountIndexStore(accountsDbDir)
	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = errors.Join(retErr, guard.Close())
	}()
	return openDbWithProductionAccountIndexConfigAndStoreGuard(
		accountsDbDir, productionConfig, guard, false,
	)
}

// OpenDbWithProductionAccountIndexConfigAndStoreGuard completes a snapshot
// bootstrap without releasing exclusive store ownership. The guard transfers
// into the opened V2 index and becomes an idempotently closed shell.
func OpenDbWithProductionAccountIndexConfigAndStoreGuard(
	accountsDbDir string,
	productionConfig ProductionAccountIndexConfig,
	guard *ProductionAccountIndexStoreGuard,
) (*AccountsDb, error) {
	return openDbWithProductionAccountIndexConfigAndStoreGuard(
		accountsDbDir, productionConfig, guard, true,
	)
}

func openDbWithProductionAccountIndexConfigAndStoreGuard(
	accountsDbDir string,
	productionConfig ProductionAccountIndexConfig,
	guard *ProductionAccountIndexStoreGuard,
	requireProductionV2 bool,
) (*AccountsDb, error) {
	if guard == nil {
		return nil, errors.New("accountsdb: nil production account-index store guard")
	}
	var accountsDB *AccountsDb
	err := guard.transferProductionAccountIndexStoreLockOnSuccess(
		accountsDbDir,
		func(storeLock *productionAccountIndexStoreLock) error {
			var openErr error
			accountsDB, openErr = openDbWithProductionAccountIndexConfig(
				accountsDbDir, productionConfig, storeLock, requireProductionV2,
			)
			return openErr
		},
	)
	return accountsDB, err
}

func openDbWithProductionAccountIndexConfig(
	accountsDbDir string,
	productionConfig ProductionAccountIndexConfig,
	heldStoreLock *productionAccountIndexStoreLock,
	requireProductionV2 bool,
) (*AccountsDb, error) {
	return openDbWithProductionAccountIndexConfigUsingPrepared(
		accountsDbDir,
		productionConfig,
		heldStoreLock,
		requireProductionV2,
		nil,
		nil,
		nil,
	)
}

func openDbWithProductionAccountIndexConfigUsingPrepared(
	accountsDbDir string,
	productionConfig ProductionAccountIndexConfig,
	heldStoreLock *productionAccountIndexStoreLock,
	requireProductionV2 bool,
	preparedCatalog *RootIndexCatalog,
	preparedImmutable *ShardedImmutableIndex,
	preparedConsumed *bool,
) (*AccountsDb, error) {
	if preparedConsumed != nil {
		*preparedConsumed = false
	}
	if heldStoreLock == nil {
		return nil, errors.New("accountsdb: opening a store requires an exclusive production account-index lock")
	}
	if err := productionConfig.Validate(); err != nil {
		return nil, err
	}
	// check for existence of the 'accounts' directory, which holds the appendvecs
	appendVecsDir := fmt.Sprintf("%s/accounts", accountsDbDir)
	_, err := os.Stat(appendVecsDir)
	if err != nil {
		return nil, err
	}

	// Validate both the checksummed selector and its global uniqueness relation
	// to every canonical appendvec/segment before any new ID can be allocated.
	largestFileId, err := ValidateLargestFileID(accountsDbDir)
	if err != nil {
		return nil, err
	}

	// Reject incompatible or incomplete stores before hashing a potentially
	// multi-gigabyte immutable base.
	legacyIndexDir := filepath.Join(accountsDbDir, "mithril_db")
	if _, legacyErr := os.Stat(legacyIndexDir); legacyErr == nil {
		return nil, fmt.Errorf("accountsdb: legacy Pebble account index %s is not supported by the Pebble-free format; bootstrap from a fresh snapshot", legacyIndexDir)
	} else if !os.IsNotExist(legacyErr) {
		return nil, fmt.Errorf("accountsdb: inspect legacy account index %s: %w", legacyIndexDir, legacyErr)
	}
	var productionIndex *ProductionAccountIndex
	var index *MutableAccountIndex
	var baseIndex *StreamAccountIndex
	rootPath := filepath.Join(accountsDbDir, RootIndexCatalogFileName)
	if _, rootErr := os.Lstat(rootPath); rootErr == nil {
		verifyStarted := time.Now()
		if preparedImmutable != nil {
			if preparedCatalog == nil || preparedConsumed == nil {
				return nil, errors.New("accountsdb: incomplete prepared snapshot adoption state")
			}
			// Ownership transfers on entry to the runtime constructor. It closes
			// the immutable generation on every failure, so the prepared handle
			// must become permanently consumed from this point onward.
			*preparedConsumed = true
			productionIndex, err = openProductionAccountIndexFromImmutableWithStoreLock(
				accountsDbDir,
				productionConfig.withCheckpointBudgetDefaults(),
				heldStoreLock,
				preparedCatalog,
				preparedImmutable,
				nil,
			)
		} else {
			productionIndex, err = openProductionAccountIndexWithStoreLock(
				accountsDbDir,
				productionConfig.withCheckpointBudgetDefaults(),
				heldStoreLock,
				false,
			)
		}
		if err != nil {
			return nil, fmt.Errorf("opening production account index: %w", err)
		}
		if preparedImmutable != nil {
			mlog.Log.Infof("Adopted verified sharded production StreamHash account index in %s", time.Since(verifyStarted))
		} else {
			mlog.Log.Infof("Verified and opened sharded production StreamHash account index in %s", time.Since(verifyStarted))
		}
	} else if !errors.Is(rootErr, os.ErrNotExist) {
		return nil, fmt.Errorf("accountsdb: inspect production root index %s: %w", rootPath, rootErr)
	} else {
		if requireProductionV2 {
			return nil, fmt.Errorf("%w: guarded bootstrap produced no V2 root", ErrAccountIndexMigrationRequired)
		}
		// Keep the Pebble-free V1 format readable for offline tooling and older
		// unit fixtures. The node's state validator requires V2, so production
		// resume cannot silently remain on this bounded global overlay.
		journalPath := filepath.Join(accountsDbDir, DeltaIndexJournalFileName)
		if _, journalErr := os.Stat(journalPath); journalErr != nil {
			return nil, fmt.Errorf("%w: no V2 root and legacy journal %s is unavailable: %v", ErrAccountIndexMigrationRequired, journalPath, journalErr)
		}
		if err := validateStreamIndexManifestArtifacts(accountsDbDir); err != nil {
			return nil, err
		}
		basePath := filepath.Join(accountsDbDir, StreamIndexFileName)
		if _, statErr := os.Stat(basePath); statErr == nil {
			verifyStarted := time.Now()
			baseIndex, err = OpenStreamAccountIndex(basePath)
			if err != nil {
				return nil, fmt.Errorf("opening StreamHash account index %s: %w", basePath, err)
			}
			mlog.Log.Infof("Verified and opened transitional V1 StreamHash account index in %s", time.Since(verifyStarted))
		} else if !os.IsNotExist(statErr) {
			return nil, fmt.Errorf("stat StreamHash account index %s: %w", basePath, statErr)
		}
		index, err = OpenMutableAccountIndex(accountsDbDir)
		if err != nil {
			var cleanupErr error
			if baseIndex != nil {
				cleanupErr = baseIndex.Close()
			}
			return nil, errors.Join(err, cleanupErr)
		}
	}

	bankhashDir := filepath.Join(accountsDbDir, "bankhash_db")
	bankhashDb, err := pebble.Open(bankhashDir, &pebble.Options{Logger: silentLogger{}})
	if err != nil {
		var cleanupErr error
		if productionIndex != nil {
			if heldStoreLock != nil {
				// The outer guard still owns this borrowed lock until the
				// complete AccountsDb open succeeds. Detach it before tearing
				// down the partially opened index on a later sidecar failure.
				productionIndex.storeLock = nil
			}
			cleanupErr = errors.Join(cleanupErr, productionIndex.Close())
		}
		if baseIndex != nil {
			cleanupErr = errors.Join(cleanupErr, baseIndex.Close())
		}
		if index != nil {
			cleanupErr = errors.Join(cleanupErr, index.Close())
		}
		return nil, errors.Join(fmt.Errorf("opening bankhashDir=%s: %w", bankhashDir, err), cleanupErr)
	}

	accountsDb := &AccountsDb{
		ProductionIndex: productionIndex,
		Index:           index, BaseIndex: baseIndex, BankHashStore: bankhashDb, AcctsDir: appendVecsDir}
	if productionIndex == nil {
		accountsDb.storeLock = heldStoreLock
	}
	accountsDb.LargestFileId.Store(largestFileId)

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

// Shutdown closes AccountsDB and reports every durability/resource teardown
// failure. A V2 immutable-generation drain observes ctx; if it times out, the
// production store lock remains held and a later call resumes the same drain.
// Shutdown must not run concurrently with account operations.
func (accountsDb *AccountsDb) Shutdown(ctx context.Context) error {
	if accountsDb == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("accountsdb: nil shutdown context")
	}
	accountsDb.shutdownMu.Lock()
	defer accountsDb.shutdownMu.Unlock()

	accountsDb.WaitForStoreWorker()
	mlog.Log.Infof("AccountsDb.Shutdown: syncing and closing index...")
	if accountsDb.Index != nil && !accountsDb.transitionalIndexClosed {
		if err := accountsDb.Index.Close(); err != nil {
			accountsDb.shutdownErr = errors.Join(
				accountsDb.shutdownErr,
				fmt.Errorf("accountsdb: close transitional mutable account index: %w", err),
			)
		}
		accountsDb.transitionalIndexClosed = true
	}
	if accountsDb.BaseIndex != nil && !accountsDb.baseIndexClosed {
		mlog.Log.Infof("AccountsDb.Shutdown: closing transitional immutable StreamHash index...")
		if err := accountsDb.BaseIndex.Close(); err != nil {
			accountsDb.shutdownErr = errors.Join(
				accountsDb.shutdownErr,
				fmt.Errorf("accountsdb: close transitional immutable account index: %w", err),
			)
		}
		accountsDb.baseIndexClosed = true
	}
	if accountsDb.BankHashStore != nil && !accountsDb.bankHashStoreClosed {
		mlog.Log.Infof("AccountsDb.Shutdown: syncing and closing bankhash store...")
		if err := accountsDb.BankHashStore.Close(); err != nil {
			accountsDb.shutdownErr = errors.Join(
				accountsDb.shutdownErr,
				fmt.Errorf("accountsdb: close bankhash store: %w", err),
			)
		}
		accountsDb.bankHashStoreClosed = true
	}
	// The production index owns the store-wide flock, so it must shut down
	// last. No other process may open the store while a sidecar is still being
	// synced or closed.
	if accountsDb.ProductionIndex != nil && !accountsDb.productionIndexClosed {
		err := accountsDb.ProductionIndex.Shutdown(ctx)
		if !accountsDb.ProductionIndex.shutdownComplete() {
			// A context timeout is not a permanent close error. The production
			// index continues its drain while retaining the store lock, and the
			// caller can supply a fresh context to retry.
			return errors.Join(
				accountsDb.shutdownErr,
				fmt.Errorf("accountsdb: drain production account index: %w", err),
			)
		}
		// If ctx cancellation raced the shutdownDone close, read the stable
		// terminal result rather than persisting a transient DeadlineExceeded.
		err = accountsDb.ProductionIndex.Shutdown(context.Background())
		if err != nil {
			accountsDb.shutdownErr = errors.Join(
				accountsDb.shutdownErr,
				fmt.Errorf("accountsdb: shut down production account index: %w", err),
			)
		}
		accountsDb.productionIndexClosed = true
	}
	if accountsDb.storeLock != nil && !accountsDb.storeLockClosed {
		if err := accountsDb.storeLock.Close(); err != nil {
			accountsDb.shutdownErr = errors.Join(
				accountsDb.shutdownErr,
				fmt.Errorf("accountsdb: release transitional account-index store lock: %w", err),
			)
		}
		accountsDb.storeLockClosed = true
	}
	mlog.Log.Infof("AccountsDb.Shutdown: done\n") // extra newline for spacing after close
	return accountsDb.shutdownErr
}

// CloseDb preserves the existing unbounded convenience API while returning
// teardown errors to callers that choose to check them.
func (accountsDb *AccountsDb) CloseDb() error {
	err := accountsDb.Shutdown(context.Background())
	if err != nil {
		mlog.Log.Errorf("CloseDb: %v", err)
	}
	return err
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
	if !accountsDb.hasAccountIndex() {
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
	if !accountsDb.hasAccountIndex() {
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
// cache results coherent with the mutable-index snapshot at that boundary.
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

// lookupAccountIndexCandidatePinned resolves the exact mutable head first. An exact
// tombstone stops resolution. Only an absent delta probes the immutable base,
// whose fingerprint-backed result remains a candidate until its appendvec
// pubkey is checked. The caller must close any returned generation pin after
// that verification, including error paths.
func (accountsDb *AccountsDb) lookupAccountIndexCandidatePinned(pubkey solana.PublicKey) (AccountIndexEntry, accountIndexSource, bool, *IndexReadView, error) {
	if accountsDb.ProductionIndex != nil {
		entry, source, found, pin, err := accountsDb.ProductionIndex.lookupCandidatePinned(pubkey)
		if err != nil {
			return AccountIndexEntry{}, accountIndexSourceNone, false, nil, fmt.Errorf("production index lookup %s: %w", pubkey, err)
		}
		return entry, source, found, pin, nil
	}
	if accountsDb.Index != nil {
		value, ok, err := accountsDb.Index.LookupWithError(pubkey)
		if err != nil {
			return AccountIndexEntry{}, accountIndexSourceNone, false, nil, fmt.Errorf("mutable index lookup %s: %w", pubkey, err)
		}
		if ok {
			if value.Tombstone {
				return AccountIndexEntry{}, accountIndexSourceNone, false, nil, nil
			}
			return value.Entry, accountIndexSourceDelta, true, nil, nil
		}
	}
	if accountsDb.BaseIndex == nil {
		return AccountIndexEntry{}, accountIndexSourceNone, false, nil, nil
	}
	entry, found, err := accountsDb.BaseIndex.LookupCandidate(pubkey)
	if err != nil {
		return AccountIndexEntry{}, accountIndexSourceNone, false, nil, fmt.Errorf("base index lookup %s: %w", pubkey, err)
	}
	if !found {
		return AccountIndexEntry{}, accountIndexSourceNone, false, nil, nil
	}
	return entry, accountIndexSourceBase, true, nil, nil
}

func (accountsDb *AccountsDb) hasAccountIndex() bool {
	return accountsDb != nil && (accountsDb.ProductionIndex != nil || accountsDb.Index != nil || accountsDb.BaseIndex != nil)
}

func (accountsDb *AccountsDb) applyAccountIndexMutations(
	mutations []deltaIndexMutation,
	meta *foldMeta,
) error {
	if accountsDb == nil {
		return errors.New("accountsdb: nil database")
	}
	accountsDb.accountIndexWriteMu.Lock()
	defer accountsDb.accountIndexWriteMu.Unlock()
	return accountsDb.applyAccountIndexMutationsLocked(mutations, meta)
}

func (accountsDb *AccountsDb) applyAccountIndexMutationsLocked(
	mutations []deltaIndexMutation,
	meta *foldMeta,
) error {
	if accountsDb.ProductionIndex != nil {
		return accountsDb.ProductionIndex.Apply(mutations, meta, true)
	}
	if accountsDb.Index == nil {
		return errors.New("accountsdb: no writable account index")
	}
	return accountsDb.Index.Apply(mutations, meta, true)
}

// lookupExactAccountIndexEntry turns a base candidate into an authoritative
// result by checking the full pubkey in its appendvec record. Delta mappings
// are exact by construction and need no second index-membership check.
func (accountsDb *AccountsDb) lookupExactAccountIndexEntry(pubkey solana.PublicKey) (AccountIndexEntry, accountIndexSource, bool, error) {
	entry, source, found, pin, err := accountsDb.lookupAccountIndexCandidatePinned(pubkey)
	if pin != nil {
		defer pin.Close()
	}
	if err != nil || !found || source != accountIndexSourceBase {
		return entry, source, found, err
	}
	fire(accountsDb.afterAccountIndexLookup)
	matches, err := accountsDb.baseIndexEntryMatchesPubkey(pubkey, entry)
	if err != nil {
		return AccountIndexEntry{}, accountIndexSourceNone, false, err
	}
	if !matches {
		return AccountIndexEntry{}, accountIndexSourceNone, false, nil
	}
	return entry, source, true, nil
}

// lookupExactAccountIndexEntries resolves a fold's previous locations in one
// batch. Exact delta entries need no I/O. Immutable-base candidates are grouped
// by appendvec so thousands of updated accounts do not turn into thousands of
// open/close pairs merely to validate StreamHash membership.
func (accountsDb *AccountsDb) lookupExactAccountIndexEntries(
	pubkeys []solana.PublicKey,
) (entries []AccountIndexEntry, found []bool, retErr error) {
	entries = make([]AccountIndexEntry, len(pubkeys))
	found = make([]bool, len(pubkeys))
	sources := make([]accountIndexSource, len(pubkeys))
	if accountsDb.ProductionIndex != nil {
		snapshot, err := accountsDb.ProductionIndex.NewSnapshot(pubkeys)
		if err != nil {
			return nil, nil, err
		}
		// Verification below still needs this generation's retirement markers.
		// The snapshot retains resources without holding the mutable state lock.
		defer func() {
			if closeErr := snapshot.Close(); closeErr != nil {
				retErr = errors.Join(retErr, closeErr)
			}
		}()
		values := make([]deltaIndexValue, len(pubkeys))
		resolved := make([]bool, len(pubkeys))
		if err := snapshot.LookupBatch(context.Background(), values, sources, resolved); err != nil {
			return nil, nil, err
		}
		for i, ok := range resolved {
			if ok && !values[i].Tombstone {
				entries[i], found[i] = values[i].Entry, true
			}
		}
	} else {
		if err := runBatchWorkers(context.Background(), len(pubkeys), func(i int) error {
			entry, source, ok, pin, err := accountsDb.lookupAccountIndexCandidatePinned(pubkeys[i])
			if pin != nil {
				defer pin.Close()
			}
			if err != nil {
				return err
			}
			entries[i], sources[i], found[i] = entry, source, ok
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}

	fire(accountsDb.afterAccountIndexLookup)
	type verificationGroup struct {
		id      appendVecID
		indexes []int
	}
	grouped := make(map[appendVecID][]int)
	for i, source := range sources {
		if found[i] && source == accountIndexSourceBase {
			entry := entries[i]
			id := appendVecID{slot: entry.Slot, fileID: entry.FileId}
			grouped[id] = append(grouped[id], i)
		}
	}
	groups := make([]verificationGroup, 0, len(grouped))
	for id, indexes := range grouped {
		sort.Slice(indexes, func(i, j int) bool {
			return entries[indexes[i]].Offset < entries[indexes[j]].Offset
		})
		groups = append(groups, verificationGroup{id: id, indexes: indexes})
	}

	if err := runBatchWorkers(context.Background(), len(groups), func(job int) (retErr error) {
		group := groups[job]
		path := filepath.Join(accountsDb.AcctsDir, fmt.Sprintf("%d.%d", group.id.slot, group.id.fileID))
		file, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				retired, markerErr := accountsDb.isAppendVecRetired(group.id.slot, group.id.fileID)
				if markerErr != nil {
					return markerErr
				}
				if !retired {
					return fmt.Errorf("accountsdb: active base-index appendvec %s is missing", path)
				}
				for _, idx := range group.indexes {
					found[idx] = false
				}
				return nil
			}
			return fmt.Errorf("open base-index appendvec %s: %w", path, err)
		}
		defer func() {
			if closeErr := file.Close(); retErr == nil && closeErr != nil {
				retErr = fmt.Errorf("close base-index appendvec %s: %w", path, closeErr)
			}
		}()

		for _, idx := range group.indexes {
			stored, readable, err := readAccountIndexEntryPubkey(file, entries[idx])
			if err != nil {
				return fmt.Errorf("read base-index pubkey from %s: %w", path, err)
			}
			if !readable {
				retired, markerErr := accountsDb.isAppendVecRetired(group.id.slot, group.id.fileID)
				if markerErr != nil {
					return markerErr
				}
				if !retired {
					return fmt.Errorf("accountsdb: active base-index appendvec %s is truncated", path)
				}
				found[idx] = false
				continue
			}
			if stored != pubkeys[idx] {
				found[idx] = false
			}
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return entries, found, nil
}

func (accountsDb *AccountsDb) baseIndexEntryMatchesPubkey(pubkey solana.PublicKey, entry AccountIndexEntry) (bool, error) {
	path := filepath.Join(accountsDb.AcctsDir, fmt.Sprintf("%d.%d", entry.Slot, entry.FileId))
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			retired, markerErr := accountsDb.isAppendVecRetired(entry.Slot, entry.FileId)
			if markerErr != nil {
				return false, markerErr
			}
			if retired {
				return false, nil
			}
			return false, fmt.Errorf("accountsdb: active base-index appendvec %s is missing", path)
		}
		return false, fmt.Errorf("open base-index appendvec %s: %w", path, err)
	}
	defer f.Close()
	stored, readable, err := readAccountIndexEntryPubkey(f, entry)
	if err != nil {
		return false, fmt.Errorf("read base-index pubkey at %s@%d: %w", path, entry.Offset, err)
	}
	if !readable {
		retired, markerErr := accountsDb.isAppendVecRetired(entry.Slot, entry.FileId)
		if markerErr != nil {
			return false, markerErr
		}
		if !retired {
			return false, fmt.Errorf("accountsdb: active base-index appendvec %s is truncated", path)
		}
		return false, nil
	}
	return stored == pubkey, nil
}

func readAccountIndexEntryPubkey(file *os.File, entry AccountIndexEntry) (solana.PublicKey, bool, error) {
	var stored solana.PublicKey
	if entry.Offset > math.MaxInt64-pubkeyOffset {
		return stored, false, nil
	}
	_, err := file.ReadAt(stored[:], int64(entry.Offset+pubkeyOffset))
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return stored, false, nil
	}
	if err != nil {
		return stored, false, err
	}
	return stored, true, nil
}

// readIndexedAccount performs one union-index fetch + file-read attempt.
func (accountsDb *AccountsDb) readIndexedAccount(pubkey solana.PublicKey) (*accounts.Account, error) {
	acctIdxEntry, source, found, pin, err := accountsDb.lookupAccountIndexCandidatePinned(pubkey)
	if pin != nil {
		defer pin.Close()
	}
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNoAccount
	}
	fire(accountsDb.afterAccountIndexLookup)

	appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, acctIdxEntry.Slot, acctIdxEntry.FileId)

	appendVecFile, err := os.Open(appendVecFileName)
	if err != nil {
		if source == accountIndexSourceBase && os.IsNotExist(err) {
			retired, markerErr := accountsDb.isAppendVecRetired(acctIdxEntry.Slot, acctIdxEntry.FileId)
			if markerErr != nil {
				return nil, markerErr
			}
			if retired {
				return nil, ErrNoAccount
			}
			return nil, fmt.Errorf("accountsdb: active base-index appendvec %s is missing", appendVecFileName)
		}
		return nil, err
	}
	defer appendVecFile.Close()

	if acctIdxEntry.Offset > math.MaxInt64 {
		return nil, fmt.Errorf("account offset %d overflows int64", acctIdxEntry.Offset)
	}
	if _, err := appendVecFile.Seek(int64(acctIdxEntry.Offset), 0); err != nil {
		return nil, fmt.Errorf("seek %s@%d: %w", appendVecFileName, acctIdxEntry.Offset, err)
	}

	acct, exact, err := unmarshalAcctFromAppendVecAcctHeaderExpected(appendVecFile, pubkey)
	if err != nil {
		if source == accountIndexSourceBase &&
			(errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
			retired, markerErr := accountsDb.isAppendVecRetired(acctIdxEntry.Slot, acctIdxEntry.FileId)
			if markerErr != nil {
				return nil, markerErr
			}
			if retired {
				return nil, ErrNoAccount
			}
		}
		return nil, fmt.Errorf("unmarshal account at %s@%d: %w", appendVecFileName, acctIdxEntry.Offset, err)
	}
	if !exact {
		if source == accountIndexSourceBase {
			return nil, ErrNoAccount
		}
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
	if accountsDb.ProductionIndex != nil && !accountsDb.RootedDurable {
		return ErrProductionAccountIndexRequiresRootedDurable
	}
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
	fileId, err := accountsDb.allocateFileID()
	if err != nil {
		panic(fmt.Sprintf("allocate appendvec file ID: %v", err))
	}
	appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, slot, fileId)
	appendVecFile, err := os.OpenFile(appendVecFileName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		//mlog.Log.Debugf("unable to open appendvec file %s for writing to accountsdb", appendVecFileName)
		panic(err)
	}
	defer appendVecFile.Close()

	appendVecAcctsBuf := new(bytes.Buffer)
	writer := new(bytes.Buffer)
	mutations := make([]deltaIndexMutation, 0, len(accts))

	for _, acct := range accts {
		if acct == nil {
			continue
		}

		// create index entry, encode it and write it to the index kv store
		// offset field is specified as the current num of bytes written to the appendvec buffer.
		writer.Reset()

		indexEntry := AccountIndexEntry{Slot: slot, FileId: fileId, Offset: uint64(appendVecAcctsBuf.Len())}

		// if an entry already existed in the index for this account, very often we can simply make the state update
		// in-place, i.e. into the account's existing appendvec blob.
		// we can make the account state update in-place iff the existing version's data length is the same as the
		// new version's data length, which is the case about 98% of the time.
		// if not, then we write out a new appendvec.
		acctIdxEntry, _, found, err := accountsDb.lookupExactAccountIndexEntry(acct.Key)
		if err != nil {
			panic(fmt.Sprintf("failed to look up existing AccountIndexEntry: %v", err))
		}
		if found {
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
			existingAppendVecFile.Close()
		}

		mutations = append(mutations, liveDeltaMutation(acct.Key, indexEntry))

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
	if err := appendVecFile.Sync(); err != nil {
		panic(err)
	}
	if len(mutations) > 0 {
		if err := accountsDb.applyAccountIndexMutations(mutations, nil); err != nil {
			panic(fmt.Sprintf("unable to publish %d account-index mutations: %v", len(mutations), err))
		}
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
					existingIdxEntry, _, found, err := accountsDb.lookupExactAccountIndexEntry(a.Key)
					if err != nil {
						return fmt.Errorf("reading from index: %w", err)
					}
					if !found {
						lengthChangedAccounts <- a
						return nil
					}

					existingAppendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, existingIdxEntry.Slot, existingIdxEntry.FileId)
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
		fileId, err := accountsDb.allocateFileID()
		if err != nil {
			return fmt.Errorf("allocate appendvec file ID: %w", err)
		}
		appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, slot, fileId)
		appendVecFile, err := os.OpenFile(appendVecFileName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			return err
		}
		defer appendVecFile.Close()
		appendVecWriter := bufio.NewWriter(appendVecFile)

		appendVecFileOffset := uint64(0)
		mutations := make([]deltaIndexMutation, 0)

		for acct := range lengthChangedAccounts {
			indexEntry := AccountIndexEntry{Slot: slot, FileId: fileId, Offset: appendVecFileOffset}
			mutations = append(mutations, liveDeltaMutation(acct.Key, indexEntry))

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
		if err := appendVecWriter.Flush(); err != nil {
			return err
		}
		if err := appendVecFile.Sync(); err != nil {
			return err
		}
		return accountsDb.applyAccountIndexMutations(mutations, nil)
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
	keys, err := accountsDb.KeysBetweenPrefixesContext(context.Background(), startPrefix, endPrefix)
	if err != nil {
		// This compatibility API predates error-returning AccountsDB methods.
		// Failing closed is consensus-safe; returning an empty range would
		// silently skip due rent on clusters where rent rewrites remain active.
		panic(fmt.Sprintf("accountsdb: enumerate keys between prefixes: %v", err))
	}
	return keys
}

func (accountsDb *AccountsDb) AllKeys() [][]byte {
	keys, err := accountsDb.AllKeysContext(context.Background())
	if err != nil {
		panic(fmt.Sprintf("accountsdb: enumerate all keys: %v", err))
	}
	return keys
}
