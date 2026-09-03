package accountsdb

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

var (
	// ErrAccountIndexMigrationRequired is returned when an AccountsDB contains
	// an older account-index format. Migration is an explicit operator action:
	// silently starting with an empty V2 head could resurrect stale accounts.
	ErrAccountIndexMigrationRequired  = errors.New("accountsdb: account-index migration required")
	ErrProductionAccountIndexExists   = errors.New("accountsdb: production account index already exists")
	ErrProductionAccountIndexPoisoned = errors.New(
		"accountsdb: production account index has an ambiguous or failed publication",
	)
	ErrProductionAccountIndexRequiresRootedDurable = errors.New(
		"accountsdb: production account index requires rooted-durable writes",
	)
)

// ProductionAccountIndex is the complete AccountsDB V2 index: a bounded,
// CRC-journaled and exact mutable head above atomically selected immutable
// StreamHash shards. The root catalog is the only immutable publication point.
// Readers pin one root generation; replaced mappings and files are reclaimed
// only after all readers of that generation drain.
type ProductionAccountIndex struct {
	root             string
	config           ProductionAccountIndexConfig
	view             *IndexViewManager
	mutable          *ShardedMutableAccountIndex
	checkpointBudget *checkpointResourceBudget
	storeLock        *productionAccountIndexStoreLock

	ready chan struct{}
	// publishRoot is immutable after Open returns. Keeping the durability seam on
	// the index makes publication outcomes testable without weakening the public
	// root-catalog API, which deliberately hides its internal rename state.
	publishRoot productionRootCatalogPublisher
	// applyMu serializes the complete WAL-apply/rewind-lineage operation. It
	// must be acquired before publishMu. In particular, publishMu must never be
	// held while mutable.Apply can wait for checkpoint/rebase capacity, because
	// those maintenance callbacks publish through publishMu themselves.
	applyMu   sync.Mutex
	publishMu sync.Mutex
	// Extent ordinals form one append-only lineage. Serializing rebase builds,
	// not merely their final publication, prevents two shards from independently
	// assigning the same next ordinal to different appendvec extents.
	rebaseGate chan struct{}
	// Exact raw-prefix enumeration must merge every keyed shard. A single permit
	// hard-bounds the otherwise large per-scan descriptor/scratch footprint and
	// applies context-aware backpressure to rare maintenance/rent scans.
	enumerationGate chan struct{}
	// rebaseIncarnation invalidates a rolling build as soon as a rewind starts,
	// before the rewind WAL frame is written. The durable root lineage remains a
	// second fence when its selector publication succeeds; this runtime fence is
	// load-bearing when that publication fails before rename and is safely
	// deferred because no in-process builder may cross the incarnation change.
	rebaseIncarnation atomic.Uint64

	poisonMu sync.Mutex
	poison   error

	gcMu                sync.Mutex
	obsoleteBases       []productionObsoleteBase
	lastRetirementPrune uint64
	// discardWG owns candidate-generation finalizers which were started after a
	// derived payload was constructed but before IndexViewManager accepted it.
	// Add happens only inside mutable maintenance callbacks; finishShutdown waits
	// for mutable.Close before Wait, so the WaitGroup has no Add/Wait race.
	discardWG    sync.WaitGroup
	discardErrMu sync.Mutex
	discardErr   error
	// resourceWatcherWG owns post-finalization bookkeeping for obsolete base and
	// checkpoint resources. These watchers wait on resources drained by view
	// shutdown, so finishShutdown joins them after the manager but before the
	// store lock is released.
	resourceWatcherWG    sync.WaitGroup
	resourceWatcherErrMu sync.Mutex
	resourceWatcherErr   error
	closed               atomic.Bool
	shutdownOnce         sync.Once
	shutdownDone         chan struct{}
	shutdownErr          error
}

type productionObsoleteBase struct {
	// resources is the complete immutable base-generation retirement group:
	// the replaced base shard and, when the append-only extent lineage advanced,
	// its replaced extent catalog. A later rolling build is admitted only after
	// every member has finalized successfully.
	resources      []*IndexGenerationResource
	coveredThrough uint64
}

type emptyProductionAccountIndexSource struct{}

func (emptyProductionAccountIndexSource) Scan(
	ctx context.Context,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) error {
	if ctx == nil {
		return errors.New("accountsdb: nil empty account-index source context")
	}
	if visit == nil {
		return errors.New("accountsdb: nil empty account-index source visitor")
	}
	return ctx.Err()
}

// InitializeEmptyProductionAccountIndex explicitly creates an empty V2 index.
// It is useful for genesis/custom-ledger construction and small isolated tests;
// normal validator bootstrap initializes from snapshot runs instead.
func InitializeEmptyProductionAccountIndex(
	ctx context.Context,
	accountsDBRoot string,
	config ProductionAccountIndexConfig,
) error {
	return InitializeProductionAccountIndex(ctx, accountsDBRoot, emptyProductionAccountIndexSource{}, config)
}

// InitializeProductionAccountIndex builds a fresh immutable V2 base and empty
// WAL. Immutable artifacts and the WAL are synced first; the root selector is
// published last, so an interrupted bootstrap can never look complete.
func InitializeProductionAccountIndex(
	ctx context.Context,
	accountsDBRoot string,
	source StreamIndexSource,
	config ProductionAccountIndexConfig,
) (retErr error) {
	return initializeProductionAccountIndex(
		ctx, accountsDBRoot, source, config, writeRootIndexCatalogAtomic,
	)
}

// InitializeProductionAccountIndexWithStoreGuard is the snapshot-bootstrap
// form of InitializeProductionAccountIndex. It borrows an already-held
// exclusive guard rather than dropping ownership between cleanup, extraction,
// immutable publication, and final database open.
func InitializeProductionAccountIndexWithStoreGuard(
	ctx context.Context,
	accountsDBRoot string,
	source StreamIndexSource,
	config ProductionAccountIndexConfig,
	guard *ProductionAccountIndexStoreGuard,
) error {
	return guard.withProductionAccountIndexStoreLock(
		accountsDBRoot,
		func(storeLock *productionAccountIndexStoreLock) error {
			return initializeProductionAccountIndexWithStoreLock(
				ctx,
				accountsDBRoot,
				source,
				config,
				writeRootIndexCatalogAtomic,
				storeLock,
			)
		},
	)
}

type productionRootCatalogPublisher func(string, *RootIndexCatalog) (bool, error)

func initializeProductionAccountIndex(
	ctx context.Context,
	accountsDBRoot string,
	source StreamIndexSource,
	config ProductionAccountIndexConfig,
	publishRoot productionRootCatalogPublisher,
) (retErr error) {
	return initializeProductionAccountIndexWithStoreLock(
		ctx, accountsDBRoot, source, config, publishRoot, nil,
	)
}

func initializeProductionAccountIndexWithStoreLock(
	ctx context.Context,
	accountsDBRoot string,
	source StreamIndexSource,
	config ProductionAccountIndexConfig,
	publishRoot productionRootCatalogPublisher,
	heldStoreLock *productionAccountIndexStoreLock,
) (retErr error) {
	config = config.withCheckpointBudgetDefaults()
	if ctx == nil {
		return errors.New("accountsdb: nil production account-index initialization context")
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if source == nil {
		return errors.New("accountsdb: nil production account-index source")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if accountsDBRoot == "" {
		return errors.New("accountsdb: empty production account-index root")
	}
	info, err := os.Lstat(accountsDBRoot)
	if err != nil {
		return fmt.Errorf("accountsdb: stat production account-index root: %w", err)
	}
	if !info.IsDir() {
		return errors.New("accountsdb: production account-index root is not a directory")
	}
	if publishRoot == nil {
		return errors.New("accountsdb: nil production root-catalog publisher")
	}
	storeLock := heldStoreLock
	if storeLock == nil {
		storeLock, err = acquireProductionAccountIndexStoreLock(accountsDBRoot, true)
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, storeLock.Close()) }()
	}
	if err := rejectExistingProductionOrLegacyIndex(accountsDBRoot); err != nil {
		return err
	}

	catalog, err := NewRootIndexCatalog(config.ShardCount)
	if err != nil {
		return err
	}
	catalog.Generation = 1
	if _, err := io.ReadFull(cryptorand.Reader, catalog.Lineage[:]); err != nil {
		return fmt.Errorf("accountsdb: generate root catalog lineage: %w", err)
	}

	build, err := BuildShardedStreamBaseWithShardCount(
		ctx,
		source,
		accountsDBRoot,
		1,
		config.ShardCount,
		catalog.RoutingKey,
		nil,
		config.CheckpointWorkers,
	)
	if err != nil {
		return fmt.Errorf("accountsdb: build initial sharded StreamHash base: %w", err)
	}
	cleanupBuild := true
	defer func() {
		if cleanupBuild {
			retErr = errors.Join(retErr, removeInitialProductionBuild(accountsDBRoot, build))
		}
	}()

	if build.RoutingKey != catalog.RoutingKey {
		return errors.New("accountsdb: initial base returned a different routing key")
	}
	catalog.SharedExtentCatalog = build.ExtentCatalogArtifact
	if len(build.Shards) != len(catalog.Shards) {
		return fmt.Errorf(
			"accountsdb: initial base returned %d shards, want %d",
			len(build.Shards), len(catalog.Shards),
		)
	}
	for i := range build.Shards {
		built := build.Shards[i]
		if built.ShardID != uint32(i) || built.Generation != build.Generation {
			return fmt.Errorf(
				"accountsdb: initial base shard %d identity is shard=%d generation=%d",
				i, built.ShardID, built.Generation,
			)
		}
		catalog.Shards[i].BaseGeneration = built.Generation
		catalog.Shards[i].BaseIndex = built.Index
		catalog.Shards[i].BaseRecords = built.Scan
	}
	if err := catalog.Validate(); err != nil {
		return err
	}

	if err := InitializeShardedMutableAccountIndexAtSequence(accountsDBRoot, catalog.CoveredSequence); err != nil {
		return fmt.Errorf("accountsdb: initialize production account-index WAL: %w", err)
	}
	cleanupWAL := true
	defer func() {
		if cleanupWAL {
			journal := filepath.Join(accountsDBRoot, ShardedDeltaIndexJournalFileName)
			retErr = errors.Join(retErr, removeIfRegular(journal), fsyncDir(accountsDBRoot))
		}
	}()
	rootRenamed, err := publishRoot(accountsDBRoot, catalog)
	if rootRenamed {
		// Rename is the point of no safe rollback. Even when directory fsync
		// reports failure, this process cannot know whether a crash would retain
		// the old directory entry or this selector. Preserve both the WAL and
		// every artifact the installed selector may reference.
		cleanupWAL = false
		cleanupBuild = false
	}
	if err != nil {
		return fmt.Errorf("accountsdb: publish initial production root catalog: %w", err)
	}

	cleanupWAL = false
	cleanupBuild = false
	return nil
}

func rejectExistingProductionOrLegacyIndex(root string) error {
	production := []string{RootIndexCatalogFileName, ShardedDeltaIndexJournalFileName}
	for _, name := range production {
		path := filepath.Join(root, name)
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("%w: %s", ErrProductionAccountIndexExists, path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("accountsdb: inspect account-index artifact %s: %w", path, err)
		}
	}
	legacy := []string{
		"mithril_db",
		DeltaIndexJournalFileName,
		StreamIndexFileName,
		StreamIndexManifestFileName,
	}
	for _, name := range legacy {
		path := filepath.Join(root, name)
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf(
				"%w: found legacy account-index artifact %s; rebuild from a fresh snapshot or run an explicit offline converter",
				ErrAccountIndexMigrationRequired,
				path,
			)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("accountsdb: inspect legacy account-index artifact %s: %w", path, err)
		}
	}
	return nil
}

func removeInitialProductionBuild(root string, build *ShardedStreamBaseBuildResult) error {
	if build == nil {
		return nil
	}
	artifacts := make([]IndexCatalogArtifact, 0, 1+len(build.Shards)*2)
	if build.OwnsExtentCatalogArtifact {
		artifacts = append(artifacts, build.ExtentCatalogArtifact)
	}
	for _, shard := range build.Shards {
		artifacts = append(artifacts, shard.Index, shard.Scan)
	}
	return removeImmutableArtifacts(root, artifacts)
}

func removeIfRegular(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("accountsdb: refusing to remove non-regular artifact %s", path)
	}
	return os.Remove(path)
}

// OpenProductionAccountIndex verifies the root and every selected artifact,
// replays the global WAL, then starts bounded checkpoint/rebase maintenance.
func OpenProductionAccountIndex(
	accountsDBRoot string,
	config ProductionAccountIndexConfig,
) (_ *ProductionAccountIndex, retErr error) {
	config = config.withCheckpointBudgetDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	storeLock, err := acquireProductionAccountIndexStoreLock(accountsDBRoot, true)
	if err != nil {
		return nil, err
	}
	return openProductionAccountIndexWithStoreLock(accountsDBRoot, config, storeLock, true)
}

// OpenProductionAccountIndexWithStoreGuard transfers a bootstrap guard into
// the opened index. On success the index retains exclusive ownership for its
// complete lifetime; on failure the guard retains ownership for retry or
// caller unwind.
func OpenProductionAccountIndexWithStoreGuard(
	accountsDBRoot string,
	config ProductionAccountIndexConfig,
	guard *ProductionAccountIndexStoreGuard,
) (*ProductionAccountIndex, error) {
	config = config.withCheckpointBudgetDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	var index *ProductionAccountIndex
	err := guard.transferProductionAccountIndexStoreLockOnSuccess(
		accountsDBRoot,
		func(storeLock *productionAccountIndexStoreLock) error {
			var openErr error
			index, openErr = openProductionAccountIndexWithStoreLock(
				accountsDBRoot, config, storeLock, false,
			)
			return openErr
		},
	)
	return index, err
}

func openProductionAccountIndexWithStoreLock(
	accountsDBRoot string,
	config ProductionAccountIndexConfig,
	storeLock *productionAccountIndexStoreLock,
	closeStoreLockOnFailure bool,
) (_ *ProductionAccountIndex, retErr error) {
	if storeLock == nil {
		return nil, errors.New("accountsdb: nil production account-index store lock")
	}
	keepStoreLock := false
	defer func() {
		if !keepStoreLock && closeStoreLockOnFailure {
			retErr = errors.Join(retErr, storeLock.Close())
		}
	}()
	catalog, err := ReadRootIndexCatalog(accountsDBRoot)
	if err != nil {
		return nil, err
	}
	if err := config.ValidateCatalogShardCount(catalog); err != nil {
		return nil, err
	}
	// Reject an impossible selected-checkpoint budget before touching any
	// root-selected immutable file. Besides avoiding expensive I/O, this keeps
	// startup fail-closed even when over-budget catalog paths are unavailable.
	selectedCheckpointBytes, err := checkpointSelectedBytesFromCatalog(catalog)
	if err != nil {
		return nil, err
	}
	checkpointBudget, err := newCheckpointResourceBudget(
		config.MaxCheckpointSelectedBytes,
		config.MaxCheckpointPhysicalBytes,
		selectedCheckpointBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: initialize delta-checkpoint resource budget: %w", err)
	}
	immutable, err := OpenShardedImmutableIndex(accountsDBRoot, catalog)
	if err != nil {
		return nil, err
	}
	index, err := openProductionAccountIndexFromImmutableWithStoreLock(
		accountsDBRoot,
		config,
		storeLock,
		catalog,
		immutable,
		checkpointBudget,
	)
	if err != nil {
		return nil, err
	}
	keepStoreLock = true
	return index, nil
}

// openProductionAccountIndexFromImmutableWithStoreLock adopts one already
// fully verified immutable generation and constructs only the mutable runtime
// around it. Ownership of immutable transfers on entry and is released on
// every failure. This is the snapshot fast path: its caller retains the same
// exclusive store lock from full verification through this adoption.
func openProductionAccountIndexFromImmutableWithStoreLock(
	accountsDBRoot string,
	config ProductionAccountIndexConfig,
	storeLock *productionAccountIndexStoreLock,
	catalog *RootIndexCatalog,
	immutable *ShardedImmutableIndex,
	checkpointBudget *checkpointResourceBudget,
) (_ *ProductionAccountIndex, retErr error) {
	closeImmutable := immutable != nil
	defer func() {
		if closeImmutable {
			retErr = errors.Join(retErr, immutable.closeUnmanaged())
		}
	}()
	if storeLock == nil {
		return nil, errors.New("accountsdb: nil production account-index store lock")
	}
	config = config.withCheckpointBudgetDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if catalog == nil || immutable == nil {
		return nil, errors.New("accountsdb: nil root catalog or immutable account index")
	}
	if err := config.ValidateCatalogShardCount(catalog); err != nil {
		return nil, err
	}
	sameCatalog, err := sameRootIndexCatalog(catalog, immutable.RootCatalog())
	if err != nil {
		return nil, fmt.Errorf("accountsdb: compare retained immutable root catalog: %w", err)
	}
	if !sameCatalog {
		return nil, errors.New("accountsdb: retained immutable generation does not match selected root catalog")
	}
	if checkpointBudget == nil {
		selectedCheckpointBytes, err := checkpointSelectedBytesFromCatalog(catalog)
		if err != nil {
			return nil, err
		}
		checkpointBudget, err = newCheckpointResourceBudget(
			config.MaxCheckpointSelectedBytes,
			config.MaxCheckpointPhysicalBytes,
			selectedCheckpointBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("accountsdb: initialize delta-checkpoint resource budget: %w", err)
		}
	}
	view, err := NewIndexViewManager(catalog, immutable, immutable.Resources())
	if err != nil {
		return nil, err
	}
	// The manager now owns every immutable resource.
	closeImmutable = false
	index := &ProductionAccountIndex{
		root:             accountsDBRoot,
		config:           config,
		view:             view,
		checkpointBudget: checkpointBudget,
		storeLock:        storeLock,
		ready:            make(chan struct{}),
		shutdownDone:     make(chan struct{}),
		publishRoot:      writeRootIndexCatalogAtomic,
		rebaseGate:       make(chan struct{}, 1),
		enumerationGate:  make(chan struct{}, 1),
	}
	cleanup := true
	readyClosed := false
	defer func() {
		if cleanup {
			if !readyClosed {
				close(index.ready)
			}
			retErr = errors.Join(retErr, view.Close())
		}
	}()
	// Startup orphan collection is safe only before the mutable writer and its
	// background publication callbacks start.
	if _, err := garbageCollectShardedImmutableIndexOrphansLocked(accountsDBRoot, catalog); err != nil {
		return nil, fmt.Errorf("accountsdb: collect unselected V2 index artifacts: %w", err)
	}

	// InitialCheckpointHandles gives the mutable layer its own ownership of the
	// same mappings pinned by the root generation; startup never maps a delta
	// twice merely to establish independent lifetimes.
	initial, err := immutable.InitialCheckpointHandles()
	if err != nil {
		return nil, err
	}
	releaseInitial := func() {
		for i, checkpoint := range initial {
			if checkpoint != nil {
				_ = checkpoint.Release()
				initial[i] = nil
			}
		}
	}
	defer releaseInitial()

	router, err := NewPersistentIndexShardRouter(int(catalog.ShardCount), catalog.RoutingKey)
	if err != nil {
		return nil, err
	}
	covered := make([]uint64, len(catalog.Shards))
	baseCovered := make([]uint64, len(catalog.Shards))
	for i := range catalog.Shards {
		covered[i] = catalog.Shards[i].effectiveCoveredSequence()
		baseCovered[i] = catalog.Shards[i].BaseCoveredSequence
	}
	mutable, err := OpenShardedMutableAccountIndex(ShardedMutableIndexConfig{
		Directory:            accountsDBRoot,
		Router:               router,
		CoveredSequences:     covered,
		BaseCoveredSequences: baseCovered,
		InitialCheckpoints:   initial,
		checkpointBudget:     checkpointBudget,
		// A fresh process has no pre-existing root pins. Once every current
		// base covers a retirement marker, replay can omit it and rewrite the
		// WAL before any maintenance goroutine starts.
		StartupRetirementPruneThrough: minimumBaseCoverage(catalog),
		MaxHotKeys:                    config.MaxHotKeys,
		MaxHotBytes:                   config.MaxHotBytes,
		SealKeys:                      config.SealKeys,
		SealMaxAge:                    config.SealMaxAge,
		RebaseKeys:                    config.RebaseKeys,
		JournalRewriteBytes:           config.JournalRewriteBytes,
		CheckpointWorkers:             config.CheckpointWorkers,
		MaxConcurrentSeals:            config.MaxConcurrentSeals,
		MaxConcurrentRebases:          config.RebaseWorkers,
		Callbacks: ShardedMutableIndexCallbacks{
			PublishCheckpoint: index.publishCheckpoint,
			RequestRebase:     index.rebaseShard,
			AcquireImmutable: func() (ShardedMutableImmutablePin, error) {
				return index.view.Acquire()
			},
		},
	})
	if err != nil {
		return nil, err
	}
	index.mutable = mutable
	index.lastRetirementPrune = minimumBaseCoverage(catalog)
	close(index.ready)
	readyClosed = true
	releaseInitial()
	cleanup = false
	return index, nil
}

func sameRootIndexCatalog(left, right *RootIndexCatalog) (bool, error) {
	if left == nil || right == nil {
		return left == right, nil
	}
	leftBytes, err := left.MarshalBinary()
	if err != nil {
		return false, err
	}
	rightBytes, err := right.MarshalBinary()
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftBytes, rightBytes), nil
}

func (index *ProductionAccountIndex) waitReady(ctx context.Context) error {
	if index == nil {
		return errors.New("accountsdb: nil production account index")
	}
	select {
	case <-index.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (index *ProductionAccountIndex) writeRootCatalog(
	catalog *RootIndexCatalog,
) (bool, error) {
	if index == nil {
		return false, errors.New("accountsdb: nil production account index")
	}
	publish := index.publishRoot
	if publish == nil {
		// Defensive compatibility for focused tests that construct the index
		// directly. OpenProductionAccountIndex always installs the publisher.
		publish = writeRootIndexCatalogAtomic
	}
	renamed, err := publish(index.root, catalog)
	if err == nil && !renamed {
		return false, errors.New(
			"accountsdb: root catalog publisher reported success before selector rename",
		)
	}
	return renamed, err
}

func (index *ProductionAccountIndex) advanceRebaseIncarnation() error {
	for {
		prior := index.rebaseIncarnation.Load()
		if prior == ^uint64(0) {
			return errors.New("accountsdb: rebase incarnation exhausted")
		}
		if index.rebaseIncarnation.CompareAndSwap(prior, prior+1) {
			return nil
		}
	}
}

func (index *ProductionAccountIndex) setPoison(err error) error {
	if err == nil {
		return nil
	}
	index.poisonMu.Lock()
	if index.poison == nil {
		index.poison = err
	}
	poison := index.poison
	index.poisonMu.Unlock()
	return fmt.Errorf("%w: %v", ErrProductionAccountIndexPoisoned, poison)
}

// checkPoison reports a failed or ambiguous publication without consulting the
// shutdown fence.  It is used after a rewind WAL decision is already durable:
// shutdown must allow that in-flight Apply to finish, while a publisher that
// poisoned the index before releasing publishMu must fence the queued rewind.
func (index *ProductionAccountIndex) checkPoison() error {
	if index == nil {
		return errors.New("accountsdb: nil production account index")
	}
	index.poisonMu.Lock()
	err := index.poison
	index.poisonMu.Unlock()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrProductionAccountIndexPoisoned, err)
	}
	return nil
}

func (index *ProductionAccountIndex) checkUsable() error {
	if index == nil {
		return errors.New("accountsdb: nil production account index")
	}
	if index.closed.Load() {
		return ErrShardedMutableClosed
	}
	return index.checkPoison()
}

// LookupCandidate returns an exact mutable result or a probabilistic cold-base
// candidate. A found exact tombstone has found=false so callers never expose an
// older base record.
func (index *ProductionAccountIndex) LookupCandidate(
	key solana.PublicKey,
) (AccountIndexEntry, accountIndexSource, bool, error) {
	if err := index.checkUsable(); err != nil {
		return AccountIndexEntry{}, accountIndexSourceNone, false, err
	}
	value, found, err := index.mutable.LookupWithError(key)
	if err != nil {
		return AccountIndexEntry{}, accountIndexSourceNone, false, err
	}
	if found {
		if value.Tombstone {
			return AccountIndexEntry{}, accountIndexSourceNone, false, nil
		}
		return value.Entry, accountIndexSourceDelta, true, nil
	}
	view, err := index.view.Acquire()
	if err != nil {
		return AccountIndexEntry{}, accountIndexSourceNone, false, err
	}
	defer view.Close()
	immutable, ok := view.Payload().(*ShardedImmutableIndex)
	if !ok || immutable == nil {
		return AccountIndexEntry{}, accountIndexSourceNone, false, errors.New("accountsdb: invalid immutable index payload")
	}
	entry, found, err := immutable.LookupBaseCandidate(key)
	if err != nil || !found {
		return AccountIndexEntry{}, accountIndexSourceNone, false, err
	}
	return entry, accountIndexSourceBase, true, nil
}

// ProductionAccountIndexSnapshot pins a coherent mutable epoch and the root
// generation installed before that epoch could discard frozen RAM.
type ProductionAccountIndexSnapshot struct {
	mutable   *ShardedMutableLookupSnapshot
	immutable *ShardedImmutableIndex
	closed    atomic.Bool
}

func (index *ProductionAccountIndex) NewSnapshot(
	keys []solana.PublicKey,
) (*ProductionAccountIndexSnapshot, error) {
	if err := index.checkUsable(); err != nil {
		return nil, err
	}
	mutable, err := index.mutable.NewSnapshot(keys)
	if err != nil {
		return nil, err
	}
	pin, ok := mutable.ImmutablePin().(*IndexReadView)
	if !ok || pin == nil {
		_ = mutable.Close()
		return nil, errors.New("accountsdb: mutable snapshot did not pin an index read view")
	}
	immutable, ok := pin.Payload().(*ShardedImmutableIndex)
	if !ok || immutable == nil {
		_ = mutable.Close()
		return nil, errors.New("accountsdb: mutable snapshot pinned an invalid immutable payload")
	}
	return &ProductionAccountIndexSnapshot{mutable: mutable, immutable: immutable}, nil
}

func (snapshot *ProductionAccountIndexSnapshot) LookupAt(
	job int,
) (deltaIndexValue, accountIndexSource, bool, error) {
	if snapshot == nil || snapshot.closed.Load() {
		return deltaIndexValue{}, accountIndexSourceNone, false, errors.New("accountsdb: production index snapshot is closed")
	}
	value, found, err := snapshot.mutable.LookupAt(job)
	if err != nil {
		return deltaIndexValue{}, accountIndexSourceNone, false, err
	}
	if found {
		return value, accountIndexSourceDelta, true, nil
	}
	key := snapshot.mutable.keys[job]
	entry, found, err := snapshot.immutable.LookupBaseCandidate(key)
	if err != nil || !found {
		return deltaIndexValue{}, accountIndexSourceNone, false, err
	}
	return deltaIndexValue{Entry: entry}, accountIndexSourceBase, true, nil
}

func (snapshot *ProductionAccountIndexSnapshot) LookupBatch(
	ctx context.Context,
	values []deltaIndexValue,
	sources []accountIndexSource,
	found []bool,
) error {
	if ctx == nil {
		return errors.New("accountsdb: nil production index batch context")
	}
	if snapshot == nil || snapshot.closed.Load() {
		return errors.New("accountsdb: production index snapshot is closed")
	}
	count := len(snapshot.mutable.keys)
	if len(values) != count || len(sources) != count || len(found) != count {
		return fmt.Errorf(
			"accountsdb: production batch buffer mismatch: keys=%d values=%d sources=%d found=%d",
			count, len(values), len(sources), len(found),
		)
	}
	clear(values)
	clear(sources)
	clear(found)
	if err := runBatchStaticRanges(ctx, count, func(workerCtx context.Context, start, end int) error {
		for job := start; job < end; job++ {
			if job&255 == 0 {
				if err := workerCtx.Err(); err != nil {
					return err
				}
			}
			value, source, ok, err := snapshot.LookupAt(job)
			if err != nil {
				return fmt.Errorf("accountsdb: production index batch lookup %d: %w", job, err)
			}
			values[job], sources[job], found[job] = value, source, ok
		}
		return nil
	}); err != nil {
		return err
	}
	return ctx.Err()
}

func (snapshot *ProductionAccountIndexSnapshot) Close() error {
	if snapshot == nil || !snapshot.closed.CompareAndSwap(false, true) {
		return nil
	}
	return snapshot.mutable.Close()
}

// Apply persists one exact atomic frame. A fold-meta regression (rewind)
// advances the runtime rebase incarnation before the WAL decision and then
// rotates durable root lineage under the publication lock. The runtime fence
// rejects every pre-rewind build even if the advisory lineage selector fails
// before rename. All Apply calls are serialized until this sequence completes:
// otherwise a later forward Apply could overtake the durable rewind.
func (index *ProductionAccountIndex) Apply(
	mutations []deltaIndexMutation,
	meta *foldMeta,
	durable bool,
) error {
	if err := index.checkUsable(); err != nil {
		return err
	}
	index.applyMu.Lock()
	defer index.applyMu.Unlock()
	// Close marks the index closed before waiting for applyMu. Recheck after
	// acquiring it so a queued writer cannot append after teardown begins.
	if err := index.checkUsable(); err != nil {
		return err
	}
	var metaCopy *foldMeta
	if meta != nil {
		copied := *meta
		metaCopy = &copied
	}
	regression := false
	if metaCopy != nil {
		if prior, ok := index.mutable.ReadFoldMeta(); ok {
			regression = foldMetaPrecedes(*metaCopy, prior)
		}
	}
	var rewindLineage [32]byte
	if regression {
		// Prepare every fallible in-memory input before the WAL decision, then
		// invalidate builds before that decision can become durable. A builder
		// that captured the previous value is rejected at its final root CAS even
		// if publishing the durable lineage later fails before rename.
		if _, err := io.ReadFull(cryptorand.Reader, rewindLineage[:]); err != nil {
			return fmt.Errorf("accountsdb: prepare rewind root lineage: %w", err)
		}
		if err := index.advanceRebaseIncarnation(); err != nil {
			return err
		}
	}
	// The WAL frame must become durable without publishMu held. At capacity,
	// mutable.Apply deliberately waits for a seal/rebase; its callback needs
	// publishMu to make exactly that progress.
	if err := index.mutable.Apply(mutations, metaCopy, durable); err != nil {
		return err
	}
	if !regression {
		return nil
	}

	// WAL-before-root is the safe crash order. A crash here replays the exact
	// rewind overlay over the prior root, and no pre-crash rebase builder can
	// survive to publish afterward. Publishing the lineage first could expose a
	// rewind incarnation whose restoring/tombstone frame was never durable.
	index.publishMu.Lock()
	defer index.publishMu.Unlock()
	// Another publisher may have renamed the root selector and poisoned the
	// process while this rewind waited.  The WAL decision is already durable, so
	// ignore only the shutdown fence; never overwrite an ambiguous selector.
	if err := index.checkPoison(); err != nil {
		return err
	}
	view, err := index.view.Acquire()
	if err != nil {
		return index.setPoison(err)
	}
	defer view.Close()
	next := view.Catalog().Clone()
	if next.Generation == ^uint64(0) {
		return index.setPoison(errors.New("accountsdb: root catalog generation exhausted"))
	}
	next.Generation++
	next.Lineage = rewindLineage
	next.RootedBatchSequence = metaCopy.BatchSeq
	next.RootedSlot = metaCopy.ThroughSlot
	currentPayload, ok := view.Payload().(*ShardedImmutableIndex)
	if !ok || currentPayload == nil {
		return index.setPoison(errors.New("accountsdb: rewind found invalid immutable payload"))
	}
	nextPayload := currentPayload.cloneForSuccessor(next)
	nextPayload.resources = currentPayload.Resources()
	renamed, err := index.writeRootCatalog(next)
	if err != nil {
		if renamed {
			return index.setPoison(fmt.Errorf("accountsdb: publish rewind root lineage: %w", err))
		}
		// The WAL/meta is the authoritative rewind decision. No selector changed,
		// and rebaseIncarnation has invalidated every build from the prior runtime
		// incarnation. Completing the logical rewind is safer than returning an
		// error that would leave AccountsDb caches at the pre-rewind epoch. A
		// restart has no surviving builders, so the old advisory root lineage is
		// harmless; a later successful rewind/publication will rotate it normally.
		mlog.Log.Warnf(
			"accountsdb: rewind committed in WAL but root lineage publication failed before rename; continuing with runtime rebase fence: %v",
			err,
		)
		return nil
	}
	if err := index.view.Publish(next, nextPayload, nextPayload.Resources(), nil); err != nil {
		return index.setPoison(fmt.Errorf("accountsdb: install rewind root lineage: %w", err))
	}
	return nil
}

func foldMetaPrecedes(next, prior foldMeta) bool {
	if next.BatchSeq != prior.BatchSeq {
		return next.BatchSeq < prior.BatchSeq
	}
	if next.ThroughSlot != prior.ThroughSlot {
		return next.ThroughSlot < prior.ThroughSlot
	}
	return next.FileId < prior.FileId
}

func (index *ProductionAccountIndex) publishCheckpoint(
	ctx context.Context,
	publication ShardedMutableCheckpointPublication,
) error {
	if ctx == nil {
		return errors.New("accountsdb: nil checkpoint publication context")
	}
	if err := index.waitReady(ctx); err != nil {
		return err
	}
	if err := index.checkUsable(); err != nil {
		return err
	}
	index.publishMu.Lock()
	defer index.publishMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	// The pre-lock check is insufficient: a preceding publisher can poison the
	// index before releasing publishMu.  Recheck while holding the serialization
	// fence so a queued callback cannot overwrite an ambiguous durable selector.
	if err := index.checkUsable(); err != nil {
		return err
	}

	view, err := index.view.Acquire()
	if err != nil {
		return err
	}
	defer view.Close()
	current, ok := view.Payload().(*ShardedImmutableIndex)
	if !ok || current == nil {
		return index.setPoison(errors.New("accountsdb: checkpoint publication found invalid immutable payload"))
	}
	root := view.Catalog()
	if publication.ShardID >= root.ShardCount ||
		len(publication.ProposedCoveredSequences) != len(root.Shards) {
		return fmt.Errorf("%w: malformed checkpoint publication", ErrInvalidRootIndexCatalog)
	}
	if root.Generation == ^uint64(0) {
		return index.setPoison(errors.New("accountsdb: root catalog generation exhausted"))
	}
	artifacts, err := IdentifyShardedDeltaCheckpointArtifacts(
		index.root,
		publication.Directory,
		publication.Next,
	)
	if err != nil {
		return err
	}
	artifactBytes, err := checkpointArtifactSetBytes(artifacts)
	if err != nil {
		return err
	}
	if publication.artifactBytes != artifactBytes {
		return fmt.Errorf(
			"%w: shard %d built artifacts changed size from %d to %d before publication",
			ErrCheckpointResourceAccounting,
			publication.ShardID,
			publication.artifactBytes,
			artifactBytes,
		)
	}
	if err := index.checkpointBudget.validatePublication(
		publication.resourceReservation,
		publication.ShardID,
		artifactBytes,
	); err != nil {
		return err
	}
	if artifacts.CoveredSequence != publication.CoveredSequence ||
		publication.ProposedCoveredSequences[publication.ShardID] != publication.CoveredSequence {
		return fmt.Errorf(
			"%w: checkpoint shard %d physical/proposed coverage mismatch",
			ErrInvalidRootIndexCatalog,
			publication.ShardID,
		)
	}

	nextRoot := root.Clone()
	nextRoot.Generation++
	effectivePublication := publication
	effectivePublication.ProposedCoveredSequences = make([]uint64, len(root.Shards))
	for shardID := range nextRoot.Shards {
		prior := root.Shards[shardID].effectiveCoveredSequence()
		proposed := publication.ProposedCoveredSequences[shardID]
		if proposed > publication.CoveredSequence {
			return fmt.Errorf(
				"%w: shard %d checkpoint coverage %d exceeds physical cut %d",
				ErrInvalidRootIndexCatalog,
				shardID,
				proposed,
				publication.CoveredSequence,
			)
		}
		// Independent shard builds may finish out of freeze order. A later
		// publication can already have advanced an unrelated shard beyond this
		// build's cut; retain that logical coverage rather than rejecting or
		// regressing it. Same-shard builds remain serialized by mutable state.
		logicalCoverage := max(prior, proposed)
		effectivePublication.ProposedCoveredSequences[shardID] = logicalCoverage
		selected := &nextRoot.Shards[shardID]
		if uint32(shardID) == publication.ShardID {
			if prior > publication.CoveredSequence {
				return fmt.Errorf(
					"%w: shard %d current coverage %d exceeds selected checkpoint cut %d",
					ErrInvalidRootIndexCatalog,
					shardID,
					prior,
					publication.CoveredSequence,
				)
			}
			selected.DeltaGeneration = artifacts.Generation
			selected.DeltaCoveredSequence = artifacts.CoveredSequence
			selected.DeltaIndex = artifacts.Index
			selected.DeltaRecords = artifacts.Records
		} else if selected.DeltaGeneration != 0 {
			// An existing physical checkpoint may be older than this logical
			// watermark; no mutations for this shard occurred in the gap.
			selected.DeltaCoveredSequence = logicalCoverage
		} else {
			selected.BaseCoveredSequence = logicalCoverage
		}
	}
	nextRoot.CoveredSequence = minimumEffectiveCoverage(nextRoot)
	if err := nextRoot.Validate(); err != nil {
		return err
	}

	nextPayload, resources, obsolete, err := current.DeriveWithCheckpoint(nextRoot, effectivePublication)
	if err != nil {
		return err
	}
	if publication.resourceReservation.previousSelectedBytes != 0 && len(obsolete) != 1 {
		return fmt.Errorf(
			"%w: checkpoint replacement has %d obsolete generation resources, want 1",
			ErrCheckpointResourceAccounting,
			len(obsolete),
		)
	}
	discard := true
	defer func() {
		if discard {
			index.discardDerivedResources(current.Resources(), resources)
		}
	}()
	renamed, err := index.writeRootCatalog(nextRoot)
	var obsoleteCheckpointBytes uint64
	if renamed {
		// All fallible accounting validation happened before the selector
		// rename. Transfer build -> selected and selected-old -> obsolete at
		// exactly the durable commit point, even if the following directory
		// sync or in-process view installation reports an ambiguity.
		obsoleteCheckpointBytes = index.checkpointBudget.commitPublication(
			publication.resourceReservation,
		)
	}
	if err != nil {
		if renamed {
			return index.setPoison(fmt.Errorf("accountsdb: publish checkpoint root catalog: %w", err))
		}
		return fmt.Errorf("accountsdb: publish checkpoint root catalog before rename: %w", err)
	}
	if err := index.view.Publish(nextRoot, nextPayload, resources, obsolete); err != nil {
		// The durable selector already names nextRoot. Continuing with a
		// different in-process root would violate the restart contract.
		discard = false // Publish owns asynchronous cleanup on every failure path.
		return index.setPoison(fmt.Errorf("accountsdb: install checkpoint root generation: %w", err))
	}
	discard = false
	if obsoleteCheckpointBytes != 0 {
		index.trackObsoleteCheckpoint(obsolete[0], obsoleteCheckpointBytes)
	}
	return nil
}

func (index *ProductionAccountIndex) rebaseShard(
	ctx context.Context,
	request ShardedMutableRebaseRequest,
) (retErr error) {
	if ctx == nil {
		return errors.New("accountsdb: nil shard rebase context")
	}
	if err := index.waitReady(ctx); err != nil {
		return err
	}
	if err := index.checkUsable(); err != nil {
		return err
	}
	sourceIncarnation := index.rebaseIncarnation.Load()
	select {
	case index.rebaseGate <- struct{}{}:
		defer func() { <-index.rebaseGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	// A reader-pinned base can be very large. Do not allocate another rolling
	// replacement until every previously obsolete base is fully closed/deleted.
	// The global gate deliberately bounds obsolete base generations to one; delta
	// checkpoint churn has its separate exact physical-byte budget.
	if err := index.waitForObsoleteBases(ctx); err != nil {
		return err
	}
	if err := index.checkUsable(); err != nil {
		return err
	}

	sourceView, err := index.view.Acquire()
	if err != nil {
		return err
	}
	defer sourceView.Close()
	sourceRoot := sourceView.Catalog().Clone()
	sourcePayload, ok := sourceView.Payload().(*ShardedImmutableIndex)
	if !ok || sourcePayload == nil {
		return errors.New("accountsdb: rebase found invalid immutable payload")
	}
	if request.ShardID >= sourceRoot.ShardCount || len(request.CoveredSequences) != len(sourceRoot.Shards) {
		return fmt.Errorf("%w: malformed shard rebase request", ErrInvalidRootIndexCatalog)
	}
	sourceShard := &sourcePayload.shards[request.ShardID]
	if request.Checkpoint == nil || sourceShard.deltaHandle != request.Checkpoint ||
		request.Checkpoint.Checkpoint() == nil ||
		request.Checkpoint.Checkpoint().CoveredSeq() != request.CoveredSequence {
		return fmt.Errorf("%w: shard %d rebase checkpoint is not root-selected", ErrInvalidRootIndexCatalog, request.ShardID)
	}
	if request.CoveredSequences[request.ShardID] != sourceRoot.Shards[request.ShardID].effectiveCoveredSequence() ||
		request.CoveredSequences[request.ShardID] < request.CoveredSequence {
		return fmt.Errorf("%w: shard %d rebase logical coverage mismatch", ErrInvalidRootIndexCatalog, request.ShardID)
	}
	oldCheckpointBytes, err := checkpointSelectedShardBytes(sourceRoot.Shards[request.ShardID])
	if err != nil {
		return err
	}
	if err := index.checkpointBudget.validateRebase(request.ShardID, oldCheckpointBytes); err != nil {
		return err
	}
	if sourceShard.base == nil {
		return fmt.Errorf("%w: shard %d rebase has no immutable base", ErrInvalidRootIndexCatalog, request.ShardID)
	}
	merge, err := newMergedShardIndexSource(sourceShard.base, request.Checkpoint.Checkpoint())
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if sourcePayload.extents.Generation() == ^uint64(0) {
		return errors.New("accountsdb: extent catalog generation exhausted")
	}
	build, err := BuildShardedStreamBaseShardWithShardCount(
		ctx,
		merge,
		index.root,
		sourcePayload.extents.Generation()+1,
		int(sourceRoot.ShardCount),
		sourceRoot.RoutingKey,
		request.ShardID,
		sourcePayload.extents,
		index.config.CheckpointWorkers,
	)
	if err != nil {
		return fmt.Errorf("accountsdb: rebuild StreamHash shard %d: %w", request.ShardID, err)
	}
	if build.RoutingKey != sourceRoot.RoutingKey {
		return fmt.Errorf("%w: rebuilt shard changed persistent routing key", ErrInvalidRootIndexCatalog)
	}
	buildUnowned := true
	defer func() {
		if buildUnowned {
			if cleanupErr := removeRebaseBuild(index.root, build); cleanupErr != nil {
				// An unselected build is normally safe to discard, but a failed
				// exact cleanup can accumulate multi-gigabyte base artifacts on
				// every retry. Fence maintenance immediately; startup's
				// identity-checked orphan collector can reconcile the residue.
				retErr = errors.Join(
					retErr,
					index.setPoison(fmt.Errorf(
						"accountsdb: reclaim unselected shard %d rebase build: %w",
						request.ShardID,
						cleanupErr,
					)),
				)
			}
		}
	}()

	index.publishMu.Lock()
	defer index.publishMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	// Rebase construction is intentionally outside publishMu and can be long.
	// Revalidate after acquiring it so a build cannot publish across a poison or
	// shutdown that occurred while it was building or queued.
	if err := index.checkUsable(); err != nil {
		return err
	}
	latestView, err := index.view.Acquire()
	if err != nil {
		return err
	}
	defer latestView.Close()
	latestRoot := latestView.Catalog()
	latestPayload, ok := latestView.Payload().(*ShardedImmutableIndex)
	if !ok || latestPayload == nil {
		return errors.New("accountsdb: rebase publication found invalid immutable payload")
	}
	// Root generation may have advanced for unrelated checkpoint publications,
	// but the source shard, extent lineage and rooted chain incarnation must be
	// exactly the ones against which this build was produced.
	if index.rebaseIncarnation.Load() != sourceIncarnation ||
		latestRoot.Lineage != sourceRoot.Lineage ||
		latestRoot.RoutingKey != sourceRoot.RoutingKey ||
		latestRoot.SharedExtentCatalog != sourceRoot.SharedExtentCatalog ||
		latestRoot.Shards[request.ShardID] != sourceRoot.Shards[request.ShardID] {
		return fmt.Errorf("%w: shard %d rebase lost its root compare-and-swap", ErrIndexGenerationRejected, request.ShardID)
	}
	if latestRoot.Generation == ^uint64(0) {
		return index.setPoison(errors.New("accountsdb: root catalog generation exhausted"))
	}

	nextRoot := latestRoot.Clone()
	nextRoot.Generation++
	nextRoot.SharedExtentCatalog = build.ExtentCatalogArtifact
	selected := &nextRoot.Shards[request.ShardID]
	oldBaseCoverage := selected.BaseCoveredSequence
	selected.BaseGeneration = build.Shard.Generation
	selected.BaseCoveredSequence = request.CoveredSequences[request.ShardID]
	selected.BaseIndex = build.Shard.Index
	selected.BaseRecords = build.Shard.Scan
	selected.DeltaGeneration = 0
	selected.DeltaCoveredSequence = 0
	selected.DeltaIndex = IndexCatalogArtifact{}
	selected.DeltaRecords = IndexCatalogArtifact{}
	nextRoot.CoveredSequence = minimumEffectiveCoverage(nextRoot)
	if err := nextRoot.Validate(); err != nil {
		return err
	}

	oldBaseResource := latestPayload.shards[request.ShardID].baseResource
	oldDeltaResource := latestPayload.shards[request.ShardID].deltaResource
	if oldBaseResource == nil || oldDeltaResource == nil {
		return fmt.Errorf(
			"%w: rebase shard %d is missing its selected base or delta generation resource",
			ErrCheckpointResourceAccounting,
			request.ShardID,
		)
	}
	nextPayload, resources, obsolete, err := latestPayload.DeriveWithRebase(nextRoot, build)
	if err != nil {
		return err
	}
	discard := true
	defer func() {
		if discard {
			index.discardDerivedResources(latestPayload.Resources(), resources)
		}
	}()
	renamed, err := index.writeRootCatalog(nextRoot)
	committedObsoleteCheckpointBytes := uint64(0)
	if renamed {
		committedObsoleteCheckpointBytes = index.checkpointBudget.commitRebase(request.ShardID)
	}
	if err != nil {
		if renamed {
			// The selector may durably name this build. It is no longer safe for
			// either rejection path to unlink the selected artifacts.
			buildUnowned = false
			return index.setPoison(fmt.Errorf("accountsdb: publish rebased root catalog: %w", err))
		}
		// Nothing selected the build. Drain its private mappings before deleting
		// exactly the three immutable artifacts it produced; old/root-pinned
		// generations are neither enumerated nor touched.
		discard = false
		drainErr := discardDerivedResourcesAndWait(latestPayload.Resources(), resources)
		buildUnowned = false
		if drainErr != nil {
			// A failed close/unmap means the process cannot prove that unlinking is
			// safe. Fail closed and retain the unselected artifacts for the exact,
			// identity-checked startup orphan collector.
			return index.setPoison(errors.Join(
				fmt.Errorf("accountsdb: publish rebased root catalog before rename: %w", err),
				fmt.Errorf("accountsdb: drain rejected rebase build: %w", drainErr),
			))
		}
		cleanupErr := removeRebaseBuild(index.root, build)
		publicationErr := fmt.Errorf(
			"accountsdb: publish rebased root catalog before rename: %w",
			err,
		)
		if cleanupErr != nil {
			// Retrying after an exact delete failure would manufacture one
			// additional full base generation on every attempt. Retain the
			// residue for startup orphan collection and stop maintenance.
			return index.setPoison(errors.Join(
				publicationErr,
				fmt.Errorf("accountsdb: reclaim rejected shard %d rebase build: %w", request.ShardID, cleanupErr),
			))
		}
		return publicationErr
	}
	buildUnowned = false
	if err := index.view.Publish(nextRoot, nextPayload, resources, obsolete); err != nil {
		discard = false
		return index.setPoison(fmt.Errorf("accountsdb: install rebased root generation: %w", err))
	}
	discard = false
	if committedObsoleteCheckpointBytes != oldCheckpointBytes {
		panic("accountsdb: validated checkpoint rebase accounting changed at commit")
	}
	index.trackObsoleteCheckpoint(oldDeltaResource, committedObsoleteCheckpointBytes)
	baseRetirement := []*IndexGenerationResource{oldBaseResource}
	if latestPayload.catalogResource != nextPayload.catalogResource {
		baseRetirement = append(baseRetirement, latestPayload.catalogResource)
	}
	index.trackObsoleteBase(baseRetirement, oldBaseCoverage)
	return nil
}

func (index *ProductionAccountIndex) trackObsoleteCheckpoint(
	resource *IndexGenerationResource,
	bytes uint64,
) {
	if index == nil || index.checkpointBudget == nil || resource == nil || bytes == 0 {
		return
	}
	index.resourceWatcherWG.Add(1)
	go func() {
		defer index.resourceWatcherWG.Done()
		<-resource.Done()
		if err := resource.Err(); err != nil {
			// A close/delete failure means the bytes may still physically exist.
			// Keep them charged and fail closed instead of manufacturing headroom.
			index.recordResourceWatcherError(fmt.Errorf(
				"accountsdb: reclaim obsolete delta checkpoint %q: %w",
				resource.Name(),
				err,
			))
			return
		}
		if err := index.checkpointBudget.releaseObsolete(bytes); err != nil {
			index.recordResourceWatcherError(err)
			return
		}
		if index.mutable != nil {
			index.mutable.wakeMaintenanceLoop()
		}
	}()
}

func minimumEffectiveCoverage(catalog *RootIndexCatalog) uint64 {
	minimum := ^uint64(0)
	for i := range catalog.Shards {
		minimum = min(minimum, catalog.Shards[i].effectiveCoveredSequence())
	}
	if minimum == ^uint64(0) {
		return 0
	}
	return minimum
}

func minimumBaseCoverage(catalog *RootIndexCatalog) uint64 {
	minimum := ^uint64(0)
	for i := range catalog.Shards {
		minimum = min(minimum, catalog.Shards[i].BaseCoveredSequence)
	}
	if minimum == ^uint64(0) {
		return 0
	}
	return minimum
}

func discardDerivedResources(
	current []*IndexGenerationResource,
	derived []*IndexGenerationResource,
) {
	retained := make(map[*IndexGenerationResource]struct{}, len(current))
	for _, resource := range current {
		retained[resource] = struct{}{}
	}
	for _, resource := range derived {
		if resource == nil {
			continue
		}
		if _, shared := retained[resource]; shared {
			continue
		}
		if err := resource.retain(); err != nil {
			continue
		}
		// Checkpoint resources can legitimately wait for the mutable owner's
		// release. Never put that wait on a failed publication's caller stack.
		go func(resource *IndexGenerationResource) { _ = resource.release() }(resource)
	}
}

// discardDerivedResources starts cleanup for resources owned only by a rejected
// derived payload and accounts that work in the production index lifetime. The
// callback which schedules it may need to return before cleanup completes (a
// delta resource can be waiting for the mutable builder's handle owner), but
// shutdown must never release the store lock first or lose a cleanup failure.
func (index *ProductionAccountIndex) discardDerivedResources(
	current []*IndexGenerationResource,
	derived []*IndexGenerationResource,
) {
	if index == nil {
		discardDerivedResources(current, derived)
		return
	}
	retained := make(map[*IndexGenerationResource]struct{}, len(current))
	for _, resource := range current {
		retained[resource] = struct{}{}
	}
	for _, resource := range derived {
		if resource == nil {
			continue
		}
		if _, shared := retained[resource]; shared {
			continue
		}
		if err := resource.retain(); err != nil {
			index.recordDiscardError(fmt.Errorf(
				"accountsdb: retain rejected generation resource %q for cleanup: %w",
				resource.Name(),
				err,
			))
			continue
		}
		index.discardWG.Add(1)
		go func(resource *IndexGenerationResource) {
			defer index.discardWG.Done()
			if err := resource.release(); err != nil {
				index.recordDiscardError(fmt.Errorf(
					"accountsdb: close rejected generation resource %q: %w",
					resource.Name(),
					err,
				))
			}
		}(resource)
	}
}

func (index *ProductionAccountIndex) recordDiscardError(err error) {
	if index == nil || err == nil {
		return
	}
	index.discardErrMu.Lock()
	index.discardErr = errors.Join(index.discardErr, err)
	index.discardErrMu.Unlock()
	index.setPoison(err)
}

func (index *ProductionAccountIndex) waitForDiscardedResources() error {
	if index == nil {
		return nil
	}
	index.discardWG.Wait()
	index.discardErrMu.Lock()
	err := index.discardErr
	index.discardErrMu.Unlock()
	return err
}

func (index *ProductionAccountIndex) recordResourceWatcherError(err error) {
	if index == nil || err == nil {
		return
	}
	index.resourceWatcherErrMu.Lock()
	index.resourceWatcherErr = errors.Join(index.resourceWatcherErr, err)
	index.resourceWatcherErrMu.Unlock()
	index.setPoison(err)
}

func (index *ProductionAccountIndex) waitForResourceWatchers() error {
	if index == nil {
		return nil
	}
	index.resourceWatcherWG.Wait()
	index.resourceWatcherErrMu.Lock()
	err := index.resourceWatcherErr
	index.resourceWatcherErrMu.Unlock()
	return err
}

func discardDerivedResourcesAndWait(
	current []*IndexGenerationResource,
	derived []*IndexGenerationResource,
) error {
	retained := make(map[*IndexGenerationResource]struct{}, len(current))
	for _, resource := range current {
		retained[resource] = struct{}{}
	}
	created := make([]*IndexGenerationResource, 0, len(derived))
	for _, resource := range derived {
		if resource == nil {
			continue
		}
		if _, shared := retained[resource]; !shared {
			created = append(created, resource)
		}
	}
	discardDerivedResources(current, derived)
	var result error
	for _, resource := range created {
		<-resource.Done()
		result = errors.Join(result, resource.Err())
	}
	return result
}

func removeRebaseBuild(root string, build *ShardedStreamBaseShardBuildResult) error {
	if build == nil {
		return nil
	}
	artifacts := []IndexCatalogArtifact{build.Shard.Index, build.Shard.Scan}
	if build.OwnsExtentCatalogArtifact {
		artifacts = append([]IndexCatalogArtifact{build.ExtentCatalogArtifact}, artifacts...)
	}
	return removeImmutableArtifacts(root, artifacts)
}

func (index *ProductionAccountIndex) trackObsoleteBase(
	resources []*IndexGenerationResource,
	coveredThrough uint64,
) {
	if len(resources) == 0 {
		return
	}
	owned := make([]*IndexGenerationResource, 0, len(resources))
	seen := make(map[*IndexGenerationResource]struct{}, len(resources))
	for _, resource := range resources {
		if resource == nil {
			continue
		}
		if _, duplicate := seen[resource]; duplicate {
			continue
		}
		seen[resource] = struct{}{}
		owned = append(owned, resource)
	}
	if len(owned) == 0 {
		return
	}
	index.gcMu.Lock()
	index.obsoleteBases = append(index.obsoleteBases, productionObsoleteBase{
		resources: owned, coveredThrough: coveredThrough,
	})
	index.gcMu.Unlock()
	for _, resource := range owned {
		index.resourceWatcherWG.Add(1)
		go func(resource *IndexGenerationResource) {
			defer index.resourceWatcherWG.Done()
			<-resource.Done()
			if err := resource.Err(); err != nil {
				// A failed close or delete can leave an entire obsolete base
				// generation resident or on disk. Do not let later rebases keep
				// accumulating artifacts until the filesystem fills; poison the
				// writer as soon as any group member's finalizer reports failure.
				index.recordResourceWatcherError(fmt.Errorf(
					"accountsdb: reclaim obsolete base-generation resource %q: %w",
					resource.Name(),
					err,
				))
			}
			index.tryPruneRetirementMarkers()
		}(resource)
	}
	index.tryPruneRetirementMarkers()
}

// waitForObsoleteBases is called while rebaseGate is held, before a new base
// build starts. Since every successful rebase registers its obsolete base
// before releasing that gate, observing no pending resource here is a complete
// admission decision rather than a racy snapshot.
func (index *ProductionAccountIndex) waitForObsoleteBases(ctx context.Context) error {
	if index == nil {
		return errors.New("accountsdb: nil production account index")
	}
	if ctx == nil {
		return errors.New("accountsdb: nil obsolete-base wait context")
	}
	for {
		var pending *IndexGenerationResource
		index.gcMu.Lock()
		for _, obsolete := range index.obsoleteBases {
			for _, resource := range obsolete.resources {
				select {
				case <-resource.Done():
					if err := resource.Err(); err != nil {
						index.gcMu.Unlock()
						return index.setPoison(fmt.Errorf(
							"accountsdb: reclaim obsolete base-generation resource %q: %w",
							resource.Name(),
							err,
						))
					}
				default:
					pending = resource
				}
				if pending != nil {
					break
				}
			}
			if pending != nil {
				break
			}
		}
		index.gcMu.Unlock()
		if pending == nil {
			index.tryPruneRetirementMarkers()
			return index.checkUsable()
		}
		select {
		case <-pending.Done():
			if err := pending.Err(); err != nil {
				return index.setPoison(fmt.Errorf(
					"accountsdb: reclaim obsolete base-generation resource %q: %w",
					pending.Name(),
					err,
				))
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (index *ProductionAccountIndex) tryPruneRetirementMarkers() {
	if index == nil || index.closed.Load() || index.mutable == nil {
		return
	}
	view, err := index.view.Acquire()
	if err != nil {
		return
	}
	safe := minimumBaseCoverage(view.Catalog())
	_ = view.Close()

	index.gcMu.Lock()
	defer index.gcMu.Unlock()
	pending := index.obsoleteBases[:0]
	for _, obsolete := range index.obsoleteBases {
		generationPending := false
		for _, resource := range obsolete.resources {
			select {
			case <-resource.Done():
				if err := resource.Err(); err != nil {
					// Poison before removing this completed retirement group. Otherwise
					// a successful sibling finalizer could prune the group and briefly
					// admit another rebase before its failing sibling watcher runs.
					index.setPoison(fmt.Errorf(
						"accountsdb: reclaim obsolete base-generation resource %q: %w",
						resource.Name(),
						err,
					))
					return
				}
			default:
				generationPending = true
			}
		}
		if generationPending {
			// This old base can require retirement markers newer than its own
			// coverage until its last reader drains.
			safe = min(safe, obsolete.coveredThrough)
			pending = append(pending, obsolete)
		}
	}
	index.obsoleteBases = pending
	if safe <= index.lastRetirementPrune || safe == 0 || index.closed.Load() {
		return
	}
	if err := index.mutable.PruneRetiredThrough(context.Background(), safe); err != nil {
		if !errors.Is(err, ErrShardedMutableClosed) {
			index.setPoison(fmt.Errorf("accountsdb: prune retirement markers through %d: %w", safe, err))
		}
		return
	}
	index.lastRetirementPrune = safe
}

func (index *ProductionAccountIndex) ReadFoldMeta() (foldMeta, bool) {
	if index == nil || index.mutable == nil {
		return foldMeta{}, false
	}
	return index.mutable.ReadFoldMeta()
}

func (index *ProductionAccountIndex) IsRetired(slot, fileID uint64) bool {
	return index != nil && index.mutable != nil && index.mutable.IsRetired(slot, fileID)
}

func (index *ProductionAccountIndex) Len() int {
	if index == nil || index.mutable == nil {
		return 0
	}
	return index.mutable.Len()
}

func (index *ProductionAccountIndex) Stats() ShardedMutableAccountIndexStats {
	if index == nil || index.mutable == nil {
		return ShardedMutableAccountIndexStats{}
	}
	return index.mutable.Stats()
}

func (index *ProductionAccountIndex) ForceSeal(ctx context.Context) error {
	if err := index.checkUsable(); err != nil {
		return err
	}
	return index.mutable.ForceSeal(ctx)
}

// Shutdown permanently refuses new operations, stops mutable maintenance, and
// drains every immutable generation before releasing the exclusive store
// lock. The teardown itself continues after ctx expires. A later call waits
// for the same teardown, making a leaked-reader timeout observable and
// retryable without ever exposing the store to a second process prematurely.
func (index *ProductionAccountIndex) Shutdown(ctx context.Context) error {
	if index == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("accountsdb: nil production account-index shutdown context")
	}
	index.shutdownOnce.Do(func() {
		if index.shutdownDone == nil {
			index.shutdownDone = make(chan struct{})
		}
		index.closed.Store(true)
		go index.finishShutdown()
	})
	done := index.shutdownDone

	// Prefer a completed teardown over an already-cancelled context when both
	// are observable, so callers receive any accumulated cleanup error.
	select {
	case <-done:
		return index.shutdownErr
	default:
	}
	select {
	case <-done:
		return index.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (index *ProductionAccountIndex) finishShutdown() {
	var shutdownErr error
	// Fence/cancel the mutable layer before waiting for the outer writer lock. An
	// Apply already holding applyMu can be backpressured on a rebase or checkpoint
	// resource; cancellation wakes that Apply instead of making shutdown wait for
	// an unrelated reader-pinned obsolete generation before it can begin teardown.
	if index.mutable != nil {
		index.mutable.beginClose()
	}
	// Apply rechecks the top-level closed fence after acquiring applyMu. Taking it
	// here waits out an in-flight WAL/root commit while every queued/new writer
	// fails before appending.
	index.applyMu.Lock()
	if index.mutable != nil {
		shutdownErr = errors.Join(shutdownErr, index.mutable.Close())
	}
	index.applyMu.Unlock()
	// A failed publication can leave candidate-generation resources whose
	// teardown had to begin asynchronously to avoid waiting on a handle still
	// owned by the mutable callback caller. mutable.Close above proves every such
	// callback has returned, so no further Add can race this drain. Keep the store
	// lock until all candidate mappings have closed and surface every failure.
	shutdownErr = errors.Join(shutdownErr, index.waitForDiscardedResources())

	// Shutdown waits for current, retired, and rejected-generation cleanup.
	// The store lock remains held across a leaked reader and every unmap/delete.
	if index.view != nil {
		shutdownErr = errors.Join(shutdownErr, index.view.Shutdown(context.Background()))
	}
	// Manager shutdown closes every obsolete generation resource and therefore
	// unblocks all registered retirement watchers. Join their accounting/error
	// work before releasing exclusive ownership of the store.
	shutdownErr = errors.Join(shutdownErr, index.waitForResourceWatchers())
	if index.storeLock != nil {
		shutdownErr = errors.Join(shutdownErr, index.storeLock.Close())
		index.storeLock = nil
	}
	index.shutdownErr = shutdownErr
	close(index.shutdownDone)
}

func (index *ProductionAccountIndex) shutdownComplete() bool {
	if index == nil {
		return true
	}
	done := index.shutdownDone
	if done == nil {
		return false
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// Close preserves the unbounded io.Closer-style API.
func (index *ProductionAccountIndex) Close() error {
	return index.Shutdown(context.Background())
}
