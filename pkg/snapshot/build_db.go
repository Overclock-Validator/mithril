package snapshot

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/panjf2000/ants/v2"
)

const (
	DefaultSnapshotIndexEntryCommitterWorkers = 64
	DefaultSnapshotIndexEntryBuilderWorkers   = 64
	DefaultSnapshotAppendVecCopyingWorkers    = 32
	DefaultSnapshotIndexShards                = 64
	DefaultSnapshotMaxConcurrentFlushers      = 8
	defaultSnapshotIndexEntryBatchSize        = 20_000
	SnapshotIndexRunDirName                   = "accounts_index_runs"
)

var (
	SnapshotIndexEntryCommitterWorkers = DefaultSnapshotIndexEntryCommitterWorkers
	SnapshotIndexEntryBuilderWorkers   = DefaultSnapshotIndexEntryBuilderWorkers
	SnapshotAppendVecCopyingWorkers    = DefaultSnapshotAppendVecCopyingWorkers
	SnapshotIndexShards                = DefaultSnapshotIndexShards
	SnapshotIndexTempDir               string
)

// CleanAccountsDbDir removes all artifacts from a previous incomplete snapshot
// run while holding the V2 store-wide exclusive lock. It deliberately leaves
// the stable lockfile in place: unlinking it could split ownership between two
// processes holding different inodes.
func CleanAccountsDbDir(accountsDbDir string) error {
	if err := os.MkdirAll(accountsDbDir, 0o775); err != nil {
		return fmt.Errorf("create AccountsDB cleanup root: %w", err)
	}
	return accountsdb.WithExclusiveProductionAccountIndexStore(accountsDbDir, func() error {
		return cleanAccountsDbDirLocked(accountsDbDir)
	})
}

func beginSnapshotBootstrap(
	accountsDbDir string,
) (*accountsdb.ProductionAccountIndexStoreGuard, error) {
	if err := os.MkdirAll(accountsDbDir, 0o775); err != nil {
		return nil, fmt.Errorf("create AccountsDB bootstrap root: %w", err)
	}
	guard, err := accountsdb.AcquireExclusiveProductionAccountIndexStore(accountsDbDir)
	if err != nil {
		return nil, err
	}
	if err := cleanAccountsDbDirLocked(accountsDbDir); err != nil {
		return nil, errors.Join(err, guard.Close())
	}
	return guard, nil
}

func cleanAccountsDbDirLocked(accountsDbDir string) error {
	var cleanupErr error
	record := func(path string, err error) {
		if err == nil || os.IsNotExist(err) {
			return
		}
		mlog.Log.Errorf("failed to remove %s: %v", path, err)
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove %s: %w", path, err))
	}
	removeRegular := func(path string) {
		info, err := os.Lstat(path)
		if err != nil {
			record(path, err)
			return
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return
		}
		record(path, os.Remove(path))
	}
	removeRealDirectory := func(path string) {
		info, err := os.Lstat(path)
		if err != nil {
			record(path, err)
			return
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return
		}
		record(path, os.RemoveAll(path))
	}
	// List of all files/directories that may be left from a previous incomplete run
	artifacts := []string{
		"accounts",
		txstatus.SnapshotSeedFileName,
		// Rooted-durable replay writes immutable transaction-status sidecars
		// beside AccountsDB. A snapshot rebuild replaces their chain lineage,
		// so retaining them would waste space and make stale diagnostics look
		// actionable.
		"transaction-status-checkpoints",
		"mithril_db",
		"mithril_db_log_shards",
		SnapshotIndexRunDirName,
		"mithril_db_build", // legacy temporary Pebble snapshot-index source
		accountsdb.StreamIndexFileName,
		accountsdb.StreamIndexFileName + ".partial",
		accountsdb.StreamIndexManifestFileName,
		accountsdb.StreamIndexManifestFileName + ".tmp",
		accountsdb.DeltaIndexJournalFileName,
		accountsdb.DeltaIndexJournalRewriteFileName,
		accountsdb.RootIndexCatalogFileName,
		accountsdb.ShardedDeltaIndexJournalFileName,
		accountsdb.ShardedDeltaIndexJournalRewriteFileName,
		accountsdb.ShardedDeltaCheckpointDirName,
		"bankhash_db",
		"largest_file_id",
		"bootstrap_high_file_id",
		"bank_hash",
		"stake_pubkeys.idx",
		"manifest",
		"mithril_state.json", // State file for tracking valid builds and replay progress
	}
	for _, artifact := range artifacts {
		path := filepath.Join(accountsDbDir, artifact)
		record(path, os.RemoveAll(path))
	}
	partials, err := filepath.Glob(filepath.Join(accountsDbDir, ".snapshot-status-cache-*.partial"))
	cleanupErr = errors.Join(cleanupErr, err)
	for _, partial := range partials {
		if isDecimalTemporaryName(filepath.Base(partial), ".snapshot-status-cache-", ".partial") {
			removeRegular(partial)
		}
	}
	collisionLists, err := filepath.Glob(filepath.Join(accountsDbDir, "."+accountsdb.StreamIndexFileName+".collisions-*"))
	cleanupErr = errors.Join(cleanupErr, err)
	for _, collisionList := range collisionLists {
		if isDecimalTemporaryName(filepath.Base(collisionList), "."+accountsdb.StreamIndexFileName+".collisions-", "") {
			removeRegular(collisionList)
		}
	}
	streamHashParts, err := filepath.Glob(filepath.Join(accountsDbDir, "streamhash-parts-*"))
	cleanupErr = errors.Join(cleanupErr, err)
	for _, partsDir := range streamHashParts {
		if isDecimalTemporaryName(filepath.Base(partsDir), "streamhash-parts-", "") {
			removeRealDirectory(partsDir)
		}
	}
	deltaCheckpoints, err := filepath.Glob(filepath.Join(accountsDbDir, accountsdb.DeltaCheckpointFilePrefix+"*"))
	cleanupErr = errors.Join(cleanupErr, err)
	for _, checkpoint := range deltaCheckpoints {
		if isDeltaCheckpointArtifactFileName(filepath.Base(checkpoint)) {
			removeRegular(checkpoint)
		}
	}
	deltaBuildDirs, err := filepath.Glob(filepath.Join(accountsDbDir, accountsdb.DeltaCheckpointBuildDirPrefix+"*"))
	cleanupErr = errors.Join(cleanupErr, err)
	for _, buildDir := range deltaBuildDirs {
		if isDecimalTemporaryName(filepath.Base(buildDir), accountsdb.DeltaCheckpointBuildDirPrefix, "") {
			removeRealDirectory(buildDir)
		}
	}
	rootCatalogPartials, err := filepath.Glob(filepath.Join(accountsDbDir, accountsdb.RootIndexCatalogFileName+".tmp-*"))
	cleanupErr = errors.Join(cleanupErr, err)
	for _, partial := range rootCatalogPartials {
		if isDecimalTemporaryName(filepath.Base(partial), accountsdb.RootIndexCatalogFileName+".tmp-", "") {
			removeRegular(partial)
		}
	}
	bootstrapHighPartials, err := filepath.Glob(filepath.Join(accountsDbDir, ".bootstrap-high-*.tmp"))
	cleanupErr = errors.Join(cleanupErr, err)
	for _, partial := range bootstrapHighPartials {
		if !isBootstrapHighTempFileName(filepath.Base(partial)) {
			continue
		}
		removeRegular(partial)
	}
	largestFileIDPartials, err := filepath.Glob(filepath.Join(accountsDbDir, ".largest-file-id-*.tmp"))
	cleanupErr = errors.Join(cleanupErr, err)
	for _, partial := range largestFileIDPartials {
		if isDecimalTemporaryName(filepath.Base(partial), ".largest-file-id-", ".tmp") {
			removeRegular(partial)
		}
	}
	entries, err := os.ReadDir(accountsDbDir)
	if err != nil && !os.IsNotExist(err) {
		mlog.Log.Errorf("failed to list AccountsDB V2 base generations in %s: %v", accountsDbDir, err)
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("list V2 base generations: %w", err))
	}
	for _, entry := range entries {
		if !accountsdb.IsProductionAccountIndexGenerationDirectory(entry.Name()) {
			continue
		}
		generationDir := filepath.Join(accountsDbDir, entry.Name())
		record(generationDir, os.RemoveAll(generationDir))
	}
	directory, err := os.Open(accountsDbDir)
	if err != nil {
		return errors.Join(cleanupErr, fmt.Errorf("open AccountsDB directory for cleanup sync: %w", err))
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(cleanupErr, syncErr, closeErr)
}

// CleanSnapshotDownloadDir removes old snapshot files based on retention settings.
// maxSnapshots controls how many snapshots to keep:
//   - 0 = delete all snapshots (stream-only mode, used by new-snapshot bootstrap)
//   - N > 0 = keep N newest snapshots, delete the rest
func CleanSnapshotDownloadDir(downloadPath string, maxSnapshots int) {
	if downloadPath == "" || maxSnapshots < 0 {
		return
	}
	entries, err := os.ReadDir(downloadPath)
	if err != nil {
		return // Directory may not exist yet
	}

	// Always clean up .partial files first (incomplete downloads from crashes)
	for _, entry := range entries {
		name := entry.Name()
		baseName := strings.TrimSuffix(name, PartialSuffix)
		_, fullErr := parseFullSnapshotArchiveSlot(baseName)
		_, _, incrementalErr := parseIncrementalSnapshotArchiveSlots(baseName)
		if strings.HasSuffix(name, PartialSuffix) && (fullErr == nil || incrementalErr == nil) && entry.Type().IsRegular() {
			path := filepath.Join(downloadPath, entry.Name())
			mlog.Log.Infof("Cleaning up incomplete download from previous run: %s", entry.Name())
			if err := os.Remove(path); err != nil {
				mlog.Log.Errorf("Failed to remove partial download %s: %v", entry.Name(), err)
			}
		}
	}

	// Collect snapshot files with their info
	type snapshotFile struct {
		name    string
		path    string
		modTime time.Time
	}
	var fullSnapshots []snapshotFile
	var incrSnapshots []snapshotFile

	for _, entry := range entries {
		name := entry.Name()
		if _, err := parseFullSnapshotArchiveSlot(name); err == nil && entry.Type().IsRegular() {
			path := filepath.Join(downloadPath, name)
			info, err := entry.Info()
			if err != nil {
				continue
			}
			fullSnapshots = append(fullSnapshots, snapshotFile{name, path, info.ModTime()})
		}
		if _, _, err := parseIncrementalSnapshotArchiveSlots(name); err == nil && entry.Type().IsRegular() {
			path := filepath.Join(downloadPath, name)
			info, err := entry.Info()
			if err != nil {
				continue
			}
			incrSnapshots = append(incrSnapshots, snapshotFile{name, path, info.ModTime()})
		}
	}

	// Sort by modification time (newest first)
	sortByTime := func(files []snapshotFile) {
		for i := 0; i < len(files)-1; i++ {
			for j := i + 1; j < len(files); j++ {
				if files[j].modTime.After(files[i].modTime) {
					files[i], files[j] = files[j], files[i]
				}
			}
		}
	}

	// maxSnapshots = 0 means delete ALL (stream-only mode, used by new-snapshot bootstrap)
	if maxSnapshots == 0 {
		for _, snap := range fullSnapshots {
			if err := os.Remove(snap.path); err != nil {
				mlog.Log.Errorf("failed to remove snapshot %s: %v", snap.name, err)
			} else {
				mlog.Log.Infof("Cleaned up snapshot file: %s", snap.name)
			}
		}
		for _, snap := range incrSnapshots {
			if err := os.Remove(snap.path); err != nil {
				mlog.Log.Errorf("failed to remove incremental snapshot %s: %v", snap.name, err)
			} else {
				mlog.Log.Infof("Cleaned up incremental snapshot file: %s", snap.name)
			}
		}
		return
	}

	// maxSnapshots > 0: keep N newest, delete the rest
	if len(fullSnapshots) > maxSnapshots {
		sortByTime(fullSnapshots)
		for i := maxSnapshots; i < len(fullSnapshots); i++ {
			if err := os.Remove(fullSnapshots[i].path); err != nil {
				mlog.Log.Errorf("failed to remove old snapshot %s: %v", fullSnapshots[i].name, err)
			} else {
				mlog.Log.Infof("Cleaned up old snapshot file (retention limit %d): %s", maxSnapshots, fullSnapshots[i].name)
			}
		}
	}

	// Clean incremental snapshots beyond the retention limit (same limit as full)
	if len(incrSnapshots) > maxSnapshots {
		sortByTime(incrSnapshots)
		for i := maxSnapshots; i < len(incrSnapshots); i++ {
			if err := os.Remove(incrSnapshots[i].path); err != nil {
				mlog.Log.Errorf("failed to remove old incremental snapshot %s: %v", incrSnapshots[i].name, err)
			} else {
				mlog.Log.Infof("Cleaned up old incremental snapshot file (retention limit %d): %s", maxSnapshots, incrSnapshots[i].name)
			}
		}
	}
}

var (
	indexEntryCommitterInProgress = &atomic.Int64{}
	indexEntryBuilderInProgress   = &atomic.Int64{}
	appendVecCopyingInProgress    = &atomic.Int64{}
)

func positiveOrDefault(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func snapshotIndexEntryCommitterWorkers() int {
	return positiveOrDefault(SnapshotIndexEntryCommitterWorkers, DefaultSnapshotIndexEntryCommitterWorkers)
}

func snapshotIndexEntryBuilderWorkers() int {
	return positiveOrDefault(SnapshotIndexEntryBuilderWorkers, DefaultSnapshotIndexEntryBuilderWorkers)
}

func snapshotAppendVecCopyingWorkers() int {
	return positiveOrDefault(SnapshotAppendVecCopyingWorkers, DefaultSnapshotAppendVecCopyingWorkers)
}

func snapshotIndexShards() int {
	return positiveOrDefault(SnapshotIndexShards, DefaultSnapshotIndexShards)
}

func snapshotMaxConcurrentFlushers() int {
	return positiveOrDefault(MaxConcurrentFlushers, DefaultSnapshotMaxConcurrentFlushers)
}

func logSnapshotBootstrapTuning() {
	indexTempDir := SnapshotIndexTempDir
	if indexTempDir == "" {
		indexTempDir = "(accountsdb)"
	}
	mlog.Log.Infof("Snapshot bootstrap tuning: append_vec_workers=%d index_builder_workers=%d index_committer_workers=%d index_shards=%d index_sort_chunk_mb=%d max_concurrent_flushers=%d zstd_decoder_concurrency=%d index_temp_dir=%s",
		snapshotAppendVecCopyingWorkers(),
		snapshotIndexEntryBuilderWorkers(),
		snapshotIndexEntryCommitterWorkers(),
		snapshotIndexShards(),
		shardSortChunkBytes()>>20,
		snapshotMaxConcurrentFlushers(),
		ZstdDecoderConcurrency,
		indexTempDir)
}

func prepareSnapshotIndexWorkDir(accountsDbDir string) (string, func(), error) {
	if SnapshotIndexTempDir == "" {
		logsDir := filepath.Join(accountsDbDir, SnapshotIndexRunDirName)
		if err := os.MkdirAll(logsDir, 0775); err != nil {
			return "", nil, err
		}
		cleanup := func() {
			if err := os.RemoveAll(logsDir); err != nil {
				mlog.Log.Warnf("failed to remove snapshot index work dir %s: %v", logsDir, err)
			}
		}
		return logsDir, cleanup, nil
	}

	if err := os.MkdirAll(SnapshotIndexTempDir, 0775); err != nil {
		return "", nil, fmt.Errorf("creating snapshot index temp dir %s: %w", SnapshotIndexTempDir, err)
	}
	logsDir, err := os.MkdirTemp(SnapshotIndexTempDir, "accounts-index-runs-*")
	if err != nil {
		return "", nil, fmt.Errorf("creating snapshot index work dir in %s: %w", SnapshotIndexTempDir, err)
	}
	mlog.Log.Infof("Snapshot index sorted-run staging: %s", logsDir)

	cleanup := func() {
		if err := os.RemoveAll(logsDir); err != nil {
			mlog.Log.Warnf("failed to remove snapshot index temp dir %s: %v", logsDir, err)
		}
	}
	return logsDir, cleanup, nil
}

func BuildAccountsDbPaths(
	ctx context.Context,
	snapshotFile string,
	incrementalSnapshotFile string,
	accountsDbDir string,
	dp *progress.DualProgress,
) (_ *accountsdb.AccountsDb, _ *SnapshotManifest, retErr error) {
	// Clean any leftover artifacts from previous incomplete runs (e.g., Ctrl+C)
	storeGuard, err := beginSnapshotBootstrap(accountsDbDir)
	if err != nil {
		return nil, nil, fmt.Errorf("clean previous AccountsDB: %w", err)
	}
	storeGuardTransferred := false
	defer func() {
		// A V2 root is published before the independent AccountsLtHash check so
		// the verifier can probe it. If any later bootstrap stage fails, remove
		// that private, not-ready store before releasing the lifetime lock; an
		// unrelated opener must never observe an unverified root between retries.
		if retErr != nil && !storeGuardTransferred {
			retErr = errors.Join(retErr, cleanAccountsDbDirLocked(accountsDbDir))
		}
		retErr = errors.Join(retErr, storeGuard.Close())
	}()

	mlog.Log.Infof("Parsing full snapshot manifest...")
	manifest, err := UnmarshalManifestFromSnapshot(ctx, snapshotFile, accountsDbDir)
	if err != nil {
		return nil, nil, fmt.Errorf("reading snapshot manifest: %w", err)
	}
	mlog.Log.Infof("Parsed full snapshot manifest")

	var incrementalManifest *SnapshotManifest
	if incrementalSnapshotFile != "" {
		mlog.Log.FileOnlyf("Parsing incremental snapshot manifest...")
		incrementalManifest, err = UnmarshalManifestFromSnapshot(ctx, incrementalSnapshotFile, accountsDbDir)
		if err != nil {
			return nil, nil, fmt.Errorf("reading incremental snapshot manifest: %w", err)
		}
		mlog.Log.FileOnlyf("Parsed incremental snapshot manifest")
	}
	if err := validateSnapshotManifestPair(manifest, incrementalManifest); err != nil {
		return nil, nil, fmt.Errorf("validate full/incremental snapshot pairing: %w", err)
	}
	if err := validateSnapshotArchiveManifestSlots(
		snapshotFile, manifest, incrementalSnapshotFile, incrementalManifest,
	); err != nil {
		return nil, nil, fmt.Errorf("validate snapshot archive identity: %w", err)
	}
	if OnFullSnapshotManifestParsed != nil {
		OnFullSnapshotManifestParsed(manifest)
	}
	if incrementalManifest != nil && OnIncrementalManifestParsed != nil {
		OnIncrementalManifestParsed(incrementalManifest)
	}

	start := time.Now()

	appendVecsOutputDir := filepath.Join(accountsDbDir, "accounts")
	if err = os.MkdirAll(appendVecsOutputDir, 0775); err != nil {
		return nil, nil, err
	}
	logSnapshotBootstrapTuning()

	defer ants.Release()

	var largestFileId atomic.Uint64
	wg := &sync.WaitGroup{}

	logsDir, cleanupIndexWorkDir, err := prepareSnapshotIndexWorkDir(accountsDbDir)
	if err != nil {
		return nil, nil, err
	}
	defer cleanupIndexWorkDir()
	numShards := snapshotIndexShards()
	sl, err := NewShardLogger(numShards, logsDir)
	if err != nil {
		return nil, nil, fmt.Errorf("creating snapshot shard logger: %w", err)
	}
	defer func() {
		if abortErr := sl.Abort(); abortErr != nil {
			mlog.Log.Warnf("failed to abort snapshot shard logger: %v", abortErr)
		}
	}()

	// Create stake pubkey collector for building stake index during appendvec processing
	stakeCollector := &stakeIndexCollector{
		entries: make([]accountsdb.StakeIndexEntry, 0, 1000000), // Pre-allocate for ~1M stake accounts
	}

	pools, err := initWorkerPools(wg, sl, manifest, incrementalManifest, accountsDbDir, &largestFileId, stakeCollector)
	if err != nil {
		return nil, nil, fmt.Errorf("initializing worker pools: %w", err)
	}
	defer pools.Release()

	// Start progress display if provided
	if dp != nil {
		// Flush mlog's buffered writer AND OS buffers before starting progress bars
		// This prevents late-flushing logs from breaking cursor positioning
		mlog.Flush()
		os.Stdout.Sync()
		os.Stderr.Sync()
		dp.Start()
	}

	// Process snapshots sequentially for better performance (less lock contention)
	// Full snapshot first
	err = readTar(ctx, wg, snapshotFile, pools, readTarOptions{
		progress:        dp,
		statusCachePath: retainedStatusCachePath(accountsDbDir),
	})

	// Wait for ALL worker tasks from full snapshot to complete before starting incremental
	workerErr := waitForSnapshotWorkers(wg, pools)
	if err == nil {
		err = workerErr
	}

	// Stop progress display after full snapshot
	if dp != nil {
		if err != nil {
			dp.Interrupt(err)
		} else {
			dp.Stop()
		}
	}

	if err != nil {
		return nil, nil, err
	}

	// Process incremental snapshot (if provided)
	if incrementalSnapshotFile != "" {
		err = readTar(ctx, wg, incrementalSnapshotFile, pools,
			readTarOptions{isIncremental: true, statusCachePath: retainedStatusCachePath(accountsDbDir)})
		// Wait for all incremental worker tasks to complete
		workerErr = waitForSnapshotWorkers(wg, pools)
		if err == nil {
			err = workerErr
		}
		if err != nil {
			return nil, nil, err
		}
	}
	if err := syncDirectory(appendVecsOutputDir); err != nil {
		return nil, nil, fmt.Errorf("persist snapshot appendvec publications: %w", err)
	}

	mlog.Log.Debugf("done processing snapshots in %s.", fmtDuration(time.Since(start)))

	// Show indexing progress for shard flush (no gap between DualProgress and this)
	indexProgress := progress.NewIndexingProgress("Flush (shard logs)")
	indexProgress.Start(numShards)
	err = sl.CloseWithProgress(ctx, func(completed, total int) {
		indexProgress.Update(completed, total)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("closing shard logger: %w", err)
	}
	if err := buildSnapshotAccountsIndex(ctx, accountsDbDir, logsDir, storeGuard); err != nil {
		return nil, nil, err
	}

	finalManifest := manifest
	if incrementalManifest != nil {
		finalManifest = incrementalManifest
	}
	preparedIndex, err := verifyBuiltSnapshotAccountsState(
		ctx, accountsDbDir, manifest, incrementalManifest, finalManifest, storeGuard,
	)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		retErr = errors.Join(retErr, preparedIndex.Close())
	}()

	mlog.Log.Infof("Snapshot processed in %s.", fmtDuration(time.Since(start)))

	if err := finalizeSnapshotBootstrapArtifacts(
		accountsDbDir,
		largestFileId.Load(),
		finalManifest.Bank.Hash,
		stakeCollector.entries,
	); err != nil {
		return nil, nil, err
	}

	accountsDb, err := accountsdb.OpenDbWithPreparedSnapshotAccountIndexAndStoreGuard(
		accountsDbDir, preparedIndex, storeGuard,
	)
	if err != nil {
		return nil, nil, err
	}
	storeGuardTransferred = true

	if incrementalManifest != nil {
		return accountsDb, incrementalManifest, nil
	} else {
		return accountsDb, manifest, nil
	}
}

type readTarOptions struct {
	// Saves snapshot to a file if non-empty.
	savePath string
	// Update a progress bar if Progress is non-nil.
	progress *progress.DualProgress
	// True if the tar file is incremental or false if it's a full snapshot.
	isIncremental bool
	// Atomically retain the raw snapshots/status_cache member here. A
	// successfully consumed incremental archive replaces the full seed.
	statusCachePath string
}

func readTar(
	ctx context.Context,
	wg *sync.WaitGroup,
	filename string,
	pools *snapshotWorkerPools,
	options readTarOptions,
) error {
	dp := options.progress
	savePath := options.savePath
	statusCache := newStatusCacheCandidate(options.statusCachePath)
	defer statusCache.cleanup()
	tarReader, bmr, closer, err := newSnapshotReaderWithProgress(ctx, filename, options.savePath)
	if err != nil {
		return err
	}
	snapshotClosed := false
	defer func() {
		if !snapshotClosed {
			_ = closer.Close()
		}
	}()

	var expectedAppendVecs map[snapshotAppendVecKey]uint64
	var expectedManifestDigest [sha256.Size]byte
	haveExpectedManifestDigest := false
	if pools != nil {
		manifest := pools.manifest
		if options.isIncremental {
			manifest = pools.incrementalManifest
		}
		expectedAppendVecs, err = expectedSnapshotAppendVecs(manifest)
		if err != nil {
			return fmt.Errorf("validate snapshot appendvec manifest: %w", err)
		}
		expectedManifestDigest = manifest.rawDigest
		haveExpectedManifestDigest = manifest.hasRawDigest
	}

	// Set up download progress callback
	if dp != nil {
		// Use SetDownloadTotal which enables dynamic Extract total estimation
		// based on observed compression ratio (recalculated in updateLoop)
		dp.SetDownloadTotal(bmr.TotalSize())
		bmr.SetProgressCallback(func(bytesRead, totalBytes int64) {
			dp.Download.Add(bytesRead - dp.Download.Current())
		})
	}

	// cleanupPartial removes the .partial download file on error/cancellation
	cleanupPartial := func(reason string) {
		if savePath != "" {
			mlog.Log.Infof("Cleaning up partial download (%s)", reason)
			CleanupPartialDownload(savePath)
		}
	}
	seenAppendVecs := make(map[string]struct{})
	manifestMembers := 0
	versionMembers := 0

	for {
		if pools != nil {
			if workerErr := pools.Err(); workerErr != nil {
				cleanupPartial("worker error")
				return workerErr
			}
		}
		if ctx.Err() != nil {
			mlog.Log.Infof("Context cancelled, stopping snapshot unpack: %v", ctx.Err())
			cleanupPartial("cancelled")
			return ctx.Err()
		}
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			mlog.Log.Errorf("reading next tar: %s\n", err)
			cleanupPartial("read error")
			return err
		}

		if handled, tarBytesRead, captureErr := statusCache.capture(header, tarReader); handled {
			if captureErr != nil {
				cleanupPartial("status cache error")
				return captureErr
			}
			statsd.Count(statsd.SnapshotTarBytesRead, tarBytesRead, nil)
			if dp != nil {
				dp.Extract.Add(tarBytesRead)
			}
			continue
		}

		if _, isManifest := parseSnapshotManifestTarPath(header.Name); isManifest && pools != nil {
			if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
				cleanupPartial("invalid manifest member type")
				return fmt.Errorf("snapshot manifest member %q is not a regular file", header.Name)
			}
			if header.Size < 0 || header.Size > maxSnapshotManifestSize {
				cleanupPartial("invalid manifest member size")
				return fmt.Errorf(
					"snapshot manifest member %q has invalid size %d (maximum %d)",
					header.Name, header.Size, maxSnapshotManifestSize,
				)
			}
			manifestMembers++
			if manifestMembers != 1 {
				cleanupPartial("duplicate manifest member")
				return fmt.Errorf("snapshot archive contains multiple canonical manifest members")
			}
			hasher := sha256.New()
			manifestBytesRead, hashErr := io.CopyN(hasher, tarReader, header.Size)
			if hashErr != nil {
				cleanupPartial("manifest read error")
				return fmt.Errorf("read snapshot manifest member %q: %w", header.Name, hashErr)
			}
			statsd.Count(statsd.SnapshotTarBytesRead, manifestBytesRead, nil)
			if dp != nil {
				dp.Extract.Add(manifestBytesRead)
			}
			if !haveExpectedManifestDigest || !bytes.Equal(hasher.Sum(nil), expectedManifestDigest[:]) {
				cleanupPartial("manifest changed between passes")
				return fmt.Errorf("snapshot archive manifest differs from the manifest selected for bootstrap")
			}
			continue
		}
		if header.Name == "version" && pools != nil {
			if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
				cleanupPartial("invalid snapshot version member type")
				return fmt.Errorf("snapshot version member is not a regular file")
			}
			if header.Size < 0 || header.Size > 32 {
				cleanupPartial("invalid snapshot version member size")
				return fmt.Errorf("snapshot version member has invalid size %d", header.Size)
			}
			versionMembers++
			if versionMembers != 1 {
				cleanupPartial("duplicate snapshot version member")
				return fmt.Errorf("snapshot archive contains multiple version members")
			}
			versionBytes := make([]byte, int(header.Size))
			if _, readErr := io.ReadFull(tarReader, versionBytes); readErr != nil {
				cleanupPartial("snapshot version read error")
				return fmt.Errorf("read snapshot version member: %w", readErr)
			}
			statsd.Count(statsd.SnapshotTarBytesRead, header.Size, nil)
			if dp != nil {
				dp.Extract.Add(header.Size)
			}
			if strings.TrimSpace(string(versionBytes)) != "1.2.0" {
				cleanupPartial("unsupported snapshot version")
				return fmt.Errorf("unsupported snapshot version %q", strings.TrimSpace(string(versionBytes)))
			}
			continue
		}

		slot, fileID, isAppendVec, parseErr := parseAppendVecTarPath(header.Name)
		if parseErr != nil {
			cleanupPartial("invalid appendvec path")
			return parseErr
		}
		if !isAppendVec {
			continue
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			cleanupPartial("invalid appendvec member type")
			return fmt.Errorf("appendvec member %q is not a regular file", header.Name)
		}
		if _, exists := seenAppendVecs[header.Name]; exists {
			cleanupPartial("duplicate appendvec")
			return fmt.Errorf("snapshot archive contains duplicate appendvec %q", header.Name)
		}
		seenAppendVecs[header.Name] = struct{}{}
		if pools == nil {
			cleanupPartial("worker pool unavailable")
			return fmt.Errorf("snapshot archive contains appendvec %q but no worker pool is available", header.Name)
		}
		appendVecKey := snapshotAppendVecKey{slot: slot, fileID: fileID}
		fileSize, exists := expectedAppendVecs[appendVecKey]
		if !exists {
			cleanupPartial("appendvec absent from manifest")
			return fmt.Errorf("snapshot archive contains appendvec slot=%d file_id=%d absent from its manifest", slot, fileID)
		}
		if fileSize > maximumSolanaAppendVecFileSize {
			cleanupPartial("oversized appendvec")
			return fmt.Errorf(
				"appendvec %q size %d exceeds Solana maximum %d",
				header.Name, fileSize, maximumSolanaAppendVecFileSize,
			)
		}
		if header.Size < 0 || uint64(header.Size) != fileSize {
			cleanupPartial("appendvec size mismatch")
			return fmt.Errorf(
				"appendvec %q archive size %d does not match manifest size %d",
				header.Name, header.Size, fileSize,
			)
		}
		if uint64(header.Size) > uint64(^uint(0)>>1) {
			cleanupPartial("appendvec exceeds address space")
			return fmt.Errorf("appendvec %q size %d exceeds this process address space", header.Name, header.Size)
		}

		appendVecPath, tarBytesRead, err := streamSnapshotAppendVec(
			pools.accountsDbDir,
			slot,
			fileID,
			fileSize,
			tarReader,
		)
		if err != nil {
			mlog.Log.Errorf("err streaming appendvec to disk: %s\n", err)
			cleanupPartial("copy error")
			return err
		}
		statsd.Count(statsd.SnapshotTarBytesRead, tarBytesRead, nil)

		// Update extract progress
		if dp != nil {
			dp.Extract.Add(tarBytesRead)
		}

		task := appendVecCopyingTask{
			Path:     appendVecPath,
			Slot:     slot,
			FileID:   fileID,
			FileSize: fileSize,
		}
		err = invokeSnapshotTask(wg, pools.appendVecCopying, task)
		if err != nil {
			mlog.Log.Errorf("error calling appendVecCopyingPool.Invoke: %v", err)
			cleanupPartial("pool error")
			return err
		}
		delete(expectedAppendVecs, appendVecKey)
	}
	// Worker stages are part of consuming the archive. Do not publish the
	// status-cache seed or a reusable downloaded snapshot until every parser
	// and index-log task has completed successfully.
	if wg != nil {
		wg.Wait()
	}
	if pools != nil {
		if workerErr := pools.Err(); workerErr != nil {
			cleanupPartial("worker error")
			return workerErr
		}
	}
	if len(expectedAppendVecs) != 0 {
		first := firstMissingSnapshotAppendVec(expectedAppendVecs)
		cleanupPartial("missing manifest appendvec")
		return fmt.Errorf(
			"snapshot archive omitted %d manifest appendvec(s), first missing slot=%d file_id=%d",
			len(expectedAppendVecs), first.slot, first.fileID,
		)
	}
	if pools != nil && manifestMembers != 1 {
		cleanupPartial("missing manifest member")
		return fmt.Errorf("snapshot archive contains %d canonical manifest members, want exactly one", manifestMembers)
	}
	if pools != nil && versionMembers != 1 {
		cleanupPartial("missing snapshot version member")
		return fmt.Errorf("snapshot archive contains %d version members, want exactly one", versionMembers)
	}
	var closeErr error
	if finisher, ok := closer.(interface{ Finish() error }); ok {
		closeErr = finisher.Finish()
	} else {
		closeErr = closer.Close()
	}
	snapshotClosed = true
	if closeErr != nil {
		cleanupPartial("snapshot close error")
		return fmt.Errorf("close consumed snapshot: %w", closeErr)
	}
	if savePath != "" && bmr.TotalSize() >= 0 {
		bytesRead := bmr.BytesRead()
		if bytesRead != bmr.TotalSize() {
			cleanupPartial("incomplete saved download")
			return fmt.Errorf(
				"snapshot download consumed %d of %d compressed bytes",
				bytesRead, bmr.TotalSize(),
			)
		}
	}

	if err := statusCache.commit(); err != nil {
		cleanupPartial("status cache commit error")
		return err
	}

	// Successfully processed the entire tar — finalize by renaming from .partial
	if err := FinalizePartialDownload(savePath); err != nil {
		mlog.Log.Errorf("Failed to finalize snapshot download: %v", err)
		// Don't return error — the snapshot was processed successfully,
		// finalization failure just means we can't reuse the cached file
	}

	return nil
}

type snapshotWorkerPools struct {
	appendVecCopying    *ants.PoolWithFunc
	indexEntryBuilder   *ants.PoolWithFunc
	indexEntryCommitter *ants.PoolWithFunc
	errors              *snapshotWorkerErrors
	manifest            *SnapshotManifest
	incrementalManifest *SnapshotManifest
	accountsDbDir       string
}

type snapshotWorkerErrors struct {
	mu  sync.Mutex
	err error
}

func (e *snapshotWorkerErrors) Record(err error) {
	if err == nil {
		return
	}
	e.mu.Lock()
	if e.err == nil {
		e.err = err
		mlog.Log.Errorf("snapshot worker failed: %v", err)
	}
	e.mu.Unlock()
}

func (e *snapshotWorkerErrors) Recover(stage string) {
	if recovered := recover(); recovered != nil {
		e.Record(fmt.Errorf("%s worker panicked: %v", stage, recovered))
	}
}

func (e *snapshotWorkerErrors) Err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

func (p *snapshotWorkerPools) Err() error {
	if p == nil || p.errors == nil {
		return nil
	}
	return p.errors.Err()
}

func waitForSnapshotWorkers(wg *sync.WaitGroup, pools *snapshotWorkerPools) error {
	wg.Wait()
	return pools.Err()
}

// invokeSnapshotTask keeps the wait-group balanced when a pool rejects a
// submission (or panics while accepting it).  Without the compensating Done,
// bootstrap can either hang forever or incorrectly outlive a failed stage.
func invokeSnapshotTask(wg *sync.WaitGroup, pool *ants.PoolWithFunc, task any) (err error) {
	if pool == nil {
		return fmt.Errorf("snapshot worker pool is nil")
	}
	wg.Add(1)
	submitted := false
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("submitting snapshot worker task: %v", recovered)
		}
		if !submitted {
			wg.Done()
		}
	}()
	err = pool.Invoke(task)
	submitted = err == nil
	return err
}

// stakeIndexCollector aggregates stake account pubkeys from multiple worker goroutines
// during appendvec processing. Used to build the stake pubkey index file.
//
// WHY: The manifest's delegation list can be stale/incomplete (Firedancer notes:
// "the cache in the manifest is partially incomplete"). Instead of trusting manifest
// data, we:
//  1. Collect stake pubkeys during appendvec parsing (by checking owner == StakeProgramAddr)
//  2. Write them to stake_pubkeys.idx after snapshot processing
//  3. At startup, load pubkeys from index and read ALL delegation fields from AccountsDB
//
// This ensures stake cache contains fresh data from AccountsDB, not potentially stale
// manifest data.
type stakeIndexCollector struct {
	mu      sync.Mutex
	entries []accountsdb.StakeIndexEntry
}

func (c *stakeIndexCollector) Add(entries []accountsdb.StakeIndexEntry) {
	if len(entries) == 0 {
		return
	}
	c.mu.Lock()
	c.entries = append(c.entries, entries...)
	c.mu.Unlock()
}

func initWorkerPools(
	wg *sync.WaitGroup,
	sl *ShardLogger,
	manifest *SnapshotManifest,
	incrementalManifest *SnapshotManifest,
	accountsDbDir string,
	largestFileId *atomic.Uint64,
	stakeCollector *stakeIndexCollector,
) (*snapshotWorkerPools, error) {
	indexEntryCommitterWorkers := snapshotIndexEntryCommitterWorkers()
	indexEntryBuilderWorkers := snapshotIndexEntryBuilderWorkers()
	appendVecCopyingWorkers := snapshotAppendVecCopyingWorkers()
	workerErrors := &snapshotWorkerErrors{}

	indexEntryCommitterPool, err := ants.NewPoolWithFunc(indexEntryCommitterWorkers, func(i any) {
		tasks := indexEntryCommitterInProgress.Add(1)
		statsd.Gauge(statsd.SnapshotWorkerPoolUtilization, float64(tasks)/float64(indexEntryCommitterWorkers), []string{"index_entry_committer"})
		start := time.Now()
		defer wg.Done()
		defer indexEntryCommitterInProgress.Add(-1)
		defer workerErrors.Recover("index entry committer")
		if workerErrors.Err() != nil {
			return
		}
		task := i.(indexEntryCommitterTask)
		if len(task.IndexEntries) != len(task.Pubkeys) {
			workerErrors.Record(fmt.Errorf("index entry committer received %d entries for %d pubkeys", len(task.IndexEntries), len(task.Pubkeys)))
			return
		}

		for idx, entry := range task.IndexEntries {
			sl.EnqueueRequest(task.Pubkeys[idx], entry)
		}
		statsd.Timing(statsd.TaskIndexEntryCommitterLatency, uint64(time.Since(start)), nil)
	})
	if err != nil {
		return nil, err
	}

	indexEntryBuilderPool, err := ants.NewPoolWithFunc(indexEntryBuilderWorkers, func(i any) {
		tasks := indexEntryBuilderInProgress.Add(1)
		statsd.Gauge(statsd.SnapshotWorkerPoolUtilization, float64(tasks)/float64(indexEntryBuilderWorkers), []string{"index_entry_builder"})
		start := time.Now()
		defer wg.Done()
		defer indexEntryBuilderInProgress.Add(-1)
		defer workerErrors.Recover("index entry builder")
		if workerErrors.Err() != nil {
			return
		}
		task := i.(indexEntryBuilderTask)
		data, cleanupMapping, err := mmapSnapshotAppendVec(task.Path, task.FileSize)
		if err != nil {
			workerErrors.Record(fmt.Errorf("mapping appendvec %d.%d: %w", task.Slot, task.FileId, err))
			return
		}
		scanErr := accountsdb.ScanIndexEntriesFromAppendVecs(
			data,
			task.FileSize,
			task.Slot,
			task.FileId,
			defaultSnapshotIndexEntryBatchSize,
			func(pubkeys []solana.PublicKey, entries []accountsdb.AccountIndexEntry, stakeEntries []accountsdb.StakeIndexEntry) error {
				if err := workerErrors.Err(); err != nil {
					return err
				}
				stakeCollector.Add(stakeEntries)
				commitTask := indexEntryCommitterTask{IndexEntries: entries, Pubkeys: pubkeys}
				if err := invokeSnapshotTask(wg, indexEntryCommitterPool, commitTask); err != nil {
					return fmt.Errorf("submitting index entry committer task: %w", err)
				}
				return nil
			},
		)
		cleanupErr := cleanupMapping()
		if err := errors.Join(scanErr, cleanupErr); err != nil {
			workerErrors.Record(fmt.Errorf("building index entries: %w", err))
			return
		}
		statsd.Timing(statsd.TasksIndexEntryBuilderLatency, uint64(time.Since(start)), nil)
	})
	if err != nil {
		indexEntryCommitterPool.Release()
		return nil, err
	}

	appendVecCopyingPool, err := ants.NewPoolWithFunc(appendVecCopyingWorkers, func(i any) {
		tasks := appendVecCopyingInProgress.Add(1)
		statsd.Gauge(statsd.SnapshotWorkerPoolUtilization, float64(tasks)/float64(appendVecCopyingWorkers), []string{"append_vec_copying"})
		start := time.Now()
		defer wg.Done()
		defer appendVecCopyingInProgress.Add(-1)
		defer workerErrors.Recover("appendvec copying")
		if workerErrors.Err() != nil {
			return
		}
		task := i.(appendVecCopyingTask)
		if task.Path == "" {
			if err := writeSnapshotAppendVec(accountsDbDir, task); err != nil {
				workerErrors.Record(err)
				return
			}
			task.Path = filepath.Join(accountsDbDir, "accounts", fmt.Sprintf("%d.%d", task.Slot, task.FileID))
		}

		for {
			prevLargestFileId := largestFileId.Load()
			if task.FileID <= prevLargestFileId {
				break
			}
			swapped := largestFileId.CompareAndSwap(prevLargestFileId, task.FileID)
			if swapped {
				break
			}
		}

		nextTask := indexEntryBuilderTask{
			Path:     task.Path,
			FileSize: task.FileSize,
			Slot:     task.Slot,
			FileId:   task.FileID,
		}
		statsd.Timing(statsd.TasksAppendVecCopyingLatency, uint64(time.Since(start)), nil)
		err = invokeSnapshotTask(wg, indexEntryBuilderPool, nextTask)
		if err != nil {
			workerErrors.Record(fmt.Errorf("submitting index entry builder task: %w", err))
		}
	})
	if err != nil {
		indexEntryBuilderPool.Release()
		indexEntryCommitterPool.Release()
		return nil, err
	}

	return &snapshotWorkerPools{
		appendVecCopying:    appendVecCopyingPool,
		indexEntryBuilder:   indexEntryBuilderPool,
		indexEntryCommitter: indexEntryCommitterPool,
		errors:              workerErrors,
		manifest:            manifest,
		incrementalManifest: incrementalManifest,
		accountsDbDir:       accountsDbDir,
	}, nil
}

func (p *snapshotWorkerPools) Release() {
	p.appendVecCopying.Release()
	p.indexEntryBuilder.Release()
	p.indexEntryCommitter.Release()
}

// buildSnapshotAccountsIndex turns the sorted, deduplicated snapshot runs into
// the complete production V2 index: sharded immutable StreamHash bases, the
// atomic root catalog, and the exact CRC-framed mutable WAL. Callers may write
// a ready state marker only after this function returns successfully.
func buildSnapshotAccountsIndex(
	ctx context.Context,
	accountsDbDir string,
	logsDir string,
	storeGuard *accountsdb.ProductionAccountIndexStoreGuard,
) error {
	source, err := accountsdb.OpenStreamIndexRunSource(logsDir)
	if err != nil {
		return fmt.Errorf("opening sorted snapshot index runs: %w", err)
	}
	config, err := accountsdb.CurrentProductionAccountIndexConfig()
	if err != nil {
		return fmt.Errorf("validating production account-index configuration: %w", err)
	}

	mlog.Log.Infof(
		"Building production AccountsDB V2 index from snapshot keys (%d StreamHash shards, %d workers)...",
		config.ShardCount,
		config.CheckpointWorkers,
	)
	if err := accountsdb.InitializeProductionAccountIndexWithStoreGuard(
		ctx, accountsDbDir, source, config, storeGuard,
	); err != nil {
		return fmt.Errorf("building production AccountsDB V2 index: %w", err)
	}
	mlog.Log.Infof("Built and durably published production AccountsDB V2 account index")
	return nil
}

// verifyBuiltSnapshotAccountsState independently validates the immutable
// index before any ready marker, largest-file selector, stake sidecar, or bank
// hash is published.  The index builder and verifier deliberately consume
// different representations so a bad sort/dedup winner cannot self-certify.
func verifyBuiltSnapshotAccountsState(
	ctx context.Context,
	accountsDbDir string,
	fullManifest *SnapshotManifest,
	incrementalManifest *SnapshotManifest,
	finalManifest *SnapshotManifest,
	storeGuard *accountsdb.ProductionAccountIndexStoreGuard,
) (*accountsdb.PreparedSnapshotAccountIndex, error) {
	if finalManifest == nil || finalManifest.Bank == nil {
		return nil, errors.New("verify snapshot account state: final manifest is incomplete")
	}
	appendVecs, err := snapshotVerificationAppendVecs(fullManifest, incrementalManifest)
	if err != nil {
		return nil, fmt.Errorf("prepare snapshot account-state verification: %w", err)
	}
	config, err := accountsdb.CurrentProductionAccountIndexConfig()
	if err != nil {
		return nil, fmt.Errorf("validate production account-index configuration for snapshot handoff: %w", err)
	}

	start := time.Now()
	mlog.Log.Infof(
		"Verifying production AccountsDB V2 state against snapshot AccountsLtHash and capitalization (%d appendvecs)...",
		len(appendVecs),
	)
	prepared, err := accountsdb.PrepareVerifiedSnapshotAccountIndexWithStoreGuard(
		ctx,
		accountsDbDir,
		appendVecs,
		finalManifest.LtHash,
		finalManifest.Bank.Capitalization,
		config,
		storeGuard,
	)
	if err != nil {
		return nil, fmt.Errorf("verify production AccountsDB V2 snapshot state: %w", err)
	}
	mlog.Log.Infof("Verified snapshot account state in %s", fmtDuration(time.Since(start)))
	return prepared, nil
}
