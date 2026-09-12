package snapshot

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"github.com/cockroachdb/pebble"
	"github.com/panjf2000/ants/v2"
)

const (
	DefaultSnapshotIndexEntryCommitterWorkers = 64
	DefaultSnapshotIndexEntryBuilderWorkers   = 64
	DefaultSnapshotAppendVecCopyingWorkers    = 32
	DefaultSnapshotIndexShards                = 64
	DefaultSnapshotMaxConcurrentFlushers      = 8
)

var (
	SnapshotIndexEntryCommitterWorkers = DefaultSnapshotIndexEntryCommitterWorkers
	SnapshotIndexEntryBuilderWorkers   = DefaultSnapshotIndexEntryBuilderWorkers
	SnapshotAppendVecCopyingWorkers    = DefaultSnapshotAppendVecCopyingWorkers
	SnapshotIndexShards                = DefaultSnapshotIndexShards
	SnapshotIndexTempDir               string
)

// CleanAccountsDbDir removes all artifacts from a previous incomplete snapshot run.
// This prevents corruption from Ctrl+C or partial downloads.
// Exported so it can be called early in startup before any failures.
func CleanAccountsDbDir(accountsDbDir string) {
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
		"bankhash_db",
		"largest_file_id",
		"manifest",
		"mithril_state.json", // State file for tracking valid builds and replay progress
	}
	for _, artifact := range artifacts {
		path := filepath.Join(accountsDbDir, artifact)
		if err := os.RemoveAll(path); err != nil {
			mlog.Log.Errorf("failed to remove %s: %v", path, err)
		}
	}
	partials, _ := filepath.Glob(filepath.Join(accountsDbDir, ".snapshot-status-cache-*.partial"))
	for _, partial := range partials {
		if err := os.Remove(partial); err != nil && !os.IsNotExist(err) {
			mlog.Log.Errorf("failed to remove stale status-cache partial %s: %v", partial, err)
		}
	}

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
		if strings.HasSuffix(entry.Name(), PartialSuffix) {
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
		if strings.HasPrefix(name, "snapshot-") && strings.HasSuffix(name, ".tar.zst") {
			path := filepath.Join(downloadPath, name)
			info, err := entry.Info()
			if err != nil {
				continue
			}
			fullSnapshots = append(fullSnapshots, snapshotFile{name, path, info.ModTime()})
		}
		if strings.HasPrefix(name, "incremental-snapshot-") && strings.HasSuffix(name, ".tar.zst") {
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
	mlog.Log.Infof("Snapshot bootstrap tuning: append_vec_workers=%d index_builder_workers=%d index_committer_workers=%d index_shards=%d max_concurrent_flushers=%d zstd_decoder_concurrency=%d index_temp_dir=%s",
		snapshotAppendVecCopyingWorkers(),
		snapshotIndexEntryBuilderWorkers(),
		snapshotIndexEntryCommitterWorkers(),
		snapshotIndexShards(),
		snapshotMaxConcurrentFlushers(),
		ZstdDecoderConcurrency,
		indexTempDir)
}

func prepareSnapshotIndexWorkDir(accountsDbDir string) (string, func(), error) {
	if SnapshotIndexTempDir == "" {
		logsDir := filepath.Join(accountsDbDir, "mithril_db_log_shards")
		if err := os.MkdirAll(logsDir, 0775); err != nil {
			return "", nil, err
		}
		return logsDir, func() {}, nil
	}

	if err := os.MkdirAll(SnapshotIndexTempDir, 0775); err != nil {
		return "", nil, fmt.Errorf("creating snapshot index temp dir %s: %w", SnapshotIndexTempDir, err)
	}
	logsDir, err := os.MkdirTemp(SnapshotIndexTempDir, "mithril-db-log-shards-*")
	if err != nil {
		return "", nil, fmt.Errorf("creating snapshot index work dir in %s: %w", SnapshotIndexTempDir, err)
	}
	mlog.Log.Infof("Snapshot index shard logs/SST staging: %s", logsDir)

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
) (*accountsdb.AccountsDb, *SnapshotManifest, error) {
	finishBootstrap := statsd.BeginSnapshotBootstrap()
	defer finishBootstrap()

	// Clean any leftover artifacts from previous incomplete runs (e.g., Ctrl+C)
	CleanAccountsDbDir(accountsDbDir)

	mlog.Log.Infof("Parsing full snapshot manifest...")
	manifest, err := UnmarshalManifestFromSnapshot(ctx, snapshotFile, accountsDbDir)
	if err != nil {
		return nil, nil, fmt.Errorf("reading snapshot manifest: %v", err)
	}
	mlog.Log.Infof("Parsed full snapshot manifest")
	if OnFullSnapshotManifestParsed != nil {
		OnFullSnapshotManifestParsed(manifest)
	}

	var incrementalManifest *SnapshotManifest
	if incrementalSnapshotFile != "" {
		mlog.Log.FileOnlyf("Parsing incremental snapshot manifest...")
		incrementalManifest, err = UnmarshalManifestFromSnapshot(ctx, incrementalSnapshotFile, accountsDbDir)
		if err != nil {
			return nil, nil, fmt.Errorf("reading incremental snapshot manifest: %v", err)
		}
		mlog.Log.FileOnlyf("Parsed incremental snapshot manifest")
		if OnIncrementalManifestParsed != nil {
			OnIncrementalManifestParsed(incrementalManifest)
		}
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
	sl := NewShardLogger(numShards, logsDir)

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
	indexDir := filepath.Join(accountsDbDir, "mithril_db")
	index, err := ingestSSTFiles(indexDir, logsDir)
	if err != nil {
		return nil, nil, fmt.Errorf("initializing pebble from SST files: %w", err)
	}
	index.Close()

	mlog.Log.Infof("Snapshot processed in %s.", fmtDuration(time.Since(start)))

	var largestFileIdBytes [8]byte
	binary.LittleEndian.PutUint64(largestFileIdBytes[:], largestFileId.Load())

	path := filepath.Join(accountsDbDir, "largest_file_id")
	if err := os.WriteFile(path, largestFileIdBytes[:], 0644); err != nil {
		mlog.Log.Errorf("error while writing largest file ID=%d to %s: %s", largestFileId.Load(), path, err)
		return nil, nil, err
	}

	// bootstrap_high_file_id: write-once record of the highest fileId produced
	// by snapshot bootstrap. The batch-fold engine uses it to classify data
	// files: anything newer without a manifest is an undecided orphan.
	bootstrapHighPath := filepath.Join(accountsDbDir, "bootstrap_high_file_id")
	if err := os.WriteFile(bootstrapHighPath, largestFileIdBytes[:], 0644); err != nil {
		mlog.Log.Errorf("error while writing bootstrap high file ID to %s: %s", bootstrapHighPath, err)
		return nil, nil, err
	}

	bankHashOutputFileName := filepath.Join(accountsDbDir, "bank_hash")
	if err := os.WriteFile(bankHashOutputFileName, manifest.Bank.Hash[:], 0644); err != nil {
		mlog.Log.Errorf("error writing bank hash=%x to file=%s: %s", manifest.Bank.Hash, bankHashOutputFileName, err)
		return nil, nil, err
	}

	// Write stake pubkey index file (with appendvec location hints)
	stakeIndexPath := filepath.Join(accountsDbDir, "stake_pubkeys.idx")
	if err := accountsdb.WriteStakePubkeyIndex(stakeIndexPath, stakeCollector.entries); err != nil {
		return nil, nil, fmt.Errorf("writing stake pubkey index: %w", err)
	}

	bankhashDir := filepath.Join(accountsDbDir, "bankhash_db")
	bankhashDb, err := pebble.Open(bankhashDir, &pebble.Options{})
	if err != nil {
		return nil, nil, fmt.Errorf("opening bankhashDir=%s: %w", bankhashDir, err)
	}
	bankhashDb.Close()

	accountsDb, err := accountsdb.OpenDb(accountsDbDir)
	if err != nil {
		return nil, nil, err
	}

	if incrementalManifest != nil {
		return accountsDb, incrementalManifest, nil
	} else {
		return accountsDb, manifest, nil
	}
}

// identify appendvec files, whose path is of the form "accounts/SLOT.ID"
func isAppendVec(filename string) bool {
	return strings.Contains(filename, "accounts/") && strings.Contains(filename, ".")
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
	defer closer.Close()

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

		if !isAppendVec(header.Name) {
			continue
		}

		writer := bytes.NewBuffer(make([]byte, 0, header.Size))
		tarBytesRead, err := io.Copy(writer, tarReader)
		if err != nil {
			mlog.Log.Errorf("err copying data to reader: %s\n", err)
			cleanupPartial("copy error")
			return err
		}
		statsd.Count(statsd.SnapshotTarBytesRead, tarBytesRead, nil)

		// Update extract progress
		if dp != nil {
			dp.Extract.Add(tarBytesRead)
		}

		task := appendVecCopyingTask{TarBuffer: writer, Filename: header.Name, FromIncrementalSnapshot: options.isIncremental}
		if pools == nil {
			cleanupPartial("worker pool unavailable")
			return fmt.Errorf("snapshot archive contains appendvec %q but no worker pool is available", header.Name)
		}
		err = invokeSnapshotTask(wg, pools.appendVecCopying, task)
		if err != nil {
			mlog.Log.Errorf("error calling appendVecCopyingPool.Invoke: %v", err)
			cleanupPartial("pool error")
			return err
		}
	}
	if pools != nil {
		if workerErr := pools.Err(); workerErr != nil {
			cleanupPartial("worker error")
			return workerErr
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
		pubkeys, entries, stakeEntries, err := accountsdb.BuildIndexEntriesFromAppendVecs(task.Data, task.FileSize, task.Slot, task.FileId)
		if err != nil {
			workerErrors.Record(fmt.Errorf("building index entries: %w", err))
			return
		}

		// Collect stake entries with appendvec location hints for building stake index
		stakeCollector.Add(stakeEntries)

		commitTask := indexEntryCommitterTask{IndexEntries: entries, Pubkeys: pubkeys}
		statsd.Timing(statsd.TasksIndexEntryBuilderLatency, uint64(time.Since(start)), nil)
		err = invokeSnapshotTask(wg, indexEntryCommitterPool, commitTask)
		if err != nil {
			workerErrors.Record(fmt.Errorf("submitting index entry committer task: %w", err))
		}
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
		filename := task.Filename
		writer := task.TarBuffer

		outFilename := filepath.Join(accountsDbDir, filename)

		// validate that the path doesn't escape accountsDbDir (via '../' sequences)
		cleanPath := filepath.Clean(outFilename)
		if !strings.HasPrefix(cleanPath, filepath.Clean(accountsDbDir)+string(os.PathSeparator)) {
			workerErrors.Record(fmt.Errorf("invalid path in tar archive: %s", filename))
			return
		}

		appendVecBytes := writer.Bytes()
		err := os.WriteFile(cleanPath, appendVecBytes, 0644)
		if err != nil {
			workerErrors.Record(fmt.Errorf("writing appendvec %s: %w", cleanPath, err))
			return
		}

		var slot, fileId uint64
		if n, err := fmt.Sscanf(filepath.Base(filename), "%d.%d", &slot, &fileId); n != 2 || err != nil {
			workerErrors.Record(fmt.Errorf(
				"failed to parse slot and file from filename=%s basename=%s; parsed n=%d arguments (expected 2) and had err=%v",
				filename, filepath.Base(filename), n, err))
			return
		}

		for {
			prevLargestFileId := largestFileId.Load()
			if fileId <= prevLargestFileId {
				break
			}
			swapped := largestFileId.CompareAndSwap(prevLargestFileId, fileId)
			if swapped {
				break
			}
		}

		// find the relevant appendvec storage info. use the info from the incremental
		// snapshot manifest if this account entry is from the incremental snapshot.
		var fileSize uint64
		var usedIncrementalSnapshotVal bool
		if task.FromIncrementalSnapshot {
			if incrementalManifest == nil {
				workerErrors.Record(fmt.Errorf("tried to process incremental snapshot without having parsed incremental snapshot manifest first"))
				return
			}
			for _, av := range incrementalManifest.AccountsDb.Storages[slot].AcctVecs {
				if av.Id == fileId {
					fileSize = av.FileSize
					usedIncrementalSnapshotVal = true
					break
				}
			}
		}

		if !usedIncrementalSnapshotVal {
			for _, av := range manifest.AccountsDb.Storages[slot].AcctVecs {
				if av.Id == fileId {
					fileSize = av.FileSize
					break
				}
			}
		}

		if fileSize == 0 {
			workerErrors.Record(fmt.Errorf("manifest has no file size for appendvec slot=%d file_id=%d", slot, fileId))
			return
		}

		nextTask := indexEntryBuilderTask{Data: appendVecBytes, FileSize: fileSize, Slot: slot, FileId: fileId}
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
		appendVecCopyingPool,
		indexEntryBuilderPool,
		indexEntryCommitterPool,
		workerErrors,
	}, nil
}

func (p *snapshotWorkerPools) Release() {
	p.appendVecCopying.Release()
	p.indexEntryBuilder.Release()
	p.indexEntryCommitter.Release()
}

// Ingest SSTs into a fresh pebble DB and return it.
func ingestSSTFiles(indexDir, logsDir string) (*pebble.DB, error) {
	db, err := pebble.Open(indexDir, accountsdb.NewAccountsIndexPebbleOptions(nil))
	if err != nil {
		return nil, fmt.Errorf("pebble.Open(%s): %w", indexDir, err)
	}

	glob := filepath.Join(logsDir, "*.sst")
	sstFiles, err := filepath.Glob(glob)
	if err != nil {
		return nil, fmt.Errorf("filepath.Glob(%s): %w", glob, err)
	}
	if len(sstFiles) == 0 {
		return nil, fmt.Errorf("filepath.Glob(%s): unexpectedly globbed 0 SST files!", glob)
	}

	err = db.Ingest(sstFiles)
	if err != nil {
		return nil, fmt.Errorf("ingesting SSTs: %w", err)
	}
	return db, nil
}
