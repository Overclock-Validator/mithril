package snapshot

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/cockroachdb/pebble"
	"github.com/panjf2000/ants/v2"
)

const (
	DefaultSnapshotIndexEntryCommitterWorkers = 64
	DefaultSnapshotIndexEntryBuilderWorkers   = 64
	DefaultSnapshotAppendVecCopyingWorkers    = 32
	DefaultSnapshotIndexShards                = 64
	DefaultSnapshotMaxConcurrentFlushers      = 8
	// DefaultSnapshotFlushSortWorkers == 0 means "auto": use runtime.NumCPU().
	DefaultSnapshotFlushSortWorkers = 0
	// DefaultSnapshotDirectIO keeps O_DIRECT big-file writes off by default;
	// buffered is the safe default and O_DIRECT is an opt-in tuning knob.
	DefaultSnapshotDirectIO = false
)

var (
	SnapshotIndexEntryCommitterWorkers = DefaultSnapshotIndexEntryCommitterWorkers
	SnapshotIndexEntryBuilderWorkers   = DefaultSnapshotIndexEntryBuilderWorkers
	SnapshotAppendVecCopyingWorkers    = DefaultSnapshotAppendVecCopyingWorkers
	SnapshotIndexShards                = DefaultSnapshotIndexShards
	SnapshotFlushSortWorkers           = DefaultSnapshotFlushSortWorkers
	SnapshotDirectIO                   = DefaultSnapshotDirectIO
	SnapshotIndexTempDir               string
)

// CleanAccountsDbDir removes all artifacts from a previous incomplete snapshot run.
// This prevents corruption from Ctrl+C or partial downloads.
// Exported so it can be called early in startup before any failures.
func CleanAccountsDbDir(accountsPaths []string) {
	if len(accountsPaths) == 0 {
		return
	}
	// List of all files/directories that may be left from a previous incomplete run
	artifacts := []string{
		"accounts",
		"mithril_db",
		"mithril_db_log_shards",
		"bankhash_db",
		"largest_file_id",
		"num_shards",
		"manifest",
		"mithril_state.json", // State file for tracking valid builds and replay progress
	}
	for _, artifact := range artifacts {
		path := filepath.Join(accountsPaths[0], artifact)
		if err := os.RemoveAll(path); err != nil {
			mlog.Log.Errorf("failed to remove %s: %v", path, err)
		}
	}
	// remove the accounts dir on the other shard disks too
	for _, p := range accountsPaths[1:] {
		path := filepath.Join(p, "accounts")
		if err := os.RemoveAll(path); err != nil {
			mlog.Log.Errorf("failed to remove %s: %v", path, err)
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

// snapshotFlushSortWorkers is how many shard buffers may be sorted concurrently
// during the index flush. 0 (the default) means auto: one per logical CPU.
func snapshotFlushSortWorkers() int {
	if SnapshotFlushSortWorkers > 0 {
		return SnapshotFlushSortWorkers
	}
	return runtime.NumCPU()
}

func logSnapshotBootstrapTuning() {
	indexTempDir := SnapshotIndexTempDir
	if indexTempDir == "" {
		indexTempDir = "(accountsdb)"
	}
	mlog.Log.Infof("Snapshot bootstrap tuning: append_vec_workers=%d index_builder_workers=%d index_committer_workers=%d index_shards=%d max_concurrent_flushers=%d flush_sort_workers=%d directio=%v zstd_decoder_concurrency=%d index_temp_dir=%s",
		snapshotAppendVecCopyingWorkers(),
		snapshotIndexEntryBuilderWorkers(),
		snapshotIndexEntryCommitterWorkers(),
		snapshotIndexShards(),
		snapshotMaxConcurrentFlushers(),
		snapshotFlushSortWorkers(),
		SnapshotDirectIO,
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
	accountsPaths []string,
	dp *progress.DualProgress,
) (*accountsdb.AccountsDb, *SnapshotManifest, error) {
	if len(accountsPaths) == 0 {
		return nil, nil, fmt.Errorf("no accounts paths configured")
	}
	// The first path holds all metadata; every path holds a shard's accounts dir.
	accountsDbDir := accountsPaths[0]
	// Clean any leftover artifacts from previous incomplete runs (e.g., Ctrl+C)
	CleanAccountsDbDir(accountsPaths)

	mlog.Log.Infof("Parsing full snapshot manifest...")
	manifest, err := UnmarshalManifestFromSnapshot(ctx, snapshotFile, accountsDbDir)
	if err != nil {
		return nil, nil, fmt.Errorf("reading snapshot manifest: %v", err)
	}
	mlog.Log.Infof("Parsed full snapshot manifest")

	var incrementalManifest *SnapshotManifest
	if incrementalSnapshotFile != "" {
		mlog.Log.Infof("Parsing incremental snapshot manifest...")
		incrementalManifest, err = UnmarshalManifestFromSnapshot(ctx, incrementalSnapshotFile, accountsDbDir)
		if err != nil {
			return nil, nil, fmt.Errorf("reading incremental snapshot manifest: %v", err)
		}
		mlog.Log.Infof("Parsed incremental snapshot manifest")
	}

	start := time.Now()

	shardDirs := make([]string, len(accountsPaths))
	for i, p := range accountsPaths {
		shardDirs[i] = filepath.Join(p, "accounts")
		if err = os.MkdirAll(shardDirs[i], 0775); err != nil {
			return nil, nil, err
		}
	}
	shardFiles, err := openShardBigFiles(shardDirs)
	if err != nil {
		return nil, nil, fmt.Errorf("opening shard big files: %w", err)
	}
	logSnapshotBootstrapTuning()

	defer ants.Release()

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

	pools, err := initWorkerPools(wg, sl, manifest, incrementalManifest, shardFiles, stakeCollector)
	if err != nil {
		return nil, nil, fmt.Errorf("initializing worker pools: %w", err)
	}

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
	err = readTar(ctx, wg, snapshotFile, pools.appendVecCopying, pools.tarBufs, readTarOptions{progress: dp})

	// Wait for ALL worker tasks from full snapshot to complete before starting incremental
	wg.Wait()

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
		err = readTar(ctx, wg, incrementalSnapshotFile, pools.appendVecCopying, pools.tarBufs,
			readTarOptions{isIncremental: true})
		if err != nil {
			return nil, nil, err
		}
		// Wait for all incremental worker tasks to complete
		wg.Wait()
	}

	// flush and close every shard's big file now that all appends are done
	if err := shardFiles.close(); err != nil {
		return nil, nil, fmt.Errorf("closing shard big files: %w", err)
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

	if err := writeShardMetadata(accountsDbDir, len(shardDirs)); err != nil {
		return nil, nil, err
	}

	bankHashOutputFileName := filepath.Join(accountsDbDir, "bank_hash")
	if err := os.WriteFile(bankHashOutputFileName, manifest.Bank.Hash[:], 0644); err != nil {
		mlog.Log.Errorf("error writing bank hash=%x to file=%s: %s", manifest.Bank.Hash, bankHashOutputFileName, err)
		return nil, nil, err
	}

	pools.Release()

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

	accountsDb, err := accountsdb.OpenDb(accountsPaths)
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
}

// tarBufPool recycles the byte buffers that appendvec data is read into, saving
// ~420k allocations totalling ~470 GB. A buffer returns to the pool once the index
// builder is done reading it. Owned by snapshotWorkerPools so it may be GCed after
// snapshot unpacking.
type tarBufPool struct{ sync.Pool }

func newTarBufPool() *tarBufPool {
	return &tarBufPool{Pool: sync.Pool{New: func() any { b := []byte(nil); return &b }}}
}

func (p *tarBufPool) get(size int) *[]byte {
	bp := p.Get().(*[]byte)
	if cap(*bp) < size {
		*bp = make([]byte, size)
	} else {
		*bp = (*bp)[:size]
	}
	return bp
}

func (p *tarBufPool) put(bp *[]byte) {
	if bp != nil {
		p.Put(bp)
	}
}

func readTar(
	ctx context.Context,
	wg *sync.WaitGroup,
	filename string,
	appendVecCopyingPool *ants.PoolWithFunc,
	bufs *tarBufPool,
	options readTarOptions,
) error {
	dp := options.progress
	savePath := options.savePath
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

	var totalTarNext, totalReadCopy, totalDispatch time.Duration
	var entryCount int
	var bytesOut int64
	readTarStart := time.Now()
	for {
		if ctx.Err() != nil {
			mlog.Log.Infof("Context cancelled, stopping snapshot unpack: %v", ctx.Err())
			cleanupPartial("cancelled")
			return ctx.Err()
		}
		t0 := time.Now()
		header, err := tarReader.Next()
		totalTarNext += time.Since(t0)
		if err == io.EOF {
			break
		} else if err != nil {
			mlog.Log.Errorf("reading next tar: %s\n", err)
			cleanupPartial("read error")
			return err
		}

		if !isAppendVec(header.Name) {
			continue
		}

		t1 := time.Now()
		bp := bufs.get(int(header.Size))
		_, err = io.ReadFull(tarReader, *bp)
		totalReadCopy += time.Since(t1)
		if err != nil {
			mlog.Log.Errorf("err reading tar entry data: %s\n", err)
			cleanupPartial("read error")
			bufs.put(bp)
			return err
		}
		entryCount++
		bytesOut += header.Size
		statsd.Count(statsd.SnapshotTarBytesRead, header.Size, nil)

		// Update extract progress
		if dp != nil {
			dp.Extract.Add(header.Size)
		}

		task := appendVecCopyingTask{Buf: *bp, BufRef: bp, Filename: header.Name, FromIncrementalSnapshot: options.isIncremental}
		wg.Add(1)
		t2 := time.Now()
		err = appendVecCopyingPool.Invoke(task)
		totalDispatch += time.Since(t2)
		if err != nil {
			mlog.Log.Errorf("error calling appendVecCopyingPool.Invoke: %v", err)
			cleanupPartial("pool error")
			return err
		}
	}

	elapsed := time.Since(readTarStart)
	mibps := 0.0
	if elapsed > 0 {
		mibps = float64(bytesOut) / (1 << 20) / elapsed.Seconds()
	}
	mlog.Log.Infof("readTar timing: entries=%d bytesOut=%dMB elapsed=%s avgMiBps=%.0f tarNext=%s readCopy=%s dispatch=%s",
		entryCount, bytesOut/(1<<20), fmtDuration(elapsed), mibps,
		fmtDuration(totalTarNext), fmtDuration(totalReadCopy), fmtDuration(totalDispatch))

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
	tarBufs             *tarBufPool
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
	shardFiles *shardBigFiles,
	stakeCollector *stakeIndexCollector,
) (*snapshotWorkerPools, error) {
	indexEntryCommitterWorkers := snapshotIndexEntryCommitterWorkers()
	indexEntryBuilderWorkers := snapshotIndexEntryBuilderWorkers()
	appendVecCopyingWorkers := snapshotAppendVecCopyingWorkers()
	tarBufs := newTarBufPool()

	indexEntryCommitterPool, err := ants.NewPoolWithFunc(indexEntryCommitterWorkers, func(i any) {
		tasks := indexEntryCommitterInProgress.Add(1)
		statsd.Gauge(statsd.SnapshotWorkerPoolUtilization, float64(tasks)/float64(indexEntryCommitterWorkers), []string{"index_entry_committer"})
		start := time.Now()
		defer wg.Done()
		task := i.(indexEntryCommitterTask)

		for idx, entry := range task.IndexEntries {
			sl.EnqueueRequest(task.Pubkeys[idx], entry)
		}
		statsd.Timing(statsd.TaskIndexEntryCommitterLatency, uint64(time.Since(start)), nil)
		indexEntryCommitterInProgress.Add(-1)
	})
	if err != nil {
		return nil, err
	}

	indexEntryBuilderPool, err := ants.NewPoolWithFunc(indexEntryBuilderWorkers, func(i any) {
		tasks := indexEntryBuilderInProgress.Add(1)
		statsd.Gauge(statsd.SnapshotWorkerPoolUtilization, float64(tasks)/float64(indexEntryBuilderWorkers), []string{"index_entry_builder"})
		start := time.Now()
		defer wg.Done()
		task := i.(indexEntryBuilderTask)
		pubkeys, entries, stakeEntries, err := accountsdb.BuildIndexEntriesFromAppendVecs(task.Data, task.FileSize, task.Slot, task.FileId, task.BaseOffset)
		tarBufs.put(task.BufRef) // done reading task.Data; recycle the buffer
		if err != nil {
			mlog.Log.Errorf("BuildIndexEntriesFromAppendVecs: %v", err)
			return
		}

		// Collect stake entries with appendvec location hints for building stake index
		stakeCollector.Add(stakeEntries)

		indexEntryBuilderInProgress.Add(-1)
		commitTask := indexEntryCommitterTask{IndexEntries: entries, Pubkeys: pubkeys}
		wg.Add(1)
		statsd.Timing(statsd.TasksIndexEntryBuilderLatency, uint64(time.Since(start)), nil)
		err = indexEntryCommitterPool.Invoke(commitTask)
		if err != nil {
			mlog.Log.Errorf("indexEntryCommitterPool.Invoke: %v", err)
		}
	})
	if err != nil {
		return nil, err
	}

	appendVecCopyingPool, err := ants.NewPoolWithFunc(appendVecCopyingWorkers, func(i any) {
		tasks := appendVecCopyingInProgress.Add(1)
		statsd.Gauge(statsd.SnapshotWorkerPoolUtilization, float64(tasks)/float64(appendVecCopyingWorkers), []string{"append_vec_copying"})
		start := time.Now()
		defer wg.Done()
		task := i.(appendVecCopyingTask)
		filename := task.Filename
		appendVecBytes := task.Buf

		// origFileId is the snapshot's id, used only to look up the valid data
		// length in the manifest
		var slot, origFileId uint64
		if n, err := fmt.Sscanf(filepath.Base(filename), "%d.%d", &slot, &origFileId); n != 2 || err != nil {
			panic(fmt.Sprintf(
				"failed to parse slot and file from filename=%s basename=%s; parsed n=%d arguments (expected 2) and had err=%v",
				filename, filepath.Base(filename), n, err))
		}

		// find the relevant appendvec storage info. use the info from the incremental
		// snapshot manifest if this account entry is from the incremental snapshot.
		var fileSize uint64
		var usedIncrementalSnapshotVal bool
		if task.FromIncrementalSnapshot {
			if incrementalManifest == nil {
				panic("tried to process incremental snapshot without having parsed incremental snapshot manifest first!")
			}
			for _, av := range incrementalManifest.AccountsDb.Storages[slot].AcctVecs {
				if av.Id == origFileId {
					fileSize = av.FileSize
					usedIncrementalSnapshotVal = true
					break
				}
			}
		}

		if !usedIncrementalSnapshotVal {
			for _, av := range manifest.AccountsDb.Storages[slot].AcctVecs {
				if av.Id == origFileId {
					fileSize = av.FileSize
					break
				}
			}
		}

		if fileSize == 0 {
			panic("programming error - fileSize for appendvec was 0")
		}
		if uint64(len(appendVecBytes)) < fileSize {
			panic(fmt.Sprintf("appendvec blob (%d bytes) shorter than manifest fileSize (%d)", len(appendVecBytes), fileSize))
		}

		fileId, base, err := shardFiles.write(appendVecBytes[:fileSize])
		if err != nil {
			mlog.Log.Errorf("err writing appendvec to shard big file: %v", err)
			appendVecCopyingInProgress.Add(-1)
			tarBufs.put(task.BufRef)
			return
		}

		appendVecCopyingInProgress.Add(-1)
		nextTask := indexEntryBuilderTask{Data: appendVecBytes, FileSize: fileSize, Slot: slot, FileId: fileId, BaseOffset: base, BufRef: task.BufRef}
		wg.Add(1)
		statsd.Timing(statsd.TasksAppendVecCopyingLatency, uint64(time.Since(start)), nil)
		err = indexEntryBuilderPool.Invoke(nextTask)
		if err != nil {
			mlog.Log.Errorf("error calling indexEntryBuilderPool.Invoke\n")
		}
	})
	if err != nil {
		return nil, err
	}

	return &snapshotWorkerPools{
		appendVecCopying:    appendVecCopyingPool,
		indexEntryBuilder:   indexEntryBuilderPool,
		indexEntryCommitter: indexEntryCommitterPool,
		tarBufs:             tarBufs,
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
