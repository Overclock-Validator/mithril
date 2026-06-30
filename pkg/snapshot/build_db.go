package snapshot

import (
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
	"github.com/Overclock-Validator/mithril/pkg/util"
	"github.com/cockroachdb/pebble"
	"github.com/panjf2000/ants/v2"
)

const (
	DefaultSnapshotIndexEntryCommitterWorkers = 64
	DefaultSnapshotIndexEntryBuilderWorkers   = 64
	DefaultSnapshotIndexShards                = 64
	DefaultSnapshotMaxConcurrentFlushers      = 8
	DefaultSnapshotBufCount                   = 4
	DefaultSnapshotWriteWorkers               = 16
)

var (
	SnapshotIndexEntryCommitterWorkers = DefaultSnapshotIndexEntryCommitterWorkers
	SnapshotIndexEntryBuilderWorkers   = DefaultSnapshotIndexEntryBuilderWorkers
	SnapshotIndexShards                = DefaultSnapshotIndexShards
	SnapshotIndexTempDir               string

	// SnapshotDirectIO, when true, opens the big snapshot files with O_DIRECT and
	// writes them with page-aligned buffers (Linux only). Off by default; flip to
	// test O_DIRECT throughput. Both modes produce an identical on-disk layout.
	SnapshotDirectIO = false

	// SnapshotBufSize is the size of each pooled buffer used during snapshot
	// unpacking. Appendvecs are packed into these buffers; when the next entry
	// won't fit, the buffer is flushed (written to the big file + dispatched to
	// the index builders) and a fresh buffer is taken from the pool. Must be a
	// multiple of the page size for O_DIRECT.
	SnapshotBufSize = 128 * 1024 * 1024 // 128 MB

	// SnapshotBufCount is the number of pooled buffers, bounding how many can be
	// in flight (held by the index builders) before readTar blocks.
	SnapshotBufCount = DefaultSnapshotBufCount

	// SnapshotWriteWorkers is how many concurrent pwrites each buffer is split
	// into (queue depth to the RAID). 1 = a single write per buffer.
	SnapshotWriteWorkers = DefaultSnapshotWriteWorkers
)

// CleanAccountsDbDir removes all artifacts from a previous incomplete snapshot run.
// This prevents corruption from Ctrl+C or partial downloads.
// Exported so it can be called early in startup before any failures.
func CleanAccountsDbDir(accountsDbDir string) {
	// List of all files/directories that may be left from a previous incomplete run
	artifacts := []string{
		"accounts",
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
	mlog.Log.Infof("Snapshot bootstrap tuning: direct_io=%t buf_size=%d buf_count=%d write_workers=%d index_builder_workers=%d index_committer_workers=%d index_shards=%d max_concurrent_flushers=%d zstd_decoder_concurrency=%d index_temp_dir=%s",
		SnapshotDirectIO,
		SnapshotBufSize,
		SnapshotBufCount,
		SnapshotWriteWorkers,
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
	// Clean any leftover artifacts from previous incomplete runs (e.g., Ctrl+C)
	CleanAccountsDbDir(accountsDbDir)

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

	appendVecsOutputDir := filepath.Join(accountsDbDir, "accounts")
	if err = os.MkdirAll(appendVecsOutputDir, 0775); err != nil {
		return nil, nil, err
	}
	logSnapshotBootstrapTuning()

	defer ants.Release()

	fullFileSizes, fullTotalSize, fullLargestFileId := manifestStats(manifest, 0)
	incrFileSizes, incrTotalSize, incrLargestFileId := manifestStats(incrementalManifest, incrementalMinSlot(manifest))
	largestFileId := max(fullLargestFileId, incrLargestFileId)

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

	pools, err := initWorkerPools(wg, sl, stakeCollector)
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

	// Create and preallocate the big snapshot file, then unpack the full snapshot
	// into it sequentially.
	mlog.Log.Infof("Full snapshot total size: %d bytes (%.1f GB)", fullTotalSize, float64(fullTotalSize)/(1024*1024*1024))
	snapshotDat, err := openBigFile(filepath.Join(appendVecsOutputDir, "snapshot.dat"), fullTotalSize)
	if err != nil {
		return nil, nil, fmt.Errorf("creating snapshot.dat: %w", err)
	}

	err = readTar(ctx, wg, snapshotFile, readTarOptions{
		progress:              dp,
		sentinelFileId:        accountsdb.SnapshotFileId,
		fileSizes:             fullFileSizes,
		indexEntryBuilderPool: pools.indexEntryBuilder,
		bigFile:               snapshotDat,
	})

	// Wait for ALL worker tasks from full snapshot to complete before starting incremental
	wg.Wait()
	snapshotDat.Close()

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
		mlog.Log.Infof("Incremental snapshot total size: %d bytes (%.1f GB)", incrTotalSize, float64(incrTotalSize)/(1024*1024*1024))
		incrDat, err := openBigFile(filepath.Join(appendVecsOutputDir, "incremental.dat"), incrTotalSize)
		if err != nil {
			return nil, nil, fmt.Errorf("creating incremental.dat: %w", err)
		}

		err = readTar(ctx, wg, incrementalSnapshotFile, readTarOptions{
			sentinelFileId:        accountsdb.IncrementalFileId,
			fileSizes:             incrFileSizes,
			indexEntryBuilderPool: pools.indexEntryBuilder,
			bigFile:               incrDat,
		})
		// Wait for all incremental worker tasks to complete
		wg.Wait()
		incrDat.Close()
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
	binary.LittleEndian.PutUint64(largestFileIdBytes[:], largestFileId)

	path := filepath.Join(accountsDbDir, "largest_file_id")
	if err := os.WriteFile(path, largestFileIdBytes[:], 0644); err != nil {
		mlog.Log.Errorf("error while writing largest file ID=%d to %s: %s", largestFileId, path, err)
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

	// Big-file sequential write target
	bigFile *os.File
	// Sentinel FileId recorded in every index entry from this snapshot
	// (accountsdb.SnapshotFileId or accountsdb.IncrementalFileId).
	sentinelFileId uint64
	// (slot, fileId) -> fileSize, prebuilt from the manifest(s).
	fileSizes map[fileSizeKey]uint64
	// Pool that index-builder batches are dispatched to.
	indexEntryBuilderPool *ants.PoolWithFunc
}

func readTar(
	ctx context.Context,
	wg *sync.WaitGroup,
	filename string,
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

	bufSize := SnapshotBufSize
	fd := options.bigFile

	// Pool of buffers that appendvecs are packed into. Only fileSize bytes per
	// entry are packed (tar padding is skipped); appendvecs are never split.
	bufPool := make(chan []byte, SnapshotBufCount)
	for range SnapshotBufCount {
		bufPool <- accountsdb.AlignedAlloc(bufSize)
	}

	writer := newBigFileWriter(fd, options.indexEntryBuilderPool, wg, SnapshotBufCount, SnapshotWriteWorkers)

	// fail aborts the unpack: remove the partial download and stop the writer (so
	// every dispatched builder task is accounted for before the caller's wg.Wait),
	// then return the error. Fatal write errors panic inside the writer.
	fail := func(reason string, err error) error {
		cleanupPartial(reason)
		writer.close()
		return err
	}

	currentBuf := <-bufPool
	currentBufPool := bufPool
	writePos := 0
	var pendingEntries []appendVecEntry
	var pendingBaseOffset uint64 // global big-file offset of pendingEntries[0]
	currentOffset := uint64(0)

	var totalTarNext, totalReadFull, totalFlush, totalBufPoolWait time.Duration
	var flushCount, entryCount int
	start := time.Now()

	for {
		if ctx.Err() != nil {
			mlog.Log.Infof("Context cancelled, stopping snapshot unpack: %v", ctx.Err())
			return fail("cancelled", ctx.Err())
		}
		t0 := time.Now()
		header, err := tarReader.Next()
		totalTarNext += time.Since(t0)
		if err == io.EOF {
			break
		} else if err != nil {
			mlog.Log.Errorf("reading next tar: %s\n", err)
			return fail("read error", err)
		}

		if !isAppendVec(header.Name) {
			continue
		}

		// Parse slot.fileId and look up fileSize so we read only the meaningful
		// bytes; tarReader.Next() auto-skips the rest of the entry.
		var slot, fileId uint64
		if n, err := fmt.Sscanf(filepath.Base(header.Name), "%d.%d", &slot, &fileId); n != 2 || err != nil {
			panic(fmt.Sprintf(
				"failed to parse slot and file from filename=%s basename=%s; parsed n=%d arguments (expected 2) and had err=%v",
				header.Name, filepath.Base(header.Name), n, err))
		}
		fileSize := options.fileSizes[fileSizeKey{slot, fileId}]
		if fileSize == 0 {
			panic(fmt.Sprintf("programming error - fileSize for appendvec slot=%d fileId=%d was 0", slot, fileId))
		}
		entrySize := int(fileSize)

		// Flush the current buffer when this entry would not fit.
		if writePos+entrySize > bufSize {
			t1 := time.Now()

			// Writes are page-aligned (required for O_DIRECT, harmless otherwise),
			// so the sub-page tail is carried to the start of the next buffer and
			// the writer always gets a page-aligned, page-multiple slice.
			writeLen := util.AlignDown(writePos, accountsdb.PageSize)
			tailLen := writePos - writeLen

			// Acquire the next buffer before sending the current one to the
			// writer, so we can copy the carry out of the current buffer.
			var newBuf []byte
			var newBufPool chan []byte
			if entrySize > bufSize {
				mlog.Log.Warnf("appendvec %s (%d bytes) exceeds buffer size (%d bytes); using one-off buffer",
					header.Name, entrySize, bufSize)
				newBuf = accountsdb.AlignedAlloc(util.AlignUp(tailLen+entrySize, accountsdb.PageSize))
				newBufPool = nil
			} else {
				t2 := time.Now()
				newBuf = <-bufPool
				totalBufPoolWait += time.Since(t2)
				newBufPool = bufPool
			}
			if tailLen > 0 {
				copy(newBuf[:tailLen], currentBuf[writeLen:writePos])
			}

			writer.submit(writeTask{
				buf:        currentBuf,
				writeLen:   writeLen,
				entries:    pendingEntries,
				baseOffset: pendingBaseOffset,
				fileId:     options.sentinelFileId,
				bufPool:    currentBufPool,
			})
			totalFlush += time.Since(t1)
			flushCount++

			pendingEntries = nil
			currentBuf = newBuf
			currentBufPool = newBufPool
			writePos = tailLen
		}

		t3 := time.Now()
		_, err = io.ReadFull(tarReader, currentBuf[writePos:writePos+entrySize])
		totalReadFull += time.Since(t3)
		if err != nil {
			mlog.Log.Errorf("err reading tar entry: %s\n", err)
			return fail("read error", err)
		}

		if len(pendingEntries) == 0 {
			pendingBaseOffset = currentOffset // first entry of a fresh batch
		}
		pendingEntries = append(pendingEntries, appendVecEntry{
			Data: currentBuf[writePos : writePos+entrySize],
			Slot: slot,
		})
		writePos += entrySize
		currentOffset += fileSize
		entryCount++

		statsd.Count(statsd.SnapshotTarBytesRead, int64(header.Size), nil)
		if dp != nil {
			dp.Extract.Add(int64(header.Size))
		}
	}

	// Flush the last buffer, page-padding the tail so the final write stays aligned.
	finalWriteLen := util.AlignUp(writePos, accountsdb.PageSize)
	for i := writePos; i < finalWriteLen; i++ {
		currentBuf[i] = 0
	}
	writer.submit(writeTask{
		buf:        currentBuf,
		writeLen:   finalWriteLen,
		entries:    pendingEntries,
		baseOffset: pendingBaseOffset,
		fileId:     options.sentinelFileId,
		bufPool:    currentBufPool,
	})
	flushCount++

	writer.close()

	if err := fd.Truncate(int64(currentOffset)); err != nil {
		return fmt.Errorf("truncating big file: %w", err)
	}

	elapsed := time.Since(start)
	mlog.Log.Infof("readTar timing: entries=%d flushes=%d bytesOut=%dMB elapsed=%s avgMiBps=%.0f tarNext=%s readFull=%s flush=%s bufPoolWait=%s",
		entryCount, flushCount, currentOffset/(1024*1024), fmtDuration(elapsed), avgMiBps(currentOffset, elapsed),
		fmtDuration(totalTarNext), fmtDuration(totalReadFull), fmtDuration(totalFlush), fmtDuration(totalBufPoolWait))

	// Successfully processed the entire tar — finalize by renaming from .partial
	if err := FinalizePartialDownload(savePath); err != nil {
		mlog.Log.Errorf("Failed to finalize snapshot download: %v", err)
		// Don't return error — the snapshot was processed successfully,
		// finalization failure just means we can't reuse the cached file
	}

	return nil
}

type snapshotWorkerPools struct {
	indexEntryBuilder   *ants.PoolWithFunc
	indexEntryCommitter *ants.PoolWithFunc
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
	stakeCollector *stakeIndexCollector,
) (*snapshotWorkerPools, error) {
	indexEntryCommitterWorkers := snapshotIndexEntryCommitterWorkers()
	indexEntryBuilderWorkers := snapshotIndexEntryBuilderWorkers()

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

		// Parse every appendvec packed into the shared buffer. Entries are in file
		// order with no gaps, so a running offset (starting at the batch's
		// BaseOffset) is each appendvec's global big-file base; it turns every
		// account's in-appendvec offset into a global offset. task.FileId is the
		// big-file sentinel.
		baseOffset := task.BaseOffset
		for _, entry := range task.Entries {
			pubkeys, entries, stakeEntries, err := accountsdb.BuildIndexEntriesFromAppendVecs(
				entry.Data, uint64(len(entry.Data)), entry.Slot, task.FileId, baseOffset)
			baseOffset += uint64(len(entry.Data))
			if err != nil {
				mlog.Log.Errorf("BuildIndexEntriesFromAppendVecs: %v", err)
				continue
			}

			// Collect stake entries with appendvec location hints for building stake index
			stakeCollector.Add(stakeEntries)

			commitTask := indexEntryCommitterTask{IndexEntries: entries, Pubkeys: pubkeys}
			wg.Add(1)
			if err := indexEntryCommitterPool.Invoke(commitTask); err != nil {
				wg.Done()
				mlog.Log.Errorf("indexEntryCommitterPool.Invoke: %v", err)
			}
		}

		// All entries parsed (their pubkeys/index entries were copied out), so the
		// shared buffer can be returned to the pool for reuse.
		if task.Pool != nil {
			task.Pool <- task.Buf
		}

		indexEntryBuilderInProgress.Add(-1)
		statsd.Timing(statsd.TasksIndexEntryBuilderLatency, uint64(time.Since(start)), nil)
	})
	if err != nil {
		return nil, err
	}

	return &snapshotWorkerPools{
		indexEntryBuilderPool,
		indexEntryCommitterPool,
	}, nil
}

func (p *snapshotWorkerPools) Release() {
	p.indexEntryBuilder.Release()
	p.indexEntryCommitter.Release()
}

// openBigFile creates and preallocates a big snapshot file. It uses O_DIRECT
// when SnapshotDirectIO is set (Linux only); otherwise a normal file.
func openBigFile(path string, totalSize uint64) (*os.File, error) {
	var f *os.File
	var err error
	if SnapshotDirectIO {
		f, err = accountsdb.OpenDirect(path, os.O_CREATE|os.O_RDWR, 0644)
	} else {
		f, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	}
	if err != nil {
		return nil, err
	}
	if err := accountsdb.Fallocate(f, int64(totalSize)); err != nil {
		f.Close()
		return nil, fmt.Errorf("fallocate %s: %w", path, err)
	}
	return f, nil
}

// fileSizeKey identifies an appendvec by (slot, fileId).
type fileSizeKey struct {
	slot   uint64
	fileId uint64
}

// manifestStats returns, in a single pass over storages at slot >= minSlot, the
// (slot,fileId)->fileSize lookup, the total appendvec size (Σ FileSize), and the
// largest appendvec id.
//
// For a full snapshot pass minSlot 0 (all storages). For an incremental snapshot
// pass incrementalMinSlot(m) to skip over the full snapshot manifest data
// which is also included in the incremental snapshot.
func manifestStats(manifest *SnapshotManifest, minSlot uint64) (fileSizes map[fileSizeKey]uint64, totalSize uint64, largestFileId uint64) {
	if manifest == nil {
		return nil, 0, 0
	}
	fileSizes = make(map[fileSizeKey]uint64)
	for _, slotAcctVecs := range manifest.AccountsDb.Storages {
		if slotAcctVecs.Slot < minSlot {
			continue
		}
		for _, av := range slotAcctVecs.AcctVecs {
			fileSizes[fileSizeKey{slotAcctVecs.Slot, av.Id}] = av.FileSize
			totalSize += av.FileSize
			if av.Id > largestFileId {
				largestFileId = av.Id
			}
		}
	}
	return fileSizes, totalSize, largestFileId
}

func avgMiBps(n uint64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / (1024 * 1024) / d.Seconds()
}

// incrementalMinSlot is one past the full snapshot's slot; storages at or below it
// are the base (already in snapshot.dat) and are skipped when sizing incremental.dat.
func incrementalMinSlot(fullManifest *SnapshotManifest) uint64 {
	if fullManifest == nil || fullManifest.Bank == nil {
		return 0
	}
	return fullManifest.Bank.Slot + 1
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
