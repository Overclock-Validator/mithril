package snapshot

import (
	"bufio"
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
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/panjf2000/ants/v2"
)

const (
	maxIndexEntryCommitter = 512
	maxIndexEntryBuilder   = 500
	maxAppendVecCopying    = 500
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

func BuildAccountsDbPaths(
	ctx context.Context,
	snapshotFile string,
	incrementalSnapshotFile string,
	accountsDbDir string,
	dp *progress.DualProgress,
) (*accountsdb.AccountsDb, *SnapshotManifest, error) {
	// Clean any leftover artifacts from previous incomplete runs (e.g., Ctrl+C)
	CleanAccountsDbDir(accountsDbDir)

	mlog.Log.Infof("Parsing manifest from %s", snapshotFile)
	manifest, err := UnmarshalManifestFromSnapshot(ctx, snapshotFile, accountsDbDir)
	if err != nil {
		return nil, nil, fmt.Errorf("reading snapshot manifest: %v", err)
	}
	mlog.Log.Infof("Parsed manifest from full snapshot")

	var incrementalManifest *SnapshotManifest
	if incrementalSnapshotFile != "" {
		mlog.Log.Infof("Parsing manifest from %s", incrementalSnapshotFile)
		incrementalManifest, err = UnmarshalManifestFromSnapshot(ctx, incrementalSnapshotFile, accountsDbDir)
		if err != nil {
			return nil, nil, fmt.Errorf("reading incremental snapshot manifest: %v", err)
		}
		mlog.Log.Infof("Parsed manifest from incremental snapshot")
	}

	start := time.Now()

	appendVecsOutputDir := filepath.Join(accountsDbDir, "accounts")
	if err = os.MkdirAll(appendVecsOutputDir, 0775); err != nil {
		return nil, nil, err
	}

	defer ants.Release()

	var largestFileId atomic.Uint64
	wg := &sync.WaitGroup{}

	logsDir := filepath.Join(accountsDbDir, "mithril_db_log_shards")
	if err = os.MkdirAll(logsDir, 0775); err != nil {
		return nil, nil, err
	}
	numShards := 256
	sl := NewShardLogger(numShards, logsDir)

	// Create stake pubkey collector for building stake index during appendvec processing
	stakeCollector := &stakeIndexCollector{
		pubkeys: make([]solana.PublicKey, 0, 1000000), // Pre-allocate for ~1M stake accounts
	}

	pools, err := initWorkerPools(wg, sl, manifest, incrementalManifest, accountsDbDir, &largestFileId, stakeCollector)
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
	err = readTar(ctx, wg, snapshotFile, pools.appendVecCopying, readTarOptions{progress: dp})

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
		err = readTar(ctx, wg, incrementalSnapshotFile, pools.appendVecCopying,
			readTarOptions{isIncremental: true})
		if err != nil {
			return nil, nil, err
		}
		// Wait for all incremental worker tasks to complete
		wg.Wait()
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

	bankHashOutputFileName := filepath.Join(accountsDbDir, "bank_hash")
	if err := os.WriteFile(bankHashOutputFileName, manifest.Bank.Hash[:], 0644); err != nil {
		mlog.Log.Errorf("error writing bank hash=%x to file=%s: %s", manifest.Bank.Hash, bankHashOutputFileName, err)
		return nil, nil, err
	}

	pools.Release()

	// Write stake pubkey index file
	stakeIndexPath := filepath.Join(accountsDbDir, "stake_pubkeys.idx")
	if err := WriteStakePubkeyIndex(stakeIndexPath, stakeCollector.pubkeys); err != nil {
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
}

func readTar(
	ctx context.Context,
	wg *sync.WaitGroup,
	filename string,
	appendVecCopyingPool *ants.PoolWithFunc,
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

	// cleanupPartial deletes the partial download file if it exists
	cleanupPartial := func(reason string) {
		if savePath != "" {
			if _, statErr := os.Stat(savePath); statErr == nil {
				mlog.Log.Infof("Deleting partial download %s (%s)", savePath, reason)
				if rmErr := os.Remove(savePath); rmErr != nil {
					mlog.Log.Errorf("Failed to delete partial download %s: %v", savePath, rmErr)
				}
			}
		}
	}

	for {
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
		wg.Add(1)
		err = appendVecCopyingPool.Invoke(task)
		if err != nil {
			mlog.Log.Errorf("error calling appendVecCopyingPool.Invoke: %v", err)
			cleanupPartial("pool error")
			return err
		}
	}

	return nil
}

type snapshotWorkerPools struct {
	appendVecCopying    *ants.PoolWithFunc
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
	pubkeys []solana.PublicKey
}

func (c *stakeIndexCollector) Add(pks []solana.PublicKey) {
	if len(pks) == 0 {
		return
	}
	c.mu.Lock()
	c.pubkeys = append(c.pubkeys, pks...)
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
	indexEntryCommitterPool, err := ants.NewPoolWithFunc(maxIndexEntryCommitter, func(i any) {
		tasks := indexEntryCommitterInProgress.Add(1)
		statsd.Gauge(statsd.SnapshotWorkerPoolUtilization, float64(tasks)/float64(maxIndexEntryCommitter), []string{"index_entry_committer"})
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

	indexEntryBuilderPool, err := ants.NewPoolWithFunc(maxIndexEntryBuilder, func(i any) {
		tasks := indexEntryBuilderInProgress.Add(1)
		statsd.Gauge(statsd.SnapshotWorkerPoolUtilization, float64(tasks)/float64(maxIndexEntryBuilder), []string{"index_entry_builder"})
		start := time.Now()
		defer wg.Done()
		task := i.(indexEntryBuilderTask)
		pubkeys, entries, stakePubkeys, err := accountsdb.BuildIndexEntriesFromAppendVecs(task.Data, task.FileSize, task.Slot, task.FileId)
		if err != nil {
			mlog.Log.Errorf("BuildIndexEntriesFromAppendVecs: %v", err)
			return
		}

		// Collect stake pubkeys for building stake index
		stakeCollector.Add(stakePubkeys)

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

	appendVecCopyingPool, err := ants.NewPoolWithFunc(maxAppendVecCopying, func(i any) {
		tasks := appendVecCopyingInProgress.Add(1)
		statsd.Gauge(statsd.SnapshotWorkerPoolUtilization, float64(tasks)/float64(maxAppendVecCopying), []string{"append_vec_copying"})
		start := time.Now()
		defer wg.Done()
		task := i.(appendVecCopyingTask)
		filename := task.Filename
		writer := task.TarBuffer

		outFilename := filepath.Join(accountsDbDir, filename)

		// validate that the path doesn't escape accountsDbDir (via '../' sequences)
		cleanPath := filepath.Clean(outFilename)
		if !strings.HasPrefix(cleanPath, filepath.Clean(accountsDbDir)+string(os.PathSeparator)) {
			panic(fmt.Sprintf("invalid path in tar archive: %s", filename))
		}

		appendVecBytes := writer.Bytes()
		err := os.WriteFile(cleanPath, appendVecBytes, 0644)
		if err != nil {
			mlog.Log.Errorf("err writing new file=%s: %v", cleanPath, err)
			appendVecCopyingInProgress.Add(-1)
			return
		}

		var slot, fileId uint64
		if n, err := fmt.Sscanf(filepath.Base(filename), "%d.%d", &slot, &fileId); n != 2 || err != nil {
			panic(fmt.Sprintf(
				"failed to parse slot and file from filename=%s basename=%s; parsed n=%d arguments (expected 2) and had err=%v",
				filename, filepath.Base(filename), n, err))
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
				panic("tried to process incremental snapshot without having parsed incremental snapshot manifest first!")
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
			panic("programming error - fileSize for appendvec was 0")
		}

		appendVecCopyingInProgress.Add(-1)
		nextTask := indexEntryBuilderTask{Data: appendVecBytes, FileSize: fileSize, Slot: slot, FileId: fileId}
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
		appendVecCopyingPool,
		indexEntryBuilderPool,
		indexEntryCommitterPool,
	}, nil
}

func (p *snapshotWorkerPools) Release() {
	p.appendVecCopying.Release()
	p.indexEntryBuilder.Release()
	p.indexEntryCommitter.Release()
}

// Ingest SSTs into a fresh pebble DB and return it.
func ingestSSTFiles(indexDir, logsDir string) (*pebble.DB, error) {
	db, err := pebble.Open(indexDir, &pebble.Options{})
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

// WriteStakePubkeyIndex writes stake pubkeys to a binary index file.
// Format: 32-byte pubkeys appended sequentially, no header.
func WriteStakePubkeyIndex(path string, pubkeys []solana.PublicKey) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := bufio.NewWriter(f)
	for _, pk := range pubkeys {
		if _, err := buf.Write(pk[:]); err != nil {
			return err
		}
	}
	return buf.Flush()
}

// LoadStakePubkeyIndex reads stake pubkeys from a binary index file.
func LoadStakePubkeyIndex(path string) ([]solana.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	count := len(data) / 32
	pubkeys := make([]solana.PublicKey, count)
	for i := 0; i < count; i++ {
		copy(pubkeys[i][:], data[i*32:(i+1)*32])
	}
	return pubkeys, nil
}
