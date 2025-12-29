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

	"github.com/Overclock-Validator/fastcache"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/snapshotdl"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/panjf2000/ants/v2"
	"k8s.io/klog/v2"
)

// fmtDuration formats a duration in a human-readable format (e.g., "19m30s")
func fmtDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%.3fms", float64(d.Microseconds())/1000)
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		mins := int(d.Minutes())
		secs := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm%ds", mins, secs)
	}
	hours := int(d.Hours())
	mins := int(d.Minutes()) % 60
	secs := int(d.Seconds()) % 60
	return fmt.Sprintf("%dh%dm%ds", hours, mins, secs)
}

func BuildAccountsDbWithIncr(
	ctx context.Context,
	fullSnapshotFile string,
	snapshotDownloadPath string,
	fullSnapshotSlot int,
	referenceSlot int,
	accountsDbDir string,
	rpcEndpoints []string,
	blockDir string,
	overcastEndpoint string,
	snapCfg snapshotdl.SnapshotConfig,
	dp *progress.DualProgress,
) (*accountsdb.AccountsDb, *SnapshotManifest, error) {
	// Clean any leftover artifacts from previous incomplete runs (e.g., Ctrl+C)
	CleanAccountsDbDir(accountsDbDir)

	manifest, err := UnmarshalManifestFromSnapshot(fullSnapshotFile, accountsDbDir)
	if err != nil {
		return nil, nil, fmt.Errorf("reading snapshot manifest: %v", err)
	}
	mlog.Log.Infof("parsed manifest from snapshotFile=%s", fullSnapshotFile)

	start := time.Now()

	appendVecsOutputDir := filepath.Join(accountsDbDir, "accounts")
	if err = os.MkdirAll(appendVecsOutputDir, 0775); err != nil {
		return nil, nil, err
	}

	defer ants.Release()

	var incrementalManifest *SnapshotManifest
	var largestFileId atomic.Uint64
	wg := &sync.WaitGroup{}

	numShards := 256
	dbFn := filepath.Join(accountsDbDir, "mithril_db")

	indexDb, err := fastcache.NewCache(fastcache.GB*256, &fastcache.Config{
		Shards:     uint32(numShards),
		MemoryType: fastcache.MMAP,
		MemoryKey:  dbFn,
	})
	if err != nil {
		panic(err)
	}

	ss := NewShardedSetter(indexDb, numShards, 100)
	logsDir := filepath.Join(accountsDbDir, "mithril_db_log_shards")
	if err = os.MkdirAll(logsDir, 0775); err != nil {
		return nil, nil, err
	}
	sl := NewShardLogger(numShards, logsDir, ss)

	indexEntryCommiterPool, _ := ants.NewPoolWithFunc(maxIndexEntryCommitter, func(i interface{}) {
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

	indexEntryBuilderPool, _ := ants.NewPoolWithFunc(maxIndexEntryBuilder, func(i interface{}) {
		tasks := indexEntryBuilderInProgress.Add(1)
		statsd.Gauge(statsd.SnapshotWorkerPoolUtilization, float64(tasks)/float64(maxIndexEntryBuilder), []string{"index_entry_builder"})
		start := time.Now()
		defer wg.Done()
		task := i.(indexEntryBuilderTask)
		pubkeys, entries, err := accountsdb.BuildIndexEntriesFromAppendVecs(task.Data, task.FileSize, task.Slot, task.FileId)
		if err != nil {
			mlog.Log.Errorf("%s\n", err)
			return
		}

		indexEntryBuilderInProgress.Add(-1)
		commitTask := indexEntryCommitterTask{IndexEntries: entries, Pubkeys: pubkeys}
		wg.Add(1)
		statsd.Timing(statsd.TasksIndexEntryBuilderLatency, uint64(time.Since(start)), nil)
		err = indexEntryCommiterPool.Invoke(commitTask)
		if err != nil {
			mlog.Log.Errorf("error calling indexEntryCommiterPool.Invoke\n")
		}
	})

	appendVecCopyingPool, _ := ants.NewPoolWithFunc(maxAppendVecCopying, func(i interface{}) {
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
			if incrementalManifest != nil {
				for _, av := range incrementalManifest.AccountsDb.Storages[slot].AcctVecs {
					if av.Id == fileId {
						fileSize = av.FileSize
						usedIncrementalSnapshotVal = true
						break
					}
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

	// Determine save path for full snapshot if streaming from HTTP
	var fullSavePath string
	if snapCfg.MaxFullSnapshots > 0 && (strings.HasPrefix(fullSnapshotFile, "http://") || strings.HasPrefix(fullSnapshotFile, "https://")) {
		if snapshotDownloadPath != "" {
			// Extract filename from URL and create save path
			urlParts := strings.Split(fullSnapshotFile, "/")
			filename := urlParts[len(urlParts)-1]
			fullSavePath = filepath.Join(snapshotDownloadPath, filename)
			mlog.Log.Infof("Will save full snapshot to %s while streaming", fullSavePath)
		}
	}

	// Start progress display if provided
	if dp != nil {
		dp.Start()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = readTarWithProgress(ctx, wg, fullSnapshotFile, fullSavePath, appendVecCopyingPool, dp)
	}()
	wg.Wait()

	// Check if processing was interrupted (context cancelled or error)
	if err != nil {
		if dp != nil {
			dp.Interrupt()
		}
		return nil, nil, fmt.Errorf("processing full snapshot: %w", err)
	}

	// Stop progress display after full snapshot is processed
	if dp != nil {
		dp.Stop()
	}

	mlog.Log.Infof("done processing full snapshot in %s.", fmtDuration(time.Since(start)))

	// Show indexing progress for shard flush
	indexProgress := progress.NewIndexingProgress("Flush (shard logs)")
	indexProgress.Start(numShards)
	err = sl.CloseWithProgress(ctx, func(completed, total int) {
		indexProgress.Update(completed, total)
	})
	if err != nil {
		indexProgress.Interrupt()
		return nil, nil, fmt.Errorf("closing shard logger: %w", err)
	}

	sl = NewShardLogger(numShards, logsDir, ss)

	// Get incremental snapshot URL (tries same source first, then searches if needed)
	mlog.Log.Infof("finding incremental snapshot matching full slot %d...", fullSnapshotSlot)
	incrSnapshotDlStart := time.Now()
	incrementalSnapshotPath, _, incrSlot, err := snapshotdl.GetIncrementalSnapshotURL(rpcEndpoints[0], fullSnapshotFile, referenceSlot, fullSnapshotSlot, snapCfg)
	if err != nil {
		klog.Fatalf("error getting incremental snapshot URL: %s", err)
	}
	mlog.Log.Infof("found incremental snapshot URL in %s: %s", fmtDuration(time.Since(incrSnapshotDlStart)), incrementalSnapshotPath)

	var downloaderOpts blockstream.BackgroundBlockDownloaderOpts
	if overcastEndpoint != "" {
		downloaderOpts = blockstream.BackgroundBlockDownloaderOpts{
			SourceType:       blockstream.BackgroundBlockDownloaderSourceOvercast,
			OutDir:           blockDir,
			OvercastEndpoint: overcastEndpoint,
			RpcEndpoints:     rpcEndpoints,
			StartSlot:        uint64(incrSlot),
		}
	} else {
		downloaderOpts = blockstream.BackgroundBlockDownloaderOpts{
			SourceType:   blockstream.BackgroundBlockDownloaderSourceRpc,
			OutDir:       blockDir,
			RpcEndpoints: rpcEndpoints,
			StartSlot:    uint64(incrSlot),
		}
	}

	catchupDownloader := blockstream.NewBlockDownloader(downloaderOpts)
	go catchupDownloader.Start()

	// Retry loop for incremental snapshot download
	// If download fails mid-way (not context cancellation), re-discover sources and retry
	maxIncrRetries := 3
	var incrementalErr error
	for incrAttempt := 0; incrAttempt < maxIncrRetries; incrAttempt++ {
		if incrAttempt > 0 {
			// Re-discover incremental snapshot URL (sources may have changed)
			mlog.Log.Infof("Incremental download failed, re-discovering sources (attempt %d/%d)...", incrAttempt+1, maxIncrRetries)
			incrementalSnapshotPath, _, incrSlot, err = snapshotdl.GetIncrementalSnapshotURL(rpcEndpoints[0], fullSnapshotFile, referenceSlot, fullSnapshotSlot, snapCfg)
			if err != nil {
				mlog.Log.Errorf("Failed to re-discover incremental snapshot: %v", err)
				continue
			}
			mlog.Log.Infof("Found new incremental snapshot URL: %s (slot %d)", incrementalSnapshotPath, incrSlot)
		}

		incrSnapshotStart := time.Now()
		incrementalManifest, err = UnmarshalManifestFromSnapshot(incrementalSnapshotPath, accountsDbDir)
		if err != nil {
			mlog.Log.Errorf("reading incremental snapshot manifest: %v", err)
			incrementalErr = fmt.Errorf("reading incremental snapshot manifest: %v", err)
			continue
		}
		mlog.Log.Infof("parsed manifest from incrementalFile=%s", incrementalSnapshotPath)

		// Determine save path for incremental snapshot if streaming from HTTP
		var incrSavePath string
		if snapCfg.MaxFullSnapshots > 0 && (strings.HasPrefix(incrementalSnapshotPath, "http://") || strings.HasPrefix(incrementalSnapshotPath, "https://")) {
			if snapshotDownloadPath != "" {
				// Extract filename from URL and create save path
				urlParts := strings.Split(incrementalSnapshotPath, "/")
				filename := urlParts[len(urlParts)-1]
				incrSavePath = filepath.Join(snapshotDownloadPath, filename)
				mlog.Log.Infof("Will save incremental snapshot to %s while streaming", incrSavePath)
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			incrementalErr = readTarIncrWithSave(ctx, wg, incrementalSnapshotPath, incrSavePath, appendVecCopyingPool)
			mlog.Log.Infof("finished reading %s in %s", incrementalSnapshotPath, fmtDuration(time.Since(start)))
		}()
		wg.Wait()
		mlog.Log.Infof("done processing incremental snapshot in %s.", fmtDuration(time.Since(incrSnapshotStart)))

		// Check if we should retry
		if incrementalErr == nil {
			break // Success
		}
		if ctx.Err() != nil {
			// Context cancelled - don't retry, just exit
			return nil, nil, ctx.Err()
		}
		// Download failed mid-way, will retry with re-discovery
		mlog.Log.Errorf("Incremental download failed: %v", incrementalErr)
	}

	if err != nil {
		return nil, nil, err
	}
	if incrementalErr != nil {
		return nil, nil, fmt.Errorf("incremental snapshot download failed after %d attempts: %w", maxIncrRetries, incrementalErr)
	}

	// Show indexing progress for incremental shard flush
	incrIndexProgress := progress.NewIndexingProgress("Flush (incr shards)")
	incrIndexProgress.Start(numShards)
	sl.CloseWithProgress(ctx, func(completed, total int) {
		incrIndexProgress.Update(completed, total)
	})
	mlog.Log.Infof("Stopping shard setter.")
	ss.Stop()

	mlog.Log.Infof("snapshots processed in %s.\n", fmtDuration(time.Since(start)))

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

	indexEntryCommiterPool.Release()
	indexEntryBuilderPool.Release()
	appendVecCopyingPool.Release()

	accountsDb := &accountsdb.AccountsDb{Index: indexDb, AcctsDir: appendVecsOutputDir}
	accountsDb.LargestFileId.Store(largestFileId.Load())
	copy(accountsDb.BankHashBytes[:], manifest.Bank.Hash[:])

	bankHashDbFn := fmt.Sprintf("%s/bankhash_db", accountsDbDir)
	accountsDb.BankHashStore, err = fastcache.NewCache(fastcache.MB*128, &fastcache.Config{
		Shards: 256,
		//MaxElementLen: 2000000000,
		MemoryType: fastcache.MMAP,
		MemoryKey:  bankHashDbFn,
	})
	if err != nil {
		panic(err)
	}

	rpcClient := rpcclient.NewRpcClient(rpcEndpoints[0])
	latestSlot, _ := rpcClient.GetSlot()
	_, incrSlot = snapshotdl.ExtractIncrementalSnapshotSlots(incrementalSnapshotPath)

	mlog.Log.Infof("node currently at slot %d, whereas chain is at slot %d. currently %d slots behind.", incrSlot, latestSlot, latestSlot-uint64(incrSlot))

	return accountsDb, incrementalManifest, nil
}

func readTarIncr(ctx context.Context, wg *sync.WaitGroup, filename string, appendVecCopyingPool *ants.PoolWithFunc) error {
	return readTarIncrWithSave(ctx, wg, filename, "", appendVecCopyingPool)
}

func readTarIncrWithSave(ctx context.Context, wg *sync.WaitGroup, filename string, savePath string, appendVecCopyingPool *ants.PoolWithFunc) error {
	tarReader, closer, err := newSnapshotReaderWithSave(filename, savePath)
	if err != nil {
		return err
	}
	defer closer.Close()

	// cleanupPartial deletes the partial download file if it exists
	cleanupPartial := func(reason string) {
		if savePath != "" {
			if _, statErr := os.Stat(savePath); statErr == nil {
				mlog.Log.Infof("Deleting partial incremental download %s (%s)", savePath, reason)
				if rmErr := os.Remove(savePath); rmErr != nil {
					mlog.Log.Errorf("Failed to delete partial incremental download %s: %v", savePath, rmErr)
				}
			}
		}
	}

	for {
		if ctx.Err() != nil {
			mlog.Log.Infof("context cancelled, stopping snapshot unpack: %v", ctx.Err())
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

		task := appendVecCopyingTask{TarBuffer: writer, Filename: header.Name, FromIncrementalSnapshot: true}
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
