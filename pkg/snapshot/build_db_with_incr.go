package snapshot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/snapshotdl"
	"github.com/cockroachdb/pebble"
	"github.com/panjf2000/ants/v2"
)

// fmtDuration formats a duration to 3 decimal places in the most appropriate unit
func fmtDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%.3fms", float64(d.Microseconds())/1000)
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60
	return fmt.Sprintf("%dm%02ds", minutes, seconds)
}

// BuildAccountsDbAuto builds the accounts database from full + incremental snapshots.
func BuildAccountsDbAuto(
	ctx context.Context,
	fullSnapshotFile string,
	snapshotDownloadPath string,
	fullSnapshotSlot int,
	referenceSlot int,
	accountsPaths []string,
	rpcEndpoints []string,
	blockDir string,
	snapCfg snapshotdl.SnapshotConfig,
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
	manifest, err := UnmarshalManifestFromSnapshot(ctx, fullSnapshotFile, accountsDbDir)
	if err != nil {
		return nil, nil, fmt.Errorf("reading snapshot manifest: %v", err)
	}
	mlog.Log.Infof("Parsed full snapshot manifest")

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

	incrementalManifest := &SnapshotManifest{}
	wg := &sync.WaitGroup{}

	numShards := snapshotIndexShards()
	logsDir, cleanupIndexWorkDir, err := prepareSnapshotIndexWorkDir(accountsDbDir)
	if err != nil {
		return nil, nil, err
	}
	defer cleanupIndexWorkDir()
	sl := NewShardLogger(numShards, logsDir)

	// Create stake pubkey collector for building stake index during appendvec processing
	stakeCollector := &stakeIndexCollector{
		entries: make([]accountsdb.StakeIndexEntry, 0, 1000000), // Pre-allocate for ~1M stake accounts
	}

	pools, err := initWorkerPools(wg, sl, manifest, incrementalManifest, shardFiles, stakeCollector)
	if err != nil {
		return nil, nil, fmt.Errorf("initializing worker pools: %w", err)
	}

	// Determine save path for full snapshot if streaming from HTTP
	var fullSavePath string
	if snapCfg.MaxFullSnapshots > 0 && (strings.HasPrefix(fullSnapshotFile, "http://") || strings.HasPrefix(fullSnapshotFile, "https://")) {
		if snapshotDownloadPath != "" {
			// Ensure snapshot download directory exists
			if err := os.MkdirAll(snapshotDownloadPath, 0o755); err != nil {
				return nil, nil, fmt.Errorf("failed to create snapshot download directory %s: %w", snapshotDownloadPath, err)
			}
			// Extract filename from URL and create save path
			urlParts := strings.Split(fullSnapshotFile, "/")
			filename := urlParts[len(urlParts)-1]
			fullSavePath = filepath.Join(snapshotDownloadPath, filename)
			mlog.Log.Infof("Will save full snapshot to %s while streaming", fullSavePath)
		}
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

	err = readTar(ctx, wg, fullSnapshotFile, pools.appendVecCopying, pools.tarBufs, readTarOptions{savePath: fullSavePath, progress: dp})
	if err != nil {
		if dp != nil {
			dp.Interrupt(err)
		}
		return nil, nil, fmt.Errorf("processing full snapshot: %w", err)
	}

	// Stop progress display after full snapshot is processed
	if dp != nil {
		dp.Stop()
	}

	// Wait for all workers to finish before continuing to incremental phase
	wg.Wait()

	// Log full snapshot processing time to debug log only (noise reduction)
	mlog.Log.Debugf("done processing full snapshot in %s.", fmtDuration(time.Since(start)))

	// Note: ShardLogger is NOT closed here - we use one logger for both phases
	// and flush once at the end. This avoids the bug where pools still reference
	// the old ShardLogger after reinit.

	// Get incremental snapshot URL (tries same source first, then searches if needed)
	mlog.Log.Infof("finding incremental snapshot matching full slot %d...", fullSnapshotSlot)
	incrSnapshotDlStart := time.Now()
	incrementalSnapshotPath, _, incrSlot, err := snapshotdl.GetIncrementalSnapshotURL(fullSnapshotFile, referenceSlot, fullSnapshotSlot, snapCfg)
	if err != nil {
		// Return error instead of fatal exit so caller can handle gracefully
		errMsg := fmt.Sprintf("failed to find incremental snapshot: %v", err)
		if strings.Contains(err.Error(), "threshold") {
			errMsg += fmt.Sprintf("\n  Hint: Try increasing incremental_threshold in config (current: %d slots)", snapCfg.IncrementalThreshold)
		} else if strings.Contains(err.Error(), "no rpc nodes") || strings.Contains(err.Error(), "no nodes found") {
			errMsg += "\n  Hint: Check RPC endpoints connectivity or try again later"
		}
		return nil, nil, fmt.Errorf("%s", errMsg)
	}
	mlog.Log.Debugf("found incremental snapshot URL in %s: %s", fmtDuration(time.Since(incrSnapshotDlStart)), incrementalSnapshotPath)

	// Retry loop for incremental snapshot download
	// If download fails mid-way (not context cancellation), re-discover sources and retry
	maxIncrRetries := 3
	for incrAttempt := range maxIncrRetries {
		if ctx.Err() != nil {
			return nil, nil, fmt.Errorf("attempting to download incremental snapshot: %w", ctx.Err())
		}
		if incrAttempt > 0 {
			// Re-discover incremental snapshot URL (sources may have changed)
			mlog.Log.Infof("Incremental download failed, re-discovering sources (attempt %d/%d)...", incrAttempt+1, maxIncrRetries)
			incrementalSnapshotPath, _, incrSlot, err = snapshotdl.GetIncrementalSnapshotURL(fullSnapshotFile, referenceSlot, fullSnapshotSlot, snapCfg)
			if err != nil {
				mlog.Log.Errorf("Failed to re-discover incremental snapshot: %v", err)
				continue
			}
			mlog.Log.Infof("Found new incremental snapshot URL: %s (slot %d)", incrementalSnapshotPath, incrSlot)
		}

		mlog.Log.Infof("Parsing incremental snapshot manifest...")
		incrementalManifestCopy, err := UnmarshalManifestFromSnapshot(ctx, incrementalSnapshotPath, accountsDbDir)
		if err != nil {
			mlog.Log.Errorf("reading incremental snapshot manifest: %v", err)
			continue
		}
		// Copy the manifest so the worker pool's pointer has the value.
		*incrementalManifest = *incrementalManifestCopy
		mlog.Log.Infof("Parsed incremental snapshot manifest")

		// Determine save path for incremental snapshot if streaming from HTTP
		var incrSavePath string
		if snapCfg.MaxFullSnapshots > 0 && (strings.HasPrefix(incrementalSnapshotPath, "http://") || strings.HasPrefix(incrementalSnapshotPath, "https://")) {
			if snapshotDownloadPath != "" {
				// Ensure snapshot download directory exists (may not exist if full was local)
				if err := os.MkdirAll(snapshotDownloadPath, 0o755); err != nil {
					return nil, nil, fmt.Errorf("failed to create snapshot download directory %s: %w", snapshotDownloadPath, err)
				}
				// Extract filename from URL and create save path
				urlParts := strings.Split(incrementalSnapshotPath, "/")
				filename := urlParts[len(urlParts)-1]
				incrSavePath = filepath.Join(snapshotDownloadPath, filename)
				mlog.Log.Infof("Will save incremental snapshot to %s while streaming", incrSavePath)
			}
		}

		err = readTar(ctx, wg, incrementalSnapshotPath, pools.appendVecCopying, pools.tarBufs, readTarOptions{savePath: incrSavePath, isIncremental: true})
		wg.Wait()
		// Check if we should retry
		if err == nil {
			break // Success
		}
		// Download failed mid-way, will retry with re-discovery
		mlog.Log.Errorf("Incremental download failed: %v", err)
	}
	if err != nil {
		return nil, nil, err
	}

	// flush and close every shard's big file now that all appends are done
	if err := shardFiles.close(); err != nil {
		return nil, nil, fmt.Errorf("closing shard big files: %w", err)
	}

	// Show indexing progress for shard flush
	indexProgress := progress.NewIndexingProgress("Flush (shard logs)")
	indexProgress.Start(numShards)
	sl.CloseWithProgress(ctx, func(completed, total int) {
		indexProgress.Update(completed, total)
	})
	indexDir := filepath.Join(accountsDbDir, "mithril_db")
	index, err := ingestSSTFiles(indexDir, logsDir)
	if err != nil {
		return nil, nil, fmt.Errorf("initializing pebble from SST files: %w", err)
	}
	index.Close()

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

	rpcClient := rpcclient.NewRpcClient(rpcEndpoints[0])
	latestSlot, err := rpcClient.GetSlot()
	_, incrSlot = snapshotdl.ExtractIncrementalSnapshotSlots(incrementalSnapshotPath)

	if err != nil || latestSlot == 0 {
		mlog.Log.Infof("Node currently at slot %d (unable to fetch chain tip)", incrSlot)
	} else if latestSlot > uint64(incrSlot) {
		mlog.Log.Infof("Node currently at slot %d, chain tip at slot %d (%d slots behind)", incrSlot, latestSlot, latestSlot-uint64(incrSlot))
	} else {
		mlog.Log.Infof("Node currently at slot %d, chain tip at slot %d", incrSlot, latestSlot)
	}

	return accountsDb, incrementalManifest, nil
}
