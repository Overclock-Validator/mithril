package snapshot

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/snapshotdl"
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
	accountsDbDir string,
	rpcEndpoints []string,
	blockDir string,
	snapCfg snapshotdl.SnapshotConfig,
	dp *progress.DualProgress,
) (_ *accountsdb.AccountsDb, _ *SnapshotManifest, retErr error) {
	// Clean any leftover artifacts from previous incomplete runs (e.g., Ctrl+C)
	storeGuard, err := beginSnapshotBootstrap(accountsDbDir)
	if err != nil {
		return nil, nil, fmt.Errorf("clean previous AccountsDB: %w", err)
	}
	storeGuardTransferred := false
	defer func() {
		// The root remains private only while this guard is live. Remove every
		// incomplete/unverified bootstrap artifact before unlocking on failure,
		// rather than exposing a structurally valid but untrusted V2 selector.
		if retErr != nil && !storeGuardTransferred {
			retErr = errors.Join(retErr, cleanAccountsDbDirLocked(accountsDbDir))
		}
		retErr = errors.Join(retErr, storeGuard.Close())
	}()

	mlog.Log.Infof("Parsing full snapshot manifest...")
	manifest, err := UnmarshalManifestFromSnapshot(ctx, fullSnapshotFile, accountsDbDir)
	if err != nil {
		return nil, nil, fmt.Errorf("reading snapshot manifest: %w", err)
	}
	mlog.Log.Infof("Parsed full snapshot manifest")
	if err := validateSnapshotManifestPair(manifest, nil); err != nil {
		return nil, nil, fmt.Errorf("validate full snapshot role: %w", err)
	}
	if err := validateSnapshotArchiveManifestSlots(fullSnapshotFile, manifest, "", nil); err != nil {
		return nil, nil, fmt.Errorf("validate full snapshot archive identity: %w", err)
	}
	if fullSnapshotSlot < 0 || manifest.Bank.Slot != uint64(fullSnapshotSlot) {
		return nil, nil, fmt.Errorf(
			"selected full snapshot slot %d does not match manifest bank slot %d",
			fullSnapshotSlot, manifest.Bank.Slot,
		)
	}
	if OnFullSnapshotManifestParsed != nil {
		OnFullSnapshotManifestParsed(manifest)
	}

	start := time.Now()

	appendVecsOutputDir := filepath.Join(accountsDbDir, "accounts")
	if err = os.MkdirAll(appendVecsOutputDir, 0775); err != nil {
		return nil, nil, err
	}
	logSnapshotBootstrapTuning()

	defer ants.Release()

	incrementalManifest := &SnapshotManifest{}
	var largestFileId atomic.Uint64
	wg := &sync.WaitGroup{}

	numShards := snapshotIndexShards()
	logsDir, cleanupIndexWorkDir, err := prepareSnapshotIndexWorkDir(accountsDbDir)
	if err != nil {
		return nil, nil, err
	}
	defer cleanupIndexWorkDir()
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

	// Determine save path for full snapshot if streaming from HTTP
	var fullSavePath string
	if snapCfg.MaxFullSnapshots > 0 && (strings.HasPrefix(fullSnapshotFile, "http://") || strings.HasPrefix(fullSnapshotFile, "https://")) {
		if snapshotDownloadPath != "" {
			// Ensure snapshot download directory exists
			if err := os.MkdirAll(snapshotDownloadPath, 0o755); err != nil {
				return nil, nil, fmt.Errorf("failed to create snapshot download directory %s: %w", snapshotDownloadPath, err)
			}
			// Parse only the URL path basename: query credentials must never
			// become part of a local filename.
			filename, identityErr := snapshotArchiveIdentity(fullSnapshotFile)
			if identityErr != nil {
				return nil, nil, fmt.Errorf("identify full snapshot cache filename: %w", identityErr)
			}
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

	err = readTar(ctx, wg, fullSnapshotFile, pools, readTarOptions{
		savePath:        fullSavePath,
		progress:        dp,
		statusCachePath: retainedStatusCachePath(accountsDbDir),
	})
	// A successful archive read is not a successful build until all three
	// worker stages have drained without error.
	workerErr := waitForSnapshotWorkers(wg, pools)
	if err == nil {
		err = workerErr
	}
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

	// Log full snapshot processing time to debug log only (noise reduction)
	mlog.Log.Debugf("done processing full snapshot in %s.", fmtDuration(time.Since(start)))

	// Note: ShardLogger is NOT closed here - we use one logger for both phases
	// and flush once at the end. This avoids the bug where pools still reference
	// the old ShardLogger after reinit.

	// Refresh the freshness reference to the CURRENT chain tip before picking
	// an incremental: the tip advanced during the full download/build, and the
	// callers' initial reference may even be the full slot itself — against
	// which any incremental looks fresh and the staleness gate never fires
	// (the bug that bootstrapped 81k slots behind on the Alpenglow cluster).
	if fresh, rerr := snapshotdl.GetReferenceSlot(snapCfg); rerr == nil && fresh > referenceSlot {
		if referenceSlot > 0 && fresh > referenceSlot+1000 {
			mlog.Log.Infof("refreshed incremental freshness reference: %d -> %d (tip advanced during full snapshot build)", referenceSlot, fresh)
		}
		referenceSlot = fresh
	} else if rerr != nil {
		mlog.Log.Warnf("could not refresh reference slot for incremental freshness gating: %v (using %d)", rerr, referenceSlot)
	}

	// Get incremental snapshot URL (tries same source first, then searches if needed)
	mlog.Log.Infof("finding incremental snapshot matching full slot %d...", fullSnapshotSlot)
	incrSnapshotDlStart := time.Now()
	incrementalSnapshotPath, _, incrSlot, err := snapshotdl.GetIncrementalSnapshotURL(fullSnapshotFile, referenceSlot, fullSnapshotSlot, snapCfg)
	if err != nil {
		// Return error instead of fatal exit so caller can handle gracefully
		errMsg := fmt.Sprintf("failed to find incremental snapshot: %v", err)
		if strings.Contains(err.Error(), "threshold") {
			errMsg += fmt.Sprintf("\n  Hint: every available incremental is staler than snapshot.incremental_threshold (current: %d slots)."+
				"\n  Either raise the threshold to accept a staler incremental — and pair it with a raised"+
				"\n  block.repair_catchup_max_gap_slots so turbine repair fills the remaining gap after bootstrap —"+
				"\n  or wait for the cluster to publish a fresher incremental.", snapCfg.IncrementalThreshold)
		} else if strings.Contains(err.Error(), "no rpc nodes") || strings.Contains(err.Error(), "no nodes found") {
			errMsg += "\n  Hint: Check RPC endpoints connectivity or try again later"
		}
		return nil, nil, fmt.Errorf("%s", errMsg)
	}
	mlog.Log.Debugf("found incremental snapshot URL in %s: %s", fmtDuration(time.Since(incrSnapshotDlStart)), incrementalSnapshotPath)
	pinnedIncrementalIdentity, err := snapshotArchiveIdentity(incrementalSnapshotPath)
	if err != nil {
		return nil, nil, fmt.Errorf("identify incremental snapshot: %w", err)
	}
	pinnedIncrementalSlot := incrSlot
	var pinnedIncrementalManifestHash [sha256.Size]byte
	havePinnedIncrementalManifestHash := false

	// Retry loop for incremental snapshot download
	// If download fails mid-way (not context cancellation), re-discover mirrors
	// of the exact same archive and retry. Switching to a different archive here
	// would mix already-published appendvecs and index-log entries from two
	// independently valid snapshots.
	maxIncrRetries := 3
	for incrAttempt := range maxIncrRetries {
		if ctx.Err() != nil {
			return nil, nil, fmt.Errorf("attempting to download incremental snapshot: %w", ctx.Err())
		}
		if incrAttempt > 0 {
			// Re-discover incremental snapshot URL (sources may have changed)
			mlog.Log.Infof("Incremental download failed, re-discovering sources (attempt %d/%d)...", incrAttempt+1, maxIncrRetries)
			candidatePath, _, candidateSlot, rediscoverErr := snapshotdl.GetIncrementalSnapshotURL(fullSnapshotFile, referenceSlot, fullSnapshotSlot, snapCfg)
			err = rediscoverErr
			if err != nil {
				mlog.Log.Errorf("Failed to re-discover incremental snapshot: %v", err)
				continue
			}
			candidateIdentity, identityErr := snapshotArchiveIdentity(candidatePath)
			if identityErr != nil {
				return nil, nil, fmt.Errorf("identify re-discovered incremental snapshot: %w", identityErr)
			}
			if candidateSlot != pinnedIncrementalSlot || candidateIdentity != pinnedIncrementalIdentity {
				return nil, nil, fmt.Errorf(
					"incremental retry would mix snapshot %q at slot %d with %q at slot %d",
					pinnedIncrementalIdentity, pinnedIncrementalSlot, candidateIdentity, candidateSlot,
				)
			}
			incrementalSnapshotPath = candidatePath
			incrSlot = candidateSlot
			mlog.Log.Infof("Found new incremental snapshot URL: %s (slot %d)", incrementalSnapshotPath, incrSlot)
		}

		mlog.Log.FileOnlyf("Parsing incremental snapshot manifest...")
		incrementalManifestCopy, err := UnmarshalManifestFromSnapshot(ctx, incrementalSnapshotPath, accountsDbDir)
		if err != nil {
			mlog.Log.Errorf("reading incremental snapshot manifest: %v", err)
			continue
		}
		manifestBytes, manifestReadErr := readManifestFile(filepath.Join(accountsDbDir, "manifest"))
		if manifestReadErr != nil {
			return nil, nil, fmt.Errorf("read published incremental manifest for retry validation: %w", manifestReadErr)
		}
		manifestHash := sha256.Sum256(manifestBytes)
		if havePinnedIncrementalManifestHash && manifestHash != pinnedIncrementalManifestHash {
			return nil, nil, fmt.Errorf(
				"incremental snapshot %q returned different manifest bytes across retries",
				pinnedIncrementalIdentity,
			)
		}
		pinnedIncrementalManifestHash = manifestHash
		havePinnedIncrementalManifestHash = true
		if err := validateSnapshotManifestPair(manifest, incrementalManifestCopy); err != nil {
			return nil, nil, fmt.Errorf("validate full/incremental snapshot pairing: %w", err)
		}
		if err := validateSnapshotArchiveManifestSlots(
			fullSnapshotFile, manifest, incrementalSnapshotPath, incrementalManifestCopy,
		); err != nil {
			return nil, nil, fmt.Errorf("validate incremental snapshot archive identity: %w", err)
		}
		if incrSlot < 0 || incrementalManifestCopy.Bank.Slot != uint64(incrSlot) {
			return nil, nil, fmt.Errorf(
				"selected incremental snapshot slot %d does not match manifest bank slot %d",
				incrSlot, incrementalManifestCopy.Bank.Slot,
			)
		}
		// Copy the manifest so the worker pool's pointer has the value.
		*incrementalManifest = *incrementalManifestCopy
		mlog.Log.FileOnlyf("Parsed incremental snapshot manifest")
		if OnIncrementalManifestParsed != nil {
			OnIncrementalManifestParsed(incrementalManifestCopy)
		}

		// Determine save path for incremental snapshot if streaming from HTTP
		var incrSavePath string
		if snapCfg.MaxFullSnapshots > 0 && (strings.HasPrefix(incrementalSnapshotPath, "http://") || strings.HasPrefix(incrementalSnapshotPath, "https://")) {
			if snapshotDownloadPath != "" {
				// Ensure snapshot download directory exists (may not exist if full was local)
				if err := os.MkdirAll(snapshotDownloadPath, 0o755); err != nil {
					return nil, nil, fmt.Errorf("failed to create snapshot download directory %s: %w", snapshotDownloadPath, err)
				}
				// Extract filename from URL and create save path
				incrSavePath = filepath.Join(snapshotDownloadPath, pinnedIncrementalIdentity)
				mlog.Log.FileOnlyf("Will save incremental snapshot to %s while streaming", incrSavePath)
			}
		}

		err = readTar(ctx, wg, incrementalSnapshotPath, pools, readTarOptions{
			savePath:        incrSavePath,
			isIncremental:   true,
			statusCachePath: retainedStatusCachePath(accountsDbDir),
		})
		workerErr = waitForSnapshotWorkers(wg, pools)
		if workerErr != nil {
			return nil, nil, fmt.Errorf("processing incremental snapshot workers: %w", workerErr)
		}
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
	if err := syncDirectory(appendVecsOutputDir); err != nil {
		return nil, nil, fmt.Errorf("persist snapshot appendvec publications: %w", err)
	}

	// Show indexing progress for shard flush
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
	preparedIndex, err := verifyBuiltSnapshotAccountsState(
		ctx, accountsDbDir, manifest, incrementalManifest, incrementalManifest, storeGuard,
	)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		retErr = errors.Join(retErr, preparedIndex.Close())
	}()

	if err := finalizeSnapshotBootstrapArtifacts(
		accountsDbDir,
		largestFileId.Load(),
		incrementalManifest.Bank.Hash,
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

	var latestSlot uint64
	if len(rpcEndpoints) != 0 {
		rpcClient := rpcclient.NewRpcClient(rpcEndpoints[0])
		latestSlot, err = rpcClient.GetSlot()
	}

	if len(rpcEndpoints) == 0 || err != nil || latestSlot == 0 {
		mlog.Log.Infof("Node currently at slot %d (unable to fetch chain tip)", incrSlot)
	} else if latestSlot > uint64(incrSlot) {
		mlog.Log.Infof("Node currently at slot %d, chain tip at slot %d (%d slots behind)", incrSlot, latestSlot, latestSlot-uint64(incrSlot))
	} else {
		mlog.Log.Infof("Node currently at slot %d, chain tip at slot %d", incrSlot, latestSlot)
	}

	return accountsDb, incrementalManifest, nil
}
