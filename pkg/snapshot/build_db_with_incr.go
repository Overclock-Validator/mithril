package snapshot

import (
	"context"
	"encoding/binary"
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
	"github.com/cockroachdb/pebble"
	"github.com/panjf2000/ants/v2"
	"k8s.io/klog/v2"
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

// BuildAccountsDbWithIncr builds the accounts database from full + incremental snapshots.
// If sourceSelector is provided, it will be used to find incremental sources from cached
// Stage 2 results (much faster than full cluster search). Pass nil to use legacy behavior.
func BuildAccountsDbWithIncr(
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
	sourceSelector *snapshotdl.SourceSelector,
) (*accountsdb.AccountsDb, *SnapshotManifest, error) {
	// Clean any leftover artifacts from previous incomplete runs (e.g., Ctrl+C)
	CleanAccountsDbDir(accountsDbDir)

	manifest, err := UnmarshalManifestFromSnapshot(ctx, fullSnapshotFile, accountsDbDir)
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

	incrementalManifest := &SnapshotManifest{}
	var largestFileId atomic.Uint64
	wg := &sync.WaitGroup{}

	numShards := 256
	logsDir := filepath.Join(accountsDbDir, "mithril_db_log_shards")
	if err = os.MkdirAll(logsDir, 0775); err != nil {
		return nil, nil, err
	}
	sl := NewShardLogger(numShards, logsDir)

	pools, err := initWorkerPools(wg, sl, manifest, incrementalManifest, accountsDbDir, &largestFileId)
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
		dp.Start()
	}

	err = readTar(ctx, wg, fullSnapshotFile, pools.appendVecCopying, readTarOptions{savePath: fullSavePath, progress: dp})
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

	// Get incremental snapshot - try cached sources first if available, then fall back to cluster search
	mlog.Log.Infof("finding incremental snapshot matching full slot %d...", fullSnapshotSlot)
	incrSnapshotDlStart := time.Now()

	// Try to get incremental from cached Stage 2 sources (much faster)
	var incrSelector *snapshotdl.IncrementalSelector
	if sourceSelector != nil {
		incrSelector = sourceSelector.GetIncrementalSelector(ctx, fullSnapshotSlot, snapCfg.Verbose)
	}

	var incrementalSnapshotPath string
	var incrSlot int
	var incrSwitchRequested atomic.Bool

	if incrSelector != nil && incrSelector.TotalSources() > 0 {
		// Use cached incremental sources with switching support
		mlog.Log.Infof("using cached Stage 2 sources for incremental (found %d sources)", incrSelector.TotalSources())
		defer incrSelector.Close()

		for {
			if ctx.Err() != nil {
				return nil, nil, fmt.Errorf("downloading incremental snapshot: %w", ctx.Err())
			}

			currentIncr := incrSelector.Current()
			if currentIncr == nil {
				// Exhausted cached sources, fall back to full search
				mlog.Log.Infof("exhausted %d cached incremental sources, falling back to cluster search...", incrSelector.TotalSources())
				incrSelector = nil
				break
			}

			incrementalSnapshotPath = currentIncr.URL
			incrSlot = currentIncr.EndSlot

			// Create cancellable context for this download attempt
			incrCtx, cancelIncr := context.WithCancel(ctx)

			// Show source info with switching hint
			threshNote := ""
			if !currentIncr.WithinThresh {
				threshNote = " (outside threshold)"
			}
			mlog.Log.Infof("📸 Incremental source %d/%d: %s (slot %d, %d slots behind tip%s, %.1f MB/s)",
				incrSelector.CurrentIndex()+1,
				incrSelector.TotalSources(),
				currentIncr.NodeIP,
				currentIncr.EndSlot,
				currentIncr.Age(),
				threshNote,
				currentIncr.SpeedMBs,
			)

			// Enable 'n' key switching for incremental if we have progress display
			incrSwitchRequested.Store(false)
			if dp != nil && incrSelector.HasMore() {
				sourceInfo := fmt.Sprintf("Incr Source %d/%d: %s (%d slots behind)",
					incrSelector.CurrentIndex()+1,
					incrSelector.TotalSources(),
					currentIncr.NodeIP,
					currentIncr.Age(),
				)
				dp.EnableSourceSwitching(sourceInfo, func() {
					incrSwitchRequested.Store(true)
					cancelIncr()
					mlog.Log.Infof("User requested incremental source switch...")
				})
			}

			// Try to download from this incremental source
			incrSnapshotStart := time.Now()
			incrementalManifestCopy, manifestErr := UnmarshalManifestFromSnapshot(incrCtx, incrementalSnapshotPath, accountsDbDir)

			if manifestErr == nil {
				*incrementalManifest = *incrementalManifestCopy
				mlog.Log.Infof("parsed manifest from incrementalFile=%s", incrementalSnapshotPath)

				// Determine save path
				var incrSavePath string
				if snapCfg.MaxFullSnapshots > 0 && (strings.HasPrefix(incrementalSnapshotPath, "http://") || strings.HasPrefix(incrementalSnapshotPath, "https://")) {
					if snapshotDownloadPath != "" {
						if err := os.MkdirAll(snapshotDownloadPath, 0o755); err != nil {
							cancelIncr()
							if dp != nil {
								dp.DisableSourceSwitching()
							}
							return nil, nil, fmt.Errorf("failed to create snapshot download directory %s: %w", snapshotDownloadPath, err)
						}
						urlParts := strings.Split(incrementalSnapshotPath, "/")
						filename := urlParts[len(urlParts)-1]
						incrSavePath = filepath.Join(snapshotDownloadPath, filename)
						mlog.Log.Infof("Will save incremental snapshot to %s while streaming", incrSavePath)
					}
				}

				err = readTar(incrCtx, wg, incrementalSnapshotPath, pools.appendVecCopying, readTarOptions{savePath: incrSavePath, isIncremental: true})
				wg.Wait()

				if dp != nil {
					dp.DisableSourceSwitching()
				}
				cancelIncr()

				if err == nil {
					mlog.Log.Infof("finished reading %s in %s", incrementalSnapshotPath, fmtDuration(time.Since(start)))
					mlog.Log.Infof("done processing incremental snapshot in %s.", fmtDuration(time.Since(incrSnapshotStart)))
					break // Success!
				}
			} else {
				err = manifestErr
				if dp != nil {
					dp.DisableSourceSwitching()
				}
				cancelIncr()
			}

			// Handle switch request or error
			if incrSwitchRequested.Load() || (incrCtx.Err() != nil && ctx.Err() == nil) {
				nextIncr := incrSelector.Next()
				if nextIncr == nil {
					mlog.Log.Infof("No more cached incremental sources, falling back to cluster search...")
					incrSelector = nil
					break
				}
				mlog.Log.Infof("Switching to incremental source %d/%d: %s (%d slots behind)",
					incrSelector.CurrentIndex()+1,
					incrSelector.TotalSources(),
					nextIncr.NodeIP,
					nextIncr.Age(),
				)
				continue
			}

			// Check parent context
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}

			// Error - try next source
			mlog.Log.Errorf("Incremental download from %s failed: %v", currentIncr.NodeIP, err)
			nextIncr := incrSelector.Next()
			if nextIncr == nil {
				mlog.Log.Infof("No more cached incremental sources, falling back to cluster search...")
				incrSelector = nil
				break
			}
		}
	}

	// Fall back to full cluster search if needed (no cached sources or all exhausted)
	if incrSelector == nil {
		incrementalSnapshotPath, _, incrSlot, err = snapshotdl.GetIncrementalSnapshotURL(fullSnapshotFile, referenceSlot, fullSnapshotSlot, snapCfg)
		if err != nil {
			klog.Fatalf("error getting incremental snapshot URL: %s", err)
		}
		mlog.Log.Infof("found incremental snapshot URL in %s: %s", fmtDuration(time.Since(incrSnapshotDlStart)), incrementalSnapshotPath)

		// Retry loop for incremental snapshot download (legacy behavior)
		maxIncrRetries := 3
		for incrAttempt := range maxIncrRetries {
			if ctx.Err() != nil {
				return nil, nil, fmt.Errorf("attempting to download incremental snapshot: %w", ctx.Err())
			}
			if incrAttempt > 0 {
				mlog.Log.Infof("Incremental download failed, re-discovering sources (attempt %d/%d)...", incrAttempt+1, maxIncrRetries)
				incrementalSnapshotPath, _, incrSlot, err = snapshotdl.GetIncrementalSnapshotURL(fullSnapshotFile, referenceSlot, fullSnapshotSlot, snapCfg)
				if err != nil {
					mlog.Log.Errorf("Failed to re-discover incremental snapshot: %v", err)
					continue
				}
				mlog.Log.Infof("Found new incremental snapshot URL: %s (slot %d)", incrementalSnapshotPath, incrSlot)
			}

			incrSnapshotStart := time.Now()
			incrementalManifestCopy, err := UnmarshalManifestFromSnapshot(ctx, incrementalSnapshotPath, accountsDbDir)
			if err != nil {
				mlog.Log.Errorf("reading incremental snapshot manifest: %v", err)
				continue
			}
			*incrementalManifest = *incrementalManifestCopy
			mlog.Log.Infof("parsed manifest from incrementalFile=%s", incrementalSnapshotPath)

			var incrSavePath string
			if snapCfg.MaxFullSnapshots > 0 && (strings.HasPrefix(incrementalSnapshotPath, "http://") || strings.HasPrefix(incrementalSnapshotPath, "https://")) {
				if snapshotDownloadPath != "" {
					if err := os.MkdirAll(snapshotDownloadPath, 0o755); err != nil {
						return nil, nil, fmt.Errorf("failed to create snapshot download directory %s: %w", snapshotDownloadPath, err)
					}
					urlParts := strings.Split(incrementalSnapshotPath, "/")
					filename := urlParts[len(urlParts)-1]
					incrSavePath = filepath.Join(snapshotDownloadPath, filename)
					mlog.Log.Infof("Will save incremental snapshot to %s while streaming", incrSavePath)
				}
			}

			err = readTar(ctx, wg, incrementalSnapshotPath, pools.appendVecCopying, readTarOptions{savePath: incrSavePath, isIncremental: true})
			wg.Wait()
			mlog.Log.Infof("finished reading %s in %s", incrementalSnapshotPath, fmtDuration(time.Since(start)))
			mlog.Log.Infof("done processing incremental snapshot in %s.", fmtDuration(time.Since(incrSnapshotStart)))
			if err == nil {
				break
			}
			mlog.Log.Errorf("Incremental download failed: %v", err)
		}
		if err != nil {
			return nil, nil, err
		}
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

	mlog.Log.Infof("snapshots processed in %s.", fmtDuration(time.Since(start)))

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

	accountsDb := &accountsdb.AccountsDb{Index: index, AcctsDir: appendVecsOutputDir}
	accountsDb.LargestFileId.Store(largestFileId.Load())

	bankhashDir := filepath.Join(accountsDbDir, "bankhash_db")
	bankhashDb, err := pebble.Open(bankhashDir, &pebble.Options{})
	if err != nil {
		return nil, nil, fmt.Errorf("opening bankhashDir=%s: %w", bankhashDir, err)
	}
	accountsDb.BankHashStore = bankhashDb

	rpcClient := rpcclient.NewRpcClient(rpcEndpoints[0])
	latestSlot, err := rpcClient.GetSlot()
	_, incrSlot = snapshotdl.ExtractIncrementalSnapshotSlots(incrementalSnapshotPath)

	if err != nil || latestSlot == 0 {
		mlog.Log.Infof("node currently at slot %d (unable to fetch chain tip)", incrSlot)
	} else if latestSlot > uint64(incrSlot) {
		mlog.Log.Infof("node currently at slot %d, chain tip at slot %d (%d slots behind)", incrSlot, latestSlot, latestSlot-uint64(incrSlot))
	} else {
		mlog.Log.Infof("node currently at slot %d, chain tip at slot %d", incrSlot, latestSlot)
	}

	return accountsDb, incrementalManifest, nil
}
