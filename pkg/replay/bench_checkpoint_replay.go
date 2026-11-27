package replay

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

// Bench only: MITHRIL_BENCH_CHECKPOINT_REPLAY=<workers>:<shred directory>
// replays, beside the node's own replay, each fold the node has committed,
// privately from the fold boundary before it, and compares every bank hash
// with the one the fold recorded. The directory holds this node's spooled
// shred files (s<slot>.shreds) for those slots. Nothing is published.
func startBenchCheckpointReplay(ctx context.Context, db *accountsdb.AccountsDb, persisted *state.MithrilState) (wait func()) {
	spec := os.Getenv("MITHRIL_BENCH_CHECKPOINT_REPLAY")
	if spec == "" || db == nil || persisted == nil {
		return func() {}
	}
	workerText, shredDir, ok := strings.Cut(spec, ":")
	workers, err := strconv.Atoi(workerText)
	if !ok || err != nil || workers < 1 || shredDir == "" {
		mlog.Log.Infof("bench checkpoint replay: want <workers>:<shred directory>, got %q", spec)
		return func() {}
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		if path := os.Getenv("MITHRIL_BENCH_CHECKPOINT_REPLAY_PROFILE"); path != "" {
			if file, err := os.Create(path); err == nil {
				// Deferred calls run last first: stop the profile, then close.
				defer file.Close()
				if pprof.StartCPUProfile(file) == nil {
					defer pprof.StopCPUProfile()
				}
			}
		}
		var folds, blocks, mismatches, failures int
		var loadTime, replayTime time.Duration
		done := make(map[uint64]bool)
		defer func() {
			perBlock := time.Duration(0)
			if blocks > 0 {
				perBlock = replayTime / time.Duration(blocks)
			}
			mlog.Log.Infof("bench checkpoint replay: %d folds, %d blocks, %d bank hash mismatches, %d failures, %d workers; load %s per fold, replay %s per block",
				folds, blocks, mismatches, failures, workers, (loadTime / time.Duration(max(1, folds))).Round(time.Microsecond), perBlock.Round(time.Microsecond))
		}()
		for ctx.Err() == nil {
			boundaries, err := db.CheckpointBoundaries()
			next := uint64(0)
			if err == nil {
				// Oldest unreplayed boundary that has a fold after it, leaving the
				// two newest alone so the fold being read is settled.
				for i := 0; i+2 < len(boundaries); i++ {
					if !done[boundaries[i]] {
						next = boundaries[i]
						break
					}
				}
			}
			if next == 0 {
				select {
				case <-ctx.Done():
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
			done[next] = true
			replayed, wrong, load, elapsed, err := benchReplayFold(ctx, db, persisted, next, workers, shredDir)
			if err != nil {
				if ctx.Err() == nil {
					failures++
				}
				if failures <= 5 {
					mlog.Log.Infof("bench checkpoint replay: fold after %d: %d blocks then %v", next, replayed, err)
				}
				continue
			}
			if replayed == 0 {
				continue
			}
			folds++
			blocks += replayed
			mismatches += wrong
			loadTime += load
			replayTime += elapsed
			mlog.Log.Infof("bench checkpoint replay: fold after %d: %d blocks, %d mismatches, load %s, replay %s", next, replayed, wrong, load.Round(time.Microsecond), elapsed.Round(time.Millisecond))
		}
	}()
	// The caller cancels ctx and then waits, so the block in flight finishes
	// before the store closes.
	return func() { <-finished }
}

// benchReplayFold replays the fold after boundary. It returns the blocks
// replayed and how many had a bank hash other than the recorded one; zero
// blocks means the fold's shreds are not in the directory.
func benchReplayFold(ctx context.Context, db *accountsdb.AccountsDb, persisted *state.MithrilState, boundary uint64, workers int, shredDir string) (replayed, wrong int, load, elapsed time.Duration, err error) {
	view, err := db.OpenCheckpointView(boundary)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	fold, err := view.FoldAfter()
	if err != nil || fold == nil || len(fold.Bankhashes) == 0 {
		mlog.Log.Infof("bench checkpoint replay: skip %d: fold %v, err %v", boundary, fold != nil, err)
		return 0, 0, 0, 0, err
	}
	for _, slot := range fold.Bankhashes {
		if _, statErr := os.Stat(filepath.Join(shredDir, turbine.SpoolFileName(slot.Slot))); statErr != nil {
			mlog.Log.Infof("bench checkpoint replay: skip %d: fold (%d, %d] has %d hashes, no shreds for slot %d", boundary, fold.FromSlot, fold.ThroughSlot, len(fold.Bankhashes), slot.Slot)
			return 0, 0, 0, 0, nil
		}
	}
	loadStart := time.Now()
	mlog.Log.Infof("bench checkpoint replay: fold after %d: setting up", boundary)
	replay, err := NewCheckpointReplay(ctx, db, view, persisted, workers)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	loaded, _, err := view.LoadAccounts(ctx, fold.Keys)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	replay.Preload(fold.Keys, loaded)
	load = time.Since(loadStart)
	mlog.Log.Infof("bench checkpoint replay: fold after %d: set up and loaded %d accounts in %s", boundary, len(fold.Keys), load.Round(time.Millisecond))
	for _, want := range fold.Bankhashes {
		if err := ctx.Err(); err != nil {
			return replayed, wrong, load, elapsed, err
		}
		packets, err := turbine.ReadSpoolFile(filepath.Join(shredDir, turbine.SpoolFileName(want.Slot)))
		if err != nil {
			return replayed, wrong, load, elapsed, err
		}
		block, err := turbine.BlockFromSpooledPackets(want.Slot, packets)
		if err != nil {
			return replayed, wrong, load, elapsed, err
		}
		start := time.Now()
		var slotCtx *sealevel.SlotCtx
		// The label marks this replay's samples (its workers inherit it) in a
		// process-wide CPU profile.
		pprof.Do(ctx, pprof.Labels("replay", "private"), func(context.Context) {
			slotCtx, err = replay.Next(block)
		})
		elapsed += time.Since(start)
		if err != nil {
			return replayed, wrong, load, elapsed, err
		}
		replayed++
		if !bytes.Equal(slotCtx.FinalBankhash, want.Bankhash[:]) {
			wrong++
			if wrong <= 3 {
				mlog.Log.Infof("bench checkpoint replay: slot %d bank hash %x, the node recorded %x", want.Slot, slotCtx.FinalBankhash, want.Bankhash[:])
			}
		}
	}
	return replayed, wrong, load, elapsed, nil
}
