package replay

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

// Bench only: MITHRIL_BENCH_CHECKPOINT_QUERY=<workers>:<pause ms>:<shred directory>
// asks, beside the node's own replay, for what a random transaction of a
// random already-folded slot did (CheckpointQuerier), one query at a time with
// the pause between them, and logs how long the answers took. Each answer is
// compared with what the node's replay recorded for that transaction (failed
// or not, compute consumed) when replay hints are on.
// MITHRIL_BENCH_CHECKPOINT_QUERY_NOHINTS=1 runs the queries without hints.
func startBenchCheckpointQuery(ctx context.Context, db *accountsdb.AccountsDb, persisted *state.MithrilState) (wait func()) {
	spec := os.Getenv("MITHRIL_BENCH_CHECKPOINT_QUERY")
	if spec == "" || db == nil || persisted == nil {
		return func() {}
	}
	parts := strings.SplitN(spec, ":", 3)
	var workers, pauseMs int
	var err1, err2 error
	if len(parts) == 3 {
		workers, err1 = strconv.Atoi(parts[0])
		pauseMs, err2 = strconv.Atoi(parts[1])
	}
	if len(parts) != 3 || err1 != nil || err2 != nil || workers < 1 || pauseMs < 0 || parts[2] == "" {
		mlog.Log.Infof("bench checkpoint query: want <workers>:<pause ms>:<shred directory>, got %q", spec)
		return func() {}
	}
	shredDir := parts[2]
	querier := &CheckpointQuerier{DB: db, Persisted: persisted, ShredDir: shredDir, Workers: workers, NoHints: os.Getenv("MITHRIL_BENCH_CHECKPOINT_QUERY_NOHINTS") == "1"}
	// MITHRIL_BENCH_CHECKPOINT_QUERY_KEEP=<n> keeps the n most recently
	// queried folds replayed in memory.
	querier.KeepFolds, _ = strconv.Atoi(os.Getenv("MITHRIL_BENCH_CHECKPOINT_QUERY_KEEP"))
	var keptBytes uint64
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		rng := rand.New(rand.NewSource(7))
		type answer struct {
			blocks               int
			total, setup, replay time.Duration
			kept, built          bool
		}
		var answers []answer
		var failures, wrong, checked, fellBack, failedTargets int
		defer func() {
			if len(answers) == 0 {
				mlog.Log.Infof("bench checkpoint query: no answers, %d failures", failures)
				return
			}
			at := func(value func(answer) float64, p float64) float64 {
				values := make([]float64, len(answers))
				for i, a := range answers {
					values[i] = value(a)
				}
				sort.Float64s(values)
				return values[min(len(values)-1, int(float64(len(values))*p))]
			}
			total := func(a answer) float64 { return float64(a.total) / 1e6 }
			var blocks int
			var replay time.Duration
			for _, a := range answers {
				blocks += a.blocks
				replay += a.replay
			}
			mlog.Log.Infof("bench checkpoint query: %d answers, %d failures, %d of %d checked answers differ from the node's record, %d fell back, %d targets were failed transactions; %d workers, hints %t; wait ms median %.0f p90 %.0f max %.0f min %.0f; setup ms median %.1f; blocks replayed median %.0f; replay %.1f ms per block",
				len(answers), failures, wrong, checked, fellBack, failedTargets, workers, !querier.NoHints,
				at(total, 0.5), at(total, 0.9), at(total, 1), at(total, 0),
				at(func(a answer) float64 { return float64(a.setup) / 1e6 }, 0.5),
				at(func(a answer) float64 { return float64(a.blocks) }, 0.5),
				float64(replay)/1e6/float64(max(1, blocks)))
			if querier.KeepFolds > 0 {
				all := answers
				for _, group := range []struct {
					name  string
					built bool
				}{{"answered from a fold already kept", false}, {"had to replay and keep their fold first", true}} {
					answers = nil
					for _, a := range all {
						if a.kept && a.built == group.built {
							answers = append(answers, a)
						}
					}
					if len(answers) > 0 {
						mlog.Log.Infof("bench checkpoint query: %d queries %s: wait ms median %.1f p90 %.1f max %.1f min %.1f",
							len(answers), group.name, at(total, 0.5), at(total, 0.9), at(total, 1), at(total, 0))
					}
				}
				answers = all
				mlog.Log.Infof("bench checkpoint query: keeping %d folds, about %.2f GiB each", querier.KeepFolds, float64(keptBytes)/(1<<30))
				return
			}
			// By how deep into its fold the target was.
			for _, band := range [][2]int{{1, 16}, {17, 64}, {65, 128}} {
				var inBand []answer
				for _, a := range answers {
					if a.blocks >= band[0] && a.blocks <= band[1] {
						inBand = append(inBand, a)
					}
				}
				if len(inBand) == 0 {
					continue
				}
				saved := answers
				answers = inBand
				mlog.Log.Infof("bench checkpoint query: targets %d to %d blocks into their fold: %d answers, wait ms median %.0f p90 %.0f max %.0f",
					band[0], band[1], len(inBand), at(total, 0.5), at(total, 0.9), at(total, 1))
				answers = saved
			}
		}()
		pause := func(d time.Duration) bool {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(d):
				return true
			}
		}
		for ctx.Err() == nil {
			boundaries, err := db.CheckpointBoundaries()
			// Folds with a settled fold after them whose shreds are here.
			var usable []uint64
			if err == nil {
				for i := 0; i+2 < len(boundaries); i++ {
					if _, statErr := os.Stat(filepath.Join(shredDir, turbine.SpoolFileName(boundaries[i]+1))); statErr == nil {
						usable = append(usable, boundaries[i])
					}
				}
			}
			if len(usable) == 0 {
				if !pause(100 * time.Millisecond) {
					return
				}
				continue
			}
			view, err := db.OpenCheckpointView(usable[rng.Intn(len(usable))])
			if err != nil {
				failures++
				continue
			}
			fold, err := view.FoldAfter()
			if err != nil || fold == nil || len(fold.Bankhashes) == 0 {
				failures++
				continue
			}
			slot := fold.Bankhashes[rng.Intn(len(fold.Bankhashes))].Slot
			// Pick the transaction (outside the timed query).
			packets, err := turbine.ReadSpoolFile(filepath.Join(shredDir, turbine.SpoolFileName(slot)))
			if err != nil {
				continue
			}
			block, err := turbine.BlockFromSpooledPackets(slot, packets)
			if err != nil || len(block.Transactions) == 0 {
				continue
			}
			target := block.Transactions[rng.Intn(len(block.Transactions))]
			for attempts := 0; attempts < 20 && target.IsVote(); attempts++ {
				target = block.Transactions[rng.Intn(len(block.Transactions))]
			}
			if len(target.Signatures) == 0 {
				continue
			}
			result, err := querier.ReplayTransaction(ctx, slot, target.Signatures[0])
			if err != nil {
				if ctx.Err() == nil {
					failures++
					if failures <= 5 {
						mlog.Log.Infof("bench checkpoint query: slot %d %s: %v", slot, target.Signatures[0], err)
					}
				}
				continue
			}
			answers = append(answers, answer{result.BlocksReplayed, result.Total, result.Setup, result.Replay, result.FromKeptFold, result.BuiltFold})
			keptBytes = max(keptBytes, result.KeptFoldBytes)
			if result.FellBack {
				fellBack++
			}
			if result.Error != nil {
				failedTargets++
			}
			if hints := hintsFor(block); hints != nil {
				if hint, known := hints.bySig[target.Signatures[0]]; known {
					checked++
					if hint.failed != (result.Error != nil) || hint.kcu != int32(result.ComputeUnitsConsumed/1000)+1 {
						wrong++
						if wrong <= 5 {
							mlog.Log.Infof("bench checkpoint query: slot %d %s: answer failed=%t %d CU, the node recorded failed=%t %d kCU", slot, target.Signatures[0], result.Error != nil, result.ComputeUnitsConsumed, hint.failed, hint.kcu-1)
						}
					}
				}
			}
			if len(answers) <= 6 {
				mlog.Log.Infof("bench checkpoint query: slot %d (%d blocks from boundary %d): %s total, setup %s, replay %s; fee %d, %d CU, error %s, %d log lines, %d accounts",
					slot, result.BlocksReplayed, result.Boundary, result.Total.Round(time.Millisecond), result.Setup.Round(100*time.Microsecond), result.Replay.Round(time.Millisecond),
					result.Fee, result.ComputeUnitsConsumed, string(result.Error), len(result.LogMessages), len(result.AccountKeys))
			}
			if !pause(time.Duration(pauseMs) * time.Millisecond) {
				return
			}
		}
	}()
	return func() { <-finished }
}
