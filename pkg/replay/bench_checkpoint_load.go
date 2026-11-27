package replay

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

// Bench only: MITHRIL_BENCH_CHECKPOINT_LOAD=<workers>:<pause ms> runs
// checkpoint loads beside replay, to measure how much they disturb it. Each
// load opens a view two fold boundaries behind the head and loads, as of that
// boundary, every account the following fold wrote, which is what a historical
// query would do before replaying that fold. A pause of 0 runs them back to
// back. Nothing is published and the results are discarded.
func startBenchCheckpointLoad(ctx context.Context, db *accountsdb.AccountsDb) {
	spec := os.Getenv("MITHRIL_BENCH_CHECKPOINT_LOAD")
	if spec == "" || db == nil {
		return
	}
	parts := strings.Split(spec, ":")
	workers, err := strconv.Atoi(parts[0])
	if err != nil || workers < 1 || len(parts) != 2 {
		mlog.Log.Infof("bench checkpoint load: want <workers>:<pause ms>, got %q", spec)
		return
	}
	pauseMs, err := strconv.Atoi(parts[1])
	if err != nil || pauseMs < 0 {
		mlog.Log.Infof("bench checkpoint load: want <workers>:<pause ms>, got %q", spec)
		return
	}
	go func() {
		var times []time.Duration
		var accounts, bytes, failures uint64
		report := func() {
			if len(times) == 0 {
				mlog.Log.Infof("bench checkpoint load: no loads completed (%d failures)", failures)
				return
			}
			sort.Slice(times, func(a, b int) bool { return times[a] < times[b] })
			var sum time.Duration
			for _, d := range times {
				sum += d
			}
			mlog.Log.Infof("bench checkpoint load: %d loads with %d workers, pause %d ms: mean %s, median %s, max %s; %d accounts and %d MiB per load on average; %d failures",
				len(times), workers, pauseMs, (sum / time.Duration(len(times))).Round(time.Microsecond), times[len(times)/2].Round(time.Microsecond),
				times[len(times)-1].Round(time.Microsecond), accounts/uint64(len(times)), bytes/uint64(len(times))>>20, failures)
		}
		defer report()
		for ctx.Err() == nil {
			boundaries, err := db.CheckpointBoundaries()
			if err != nil || len(boundaries) < 3 {
				select {
				case <-ctx.Done():
				case <-time.After(50 * time.Millisecond):
				}
				continue
			}
			start := time.Now()
			view, err := db.OpenCheckpointView(boundaries[len(boundaries)-3])
			if err == nil {
				var keys []solana.PublicKey
				if keys, err = view.KeysWrittenAfter(); err == nil {
					var stats accountsdb.CheckpointLoadStats
					if _, stats, err = view.LoadAccountsWith(ctx, keys, workers); err == nil {
						times = append(times, time.Since(start))
						accounts += stats.Requested
						bytes += stats.DataBytes
					}
				}
			}
			if err != nil && ctx.Err() == nil {
				failures++
			}
			if pauseMs > 0 {
				select {
				case <-ctx.Done():
				case <-time.After(time.Duration(pauseMs) * time.Millisecond):
				}
			}
		}
	}()
}
