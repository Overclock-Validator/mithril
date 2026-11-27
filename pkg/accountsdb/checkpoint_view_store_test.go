package accountsdb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// MITHRIL_CHECKPOINT_VIEW_STORE=<accounts store directory> measures a
// checkpoint view on a real store that no node is using. For each of the
// newest retained boundaries below the head it loads, as of that boundary,
// every account the following fold wrote: the starting state a replay of that
// fold needs for the accounts it changes. It reports the time, and for
// comparison the time to read the same keys one at a time through the index
// at their current versions. It writes nothing.
func TestCheckpointViewOnStore(t *testing.T) {
	dir := os.Getenv("MITHRIL_CHECKPOINT_VIEW_STORE")
	if dir == "" {
		t.Skip("set MITHRIL_CHECKPOINT_VIEW_STORE to measure against a real store")
	}
	db, err := OpenDb(dir)
	require.NoError(t, err)
	defer db.CloseDb()
	db.InitCaches()
	points, err := db.ListRewindPoints()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(points), 2, "the store needs at least two retained fold boundaries")
	headers, err := ListFoldManifests(db.AcctsDir)
	require.NoError(t, err)
	manifestOf := make(map[uint64]string, len(headers))
	for _, header := range headers {
		manifestOf[header.BatchSeq] = header.Path
	}
	t.Logf("store %s: %d retained boundaries, head slot %d", dir, len(points), points[len(points)-1].ThroughSlot)

	for back := 1; back <= 6 && back < len(points); back++ {
		boundary := points[len(points)-1-back]
		next, err := ReadSegmentManifest(manifestOf[boundary.BatchSeq+1])
		require.NoError(t, err)
		keys := make([]solana.PublicKey, len(next.Records))
		for i := range next.Records {
			keys[i] = solana.PublicKey(next.Records[i].Pubkey)
		}

		openStart := time.Now()
		view, err := db.OpenCheckpointView(boundary.ThroughSlot)
		require.NoError(t, err)
		openTime := time.Since(openStart)
		view.mu.Lock()
		changed := len(view.changed)
		view.mu.Unlock()

		var loadTimes []time.Duration
		var stats CheckpointLoadStats
		for range 3 {
			start := time.Now()
			loaded, s, err := view.LoadAccounts(context.Background(), keys)
			require.NoError(t, err)
			loadTimes = append(loadTimes, time.Since(start))
			stats = s
			for i, account := range loaded {
				if account != nil {
					require.LessOrEqual(t, account.Slot, boundary.ThroughSlot, "key %s is newer than the boundary", keys[i])
				}
			}
		}

		// The ordinary path, for scale: one index lookup and one read per key,
		// with the account cache emptied first.
		db.CommonAcctsCache.Clear()
		db.VoteAcctCache.Clear()
		start := time.Now()
		for _, key := range keys {
			if _, err := db.GetAccount(0, key); err != nil && err != ErrNoAccount {
				require.NoError(t, err)
			}
		}
		single := time.Since(start)

		t.Logf("boundary %d (%d folds back): next fold wrote %d accounts | open view %s (%d changed since) | bulk load %s, %s, %s: %d files, %.1f MiB, %d absent | one at a time through the index %s",
			boundary.ThroughSlot, back, len(keys), openTime.Round(time.Microsecond), changed,
			loadTimes[0].Round(time.Microsecond), loadTimes[1].Round(time.Microsecond), loadTimes[2].Round(time.Microsecond),
			stats.Files, float64(stats.DataBytes)/(1<<20), stats.Absent, single.Round(time.Microsecond))
	}
}
