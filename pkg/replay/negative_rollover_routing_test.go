package replay

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestPrefetchNegativeRolloverRouting(t *testing.T) {
	key := func(i int) (k solana.PublicKey) {
		binary.BigEndian.PutUint32(k[:4], uint32(i+1))
		return
	}
	for _, origin := range []string{"foreground", "prefetch"} {
		t.Run(origin, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(dir, "accounts"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
			db, err := accountsdb.OpenDb(dir)
			require.NoError(t, err)
			defer db.CloseDb()
			db.InitCaches()
			ctx := context.Background()
			tail := newUnrootedTail(db, db, 512, 128, "")
			// Two more eligible admissions in one batch will cross the 8,192-key bound.
			for start := 0; start < 8191; start += 512 {
				var keys []solana.PublicKey
				for i := start; i < min(start+512, 8191); i++ {
					keys = append(keys, key(i))
				}
				_, s, err := getAccountsBatchSharedWithStats(ctx, tail, 49, keys)
				require.NoError(t, err)
				require.EqualValues(t, len(keys), s.IndexMisses)
			}
			keys := []solana.PublicKey{key(0), key(8191), key(8192)}
			if origin == "prefetch" {
				p := &accountPrefetcher{ctx: ctx, src: tail, workers: 1}
				p.warmJob(accountPrefetchJob{slot: 50, keys: keys}, true)
				require.EqualValues(t, 1, p.warmed.Load())
			} else {
				_, s, err := getAccountsBatchSharedWithStats(ctx, tail, 50, keys)
				require.NoError(t, err)
				require.EqualValues(t, 2, s.IndexMisses)
			}
			// Verify the retained keys using the ordinary foreground routing and counters.
			_, s, err := getAccountsBatchSharedWithStats(ctx, tail, 50, keys)
			require.NoError(t, err)
			wantHits := 1
			if origin == "prefetch" {
				wantHits = 3
			}
			require.EqualValues(t, wantHits, s.NegativeCacheHits)
			require.EqualValues(t, len(keys)-wantHits, s.IndexMisses)
			_, s, err = getAccountsBatchSharedWithStats(ctx, tail, 50, []solana.PublicKey{key(5)})
			require.NoError(t, err)
			require.EqualValues(t, 1, s.IndexMisses, "unrequested old entry must not carry over")
		})
	}
}
