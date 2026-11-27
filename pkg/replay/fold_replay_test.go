package replay

import (
	"errors"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// A kept replay's tail gives back the state after any slot it replayed, and a
// view of it never writes into it.
func TestCheckpointTailReadsBackAnySlot(t *testing.T) {
	key, untouched := solana.PublicKey{1}, solana.PublicKey{2}
	base := &checkpointTail{overlay: accounts.NewWorkingSet(), versions: make(map[solana.PublicKey][]accountVersion)}
	base.fetched.Store(key, &accounts.Account{Key: key, Lamports: 1})
	base.fetched.Store(untouched, &accounts.Account{Key: untouched, Lamports: 7})
	base.Add(10, []*accounts.Account{{Key: key, Lamports: 10}}, nil)
	base.Add(12, []*accounts.Account{{Key: key, Lamports: 12}}, nil)
	base.Add(15, []*accounts.Account{{Key: key, Lamports: 15}}, nil)

	for through, want := range map[uint64]uint64{9: 1, 10: 10, 11: 10, 12: 12, 14: 12, 15: 15, 99: 15} {
		view := base.viewAfter(through)
		acct, err := view.GetAccountShared(0, key)
		require.NoError(t, err)
		require.Equal(t, want, acct.Lamports, "as of slot %d", through)
		batch, err := view.GetAccountsBatchShared(t.Context(), 0, []solana.PublicKey{key, untouched})
		require.NoError(t, err)
		require.Equal(t, want, batch[0].Lamports)
		require.Equal(t, uint64(7), batch[1].Lamports, "an account no block changed comes from the boundary")
	}

	// A query's own block writes only to its own view.
	view := base.viewAfter(11)
	view.Add(12, []*accounts.Account{{Key: key, Lamports: 999}}, nil)
	acct, err := view.GetAccountShared(0, key)
	require.NoError(t, err)
	require.Equal(t, uint64(999), acct.Lamports)
	other, err := base.viewAfter(12).GetAccountShared(0, key)
	require.NoError(t, err)
	require.Equal(t, uint64(12), other.Lamports, "another query still sees the kept replay's version")
	require.Len(t, base.versions[key], 3)
}

func TestKeptFoldsBuildOnceAndForgetTheOldest(t *testing.T) {
	var kept keptFolds
	builds := 0
	build := func(boundary uint64) func() (*FoldReplay, error) {
		return func() (*FoldReplay, error) {
			builds++
			return &FoldReplay{boundary: boundary}, nil
		}
	}
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			fold, _, err := kept.get(100, 2, build(100))
			require.NoError(t, err)
			require.Equal(t, uint64(100), fold.Boundary())
		}()
	}
	wait.Wait()
	require.Equal(t, 1, builds, "queries arriving together share one replay")

	_, built, _ := kept.get(200, 2, build(200))
	require.True(t, built)
	_, built, _ = kept.get(100, 2, build(100))
	require.False(t, built, "still kept")
	kept.get(300, 2, build(300)) // 200 is now the least recently used
	_, built, _ = kept.get(100, 2, build(100))
	require.False(t, built)
	_, built, _ = kept.get(200, 2, build(200))
	require.True(t, built, "the least recently used fold was dropped")

	// A failed build is not kept.
	failing := errors.New("no shreds")
	_, _, err := kept.get(400, 2, func() (*FoldReplay, error) { return nil, failing })
	require.ErrorIs(t, err, failing)
	fold, built, err := kept.get(400, 2, build(400))
	require.NoError(t, err)
	require.True(t, built)
	require.Equal(t, uint64(400), fold.Boundary())
}
