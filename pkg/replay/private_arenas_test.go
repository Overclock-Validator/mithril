package replay

import (
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/stretchr/testify/require"
)

func TestPrivateArenasAreClearedBetweenBlocksAndNeverShared(t *testing.T) {
	set := acquirePrivateArenas(3)
	require.Len(t, set.borrowed, 3)
	data, fromArena := set.params.AllocN(64)
	require.True(t, fromArena)
	for i := range data {
		data[i] = 0xff
	}
	record, _ := set.borrowed[2].Alloc()
	require.NotNil(t, record)

	private := &b.Block{PrivateReplay: true, PrivateArenas: set}
	require.Same(t, set.borrowed[2], borrowedAccountArena(private, 2))
	require.Nil(t, borrowedAccountArena(private, 3), "a worker beyond the set gets the heap")
	require.Nil(t, borrowedAccountArena(&b.Block{PrivateReplay: true}, 0), "a private block without a set never falls back to the node's arenas")

	set.release()
	require.GreaterOrEqual(t, privateParameterPeak.Load(), uint64(64))
	require.Zero(t, set.params.Used())
	require.Zero(t, set.borrowed[2].Used())
	again, _ := set.params.AllocN(64)
	for _, value := range again {
		require.Zero(t, value, "a reused arena starts clean")
	}

	// A wider replay grows a reused set; a narrower one keeps it.
	wider := acquirePrivateArenas(5)
	require.GreaterOrEqual(t, len(wider.borrowed), 5)
	wider.release()
}
