package replay

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A slot with no spool file ends the stream with one error entry; a cancelled
// context ends it without blocking on a reader that has gone away.
func TestDecodeSpooledBlocksAheadStopsAtTheFirstError(t *testing.T) {
	decoded := DecodeSpooledBlocksAhead(t.Context(), t.TempDir(), []uint64{5, 6, 7}, 2)
	first, open := <-decoded
	require.True(t, open)
	require.Nil(t, first.Block)
	require.ErrorContains(t, first.Err, "slot 5")
	_, open = <-decoded
	require.False(t, open, "nothing follows the first error")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	decoded = DecodeSpooledBlocksAhead(ctx, t.TempDir(), nil, 0)
	_, open = <-decoded
	require.False(t, open)
}
