package blockhistory

import (
	"context"
	"math"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/stretchr/testify/require"
)

func TestBlocksRetainedCoverage(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 100)
	require.NoError(t, err)
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 20}))
	require.NoError(t, store.RecordSkipped(21))
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 22}))
	require.NoError(t, store.Prepare(22))
	require.NoError(t, store.SetRooted(22))
	for _, reopen := range []bool{false, true} {
		if reopen {
			store, err = Open(dir, 100)
			require.NoError(t, err)
			require.NoError(t, store.SetRooted(22))
		}
		got, _, err := store.Blocks(t.Context(), 20, 22, 22)
		require.NoError(t, err)
		require.Equal(t, []uint64{20, 22}, got)
		got, _, err = store.Blocks(t.Context(), 21, 21, 22)
		require.NoError(t, err)
		require.Equal(t, []uint64{}, got)
		got, unknown, err := store.Blocks(t.Context(), 19, 22, 22)
		require.ErrorIs(t, err, ErrNotAvailable)
		require.Equal(t, uint64(19), unknown)
		require.Nil(t, got)
	}
	// A prepared skipped record must not certify a gap before its fold commits.
	require.NoError(t, store.RecordSkipped(23))
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 24}))
	require.NoError(t, store.Prepare(24))
	store.rooted.Store(23)
	got, unknown, err := store.Blocks(t.Context(), 22, 23, 23)
	require.ErrorIs(t, err, ErrNotAvailable)
	require.Equal(t, uint64(23), unknown)
	require.Nil(t, got)
	require.NoError(t, store.SetRooted(24))
	got, _, err = store.Blocks(t.Context(), 22, 24, 24)
	require.NoError(t, err)
	require.Equal(t, []uint64{22, 24}, got)
	// The first missing middle record invalidates the entire result.
	delete(store.persisted, 23)
	got, unknown, err = store.Blocks(t.Context(), 22, 24, 24)
	require.ErrorIs(t, err, ErrNotAvailable)
	require.Equal(t, uint64(23), unknown)
	require.Nil(t, got)
}

func TestBlocksPruneAndRewind(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 2)
	require.NoError(t, err)
	for slot := uint64(1); slot <= 4; slot++ {
		require.NoError(t, store.RecordBlock(&b.Block{Slot: slot}))
		require.NoError(t, store.Prepare(slot))
		require.NoError(t, store.SetRooted(slot))
	}
	store, err = Open(dir, 2)
	require.NoError(t, err)
	require.NoError(t, store.SetRooted(4))
	got, unknown, err := store.Blocks(t.Context(), 2, 4, 4)
	require.ErrorIs(t, err, ErrNotAvailable)
	require.Equal(t, uint64(2), unknown)
	require.Nil(t, got)
	require.NoError(t, store.Rewind(3))
	got, _, err = store.Blocks(t.Context(), 3, 4, 4)
	require.ErrorIs(t, err, ErrNotAvailable)
	require.Nil(t, got)
	require.NoError(t, store.RecordSkipped(4))
	require.NoError(t, store.Prepare(4))
	require.NoError(t, store.SetRooted(4))
	got, _, err = store.Blocks(t.Context(), 3, 4, 4)
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, got, "the abandoned produced slot is now known skipped")
}

// rangeContext stages cancellation/root publication at a known map-probe boundary
// without adding hooks to the store or relying on scheduler timing.
type rangeContext struct {
	context.Context
	checks int
	at     int
	action func()
}

func (ctx *rangeContext) Err() error {
	ctx.checks++
	if ctx.checks == ctx.at {
		ctx.action()
	}
	return ctx.Context.Err()
}

func TestBlocksCancellationAndRootChange(t *testing.T) {
	for _, changeRoot := range []bool{false, true} {
		store, err := Open(t.TempDir(), 100)
		require.NoError(t, err)
		for slot := uint64(0); slot < 4096; slot++ {
			store.persisted[slot] = Record{Slot: slot}
			store.sourceBatch[slot] = 4095
		}
		store.rooted.Store(4095)
		base, cancel := context.WithCancel(t.Context())
		defer cancel()
		ctx := &rangeContext{Context: base, at: 100}
		ctx.action = cancel
		if changeRoot {
			ctx.action = func() { store.rooted.Store(4094) }
		}
		got, _, err := store.Blocks(ctx, 0, 4095, 4095)
		require.Nil(t, got, "never return the prefix accumulated before invalidation")
		if changeRoot {
			require.ErrorIs(t, err, ErrNotAvailable)
		} else {
			require.ErrorIs(t, err, context.Canceled)
		}
	}
}

func TestBlocksRangeBoundaries(t *testing.T) {
	store, err := Open(t.TempDir(), math.MaxUint64)
	require.NoError(t, err)
	store.rooted.Store(math.MaxUint64)
	for _, slot := range []uint64{math.MaxUint64 - 1, math.MaxUint64} {
		store.persisted[slot] = Record{Slot: slot}
		store.sourceBatch[slot] = math.MaxUint64
	}
	got, _, err := store.Blocks(t.Context(), math.MaxUint64-1, math.MaxUint64, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, []uint64{math.MaxUint64 - 1, math.MaxUint64}, got)
	got, _, err = store.Blocks(t.Context(), 2, 1, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, []uint64{}, got)
	// Exactly 500000 difference permits 500001 inclusive positions. It reaches
	// the coverage check rather than requiring a half-million-record fixture.
	_, unknown, err := store.Blocks(t.Context(), 0, MaxBlocksRange, math.MaxUint64)
	require.ErrorIs(t, err, ErrNotAvailable)
	require.Zero(t, unknown)
	_, _, err = store.Blocks(t.Context(), 0, MaxBlocksRange+1, math.MaxUint64)
	require.ErrorContains(t, err, "slot range exceeds")
	require.NotErrorIs(t, err, ErrNotAvailable)
}
