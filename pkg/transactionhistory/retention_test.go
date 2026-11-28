package transactionhistory

import (
	"errors"
	"math"
	"os"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type retentionTestFS struct {
	vfs.FS
	available atomic.Uint64
	checks    atomic.Uint64
	failure   atomic.Pointer[error]
}

func (fs *retentionTestFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	fs.checks.Add(1)
	var err error
	if failure := fs.failure.Load(); failure != nil {
		err = *failure
	}
	return vfs.DiskUsage{AvailBytes: fs.available.Load()}, err
}

func retentionFixture(t *testing.T) (*retentionTestFS, Limits, Retention) {
	t.Helper()
	fs := &retentionTestFS{FS: vfs.NewMem()}
	fs.available.Store(1 << 30)
	limits := DefaultLimits()
	limits.MaxBatchBytes = 1024
	return fs, limits, Retention{Slots: 1000, MaxBytes: 1024, MinFreeBytes: 10000}
}

func openRetentionFixture(t *testing.T, fs vfs.FS, limits Limits, retention Retention) *Store {
	t.Helper()
	store, err := openStoreWithRetention("history", [32]byte{9}, limits, fs, &retention)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func requireHistoryStats(t *testing.T, store *Store, want Stats) {
	t.Helper()
	got, err := store.Stats()
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestRetentionValidationBeforeFilesystemCreation(t *testing.T) {
	for _, invalid := range []Retention{{}, {1, 1023, 1}, {1, 1024, 0}, {1, 1024, math.MaxUint64}} {
		fs, limits, _ := retentionFixture(t)
		store, err := openStoreWithRetention("new", [32]byte{9}, limits, fs, &invalid)
		require.Error(t, err)
		require.Nil(t, store)
		_, err = fs.Stat("new")
		require.True(t, os.IsNotExist(err))
		require.Zero(t, fs.checks.Load(), "invalid policy does not inspect or create storage")
	}
	fs, limits, retention := retentionFixture(t)
	fs.available.Store(retention.MinFreeBytes + limits.MaxBatchBytes - 1)
	store, err := openStoreWithRetention("new", [32]byte{9}, limits, fs, &retention)
	require.ErrorIs(t, err, ErrRetention)
	require.Nil(t, store)
	_, err = fs.Stat("new")
	require.True(t, os.IsNotExist(err))
	// Exercise the real platform helper against a not-yet-created nested path.
	limits = DefaultLimits()
	dir := t.TempDir() + "/one/two"
	store, err = OpenWithRetention(dir, [32]byte{9}, limits, Retention{Slots: 2, MaxBytes: limits.MaxBatchBytes, MinFreeBytes: 1})
	require.NoError(t, err)
	require.NoError(t, store.Close())
}

func TestRetentionAccountingForksReopenAndSnapshots(t *testing.T) {
	fs, limits, retention := retentionFixture(t)
	retention.MaxBytes = 4096
	store := openRetentionFixture(t, fs, limits, retention)
	first := testBlock(t, 8, 1, solana.MessageVersionLegacy)
	fork := first
	fork.ID.Blockhash[1]++
	empty := testBlock(t, 9, 0, solana.MessageVersionLegacy)
	var total uint64
	for _, block := range []Block{first, fork, empty} {
		require.NoError(t, store.WriteBlock(block))
		c, err := prepareBlock(block, limits)
		require.NoError(t, err)
		total += completionBytes(c)
	}
	require.Equal(t, uint64(237), completionBytes(completion{}), "empty declarations are not free")
	requireHistoryStats(t, store, Stats{3, total, 8, 9})
	require.NoError(t, store.WriteBlock(first))
	requireHistoryStats(t, store, Stats{3, total, 8, 9})
	require.NoError(t, store.Close())
	store = openRetentionFixture(t, fs, limits, retention)
	requireHistoryStats(t, store, Stats{3, total, 8, 9})
	old, err := store.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, old.Close()) }()
	removed, err := store.PruneBefore(9, 2)
	require.NoError(t, err)
	require.Equal(t, uint32(2), removed)
	requireHistoryStats(t, store, Stats{1, 237, 9, 9})
	got, err := old.Block(first.ID)
	require.NoError(t, err)
	require.Equal(t, first, got)
	got, err = old.Block(fork.ID)
	require.NoError(t, err)
	require.Equal(t, fork, got)
	removed, err = store.PruneBefore(10, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), removed)
	requireHistoryStats(t, store, Stats{})
}

func TestRetentionBoundedCleanupAndRetry(t *testing.T) {
	for _, count := range []uint64{64, 65} {
		t.Run(map[uint64]string{64: "exact64", 65: "needs65"}[count], func(t *testing.T) {
			fs, limits, retention := retentionFixture(t)
			seed, err := openStore("history", [32]byte{9}, limits, fs)
			require.NoError(t, err)
			first := testBlock(t, 1, 0, solana.MessageVersionLegacy)
			for slot := uint64(1); slot <= count; slot++ {
				require.NoError(t, seed.WriteBlock(testBlock(t, slot, 0, solana.MessageVersionLegacy)))
			}
			require.NoError(t, seed.Close())
			store := openRetentionFixture(t, fs, limits, retention)
			want := Stats{count, count * 237, 1, count}
			requireHistoryStats(t, store, want) // Open does not silently repair excess quota.
			require.NoError(t, store.WriteBlock(first))
			conflict := first
			conflict.Bankhash[0]++
			require.ErrorIs(t, store.WriteBlock(conflict), ErrConflict)
			requireHistoryStats(t, store, want)
			next := testBlock(t, 100, 1, solana.MessageVersionLegacy)
			next.Records[0].Metadata = make([]byte, 980-(recordHeaderSize+len(next.Records[0].Transaction)+218+237))
			c, err := prepareBlock(next, limits)
			require.NoError(t, err)
			require.Equal(t, uint64(980), completionBytes(c))
			err = store.WriteBlock(next)
			if count == 65 {
				require.ErrorIs(t, err, ErrRetention)
				require.Nil(t, store.poison)
				requireHistoryStats(t, store, Stats{1, 237, 65, 65})
				err = store.WriteBlock(next)
			}
			require.NoError(t, err)
			requireHistoryStats(t, store, Stats{1, 980, 100, 100})
		})
	}
}

func TestRetentionDiskPressureDoesNotAssumeReclamation(t *testing.T) {
	fs, limits, retention := retentionFixture(t)
	fs.available.Store(retention.MinFreeBytes + limits.MaxBatchBytes)
	store := openRetentionFixture(t, fs, limits, retention)
	for slot := uint64(1); slot <= 4; slot++ {
		require.NoError(t, store.WriteBlock(testBlock(t, slot, 0, solana.MessageVersionLegacy)))
	}
	requireHistoryStats(t, store, Stats{4, 948, 1, 4})
	old, err := store.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, old.Close()) }()
	commit := store.commit
	store.commit = func(batch *pebble.Batch) error {
		err := commit(batch)
		// Emulate concurrent disk consumption after a successful deletion. Its
		// tombstones have not returned any filesystem capacity to the caller.
		fs.available.Store(retention.MinFreeBytes + limits.MaxBatchBytes - 1)
		return err
	}
	err = store.WriteBlock(testBlock(t, 5, 1, solana.MessageVersionLegacy))
	require.ErrorIs(t, err, ErrRetention)
	require.Nil(t, store.poison)
	requireHistoryStats(t, store, Stats{3, 711, 2, 4})
	_, err = old.Block(testBlock(t, 1, 0, solana.MessageVersionLegacy).ID)
	require.NoError(t, err)
	removed, err := store.PruneBefore(5, 1)
	require.ErrorIs(t, err, ErrRetention)
	require.Zero(t, removed)
	failure := errors.New("statfs failed")
	fs.failure.Store(&failure)
	require.ErrorIs(t, store.WriteBlock(testBlock(t, 6, 0, solana.MessageVersionLegacy)), failure)
	require.Nil(t, store.poison)
	fs.failure.Store(nil)
	fs.available.Store(retention.MinFreeBytes + limits.MaxBatchBytes)
	store.commit = commit
	require.NoError(t, store.WriteBlock(testBlock(t, 5, 1, solana.MessageVersionLegacy)))
}

func TestRetentionAccountingRequiresAcknowledgement(t *testing.T) {
	for _, deletion := range []bool{false, true} {
		t.Run(map[bool]string{false: "write", true: "delete"}[deletion], func(t *testing.T) {
			fs, limits, retention := retentionFixture(t)
			store := openRetentionFixture(t, fs, limits, retention)
			block := testBlock(t, 8, 0, solana.MessageVersionLegacy)
			if deletion {
				require.NoError(t, store.WriteBlock(block))
			}
			beforeBlocks, beforeBytes := store.blocks, store.bytes
			failure := errors.New("ambiguous acknowledgement")
			store.commit = func(batch *pebble.Batch) error {
				require.NoError(t, batch.Commit(pebble.Sync))
				return failure
			}
			var err error
			if deletion {
				_, err = store.PruneBefore(9, 1)
			} else {
				err = store.WriteBlock(block)
			}
			require.ErrorIs(t, err, failure)
			require.Equal(t, beforeBlocks, store.blocks)
			require.Equal(t, beforeBytes, store.bytes)
			_, err = store.Stats()
			require.ErrorIs(t, err, ErrUnavailable)
			require.NoError(t, store.Close())
			reopened := openRetentionFixture(t, fs, limits, retention)
			want := Stats{1, 237, 8, 8}
			if deletion {
				want = Stats{}
			}
			requireHistoryStats(t, reopened, want)
		})
	}
}

func TestRetentionWindowMaxSlotAndDeepRewind(t *testing.T) {
	fs, limits, retention := retentionFixture(t)
	retention.Slots = 2
	store := openRetentionFixture(t, fs, limits, retention)
	require.NoError(t, store.WriteBlock(testBlock(t, 0, 0, solana.MessageVersionLegacy)))
	requireHistoryStats(t, store, Stats{1, 237, 0, 0})
	last := testBlock(t, math.MaxUint64, 0, solana.MessageVersionLegacy)
	require.NoError(t, store.WriteBlock(last))
	require.NoError(t, store.WriteBlock(testBlock(t, math.MaxUint64-1, 0, solana.MessageVersionLegacy)))
	require.ErrorIs(t, store.WriteBlock(testBlock(t, 100, 0, solana.MessageVersionLegacy)), ErrRetention)
	requireHistoryStats(t, store, Stats{2, 474, math.MaxUint64 - 1, math.MaxUint64})
	fork := testBlock(t, math.MaxUint64, 1, solana.MessageVersionLegacy)
	fork.ID.Blockhash[1]++
	fork.Records[0].Metadata = make([]byte, 980-(recordHeaderSize+len(fork.Records[0].Transaction)+218+237))
	require.NoError(t, store.WriteBlock(fork)) // Quota pruning can delete an old MaxUint64 candidate.
	requireHistoryStats(t, store, Stats{1, 980, math.MaxUint64, math.MaxUint64})
}

func TestRetentionRejectsMalformedCompletionOnOpen(t *testing.T) {
	fs, limits, retention := retentionFixture(t)
	store := openRetentionFixture(t, fs, limits, retention)
	block := testBlock(t, 8, 0, solana.MessageVersionLegacy)
	require.NoError(t, store.WriteBlock(block))
	require.NoError(t, store.db.Set(blockKey('c', block.ID), []byte{1}, pebble.Sync))
	require.NoError(t, store.Close())
	reopened, err := openStoreWithRetention("history", [32]byte{9}, limits, fs, &retention)
	require.ErrorIs(t, err, ErrUnavailable)
	require.Nil(t, reopened)
}
