package blockhistory

import (
	"errors"
	"sync"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestStoreConcurrentReadAndPrepare(t *testing.T) {
	store, err := Open(t.TempDir(), 100)
	require.NoError(t, err)
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 1}))
	require.NoError(t, store.Prepare(10))
	require.NoError(t, store.SetRooted(10))

	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for i := 0; i < 100000; i++ {
			_, _ = store.Get(1)
		}
	}()
	for i := 0; i < 20; i++ {
		require.NoError(t, store.RecordBlock(&b.Block{Slot: 1}))
		require.NoError(t, store.Prepare(10))
	}
	readers.Wait()
}

func TestStoreOnlyPublishesRootedHistoryAndReplacesFork(t *testing.T) {
	store, err := Open(t.TempDir(), 100)
	require.NoError(t, err)

	firstHash := solana.Hash{1}
	secondHash := solana.Hash{2}
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 10, Blockhash: firstHash}))
	_, err = store.Get(10)
	require.ErrorIs(t, err, ErrNotAvailable)

	require.NoError(t, store.RecordBlock(&b.Block{Slot: 10, Blockhash: secondHash}))
	require.NoError(t, store.Prepare(10))
	require.NoError(t, store.SetRooted(10))
	record, err := store.Get(10)
	require.NoError(t, err)
	require.Equal(t, secondHash.String(), record.Blockhash)
}

func TestStoreHidesPreparedBatchUntilItsFoldCommits(t *testing.T) {
	store, err := Open(t.TempDir(), 100)
	require.NoError(t, err)
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 11}))
	require.NoError(t, store.Prepare(12))
	store.rooted.Store(11) // The account fold through slot 12 has not committed.
	_, err = store.Get(11)
	require.ErrorIs(t, err, ErrNotAvailable)
	_, err = store.FirstAvailableBlock()
	require.ErrorIs(t, err, ErrNotAvailable)
	require.NoError(t, store.SetRooted(12))
	_, err = store.Get(11)
	require.NoError(t, err)
}

func TestStoreUsesAlpenglowFooterBlockTime(t *testing.T) {
	store, err := Open(t.TempDir(), 100)
	require.NoError(t, err)
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 1, FooterProducerTimeNanos: 1_790_110_767_123_456_789}))
	require.NoError(t, store.Prepare(1))
	require.NoError(t, store.SetRooted(1))
	record, err := store.Get(1)
	require.NoError(t, err)
	require.NotNil(t, record.BlockTime)
	require.Equal(t, int64(1_790_110_767), *record.BlockTime)
}

func TestStorePrepareWithoutPendingHistoryIsNoOp(t *testing.T) {
	store, err := Open(t.TempDir(), 100)
	require.NoError(t, err)
	require.NoError(t, store.Prepare(10))
	require.NoError(t, store.SetRooted(10))
}

func TestStoreKeepsLedgerAndBlockFloorsDistinct(t *testing.T) {
	store, err := Open(t.TempDir(), 100)
	require.NoError(t, err)
	require.NoError(t, store.RecordSkipped(8))
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 9}))
	require.NoError(t, store.Prepare(9))
	require.NoError(t, store.SetRooted(9))

	minimum, err := store.MinimumLedgerSlot()
	require.NoError(t, err)
	require.Equal(t, uint64(8), minimum)
	firstBlock, err := store.FirstAvailableBlock()
	require.NoError(t, err)
	require.Equal(t, uint64(9), firstBlock)
	_, err = store.Get(8)
	require.ErrorIs(t, err, ErrSlotSkipped)
}

func TestStoreRetentionPrunesOldRecords(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 2)
	require.NoError(t, err)
	for slot := uint64(1); slot <= 4; slot++ {
		require.NoError(t, store.RecordBlock(&b.Block{Slot: slot}))
		require.NoError(t, store.Prepare(slot))
		require.NoError(t, store.SetRooted(slot))
	}
	_, err = store.Get(1)
	require.True(t, errors.Is(err, ErrNotAvailable))
	minimum, err := store.MinimumLedgerSlot()
	require.NoError(t, err)
	require.Equal(t, uint64(3), minimum)

	reopened, err := Open(dir, 2)
	require.NoError(t, err)
	require.NoError(t, reopened.SetRooted(4))
	first, err := reopened.FirstAvailableBlock()
	require.NoError(t, err)
	require.Equal(t, uint64(3), first)
}

func TestStoreDropsUnselectedBatchAfterRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 100)
	require.NoError(t, err)
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 10, Blockhash: solana.Hash{1}}))
	require.NoError(t, store.Prepare(10)) // Simulate crash before AccountsDB commits slot 10.

	reopened, err := Open(dir, 100)
	require.NoError(t, err)
	require.NoError(t, reopened.SetRooted(9))
	require.NoError(t, reopened.RecordBlock(&b.Block{Slot: 10, Blockhash: solana.Hash{2}}))
	require.NoError(t, reopened.Prepare(10))
	require.NoError(t, reopened.SetRooted(10))
	record, err := reopened.Get(10)
	require.NoError(t, err)
	require.Equal(t, solana.Hash{2}.String(), record.Blockhash)
}

func TestStoreRewindDropsPersistedAndSpeculativeForkHistory(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 100)
	require.NoError(t, err)
	for slot := uint64(10); slot <= 12; slot++ {
		require.NoError(t, store.RecordBlock(&b.Block{Slot: slot, Blockhash: solana.Hash{byte(slot)}}))
		require.NoError(t, store.Prepare(slot))
		require.NoError(t, store.SetRooted(slot))
	}
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 13, Blockhash: solana.Hash{13}}))

	require.NoError(t, store.Rewind(10))
	_, err = store.Get(11)
	require.ErrorIs(t, err, ErrNotAvailable)

	// A replacement fold must not inherit the abandoned slot 13 record.
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 11, Blockhash: solana.Hash{21}}))
	require.NoError(t, store.Prepare(13))
	require.NoError(t, store.SetRooted(13))
	_, err = store.Get(13)
	require.ErrorIs(t, err, ErrNotAvailable)
	record, err := store.Get(11)
	require.NoError(t, err)
	require.Equal(t, solana.Hash{21}.String(), record.Blockhash)
}

func TestStoreDiscardsUnrootedForkSuffixWithoutAdvancingRoot(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 100)
	require.NoError(t, err)
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 10, Blockhash: solana.Hash{10}}))
	require.NoError(t, store.Prepare(10))
	require.NoError(t, store.SetRooted(10))
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 11, Blockhash: solana.Hash{11}}))
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 12, Blockhash: solana.Hash{12}}))
	require.NoError(t, store.Prepare(12))
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 13, Blockhash: solana.Hash{13}}))

	require.NoError(t, store.DiscardUnrootedFrom(12))
	require.Equal(t, uint64(10), store.Rooted())
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 12, Blockhash: solana.Hash{22}}))
	require.NoError(t, store.Prepare(13))
	require.NoError(t, store.SetRooted(13))

	reopened, err := Open(dir, 100)
	require.NoError(t, err)
	require.NoError(t, reopened.SetRooted(13))
	for slot, hash := range map[uint64]solana.Hash{10: {10}, 11: {11}, 12: {22}} {
		record, err := reopened.Get(slot)
		require.NoError(t, err)
		require.Equal(t, hash.String(), record.Blockhash)
	}
	_, err = reopened.Get(13)
	require.ErrorIs(t, err, ErrNotAvailable)
}
