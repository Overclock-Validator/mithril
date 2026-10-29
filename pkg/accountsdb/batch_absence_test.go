package accountsdb

import (
	"context"
	"encoding/binary"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/cockroachdb/pebble/v2"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestBatchAbsenceReuseAndCreation(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	key := solana.PublicKey{42}
	keys := []solana.PublicKey{key, key}
	first, s, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, keys)
	require.NoError(t, err)
	require.EqualValues(t, 2, s.IndexMisses)
	first[0].Lamports = 99 // placeholders are private to the caller, not cache values
	second, s, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, keys)
	require.NoError(t, err)
	require.EqualValues(t, 2, s.NegativeCacheHits)
	require.Zero(t, second[0].Lamports)
	require.Equal(t, uint64(math.MaxUint64), second[0].RentEpoch)
	_, err = db.GetAccount(100, key)
	require.ErrorIs(t, err, ErrNoAccount)
	created := foldAcct(42, 17, []byte("created"))
	commitTestBatch(t, db, 110, created)
	second, s, err = db.GetAccountsBatchSharedWithStats(context.Background(), 110, keys)
	require.NoError(t, err)
	require.Zero(t, s.NegativeCacheHits)
	require.EqualValues(t, 17, second[0].Lamports)
}

func TestBatchAbsenceDeletionRecreationAndRewind(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	key := solana.PublicKey{42}
	keys := []solana.PublicKey{key}
	commitTestBatch(t, db, 100, foldAcct(42, 17, []byte("original")))
	commitTestBatch(t, db, 110, foldAcct(42, 0, nil))
	for range 2 {
		out, err := db.GetAccountsBatch(context.Background(), 110, keys)
		require.NoError(t, err)
		require.Zero(t, out[0].Lamports)
	}
	_, err := db.RewindToBatchBoundary(100)
	require.NoError(t, err)
	out, _, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, keys)
	require.NoError(t, err)
	require.EqualValues(t, 17, out[0].Lamports)
	commitTestBatch(t, db, 120, foldAcct(42, 0, nil))
	_, err = db.GetAccountsBatch(context.Background(), 120, keys)
	require.NoError(t, err)
	commitTestBatch(t, db, 130, foldAcct(42, 23, []byte("recreated")))
	out, err = db.GetAccountsBatch(context.Background(), 130, keys)
	require.NoError(t, err)
	require.EqualValues(t, 23, out[0].Lamports)
}

func TestBatchAbsenceRejectsStaleSnapshot(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	key := solana.PublicKey{42}
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	db.batchHooks.beforeCacheAdmission = func(pk solana.PublicKey) {
		if pk == key {
			close(reached)
			<-release
		}
	}
	done := make(chan error, 1)
	go func() { _, err := db.GetAccountsBatch(context.Background(), 100, []solana.PublicKey{key}); done <- err }()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("missing read did not reach admission")
	}
	commitTestBatch(t, db, 110, foldAcct(42, 17, nil))
	once.Do(func() { close(release) })
	require.NoError(t, <-done)
	db.batchHooks.beforeCacheAdmission = nil
	// Force the new read through the index, rather than a positive cache hit.
	db.CommonAcctsCache.Delete(key)
	out, s, err := db.GetAccountsBatchSharedWithStats(context.Background(), 110, []solana.PublicKey{key})
	require.NoError(t, err)
	require.Zero(t, s.NegativeCacheHits)
	require.EqualValues(t, 17, out[0].Lamports)
}

func TestBatchAbsencePendingFoldWins(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	key := solana.PublicKey{42}
	keys := []solana.PublicKey{key}
	_, err := db.GetAccountsBatch(context.Background(), 100, keys)
	require.NoError(t, err)
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	db.foldHooks.afterPublicationStart = func() { close(reached); <-release }
	done := make(chan error, 1)
	go func() {
		_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 110, Delta: []*accounts.Account{foldAcct(42, 17, nil)}}}, 110, nil, nil)
		done <- err
	}()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("fold did not publish")
	}
	out, s, err := db.GetAccountsBatchSharedWithStats(context.Background(), 110, keys)
	require.NoError(t, err)
	require.EqualValues(t, 17, out[0].Lamports)
	require.EqualValues(t, 1, s.PendingFoldHits)
	require.Zero(t, s.NegativeCacheHits)
	once.Do(func() { close(release) })
	require.NoError(t, <-done)
}

func TestBatchAbsenceLegacyWriteAndErrors(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	key := solana.PublicKey{42}
	keys := []solana.PublicKey{key}
	_, err := db.GetAccountsBatch(context.Background(), 100, keys)
	require.NoError(t, err)
	db.storeAccountsSync([]*accounts.Account{foldAcct(42, 17, nil)}, 110)
	out, err := db.GetAccountsBatch(context.Background(), 110, keys)
	require.NoError(t, err)
	require.EqualValues(t, 17, out[0].Lamports)
	bad := solana.PublicKey{43}
	require.NoError(t, db.Index.Set(bad[:], []byte{1}, pebble.Sync))
	for range 2 {
		_, err = db.GetAccountsBatch(context.Background(), 110, []solana.PublicKey{bad})
		require.Error(t, err)
	}
	require.NotContains(t, db.batchAbsent, bad, "decode errors cannot establish absence")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = db.GetAccountsBatch(ctx, 110, []solana.PublicKey{{44}})
	require.ErrorIs(t, err, context.Canceled)
}

func TestBatchAbsenceBoundAndConcurrentReuse(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	// Small batches are admitted; enough distinct keys must still stay bounded.
	for batch := 0; batch < 20; batch++ {
		keys := make([]solana.PublicKey, 512)
		for i := range keys {
			binary.LittleEndian.PutUint64(keys[i][:], uint64(batch*512+i))
		}
		_, err := db.GetAccountsBatch(context.Background(), 100, keys)
		require.NoError(t, err)
		require.LessOrEqual(t, len(db.batchAbsent), batchAbsentLimit)
	}
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				out, err := db.GetAccountsBatch(context.Background(), 100, []solana.PublicKey{{byte(worker)}})
				if err != nil || len(out) != 1 || out[0].Lamports != 0 {
					t.Errorf("concurrent missing read: %v", err)
				}
			}
		}()
	}
	wg.Wait()
}
