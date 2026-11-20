package accountsdb

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// A fold that does not write a key leaves what the batch path learned about
// that key's absence in place; a fold that creates the key removes it.
func TestBatchAbsenceSurvivesUnrelatedFold(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	a, b := solana.PublicKey{42}, solana.PublicKey{43}
	keys := []solana.PublicKey{a, b}

	_, s, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, keys)
	require.NoError(t, err)
	require.EqualValues(t, 2, s.IndexMisses)

	commitTestBatch(t, db, 110, foldAcct(44, 5, []byte("unrelated")))
	out, s, err := db.GetAccountsBatchSharedWithStats(context.Background(), 110, keys)
	require.NoError(t, err)
	require.EqualValues(t, 2, s.NegativeCacheHits, "an unrelated fold dropped the absences")
	require.Zero(t, s.IndexMisses)
	require.Zero(t, out[0].Lamports)
	require.Zero(t, out[1].Lamports)

	commitTestBatch(t, db, 120, foldAcct(43, 9, []byte("created")))
	out, s, err = db.GetAccountsBatchSharedWithStats(context.Background(), 120, keys)
	require.NoError(t, err)
	require.EqualValues(t, 1, s.NegativeCacheHits)
	require.Zero(t, s.IndexMisses)
	require.Zero(t, out[0].Lamports)
	require.EqualValues(t, 9, out[1].Lamports, "a created account was reported absent")

	// Deleting it again must be seen too, and the survivor is still a hit.
	commitTestBatch(t, db, 130, foldAcct(43, 0, nil))
	out, s, err = db.GetAccountsBatchSharedWithStats(context.Background(), 130, keys)
	require.NoError(t, err)
	require.EqualValues(t, 1, s.NegativeCacheHits)
	require.Zero(t, out[1].Lamports)
}

// While a fold is between publication and its index commit, carried absences
// answer for keys it does not write, its own keys come from the pending view,
// and new absences learned in that window are kept once it finishes.
func TestBatchAbsenceCarriedThroughPendingFold(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	kept, created, late := solana.PublicKey{42}, solana.PublicKey{43}, solana.PublicKey{45}
	_, err := db.GetAccountsBatch(context.Background(), 100, []solana.PublicKey{kept, created})
	require.NoError(t, err)

	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	db.foldHooks.afterPublicationStart = func() { close(reached); <-release }
	done := make(chan error, 1)
	go func() {
		_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 110, Delta: []*accounts.Account{foldAcct(43, 17, nil)}}}, 110, nil, nil)
		done <- err
	}()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("fold did not publish")
	}

	out, s, err := db.GetAccountsBatchSharedWithStats(context.Background(), 110, []solana.PublicKey{kept, created, late})
	require.NoError(t, err)
	require.EqualValues(t, 1, s.NegativeCacheHits, "the carried absence was not used during the fold")
	require.EqualValues(t, 1, s.PendingFoldHits)
	require.EqualValues(t, 1, s.IndexMisses)
	require.Zero(t, out[0].Lamports)
	require.EqualValues(t, 17, out[1].Lamports)

	once.Do(func() { close(release) })
	require.NoError(t, <-done)
	db.foldHooks.afterPublicationStart = nil

	out, s, err = db.GetAccountsBatchSharedWithStats(context.Background(), 110, []solana.PublicKey{kept, created, late})
	require.NoError(t, err)
	require.EqualValues(t, 2, s.NegativeCacheHits)
	require.Zero(t, s.IndexMisses)
	require.EqualValues(t, 17, out[1].Lamports)
}

// Rewind changes durable state without a changed-key view, so it must still
// invalidate every absence, carried or not.
func TestBatchAbsenceRewindStillClearsCarried(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	key := solana.PublicKey{42}
	keys := []solana.PublicKey{key}
	commitTestBatch(t, db, 100, foldAcct(50, 1, nil))
	commitTestBatch(t, db, 110, foldAcct(42, 17, []byte("exists at 110")))
	commitTestBatch(t, db, 120, foldAcct(42, 0, nil))
	for range 2 {
		_, err := db.GetAccountsBatch(context.Background(), 120, keys)
		require.NoError(t, err)
	}
	commitTestBatch(t, db, 130, foldAcct(51, 1, nil))
	_, err := db.RewindToBatchBoundary(110)
	require.NoError(t, err)
	out, s, err := db.GetAccountsBatchSharedWithStats(context.Background(), 110, keys)
	require.NoError(t, err)
	require.Zero(t, s.NegativeCacheHits, "an absence survived a rewind")
	require.EqualValues(t, 17, out[0].Lamports)
}

func TestCarryBatchAbsencesLocked(t *testing.T) {
	key := func(i int) solana.PublicKey {
		var k solana.PublicKey
		binary.LittleEndian.PutUint64(k[:], uint64(i))
		return k
	}
	build := func(absent, changed []int) (*AccountsDb, map[[32]byte]dedupedVersion) {
		db := &AccountsDb{readCacheEpoch: 7, batchAbsentEpoch: 7, batchAbsent: map[solana.PublicKey]struct{}{}}
		for _, i := range absent {
			db.batchAbsent[key(i)] = struct{}{}
		}
		union := map[[32]byte]dedupedVersion{}
		for _, i := range changed {
			union[[32]byte(key(i))] = dedupedVersion{}
		}
		return db, union
	}
	rangeOf := func(lo, hi int) []int {
		var r []int
		for i := lo; i < hi; i++ {
			r = append(r, i)
		}
		return r
	}

	// Fewer changed keys than entries, and more: both loops must agree.
	for _, tc := range []struct{ absent, changed []int }{
		{rangeOf(0, 100), []int{3, 50, 99, 1000}},
		{[]int{3, 50, 99}, rangeOf(40, 400)},
		{rangeOf(0, 10), nil},
	} {
		db, union := build(tc.absent, tc.changed)
		db.readCacheEpoch++
		db.carryBatchAbsencesLocked(union)
		require.Equal(t, db.readCacheEpoch, db.batchAbsentEpoch)
		for _, i := range tc.absent {
			_, changed := union[[32]byte(key(i))]
			require.Equal(t, !changed, db.batchAccountAbsentLocked(key(i)), "key %d", i)
		}
	}

	// Entries that were already stale are not revived.
	db, union := build(rangeOf(0, 10), nil)
	db.batchAbsentEpoch = 5
	db.readCacheEpoch++
	db.carryBatchAbsencesLocked(union)
	require.False(t, db.batchAccountAbsentLocked(key(1)))

	// Two epoch advances with only one carry (a rewind between folds) leave
	// them stale as well.
	db, union = build(rangeOf(0, 10), nil)
	db.readCacheEpoch += 2
	db.carryBatchAbsencesLocked(union)
	require.False(t, db.batchAccountAbsentLocked(key(1)))
}

func TestBatchCacheAdmissionAcrossActiveFold(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	unchanged := foldAcct(1, 11, []byte{1})
	deleted := foldAcct(2, 22, []byte{2})
	created := foldAcct(3, 33, []byte{3})
	missing, carried := solana.PublicKey{4}, solana.PublicKey{5}
	commitTestBatch(t, db, 100, unchanged, deleted)
	epoch := db.readCacheEpoch
	_, err := db.GetAccountsBatch(context.Background(), 100, []solana.PublicKey{carried})
	require.NoError(t, err)
	db.CommonAcctsCache.Delete(unchanged.Key)
	db.foldHooks.afterPublicationStart = func() {
		result, _ := db.cacheBatchReadAccount(unchanged.Key, unchanged, true, epoch)
		require.Equal(t, batchCacheCommon, result)
		result, _ = db.cacheBatchReadAccount(deleted.Key, deleted, true, epoch)
		require.Equal(t, batchCacheEpochRejected, result)

		// A reader from two transitions ago lacks a complete changed-key view.
		result, _ = db.cacheBatchReadAccount(unchanged.Key, unchanged, true, epoch-1)
		require.Equal(t, batchCacheEpochRejected, result)
		tooOld := solana.PublicKey{6}
		db.cacheBatchAbsences(context.Background(), []solana.PublicKey{tooOld}, []int{0}, []bool{false}, []bool{true}, epoch-1)
		require.NotContains(t, db.batchAbsent, tooOld)
		db.cacheBatchAbsences(context.Background(), []solana.PublicKey{created.Key, missing}, []int{0, 1}, []bool{false, false}, []bool{true, true}, epoch)
		require.Contains(t, db.batchAbsent, carried)
		require.Contains(t, db.batchAbsent, missing)
		require.NotContains(t, db.batchAbsent, created.Key)
		require.Equal(t, db.readCacheEpoch, db.batchAbsentEpoch)
	}
	commitTestBatch(t, db, 110, created, foldAcct(2, 0, nil))
	db.foldHooks.afterPublicationStart = nil
	out, stats, err := db.GetAccountsBatchSharedWithStats(context.Background(), 110, []solana.PublicKey{unchanged.Key, missing, created.Key, deleted.Key})
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.NegativeCacheHits)
	require.EqualValues(t, 11, out[0].Lamports)
	require.Zero(t, out[1].Lamports)
	require.EqualValues(t, 33, out[2].Lamports)
	require.Zero(t, out[3].Lamports)

	// A completed fold and a rewind each advance one epoch but leave no map.
	checkRejected := func(oldEpoch uint64) {
		result, _ := db.cacheBatchReadAccount(created.Key, created, true, oldEpoch)
		require.Equal(t, batchCacheEpochRejected, result)
		stale := solana.PublicKey{7}
		db.cacheBatchAbsences(context.Background(), []solana.PublicKey{stale}, []int{0}, []bool{false}, []bool{true}, oldEpoch)
		require.NotContains(t, db.batchAbsent, stale)
	}
	checkRejected(epoch)
	beforeRewind := db.readCacheEpoch
	_, err = db.RewindToBatchBoundary(100)
	require.NoError(t, err)
	checkRejected(beforeRewind)
}
