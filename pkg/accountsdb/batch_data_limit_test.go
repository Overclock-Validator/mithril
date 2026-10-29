package accountsdb

import (
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestAccountDataBudgetConcurrentAndOverflow(t *testing.T) {
	budget := &accountDataBudget{limit: 64}
	var accepted atomic.Uint64
	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Go(func() {
			if budget.reserve(1) == nil {
				accepted.Add(1)
			}
		})
	}
	wg.Wait()
	require.Equal(t, uint64(64), accepted.Load())
	require.Equal(t, uint64(64), budget.charged.Load())
	require.NoError(t, budget.reserve(0))
	require.ErrorIs(t, budget.reserve(math.MaxUint64), ErrAccountDataLimit)
	maximum := &accountDataBudget{limit: math.MaxUint64}
	require.NoError(t, maximum.reserve(math.MaxUint64))
	require.ErrorIs(t, maximum.reserve(1), ErrAccountDataLimit)
	var unbounded *accountDataBudget
	require.NoError(t, unbounded.reserve(math.MaxUint64))
}

func TestBatchDataLimitColdCacheDuplicatesAndMissing(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	one, two := foldAcct(1, 10, []byte{1, 2, 3, 4}), foldAcct(2, 20, []byte{5, 6, 7})
	commitTestBatch(t, db, 100, one, two)
	clear := func() { db.CommonAcctsCache.Delete(one.Key); db.CommonAcctsCache.Delete(two.Key) }
	clear()
	keys := []solana.PublicKey{two.Key, {99}, one.Key, one.Key}
	out, stats, err := db.GetAccountsBatchSharedWithDataLimit(t.Context(), 100, keys, 10)
	require.ErrorIs(t, err, ErrAccountDataLimit)
	require.Nil(t, out)
	require.LessOrEqual(t, stats.DataBytesCharged, uint64(10))
	require.Positive(t, stats.DataBytesCharged)
	require.Zero(t, stats.CommonCacheAdmissions)
	require.False(t, db.CommonAcctsCache.Has(one.Key))
	require.False(t, db.CommonAcctsCache.Has(two.Key))
	out, stats, err = db.GetAccountsBatchSharedWithDataLimit(t.Context(), 100, keys, 11)
	require.NoError(t, err)
	require.Equal(t, uint64(11), stats.DataBytesCharged)
	require.Len(t, out, 4)
	require.Equal(t, two.Data, out[0].Data)
	require.Zero(t, out[1].Lamports)
	require.Equal(t, one.Data, out[2].Data)
	require.Equal(t, out[2], out[3])
	_, cached, err := db.GetAccountsBatchSharedWithDataLimit(t.Context(), 100, keys, 11)
	require.NoError(t, err)
	require.Equal(t, uint64(3), cached.CacheHits)
	require.Equal(t, uint64(11), cached.DataBytesCharged)
	out, _, err = db.GetAccountsBatchSharedWithDataLimit(t.Context(), 100, keys, 10)
	require.ErrorIs(t, err, ErrAccountDataLimit)
	require.Nil(t, out)
	legacy, legacyStats, err := db.GetAccountsBatchSharedWithStats(t.Context(), 100, keys)
	require.NoError(t, err)
	require.Len(t, legacy, 4)
	require.Zero(t, legacyStats.DataBytesCharged)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	out, stats, err = db.GetAccountsBatchSharedWithDataLimit(ctx, 100, keys, 11)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, out)
	require.Zero(t, stats.DataBytesCharged)
}

func TestBatchDataLimitPendingPrecedenceBeforeClone(t *testing.T) {
	key := solana.PublicKey{1}
	large := &accounts.Account{Key: key, Lamports: 1, Data: make([]byte, 1<<20)}
	small := &accounts.Account{Key: key, Lamports: 2, Data: []byte{1}}
	db := &AccountsDb{inProgressStoreRequests: list.New()}
	db.inProgressStoreRequests.PushBack(storeRequest{m: map[solana.PublicKey]*accounts.Account{key: small}})
	db.inProgressStoreRequests.PushBack(storeRequest{m: map[solana.PublicKey]*accounts.Account{key: large}})
	budget := &accountDataBudget{limit: 64}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	out, err := db.getStoreInProgressAccountsWithBudget([]solana.PublicKey{key}, budget)
	runtime.ReadMemStats(&after)
	require.ErrorIs(t, err, ErrAccountDataLimit)
	require.Nil(t, out)
	require.Zero(t, budget.charged.Load())
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(256<<10), "oversized pending data must not be cloned first; this is not an RSS assertion")
	db.inProgressStoreRequests.Remove(db.inProgressStoreRequests.Back())
	out, err = db.getStoreInProgressAccountsWithBudget([]solana.PublicKey{key, key}, &accountDataBudget{limit: 2})
	require.NoError(t, err)
	out[0].Data[0] = 9
	require.Equal(t, byte(1), out[1].Data[0])
	require.Equal(t, byte(1), small.Data[0])
	db.InitCaches()
	db.inProgressStoreRequests.Init()
	require.True(t, db.CommonAcctsCache.Set(key, small))
	db.pendingFold = map[[32]byte]dedupedVersion{key: {acct: large, ownerSlot: 100}}
	out, stats, err := db.GetAccountsBatchSharedWithDataLimit(t.Context(), 100, []solana.PublicKey{key}, 64)
	require.ErrorIs(t, err, ErrAccountDataLimit)
	require.Nil(t, out)
	require.Zero(t, stats.DataBytesCharged)
	db.pendingFold = nil
}

func TestBatchDataLimitColdHeaderBeforeAllocationAndFailedCharge(t *testing.T) {
	var header [hdrLen]byte
	binary.LittleEndian.PutUint64(header[8:16], math.MaxUint64)
	var account AppendVecAccount
	budget := &accountDataBudget{limit: 4}
	err := account.unmarshalWithBudget(bytes.NewReader(header[:]), math.MaxUint64, budget)
	require.ErrorIs(t, err, ErrAccountDataLimit)
	require.Nil(t, account.Data)
	require.Zero(t, budget.charged.Load())
	binary.LittleEndian.PutUint64(header[8:16], 4)
	err = account.unmarshalWithBudget(bytes.NewReader(append(header[:], 1)), math.MaxUint64, budget)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, uint64(4), budget.charged.Load(), "failed decode charge must survive")
	err = account.unmarshalWithBudget(bytes.NewReader(append(header[:], 1, 2, 3, 4)), math.MaxUint64, budget)
	require.ErrorIs(t, err, ErrAccountDataLimit)
	var exact AppendVecAccount
	err = exact.unmarshalWithBudget(bytes.NewReader(append(header[:], 1, 2, 3, 4)), math.MaxUint64, &accountDataBudget{limit: 4})
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3, 4}, exact.Data)
}

func TestBatchDataLimitDecodeFailureAndCancellationCharges(t *testing.T) {
	t.Run("decode failure before cache publication", func(t *testing.T) {
		db, _ := newFoldTestDb(t)
		defer db.CloseDb()
		one, two := foldAcct(1, 10, []byte{1, 2, 3, 4}), foldAcct(2, 20, []byte{5, 6, 7, 8})
		result := commitTestBatch(t, db, 100, one, two)
		var lastOffset uint64
		for _, key := range []solana.PublicKey{one.Key, two.Key} {
			db.CommonAcctsCache.Delete(key)
			data, closer, err := db.Index.Get(key[:])
			require.NoError(t, err)
			entry, err := UnmarshalAcctIdxEntryValue(data)
			require.NoError(t, err)
			require.NoError(t, closer.Close())
			lastOffset = max(lastOffset, entry.Offset)
		}
		path := filepath.Join(db.AcctsDir, fmt.Sprintf("100.%d", result.FileId))
		require.NoError(t, os.Truncate(path, int64(lastOffset+hdrLen+1)))
		out, stats, err := db.GetAccountsBatchSharedWithDataLimit(t.Context(), 100, []solana.PublicKey{one.Key, two.Key}, 8)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.Nil(t, out, "one valid account cannot escape a failed batch")
		require.Equal(t, uint64(8), stats.DataBytesCharged, "successful and failed decode reservations both survive")
		require.Equal(t, uint64(1), stats.AppendVecAccounts)
		require.Equal(t, uint64(1), stats.ReadFailures)
		require.Zero(t, stats.CommonCacheAdmissions)
		require.False(t, db.CommonAcctsCache.Has(one.Key))
		require.False(t, db.CommonAcctsCache.Has(two.Key))
	})
	t.Run("cancellation after hydration", func(t *testing.T) {
		db, _ := newFoldTestDb(t)
		defer db.CloseDb()
		one := foldAcct(1, 10, []byte{1, 2, 3, 4})
		commitTestBatch(t, db, 100, one)
		db.CommonAcctsCache.Delete(one.Key)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		db.batchHooks.beforeCacheAdmission = func(solana.PublicKey) { cancel() }
		defer func() { db.batchHooks.beforeCacheAdmission = nil }()
		_, stats, err := db.GetAccountsBatchSharedWithDataLimit(ctx, 100, []solana.PublicKey{one.Key}, 4)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, uint64(4), stats.DataBytesCharged)
		// Cancellation during publication may retain an already-admitted
		// immutable cache value; it must not refund its data charge.
	})
}
