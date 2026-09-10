package accountsdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDedupeBatchRequestIsStableAndUsesScatterOnlyWhenNeeded(t *testing.T) {
	uniqueInput := make([]solana.PublicKey, batchDedupeLinearScanMax+4)
	for i := range uniqueInput {
		binary.LittleEndian.PutUint64(uniqueInput[i][:8], uint64(i+1))
	}
	request, err := dedupeBatchRequest(context.Background(), uniqueInput)
	require.NoError(t, err)
	require.Len(t, request.keys, len(uniqueInput))
	assert.Same(t, &uniqueInput[0], &request.keys[0], "an all-unique request must retain its input slice")
	assert.Nil(t, request.scatter)
	assert.Nil(t, request.logicalCopies)

	lateKey := solana.PublicKey{0xfe}
	input := append([]solana.PublicKey(nil), uniqueInput...)
	input = append(input, uniqueInput[3], lateKey, uniqueInput[0], lateKey)
	request, err = dedupeBatchRequest(context.Background(), input)
	require.NoError(t, err)
	require.Len(t, request.keys, len(uniqueInput)+1)
	assert.Equal(t, uniqueInput, request.keys[:len(uniqueInput)])
	assert.Equal(t, lateKey, request.keys[len(uniqueInput)])
	for i := range uniqueInput {
		assert.Equal(t, i, request.scatter[i])
	}
	assert.Equal(t, 3, request.scatter[len(uniqueInput)])
	assert.Equal(t, len(uniqueInput), request.scatter[len(uniqueInput)+1])
	assert.Equal(t, 0, request.scatter[len(uniqueInput)+2])
	assert.Equal(t, len(uniqueInput), request.scatter[len(uniqueInput)+3])
	assert.Equal(t, uint64(2), request.logicalCopies[0])
	assert.Equal(t, uint64(2), request.logicalCopies[3])
	assert.Equal(t, uint64(2), request.logicalCopies[len(uniqueInput)])

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = dedupeBatchRequest(cancelled, input)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestGetAccountsBatchGroupedReadsPreserveOrder(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	const accountCount = appendVecReadChunkSize*2 + 7
	delta := make([]*accounts.Account, accountCount)
	keys := make([]solana.PublicKey, accountCount)
	for i := range delta {
		keys[i][0] = byte(i + 1)
		keys[i][1] = byte((i + 1) >> 8)
		delta[i] = &accounts.Account{
			Key:       keys[i],
			Lamports:  uint64(1000 + i),
			Owner:     [32]byte{7},
			RentEpoch: uint64(i),
			Data:      []byte{byte(i), byte(i >> 8)},
		}
	}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: delta}}, 100, nil, nil)
	require.NoError(t, err)
	for _, key := range keys {
		db.CommonAcctsCache.Delete(key)
		db.VoteAcctCache.Delete(key)
	}

	missing := solana.PublicKey{0xff, 0xff}
	request := []solana.PublicKey{keys[accountCount-1], missing, keys[0], keys[64], keys[64]}
	out, err := db.GetAccountsBatch(context.Background(), 100, request)
	require.NoError(t, err)
	require.Len(t, out, len(request))
	assert.Equal(t, request[0], out[0].Key)
	assert.Equal(t, uint64(1000+accountCount-1), out[0].Lamports)
	assert.Equal(t, missing, out[1].Key)
	assert.Zero(t, out[1].Lamports)
	assert.Equal(t, uint64(^uint64(0)), out[1].RentEpoch)
	assert.Equal(t, uint64(1000), out[2].Lamports)
	assert.Equal(t, uint64(1064), out[3].Lamports)
	assert.Equal(t, out[3].Data, out[4].Data)
}

func TestGetAccountsBatchStatsAndSmallBatchAdmissions(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	common := foldAcct(1, 100, []byte{1, 2, 3})
	vote := foldAcct(2, 200, []byte{4, 5})
	vote.Owner = addresses.VoteProgramAddr
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{common, vote}}}, 100, nil, nil)
	require.NoError(t, err)
	db.CommonAcctsCache.Delete(common.Key)
	db.VoteAcctCache.Delete(vote.Key)
	missing := solana.PublicKey{9}
	keys := []solana.PublicKey{common.Key, vote.Key, missing}

	out, stats, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, keys)
	require.NoError(t, err)
	require.Len(t, out, len(keys))
	assert.Equal(t, uint64(len(keys)), stats.RequestedKeys)
	assert.Equal(t, uint64(len(keys)), stats.DurableKeys)
	assert.Equal(t, uint64(2), stats.IndexHits)
	assert.Equal(t, uint64(1), stats.IndexMisses)
	assert.Equal(t, uint64(1), stats.UniqueAppendVecs)
	assert.Equal(t, uint64(1), stats.AppendVecChunks)
	assert.Equal(t, uint64(2), stats.AppendVecAccounts)
	assert.Equal(t, uint64(2), stats.DecodedAccountObjects)
	assert.Equal(t, uint64(5), stats.DecodedAccountBytes)
	assert.Equal(t, uint64(1), stats.PlaceholderObjects)
	assert.Equal(t, uint64(1), stats.CommonCacheAdmissions)
	assert.Equal(t, uint64(1), stats.VoteCacheAdmissions)
	assert.Zero(t, stats.CommonCacheAdmissionsSkipped)
	assert.Positive(t, stats.IndexLookupNanoseconds)
	assert.Positive(t, stats.AppendVecReadNanoseconds)

	_, second, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, keys)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), second.CacheHits, "small batch should reuse admitted common and vote accounts")
	assert.Equal(t, uint64(1), second.IndexMisses, "negative entries are deliberately not cached yet")
}

func TestUniqueSharedBatchPathPreservesResultsAndStats(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	first := foldAcct(1, 100, []byte{1, 2, 3})
	second := foldAcct(2, 200, []byte{4, 5})
	_, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{first, second}}},
		100,
		nil,
		nil,
	)
	require.NoError(t, err)
	db.CommonAcctsCache.Delete(first.Key)
	db.CommonAcctsCache.Delete(second.Key)

	missing := solana.PublicKey{9}
	keys := []solana.PublicKey{second.Key, missing, first.Key}
	out, stats, err := db.GetUniqueAccountsBatchSharedWithStats(
		context.Background(), 100, keys,
	)
	require.NoError(t, err)
	require.Len(t, out, len(keys))
	assert.Equal(t, second.Key, out[0].Key)
	assert.Equal(t, uint64(200), out[0].Lamports)
	assert.Equal(t, missing, out[1].Key)
	assert.Zero(t, out[1].Lamports)
	assert.Equal(t, first.Key, out[2].Key)
	assert.Equal(t, uint64(100), out[2].Lamports)
	assert.Equal(t, uint64(len(keys)), stats.RequestedKeys)
	assert.Equal(t, uint64(len(keys)), stats.UniqueKeys)
	assert.Zero(t, stats.DuplicateKeys)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = db.GetUniqueAccountsBatchSharedWithStats(cancelled, 100, keys)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestLargeBatchSelectivelyAdmitsReusableAccounts(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	count := commonAccountImmediateAdmissionLimit + 1
	delta := make([]*accounts.Account, count)
	keys := make([]solana.PublicKey, count)
	for i := range keys {
		binary.LittleEndian.PutUint32(keys[i][:4], uint32(i+1))
		delta[i] = &accounts.Account{Key: keys[i], Lamports: uint64(i + 1), Owner: [32]byte{7}}
	}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: delta}}, 100, nil, nil)
	require.NoError(t, err)
	hotKey := solana.PublicKey{}
	hotKey[len(hotKey)-1] = 0xfe
	hot := &accounts.Account{Key: hotKey, Lamports: 99, Owner: [32]byte{7}}
	require.True(t, db.CommonAcctsCache.Set(hot.Key, hot))

	_, first, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, keys)
	require.NoError(t, err)
	assert.Equal(t, uint64(count), first.IndexHits)
	assert.Equal(t, uint64(count), first.CommonCacheAdmissionsSkipped)
	assert.Zero(t, first.CommonCacheAdmissions)
	assert.True(t, db.CommonAcctsCache.Has(hot.Key), "one-shot scan must preserve established heat")

	_, second, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, keys)
	require.NoError(t, err)
	assert.Zero(t, second.CacheHits, "the second observation performs the selective admission")
	assert.Equal(t, uint64(count), second.IndexHits)
	assert.Equal(t, uint64(count), second.CommonCacheAdmissions)

	_, third, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, keys)
	require.NoError(t, err)
	assert.Equal(t, uint64(count), third.CacheHits, "reusable large-batch accounts must become cache hits")
	assert.Zero(t, third.IndexHits)
	assert.True(t, db.CommonAcctsCache.Has(hot.Key))
}

func TestGetAccountsBatchExactMatchOrderAndInputImmutability(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	const count = 145
	delta := make([]*accounts.Account, count)
	keys := make([]solana.PublicKey, count)
	for i := range keys {
		binary.BigEndian.PutUint64(keys[i][:8], uint64(2*i+2))
		delta[i] = &accounts.Account{Key: keys[i], Lamports: uint64(i + 1), Owner: [32]byte{7}}
	}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: delta}}, 100, nil, nil)
	require.NoError(t, err)
	for _, key := range keys {
		db.CommonAcctsCache.Delete(key)
	}

	request := make([]solana.PublicKey, 0, count+4)
	for i := range keys {
		requestIdx := (i * 257) % count
		request = append(request, keys[requestIdx])
	}
	between := solana.PublicKey{}
	binary.BigEndian.PutUint64(between[:8], 3) // strictly between stored keys 2 and 4
	request = append(request, between, keys[0], keys[count/2], keys[count/2])
	original := append([]solana.PublicKey(nil), request...)

	out, err := db.GetAccountsBatch(context.Background(), 100, request)
	require.NoError(t, err)
	assert.Equal(t, original, request, "resolver must sort only its local index list")
	require.Len(t, out, len(request))
	for i := 0; i < count; i++ {
		requestIdx := (i * 257) % count
		assert.Equal(t, uint64(requestIdx+1), out[i].Lamports)
	}
	assert.Equal(t, between, out[count].Key)
	assert.Zero(t, out[count].Lamports, "an interior miss must not resolve to a neighboring key")
	assert.Equal(t, keys[0], out[count+1].Key)
	assert.Equal(t, out[count+2].Lamports, out[count+3].Lamports)
}

func TestGetAccountsBatchRejectsCancelledContext(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.GetAccountsBatch(ctx, 100, []solana.PublicKey{{1}})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestBatchCacheAdmissionCannotOverwriteNewerFold(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	old := foldAcct(1, 10, []byte{1})
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{old}}}, 100, nil, nil)
	require.NoError(t, err)
	db.CommonAcctsCache.Delete(old.Key)

	readComplete := make(chan struct{})
	resumeAdmission := make(chan struct{})
	var once sync.Once
	db.batchHooks.beforeCacheAdmission = func(pk solana.PublicKey) {
		if pk != old.Key {
			return
		}
		once.Do(func() {
			close(readComplete)
			<-resumeAdmission
		})
	}

	type batchResult struct {
		out []*accounts.Account
		err error
	}
	done := make(chan batchResult, 1)
	go func() {
		out, err := db.GetAccountsBatch(context.Background(), 100, []solana.PublicKey{old.Key})
		done <- batchResult{out: out, err: err}
	}()

	select {
	case <-readComplete:
	case <-time.After(5 * time.Second):
		t.Fatal("old batch did not reach cache admission hook")
	}
	updated := foldAcct(1, 20, []byte{2})
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 101, Delta: []*accounts.Account{updated}}}, 101, nil, nil)
	require.NoError(t, err)
	close(resumeAdmission)
	result := <-done
	require.NoError(t, result.err)
	require.Len(t, result.out, 1)
	assert.Equal(t, uint64(10), result.out[0].Lamports, "in-flight snapshot remains internally old")
	db.batchHooks.beforeCacheAdmission = nil

	got, err := db.GetAccount(101, old.Key)
	require.NoError(t, err)
	assert.Equal(t, uint64(20), got.Lamports)
	assert.Equal(t, []byte{2}, got.Data)
	if cached, ok := db.CommonAcctsCache.Get(old.Key); ok {
		assert.Equal(t, uint64(20), cached.Lamports, "stale batch must never win cache publication")
	}
}

func TestFoldPublicationServesNewValuesWithoutBlockingReaders(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	old := foldAcct(1, 10, []byte{1})
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{old}}}, 100, nil, nil)
	require.NoError(t, err)
	require.True(t, db.CommonAcctsCache.Set(old.Key, old))

	publicationStarted := make(chan struct{})
	releaseCommit := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseCommit) })
	db.foldHooks.afterPublicationStart = func() {
		close(publicationStarted)
		<-releaseCommit
	}

	updated := foldAcct(1, 20, []byte{2})
	commitDone := make(chan error, 1)
	go func() {
		_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 101, Delta: []*accounts.Account{updated}}}, 101, nil, nil)
		commitDone <- err
	}()
	select {
	case <-publicationStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("fold did not publish its pending view")
	}

	type singleResult struct {
		acct  *accounts.Account
		stats AccountReadStats
		err   error
	}
	singleDone := make(chan singleResult, 1)
	go func() {
		acct, stats, err := db.GetAccountWithStats(101, old.Key)
		singleDone <- singleResult{acct: acct, stats: stats, err: err}
	}()
	select {
	case result := <-singleDone:
		require.NoError(t, result.err)
		assert.Equal(t, uint64(20), result.acct.Lamports)
		assert.True(t, result.stats.PendingFoldHit)
	case <-time.After(time.Second):
		t.Fatal("single account read blocked behind index commit")
	}

	batch, stats, err := db.GetAccountsBatchSharedWithStats(context.Background(), 101, []solana.PublicKey{old.Key})
	require.NoError(t, err)
	require.Len(t, batch, 1)
	assert.Equal(t, uint64(20), batch[0].Lamports)
	assert.Equal(t, uint64(1), stats.PendingFoldHits)
	assert.Zero(t, stats.IndexHits)

	releaseOnce.Do(func() { close(releaseCommit) })
	require.NoError(t, <-commitDone)
	db.foldHooks.afterPublicationStart = nil
	got, err := db.GetAccount(101, old.Key)
	require.NoError(t, err)
	assert.Equal(t, uint64(20), got.Lamports)
}

func TestRefreshReadCachesIsScanResistantAndCoherent(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	key := solana.PublicKey{1}
	cold := &accounts.Account{Key: key, Lamports: 10, Owner: [32]byte{7}}
	db.refreshReadCaches([]*accounts.Account{cold})
	assert.False(t, db.CommonAcctsCache.Has(key), "fold must not admit a cold common account")

	db.CommonAcctsCache.Set(key, cold)
	updated := &accounts.Account{Key: key, Lamports: 20, Owner: [32]byte{7}}
	db.refreshReadCaches([]*accounts.Account{updated})
	got, ok := db.CommonAcctsCache.Get(key)
	require.True(t, ok)
	assert.Same(t, updated, got, "resident common entry must be refreshed")

	vote := &accounts.Account{Key: key, Lamports: 30, Owner: addresses.VoteProgramAddr}
	db.refreshReadCaches([]*accounts.Account{vote})
	assert.False(t, db.CommonAcctsCache.Has(key), "owner transition must evict common cache")
	got, ok = db.VoteAcctCache.Get(key)
	require.True(t, ok)
	assert.Same(t, vote, got)

	backToCommon := &accounts.Account{Key: key, Lamports: 40, Owner: [32]byte{7}}
	db.refreshReadCaches([]*accounts.Account{backToCommon})
	assert.False(t, db.VoteAcctCache.Has(key), "owner transition must evict vote cache")
	assert.False(t, db.CommonAcctsCache.Has(key), "cold common transition should not be admitted")

	db.CommonAcctsCache.Set(key, backToCommon)
	db.VoteAcctCache.Set(key, vote)
	db.refreshReadCaches([]*accounts.Account{{Key: key}})
	assert.False(t, db.CommonAcctsCache.Has(key))
	assert.False(t, db.VoteAcctCache.Has(key))
}

func TestRunBatchWorkersBoundsConcurrency(t *testing.T) {
	const jobs = 10_000
	var active atomic.Int64
	var peak atomic.Int64
	var completed atomic.Int64
	err := runBatchWorkers(context.Background(), jobs, func(int) error {
		now := active.Add(1)
		for {
			old := peak.Load()
			if now <= old || peak.CompareAndSwap(old, now) {
				break
			}
		}
		runtime.Gosched()
		completed.Add(1)
		active.Add(-1)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(jobs), completed.Load())
	assert.LessOrEqual(t, peak.Load(), int64(max(1, runtime.GOMAXPROCS(0)*2)))
}

func TestRunBatchWorkersLimitedAppliesHardConcurrencyBound(t *testing.T) {
	const (
		jobs  = 1_000
		limit = 3
	)
	var active atomic.Int64
	var peak atomic.Int64
	release := make(chan struct{})
	started := make(chan struct{}, limit)
	done := make(chan error, 1)
	go func() {
		done <- runBatchWorkersLimited(context.Background(), jobs, limit, func(int) error {
			now := active.Add(1)
			for {
				old := peak.Load()
				if now <= old || peak.CompareAndSwap(old, now) {
					break
				}
			}
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			active.Add(-1)
			return nil
		})
	}()

	wantWorkers := min(limit, max(1, runtime.GOMAXPROCS(0)*2))
	for range wantWorkers {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("bounded workers did not start")
		}
	}
	close(release)
	require.NoError(t, <-done)
	assert.LessOrEqual(t, peak.Load(), int64(limit))
}

func TestRunBatchWorkersReturnsContextAndWorkErrors(t *testing.T) {
	want := errors.New("boom")
	err := runBatchWorkers(context.Background(), 100, func(job int) error {
		if job == 0 {
			return want
		}
		return nil
	})
	assert.ErrorIs(t, err, want)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = runBatchWorkers(ctx, 1, func(int) error {
		t.Fatal("cancelled batch must not start work")
		return nil
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func BenchmarkGetAccountsBatchColdFoldSegment(b *testing.B) {
	db, _ := newFoldTestDb(b)
	defer db.CloseDb()

	const accountCount = 8192
	delta := make([]*accounts.Account, accountCount)
	storedKeys := make([]solana.PublicKey, accountCount)
	keys := make([]solana.PublicKey, accountCount)
	for i := range delta {
		storedKeys[i][0] = byte(i)
		storedKeys[i][1] = byte(i >> 8)
		delta[i] = &accounts.Account{
			Key:       storedKeys[i],
			Lamports:  uint64(i + 1),
			Owner:     [32]byte{7},
			RentEpoch: math.MaxUint64,
			Data:      make([]byte, 64),
		}
	}
	for i := range keys {
		// Request in a deterministic permutation rather than append order.
		requestIdx := (i * 4051) & (accountCount - 1)
		keys[i] = storedKeys[requestIdx]
	}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: delta}}, 100, nil, nil)
	require.NoError(b, err)

	evict := func() {
		for _, key := range keys {
			db.CommonAcctsCache.Delete(key)
			db.VoteAcctCache.Delete(key)
		}
	}
	resetAdmission := func() {
		db.readCacheEpochMu.Lock()
		db.commonAdmission = newCommonCacheAdmission()
		db.readCacheEpochMu.Unlock()
	}
	mustRead := func() {
		out, err := db.GetAccountsBatch(context.Background(), 100, keys)
		if err != nil || len(out) != len(keys) {
			b.Fatalf("batch: len=%d err=%v", len(out), err)
		}
	}

	b.Run("former-goroutine-per-account", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			evict()
			b.StartTimer()
			out, err := legacyAccountsBatchForBenchmark(context.Background(), db, 100, keys)
			if err != nil || len(out) != len(keys) {
				b.Fatalf("legacy batch: len=%d err=%v", len(out), err)
			}
		}
	})

	b.Run("one-shot-cold", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			evict()
			resetAdmission()
			b.StartTimer()
			mustRead()
		}
	})

	b.Run("second-observation-admit", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			evict()
			resetAdmission()
			mustRead()
			b.StartTimer()
			mustRead()
		}
	})

	b.Run("third-observation-cache-hit", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			evict()
			resetAdmission()
			mustRead()
			mustRead()
			b.StartTimer()
			mustRead()
		}
	})

	b.Run("three-block-amortized", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(3, "blocks/op")
		for range b.N {
			b.StopTimer()
			evict()
			resetAdmission()
			b.StartTimer()
			mustRead()
			mustRead()
			mustRead()
		}
	})
}

func BenchmarkUniqueBatchProof30KCacheHits(b *testing.B) {
	db, _ := newFoldTestDb(b)
	defer db.CloseDb()

	const accountCount = 30_000
	keys := make([]solana.PublicKey, accountCount)
	for i := range keys {
		binary.LittleEndian.PutUint64(keys[i][:8], uint64(i+1))
		acct := &accounts.Account{Key: keys[i], Lamports: uint64(i + 1), Owner: [32]byte{7}}
		if !db.CommonAcctsCache.Set(keys[i], acct) {
			b.Fatalf("cache rejected fixture account %d", i)
		}
	}

	b.Run("defensive-dedupe", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			out, _, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, keys)
			if err != nil {
				b.Fatal(err)
			}
			runtime.KeepAlive(out)
		}
	})
	b.Run("caller-proved-unique", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			out, _, err := db.GetUniqueAccountsBatchSharedWithStats(context.Background(), 100, keys)
			if err != nil {
				b.Fatal(err)
			}
			runtime.KeepAlive(out)
		}
	})
}

func BenchmarkResolveBatchAccountLocations(b *testing.B) {
	db, _ := newFoldTestDb(b)
	defer db.CloseDb()

	const accountCount = 1 << 16
	storedKeys := make([]solana.PublicKey, accountCount)
	mutations := make([]deltaIndexMutation, accountCount)
	for i := range storedKeys {
		binary.BigEndian.PutUint64(storedKeys[i][:8], uint64(i+1))
		entry := AccountIndexEntry{Slot: 100, FileId: 1, Offset: uint64(i)}
		mutations[i] = liveDeltaMutation(storedKeys[i], entry)
	}
	require.NoError(b, db.Index.Apply(mutations, nil, false))

	run := func(b *testing.B, request []solana.PublicKey) {
		b.Helper()
		b.ReportAllocs()
		b.ReportMetric(float64(len(request)), "keys/op")
		validated := false
		for range b.N {
			cold := make([]int, len(request))
			for i := range cold {
				cold[i] = i
			}
			out := make([]*accounts.Account, len(request))
			snapshot := db.Index.NewSnapshot()
			locations, found, err := resolveBatchAccountLocations(
				context.Background(), snapshot, request, cold, out,
			)
			closeErr := snapshot.Close()
			if err != nil || closeErr != nil || len(locations) != len(request) ||
				len(found) != len(request) || !found[0] || !found[len(found)-1] {
				b.Fatalf("resolve: locations=%d found=%d err=%v close=%v", len(locations), len(found), err, closeErr)
			}
			if !validated {
				b.StopTimer()
				for i, ok := range found {
					if !ok {
						b.Fatalf("resolver missed interior result %d", i)
					}
				}
				validated = true
				b.StartTimer()
			}
		}
	}

	for _, size := range []int{1, 2, 4, 8, 16, 64, 256, 1024, 8192, 65536} {
		request := make([]solana.PublicKey, size)
		for i := range request {
			// Spread every request across the fixture's full keyspace. Since 4051
			// is odd, the full-size case remains a complete permutation.
			request[i] = storedKeys[(i*4051)&(accountCount-1)]
		}
		b.Run(fmt.Sprintf("%06d-keys", size), func(b *testing.B) {
			run(b, request)
		})
	}
}

func BenchmarkCommonCacheBulkAdmission(b *testing.B) {
	const accountCount = 20_000
	db := &AccountsDb{}
	db.InitCaches()
	accts := make([]*accounts.Account, accountCount)
	for i := range accts {
		var key solana.PublicKey
		binary.LittleEndian.PutUint32(key[:4], uint32(i+1))
		accts[i] = &accounts.Account{Key: key, Lamports: uint64(i + 1), Owner: [32]byte{7}}
	}

	b.Run("accepted-publication", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for _, acct := range accts {
				db.cacheBatchReadAccount(acct.Key, acct, true, 0)
			}
		}
	})
	b.Run("skip-one-shot-admission", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for _, acct := range accts {
				db.cacheBatchReadAccount(acct.Key, acct, false, 0)
			}
		}
	})
}

// legacyAccountsBatchForBenchmark preserves the previous implementation only
// as a benchmark baseline: one goroutine per cold key, gated by a semaphore.
func legacyAccountsBatchForBenchmark(ctx context.Context, db *AccountsDb, slot uint64, keys []solana.PublicKey) ([]*accounts.Account, error) {
	out := db.getStoreInProgressAccounts(keys)
	sem := make(chan struct{}, runtime.GOMAXPROCS(0)*2)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	for i, key := range keys {
		if out[i] != nil {
			continue
		}
		wg.Add(1)
		go func(idx int, pubkey solana.PublicKey) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := ctx.Err(); err != nil {
				errOnce.Do(func() { firstErr = err })
				return
			}
			acct, err := db.getStoredAccount(slot, pubkey)
			if err != nil && err != ErrNoAccount {
				errOnce.Do(func() { firstErr = err })
				return
			}
			out[idx] = batchAccountOrPlaceholder(pubkey, acct)
		}(i, key)
	}
	wg.Wait()
	return out, firstErr
}
