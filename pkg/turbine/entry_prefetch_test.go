package turbine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func prefetchTestPayload(t *testing.T, txs []*solana.Transaction) []byte {
	t.Helper()
	entry := Entry{NumHashes: 1, Hash: solana.Hash{7}, Txns: make([]solana.Transaction, len(txs))}
	for i, tx := range txs {
		entry.Txns[i] = *tx
	}
	raw, err := marshalEntryBatch([]Entry{entry})
	require.NoError(t, err)
	return raw
}

func prefetchTestShreds(t *testing.T, slot uint64, payloads ...[]byte) [][]*Shred {
	t.Helper()
	gen := ShredGenerator{Slot: slot, ParentSlot: slot - 1, Version: 1}
	var nextData, nextCode uint32
	var root solana.Hash
	batches := make([][]*Shred, len(payloads))
	for i, raw := range payloads {
		packets, nextRoot, data, code, err := gen.MakeShredsFromData(testShredLeader(t), raw, i == len(payloads)-1, root, nextData, nextCode)
		require.NoError(t, err)
		root, nextData, nextCode = nextRoot, data, code
		for _, packet := range packets {
			shred, err := ParseShred(packet)
			require.NoError(t, err)
			if shred.Type == ShredTypeData {
				batches[i] = append(batches[i], shred)
			}
		}
	}
	return batches
}

func feedPrefetchShreds(t *testing.T, a *SlotAssembler, shreds []*Shred) *block.Block {
	t.Helper()
	var result *block.Block
	for _, shred := range shreds {
		blk, err := a.AddShred(shred)
		require.NoError(t, err)
		if blk != nil {
			require.Nil(t, result)
			result = blk
		}
	}
	return result
}

func waitPrefetchedBatch(t *testing.T, a *SlotAssembler, slot uint64, start uint32) *prefetchedShredBatch {
	t.Helper()
	var batch *prefetchedShredBatch
	require.Eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		if s := a.slots[slot]; s != nil && s.prefetch != nil {
			batch = s.prefetch.batches[start]
		}
		return batch != nil
	}, 3*time.Second, time.Millisecond)
	waitSignal(t, batch.ready, "prefetched component preparation")
	require.NoError(t, batch.err)
	require.NoError(t, batch.submitErr)
	return batch
}

func TestEntryPrefetchVerifiesBeforeLastShredAndReusesResults(t *testing.T) {
	var calls atomic.Int32
	v := newTransactionVerifier(2, 16, func(tx *solana.Transaction) error {
		calls.Add(1)
		return txverify.VerifyTransaction(tx)
	})
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	const slot = 100
	batches := prefetchTestShreds(t, slot,
		prefetchTestPayload(t, verifierSignedTransactions(t, 3)),
		prefetchTestPayload(t, verifierSignedTransactions(t, 4)))
	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	cached := waitPrefetchedBatch(t, a, slot, 0)
	require.NotNil(t, cached.verification)
	_, err := cached.verification.wait()
	require.NoError(t, err)
	require.Equal(t, int32(3), calls.Load())
	for range 5 {
		require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	}
	blk := feedPrefetchShreds(t, a, batches[1])
	require.NotNil(t, blk)
	require.Len(t, blk.Transactions, 7)
	require.True(t, blk.TransactionSignaturesVerified())
	require.Equal(t, int32(7), calls.Load(), "cached transactions must not be verified twice")
	timings, ok := blk.TurbineIngressTimings()
	require.True(t, ok)
	require.Equal(t, uint64(3), timings.EarlyVerifiedTransactions)
	require.LessOrEqual(t, cached.verification.finishedAt.UnixNano(), blk.ShredFullNanos)
}

func TestEntryPrefetchWaitsForGapAcrossMultipleFECSets(t *testing.T) {
	var calls atomic.Int32
	v := newTransactionVerifier(2, 16, func(*solana.Transaction) error { calls.Add(1); return nil })
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	const slot = 104
	batches := prefetchTestShreds(t, slot,
		prefetchTestPayload(t, verifierSignedTransactions(t, 300)), buildAlpenglowEndingTick(t))
	require.Greater(t, len(batches[0]), dataShredsPerFECBlock)
	// Arrival of DATA_COMPLETE and later FEC sets cannot bypass the hole.
	for i := len(batches[0]) - 1; i >= 0; i-- {
		if i != 1 {
			require.Nil(t, feedPrefetchShreds(t, a, batches[0][i:i+1]))
		}
	}
	a.mu.Lock()
	require.Empty(t, a.slots[slot].completeBatches)
	require.Nil(t, a.slots[slot].prefetch)
	a.mu.Unlock()
	require.Zero(t, calls.Load())
	require.Nil(t, feedPrefetchShreds(t, a, batches[0][1:2]))
	cached := waitPrefetchedBatch(t, a, slot, 0)
	_, err := cached.verification.wait()
	require.NoError(t, err)
	require.Equal(t, int32(300), calls.Load())
	blk := feedPrefetchShreds(t, a, batches[1])
	require.NotNil(t, blk)
	require.Equal(t, int32(300), calls.Load())
}

func TestEntryPrefetchResetKeepsOldReservationsUntilReadersJoin(t *testing.T) {
	txs := verifierSignedTransactions(t, 2)
	txs[0].Signatures[0][9] ^= 0x40
	oldSignature := txs[0].Signatures[0]
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	v := newTransactionVerifier(1, 8, func(tx *solana.Transaction) error {
		if tx.Signatures[0] == oldSignature {
			close(started)
			<-release
		}
		return txverify.VerifyTransaction(tx)
	})
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	defer releaseOnce.Do(func() { close(release) })
	const slot = 108
	old := prefetchTestShreds(t, slot, prefetchTestPayload(t, txs[:1]), buildAlpenglowEndingTick(t))
	fresh := prefetchTestShreds(t, slot, prefetchTestPayload(t, txs[1:]), buildAlpenglowEndingTick(t))
	require.Nil(t, feedPrefetchShreds(t, a, old[0]))
	waitSignal(t, started, "old generation verifier")
	oldBatch := waitPrefetchedBatch(t, a, slot, 0)
	a.ResetSlot(slot)
	a.mu.Lock()
	require.Equal(t, 1, p.slots)
	require.Positive(t, p.bytes)
	a.mu.Unlock()
	require.Nil(t, feedPrefetchShreds(t, a, fresh[0]))
	newBatch := waitPrefetchedBatch(t, a, slot, 0)
	require.NotSame(t, oldBatch, newBatch)
	a.mu.Lock()
	require.Equal(t, 2, p.slots)
	a.mu.Unlock()
	releaseOnce.Do(func() { close(release) })
	_, err := oldBatch.verification.wait()
	require.ErrorIs(t, err, context.Canceled)
	_, err = newBatch.verification.wait()
	require.NoError(t, err)
	blk := feedPrefetchShreds(t, a, fresh[1])
	require.NotNil(t, blk)
	require.Equal(t, txs[1].Signatures[0], blk.Transactions[0].Signatures[0])
	require.Eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return p.slots == 0 && p.bytes == 0
	}, 3*time.Second, time.Millisecond)
}

func TestEntryPrefetchSaturationDoesNotBlockShredAdmissionAndShutdownJoins(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var first, releaseOnce sync.Once
	v := newTransactionVerifier(1, 8, func(*solana.Transaction) error {
		first.Do(func() { close(started) })
		<-release
		return nil
	})
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	defer releaseOnce.Do(func() { close(release) })
	payload := prefetchTestPayload(t, verifierSignedTransactions(t, 1))
	for i := 0; i < entryPrefetchSlots+1; i++ {
		batches := prefetchTestShreds(t, uint64(200+i), payload, buildAlpenglowEndingTick(t))
		admitted := make(chan struct{})
		go func() {
			defer close(admitted)
			for _, sh := range batches[0] {
				_, err := a.addShredFrom(sh, false)
				if err != nil {
					t.Errorf("admit shred: %v", err)
				}
			}
		}()
		waitSignal(t, admitted, "nonblocking ingress while prefetch saturated")
	}
	waitSignal(t, started, "blocked verifier")
	a.mu.Lock()
	require.Equal(t, entryPrefetchSlots, p.slots)
	require.LessOrEqual(t, p.bytes, entryPrefetchBytes)
	require.Nil(t, a.slots[200+entryPrefetchSlots].prefetch)
	a.mu.Unlock()
	done := make(chan struct{})
	go func() { p.closeAndWait(); close(done) }()
	select {
	case <-done:
		t.Fatal("early pool released buffers before signature worker joined")
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	waitSignal(t, done, "saturated pool shutdown")
	a.mu.Lock()
	require.Zero(t, p.slots)
	require.Zero(t, p.bytes)
	require.Nil(t, a.entryPrefetch)
	a.mu.Unlock()
}

func TestEntryPrefetchInvalidRetainedTransactionFailsClosed(t *testing.T) {
	txs := verifierSignedTransactions(t, 3)
	txs[1].Signatures[0][3] ^= 0x80
	v := newTransactionVerifier(2, 16, nil)
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	batches := prefetchTestShreds(t, 300, prefetchTestPayload(t, txs), buildAlpenglowEndingTick(t))
	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	cached := waitPrefetchedBatch(t, a, 300, 0)
	index, err := cached.verification.wait()
	require.Equal(t, 1, index)
	require.ErrorContains(t, err, "invalid signature")
	var finalErr error
	for _, sh := range batches[1] {
		blk, err := a.AddShred(sh)
		require.Nil(t, blk)
		if err != nil {
			finalErr = err
		}
	}
	require.ErrorContains(t, finalErr, "transaction 1")
	require.False(t, a.SlotCompleted(300))
}

func TestEntryPrefetchUpdateParentDiscardsInvalidOptimisticPrefix(t *testing.T) {
	txs := verifierSignedTransactions(t, 2)
	txs[0].Signatures[0][3] ^= 0x80
	v := newTransactionVerifier(2, 16, nil)
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	const slot = 304
	batches := prefetchTestShreds(t, slot,
		prefetchTestPayload(t, txs[:1]),
		testAlpenglowParentMarkerBytes(blockMarkerVariantUpdateParent, slot-2, solana.Hash{12}),
		prefetchTestPayload(t, txs[1:]))
	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	cached := waitPrefetchedBatch(t, a, slot, 0)
	_, err := cached.verification.wait()
	require.ErrorContains(t, err, "invalid signature")
	require.Nil(t, feedPrefetchShreds(t, a, batches[1]))
	blk := feedPrefetchShreds(t, a, batches[2])
	require.NotNil(t, blk)
	require.Len(t, blk.Transactions, 1)
	require.Equal(t, txs[1].Signatures[0], blk.Transactions[0].Signatures[0])
	require.Equal(t, uint64(slot-2), blk.SourceParentSlot)
	require.True(t, blk.TransactionSignaturesVerified())
}

func TestEntryPrefetchCanceledCompletionCanRetrySameGeneration(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var first, releaseOnce sync.Once
	v := newTransactionVerifier(1, 8, func(tx *solana.Transaction) error {
		first.Do(func() { close(started); <-release })
		return txverify.VerifyTransaction(tx)
	})
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	defer releaseOnce.Do(func() { close(release) })
	const slot = 308
	batches := prefetchTestShreds(t, slot,
		prefetchTestPayload(t, verifierSignedTransactions(t, 3)), buildAlpenglowEndingTick(t))
	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	waitSignal(t, started, "early verification")
	cached := waitPrefetchedBatch(t, a, slot, 0)
	var work *slotCompletionWork
	for _, sh := range batches[1] {
		var err error
		work, err = a.addShredFrom(sh, false)
		require.NoError(t, err)
	}
	require.NotNil(t, work)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceled := make(chan struct{})
	originalCancel := cached.verification.cancel
	cached.verification.cancel = func() { close(canceled); originalCancel() }
	done := make(chan processedSlotCompletion, 1)
	go func() { done <- a.processCompletion(ctx, work) }()
	// The completion has no expensive decode left and blocks joining this
	// one already-prepared future; cancel while it owns those transactions.
	time.Sleep(20 * time.Millisecond)
	cancel()
	waitSignal(t, canceled, "completion canceled its signature request")
	releaseOnce.Do(func() { close(release) })
	var processed processedSlotCompletion
	select {
	case processed = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("canceled completion failed to join")
	}
	require.True(t, processed.canceled)
	_, err := a.finalizeCompletion(work, processed)
	require.NoError(t, err)
	_, err = cached.verification.wait()
	require.True(t, errors.Is(err, context.Canceled))
	a.mu.Lock()
	retry := a.claimCompletionLocked(a.slots[slot], false)
	a.mu.Unlock()
	require.NotNil(t, retry)
	processed = a.processCompletion(context.Background(), retry)
	require.NoError(t, processed.err)
	blk, err := a.finalizeCompletion(retry, processed)
	require.NoError(t, err)
	require.NotNil(t, blk)
	require.True(t, blk.TransactionSignaturesVerified())
}
