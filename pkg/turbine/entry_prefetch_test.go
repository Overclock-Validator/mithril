package turbine

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
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
	a.mu.Lock()
	require.Equal(t, 2, p.slots)
	a.mu.Unlock()
	// With one verifier worker only one request may prefetch. The fresh
	// generation keeps its pool reservation while admission waits for the old
	// reader to join; it must not release or reuse the old generation's bytes.
	releaseOnce.Do(func() { close(release) })
	_, err := oldBatch.verification.wait()
	require.ErrorIs(t, err, context.Canceled)
	newBatch := waitPrefetchedBatch(t, a, slot, 0)
	require.NotSame(t, oldBatch, newBatch)
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
	p.cleanup.Wait()
	a.mu.Lock()
	g := StreamGeneration{slot: 300, state: a.slots[300]}
	slots, bytes := p.slots, p.bytes
	a.mu.Unlock()
	require.Equal(t, StreamGone, a.StreamStatusOf(g))
	require.Zero(t, slots)
	require.Zero(t, bytes)
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
	require.Eventually(t, func() bool {
		stack := make([]byte, 2<<20)
		for _, goroutine := range strings.Split(string(stack[:runtime.Stack(stack, true)]), "\n\n") {
			if strings.Contains(goroutine, "(*SlotAssembler).processCompletion") && strings.Contains(goroutine, "(*transactionVerification).waitContext") {
				return true
			}
		}
		return false
	}, 3*time.Second, time.Millisecond, "completion must enter the owning verification join")
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

// A complete later batch must verify while an earlier batch still has a gap.
// Its preceding DATA_COMPLETE shred remains necessary to establish its start.
func TestEntryPrefetchBypassesEarlierGap(t *testing.T) {
	for _, lateBoundary := range []bool{false, true} {
		t.Run(fmt.Sprint("lateBoundary=", lateBoundary), func(t *testing.T) {
			var calls atomic.Int32
			v := newTransactionVerifier(2, 8, func(tx *solana.Transaction) error { calls.Add(1); return txverify.VerifyTransaction(tx) })
			defer v.closeAndWait()
			a := NewSlotAssembler()
			p := newEntryPrefetchPool(context.Background(), a, v)
			defer p.closeAndWait()
			const slot = 909
			txs := verifierSignedTransactions(t, 60)
			batches := prefetchTestShreds(t, slot, prefetchTestPayload(t, txs[:30]), prefetchTestPayload(t, txs[30:]), buildAlpenglowEndingTick(t))
			require.Greater(t, len(batches[0]), 2)
			end := len(batches[0]) - 1
			for i, sh := range batches[0] {
				if i == 1 || (lateBoundary && i == end) {
					continue
				}
				require.Nil(t, feedPrefetchShreds(t, a, []*Shred{sh}))
			}
			require.Nil(t, feedPrefetchShreds(t, a, batches[1]))
			if lateBoundary {
				a.mu.Lock()
				empty := len(a.slots[slot].completeBatches) == 0
				a.mu.Unlock()
				require.True(t, empty, "unknown preceding boundary must prevent speculation")
				require.Nil(t, feedPrefetchShreds(t, a, batches[0][end:]))
			}
			later := waitPrefetchedBatch(t, a, slot, batches[1][0].Index)
			_, err := later.verification.wait()
			require.NoError(t, err)
			require.Equal(t, int32(30), calls.Load())
			require.Nil(t, feedPrefetchShreds(t, a, batches[0][1:2]))
			earlier := waitPrefetchedBatch(t, a, slot, 0)
			_, err = earlier.verification.wait()
			require.NoError(t, err)
			blk := feedPrefetchShreds(t, a, batches[2])
			require.NotNil(t, blk)
			require.True(t, blk.TransactionSignaturesVerified())
			require.Len(t, blk.Transactions, 60)
			require.Equal(t, int32(60), calls.Load(), "every signature verified exactly once")
			for i, tx := range txs {
				require.Equal(t, tx.Signatures[0], blk.Transactions[i].Signatures[0])
			}
		})
	}
}

func TestEntryPrefetchDiscoversRecoveredBoundary(t *testing.T) {
	v := newTransactionVerifier(2, 8, nil)
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	const slot = 910
	txs := verifierSignedTransactions(t, 60)
	gen := ShredGenerator{Slot: slot, ParentSlot: slot - 1, Version: 1}
	raw := prefetchTestPayload(t, txs[:30])
	packets, root, nextData, nextCode, err := gen.MakeShredsFromData(testShredLeader(t), raw, false, solana.Hash{}, 0, 0)
	require.NoError(t, err)
	var code []*Shred
	for _, packet := range packets {
		sh, err := ParseShred(packet)
		require.NoError(t, err)
		if sh.Type == ShredTypeCode {
			code = append(code, sh)
			continue
		}
		if !sh.DataComplete() {
			require.Nil(t, feedPrefetchShreds(t, a, []*Shred{sh}))
		}
	}
	packets, _, _, _, err = gen.MakeShredsFromData(testShredLeader(t), prefetchTestPayload(t, txs[30:]), false, root, nextData, nextCode)
	require.NoError(t, err)
	for _, packet := range packets {
		sh, err := ParseShred(packet)
		require.NoError(t, err)
		if sh.Type == ShredTypeData {
			require.Nil(t, feedPrefetchShreds(t, a, []*Shred{sh}))
		}
	}
	a.mu.Lock()
	empty := len(a.slots[slot].completeBatches) == 0
	a.mu.Unlock()
	require.True(t, empty)
	require.NotEmpty(t, code)
	for _, sh := range code {
		require.Nil(t, feedPrefetchShreds(t, a, []*Shred{sh}))
	}
	later := waitPrefetchedBatch(t, a, slot, nextData)
	_, err = later.verification.wait()
	require.NoError(t, err)
	first := waitPrefetchedBatch(t, a, slot, 0)
	_, err = first.verification.wait()
	require.NoError(t, err)
}

func TestEntryPrefetchIndexDisabledAndLateInstall(t *testing.T) {
	a := NewSlotAssembler()
	batches := prefetchTestShreds(t, 911, prefetchTestPayload(t, verifierSignedTransactions(t, 3)), buildAlpenglowEndingTick(t))
	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	a.mu.Lock()
	absent := a.slots[911].batchIndex == nil
	a.mu.Unlock()
	require.True(t, absent)
	v := newTransactionVerifier(2, 8, nil)
	defer v.closeAndWait()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	// Seeding also works on a coding-only admission with no new data recovery.
	a.mu.Lock()
	a.prefetchEntriesLocked(a.slots[911])
	a.mu.Unlock()
	cached := waitPrefetchedBatch(t, a, 911, 0)
	_, err := cached.verification.wait()
	require.NoError(t, err)
}

func TestEntryPrefetchResetRetainsQueuedReservations(t *testing.T) {
	a := NewSlotAssembler()
	ctx, cancel := context.WithCancel(context.Background())
	// Hold workers until after reset so all tokens remain in the channel.
	p := &entryPrefetchPool{a: a, ctx: ctx, cancel: cancel, jobs: make(chan *slotState, entryPrefetchSlots)}
	a.entryPrefetch = p
	startWorker := sync.OnceFunc(func() {
		p.workers.Add(1)
		go p.run()
	})
	defer func() {
		startWorker()
		p.closeAndWait()
	}()
	for i := 0; i < entryPrefetchSlots; i++ {
		s := &slotState{slot: uint64(i), completeBatches: []shredBatchRange{{0, 0}}}
		a.mu.Lock()
		a.slots[s.slot] = s
		a.prefetchEntriesLocked(s)
		a.releasePrefetchLocked(s)
		a.mu.Unlock()
	}
	require.Equal(t, entryPrefetchSlots, len(p.jobs))
	// Cleanup must not admit another generation while stale queue tokens live.
	require.Never(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return p.slots != entryPrefetchSlots
	}, 50*time.Millisecond, time.Millisecond)
	fresh := &slotState{slot: 100, completeBatches: []shredBatchRange{{0, 0}}}
	a.mu.Lock()
	a.prefetchEntriesLocked(fresh)
	reserved := fresh.prefetch != nil
	a.mu.Unlock()
	// Start the worker before assertions so test failures cannot strand cleanup.
	startWorker()
	require.False(t, reserved)
	p.cleanup.Wait()
	a.mu.Lock()
	slots := p.slots
	a.mu.Unlock()
	require.Zero(t, slots)
}

// Failed full blocks remain available for diagnostics, but must not consume
// the prefetch budget or remain usable streaming generations until retention.
func TestEntryPrefetchFailedCompletionReleasesCapacity(t *testing.T) {
	for _, dropEvents := range []bool{false, true} {
		t.Run(fmt.Sprintf("drop_events=%v", dropEvents), func(t *testing.T) {
			v := newTransactionVerifier(2, 16, nil)
			defer v.closeAndWait()
			a := NewSlotAssembler()
			p := newEntryPrefetchPool(context.Background(), a, v)
			defer p.closeAndWait()
			capacity := 16
			if dropEvents {
				capacity = 0
			}
			events := make(chan StreamEvent, capacity)
			a.SubscribeStream(events)
			payload := prefetchTestPayload(t, verifierSignedTransactions(t, 1))
			for i := 0; i <= entryPrefetchSlots; i++ {
				slot := uint64(900 + i)
				// Zero entry count plus trailing bytes is an invalid component.
				batches := prefetchTestShreds(t, slot, payload, make([]byte, 16))
				require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
				batch := waitPrefetchedBatch(t, a, slot, 0)
				_, err := batch.verification.wait()
				require.NoError(t, err)
				a.mu.Lock()
				s := a.slots[slot]
				g := StreamGeneration{slot: slot, state: s}
				a.mu.Unlock()
				var failure error
				for _, sh := range batches[1] {
					blk, err := a.AddShred(sh)
					require.Nil(t, blk)
					if err != nil {
						failure = err
					}
				}
				require.Error(t, failure)
				count, last := a.SlotAssemblyErrors(slot)
				require.Positive(t, count)
				require.Equal(t, failure.Error(), last)
				require.False(t, a.SlotCompleted(slot))
				require.Equal(t, StreamGone, a.StreamStatusOf(g))
				require.Empty(t, a.PendingStreamBatches(g, 0))
				if !dropEvents {
					event := nextStreamEvent(t, events, StreamCancelled)
					require.Equal(t, g, event.Generation)
					require.Equal(t, "completion_failed", event.Reason)
				}
				p.cleanup.Wait()
				a.mu.Lock()
				retained, slots, bytes := a.slots[slot], p.slots, p.bytes
				// Repeated release/admission cannot double-refund or resurrect it.
				a.releasePrefetchLocked(s)
				a.prefetchEntriesLocked(s)
				afterSlots := p.slots
				a.mu.Unlock()
				require.Same(t, s, retained, "preserve poisoned-slot diagnostics")
				require.Zero(t, slots)
				require.Zero(t, bytes)
				require.Zero(t, afterSlots)
			}
			good := prefetchTestShreds(t, 920, payload, buildAlpenglowEndingTick(t))
			require.Nil(t, feedPrefetchShreds(t, a, good[0]))
			waitPrefetchedBatch(t, a, 920, 0)
			blk := feedPrefetchShreds(t, a, good[1])
			require.NotNil(t, blk)
			require.True(t, blk.TransactionSignaturesVerified())
		})
	}
}

func TestEntryPrefetchFailedCompletionJoinsReaders(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	v := newTransactionVerifier(1, 8, func(*solana.Transaction) error {
		close(started)
		<-release
		return nil
	})
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	defer once.Do(func() { close(release) })
	batches := prefetchTestShreds(t, 930, prefetchTestPayload(t, verifierSignedTransactions(t, 1)), make([]byte, 16))
	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	waitSignal(t, started, "prefetch verifier")
	waitPrefetchedBatch(t, a, 930, 0)
	var failure error
	for _, sh := range batches[1] {
		blk, err := a.AddShred(sh)
		require.Nil(t, blk)
		if err != nil {
			failure = err
		}
	}
	require.Error(t, failure)
	a.mu.Lock()
	s := a.slots[930]
	released, ctxErr, slots, bytes := s.prefetch.released, s.prefetch.ctx.Err(), p.slots, p.bytes
	a.mu.Unlock()
	require.True(t, released)
	require.ErrorIs(t, ctxErr, context.Canceled)
	require.Equal(t, 1, slots, "reader still owns the reservation")
	require.Positive(t, bytes)
	require.Equal(t, StreamGone, a.StreamStatusOf(StreamGeneration{slot: 930, state: s}))
	once.Do(func() { close(release) })
	p.cleanup.Wait()
	a.mu.Lock()
	slots, bytes = p.slots, p.bytes
	a.mu.Unlock()
	require.Zero(t, slots)
	require.Zero(t, bytes)
}

// A failed generation that never received a reservation must not acquire one
// later when capacity becomes available (or prefetch is attached).
func TestEntryPrefetchFailedCompletionWithoutReservation(t *testing.T) {
	a := NewSlotAssembler()
	batches := prefetchTestShreds(t, 940, prefetchTestPayload(t, verifierSignedTransactions(t, 1)), make([]byte, 16))
	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	var failure error
	for _, sh := range batches[1] {
		blk, err := a.AddShred(sh)
		require.Nil(t, blk)
		if err != nil {
			failure = err
		}
	}
	require.Error(t, failure)
	v := newTransactionVerifier(1, 8, nil)
	defer v.closeAndWait()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	a.mu.Lock()
	s := a.slots[940]
	a.prefetchEntriesLocked(s)
	reserved, slots := s.prefetch, p.slots
	a.mu.Unlock()
	require.Nil(t, reserved)
	require.Zero(t, slots)
	require.Equal(t, StreamGone, a.StreamStatusOf(StreamGeneration{slot: 940, state: s}))
}
