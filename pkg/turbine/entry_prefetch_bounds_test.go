package turbine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEntryPrefetchByteBoundsFallBackToCompleteVerification(t *testing.T) {
	for _, mode := range []string{"budget_full", "oversized_component"} {
		t.Run(mode, func(t *testing.T) {
			v := newTransactionVerifier(2, 16, nil)
			defer v.closeAndWait()
			a := NewSlotAssembler()
			p := newEntryPrefetchPool(context.Background(), a, v)
			defer p.closeAndWait()
			if mode == "budget_full" {
				// Model other generations owning the entire encoded-byte budget.
				a.mu.Lock()
				p.bytes = entryPrefetchBytes
				a.mu.Unlock()
				defer func() { a.mu.Lock(); p.bytes -= entryPrefetchBytes; a.mu.Unlock() }()
			}
			raw := prefetchTestPayload(t, verifierSignedTransactions(t, 3))
			if mode == "oversized_component" {
				// Ordinary entry components permit trailing FEC padding. The
				// complete decoder must accept this even though prefetch skips it.
				raw = append(raw, make([]byte, entryPrefetchBatchBytes+1-len(raw))...)
			}
			const slot = 400
			batches := prefetchTestShreds(t, slot, raw, buildAlpenglowEndingTick(t))
			require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
			require.Eventually(t, func() bool {
				a.mu.Lock()
				defer a.mu.Unlock()
				f := a.slots[slot].prefetch
				return f != nil && !f.queued && len(f.batches) == 0
			}, 3*time.Second, time.Millisecond)
			if mode == "oversized_component" {
				a.mu.Lock()
				state := a.slots[slot]
				reason, released := state.streamCancelReason, state.prefetch.released
				a.mu.Unlock()
				require.Equal(t, "prefetch_batch_too_large", reason)
				require.True(t, released)
				require.Equal(t, StreamGone, a.StreamStatusOf(StreamGeneration{slot: slot, state: state}))
			}
			blk := feedPrefetchShreds(t, a, batches[1])
			require.NotNil(t, blk)
			require.Len(t, blk.Transactions, 3)
			require.True(t, blk.TransactionSignaturesVerified())
			timings, ok := blk.TurbineIngressTimings()
			require.True(t, ok)
			require.Zero(t, timings.EarlyVerifiedTransactions)
		})
	}
}

// A closed range needs no additional shred to become eligible after another
// generation releases its reservation. Cancellation must not revive stale work.
func TestEntryPrefetchRetriesByteBudgetOnRelease(t *testing.T) {
	for _, cancelWaiting := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelWaiting), func(t *testing.T) {
			v := newTransactionVerifier(2, 16, nil)
			defer v.closeAndWait()
			a := NewSlotAssembler()
			p := newEntryPrefetchPool(context.Background(), a, v)
			defer p.closeAndWait()
			holderCtx, cancel := context.WithCancel(context.Background())
			holder := &slotState{slot: 399, prefetch: &slotEntryPrefetch{pool: p, ctx: holderCtx, cancel: cancel, bytes: entryPrefetchBytes}}
			a.mu.Lock()
			a.slots[399] = holder
			p.slots = 1
			p.bytes = entryPrefetchBytes
			p.active = map[*slotState]struct{}{holder: {}}
			a.mu.Unlock()
			batches := prefetchTestShreds(t, 400, prefetchTestPayload(t, verifierSignedTransactions(t, 3)), buildAlpenglowEndingTick(t))
			require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
			require.Eventually(t, func() bool {
				a.mu.Lock()
				defer a.mu.Unlock()
				f := a.slots[400].prefetch
				return f != nil && f.budgetBlocked && !f.queued
			}, 3*time.Second, time.Millisecond)
			if cancelWaiting {
				a.ResetSlot(400)
			}
			a.ResetSlot(399)
			if cancelWaiting {
				require.Eventually(t, func() bool {
					a.mu.Lock()
					defer a.mu.Unlock()
					return p.slots == 0 && p.bytes == 0 && len(p.active) == 0
				}, 3*time.Second, time.Millisecond)
			} else {
				batch := waitPrefetchedBatch(t, a, 400, 0)
				_, err := batch.verification.wait()
				require.NoError(t, err)
				require.Len(t, entryBatchTransactions(batch.entries), 3)
			}
		})
	}
}

func TestEntryPrefetchOversizedRangeAfterVerifiedPrefix(t *testing.T) {
	v := newTransactionVerifier(2, 16, nil)
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	raw := prefetchTestPayload(t, verifierSignedTransactions(t, 3))
	large := prefetchTestPayload(t, verifierSignedTransactions(t, 3))
	large = append(large, make([]byte, entryPrefetchBatchBytes+1-len(large))...)
	batches := prefetchTestShreds(t, 401, raw, large, buildAlpenglowEndingTick(t))
	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	prefix := waitPrefetchedBatch(t, a, 401, 0)
	_, err := prefix.verification.wait()
	require.NoError(t, err)
	require.Nil(t, feedPrefetchShreds(t, a, batches[1]))
	require.Eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.slots[401].prefetch.released
	}, 3*time.Second, time.Millisecond)
	blk := feedPrefetchShreds(t, a, batches[2])
	require.NotNil(t, blk)
	require.Len(t, blk.Transactions, 6)
	require.True(t, blk.TransactionSignaturesVerified())
	timings, ok := blk.TurbineIngressTimings()
	require.True(t, ok)
	require.Zero(t, timings.EarlyVerifiedTransactions, "cancelled prefix cache is not reused")
}
