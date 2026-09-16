package turbine

import (
	"context"
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
