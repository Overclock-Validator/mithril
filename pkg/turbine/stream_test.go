package turbine

import (
	"context"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func nextStreamEvent(t *testing.T, ch <-chan StreamEvent, kind StreamEventKind) StreamEvent {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case event := <-ch:
			if event.Kind == kind {
				return event
			}
		case <-deadline:
			t.Fatalf("no stream event of kind %d", kind)
		}
	}
}

// The feed publishes each prefetched batch once it is decoded — the header
// marker with its parent identity, then entry batches whose transactions are
// the very objects the completed block references — and ends with a
// completion event for the same generation. The final component (the ending
// tick) is decoded by completion, never by the prefetch, so it is not fed.
func TestStreamFeedPublishesBatchesAndCompletion(t *testing.T) {
	// The production verifier (nil hook) is the one that attaches message
	// identities; a per-transaction hook verifies without producing them.
	v := newTransactionVerifier(2, 16, nil)
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	events := make(chan StreamEvent, 64)
	a.SubscribeStream(events)

	const slot = 300
	parentID := solana.Hash{9, 9, 9}
	batches := prefetchTestShreds(t, slot,
		testAlpenglowParentMarkerBytes(blockMarkerVariantHeader, slot-1, parentID),
		prefetchTestPayload(t, verifierSignedTransactions(t, 3)),
		prefetchTestPayload(t, verifierSignedTransactions(t, 4)),
		buildAlpenglowEndingTick(t))
	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	header := nextStreamEvent(t, events, StreamBatchReady)
	require.Equal(t, uint64(slot), header.Slot)
	require.False(t, header.Generation.IsZero())
	require.Equal(t, StreamMarkerHeader, header.Batch.Marker)
	require.Equal(t, uint64(slot-1), header.Batch.ParentSlot)
	require.Equal(t, parentID, header.Batch.ParentBlockID)
	require.Empty(t, header.Batch.Transactions)
	_, verified, err := header.Batch.WaitVerification(context.Background())
	require.NoError(t, err)
	require.False(t, verified, "markers carry no verification")

	require.Nil(t, feedPrefetchShreds(t, a, batches[1]))
	first := nextStreamEvent(t, events, StreamBatchReady)
	require.Equal(t, header.Generation, first.Generation)
	require.Equal(t, batches[1][0].Index, first.Batch.Start)
	require.Equal(t, StreamMarkerNone, first.Batch.Marker)
	require.Len(t, first.Batch.Transactions, 3)
	require.NoError(t, first.Batch.Err)
	require.Equal(t, StreamActive, a.StreamStatusOf(first.Generation))

	identities, verified, err := first.Batch.WaitVerification(context.Background())
	require.NoError(t, err)
	require.True(t, verified)
	require.Len(t, identities, 3)
	prepared, err := block.PrepareVerifiedTransactionMessageIdentities(first.Batch.Transactions, identities)
	require.NoError(t, err)
	require.Equal(t, 3, prepared.Len())

	// Recovery path: the pending list must show the same batches by range.
	pending := a.PendingStreamBatches(first.Generation, 0)
	require.Len(t, pending, 2)
	require.Equal(t, header.Batch.Start, pending[0].Start)
	require.Equal(t, first.Batch.Start, pending[1].Start)
	require.Equal(t, first.Batch.End, pending[1].End)
	require.Empty(t, a.PendingStreamBatches(first.Generation, first.Batch.End+1))

	require.Nil(t, feedPrefetchShreds(t, a, batches[2]))
	second := nextStreamEvent(t, events, StreamBatchReady)
	require.Equal(t, first.Generation, second.Generation)
	require.Len(t, second.Batch.Transactions, 4)
	// Make sure the prefetch has retained both entry batches before the last
	// component completes the slot; completion then reuses them by identity.
	waitPrefetchedBatch(t, a, slot, second.Batch.Start)

	blk := feedPrefetchShreds(t, a, batches[3])
	require.NotNil(t, blk)
	done := nextStreamEvent(t, events, StreamCompleted)
	require.Equal(t, first.Generation, done.Generation)
	require.Equal(t, StreamDone, a.StreamStatusOf(first.Generation))
	require.Empty(t, a.PendingStreamBatches(first.Generation, 0), "a completed generation has nothing pending")

	// Pointer identity: the prefix a streaming consumer executed is the block.
	require.Len(t, blk.Transactions, 7)
	for i, tx := range first.Batch.Transactions {
		require.Same(t, tx, blk.Transactions[i])
	}
	for i, tx := range second.Batch.Transactions {
		require.Same(t, tx, blk.Transactions[3+i])
	}
	require.Equal(t, uint64(slot-1), blk.SourceParentSlot)
	require.True(t, blk.HasAlpenglowParentBlockID)
	require.Equal(t, parentID, solana.Hash(blk.AlpenglowParentBlockID))
	require.Zero(t, a.StreamDroppedEvents())
}

// A reset while a slot is streaming cancels the generation; re-assembling the
// slot produces a different generation.
func TestStreamFeedCancelsOnResetAndRenewsGeneration(t *testing.T) {
	v := newTransactionVerifier(2, 16, func(*solana.Transaction) error { return nil })
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	events := make(chan StreamEvent, 64)
	a.SubscribeStream(events)

	const slot = 301
	batches := prefetchTestShreds(t, slot,
		prefetchTestPayload(t, verifierSignedTransactions(t, 2)),
		prefetchTestPayload(t, verifierSignedTransactions(t, 2)))
	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	first := nextStreamEvent(t, events, StreamBatchReady)

	a.ResetSlot(slot)
	cancelled := nextStreamEvent(t, events, StreamCancelled)
	require.Equal(t, first.Generation, cancelled.Generation)
	require.Equal(t, "reset", cancelled.Reason)
	require.Equal(t, StreamGone, a.StreamStatusOf(first.Generation))
	require.Empty(t, a.PendingStreamBatches(first.Generation, 0))

	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	renewed := nextStreamEvent(t, events, StreamBatchReady)
	require.NotEqual(t, first.Generation, renewed.Generation)
	require.Equal(t, StreamActive, a.StreamStatusOf(renewed.Generation))
}

// Dropped wake-ups are counted and never lose state: the batches remain
// discoverable through PendingStreamBatches.
func TestStreamFeedDropsWakeupsWhenSubscriberIsFull(t *testing.T) {
	v := newTransactionVerifier(2, 16, func(*solana.Transaction) error { return nil })
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	events := make(chan StreamEvent) // unbuffered and never drained: every send drops
	a.SubscribeStream(events)

	const slot = 302
	batches := prefetchTestShreds(t, slot,
		prefetchTestPayload(t, verifierSignedTransactions(t, 2)),
		prefetchTestPayload(t, verifierSignedTransactions(t, 2)))
	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	cached := waitPrefetchedBatch(t, a, slot, 0)
	require.NotNil(t, cached)
	require.Eventually(t, func() bool { return a.StreamDroppedEvents() >= 1 }, 3*time.Second, time.Millisecond)

	a.mu.Lock()
	state := a.slots[slot]
	a.mu.Unlock()
	g := StreamGeneration{slot: slot, state: state}
	pending := a.PendingStreamBatches(g, 0)
	require.Len(t, pending, 1)
	require.Len(t, pending[0].Transactions, 2)
}
