package replay

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/stretchr/testify/require"
)

func TestPreparedStatusDeltaRebindsSnapshotOffsets(t *testing.T) {
	for _, from := range []uint64{0, 7, txstatus.MaxCachedKeyIndex} {
		for _, to := range []uint64{0, 7, txstatus.MaxCachedKeyIndex} {
			t.Run(fmt.Sprintf("%d_to_%d", from, to), func(t *testing.T) {
				ancestor := statusCacheTestTransaction(1, 2, 3)
				seed := func(offset uint64) *TransactionStatusCache {
					cache, err := NewTransactionStatusCacheFromAgaveSnapshot([]txstatus.SnapshotSlotDelta{
						{Slot: 0, IsRoot: true, Statuses: []txstatus.SnapshotStatus{snapshotStatusCacheStatusForTx(t, ancestor, offset)}},
					}, 0)
					require.NoError(t, err)
					return cache
				}
				candidate := statusCacheTestBlock(1, statusCacheTestTransaction(1, 4, 5))
				plan, err := planBlockTransactionExecution(candidate)
				require.NoError(t, err)
				prepared := seed(from).prepareTransactionStatusDelta(plan.messageIdentities)
				cache := seed(to)
				pinned := cache.View()
				require.NoError(t, cache.commitBlockWithPreparedDelta(candidate, plan, prepared))
				require.Equal(t, uint8(to), cache.tip.delta[candidate.Transactions[0].Message.RecentBlockhash].keyIndex)
				found, err := cache.View().ContainsTransaction(candidate.Transactions[0])
				require.NoError(t, err)
				require.True(t, found)
				found, err = pinned.ContainsTransaction(candidate.Transactions[0])
				require.NoError(t, err)
				require.False(t, found)
				blob, err := cache.SnapshotThrough(1)
				require.NoError(t, err)
				restored, err := NewTransactionStatusCacheFromSnapshot(blob)
				require.NoError(t, err)
				found, err = restored.View().ContainsTransaction(candidate.Transactions[0])
				require.NoError(t, err)
				require.True(t, found)
				require.NoError(t, cache.Unwind(1))
				found, err = cache.View().ContainsTransaction(candidate.Transactions[0])
				require.NoError(t, err)
				require.False(t, found)
				found, err = cache.View().ContainsTransaction(ancestor)
				require.NoError(t, err)
				require.True(t, found)
			})
		}
	}
}

func TestPreparedStatusDeltaDoesNotPublishUntilCommit(t *testing.T) {
	prior := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(prior)
	cache := NewTransactionStatusCache()
	require.NoError(t, cache.CommitBlock(statusCacheTestBlock(10)))
	candidate := statusCacheTestBlock(11, benchmarkUniqueTransactions(33760)...)
	plan, err := planBlockTransactionExecution(candidate)
	require.NoError(t, err)
	p := cache.startStatusPreparation(plan)
	// A rejected bank joins and discards the prepared maps. Waiting is also
	// idempotent for the normal commit followed by ProcessBlock's deferred join.
	require.Same(t, p.wait(), p.wait())
	found, err := cache.View().ContainsTransaction(candidate.Transactions[0])
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, uint64(10), cache.tip.slot)
	cache.mu.Lock()
	cache.coverageComplete = false
	cache.mu.Unlock()
	var incomplete *IncompleteTransactionStatusCoverageError
	require.ErrorAs(t, cache.commitBlockWithPreparedDelta(candidate, plan, p.wait()), &incomplete)
	require.Equal(t, uint64(10), cache.tip.slot)
}

func TestPreparedStatusDeltaRejectsWrongPlanWithoutPublishingIt(t *testing.T) {
	cache := NewTransactionStatusCache()
	require.NoError(t, cache.CommitBlock(statusCacheTestBlock(10)))
	left := statusCacheTestBlock(11, statusCacheTestTransaction(1, 2, 3))
	right := statusCacheTestBlock(11, statusCacheTestTransaction(1, 4, 5))
	leftPlan, err := planBlockTransactionExecution(left)
	require.NoError(t, err)
	rightPlan, err := planBlockTransactionExecution(right)
	require.NoError(t, err)
	prepared := cache.prepareTransactionStatusDelta(leftPlan.messageIdentities)
	// A mismatched prepared delta falls back to the actual block's identities.
	require.NoError(t, cache.commitBlockWithPreparedDelta(right, rightPlan, prepared))
	found, err := cache.View().ContainsTransaction(left.Transactions[0])
	require.NoError(t, err)
	require.False(t, found)
	found, err = cache.View().ContainsTransaction(right.Transactions[0])
	require.NoError(t, err)
	require.True(t, found)
}

func TestPreparedStatusDeltaEmptyBlock(t *testing.T) {
	cache := NewTransactionStatusCache()
	block := statusCacheTestBlock(10)
	plan, err := planBlockTransactionExecution(block)
	require.NoError(t, err)
	p := cache.startStatusPreparation(plan)
	require.Nil(t, p, "empty bank must not queue background work")
	require.NoError(t, cache.commitBlockWithPreparedDelta(block, plan, p.wait()))
}

func TestPreparedStatusDeltaScheduling(t *testing.T) {
	for _, threads := range []int{1, 2} {
		for _, count := range []int{1, 32, 33} {
			t.Run(fmt.Sprintf("threads_%d/txs_%d", threads, count), func(t *testing.T) {
				previous := runtime.GOMAXPROCS(threads)
				defer runtime.GOMAXPROCS(previous)
				cache := NewTransactionStatusCache()
				block := statusCacheTestBlock(10, benchmarkUniqueTransactions(count)...)
				plan, err := planBlockTransactionExecution(block)
				require.NoError(t, err)
				p := cache.startStatusPreparation(plan)
				defer p.wait()
				require.Equal(t, threads > 1 && count > 32, p != nil)
				require.NoError(t, cache.commitBlockWithPreparedDelta(block, plan, p.wait()))
				for _, tx := range block.Transactions {
					found, err := cache.View().ContainsTransaction(tx)
					require.NoError(t, err)
					require.True(t, found)
				}
			})
		}
	}
}

func TestPreparedStatusDeltaRebindsMissingSnapshotGroup(t *testing.T) {
	ancestor := statusCacheTestTransaction(1, 2, 3)
	seed, err := NewTransactionStatusCacheFromAgaveSnapshot([]txstatus.SnapshotSlotDelta{
		{Slot: 0, IsRoot: true, Statuses: []txstatus.SnapshotStatus{snapshotStatusCacheStatusForTx(t, ancestor, 7)}},
	}, 0)
	require.NoError(t, err)
	candidate := statusCacheTestBlock(1, statusCacheTestTransaction(1, 4, 5))
	plan, err := planBlockTransactionExecution(candidate)
	require.NoError(t, err)
	prepared := seed.prepareTransactionStatusDelta(plan.messageIdentities)
	// Model restore/branch replacement removing the group after preparation.
	cache := NewTransactionStatusCache()
	require.NoError(t, cache.commitBlockWithPreparedDelta(candidate, plan, prepared))
	inline := NewTransactionStatusCache()
	require.NoError(t, inline.commitBlockWithPlan(candidate, plan))
	require.Equal(t, inline.tip.delta, cache.tip.delta)
	found, err := cache.View().ContainsTransaction(candidate.Transactions[0])
	require.NoError(t, err)
	require.True(t, found)
}
