package replay

import (
	"testing"
)

func TestPreparedCommitRejectsTransactionReplacement(t *testing.T) {
	cache := NewTransactionStatusCache()
	if err := cache.CommitBlock(statusCacheTestBlock(10)); err != nil {
		t.Fatal(err)
	}

	candidate := statusCacheTestBlock(11, statusCacheTestTransaction(1, 2, 3))
	plan, err := planBlockTransactionExecution(candidate)
	if err != nil {
		t.Fatal(err)
	}
	prepared := cache.prepareTransactionStatusDelta(plan.messageIdentities)
	candidate.Transactions[0] = statusCacheTestTransaction(4, 5, 6)

	err = cache.commitBlockWithPreparedDelta(candidate, plan, prepared)
	if err == nil || err.Error() != "prepared transaction message identities do not match block" {
		t.Fatalf("commit error = %v, want prepared-plan binding failure", err)
	}
	cache.mu.RLock()
	tipSlot := cache.tip.slot
	cache.mu.RUnlock()
	if tipSlot != 10 {
		t.Fatalf("status-cache tip = %d after stale-plan rejection, want 10", tipSlot)
	}
}
