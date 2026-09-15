package replay

import (
	"errors"
	"testing"
)

func TestPreparedCommitRechecksAncestorAfterForkSwitch(t *testing.T) {
	cache := NewTransactionStatusCache()
	requireNoError := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	requireNoError(cache.CommitBlock(statusCacheTestBlock(10)))
	requireNoError(cache.CommitBlock(statusCacheTestBlock(
		11,
		statusCacheTestTransaction(6, 6, 1),
	)))

	retried := statusCacheTestTransaction(7, 8, 2)
	unique := statusCacheTestTransaction(7, 9, 3)
	candidate := statusCacheTestBlock(12, retried, unique)
	plan, err := planBlockTransactionExecution(candidate)
	requireNoError(err)
	requireNoError(cache.validateBlockWithPlan(candidate, plan))
	prepared := cache.prepareTransactionStatusDelta(plan.messageIdentities)

	requireNoError(cache.Unwind(11))
	replacement := statusCacheTestBlock(
		11,
		statusCacheTestTransaction(7, 8, 4),
	)
	requireNoError(cache.CommitBlock(replacement))

	err = cache.commitBlockWithPreparedDelta(candidate, plan, prepared)
	var ancestorErr *AncestorAlreadyProcessedTransactionMessagesError
	if !errors.As(err, &ancestorErr) {
		t.Fatalf("prepared commit error = %v, want ancestor AlreadyProcessed", err)
	}
	if ancestorErr.AlreadyProcessedCount != 1 ||
		len(ancestorErr.Occurrences) != 1 ||
		ancestorErr.Occurrences[0].Index != 0 ||
		ancestorErr.Occurrences[0].ProcessedSlot != 11 {
		t.Fatalf("ancestor error = %+v, want candidate tx 0 processed at slot 11", ancestorErr)
	}

	cache.mu.RLock()
	tipSlot := cache.tip.slot
	cache.mu.RUnlock()
	if tipSlot != 11 {
		t.Fatalf("status-cache tip = %d after rejected commit, want 11", tipSlot)
	}
	view := cache.View()
	containsUnique, err := view.ContainsTransaction(unique)
	requireNoError(err)
	if containsUnique {
		t.Fatal("rejected prepared commit partially published its unique transaction")
	}
}

func TestConcurrentPreparedSiblingCommitsPublishExactlyOne(t *testing.T) {
	cache := NewTransactionStatusCache()
	if err := cache.CommitBlock(statusCacheTestBlock(10)); err != nil {
		t.Fatal(err)
	}

	left := statusCacheTestBlock(11, statusCacheTestTransaction(1, 2, 3))
	right := statusCacheTestBlock(11, statusCacheTestTransaction(4, 5, 6))
	leftPlan, err := planBlockTransactionExecution(left)
	if err != nil {
		t.Fatal(err)
	}
	rightPlan, err := planBlockTransactionExecution(right)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.validateBlockWithPlan(left, leftPlan); err != nil {
		t.Fatalf("prevalidate left sibling: %v", err)
	}
	if err := cache.validateBlockWithPlan(right, rightPlan); err != nil {
		t.Fatalf("prevalidate right sibling: %v", err)
	}

	leftPrepared := cache.prepareTransactionStatusDelta(leftPlan.messageIdentities)
	rightPrepared := cache.prepareTransactionStatusDelta(rightPlan.messageIdentities)
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		results <- cache.commitBlockWithPreparedDelta(left, leftPlan, leftPrepared)
	}()
	go func() {
		<-start
		results <- cache.commitBlockWithPreparedDelta(right, rightPlan, rightPrepared)
	}()
	close(start)

	successes := 0
	lineageFailures := 0
	for range 2 {
		err := <-results
		if err == nil {
			successes++
			continue
		}
		var lineageErr *TransactionStatusLineageError
		if errors.As(err, &lineageErr) {
			lineageFailures++
			continue
		}
		t.Fatalf("unexpected sibling commit error: %v", err)
	}
	if successes != 1 || lineageFailures != 1 {
		t.Fatalf("sibling results: %d successes, %d lineage failures; want 1 and 1", successes, lineageFailures)
	}

	view := cache.View()
	leftVisible, err := view.ContainsTransaction(left.Transactions[0])
	if err != nil {
		t.Fatal(err)
	}
	rightVisible, err := view.ContainsTransaction(right.Transactions[0])
	if err != nil {
		t.Fatal(err)
	}
	if leftVisible == rightVisible {
		t.Fatalf("published sibling statuses: left=%t right=%t; want exactly one", leftVisible, rightVisible)
	}
}
