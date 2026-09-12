package turbine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

func TestTransactionVerifierCancellationDuringBlockedAdmissionJoinsAdmittedJobs(t *testing.T) {
	const workers = 2
	blocker := verifierTestBlock(workers)
	target := verifierTestBlock(2 * workers)
	release := make(chan struct{})
	var releaseOnce sync.Once
	started := make(chan struct{}, workers)
	var targetFirstCalls atomic.Int32
	var targetLaterCalls atomic.Int32
	// Single-transaction groups make the blocked-admission boundary exact.
	verifier := newTransactionVerifierWithBatchTarget(workers, 1, 1, func(tx *solana.Transaction) error {
		switch tx {
		case blocker.Transactions[0], blocker.Transactions[1]:
			started <- struct{}{}
			<-release
		case target.Transactions[0]:
			targetFirstCalls.Add(1)
		default:
			targetLaterCalls.Add(1)
		}
		return nil
	})
	defer verifier.closeAndWait()
	defer releaseOnce.Do(func() { close(release) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blockerDone := make(chan error, 1)
	targetDone := make(chan error, 1)
	go func() { blockerDone <- verifier.verifyBlock(blocker) }()
	for range workers {
		waitSignal(t, started, "occupied verifier worker")
	}

	// Both workers are occupied. The target's first group fills the one-entry
	// queue, and its second blocks on admission. Cancellation must still join
	// the first group while preventing any later transactions from running.
	go func() { targetDone <- verifier.verifyBlockContext(ctx, target) }()
	deadline := time.Now().Add(3 * time.Second)
	for len(verifier.jobs) != cap(verifier.jobs) {
		if time.Now().After(deadline) {
			t.Fatal("transaction queue did not fill")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-targetDone:
		t.Fatalf("canceled verifier returned before its admitted job joined: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-targetDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("target verification error = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled verifier did not return after admitted jobs drained")
	}
	select {
	case err := <-blockerDone:
		if err != nil {
			t.Fatalf("blocker verification: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocker verification did not drain")
	}
	if got := targetFirstCalls.Load(); got != 1 {
		t.Fatalf("admitted target transaction calls = %d, want 1", got)
	}
	if got := targetLaterCalls.Load(); got != 0 {
		t.Fatalf("later target transaction calls = %d, want 0", got)
	}
}

func TestCanceledQueuedSlotCompletionReleasesTokenWithoutPoisoning(t *testing.T) {
	assembler := NewSlotAssembler()
	started := make(chan struct{})
	release := make(chan struct{})
	var verifyCalls atomic.Int32
	verify := func(context.Context, *block.Block) error {
		if verifyCalls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}
	assembler.verifyTransactions = verify

	// Use an inert first token solely to occupy the one completion worker.
	blockerState := &slotState{
		slot:       76,
		shreds:     make(map[uint32]*Shred),
		fecSets:    make(map[uint32]*fecState),
		lastIndex:  ^uint32(0),
		completing: true,
	}
	assembler.slots[blockerState.slot] = blockerState
	blockerWork := &slotCompletionWork{
		state:              blockerState,
		queuedAt:           time.Now(),
		verifyTransactions: verify,
	}

	// Claim a real complete fixture slot so cancellation exercises the same
	// generation token that production assembly queues.
	var targetWork *slotCompletionWork
	for idx, packet := range fixtures.DataShreds(t, "mainnet", 102815960) {
		shred, err := ParseShred(packet)
		if err != nil {
			t.Fatalf("ParseShred(%d): %v", idx, err)
		}
		work, err := assembler.addShredFrom(shred, false)
		if err != nil {
			t.Fatalf("addShredFrom(%d): %v", idx, err)
		}
		if work != nil {
			targetWork = work
		}
	}
	if targetWork == nil || targetWork.state == nil || !targetWork.state.completing {
		t.Fatal("fixture slot did not yield a claimed completion token")
	}
	targetState := targetWork.state

	pool := newSlotCompletionPool(assembler, nil, nil, 1, 1)
	if !pool.enqueue(context.Background(), blockerWork, false) {
		t.Fatal("failed to enqueue blocker completion")
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		close(release)
		pool.closeAndWait()
		t.Fatal("timed out occupying completion worker")
	}

	ctx, cancel := context.WithCancel(context.Background())
	if !pool.enqueue(ctx, targetWork, false) {
		cancel()
		close(release)
		pool.closeAndWait()
		t.Fatal("failed to queue target completion")
	}
	cancel()
	close(release)
	pool.closeAndWait()

	assembler.mu.Lock()
	liveState := assembler.slots[targetState.slot]
	completing := targetState.completing
	errCount := targetState.errCount
	lastErr := targetState.lastErr
	_, completed := assembler.completedSlots[targetState.slot]
	assembler.mu.Unlock()

	if liveState != targetState {
		t.Fatal("canceled completion removed or replaced the claimed slot state")
	}
	if completing {
		t.Fatal("canceled completion left its generation token claimed")
	}
	if errCount != 0 || lastErr != "" {
		t.Fatalf("canceled completion poisoned slot state: count=%d latest=%q", errCount, lastErr)
	}
	if completed {
		t.Fatal("canceled completion marked the slot completed")
	}
	if got := verifyCalls.Load(); got != 1 {
		t.Fatalf("transaction verifier calls = %d, want only blocker", got)
	}
}
