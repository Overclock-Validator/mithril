package turbine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestPrefetchLeavesCompletionPermitAndCancellationJoins(t *testing.T) {
	release := make(chan struct{})
	v := newTransactionVerifier(2, 32, func(*solana.Transaction) error { <-release; return nil })
	defer v.closeAndWait()
	defer close(release)
	for range 3 {
		_, err := v.submitPrefetchTransactions(context.Background(), verifierTestBlock(64).Transactions)
		require.NoError(t, err)
	}
	require.Equal(t, 3, len(v.requests))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocked := make(chan error, 1)
	go func() { _, err := v.submitPrefetchTransactions(ctx, verifierTestBlock(1).Transactions); blocked <- err }()
	accepted := make(chan *transactionVerification, 1)
	go func() {
		r, err := v.submitTransactions(context.Background(), verifierTestBlock(1).Transactions)
		if err != nil {
			t.Error(err)
		}
		accepted <- r
	}()
	select {
	case r := <-accepted:
		require.NotNil(t, r)
	case <-time.After(3 * time.Second):
		t.Fatal("prefetch occupied the reserved completion permit")
	}
	require.Equal(t, cap(v.requests), len(v.requests), "total bound must not increase")
	cancel()
	select {
	case err := <-blocked:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("canceled prefetch admission did not return")
	}
	// The admitted jobs are still reading transactions until release closes.
	require.Equal(t, 4, len(v.requests))
}

// Exercise permit arbitration without depending on cryptographic job duration.
// Holding permits models admitted requests; each release also joins its request.
func TestCompletionWinsAdmissionAndPrefetchResumes(t *testing.T) {
	v := newTransactionVerifier(1, 8, nil)
	ctx, cancel := context.WithCancel(context.Background())
	var cleanup sync.WaitGroup
	defer v.closeAndWait()
	defer cleanup.Wait()
	defer cancel()
	release := func(prefetch bool) { v.releaseRequest(prefetch); v.request.Done() }
	for range 2 {
		require.NoError(t, v.acquireRequest(ctx, false))
	}
	firstReleased := false
	defer func() {
		if !firstReleased {
			release(false)
		}
		release(false)
	}()
	completionAdmitted := make(chan struct{})
	completionRelease := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(completionRelease) })
	cleanup.Go(func() {
		if err := v.acquireRequest(ctx, false); err != nil {
			return
		}
		close(completionAdmitted)
		select {
		case <-completionRelease:
		case <-ctx.Done():
		}
		release(false)
	})
	require.Eventually(t, func() bool { v.mu.Lock(); defer v.mu.Unlock(); return v.completionWaiters == 1 }, 3*time.Second, time.Millisecond)
	prefetchAdmitted := make(chan struct{})
	cleanup.Go(func() {
		if err := v.acquireRequest(ctx, true); err != nil {
			return
		}
		close(prefetchAdmitted)
		release(true)
	})
	release(false)
	firstReleased = true
	waitSignal(t, completionAdmitted, "priority completion admission")
	select {
	case <-prefetchAdmitted:
		t.Fatal("prefetch bypassed waiting completion")
	default:
	}
	once.Do(func() { close(completionRelease) })
	waitSignal(t, prefetchAdmitted, "prefetch resumed after completion")
}

func TestCanceledCompletionDoesNotBlockPrefetch(t *testing.T) {
	v := newTransactionVerifier(1, 8, nil)
	defer v.closeAndWait()
	for range 2 {
		require.NoError(t, v.acquireRequest(context.Background(), false))
	}
	defer func() {
		for range 2 {
			v.releaseRequest(false)
			v.request.Done()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- v.acquireRequest(ctx, false) }()
	require.Eventually(t, func() bool { v.mu.Lock(); defer v.mu.Unlock(); return v.completionWaiters == 1 }, 3*time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("completion cancellation stranded admission")
	}
	v.mu.Lock()
	require.Zero(t, v.completionWaiters)
	v.mu.Unlock()
}

func TestCloseWakesBothAdmissionClassesAndJoinsAcceptedWork(t *testing.T) {
	release := make(chan struct{})
	v := newTransactionVerifier(1, 8, func(*solana.Transaction) error { <-release; return nil })
	var once sync.Once
	defer v.closeAndWait()
	defer once.Do(func() { close(release) })
	_, err := v.submitPrefetchTransactions(context.Background(), verifierTestBlock(1).Transactions)
	require.NoError(t, err)
	_, err = v.submitTransactions(context.Background(), verifierTestBlock(1).Transactions)
	require.NoError(t, err)
	results := make(chan error, 2)
	for _, prefetch := range []bool{true, false} {
		go func() {
			_, err := v.submitRequest(context.Background(), verifierTestBlock(1).Transactions, prefetch)
			results <- err
		}()
	}
	closed := make(chan struct{})
	go func() { v.closeAndWait(); close(closed) }()
	for range 2 {
		select {
		case err := <-results:
			require.ErrorIs(t, err, errTransactionVerifierClosed)
		case <-time.After(3 * time.Second):
			t.Fatal("close stranded admission")
		}
	}
	select {
	case <-closed:
		t.Fatal("close returned while jobs still own transactions")
	default:
	}
	once.Do(func() { close(release) })
	waitSignal(t, closed, "close joined accepted work")
}
