package turbine

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func verifierTestBlock(count int) *block.Block {
	blk := &block.Block{Slot: 77, Transactions: make([]*solana.Transaction, count)}
	for idx := range blk.Transactions {
		blk.Transactions[idx] = &solana.Transaction{}
	}
	return blk
}

func updateAtomicMax(dst *atomic.Int32, candidate int32) {
	for {
		current := dst.Load()
		if candidate <= current || dst.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func TestTransactionVerifierBoundsConcurrencyAndQueue(t *testing.T) {
	const workers = 3
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	verifier := newTransactionVerifier(workers, 2*workers, func(*solana.Transaction) error {
		current := active.Add(1)
		updateAtomicMax(&maximum, current)
		defer active.Add(-1)
		<-release
		return nil
	})
	defer verifier.closeAndWait()
	if got, want := cap(verifier.jobs), 1; got != want {
		t.Fatalf("group queue capacity = %d, want %d", got, want)
	}

	done := make(chan error, 1)
	go func() { done <- verifier.verifyBlock(verifierTestBlock(24)) }()
	deadline := time.After(3 * time.Second)
	for active.Load() != workers {
		select {
		case <-deadline:
			t.Fatalf("active workers = %d, want %d", active.Load(), workers)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if maximum.Load() > workers {
		t.Fatalf("maximum verifier concurrency = %d, exceeds %d", maximum.Load(), workers)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("verifyBlock: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bounded verifier did not join")
	}
	if maximum.Load() != workers {
		t.Fatalf("maximum verifier concurrency = %d, want %d", maximum.Load(), workers)
	}
}

func TestTransactionVerifierReturnsLowestFailingIndex(t *testing.T) {
	blk := verifierTestBlock(24)
	lowErr := errors.New("low index failure")
	highErr := errors.New("high index failure")
	verifier := newTransactionVerifier(4, 8, func(tx *solana.Transaction) error {
		switch tx {
		case blk.Transactions[1]:
			time.Sleep(10 * time.Millisecond)
			return lowErr
		case blk.Transactions[9]:
			return highErr
		default:
			return nil
		}
	})
	defer verifier.closeAndWait()

	err := verifier.verifyBlock(blk)
	if !errors.Is(err, lowErr) || !strings.Contains(err.Error(), "transaction 1") {
		t.Fatalf("verifyBlock error = %v, want lowest transaction index", err)
	}
}

func TestTransactionVerifierPanicFailsClosedAndPoolRemainsUsable(t *testing.T) {
	blk := verifierTestBlock(2)
	panicTx := blk.Transactions[0]
	verifier := newTransactionVerifier(2, 4, func(tx *solana.Transaction) error {
		if tx == panicTx {
			panic("test verifier panic")
		}
		return nil
	})
	defer verifier.closeAndWait()

	err := verifier.verifyBlock(blk)
	if err == nil || !strings.Contains(err.Error(), "transaction 0") || !strings.Contains(err.Error(), "test verifier panic") {
		t.Fatalf("panic error = %v", err)
	}
	if err := verifier.verifyBlock(&block.Block{Slot: 78, Transactions: []*solana.Transaction{{}}}); err != nil {
		t.Fatalf("pool unusable after recovered panic: %v", err)
	}
}

func TestTransactionVerifierRejectsNilAtDeterministicIndex(t *testing.T) {
	blk := verifierTestBlock(4)
	blk.Transactions[2] = nil
	verifier := newTransactionVerifier(4, 8, func(*solana.Transaction) error { return nil })
	defer verifier.closeAndWait()

	err := verifier.verifyBlock(blk)
	if got, want := fmt.Sprint(err), "slot 77 transaction 2 is nil"; got != want {
		t.Fatalf("nil transaction error = %q, want %q", got, want)
	}
}

// Every transaction in a block must be verified and joined, whatever the count.
// Workers group transactions, so a count that divides badly into groups must
// not leave a remainder waiting for company: every tail is dispatched as
// soon as it is available, without waiting for another request.
//
// A counting verifier is injected so the assertion is on what was actually
// verified, not merely on returning without error.
func TestTransactionVerifierVerifiesEveryTransactionForAwkwardCounts(t *testing.T) {
	for _, count := range []int{1, 3, 7, 8, 9, 33, 65} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			var seen atomic.Int32
			verifier := newTransactionVerifier(4, 8, func(*solana.Transaction) error {
				seen.Add(1)
				return nil
			})
			defer verifier.closeAndWait()

			done := make(chan error, 1)
			go func() { done <- verifier.verifyBlock(verifierTestBlock(count)) }()

			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(30 * time.Second):
				t.Fatalf("count=%d: transactions stranded in an unfinished group", count)
			}
			require.Equal(t, int32(count), seen.Load(),
				"count=%d: every transaction must be verified exactly once", count)
		})
	}
}

func TestTransactionVerifierRefillsWhileEarlierGroupIsBlocked(t *testing.T) {
	blk := verifierTestBlock(24)
	started := make(chan struct{})
	release := make(chan struct{})
	refilled := make(chan struct{})
	v := newTransactionVerifierWithBatchTarget(2, 16, 8, func(tx *solana.Transaction) error {
		switch tx {
		case blk.Transactions[0]:
			close(started)
			<-release
		case blk.Transactions[16]:
			close(refilled)
		}
		return nil
	})
	defer v.closeAndWait()
	defer close(release)
	r, err := v.submitTransactions(context.Background(), blk.Transactions)
	require.NoError(t, err)
	waitSignal(t, started, "slow first group")
	waitSignal(t, refilled, "rolling refill before first group finishes")
	select {
	case <-r.done:
		t.Fatal("request finished without joining its blocked group")
	default:
	}
}

func TestTransactionVerifierPartialTailStartsWithoutAnotherSubmission(t *testing.T) {
	for _, target := range []int{4, 8} {
		t.Run(fmt.Sprintf("target=%d", target), func(t *testing.T) {
			seen := make(chan struct{}, 3)
			v := newTransactionVerifierWithBatchTarget(2, 16, target, func(*solana.Transaction) error {
				seen <- struct{}{}
				return nil
			})
			defer v.closeAndWait()
			r, err := v.submitTransactions(context.Background(), verifierTestBlock(3).Transactions)
			require.NoError(t, err)
			for range 3 {
				waitSignal(t, seen, "available partial batch transaction")
			}
			_, err = r.wait()
			require.NoError(t, err)
			require.False(t, r.finishedAt.IsZero())
		})
	}
}

func TestTransactionVerifierLargeRequestDoesNotQueuePastSmallRequest(t *testing.T) {
	large := verifierTestBlock(800)
	small := verifierTestBlock(1)
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	var largeCalls atomic.Int32
	var callsBeforeSmall atomic.Int32
	v := newTransactionVerifierWithBatchTarget(1, 16, 8, func(tx *solana.Transaction) error {
		if tx == small.Transactions[0] {
			callsBeforeSmall.Store(largeCalls.Load())
			return nil
		}
		largeCalls.Add(1)
		if tx == large.Transactions[0] {
			close(started)
			<-release
		}
		return nil
	})
	defer v.closeAndWait()
	defer releaseOnce.Do(func() { close(release) })
	largeRequest, err := v.submitTransactions(context.Background(), large.Transactions)
	require.NoError(t, err)
	waitSignal(t, started, "large request first group")
	smallRequest, err := v.submitTransactions(context.Background(), small.Transactions)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(v.jobs) == 1 }, 3*time.Second, time.Millisecond)
	releaseOnce.Do(func() { close(release) })
	_, err = smallRequest.wait()
	require.NoError(t, err)
	require.Equal(t, int32(8), callsBeforeSmall.Load(), "large request may only stay one group ahead")
	_, err = largeRequest.wait()
	require.NoError(t, err)
	require.Equal(t, int32(800), largeCalls.Load())
}

func TestTransactionVerifierAsyncAdmissionAppliesCancelableBackpressure(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var first sync.Once
	v := newTransactionVerifierWithBatchTarget(1, 8, 8, func(*solana.Transaction) error {
		first.Do(func() { close(started) })
		<-release
		return nil
	})
	defer v.closeAndWait()
	defer close(release)
	for range 2 {
		_, err := v.submitTransactions(context.Background(), verifierTestBlock(1).Transactions)
		require.NoError(t, err)
	}
	waitSignal(t, started, "occupied request slots")
	require.Equal(t, cap(v.requests), len(v.requests))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := v.submitTransactions(ctx, verifierTestBlock(1).Transactions)
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("unbounded request admitted instead of waiting: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("request admission ignored cancellation")
	}
}

func TestTransactionVerificationWaitContextCancelsAndJoins(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	var seen atomic.Int32
	v := newTransactionVerifierWithBatchTarget(1, 8, 8, func(*solana.Transaction) error {
		if seen.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	})
	defer v.closeAndWait()
	defer releaseOnce.Do(func() { close(release) })
	r, err := v.submitTransactions(context.Background(), verifierTestBlock(800).Transactions)
	require.NoError(t, err)
	waitSignal(t, started, "first admitted group")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := make(chan error, 1)
	go func() { _, err := r.waitContext(ctx); result <- err }()
	select {
	case err := <-result:
		t.Fatalf("wait returned while transactions still owned by worker: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("canceled request did not join")
	}
	require.Equal(t, int32(8), seen.Load(), "cancellation must stop later group admission")
}

func TestTransactionVerifierCloseRacesAdmissionWithoutStrandingRequests(t *testing.T) {
	v := newTransactionVerifier(2, 16, func(*solana.Transaction) error { return nil })
	var callers sync.WaitGroup
	for range 32 {
		callers.Go(func() {
			r, err := v.submitTransactions(context.Background(), verifierTestBlock(17).Transactions)
			if err != nil {
				if !errors.Is(err, errTransactionVerifierClosed) {
					t.Errorf("submit error: %v", err)
				}
				return
			}
			_, err = r.wait()
			if err != nil {
				t.Errorf("admitted request error: %v", err)
			}
		})
	}
	v.closeAndWait()
	callers.Wait()
	_, err := v.submitTransactions(context.Background(), verifierTestBlock(1).Transactions)
	require.ErrorIs(t, err, errTransactionVerifierClosed)
}

func verifierSignedTransactions(t *testing.T, count int) []*solana.Transaction {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 71 // Deterministic test-only key; never a validator identity.
	key := ed25519.NewKeyFromSeed(seed)
	var public solana.PublicKey
	copy(public[:], key[32:])
	txs := make([]*solana.Transaction, count)
	for i := range txs {
		tx := &solana.Transaction{
			Message: solana.Message{
				Header:          solana.MessageHeader{NumRequiredSignatures: 1},
				AccountKeys:     []solana.PublicKey{public},
				RecentBlockhash: solana.Hash{byte(i), byte(i >> 8)},
			},
			Signatures: make([]solana.Signature, 1),
		}
		message, err := tx.Message.MarshalBinary()
		require.NoError(t, err)
		copy(tx.Signatures[0][:], ed25519.Sign(key, message))
		txs[i] = tx
	}
	return txs
}

func TestTransactionVerifierRejectsEveryInvalidSignatureLane(t *testing.T) {
	v := newTransactionVerifier(2, 16, nil)
	defer v.closeAndWait()
	for invalid := range 8 {
		t.Run(fmt.Sprintf("lane=%d", invalid), func(t *testing.T) {
			txs := verifierSignedTransactions(t, 8)
			txs[invalid].Signatures[0][13] ^= 0x40
			r, err := v.submitTransactions(context.Background(), txs)
			require.NoError(t, err)
			index, err := r.wait()
			require.ErrorContains(t, err, "invalid signature")
			require.Equal(t, invalid, index)
		})
	}
	r, err := v.submitTransactions(context.Background(), verifierSignedTransactions(t, 17))
	require.NoError(t, err)
	_, err = r.wait()
	require.NoError(t, err, "valid transactions must still verify after invalid lanes")
}

func TestTransactionVerifierKeepsMultisignatureTransactionsIntactAcrossTargets(t *testing.T) {
	counts := []int{2, 2, 1, 4, 9, 3, 2, 1}
	txs := make([]*solana.Transaction, len(counts))
	for i, count := range counts {
		keys := make([]ed25519.PrivateKey, count)
		public := make([]solana.PublicKey, count)
		for signer := range keys {
			seed := make([]byte, ed25519.SeedSize)
			seed[0], seed[1] = 83, byte(signer)
			keys[signer] = ed25519.NewKeyFromSeed(seed)
			copy(public[signer][:], keys[signer][32:])
		}
		tx := &solana.Transaction{
			Message: solana.Message{
				Header: solana.MessageHeader{
					NumRequiredSignatures:     uint8(count),
					NumReadonlySignedAccounts: uint8(count - 1),
				},
				AccountKeys:     public,
				RecentBlockhash: solana.Hash{byte(i)},
			},
			Signatures: make([]solana.Signature, count),
		}
		message, err := tx.Message.MarshalBinary()
		require.NoError(t, err)
		for signer, key := range keys {
			copy(tx.Signatures[signer][:], ed25519.Sign(key, message))
		}
		txs[i] = tx
	}
	for _, target := range []int{4, 8} {
		t.Run(fmt.Sprintf("target=%d", target), func(t *testing.T) {
			v := newTransactionVerifierWithBatchTarget(2, 16, target, nil)
			defer v.closeAndWait()
			r, err := v.submitTransactions(context.Background(), txs)
			require.NoError(t, err)
			_, err = r.wait()
			require.NoError(t, err)

			// Corrupt a non-first signer after an oversized (nine-signature)
			// transaction. Results must still map to the original tx index.
			txs[6].Signatures[1][11] ^= 0x20
			r, err = v.submitTransactions(context.Background(), txs)
			require.NoError(t, err)
			index, err := r.wait()
			require.ErrorContains(t, err, "invalid signature")
			require.Equal(t, 6, index)
			txs[6].Signatures[1][11] ^= 0x20
		})
	}
}
