package turbine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestWideVerificationJobsPreserveSignatureFailureIndex(t *testing.T) {
	txs := verifierSignedTransactions(t, 320)
	txs[33].Signatures[0][0] ^= 1
	txs[98].Signatures[0][0] ^= 1
	for _, groups := range []int{1, 4, 8} {
		v := newTransactionVerifierWithJobGroups(2, 32, 8, groups, nil)
		r, err := v.submitTransactions(context.Background(), txs)
		require.NoError(t, err)
		index, err := r.wait()
		require.Error(t, err)
		require.Equal(t, 33, index)
		v.closeAndWait()
	}
}

func TestWideVerificationJobsYieldToReadySmallRequest(t *testing.T) {
	for _, groups := range []int{4, 8} {
		t.Run(fmt.Sprint(groups), func(t *testing.T) {
			large, small := verifierTestBlock(800), verifierTestBlock(4)
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			var calls, beforeSmall atomic.Int32
			v := newTransactionVerifierWithJobGroups(1, 16, 8, groups, func(tx *solana.Transaction) error {
				for _, s := range small.Transactions {
					if tx == s {
						beforeSmall.Store(calls.Load())
						return nil
					}
				}
				calls.Add(1)
				if tx == large.Transactions[0] {
					close(started)
					<-release
				}
				return nil
			})
			defer v.closeAndWait()
			defer releaseOnce.Do(func() { close(release) })
			big, err := v.submitTransactions(context.Background(), large.Transactions)
			require.NoError(t, err)
			waitSignal(t, started, "large job")
			little, err := v.submitTransactions(context.Background(), small.Transactions)
			require.NoError(t, err)
			require.Eventually(t, func() bool { return len(v.jobs) == 1 }, 3*time.Second, time.Millisecond)
			releaseOnce.Do(func() { close(release) })
			_, err = little.wait()
			require.NoError(t, err)
			require.Equal(t, int32(groups*8), beforeSmall.Load(), "one large job, not an entire catch-up request, precedes small work")
			_, err = big.wait()
			require.NoError(t, err)
			require.Equal(t, int32(800), calls.Load())
		})
	}
}

func TestWideVerificationJobsCancelBetweenVectorsAndJoin(t *testing.T) {
	for _, groups := range []int{4, 8} {
		t.Run(fmt.Sprint(groups), func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			var calls atomic.Int32
			v := newTransactionVerifierWithJobGroups(1, 16, 8, groups, func(*solana.Transaction) error {
				if calls.Add(1) == 1 {
					close(started)
					<-release
				}
				return nil
			})
			defer v.closeAndWait()
			defer releaseOnce.Do(func() { close(release) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := v.submitTransactions(ctx, verifierTestBlock(800).Transactions)
			require.NoError(t, err)
			waitSignal(t, started, "first vector")
			cancel()
			select {
			case <-r.done:
				t.Fatal("released transactions still read by a worker")
			default:
			}
			releaseOnce.Do(func() { close(release) })
			_, err = r.wait()
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, int32(8), calls.Load(), "canceled large job finishes its admitted vector only")
		})
	}
}

func TestWideVerificationJobsDoNotWaitForFourTransactionBatch(t *testing.T) {
	for _, groups := range []int{4, 8} {
		v := newTransactionVerifierWithJobGroups(2, 32, 8, groups, func(*solana.Transaction) error { return nil })
		r, err := v.submitTransactions(context.Background(), verifierTestBlock(4).Transactions)
		require.NoError(t, err)
		select {
		case <-r.done:
		case <-time.After(3 * time.Second):
			t.Fatal("partial work waited for another submission")
		}
		_, err = r.wait()
		require.NoError(t, err)
		v.closeAndWait()
	}
}
