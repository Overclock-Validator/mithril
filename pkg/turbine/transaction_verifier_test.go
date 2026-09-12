package turbine

import (
	"errors"
	"fmt"
	"strings"
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
	if cap(verifier.jobs) != 2*workers {
		t.Fatalf("queue capacity = %d, want %d", cap(verifier.jobs), 2*workers)
	}

	done := make(chan error, 1)
	go func() { done <- verifier.verifyBlock(verifierTestBlock(12)) }()
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
	blk := verifierTestBlock(6)
	lowErr := errors.New("low index failure")
	highErr := errors.New("high index failure")
	verifier := newTransactionVerifier(4, 8, func(tx *solana.Transaction) error {
		switch tx {
		case blk.Transactions[1]:
			time.Sleep(10 * time.Millisecond)
			return lowErr
		case blk.Transactions[3]:
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
// not leave a remainder waiting for company: verifyBlockContext joins each
// wave with done.Wait(), and a stranded job would hang it forever.
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
