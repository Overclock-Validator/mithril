package turbine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestEntryIdentityRecoveryVerifiesFinalTransactions(t *testing.T) {
	for _, fault := range []string{"partial_identities", "wrong_pointer", "missing_range", "oversized_range", "nil_batch"} {
		for _, invalidFinal := range []bool{false, true} {
			name := fault + "/valid"
			if invalidFinal {
				name = fault + "/invalid"
			}
			t.Run(name, func(t *testing.T) {
				v := newTransactionVerifier(2, 16, nil)
				defer v.closeAndWait()
				txs := verifierSignedTransactions(t, 3)
				entries := []Entry{{Txns: []solana.Transaction{*txs[0], *txs[1], *txs[2]}}}
				blk := &block.Block{Slot: 812, Transactions: entryBatchTransactions(entries)}
				request, err := v.submitTransactions(context.Background(), blk.Transactions)
				require.NoError(t, err)
				_, err = request.wait()
				require.NoError(t, err)
				batches := []*prefetchedShredBatch{{entries: entries, verification: request}}
				switch fault {
				case "partial_identities":
					request.identities = request.identities[:2]
				case "wrong_pointer":
					copyTx := *blk.Transactions[0]
					blk.Transactions[0] = &copyTx
				case "missing_range":
					batches = nil
				case "oversized_range":
					batches[0].entries = append(batches[0].entries, Entry{Txns: []solana.Transaction{*txs[0]}})
				case "nil_batch":
					batches = []*prefetchedShredBatch{nil}
				}
				if invalidFinal {
					// The old request verified a different, valid transaction.
					// A successful old verdict must not bless these final bytes.
					copyTx := *blk.Transactions[0]
					copyTx.Signatures = append([]solana.Signature(nil), copyTx.Signatures...)
					copyTx.Signatures[0][0] ^= 1
					blk.Transactions[0] = &copyTx
				}
				err = verifyDecodedEntryBatches(context.Background(), blk, batches, v)
				if invalidFinal {
					require.ErrorContains(t, err, "failed signature verification")
					return
				}
				require.NoError(t, err)
				prepared, err := blk.PrepareTransactionMessageIdentities()
				require.NoError(t, err)
				for i, tx := range blk.Transactions {
					want, err := txstatus.IdentityForTransaction(tx)
					require.NoError(t, err)
					require.Equal(t, want, prepared.Identity(i))
				}
			})
		}
	}
}

func TestEntryIdentityRecoveryPreservesCancellationAndVerifierFailure(t *testing.T) {
	blk := &block.Block{Slot: 813, Transactions: verifierSignedTransactions(t, 2)}
	v := newTransactionVerifier(2, 16, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, verifyDecodedEntryBatches(ctx, blk, nil, v), context.Canceled)
	v.closeAndWait()
	require.ErrorIs(t, verifyDecodedEntryBatches(context.Background(), blk, nil, v), errTransactionVerifierClosed)
}

func TestEntryIdentityRecoveryJoinsCanceledReaders(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	v := newTransactionVerifier(1, 8, func(*solana.Transaction) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	})
	defer v.closeAndWait()
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	txs := verifierSignedTransactions(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := v.submitTransactions(ctx, txs)
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("reader never started")
	}
	result := make(chan error, 1)
	// This deliberately inconsistent range sends us through recovery while
	// the old verifier still owns the block's transaction buffers.
	go func() {
		result <- verifyDecodedEntryBatches(ctx, &block.Block{Transactions: txs}, []*prefetchedShredBatch{{verification: request}}, v)
	}()
	cancel()
	select {
	case err := <-result:
		t.Fatalf("returned before old reader released its buffers: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("recovery did not join canceled readers")
	}
	select {
	case <-request.done:
	default:
		t.Fatal("request ownership was not released")
	}
}
