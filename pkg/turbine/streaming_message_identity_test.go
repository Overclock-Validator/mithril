package turbine

import (
	"context"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestMessageIdentitiesPreparedBeforeFinalShred(t *testing.T) {
	v := newTransactionVerifier(2, 16, nil)
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	batches := prefetchTestShreds(t, 100,
		prefetchTestPayload(t, verifierSignedTransactions(t, 3)),
		prefetchTestPayload(t, verifierSignedTransactions(t, 4)))
	require.Nil(t, feedPrefetchShreds(t, a, batches[0]))
	cached := waitPrefetchedBatch(t, a, 100, 0)
	_, err := cached.verification.wait()
	require.NoError(t, err)
	require.Len(t, cached.verification.identities, 3)
	for i := range cached.entries[0].Txns {
		tx := &cached.entries[0].Txns[i]
		got, ok := cached.verification.identities[i].ForTransaction(tx)
		require.True(t, ok)
		want, err := txstatus.IdentityForTransaction(tx)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	blk := feedPrefetchShreds(t, a, batches[1])
	require.NotNil(t, blk)
	prepared, err := blk.PrepareTransactionMessageIdentities()
	require.NoError(t, err)
	for i, tx := range blk.Transactions {
		want, err := txstatus.IdentityForTransaction(tx)
		require.NoError(t, err)
		require.Equal(t, want, prepared.Identity(i))
	}
}

func TestVerifiedIdentityCacheRejectsMismatchedAndPartialCoverage(t *testing.T) {
	v := newTransactionVerifier(2, 16, nil)
	defer v.closeAndWait()
	txs := verifierSignedTransactions(t, 3)
	request, err := v.submitTransactions(context.Background(), txs)
	require.NoError(t, err)
	_, err = request.wait()
	require.NoError(t, err)
	blk := &block.Block{Transactions: txs}
	require.NoError(t, blk.CacheVerifiedTransactionMessageIdentities(request.identities))
	prepared, err := blk.PrepareTransactionMessageIdentities()
	require.NoError(t, err)
	require.Error(t, blk.CacheVerifiedTransactionMessageIdentities(request.identities[:2]))
	copyTx := *txs[0]
	for _, changed := range [][]*solana.Transaction{{txs[1], txs[0], txs[2]}, {&copyTx, txs[1], txs[2]}} {
		other := &block.Block{Transactions: changed}
		require.Error(t, other.CacheVerifiedTransactionMessageIdentities(request.identities))
	}
	again, err := blk.PrepareTransactionMessageIdentities()
	require.NoError(t, err)
	require.Same(t, prepared, again, "failed cache adoption must not replace a valid cache")
	txs[0].Message.RecentBlockhash[0] ^= 1
	require.Error(t, blk.CacheVerifiedTransactionMessageIdentities(request.identities))
}

func TestMessageIdentityFallbackPreservesRetainedTransactionOrder(t *testing.T) {
	v := newTransactionVerifier(2, 16, nil)
	defer v.closeAndWait()
	a := NewSlotAssembler()
	p := newEntryPrefetchPool(context.Background(), a, v)
	defer p.closeAndWait()
	batches := prefetchTestShreds(t, 100,
		prefetchTestPayload(t, verifierSignedTransactions(t, 3)),
		prefetchTestPayload(t, verifierSignedTransactions(t, 4)),
		prefetchTestPayload(t, verifierSignedTransactions(t, 5)))
	for i := 0; i < 2; i++ {
		require.Nil(t, feedPrefetchShreds(t, a, batches[i]))
		cached := waitPrefetchedBatch(t, a, 100, batches[i][0].Index)
		_, err := cached.verification.wait()
		require.NoError(t, err)
		if i == 1 {
			// Force a canceled, joined result in the middle of the retained
			// sequence. Completion must reverify and scatter its identities.
			done := make(chan struct{})
			close(done)
			cached.verification = &transactionVerification{done: done, err: context.Canceled, index: -1, cancel: func() {}}
		}
	}
	blk := feedPrefetchShreds(t, a, batches[2])
	require.NotNil(t, blk)
	require.Len(t, blk.Transactions, 12)
	prepared, err := blk.PrepareTransactionMessageIdentities()
	require.NoError(t, err)
	for i, tx := range blk.Transactions {
		want, err := txstatus.IdentityForTransaction(tx)
		require.NoError(t, err)
		require.Equal(t, want, prepared.Identity(i))
	}
}
