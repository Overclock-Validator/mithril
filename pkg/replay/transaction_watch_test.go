package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestTransactionWatchSetParsing(t *testing.T) {
	set, err := NewTransactionWatchSet(nil)
	require.NoError(t, err)
	require.Nil(t, set, "no addresses means no watch set")
	require.Zero(t, set.Len())

	a, b := solana.PublicKey{1}.String(), solana.PublicKey{2}.String()
	set, err = NewTransactionWatchSet([]string{a, b})
	require.NoError(t, err)
	require.Equal(t, 2, set.Len())
	_, err = NewTransactionWatchSet([]string{a, a})
	require.ErrorContains(t, err, "duplicate")
	_, err = NewTransactionWatchSet([]string{"not an address"})
	require.Error(t, err)
	_, err = NewTransactionWatchSet(make([]string, maxWatchedAddresses+1))
	require.ErrorContains(t, err, "at most")
}

// A watch set captures only the transactions naming a watched address, in
// block order, and leaves execution identical whether streamed or not.
func TestWatchSetCapturesOnlyWatchedTransactions(t *testing.T) {
	previous := StreamingExecutionCfg
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	t.Cleanup(func() { StreamingExecutionCfg = previous })
	env := newLifecycleEnv(t)
	txs := transferTransactions(t, 12, 999_000)
	// The fixture transfers all name the same accounts. Give two of them, not
	// first or last in any group, a read-only account of their own to watch.
	marks := []solana.PublicKey{{0xA2, 2}, {0xA9, 9}}
	for n, index := range []int{2, 9} {
		message := &txs[index].Message
		message.AccountKeys = append(message.AccountKeys, marks[n])
		message.Header.NumReadonlyUnsignedAccounts++
		payer := txfixture.PayerPrivateKey()
		_, err := txs[index].Sign(func(solana.PublicKey) *solana.PrivateKey { return &payer })
		require.NoError(t, err)
	}

	var all blockMetadataCapture
	want, _ := lifecycleStreamCapture(t, env, txs, []int{3, 7}, 2, 4, &all)
	require.NoError(t, all.Err)
	require.Len(t, all.Record.Transactions, len(txs))
	require.False(t, all.Record.Watched)

	watch, err := NewTransactionWatchSet([]string{marks[0].String(), marks[1].String()})
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		splits []int
		suffix int
	}{{"one group", nil, 0}, {"several groups", []int{3, 7}, 2}, {"only suffix", nil, 12}} {
		t.Run(tc.name, func(t *testing.T) {
			capture := blockMetadataCapture{watch: watch}
			got, _ := lifecycleStreamCapture(t, env, txs, tc.splits, tc.suffix, 4, &capture)
			requireSameLifecycleOutcome(t, want, got)
			require.NoError(t, capture.Err)
			require.True(t, capture.Record.Watched)
			require.Equal(t, []*transactionMetadata{all.Record.Transactions[2], all.Record.Transactions[9]},
				capture.Record.Transactions, "the watched records equal the unfiltered ones, in block order")
		})
	}

	// The path a block takes when it is not streamed resets its capture for the
	// attempt. The watch set must survive that reset.
	for _, workers := range []int{0, 4} {
		capture := blockMetadataCapture{watch: watch}
		block := env.block(txs)
		block.MarkTransactionSignaturesVerified()
		_, err := processBlock(env.acctsDb, block, env.epochSchedule, workers, nil, &persistedTracker{},
			&lifecycleTail{durable: env.durable}, NewTransactionStatusCache(), false, env.parent, &capture)
		require.NoError(t, err)
		require.NoError(t, capture.Err)
		require.True(t, capture.Record.Watched, "whole block, %d workers", workers)
		require.Equal(t, []*transactionMetadata{all.Record.Transactions[2], all.Record.Transactions[9]},
			capture.Record.Transactions, "whole block, %d workers", workers)
	}

	// A block naming no watched address is still recorded, with no transactions,
	// so coverage can tell it from a block that was never captured.
	nobody, err := NewTransactionWatchSet([]string{solana.PublicKey{0xEE, 0xEE}.String()})
	require.NoError(t, err)
	capture := blockMetadataCapture{watch: nobody}
	got, _ := lifecycleStreamCapture(t, env, txs, []int{3, 7}, 2, 4, &capture)
	requireSameLifecycleOutcome(t, want, got)
	require.NoError(t, capture.Err)
	require.NotNil(t, capture.Record)
	require.Empty(t, capture.Record.Transactions)
	stored, err := capture.Record.historyBlock()
	require.NoError(t, err)
	store, err := transactionhistory.Open(t.TempDir(), [32]byte{1}, transactionhistory.DefaultLimits())
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.WriteBlock(stored))
	snapshot, err := store.Snapshot()
	require.NoError(t, err)
	defer snapshot.Close()
	recorded, err := snapshot.HasBlock(stored.ID)
	require.NoError(t, err)
	require.True(t, recorded)
}
