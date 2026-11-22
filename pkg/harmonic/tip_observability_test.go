package harmonic

import (
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func observedTestTips() [tipAccounts]solana.PublicKey {
	return [tipAccounts]solana.PublicKey{txfixture.DestPubkey()}
}

func TestObserveTipIntents(t *testing.T) {
	tips := observedTestTips()
	tx := transfer(t, 1_000)
	before, err := tx.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, TipIntentObservation{Transactions: 1, DirectTipIntents: 1}, observeTipIntents([]*solana.Transaction{tx}, tips))
	after, err := tx.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, before, after, "observation does not modify transaction data")

	t.Run("multiple transfers count one transaction", func(t *testing.T) {
		tx := transfer(t, 1_000)
		tx.Message.Instructions = append(tx.Message.Instructions, tx.Message.Instructions[0])
		require.Equal(t, uint64(1), observeTipIntents([]*solana.Transaction{tx}, tips).DirectTipIntents)
	})
	t.Run("zero transfer and unset account are not tips", func(t *testing.T) {
		require.Zero(t, observeTipIntents([]*solana.Transaction{transfer(t, 0)}, tips).DirectTipIntents)
		require.Zero(t, observeTipIntents([]*solana.Transaction{tx}, [tipAccounts]solana.PublicKey{}).DirectTipIntents)
	})
	t.Run("another program is not a System transfer", func(t *testing.T) {
		tx := transfer(t, 1_000)
		tx.Message.AccountKeys[tx.Message.Instructions[0].ProgramIDIndex] = solana.PublicKey{42}
		require.Zero(t, observeTipIntents([]*solana.Transaction{tx}, tips).DirectTipIntents)
	})
	t.Run("malformed indexes do not alias static keys", func(t *testing.T) {
		tx := transfer(t, 1_000)
		tx.Message.Instructions[0].Accounts[1] = uint16(len(tx.Message.AccountKeys))
		require.Zero(t, observeTipIntents([]*solana.Transaction{tx}, tips).DirectTipIntents)
		tx.Message.Instructions[0].ProgramIDIndex = uint16(len(tx.Message.AccountKeys))
		require.Zero(t, observeTipIntents([]*solana.Transaction{tx}, tips).DirectTipIntents)
	})
	t.Run("lookup coverage is explicit and not resolved", func(t *testing.T) {
		tx := transfer(t, 1_000)
		tx.Message.SetAddressTableLookups([]solana.MessageAddressTableLookup{{
			AccountKey: solana.PublicKey{77}, WritableIndexes: []uint8{0},
		}})
		// A static destination remains observable in a transaction that also
		// uses a lookup, but a loaded destination is not guessed or fetched.
		require.Equal(t, TipIntentObservation{Transactions: 1, DirectTipIntents: 1, LookupTransactions: 1}, observeTipIntents([]*solana.Transaction{tx}, tips))
		tx.Message.Instructions[0].Accounts[1] = uint16(len(tx.Message.AccountKeys))
		require.Equal(t, TipIntentObservation{Transactions: 1, LookupTransactions: 1}, observeTipIntents([]*solana.Transaction{tx}, tips))
		require.False(t, tx.Message.IsResolved())
	})
}

func TestTipIntentRecordingRequiresBundleCommit(t *testing.T) {
	for _, failure := range []string{"reverted_bundle", "rejected_duplicate"} {
		t.Run(failure, func(t *testing.T) {
			h := startExecutorWith(t, ExecutorConfig{TipAccounts: observedTestTips()}, true, nil)
			env := gatedBank(t, costmodel.Limits{})
			h.startSlot(10, env, time.Hour)
			first := transfer(t, 1)
			h.send(t, bundleEvent(10, 1, false, first))
			h.sync(t)
			received := uint64(2)
			if failure == "reverted_bundle" {
				h.send(t, bundleEvent(10, 2, true, transfer(t, 2), transfer(t, 1<<60)))
				received = 3
			} else {
				h.send(t, bundleEvent(10, 2, false, first))
			}
			h.sync(t)
			r := h.end(t, 10)
			require.Equal(t, received, r.TipIntentsReceived.DirectTipIntents)
			require.Equal(t, uint64(1), r.TipIntentsRecorded.DirectTipIntents)
			require.Equal(t, []*solana.Transaction{first}, env.Bank.ForgedTransactions())
		})
	}
}

func TestTipIntentRecordingExcludesAbortedScratch(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	h := startExecutorWith(t, ExecutorConfig{TipAccounts: observedTestTips()}, true, func(uint64) {
		close(entered)
		<-release
	})
	env := gatedBank(t, costmodel.Limits{})
	h.startSlot(10, env, time.Hour)
	h.send(t, bundleEvent(10, 1, true, transfer(t, 1)))
	<-entered
	h.exec.SlotAborted(10, "test abort")
	// The event round trip ensures the abort is processed before the worker
	// returns. Uncommitted scratch must never count as a recorded intent.
	h.send(t, StreamLostEvent{Slot: 1 << 63})
	unblock()
	select {
	case r := <-h.reports:
		require.True(t, r.Aborted)
		require.Equal(t, uint64(1), r.TipIntentsReceived.DirectTipIntents)
		require.Zero(t, r.TipIntentsRecorded.DirectTipIntents)
		require.Empty(t, env.Bank.ForgedTransactions())
	case <-time.After(2 * time.Second):
		t.Fatal("no aborted slot report")
	}
}

func TestTipIntentRecordingCountsIntentNotPayment(t *testing.T) {
	h := startExecutorWith(t, ExecutorConfig{TipAccounts: observedTestTips()}, true, nil)
	env := gatedBank(t, costmodel.Limits{})
	h.startSlot(10, env, time.Hour)
	// A standalone failed transfer can land and pay only its transaction fee.
	// The telemetry deliberately counts its recorded intent, not paid tips.
	h.send(t, bundleEvent(10, 1, false, transfer(t, 1<<60)))
	h.sync(t)
	r := h.end(t, 10)
	require.Equal(t, uint64(1), r.Executed)
	require.Equal(t, uint64(1), r.TipIntentsRecorded.DirectTipIntents)
	require.Len(t, env.Bank.ForgedTransactions(), 1)
}

func TestTipIntentRecordingExcludesDeadlineRejection(t *testing.T) {
	h := startExecutorWith(t, ExecutorConfig{TipAccounts: observedTestTips()}, true, nil)
	clock, now := h.useClock()
	env := gatedBank(t, costmodel.Limits{})
	limit := now.Add(time.Hour + 5*time.Millisecond)
	h.exec.SlotStarted(blockprod.LeaderSlotInfo{Slot: 10, Bank: env.Bank, Start: now, End: now.Add(time.Hour), DrainDeadline: limit})
	clock.set(limit)
	h.send(t, bundleEvent(10, 1, false, transfer(t, 1)))
	h.sync(t)
	r := h.end(t, 10)
	require.Equal(t, uint64(1), r.TipIntentsReceived.DirectTipIntents)
	require.Zero(t, r.TipIntentsRecorded.DirectTipIntents)
	require.Empty(t, env.Bank.ForgedTransactions())
}
