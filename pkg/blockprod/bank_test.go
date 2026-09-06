package blockprod

import (
	"sync"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type captureSink struct {
	batches [][]turbine.Entry
	bytes   []int
}

func (s *captureSink) OnEntryBatch(entries []turbine.Entry, batchBytes int) {
	s.batches = append(s.batches, append([]turbine.Entry(nil), entries...))
	s.bytes = append(s.bytes, batchBytes)
}

func TestWorkingBankForgesTransfer(t *testing.T) {
	sink := &captureSink{}
	env := NewTestEnv(TestEnvConfig{Sink: sink})
	defer env.Close()

	wire := txfixture.MustSignedTransferWire(0)
	result, reason := env.Bank.Forge(wire)
	assert.Equal(t, ForgeAccepted, result)
	assert.Equal(t, costmodel.ExceedNone, reason)
	assert.Equal(t, 1, env.Bank.EntryBuilder().PendingCount())
}

func TestWorkingBankRejectsExactDuplicateMessage(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()

	wire := txfixture.MustSignedTransferWire(0)
	result, reason := env.Bank.Forge(wire)
	require.Equal(t, ForgeAccepted, result)
	require.Equal(t, costmodel.ExceedNone, reason)

	destAfterFirst, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	costAfterFirst := env.Bank.CostTracker().BlockCost()

	result, reason = env.Bank.Forge(wire)
	require.Equal(t, ForgeDroppedAlreadyProcessed, result)
	require.Equal(t, costmodel.ExceedNone, reason)

	destAfterDuplicate, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	assert.Equal(t, destAfterFirst.Lamports, destAfterDuplicate.Lamports)
	assert.Equal(t, costAfterFirst, env.Bank.CostTracker().BlockCost())
	assert.Len(t, env.Bank.ForgedTransactions(), 1)
	assert.Equal(t, uint64(1), env.Bank.NumSignatures())
	assert.Equal(t, 1, env.Bank.EntryBuilder().PendingCount())
}

func TestWorkingBankRejectsExactAncestorMessageBeforeExecution(t *testing.T) {
	tx := mustBankTestTransaction(t, txfixture.MustSignedTransferWire(0))
	statuses := mustAncestorTransactionStatusView(t, tx)
	env := NewTestEnv(TestEnvConfig{TransactionStatuses: statuses})
	defer env.Close()

	destBefore, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	result, reason := env.Bank.ForgeTransaction(tx, len(txfixture.MustSignedTransferWire(0)))
	require.Equal(t, ForgeDroppedAlreadyProcessed, result)
	require.Equal(t, costmodel.ExceedNone, reason)

	destAfter, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	assert.Equal(t, destBefore.Lamports, destAfter.Lamports)
	assert.Zero(t, env.Bank.CostTracker().BlockCost())
	assert.Empty(t, env.Bank.ForgedTransactions())
	assert.Zero(t, env.Bank.EntryBuilder().PendingCount())
}

func TestWorkingBankMissingAncestorViewFailsClosed(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	env.Bank.ancestorStatuses = nil

	result, reason := env.Bank.Forge(txfixture.MustSignedTransferWire(0))
	require.Equal(t, ForgeDroppedNoLeader, result)
	require.Equal(t, costmodel.ExceedNone, reason)
	assert.Zero(t, env.Bank.CostTracker().BlockCost())
	assert.Empty(t, env.Bank.ForgedTransactions())
}

func TestWorkingBankIncompleteAncestorViewFailsClosed(t *testing.T) {
	cache, err := replay.NewTransactionStatusCacheFromSnapshot(nil)
	require.NoError(t, err)
	env := NewTestEnv(TestEnvConfig{TransactionStatuses: cache.View()})
	defer env.Close()

	result, reason := env.Bank.Forge(txfixture.MustSignedTransferWire(0))
	require.Equal(t, ForgeDroppedNoLeader, result)
	require.Equal(t, costmodel.ExceedNone, reason)
	assert.Zero(t, env.Bank.CostTracker().BlockCost())
	assert.Empty(t, env.Bank.ForgedTransactions())
}

func TestWorkingBankRejectsResignedAncestorMessage(t *testing.T) {
	wire := txfixture.MustSignedTransferWire(0)
	ancestor := mustBankTestTransaction(t, wire)
	statuses := mustAncestorTransactionStatusView(t, ancestor)
	env := NewTestEnv(TestEnvConfig{TransactionStatuses: statuses})
	defer env.Close()

	retry := *ancestor
	retry.Signatures = append([]solana.Signature(nil), ancestor.Signatures...)
	retry.Signatures[0][0] ^= 0xff
	result, reason := env.Bank.ForgeTransaction(&retry, len(wire))
	require.Equal(t, ForgeDroppedAlreadyProcessed, result)
	require.Equal(t, costmodel.ExceedNone, reason)
	assert.Zero(t, env.Bank.CostTracker().BlockCost())
	assert.Empty(t, env.Bank.ForgedTransactions())
}

func TestWorkingBankAllowsAncestorPayloadWithDifferentRecentBlockhash(t *testing.T) {
	wire := txfixture.MustSignedTransferWire(0)
	producerTx := mustBankTestTransaction(t, wire)
	ancestor := *producerTx
	ancestor.Message.RecentBlockhash = solana.Hash{0x42}
	statuses := mustAncestorTransactionStatusView(t, &ancestor)
	env := NewTestEnv(TestEnvConfig{TransactionStatuses: statuses})
	defer env.Close()

	result, reason := env.Bank.ForgeTransaction(producerTx, len(wire))
	require.Equal(t, ForgeAccepted, result)
	require.Equal(t, costmodel.ExceedNone, reason)
	assert.NotZero(t, env.Bank.CostTracker().BlockCost())
	assert.Len(t, env.Bank.ForgedTransactions(), 1)
}

func TestWorkingBankPinnedAncestorViewSurvivesConcurrentReplayUnwind(t *testing.T) {
	wire := txfixture.MustSignedTransferWire(0)
	ancestor := mustBankTestTransaction(t, wire)
	cache := replay.NewTransactionStatusCache()
	require.NoError(t, cache.CommitBlock(bankTestStatusBlock(41, ancestor)))
	pinned := cache.View()
	env := NewTestEnv(TestEnvConfig{TransactionStatuses: pinned})
	defer env.Close()

	replacement := mustBankTestTransaction(t, txfixture.MustSignedTransferWire(1))
	start := make(chan struct{})
	updateErr := make(chan error, 1)
	go func() {
		<-start
		for range 64 {
			cache.Unwind(41)
			if err := cache.CommitBlock(bankTestStatusBlock(41, replacement)); err != nil {
				updateErr <- err
				return
			}
		}
		updateErr <- nil
	}()

	close(start)
	retry := *ancestor
	retry.Signatures = append([]solana.Signature(nil), ancestor.Signatures...)
	retry.Signatures[0][0] ^= 0xff
	result, reason := env.Bank.ForgeTransaction(&retry, len(wire))
	require.NoError(t, <-updateErr)
	require.Equal(t, ForgeDroppedAlreadyProcessed, result)
	require.Equal(t, costmodel.ExceedNone, reason)
	assert.Zero(t, env.Bank.CostTracker().BlockCost())
	assert.Empty(t, env.Bank.ForgedTransactions())

	contains, err := cache.View().ContainsTransaction(ancestor)
	require.NoError(t, err)
	assert.False(t, contains, "the replay cache should now expose the replacement branch")
}

func mustBankTestTransaction(t *testing.T, wire []byte) *solana.Transaction {
	t.Helper()
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	return tx
}

func mustAncestorTransactionStatusView(t *testing.T, tx *solana.Transaction) *replay.TransactionStatusView {
	t.Helper()
	cache := replay.NewTransactionStatusCache()
	require.NoError(t, cache.CommitBlock(bankTestStatusBlock(41, tx)))
	return cache.View()
}

func bankTestStatusBlock(slot uint64, txs ...*solana.Transaction) *b.Block {
	return &b.Block{Slot: slot, ParentSlot: slot - 1, Transactions: txs}
}

func TestWorkingBankRejectsSameMessageWithDifferentSignature(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()

	wire := txfixture.MustSignedTransferWire(0)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	require.NotEmpty(t, tx.Signatures)

	result, _ := env.Bank.ForgeTransaction(tx, len(wire))
	require.Equal(t, ForgeAccepted, result)
	destAfterFirst, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)

	duplicate := *tx
	duplicate.Signatures = append([]solana.Signature(nil), tx.Signatures...)
	duplicate.Signatures[0][0] ^= 0xff
	result, reason := env.Bank.ForgeTransaction(&duplicate, len(wire))
	require.Equal(t, ForgeDroppedAlreadyProcessed, result)
	require.Equal(t, costmodel.ExceedNone, reason)
	destAfterDuplicate, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	assert.Equal(t, destAfterFirst.Lamports, destAfterDuplicate.Lamports)
	assert.Len(t, env.Bank.ForgedTransactions(), 1)
	assert.Equal(t, uint64(1), env.Bank.NumSignatures())
	assert.Equal(t, 1, env.Bank.EntryBuilder().PendingCount())
}

func TestWorkingBankConcurrentDuplicateMessageCommitsOnce(t *testing.T) {
	sink := &captureSink{}
	env := NewTestEnv(TestEnvConfig{Sink: sink})
	defer env.Close()

	wire := txfixture.MustSignedTransferWire(0)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	expectedCost, err := costmodel.EstimateTransactionCost(tx, env.SlotCtx.Features)
	require.NoError(t, err)

	destBefore, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	destLamportsBefore := destBefore.Lamports
	payerBefore, err := env.SlotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	payerLamportsBefore := payerBefore.Lamports

	const submissions = 64
	type outcome struct {
		result ForgeResult
		reason costmodel.ExceedReason
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, submissions)
	var wg sync.WaitGroup
	wg.Add(submissions)
	for range submissions {
		go func() {
			defer wg.Done()
			<-start
			result, reason := env.Bank.Forge(wire)
			outcomes <- outcome{result: result, reason: reason}
		}()
	}
	close(start)
	wg.Wait()
	close(outcomes)

	accepted := 0
	alreadyProcessed := 0
	for got := range outcomes {
		require.Equal(t, costmodel.ExceedNone, got.reason)
		switch got.result {
		case ForgeAccepted:
			accepted++
		case ForgeDroppedAlreadyProcessed:
			alreadyProcessed++
		default:
			t.Fatalf("unexpected forge result: %s", got.result)
		}
	}
	require.Equal(t, 1, accepted)
	require.Equal(t, submissions-1, alreadyProcessed)

	destAfter, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	assert.Equal(t, destLamportsBefore+1, destAfter.Lamports)
	payerAfter, err := env.SlotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	assert.Equal(t, payerLamportsBefore-5_001, payerAfter.Lamports)
	assert.Equal(t, expectedCost.Sum(), env.Bank.CostTracker().BlockCost())
	feeInfo := env.Bank.TxFeeAccumulator()
	assert.Equal(t, uint64(5_000), feeInfo.ExecutionFees)
	assert.Zero(t, feeInfo.PriorityFees)
	assert.Equal(t, uint64(5_000), feeInfo.TotalFees)
	assert.Len(t, env.Bank.ForgedTransactions(), 1)
	assert.Equal(t, uint64(1), env.Bank.NumSignatures())
	assert.Equal(t, 1, env.Bank.EntryBuilder().PendingCount())
	assert.Empty(t, sink.batches)

	env.Bank.Freeze()
	assert.Equal(t, 0, env.Bank.EntryBuilder().PendingCount())
	require.Len(t, sink.batches, 1)
	require.Len(t, sink.batches[0], 1)
	assert.Len(t, sink.batches[0][0].Txns, 1)
	assert.Greater(t, sink.bytes[0], 0)
}

func TestWorkingBankLifecycleWinsOverAlreadyProcessed(t *testing.T) {
	tests := []struct {
		name string
		stop func(*WorkingBank)
	}{
		{name: "freeze", stop: (*WorkingBank).Freeze},
		{name: "close", stop: (*WorkingBank).Close},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := NewTestEnv(TestEnvConfig{})
			defer env.Close()

			wire := txfixture.MustSignedTransferWire(0)
			result, reason := env.Bank.Forge(wire)
			require.Equal(t, ForgeAccepted, result)
			require.Equal(t, costmodel.ExceedNone, reason)

			destAfterFirst, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
			require.NoError(t, err)
			destLamportsAfterFirst := destAfterFirst.Lamports
			costAfterFirst := env.Bank.CostTracker().BlockCost()
			feesAfterFirst := env.Bank.TxFeeAccumulator()

			tt.stop(env.Bank)
			result, reason = env.Bank.Forge(wire)
			require.Equal(t, ForgeDroppedNoLeader, result)
			require.Equal(t, costmodel.ExceedNone, reason)

			destAfterDuplicate, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
			require.NoError(t, err)
			assert.Equal(t, destLamportsAfterFirst, destAfterDuplicate.Lamports)
			assert.Equal(t, costAfterFirst, env.Bank.CostTracker().BlockCost())
			assert.Equal(t, feesAfterFirst, env.Bank.TxFeeAccumulator())
			assert.Len(t, env.Bank.ForgedTransactions(), 1)
			assert.Equal(t, uint64(1), env.Bank.NumSignatures())
		})
	}
}

func TestWorkingBankDroppedTransactionDoesNotPoisonMessageHash(t *testing.T) {
	limits := costmodel.DefaultLimits()
	limits.BlockCost = 1
	env := NewTestEnv(TestEnvConfig{Limits: limits})
	defer env.Close()

	wire := txfixture.MustSignedTransferWire(0)
	destBefore, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	destLamportsBefore := destBefore.Lamports

	result, reason := env.Bank.Forge(wire)
	require.Equal(t, ForgeDroppedCost, result)
	require.Equal(t, costmodel.ExceedBlockCost, reason)
	assert.Zero(t, env.Bank.CostTracker().BlockCost())
	feeInfo := env.Bank.TxFeeAccumulator()
	assert.Zero(t, feeInfo.ExecutionFees)
	assert.Zero(t, feeInfo.PriorityFees)
	assert.Zero(t, feeInfo.TotalFees)
	assert.Empty(t, env.Bank.ForgedTransactions())
	assert.Zero(t, env.Bank.EntryBuilder().PendingCount())
	destAfterDrop, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	assert.Equal(t, destLamportsBefore, destAfterDrop.Lamports)

	// Relax the test-only tracker after proving the first admission was dropped.
	// Retrying the identical message must still be eligible for execution.
	env.Bank.mu.Lock()
	env.Bank.costs = costmodel.NewCostTracker(costmodel.DefaultLimits())
	env.Bank.mu.Unlock()
	result, reason = env.Bank.Forge(wire)
	require.Equal(t, ForgeAccepted, result)
	require.Equal(t, costmodel.ExceedNone, reason)

	result, reason = env.Bank.Forge(wire)
	require.Equal(t, ForgeDroppedAlreadyProcessed, result)
	require.Equal(t, costmodel.ExceedNone, reason)
	assert.Len(t, env.Bank.ForgedTransactions(), 1)
	assert.Equal(t, uint64(1), env.Bank.NumSignatures())
	assert.Equal(t, 1, env.Bank.EntryBuilder().PendingCount())
}

func TestWorkingBankDropsInvalidWire(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()

	result, _ := env.Bank.Forge([]byte{0x00})
	assert.Equal(t, ForgeDroppedParse, result)
}

func TestWorkingBankRejectsStalePacketAfterFreeze(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()

	env.Bank.Freeze()
	result, reason := env.Bank.Forge(txfixture.MustSignedTransferWire(0))
	assert.Equal(t, ForgeDroppedNoLeader, result)
	assert.Equal(t, costmodel.ExceedNone, reason)
	assert.Empty(t, env.Bank.ForgedTransactions())
}

func TestWorkingBankDropsWhenBlockCostExceeded(t *testing.T) {
	limits := costmodel.DefaultLimits()
	limits.BlockCost = 1
	env := NewTestEnv(TestEnvConfig{Limits: limits})
	defer env.Close()

	wire := txfixture.MustSignedTransferWire(0)
	result, reason := env.Bank.Forge(wire)
	assert.Equal(t, ForgeDroppedCost, result)
	assert.Equal(t, costmodel.ExceedBlockCost, reason)
}

func TestWorkingBankFlushesOnBatchLimit(t *testing.T) {
	sink := &captureSink{}
	limits := costmodel.DefaultLimits()
	limits.MaxBatchBytes = 300
	env := NewTestEnv(TestEnvConfig{Limits: limits, Sink: sink})
	defer env.Close()

	for seq := uint64(0); seq < 3; seq++ {
		wire := txfixture.MustSignedTransferWire(seq)
		result, _ := env.Bank.Forge(wire)
		require.Equal(t, ForgeAccepted, result)
	}

	assert.GreaterOrEqual(t, len(sink.batches), 1)
	totalTxns := 0
	for _, batch := range sink.batches {
		for _, entry := range batch {
			totalTxns += len(entry.Txns)
		}
	}
	assert.GreaterOrEqual(t, totalTxns, 1)
}

func TestEntryBuilderFlush(t *testing.T) {
	builder := NewEntryBuilder(costmodel.DefaultLimits(), solana.Hash{0xcd})
	wire := txfixture.MustSignedTransferWire(0)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	_, _, _ = builder.Append(*tx, len(wire))

	entries, batchBytes := builder.Flush()
	require.Len(t, entries, 1)
	assert.Equal(t, 1, len(entries[0].Txns))
	assert.Greater(t, batchBytes, 0)
}

func TestEntryBuilderDefaultTargetCoalescesTransactions(t *testing.T) {
	builder := NewEntryBuilder(costmodel.DefaultLimits(), solana.Hash{0xcd})
	for seq := uint64(0); seq < 2; seq++ {
		wire := txfixture.MustSignedTransferWire(seq)
		tx, err := solana.TransactionFromBytes(wire)
		require.NoError(t, err)
		entries, _, flushed := builder.Append(*tx, len(wire))
		assert.False(t, flushed)
		assert.Empty(t, entries)
	}
	assert.Equal(t, 2, builder.PendingCount())
}

func TestControllerWorkingBank(t *testing.T) {
	controller := NewController()
	assert.Nil(t, controller.WorkingBank())

	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	controller.SetWorkingBank(env.Bank)
	assert.Equal(t, env.Bank, controller.WorkingBank())
}
