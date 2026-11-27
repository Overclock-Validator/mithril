package replay

import (
	"encoding/binary"
	"testing"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func computeUnitPriceIx(price uint64) solana.Instruction {
	data := make([]byte, 9)
	data[0] = 3
	binary.LittleEndian.PutUint64(data[1:], price)
	return solana.NewInstruction(solana.PublicKey(a.ComputeBudgetProgramAddr), nil, data)
}

func computeUnitLimitIx(limit uint32) solana.Instruction {
	data := make([]byte, 5)
	data[0] = 2
	binary.LittleEndian.PutUint32(data[1:], limit)
	return solana.NewInstruction(solana.PublicKey(a.ComputeBudgetProgramAddr), nil, data)
}

func feeTestTx(t *testing.T, payer solana.PublicKey, ixs ...solana.Instruction) *solana.Transaction {
	t.Helper()
	tx, err := solana.NewTransaction(ixs, solana.Hash{1}, solana.TransactionPayer(payer))
	require.NoError(t, err)
	tx.Signatures = make([]solana.Signature, tx.Message.Header.NumRequiredSignatures)
	return tx
}

func TestTransactionPrioritizationFee(t *testing.T) {
	f := features.NewFeaturesDefault()
	payer, to := solana.PublicKey{1}, solana.PublicKey{2}
	transfer := system.NewTransferInstruction(1, payer, to).Build()

	price, writable, ok := TransactionPrioritizationFee(feeTestTx(t, payer, computeUnitPriceIx(5000), transfer), f, nil)
	require.True(t, ok)
	require.EqualValues(t, 5000, price)
	require.ElementsMatch(t, []solana.PublicKey{payer, to}, writable, "programs are read-only")

	_, _, ok = TransactionPrioritizationFee(feeTestTx(t, payer, transfer), f, nil)
	require.True(t, ok, "a transaction without a price pays zero")

	_, _, ok = TransactionPrioritizationFee(feeTestTx(t, payer, computeUnitLimitIx(0), computeUnitPriceIx(9), transfer), f, nil)
	require.False(t, ok, "a zero compute unit limit says nothing about price")

	_, _, ok = TransactionPrioritizationFee(feeTestTx(t, payer, computeUnitPriceIx(1), computeUnitPriceIx(2), transfer), f, nil)
	require.False(t, ok, "a duplicate budget instruction does not sanitize")

	vote := solana.NewInstruction(solana.PublicKey(a.VoteProgramAddr), solana.AccountMetaSlice{solana.Meta(to).WRITE()}, []byte{1})
	_, _, ok = TransactionPrioritizationFee(feeTestTx(t, payer, vote), f, nil)
	require.False(t, ok, "simple votes are left out")
	_, _, ok = TransactionPrioritizationFee(feeTestTx(t, payer, vote, transfer), f, nil)
	require.True(t, ok, "a vote with another instruction is not simple")
}

// A pool reached through a lookup table counts once the table is resolved,
// as replay resolves it before execution.
func TestTransactionPrioritizationFeeCountsLoadedWritables(t *testing.T) {
	f := features.NewFeaturesDefault()
	payer, pool, table := solana.PublicKey{1}, solana.PublicKey{9}, solana.PublicKey{8}
	swap := solana.NewInstruction(solana.PublicKey{7}, solana.AccountMetaSlice{solana.Meta(pool).WRITE()}, []byte{1})
	tx, err := solana.NewTransaction([]solana.Instruction{computeUnitPriceIx(42), swap}, solana.Hash{1},
		solana.TransactionPayer(payer),
		solana.TransactionAddressTables(map[solana.PublicKey]solana.PublicKeySlice{table: {pool}}))
	require.NoError(t, err)
	tx.Signatures = make([]solana.Signature, 1)
	require.NotZero(t, tx.Message.AddressTableLookups.NumLookups())
	require.NoError(t, tx.Message.ResolveLookups())

	price, writable, ok := TransactionPrioritizationFee(tx, f, nil)
	require.True(t, ok)
	require.EqualValues(t, 42, price)
	require.ElementsMatch(t, []solana.PublicKey{payer, pool}, writable)
}

func TestFeeTransactionsAreBufferedPerBank(t *testing.T) {
	rpcCaptures.reset()
	t.Cleanup(rpcCaptures.reset)
	payer, to := solana.PublicKey{1}, solana.PublicKey{2}
	transfer := feeTestTx(t, payer, system.NewTransferInstruction(1, payer, to).Build())
	vote := feeTestTx(t, payer, solana.NewInstruction(solana.PublicKey(a.VoteProgramAddr), nil, []byte{1}))
	stale, bank := &sealevel.SlotCtx{Slot: 10}, &sealevel.SlotCtx{Slot: 10}

	recordFeeTransactions(bank, []*solana.Transaction{transfer})
	require.Nil(t, rpcCaptures.take(bank), "nothing is kept without a fee recorder")

	rpcFeeRecording.Store(true)
	t.Cleanup(func() { rpcFeeRecording.Store(false) })
	recordFeeTransactions(stale, []*solana.Transaction{transfer}) // a discarded speculative bank
	recordFeeTransactions(bank, []*solana.Transaction{transfer})
	recordFeeTransactions(bank, []*solana.Transaction{vote}) // a later group
	bc := rpcCaptures.take(bank)
	require.Equal(t, []*solana.Transaction{transfer, vote}, bc.feeTxs, "votes are left out when the record is built")
	require.False(t, bc.hasHistory(), "fee records alone do not reach the indexer")
	require.Nil(t, rpcCaptures.take(stale), "taking a bank drops discarded banks at its slot")
}

// Streaming records each group as it executes, in block order.
func TestStreamingGroupsRecordFeeTransactions(t *testing.T) {
	rpcCaptures.reset()
	t.Cleanup(rpcCaptures.reset)
	rpcFeeRecording.Store(true)
	t.Cleanup(func() { rpcFeeRecording.Store(false) })
	env := newGroupExecutionEnv(t, 4, 3_200_000)
	defer env.cleanup()
	txs := transferTransactions(t, 6, 999_000)
	require.NoError(t, env.exec.executeTransactionGroup(txs[:2], nil, false))
	require.NoError(t, env.exec.executeTransactionGroup(txs[2:], nil, false))
	bc := rpcCaptures.take(env.exec.slotCtx)
	require.NotNil(t, bc)
	require.Equal(t, txs, bc.feeTxs)
}
