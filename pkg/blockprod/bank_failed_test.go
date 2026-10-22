package blockprod

import (
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func bankLamports(t *testing.T, slotCtx *sealevel.SlotCtx, pubkey solana.PublicKey) uint64 {
	t.Helper()
	acct, err := slotCtx.GetAccount(pubkey)
	if err != nil {
		return 0
	}
	return acct.Lamports
}

func TestWorkingBankIncludesExecutionFailureAsFailed(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	// The fee is payable, but the payer holds 10 SOL and cannot transfer 20.
	tx := mustSignBankTestTransaction(t,
		system.NewTransferInstruction(20_000_000_000, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build())
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	payerBefore := bankLamports(t, env.SlotCtx, txfixture.PayerPubkey())
	destBefore := bankLamports(t, env.SlotCtx, txfixture.DestPubkey())

	result, reason := env.Bank.ForgeTransaction(tx, len(wire))
	require.Equal(t, ForgeAccepted, result)
	require.Equal(t, costmodel.ExceedNone, reason)
	assert.Equal(t, payerBefore-5_000, bankLamports(t, env.SlotCtx, txfixture.PayerPubkey()), "only the fee is charged")
	assert.Equal(t, destBefore, bankLamports(t, env.SlotCtx, txfixture.DestPubkey()))
	assert.Equal(t, uint64(5_000), env.Bank.TxFeeAccumulator().TotalFees)
	assert.Equal(t, []*solana.Transaction{tx}, env.Bank.ForgedTransactions())
	assert.Equal(t, uint64(1), env.Bank.NumSignatures())
	assert.Equal(t, 1, env.Bank.EntryBuilder().PendingCount())
	assert.NotZero(t, env.Bank.CostTracker().BlockCost())

	result, _ = env.Bank.ForgeTransaction(tx, len(wire))
	assert.Equal(t, ForgeDroppedAlreadyProcessed, result, "the failed status is recorded")
	assert.Equal(t, payerBefore-5_000, bankLamports(t, env.SlotCtx, txfixture.PayerPubkey()))
}

func TestWorkingBankDropsFailuresBeforeExecution(t *testing.T) {
	payerKey := txfixture.PayerPrivateKey()
	sign := func(t *testing.T, blockhash solana.Hash, instructions ...solana.Instruction) *solana.Transaction {
		tx, err := solana.NewTransaction(instructions, blockhash, solana.TransactionPayer(txfixture.PayerPubkey()))
		require.NoError(t, err)
		_, err = tx.Sign(func(solana.PublicKey) *solana.PrivateKey { return &payerKey })
		require.NoError(t, err)
		return tx
	}
	transfer := system.NewTransferInstruction(1, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build()
	cases := map[string]*solana.Transaction{
		"compute-budget instruction error": sign(t, txfixture.TestBlockhash(),
			solana.NewInstruction(computebudget.ProgramID, nil, []byte{0xFF}), transfer),
		"unknown blockhash": sign(t, solana.Hash{0x42}, transfer),
	}
	for name, tx := range cases {
		t.Run(name, func(t *testing.T) {
			env := NewTestEnv(TestEnvConfig{})
			defer env.Close()
			payerBefore := bankLamports(t, env.SlotCtx, txfixture.PayerPubkey())

			result, _ := env.Bank.ForgeTransaction(tx, 0)
			require.Equal(t, ForgeDroppedExecution, result)
			assert.Equal(t, payerBefore, bankLamports(t, env.SlotCtx, txfixture.PayerPubkey()))
			assert.Empty(t, env.Bank.ForgedTransactions())
			assert.Zero(t, env.Bank.TxFeeAccumulator().TotalFees)
			assert.Zero(t, env.Bank.EntryBuilder().PendingCount())
		})
	}
}

// TestLeaderFailedTransactionsMatchReplay forges each transaction on a leader
// bank and processes it with replay from the same state: replay must accept
// exactly what the leader includes, with the same balances and fee.
func TestLeaderFailedTransactionsMatchReplay(t *testing.T) {
	rentSysvar := sealevel.NewDefaultRentSysvar()
	rentMin := rentSysvar.MinimumBalance(0)
	newAccount := solana.PublicKey{0xEE}
	transfer := func(lamports uint64, to solana.PublicKey) solana.Instruction {
		return system.NewTransferInstruction(lamports, txfixture.PayerPubkey(), to).Build()
	}
	cases := []struct {
		name     string
		payer    uint64
		tx       func(t *testing.T) *solana.Transaction
		included bool
	}{
		{name: "success", tx: func(t *testing.T) *solana.Transaction { return mustSignBankTestTransaction(t, transfer(1, txfixture.DestPubkey())) }, included: true},
		{name: "instruction error", tx: func(t *testing.T) *solana.Transaction {
			return mustSignBankTestTransaction(t, transfer(20_000_000_000, txfixture.DestPubkey()))
		}, included: true},
		{name: "new account below rent", tx: func(t *testing.T) *solana.Transaction { return mustSignBankTestTransaction(t, transfer(1, newAccount)) }, included: true},
		{name: "payer left below rent", payer: 5_000 + 1_000_000 + 100, tx: func(t *testing.T) *solana.Transaction {
			return mustSignBankTestTransaction(t, transfer(1_000_000, txfixture.DestPubkey()))
		}, included: true},
		{name: "fees-only load failure", tx: func(t *testing.T) *solana.Transaction {
			return mustSignBankTestTransaction(t, computebudget.NewSetLoadedAccountsDataSizeLimitInstruction(1).Build(), transfer(1, txfixture.DestPubkey()))
		}, included: true},
		{name: "fee-payer rent", payer: rentMin + 5_000 - 1, tx: func(t *testing.T) *solana.Transaction {
			return mustSignBankTestTransaction(t, transfer(1, txfixture.DestPubkey()))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := tc.tx(t)
			leader := NewTestEnv(TestEnvConfig{})
			defer leader.Close()
			if tc.payer != 0 {
				setPayerLamports(t, leader, tc.payer)
			}
			result, _ := leader.Bank.ForgeTransaction(tx, 0)
			require.Equal(t, tc.included, result == ForgeAccepted, "leader result %s", result)

			validator := NewTestEnv(TestEnvConfig{})
			defer validator.Close()
			if tc.payer != 0 {
				setPayerLamports(t, validator, tc.payer)
			}
			feeInfo, _, _ := replay.ProcessTransaction(validator.SlotCtx, &sync.WaitGroup{}, tx, nil, nil, nil, false)
			require.Equal(t, tc.included, feeInfo != nil, "replay must accept exactly what the leader includes")
			if !tc.included {
				return
			}
			assert.Equal(t, feeInfo.TotalFee, leader.Bank.TxFeeAccumulator().TotalFees)
			for _, key := range []solana.PublicKey{txfixture.PayerPubkey(), txfixture.DestPubkey(), newAccount} {
				assert.Equal(t, bankLamports(t, validator.SlotCtx, key), bankLamports(t, leader.SlotCtx, key), "%s", key)
			}
		})
	}
}
