package blockprod

import (
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestLeaderExecutionOptionsPreserveResults(t *testing.T) {
	for _, testCase := range []string{"success", "instruction_failure", "payer_failure"} {
		t.Run(testCase, func(t *testing.T) {
			env := NewTestEnv(TestEnvConfig{})
			defer env.Close()
			tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(42))
			require.NoError(t, err)
			if testCase == "instruction_failure" {
				tx.Message.Instructions[0].Data[0] = 0xff
			}
			if testCase == "payer_failure" {
				setPayerLamports(t, env, 1)
			}
			input := replay.LoadAndExecuteTransactionInput{SlotCtx: env.SlotCtx, Transaction: tx, LeanResult: true}
			reference := replay.LoadAndExecuteTransaction(input)
			input.SkipTimingMetrics = true
			// A one-object arena also exercises the heap fallback. Repeated calls
			// reset it, while the previous result's account state remains valid.
			input.Arena = arena.New[sealevel.BorrowedAccount](1)
			before := atomic.LoadUint64(&metrics.GlobalBlockReplay.InstructionsAndAccountMetasFromTx.Count)
			beforeDispatch := atomic.LoadUint64(&metrics.GlobalBlockReplay.GetNextIxCtx.Count)
			for i := 0; i < 3; i++ {
				got := replay.LoadAndExecuteTransaction(input)
				require.Equal(t, reference.ProcessingResult, got.ProcessingResult)
				require.Equal(t, reference.FeeInfo, got.FeeInfo)
				require.Equal(t, reference.LoadedAccountsDataSize, got.LoadedAccountsDataSize)
				if reference.ExecCtx != nil {
					require.NotNil(t, got.ExecCtx)
					require.Equal(t, reference.ExecCtx.ComputeMeter.Used(), got.ExecCtx.ComputeMeter.Used())
					require.Equal(t, reference.ExecCtx.TransactionContext.Accounts.Accounts, got.ExecCtx.TransactionContext.Accounts.Accounts)
				}
			}
			require.Equal(t, before, atomic.LoadUint64(&metrics.GlobalBlockReplay.InstructionsAndAccountMetasFromTx.Count))
			require.Equal(t, beforeDispatch, atomic.LoadUint64(&metrics.GlobalBlockReplay.GetNextIxCtx.Count))
		})
	}
}

func TestWorkingBankReusesBorrowedAccountsWithoutChangingMessages(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	payerBefore, err := env.SlotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	payerBalance := payerBefore.Lamports
	destBefore, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	destBalance := destBefore.Lamports
	const count = 130
	for i := 0; i < count; i++ {
		wire := txfixture.MustSignedTransferWire(uint64(i))
		tx, err := solana.TransactionFromBytes(wire)
		require.NoError(t, err)
		result, _ := env.Bank.ForgeTransaction(tx, len(wire))
		require.Equal(t, ForgeAccepted, result)
		after, err := tx.MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, wire, after)
	}
	payerAfter, err := env.SlotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	destAfter, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	const transferred = count * (count + 1) / 2
	require.Equal(t, payerBalance-transferred-count*5000, payerAfter.Lamports)
	require.Equal(t, destBalance+transferred, destAfter.Lamports)
}
