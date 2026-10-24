package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func TestLoadAndExecuteTransactionInstructionErrorIndex(t *testing.T) {
	for _, tc := range []struct {
		name      string
		amounts   []uint64
		rentError bool
		wantIndex uint8
	}{
		{name: "first instruction", amounts: []uint64{20_000_000_000}, wantIndex: 0},
		{name: "later instruction", amounts: []uint64{8, 20_000_000_000}, wantIndex: 1},
		{name: "rent error has no instruction index", amounts: []uint64{8}, rentError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slotCtx, cleanup := newCommitTestSlotCtx()
			t.Cleanup(cleanup)
			if tc.rentError {
				destination, err := slotCtx.GetAccount(txfixture.DestPubkey())
				require.NoError(t, err)
				rent := sealevel.NewDefaultRentSysvar()
				destination.Lamports = rent.MinimumBalance(0) - 10
				require.NoError(t, slotCtx.SetAccount(txfixture.DestPubkey(), destination))
			}
			var instructions []solana.Instruction
			for _, amount := range tc.amounts {
				instructions = append(instructions, system.NewTransferInstruction(amount, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build())
			}
			tx := signLifecycleInstructions(t, instructions...)
			output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{SlotCtx: slotCtx, Transaction: tx, LeanResult: true})
			txErr := output.ProcessingResult.TransactionError
			require.NotNil(t, txErr)
			if tc.rentError {
				require.Equal(t, TransactionErrorInsufficientFundsForRent, txErr.ErrorType)
				require.Nil(t, txErr.InstructionIndex)
				require.NotNil(t, txErr.AccountIndex)
				require.Equal(t, uint8(1), *txErr.AccountIndex)
				return
			}
			require.Equal(t, TransactionErrorInstructionError, txErr.ErrorType)
			require.NotNil(t, txErr.InstructionIndex)
			require.Equal(t, tc.wantIndex, *txErr.InstructionIndex)
			require.ErrorIs(t, txErr.InstructionError, sealevel.SystemProgErrResultWithNegativeLamports)
			require.Nil(t, txErr.AccountIndex)
		})
	}
}
