package replay

import (
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/rent"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func failedTxOutput(errType TransactionErrorType, err error, executed bool) LoadAndExecuteTransactionOutput {
	out := LoadAndExecuteTransactionOutput{
		ProcessingResult: TransactionProcessingResult{
			TransactionError: &TransactionError{ErrorType: errType, InstructionError: err},
		},
		ComputeBudgetLimits: &sealevel.ComputeBudgetLimits{},
		FeeInfo:             &fees.TxFeeInfo{ExecutionFee: 5_000, TotalFee: 5_000},
	}
	if executed {
		out.ExecCtx = &sealevel.ExecutionCtx{}
	}
	return out
}

func TestIncludableFailureClassification(t *testing.T) {
	postExecRent := &rent.RentStateTransitionError{AccountIndex: 1, Err: errors.New("rent state")}
	includable := []LoadAndExecuteTransactionOutput{
		failedTxOutput(TransactionErrorMaxLoadedAccountsDataSizeExceeded, nil, false),
		failedTxOutput(TransactionErrorInvalidProgramForExecution, nil, false),
		failedTxOutput(TransactionErrorProgramAccountNotFound, nil, false),
		failedTxOutput(TransactionErrorInstructionError, sealevel.InstrErrInvalidArgument, true),
		failedTxOutput(TransactionErrorInsufficientFundsForRent, postExecRent, true),
	}
	for _, out := range includable {
		errType := out.ProcessingResult.TransactionError.ErrorType
		assert.Truef(t, includableFailure(out), "%s", errType)
		out.ProcessedAsNoOp = true
		assert.Falsef(t, includableFailure(out), "%s no-op", errType)
	}

	// The same types raised before execution are not includable.
	assert.False(t, includableFailure(failedTxOutput(TransactionErrorInstructionError, sealevel.InstrErrInvalidArgument, false)),
		"compute-budget instruction error")
	assert.False(t, includableFailure(failedTxOutput(TransactionErrorInsufficientFundsForRent, fees.ErrInsufficientFundsForRent, false)),
		"fee-payer rent error")
	assert.False(t, includableFailure(failedTxOutput(TransactionErrorInsufficientFundsForRent, fees.ErrInsufficientFundsForRent, true)),
		"fee-payer rent error on the loaded copy")
	assert.False(t, includableFailure(LoadAndExecuteTransactionOutput{}), "success")

	for errType := TransactionErrorAccountInUse; errType <= TransactionErrorProgramCacheHitMaxLimit; errType++ {
		switch errType {
		case TransactionErrorMaxLoadedAccountsDataSizeExceeded, TransactionErrorInvalidProgramForExecution,
			TransactionErrorProgramAccountNotFound, TransactionErrorInstructionError, TransactionErrorInsufficientFundsForRent:
			continue
		}
		assert.Falsef(t, includableFailure(failedTxOutput(errType, postExecRent, true)), "%s", errType)
	}
}

func signedCommitTestTransaction(t *testing.T, instructions ...solana.Instruction) *solana.Transaction {
	t.Helper()
	tx, err := solana.NewTransaction(instructions, txfixture.TestBlockhash(), solana.TransactionPayer(txfixture.PayerPubkey()))
	require.NoError(t, err)
	payerKey := txfixture.PayerPrivateKey()
	_, err = tx.Sign(func(solana.PublicKey) *solana.PrivateKey { return &payerKey })
	require.NoError(t, err)
	return tx
}

// TestIncludableFailureFromExecution classifies results the pipeline actually
// produces, so each error is checked at the stage that raises it.
func TestIncludableFailureFromExecution(t *testing.T) {
	rentSysvar := sealevel.NewDefaultRentSysvar()
	rentMin := rentSysvar.MinimumBalance(0)
	newAccount := solana.PublicKey{0xEE}
	cases := []struct {
		name       string
		payer      uint64
		tx         func(t *testing.T) *solana.Transaction
		want       TransactionErrorType
		includable bool
	}{
		{
			name:       "instruction error during execution",
			tx:         func(t *testing.T) *solana.Transaction { return signedCommitTestTransaction(t, transfer(20_000_000_000, txfixture.DestPubkey())) },
			want:       TransactionErrorInstructionError,
			includable: true,
		},
		{
			name:       "rent-state check after execution",
			tx:         func(t *testing.T) *solana.Transaction { return signedCommitTestTransaction(t, transfer(1, newAccount)) },
			want:       TransactionErrorInsufficientFundsForRent,
			includable: true,
		},
		{
			name: "fees-only load failure",
			tx: func(t *testing.T) *solana.Transaction {
				return signedCommitTestTransaction(t,
					computebudget.NewSetLoadedAccountsDataSizeLimitInstruction(1).Build(), transfer(1, txfixture.DestPubkey()))
			},
			want:       TransactionErrorMaxLoadedAccountsDataSizeExceeded,
			includable: true,
		},
		{
			name: "compute-budget instruction error before execution",
			tx: func(t *testing.T) *solana.Transaction {
				return signedCommitTestTransaction(t,
					solana.NewInstruction(computebudget.ProgramID, nil, []byte{0xFF}), transfer(1, txfixture.DestPubkey()))
			},
			want: TransactionErrorInstructionError,
		},
		{
			name:  "fee-payer rent check before execution",
			payer: rentMin + 5_000 - 1,
			tx:    func(t *testing.T) *solana.Transaction { return signedCommitTestTransaction(t, transfer(1, txfixture.DestPubkey())) },
			want:  TransactionErrorInsufficientFundsForRent,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slotCtx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			if tc.payer != 0 {
				payer, err := slotCtx.GetAccount(txfixture.PayerPubkey())
				require.NoError(t, err)
				payer.Lamports = tc.payer
				require.NoError(t, slotCtx.SetAccount(txfixture.PayerPubkey(), payer))
			}
			output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
				SlotCtx: slotCtx, Transaction: tc.tx(t), LeanResult: true,
			})
			require.NotNil(t, output.ProcessingResult.TransactionError)
			require.Equal(t, tc.want, output.ProcessingResult.TransactionError.ErrorType)
			assert.Equal(t, tc.includable, includableFailure(output))
		})
	}
}

func transfer(lamports uint64, to solana.PublicKey) solana.Instruction {
	return system.NewTransferInstruction(lamports, txfixture.PayerPubkey(), to).Build()
}
