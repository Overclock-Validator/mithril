package rpcserver

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendTransactionNoOpFailureOmitsFeeAndResources(t *testing.T) {
	output := replay.LoadAndExecuteTransactionOutput{
		ProcessingResult: replay.TransactionProcessingResult{
			TransactionError: &replay.TransactionError{
				ErrorType: replay.TransactionErrorInvalidAccountForFee,
			},
		},
		FeeInfo:                &fees.TxFeeInfo{},
		ComputeBudgetLimits:    &sealevel.ComputeBudgetLimits{ComputeUnitLimit: 789_123},
		LoadedAccountsDataSize: 45_678,
		ProcessedAsNoOp:        true,
	}

	result := sendTransactionFailureResultFromOutput(output)
	assert.Nil(t, result.Fee)
	require.NotNil(t, result.UnitsConsumed)
	require.NotNil(t, result.LoadedAccountsDataSize)
	assert.Zero(t, *result.UnitsConsumed)
	assert.Zero(t, *result.LoadedAccountsDataSize)
}

func TestStrictFeePayerFailureOmitsFee(t *testing.T) {
	output := replay.LoadAndExecuteTransactionOutput{
		ProcessingResult: replay.TransactionProcessingResult{
			TransactionError: &replay.TransactionError{
				ErrorType: replay.TransactionErrorInvalidAccountForFee,
			},
		},
		FeeInfo: &fees.TxFeeInfo{TotalFee: 5_000},
	}

	assert.False(t, processingOutputChargesFee(output))
	assert.Nil(t, sendTransactionFailureResultFromOutput(output).Fee)

	output.ProcessingResult.TransactionError.ErrorType = replay.TransactionErrorProgramAccountNotFound
	assert.True(t, processingOutputChargesFee(output), "fees-only load failures commit the fee")
	result := sendTransactionFailureResultFromOutput(output)
	require.NotNil(t, result.Fee)
	assert.Equal(t, uint64(5_000), *result.Fee)
}

func TestNonExecutedSimulationBalances(t *testing.T) {
	pre := []uint64{100_000, 2_000, 0}
	noOp := replay.LoadAndExecuteTransactionOutput{
		ProcessingResult: replay.TransactionProcessingResult{
			TransactionError: &replay.TransactionError{ErrorType: replay.TransactionErrorInvalidAccountForFee},
		},
		FeeInfo:         &fees.TxFeeInfo{},
		ProcessedAsNoOp: true,
	}
	assert.Equal(t, pre, nonExecutedSimulationPostBalances(noOp, pre))

	feesOnly := replay.LoadAndExecuteTransactionOutput{
		ProcessingResult: replay.TransactionProcessingResult{
			TransactionError: &replay.TransactionError{ErrorType: replay.TransactionErrorProgramAccountNotFound},
		},
		FeeInfo: &fees.TxFeeInfo{TotalFee: 5_000},
	}
	assert.Equal(t, []uint64{95_000, 2_000, 0}, nonExecutedSimulationPostBalances(feesOnly, pre))
	assert.Equal(t, []uint64{100_000, 2_000, 0}, pre, "helper must not mutate pre-balances")
}

func TestEarlyProcessingResultsStillCollectTokenBalances(t *testing.T) {
	payerKey := solana.PublicKey{0xA1}
	tokenKey := solana.PublicKey{0xA2}
	mintKey := solana.PublicKey{0xA3}
	ownerKey := solana.PublicKey{0xA4}

	tokenData := make([]byte, tokenAccountSize)
	copy(tokenData[tokenAccountMintOffset:], mintKey[:])
	copy(tokenData[tokenAccountOwnerOffset:], ownerKey[:])
	binary.LittleEndian.PutUint64(tokenData[tokenAccountAmountOffset:], 42)

	slotCtx := &sealevel.SlotCtx{Accounts: accounts.NewMemAccounts()}
	require.NoError(t, slotCtx.SetAccount(payerKey, &accounts.Account{
		Key: payerKey, Lamports: 1_000_000, Owner: addresses.SystemProgramAddr,
	}))
	require.NoError(t, slotCtx.SetAccount(tokenKey, &accounts.Account{
		Key: tokenKey, Lamports: 2_000_000, Owner: splTokenProgramID, Data: tokenData,
	}))
	tx := &solana.Transaction{Message: solana.Message{
		AccountKeys: []solana.PublicKey{payerKey, tokenKey, splTokenProgramID},
	}}
	output := replay.LoadAndExecuteTransactionOutput{
		ProcessingResult: replay.TransactionProcessingResult{
			TransactionError: &replay.TransactionError{ErrorType: replay.TransactionErrorInvalidAccountForFee},
		},
	}

	pre, post := simulationAccountSnapshotsForOutput(slotCtx, tx, output)
	require.Len(t, pre, len(tx.Message.AccountKeys))
	require.Len(t, post, len(tx.Message.AccountKeys))
	preTokens := tokenBalancesFromAccounts(pre, nil, 0)
	postTokens := tokenBalancesFromAccounts(post, nil, 0)
	require.Len(t, preTokens, 1)
	require.Len(t, postTokens, 1)
	assert.Equal(t, uint8(1), preTokens[0].AccountIndex)
	assert.Equal(t, "42", preTokens[0].UiTokenAmount.Amount)
	assert.Equal(t, preTokens, postTokens)
}
