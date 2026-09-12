package replay

import (
	"encoding/binary"
	"math"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplySuccessfulTransactionCommitsTransfer(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()

	wire := txfixture.MustSignedTransferWire(0)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)

	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:     slotCtx,
		Transaction: tx,
	})
	require.Nil(t, output.ProcessingResult.TransactionError)
	require.NotNil(t, output.ProcessingResult.ProcessedTransaction)
	require.NotNil(t, output.ProcessingResult.ProcessedTransaction.Executed)
	require.Len(t, output.PreBalances, len(tx.Message.AccountKeys))
	require.NotEmpty(t, output.ExecutionResult.AccountUpdates)
	require.Len(t, output.ProcessingResult.ProcessedTransaction.Executed.LoadedTransaction.Accounts, len(tx.Message.AccountKeys))

	payerBefore, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	destBefore, err := slotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)

	require.NoError(t, ApplySuccessfulTransaction(slotCtx, output))

	payerAfter, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	destAfter, err := slotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)

	assert.Less(t, payerAfter.Lamports, payerBefore.Lamports)
	assert.Greater(t, destAfter.Lamports, destBefore.Lamports)
}

func TestApplySuccessfulTransactionCommitsV1TransferWithInlineBudget(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	slotCtx.Features.EnableFeature(features.EnableTxV1, 0)

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(7))
	require.NoError(t, err)
	_, err = tx.Message.SetVersion(solana.MessageVersionV1)
	require.NoError(t, err)
	tx.Message.TransactionConfig = solana.TransactionConfig{}.
		WithPriorityFee(42).
		WithComputeUnitLimit(200_000).
		WithLoadedAccountsDataSizeLimit(sealevel.MaxLoadedAccountsDataSizeBytes)
	payerPrivateKey := txfixture.PayerPrivateKey()
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key == txfixture.PayerPubkey() {
			return &payerPrivateKey
		}
		return nil
	})
	require.NoError(t, err)
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	tx, err = solana.TransactionFromBytes(wire)
	require.NoError(t, err)

	payerBefore, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	destBefore, err := slotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:     slotCtx,
		Transaction: tx,
	})
	require.Nil(t, output.ProcessingResult.TransactionError)
	require.NotNil(t, output.FeeInfo)
	assert.Equal(t, uint64(42), output.FeeInfo.PriorityFee)
	assert.Equal(t, uint64(5_042), output.FeeInfo.TotalFee)
	assert.Equal(t, uint32(3*txAcctBaseSize), output.LoadedAccountsDataSize)
	require.NoError(t, ApplySuccessfulTransaction(slotCtx, output))

	payerAfter, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	destAfter, err := slotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	assert.Equal(t, payerBefore.Lamports-5_042-8, payerAfter.Lamports)
	assert.Equal(t, destBefore.Lamports+8, destAfter.Lamports)
}

func TestApplyFeesOnlyTransactionCommitsProcessableLoadFailures(t *testing.T) {
	for _, errType := range []TransactionErrorType{
		TransactionErrorMaxLoadedAccountsDataSizeExceeded,
		TransactionErrorInvalidProgramForExecution,
		TransactionErrorProgramAccountNotFound,
	} {
		t.Run(errType.String(), func(t *testing.T) {
			slotCtx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
			require.NoError(t, err)

			payerBefore, err := slotCtx.GetAccount(txfixture.PayerPubkey())
			require.NoError(t, err)
			output := LoadAndExecuteTransactionOutput{
				ProcessingResult: TransactionProcessingResult{
					TransactionError: &TransactionError{ErrorType: errType},
				},
				ComputeBudgetLimits: &sealevel.ComputeBudgetLimits{},
				FeeInfo: &fees.TxFeeInfo{
					ExecutionFee: 5_000,
					TotalFee:     5_000,
				},
			}

			feeInfo, err := ApplyFeesOnlyTransaction(slotCtx, tx, output)
			require.NoError(t, err)
			require.NotNil(t, feeInfo)
			assert.Equal(t, uint64(5_000), feeInfo.TotalFee)
			payerAfter, err := slotCtx.GetAccount(txfixture.PayerPubkey())
			require.NoError(t, err)
			assert.Equal(t, payerBefore.Lamports-5_000, payerAfter.Lamports)
		})
	}
}

func TestApplyFeesOnlyTransactionRejectsUnprocessableError(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)
	payerBefore, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)

	output := LoadAndExecuteTransactionOutput{
		ProcessingResult: TransactionProcessingResult{
			TransactionError: &TransactionError{ErrorType: TransactionErrorUnsupportedVersion},
		},
		ComputeBudgetLimits: &sealevel.ComputeBudgetLimits{},
		FeeInfo:             &fees.TxFeeInfo{ExecutionFee: 5_000, TotalFee: 5_000},
	}
	feeInfo, err := ApplyFeesOnlyTransaction(slotCtx, tx, output)
	assert.Nil(t, feeInfo)
	assert.ErrorContains(t, err, "not a processable fees-only load failure")
	payerAfter, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	assert.Equal(t, payerBefore.Lamports, payerAfter.Lamports)
}

func TestApplyFeesOnlyTransactionRejectsIncompleteProcessingState(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)
	payerBefore, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)

	output := LoadAndExecuteTransactionOutput{
		ProcessingResult: TransactionProcessingResult{
			TransactionError: &TransactionError{ErrorType: TransactionErrorProgramAccountNotFound},
		},
		ComputeBudgetLimits: &sealevel.ComputeBudgetLimits{},
		// FeeInfo intentionally omitted: this output did not complete the
		// validated fee-payer phase and is unsafe to publish.
	}
	feeInfo, err := ApplyFeesOnlyTransaction(slotCtx, tx, output)
	assert.Nil(t, feeInfo)
	assert.ErrorContains(t, err, "missing required processing state")
	payerAfter, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	assert.Equal(t, payerBefore.Lamports, payerAfter.Lamports)
}

func TestProcessTransactionSIMD0290InvalidPayerNoOp(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	slotCtx.Features.EnableFeature(features.EnableTxV1, 0)
	slotCtx.Features.EnableFeature(features.RelaxFeePayerConstraint, 0)

	tx := signedV1Transfer(t, solana.TransactionConfig{}.
		WithComputeUnitLimit(12_345).
		WithLoadedAccountsDataSizeLimit(54_321))
	payer, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	payer.Owner = addresses.VoteProgramAddr
	require.NoError(t, slotCtx.SetAccount(txfixture.PayerPubkey(), payer))
	payerBefore := payer.Lamports

	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:     slotCtx,
		Transaction: tx,
		LeanResult:  true,
	})
	require.NotNil(t, output.ProcessingResult.TransactionError)
	assert.True(t, output.ProcessedAsNoOp)
	require.NotNil(t, output.FeeInfo)
	assert.Zero(t, output.FeeInfo.TotalFee)
	assert.Equal(t, uint32(54_321), output.LoadedAccountsDataSize)
	assert.Nil(t, output.ExecCtx)

	var sigverify sync.WaitGroup
	feeInfo, computeUnits, processErr := ProcessTransaction(slotCtx, &sigverify, tx, nil, nil, nil, false)
	sigverify.Wait()
	require.ErrorIs(t, processErr, fees.ErrInvalidAccountForFee)
	require.NotNil(t, feeInfo)
	assert.Zero(t, feeInfo.TotalFee)
	assert.Equal(t, uint64(12_345), computeUnits)
	payerAfter, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	assert.Equal(t, payerBefore, payerAfter.Lamports)
	assert.Empty(t, slotCtx.ModifiedAccts)
}

func TestProcessTransactionInvalidPayerIsUnprocessableBeforeSIMD0290(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	slotCtx.Features.EnableFeature(features.EnableTxV1, 0)

	tx := signedV1Transfer(t, solana.TransactionConfig{}.
		WithComputeUnitLimit(12_345).
		WithLoadedAccountsDataSizeLimit(54_321))
	payer, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	payer.Owner = addresses.VoteProgramAddr
	require.NoError(t, slotCtx.SetAccount(txfixture.PayerPubkey(), payer))

	var sigverify sync.WaitGroup
	feeInfo, computeUnits, processErr := ProcessTransaction(slotCtx, &sigverify, tx, nil, nil, nil, false)
	sigverify.Wait()
	require.ErrorIs(t, processErr, fees.ErrInvalidAccountForFee)
	assert.Nil(t, feeInfo)
	assert.Zero(t, computeUnits)
}

func TestSIMD0290NeverConvertsDurableNonceFeePayerFailureToNoOp(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	slotCtx.Features.EnableFeature(features.EnableTxV1, 0)
	slotCtx.Features.EnableFeature(features.RelaxFeePayerConstraint, 0)

	const nonceDiscriminator = sealevel.SystemProgramInstrTypeAdvanceNonceAccount
	nonceKey := solana.PublicKey{0xD5}
	durableNonce := solana.Hash{0xAA}
	nonceState := sealevel.NonceStateVersions{
		Type: sealevel.NonceVersionCurrent,
		Current: sealevel.NonceData{
			IsInitialized: true,
			Authority:     txfixture.PayerPubkey(),
			DurableNonce:  durableNonce,
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
		},
	}
	nonceData, err := nonceState.Marshal()
	require.NoError(t, err)
	require.NoError(t, slotCtx.SetAccount(nonceKey, &accounts.Account{
		Key: nonceKey, Lamports: 10_000_000, Owner: addresses.SystemProgramAddr,
		Data: nonceData, RentEpoch: math.MaxUint64,
	}))
	payer, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	payer.Owner = addresses.VoteProgramAddr
	require.NoError(t, slotCtx.SetAccount(txfixture.PayerPubkey(), payer))

	advanceData := make([]byte, 4)
	binary.LittleEndian.PutUint32(advanceData, nonceDiscriminator)
	msg := solana.Message{
		Header: solana.MessageHeader{
			NumRequiredSignatures:       1,
			NumReadonlyUnsignedAccounts: 1,
		},
		AccountKeys:     []solana.PublicKey{txfixture.PayerPubkey(), nonceKey, addresses.SystemProgramAddr},
		RecentBlockhash: durableNonce,
		TransactionConfig: solana.TransactionConfig{}.
			WithComputeUnitLimit(12_345).
			WithLoadedAccountsDataSizeLimit(54_321),
		Instructions: []solana.CompiledInstruction{{
			ProgramIDIndex: 2,
			Accounts:       []uint16{1, 0},
			Data:           advanceData,
		}},
	}
	_, err = msg.SetVersion(solana.MessageVersionV1)
	require.NoError(t, err)
	tx := &solana.Transaction{Message: msg}
	payerPrivateKey := txfixture.PayerPrivateKey()
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key == txfixture.PayerPubkey() {
			return &payerPrivateKey
		}
		return nil
	})
	require.NoError(t, err)

	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:     slotCtx,
		Transaction: tx,
		LeanResult:  true,
	})
	require.NotNil(t, output.ProcessingResult.TransactionError)
	assert.False(t, output.ProcessedAsNoOp)
	assert.Equal(t, TransactionErrorInvalidAccountForFee, output.ProcessingResult.TransactionError.ErrorType)

	var sigverify sync.WaitGroup
	feeInfo, computeUnits, processErr := ProcessTransaction(slotCtx, &sigverify, tx, nil, nil, nil, false)
	sigverify.Wait()
	require.ErrorIs(t, processErr, fees.ErrInvalidAccountForFee)
	assert.Nil(t, feeInfo)
	assert.Zero(t, computeUnits)
}

func TestFeesOnlyLoadedDataSizeUsesFeatureSelectedSemantics(t *testing.T) {
	for _, tc := range []struct {
		name         string
		amendment    bool
		expectedSize uint32
	}{
		{name: "rollback account data before amendment", expectedSize: fees.NonceStateSize},
		{name: "partial SIMD-0186 accumulator after amendment", amendment: true, expectedSize: 272},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slotCtx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			if tc.amendment {
				slotCtx.Features.EnableFeature(features.DefineLtdsFeeOnlySemantics, 0)
			}

			payerKey := txfixture.PayerPubkey()
			nonceKey := solana.PublicKey{0xD5}
			missingProgram := solana.PublicKey{0xD6}
			durableNonce := solana.Hash{0xAA}
			nonceState := sealevel.NonceStateVersions{
				Type: sealevel.NonceVersionCurrent,
				Current: sealevel.NonceData{
					IsInitialized: true,
					Authority:     payerKey,
					DurableNonce:  durableNonce,
					FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
				},
			}
			nonceData, err := nonceState.Marshal()
			require.NoError(t, err)
			require.NoError(t, slotCtx.SetAccount(nonceKey, &accounts.Account{
				Key: nonceKey, Lamports: 10_000_000, Owner: addresses.SystemProgramAddr,
				Data: nonceData, RentEpoch: math.MaxUint64,
			}))
			// A present zero-lamport program account reaches Agave's processable
			// ProgramAccountNotFound load result instead of the unprocessable
			// account-key lookup error.
			require.NoError(t, slotCtx.SetAccount(missingProgram, &accounts.Account{Key: missingProgram}))

			advanceData := make([]byte, 4)
			binary.LittleEndian.PutUint32(advanceData, sealevel.SystemProgramInstrTypeAdvanceNonceAccount)
			tx := &solana.Transaction{
				Signatures: []solana.Signature{{}},
				Message: solana.Message{
					Header: solana.MessageHeader{
						NumRequiredSignatures:       1,
						NumReadonlyUnsignedAccounts: 2,
					},
					AccountKeys:     []solana.PublicKey{payerKey, nonceKey, addresses.SystemProgramAddr, missingProgram},
					RecentBlockhash: durableNonce,
					Instructions: []solana.CompiledInstruction{
						{ProgramIDIndex: 2, Accounts: []uint16{1, 0}, Data: advanceData},
						{ProgramIDIndex: 3},
					},
				},
			}

			output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
				SlotCtx: slotCtx, Transaction: tx, LeanResult: true,
			})
			require.NotNil(t, output.ProcessingResult.TransactionError)
			assert.Equal(t, TransactionErrorProgramAccountNotFound, output.ProcessingResult.TransactionError.ErrorType)
			assert.Equal(t, tc.expectedSize, output.LoadedAccountsDataSize)
		})
	}
}

func TestHandleFailedTxMissingPayerReturnsError(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)
	slotCtx.Accounts = accounts.NewMemAccounts()

	feeInfo, handleErr := handleFailedTx(
		slotCtx,
		tx,
		nil,
		&sealevel.ComputeBudgetLimits{},
		TxErrMaxLoadedAccountsDataSizeExceeded,
		nil,
	)
	assert.Nil(t, feeInfo)
	assert.ErrorIs(t, handleErr, fees.ErrFeePayerNotFound)
}

func TestApplySuccessfulTransactionCommitsLeanResult(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)

	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:     slotCtx,
		Transaction: tx,
		LeanResult:  true,
	})
	require.Nil(t, output.ProcessingResult.TransactionError)
	assert.Nil(t, output.ProcessingResult.ProcessedTransaction)
	require.NotNil(t, output.ExecutionResult)
	assert.Nil(t, output.ExecutionResult.AccountUpdates)
	assert.Nil(t, output.ExecutionResult.ModifiedVoteAccounts)
	assert.Nil(t, output.PreBalances)
	assert.IsType(t, discardLogger{}, output.ExecCtx.Log)
	assert.Nil(t, output.ExecCtx.ModifiedVoteStates)
	assert.ElementsMatch(t,
		[]solana.PublicKey{txfixture.PayerPubkey(), txfixture.DestPubkey()},
		output.ExecutionResult.WritableAccounts,
	)
	assert.Len(t, output.ExecutionResult.WritableAccounts, len(output.ExecutionResult.WritableAccountSet), "lean writable list must be deduplicated")

	destBefore, err := slotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	require.NoError(t, ApplySuccessfulTransaction(slotCtx, output))
	destAfter, err := slotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	assert.Greater(t, destAfter.Lamports, destBefore.Lamports)
}

func TestLeanResultCanCaptureDiagnostics(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)

	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:            slotCtx,
		Transaction:        tx,
		LeanResult:         true,
		CapturePreBalances: true,
		RecordLogs:         true,
	})
	require.Nil(t, output.ProcessingResult.TransactionError)
	require.Len(t, output.PreBalances, len(tx.Message.AccountKeys))
	assert.Equal(t, uint64(10_000_000_000), output.PreBalances[0])
	assert.IsType(t, &sealevel.LogRecorder{}, output.ExecCtx.Log)
	assert.Nil(t, output.ProcessingResult.ProcessedTransaction)
}

func TestProcessTransactionPublicationMetrics(t *testing.T) {
	for _, test := range []struct {
		name      string
		replay    bool
		removeADH bool
	}{
		{name: "replay-rich", replay: true},
		{name: "replay-lean-without-adh", replay: true, removeADH: true},
		{name: "block-production-is-not-recorded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			previousMetrics := metrics.GlobalBlockReplay
			defer func() { metrics.GlobalBlockReplay = previousMetrics }()
			slotCtx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			slotCtx.Replay = test.replay
			if test.removeADH {
				slotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)
			}
			tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
			require.NoError(t, err)
			metrics.GlobalBlockReplay = metrics.BlockReplay{}
			var sigverify sync.WaitGroup
			feeInfo, computeUnits, err := ProcessTransaction(slotCtx, &sigverify, tx, nil, nil, nil, false)
			sigverify.Wait()
			require.NoError(t, err)
			require.NotNil(t, feeInfo)
			assert.NotZero(t, computeUnits)

			got := metrics.GlobalBlockReplay
			if !test.replay {
				assert.Zero(t, got.TxUpdateAccounts.Count)
				assert.Zero(t, got.TxPublishRecordWritableAcct.Count)
				assert.Zero(t, got.TxPublishTouchedAccountState.Count)
				assert.Zero(t, got.TxPublishStakeVoteBookkeeping.Count)
				assert.Zero(t, got.TxPublicationTouchedAccounts)
				assert.Zero(t, got.TxPublicationTouchedAccountBytes)
				return
			}
			assert.Equal(t, uint64(1), got.TxUpdateAccounts.Count)
			if test.removeADH {
				assert.Zero(t, got.TxPublishRecordWritableAcct.Count)
			} else {
				assert.Equal(t, uint64(1), got.TxPublishRecordWritableAcct.Count)
			}
			assert.Equal(t, uint64(1), got.TxPublishTouchedAccountState.Count)
			assert.Equal(t, uint64(1), got.TxPublishStakeVoteBookkeeping.Count)
			assert.Equal(t, uint64(2), got.TxPublicationTouchedAccounts)
			assert.Zero(t, got.TxPublicationTouchedAccountBytes)
			children := got.TxPublishRecordWritableAcct.SumNanoseconds +
				got.TxPublishTouchedAccountState.SumNanoseconds +
				got.TxPublishStakeVoteBookkeeping.SumNanoseconds
			assert.LessOrEqual(t, children, got.TxUpdateAccounts.SumNanoseconds)
		})
	}
}

func TestProcessTransactionFailedPublicationMetrics(t *testing.T) {
	for _, replay := range []bool{true, false} {
		name := "block-production-is-not-recorded"
		if replay {
			name = "replay"
		}
		t.Run(name, func(t *testing.T) {
			previousMetrics := metrics.GlobalBlockReplay
			defer func() { metrics.GlobalBlockReplay = previousMetrics }()
			slotCtx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			slotCtx.Replay = replay

			tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(tx.Message.Instructions[0].Data), 12)
			binary.LittleEndian.PutUint64(tx.Message.Instructions[0].Data[4:], math.MaxUint64)
			payerBefore, err := slotCtx.GetAccount(txfixture.PayerPubkey())
			require.NoError(t, err)
			payerBefore.RentEpoch = 7
			require.NoError(t, slotCtx.SetAccount(txfixture.PayerPubkey(), payerBefore))

			metrics.GlobalBlockReplay = metrics.BlockReplay{}
			var sigverify sync.WaitGroup
			feeInfo, _, processErr := ProcessTransaction(slotCtx, &sigverify, tx, nil, nil, nil, false)
			sigverify.Wait()
			require.Error(t, processErr)
			require.NotNil(t, feeInfo)
			payerAfter, err := slotCtx.GetAccount(txfixture.PayerPubkey())
			require.NoError(t, err)
			assert.Less(t, payerAfter.Lamports, payerBefore.Lamports)
			assert.Equal(t, uint64(7), payerAfter.RentEpoch,
				"ordinary-blockhash failure must preserve the loaded payer rent epoch")

			got := metrics.GlobalBlockReplay
			if !replay {
				assert.Zero(t, got.TxFailedUpdateAccounts.Count)
				assert.Zero(t, got.TxFailedPublicationPreparation.Count)
				assert.Zero(t, got.TxFailedPayerPublication.Count)
				assert.Zero(t, got.TxFailedNoncePublication.Count)
				return
			}
			assert.Equal(t, uint64(1), got.TxFailedUpdateAccounts.Count)
			assert.Equal(t, uint64(1), got.TxFailedPublicationPreparation.Count)
			assert.Equal(t, uint64(1), got.TxFailedPayerPublication.Count)
			assert.Zero(t, got.TxFailedNoncePublication.Count)
			children := got.TxFailedPublicationPreparation.SumNanoseconds +
				got.TxFailedPayerPublication.SumNanoseconds +
				got.TxFailedNoncePublication.SumNanoseconds
			assert.LessOrEqual(t, children, got.TxFailedUpdateAccounts.SumNanoseconds)
			assert.Zero(t, got.TxUpdateAccounts.Count)
		})
	}
}

func TestHandleFailedTxNoncePublicationMetrics(t *testing.T) {
	previousMetrics := metrics.GlobalBlockReplay
	defer func() { metrics.GlobalBlockReplay = previousMetrics }()
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	slotCtx.Replay = true

	previousRecent := sealevel.SysvarCache.RecentBlockHashes.Sysvar
	emptyRecent := sealevel.SysvarRecentBlockhashes{}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &emptyRecent
	defer func() { sealevel.SysvarCache.RecentBlockHashes.Sysvar = previousRecent }()

	authority := txfixture.PayerPubkey()
	nonceKey := solana.PublicKey{0xD5}
	durableNonce := [32]byte{0xAA}
	nonceState := sealevel.NonceStateVersions{
		Type: sealevel.NonceVersionCurrent,
		Current: sealevel.NonceData{
			IsInitialized: true,
			Authority:     authority,
			DurableNonce:  durableNonce,
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
		},
	}
	nonceData, err := nonceState.Marshal()
	require.NoError(t, err)
	require.NoError(t, slotCtx.SetAccount(nonceKey, &accounts.Account{
		Key: nonceKey, Lamports: 1, Owner: addresses.SystemProgramAddr,
		Data: nonceData, RentEpoch: math.MaxUint64,
	}))
	slotCtx.LastBlockhash = [32]byte{0x77}

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)
	payerBefore, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	payerBefore.RentEpoch = 7
	require.NoError(t, slotCtx.SetAccount(txfixture.PayerPubkey(), payerBefore))
	tx.Message.RecentBlockhash = durableNonce
	instructionData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instructionData, sealevel.SystemProgramInstrTypeAdvanceNonceAccount)
	instruction := sealevel.Instruction{
		ProgramId: addresses.SystemProgramAddr,
		Accounts: []sealevel.AccountMeta{
			{Pubkey: nonceKey, IsWritable: true},
			{Pubkey: authority, IsSigner: true},
		},
		Data: instructionData,
	}

	metrics.GlobalBlockReplay = metrics.BlockReplay{}
	feeInfo, handleErr := handleFailedTx(
		slotCtx,
		tx,
		[]sealevel.Instruction{instruction},
		&sealevel.ComputeBudgetLimits{},
		sealevel.InstrErrInvalidArgument,
		nil,
	)
	require.ErrorIs(t, handleErr, sealevel.InstrErrInvalidArgument)
	require.NotNil(t, feeInfo)
	assert.Contains(t, slotCtx.ModifiedAccts, txfixture.PayerPubkey())
	assert.Contains(t, slotCtx.ModifiedAccts, nonceKey)
	payerAfter, err := slotCtx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	assert.Equal(t, uint64(math.MaxUint64), payerAfter.RentEpoch,
		"durable-nonce rollback must retain fee-payer rent normalization")
	got := metrics.GlobalBlockReplay
	assert.Equal(t, uint64(1), got.TxFailedUpdateAccounts.Count)
	assert.Equal(t, uint64(1), got.TxFailedPublicationPreparation.Count)
	assert.Equal(t, uint64(1), got.TxFailedPayerPublication.Count)
	assert.Equal(t, uint64(1), got.TxFailedNoncePublication.Count)
	children := got.TxFailedPublicationPreparation.SumNanoseconds +
		got.TxFailedPayerPublication.SumNanoseconds + got.TxFailedNoncePublication.SumNanoseconds
	assert.LessOrEqual(t, children, got.TxFailedUpdateAccounts.SumNanoseconds)
}

func TestHandleFailedTxSameNonceAndFeePayerRetainsRentNormalization(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()

	previousRecent := sealevel.SysvarCache.RecentBlockHashes.Sysvar
	emptyRecent := sealevel.SysvarRecentBlockhashes{}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &emptyRecent
	defer func() { sealevel.SysvarCache.RecentBlockHashes.Sysvar = previousRecent }()

	payerKey := txfixture.PayerPubkey()
	durableNonce := [32]byte{0xAB}
	nonceState := sealevel.NonceStateVersions{
		Type: sealevel.NonceVersionCurrent,
		Current: sealevel.NonceData{
			IsInitialized: true,
			Authority:     payerKey,
			DurableNonce:  durableNonce,
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
		},
	}
	nonceData, err := nonceState.Marshal()
	require.NoError(t, err)
	payerBefore, err := slotCtx.GetAccount(payerKey)
	require.NoError(t, err)
	payerBefore.Data = nonceData
	payerBefore.RentEpoch = 7
	require.NoError(t, slotCtx.SetAccount(payerKey, payerBefore))
	slotCtx.LastBlockhash = [32]byte{0x77}

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)
	tx.Message.RecentBlockhash = durableNonce
	instructionData := make([]byte, 4)
	binary.LittleEndian.PutUint32(instructionData, sealevel.SystemProgramInstrTypeAdvanceNonceAccount)
	instruction := sealevel.Instruction{
		ProgramId: addresses.SystemProgramAddr,
		Accounts: []sealevel.AccountMeta{
			{Pubkey: payerKey, IsWritable: true, IsSigner: true},
		},
		Data: instructionData,
	}

	feeInfo, handleErr := handleFailedTx(
		slotCtx,
		tx,
		[]sealevel.Instruction{instruction},
		&sealevel.ComputeBudgetLimits{},
		sealevel.InstrErrInvalidArgument,
		nil,
	)
	require.ErrorIs(t, handleErr, sealevel.InstrErrInvalidArgument)
	require.NotNil(t, feeInfo)

	payerAfter, err := slotCtx.GetAccount(payerKey)
	require.NoError(t, err)
	assert.Equal(t, payerBefore.Lamports-feeInfo.TotalFee, payerAfter.Lamports)
	assert.Equal(t, uint64(math.MaxUint64), payerAfter.RentEpoch)
	updatedNonce, err := sealevel.UnmarshalNonceStateVersions(payerAfter.Data)
	require.NoError(t, err)
	assert.NotEqual(t, durableNonce, updatedNonce.State().DurableNonce,
		"failed durable-nonce transaction must still advance the nonce")
}

func BenchmarkLoadAndExecuteTransferResultMode(b *testing.B) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(b, err)

	for _, bench := range []struct {
		name        string
		lean        bool
		preBalances bool
	}{
		{name: "rich"},
		{name: "lean", lean: true},
		{name: "lean-with-pre-balances", lean: true, preBalances: true},
	} {
		b.Run(bench.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
					SlotCtx:            slotCtx,
					Transaction:        tx,
					LeanResult:         bench.lean,
					CapturePreBalances: bench.preBalances,
				})
				if output.ProcessingResult.TransactionError != nil {
					b.Fatal(output.ProcessingResult.TransactionError)
				}
			}
		})
	}
}

func BenchmarkApplySuccessfulTransactionPublicationMetrics(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		b.Run(name, func(b *testing.B) {
			previousMetrics := metrics.GlobalBlockReplay
			defer func() { metrics.GlobalBlockReplay = previousMetrics }()
			slotCtx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			slotCtx.Replay = enabled

			tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
			require.NoError(b, err)
			output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
				SlotCtx: slotCtx, Transaction: tx,
			})
			require.Nil(b, output.ProcessingResult.TransactionError)
			require.NotNil(b, output.ExecCtx)
			require.NotNil(b, output.ExecutionResult)

			metrics.GlobalBlockReplay = metrics.BlockReplay{}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := applySuccessfulTransactionState(slotCtx, output.ExecCtx, output.ExecutionResult); err != nil {
						panic(err)
					}
				}
			})
			b.StopTimer()
			b.ReportMetric(2, "touched/op")
		})
	}
}

func TestApplySuccessfulTransactionRejectsFailedOutput(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()

	output := sanitizeFailureOutput()
	err := ApplySuccessfulTransaction(slotCtx, output)
	require.Error(t, err)
}

func signedV1Transfer(t *testing.T, config solana.TransactionConfig) *solana.Transaction {
	t.Helper()
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(7))
	require.NoError(t, err)
	_, err = tx.Message.SetVersion(solana.MessageVersionV1)
	require.NoError(t, err)
	tx.Message.TransactionConfig = config
	payerPrivateKey := txfixture.PayerPrivateKey()
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key == txfixture.PayerPubkey() {
			return &payerPrivateKey
		}
		return nil
	})
	require.NoError(t, err)
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	tx, err = solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	return tx
}

func newCommitTestSlotCtx() (*sealevel.SlotCtx, func()) {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)

	mem := accounts.NewMemAccounts()
	systemAcct := &accounts.Account{
		Key:        addresses.SystemProgramAddr,
		Lamports:   1,
		Owner:      addresses.NativeLoaderAddr,
		Executable: true,
		RentEpoch:  math.MaxUint64,
	}
	_ = mem.SetAccountWithoutLock(addresses.SystemProgramAddr, systemAcct)
	_ = mem.SetAccountWithoutLock(txfixture.PayerPubkey(), &accounts.Account{
		Key: txfixture.PayerPubkey(), Lamports: 10_000_000_000, Owner: addresses.SystemProgramAddr, RentEpoch: math.MaxUint64,
	})
	_ = mem.SetAccountWithoutLock(txfixture.DestPubkey(), &accounts.Account{
		Key: txfixture.DestPubkey(), Lamports: 10_000_000, Owner: addresses.SystemProgramAddr, RentEpoch: math.MaxUint64,
	})

	prevRBH := sealevel.SysvarCache.RecentBlockHashes.Sysvar
	rbh := sealevel.SysvarRecentBlockhashes{{Blockhash: txfixture.TestBlockhash(), FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000}}}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &rbh

	prevRent := sealevel.SysvarCache.Rent.Sysvar
	rent := sealevel.NewDefaultRentSysvar()
	sealevel.SysvarCache.Rent.Sysvar = &rent

	slotCtx := &sealevel.SlotCtx{
		Slot:            42,
		Features:        feats,
		Accounts:        mem,
		FeeRateGovernor: &sealevel.FeeRateGovernor{PrevLamportsPerSignature: 5000},
		LastBlockhash:   txfixture.TestBlockhash(),
		AcctMapsMu:      &sync.Mutex{},
		ModifiedAccts:   make(map[solana.PublicKey]bool),
		WritableAccts:   make(map[solana.PublicKey]bool),
	}
	return slotCtx, func() {
		sealevel.SysvarCache.RecentBlockHashes.Sysvar = prevRBH
		sealevel.SysvarCache.Rent.Sysvar = prevRent
	}
}
