package replay

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func transactionMetadataLogFixture(t *testing.T, failed, removeADH bool) (*sealevel.SlotCtx, *solana.Transaction) {
	t.Helper()
	ctx, cpiTx, _ := transactionMetadataCPIFixture(t, removeADH)
	program := cpiTx.Message.AccountKeys[cpiTx.Message.Instructions[0].ProgramIDIndex]
	account, err := ctx.GetAccount(program)
	require.NoError(t, err)
	account.Data = fixtures.Load(t, "sealevel", "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr.so")
	require.NoError(t, ctx.SetAccount(program, account))
	instructions := []solana.Instruction{
		system.NewTransferInstruction(8, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build(),
		solana.NewInstruction(program, solana.AccountMetaSlice{solana.Meta(txfixture.PayerPubkey()).SIGNER()}, []byte("metadata log")),
	}
	if failed {
		instructions = append(instructions, solana.NewInstruction(solana.SystemProgramID, nil, []byte{255}))
	}
	return ctx, signLifecycleInstructions(t, instructions...)
}

func TestTransactionMetadataRealLogsPreserveExecution(t *testing.T) {
	for _, tc := range []struct {
		name   string
		failed bool
	}{
		{name: "success"},
		{name: "rollback", failed: true},
	} {
		for _, removeADH := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/remove-ADH=%t", tc.name, removeADH), func(t *testing.T) {
				var baseline []*accounts.Account
				var modified, writable map[solana.PublicKey]bool
				var baselineFee, baselineUnits uint64
				for _, enabled := range []bool{false, true} {
					ctx, tx := transactionMetadataLogFixture(t, tc.failed, removeADH)
					var capture *transactionMetadataCapture
					if enabled {
						capture = &transactionMetadataCapture{}
					}
					fee, units, err := processTransaction(ctx, &sync.WaitGroup{}, tx, nil, nil, nil, false, capture)
					if tc.failed {
						require.ErrorIs(t, err, sealevel.InstrErrInvalidInstructionData)
					} else {
						require.NoError(t, err, "compute units consumed: %d", units)
					}
					require.NotNil(t, fee)
					require.Equal(t, uint64(5000), fee.TotalFee)
					require.NotZero(t, units)
					state := metadataFailureState(t, ctx.Accounts, tx.Message.AccountKeys)
					destination, err := ctx.GetAccount(txfixture.DestPubkey())
					require.NoError(t, err)
					wantDestination := uint64(10_000_008)
					if tc.failed {
						wantDestination = 10_000_000
					}
					require.Equal(t, wantDestination, destination.Lamports)
					if !enabled {
						baseline, baselineFee, baselineUnits = state, fee.TotalFee, units
						modified, writable = maps.Clone(ctx.ModifiedAccts), maps.Clone(ctx.WritableAccts)
						continue
					}
					require.Equal(t, baseline, state)
					require.Equal(t, baselineFee, fee.TotalFee)
					require.Equal(t, baselineUnits, units)
					require.Equal(t, modified, ctx.ModifiedAccts)
					require.Equal(t, writable, ctx.WritableAccts)
					require.NoError(t, capture.Err)
					require.NotNil(t, capture.Record)
					require.Equal(t, units, capture.Record.ComputeUnitsConsumed)
					program := tx.Message.AccountKeys[tx.Message.Instructions[1].ProgramIDIndex].String()
					logs := []string{
						"Program " + solana.SystemProgramID.String() + " invoke [1]",
						"Program " + solana.SystemProgramID.String() + " success",
						"Program " + program + " invoke [1]",
						"Program log: Signed by " + txfixture.PayerPubkey().String(),
						`Program log: Memo (len 12): "metadata log"`,
						"Program " + program + " success",
					}
					if tc.failed {
						logs = append(logs, "Program "+solana.SystemProgramID.String()+" invoke [1]", "Program "+solana.SystemProgramID.String()+" failed: invalid instruction data")
					}
					require.Equal(t, logs, capture.Record.LogMessages)
					if tc.failed {
						require.JSONEq(t, `{"InstructionError":[2,"InvalidInstructionData"]}`, string(capture.Record.Error))
					} else {
						require.JSONEq(t, "null", string(capture.Record.Error))
					}
				}
			})
		}
	}
}

func TestTransactionMetadataLogSliceOwnership(t *testing.T) {
	ctx, tx := transactionMetadataLogFixture(t, false, true)
	metadata, err := prepareTransactionMetadata(ctx, tx, nil)
	require.NoError(t, err)
	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx: ctx, Transaction: tx, LeanResult: true, RecordLogs: true, boundedMetadata: true,
	})
	require.Nil(t, output.ProcessingResult.TransactionError)
	require.NotNil(t, output.ExecCtx)
	recorder, ok := output.ExecCtx.Log.(*sealevel.LogRecorder)
	require.True(t, ok)
	require.NotEmpty(t, recorder.Logs)
	require.NoError(t, ApplySuccessfulTransaction(ctx, output))
	require.NoError(t, metadata.finish(ctx, tx, output, output.FeeInfo.TotalFee, nil))
	expected := slices.Clone(recorder.Logs)
	require.Equal(t, expected, metadata.LogMessages)
	recorder.Logs[0] = "changed recorder entry"
	recorder.Log("later recorder entry")
	require.Equal(t, expected, metadata.LogMessages)
	metadata.LogMessages[0] = "changed receipt entry"
	require.Equal(t, "changed recorder entry", recorder.Logs[0])
}

func TestTransactionMetadataNoOpLogsAreRecordedEmpty(t *testing.T) {
	ctx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	ctx.Features.EnableFeature(features.EnableTxV1, 0)
	ctx.Features.EnableFeature(features.RelaxFeePayerConstraint, 0)
	tx := signedV1Transfer(t, solana.TransactionConfig{}.WithComputeUnitLimit(12_345).WithLoadedAccountsDataSizeLimit(54_321))
	payer, err := ctx.GetAccount(txfixture.PayerPubkey())
	require.NoError(t, err)
	payer.Owner = addresses.VoteProgramAddr
	require.NoError(t, ctx.SetAccount(txfixture.PayerPubkey(), payer))
	var capture transactionMetadataCapture
	fee, _, err := processTransaction(ctx, &sync.WaitGroup{}, tx, nil, nil, nil, false, &capture)
	require.Error(t, err)
	require.NotNil(t, fee)
	require.NoError(t, capture.Err)
	require.NotNil(t, capture.Record)
	require.NotNil(t, capture.Record.LogMessages)
	require.Empty(t, capture.Record.LogMessages)
	require.Zero(t, capture.Record.Fee)
	require.Zero(t, capture.Record.ComputeUnitsConsumed)
	require.Equal(t, capture.Record.PreBalances, capture.Record.PostBalances)
}

func TestTransactionMetadataBoundedLogSyscall(t *testing.T) {
	// Exercise the real syscall and VM context without claiming that a program
	// generated these calls. The Memo tests above cover actual bytecode execution.
	for _, bounded := range []bool{false, true} {
		recorder := &sealevel.LogRecorder{}
		if bounded {
			recorder = sealevel.NewBoundedLogRecorder()
		}
		ctx := &sealevel.ExecutionCtx{Log: recorder, ComputeMeter: cu.NewComputeMeterDefault()}
		vm := sbpf.NewInterpreter(&sbpf.Program{}, &sbpf.VMOpts{Context: ctx, ComputeMeter: &ctx.ComputeMeter})
		t.Cleanup(vm.Finish)
		for range 200 {
			_, err := sealevel.SyscallLog64Impl(vm, ^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0))
			require.NoError(t, err)
		}
		require.Equal(t, uint64(200*cu.CULog64Units), ctx.ComputeMeter.Used())
		if !bounded {
			require.Len(t, recorder.Logs, 200)
			continue
		}
		var bytes, markers int
		for _, message := range recorder.Logs {
			if message == "Log truncated" {
				markers++
			} else {
				bytes += len(message)
			}
		}
		require.Equal(t, 1, markers)
		require.Less(t, bytes, 10_000)
		require.LessOrEqual(t, len(recorder.Logs), 10_001)
	}
}

func TestTransactionMetadataNativeTransferHasNoSyscallLogs(t *testing.T) {
	ctx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)
	var capture transactionMetadataCapture
	fee, _, err := processTransaction(ctx, &sync.WaitGroup{}, tx, nil, nil, nil, false, &capture)
	require.NoError(t, err)
	require.NotNil(t, fee)
	require.NoError(t, capture.Err)
	require.NotNil(t, capture.Record)
	require.Equal(t, []string{
		"Program " + solana.SystemProgramID.String() + " invoke [1]",
		"Program " + solana.SystemProgramID.String() + " success",
	}, capture.Record.LogMessages)
}
