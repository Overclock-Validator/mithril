package sealevel

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/stretchr/testify/require"
)

func TestBoundedInnerInstructionsEntryLimit(t *testing.T) {
	ctx := &ExecutionCtx{TransactionContext: &TransactionCtx{InstructionStack: []uint64{0}}}
	ctx.EnableBoundedInnerInstructions()
	for range maxRecordedInnerInstructions {
		ctx.recordInnerInstruction(nil, nil, 0)
	}
	require.Len(t, ctx.InnerInstrs, maxRecordedInnerInstructions)
	require.NoError(t, ctx.InnerInstructionsCaptureError)
	ctx.recordInnerInstruction(nil, nil, 0)
	require.ErrorIs(t, ctx.InnerInstructionsCaptureError, errInnerInstructionCaptureLimit)
	require.Nil(t, ctx.InnerInstrs)
	ctx.recordInnerInstruction([]byte{1}, nil, 0)
	require.Nil(t, ctx.InnerInstrs, "overflow remains unavailable after later small input")
}

func TestBoundedInnerInstructionsPayloadLimit(t *testing.T) {
	data := make([]byte, MaxCpiInstructionDataLen)
	instructionAccts := make([]InstructionAccount, MaxCpiInstructionAccounts)
	ctx := &ExecutionCtx{TransactionContext: &TransactionCtx{InstructionStack: []uint64{0}}}
	ctx.EnableBoundedInnerInstructions()
	for range maxRecordedInnerInstructions {
		ctx.recordInnerInstruction(data, instructionAccts, 0)
	}
	require.NoError(t, ctx.InnerInstructionsCaptureError)
	require.Len(t, ctx.InnerInstrs, maxRecordedInnerInstructions)
	require.Equal(t, maxRecordedInnerInstructionBytes, ctx.innerInstructionBytes)

	for _, oversized := range []struct {
		name     string
		data     []byte
		accounts []InstructionAccount
	}{
		{name: "data", data: make([]byte, MaxCpiInstructionDataLen+1)},
		{name: "accounts", accounts: make([]InstructionAccount, MaxCpiInstructionAccounts+1)},
	} {
		t.Run(oversized.name, func(t *testing.T) {
			ctx := &ExecutionCtx{TransactionContext: &TransactionCtx{InstructionStack: []uint64{0}}}
			ctx.EnableBoundedInnerInstructions()
			ctx.recordInnerInstruction([]byte{1}, nil, 0)
			ctx.recordInnerInstruction(oversized.data, oversized.accounts, 0)
			require.ErrorIs(t, ctx.InnerInstructionsCaptureError, errInnerInstructionCaptureLimit)
			require.Nil(t, ctx.InnerInstrs, "one oversized entry invalidates the entire capture")
			ctx.recordInnerInstruction(nil, nil, 0)
			require.Nil(t, ctx.InnerInstrs, "later small input cannot revive capture")
			var rejected bool
			allocations := testing.AllocsPerRun(20, func() {
				ctx := ExecutionCtx{}
				ctx.EnableBoundedInnerInstructions()
				ctx.recordInnerInstruction(oversized.data, oversized.accounts, 0)
				rejected = ctx.InnerInstructionsCaptureError == errInnerInstructionCaptureLimit && ctx.InnerInstrs == nil
			})
			require.True(t, rejected)
			require.Zero(t, allocations, "reject before allocating copied data or accounts")
		})
	}
}

func TestInnerInstructionsDefaultAndOwnership(t *testing.T) {
	ctx := &ExecutionCtx{RecordInnerInstructions: true, TransactionContext: &TransactionCtx{InstructionStack: []uint64{0}}}
	ctx.recordInnerInstruction(make([]byte, maxRecordedInnerInstructionBytes+1), make([]InstructionAccount, MaxCpiInstructionAccounts+1), 0)
	for range maxRecordedInnerInstructions {
		ctx.recordInnerInstruction(nil, nil, 0)
	}
	require.Len(t, ctx.InnerInstrs, maxRecordedInnerInstructions+1)
	require.NoError(t, ctx.InnerInstructionsCaptureError)

	ctx = &ExecutionCtx{TransactionContext: &TransactionCtx{InstructionStack: []uint64{0}}}
	ctx.EnableBoundedInnerInstructions()
	ctx.SetCurrentTopLevelInstr(3)
	data := []byte{1, 2}
	instructionAccts := []InstructionAccount{{IndexInTransaction: 5}, {IndexInTransaction: 7}}
	ctx.recordInnerInstruction(data, instructionAccts, 9)
	data[0], instructionAccts[0].IndexInTransaction = 99, 99
	require.Equal(t, []RecordedInnerInstr{{TopLevelIdx: 3, StackHeight: 2, ProgramIdIndex: 9, Accounts: []uint8{5, 7}, Data: []byte{1, 2}}}, ctx.InnerInstrs)
}

func TestBoundedInnerInstructionsFailedPushDoesNotChangeExecution(t *testing.T) {
	var baseline TransactionAccounts
	for _, bounded := range []bool{false, true} {
		txAccounts := NewTransactionAccounts([]accounts.Account{{Key: addresses.SystemProgramAddr, Lamports: 1}})
		txCtx := NewTransactionCtx(*txAccounts, 5, 1)
		parent, err := txCtx.NextInstructionCtx()
		require.NoError(t, err)
		parent.Configure([]uint64{0}, nil, nil)
		require.NoError(t, txCtx.Push())
		ctx := &ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), RecordInnerInstructions: true, SkipTimingMetrics: true}
		if bounded {
			ctx.EnableBoundedInnerInstructions()
		}
		for range maxRecordedInnerInstructions + 2 {
			err := ctx.ProcessInstruction([]byte{1}, nil, []uint64{0})
			require.ErrorIs(t, err, InstrErrCallDepth)
			require.Equal(t, uint64(1), txCtx.InstructionTraceLength())
			require.Equal(t, uint64(1), ctx.StackHeight())
			require.Zero(t, ctx.ComputeMeter.Used())
			if bounded {
				require.ErrorIs(t, ctx.InnerInstructionsCaptureError, InstrErrCallDepth)
				require.Nil(t, ctx.InnerInstrs, "even the first failed push makes capture unavailable")
			}
		}
		if bounded {
			require.ErrorIs(t, ctx.InnerInstructionsCaptureError, InstrErrCallDepth)
			require.Nil(t, ctx.InnerInstrs)
			require.Equal(t, baseline, txCtx.Accounts)
			ctx.recordInnerInstruction(nil, nil, 0)
			require.Nil(t, ctx.InnerInstrs, "later recording cannot revive a failed capture")
		} else {
			require.Len(t, ctx.InnerInstrs, maxRecordedInnerInstructions+2)
			require.NoError(t, ctx.InnerInstructionsCaptureError)
			baseline = txCtx.Accounts
		}
	}
}

func TestBoundedInnerInstructionsExecutionFailureRemainsRecorded(t *testing.T) {
	var baseline TransactionAccounts
	for _, bounded := range []bool{false, true} {
		txAccounts := NewTransactionAccounts([]accounts.Account{{
			Key: addresses.SystemProgramAddr, Owner: addresses.NativeLoaderAddr, Lamports: 1, Executable: true,
		}})
		txCtx := NewTransactionCtx(*txAccounts, 5, 64)
		parent, err := txCtx.NextInstructionCtx()
		require.NoError(t, err)
		parent.Configure([]uint64{0}, nil, nil)
		require.NoError(t, txCtx.Push())
		ctx := &ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), RecordInnerInstructions: true, SkipTimingMetrics: true}
		if bounded {
			ctx.EnableBoundedInnerInstructions()
		}
		err = ctx.ProcessInstruction([]byte{255}, nil, []uint64{0})
		require.ErrorIs(t, err, InstrErrInvalidInstructionData)
		require.Equal(t, uint64(2), txCtx.InstructionTraceLength())
		require.Equal(t, uint64(1), ctx.StackHeight())
		require.Equal(t, uint64(cu.CUSystemProgramDefaultComputeUnits), ctx.ComputeMeter.Used())
		require.NoError(t, ctx.InnerInstructionsCaptureError)
		require.Equal(t, []RecordedInnerInstr{{StackHeight: 2, ProgramIdIndex: 0, Accounts: []uint8{}, Data: []byte{255}}}, ctx.InnerInstrs)
		if bounded {
			require.Equal(t, baseline, txCtx.Accounts)
			// A later rejected push invalidates the earlier, otherwise valid CPI.
			txCtx.InstructionTraceCapacity = txCtx.InstructionTraceLength()
			require.ErrorIs(t, ctx.ProcessInstruction([]byte{255}, nil, []uint64{0}), InstrErrCallDepth)
			require.ErrorIs(t, ctx.InnerInstructionsCaptureError, InstrErrCallDepth)
			require.Nil(t, ctx.InnerInstrs)
		} else {
			baseline = txCtx.Accounts
		}
	}
}

func TestBoundedInnerInstructionsOversizeDoesNotChangeExecution(t *testing.T) {
	// Direct invocation deliberately bypasses the syscall's size checks. The
	// optional recorder must not change the native instruction's error or CU.
	data := make([]byte, MaxCpiInstructionDataLen+1)
	data[0] = 255
	var baseline TransactionAccounts
	for _, bounded := range []bool{false, true} {
		txAccounts := NewTransactionAccounts([]accounts.Account{{
			Key: addresses.SystemProgramAddr, Owner: addresses.NativeLoaderAddr, Lamports: 1, Executable: true,
		}})
		txCtx := NewTransactionCtx(*txAccounts, 5, 64)
		parent, err := txCtx.NextInstructionCtx()
		require.NoError(t, err)
		parent.Configure([]uint64{0}, nil, nil)
		require.NoError(t, txCtx.Push())
		ctx := &ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), RecordInnerInstructions: true, SkipTimingMetrics: true}
		if bounded {
			ctx.EnableBoundedInnerInstructions()
		}
		require.ErrorIs(t, ctx.ProcessInstruction(data, nil, []uint64{0}), InstrErrInvalidInstructionData)
		require.Equal(t, uint64(2), txCtx.InstructionTraceLength())
		require.Equal(t, uint64(1), ctx.StackHeight())
		require.Equal(t, uint64(cu.CUSystemProgramDefaultComputeUnits), ctx.ComputeMeter.Used())
		if bounded {
			require.ErrorIs(t, ctx.InnerInstructionsCaptureError, errInnerInstructionCaptureLimit)
			require.Nil(t, ctx.InnerInstrs)
			require.Equal(t, baseline, txCtx.Accounts)
		} else {
			require.NoError(t, ctx.InnerInstructionsCaptureError)
			require.Len(t, ctx.InnerInstrs, 1)
			require.Equal(t, data, ctx.InnerInstrs[0].Data)
			baseline = txCtx.Accounts
		}
	}
}

func TestBoundedInnerInstructionsKeepsFirstFailure(t *testing.T) {
	ctx := &ExecutionCtx{}
	ctx.EnableBoundedInnerInstructions()
	ctx.recordInnerInstruction(make([]byte, maxRecordedInnerInstructionBytes+1), nil, 0)
	ctx.invalidateInnerInstructionCapture(InstrErrReentrancyNotAllowed)
	require.ErrorIs(t, ctx.InnerInstructionsCaptureError, errInnerInstructionCaptureLimit)
	require.Nil(t, ctx.InnerInstrs)
}
