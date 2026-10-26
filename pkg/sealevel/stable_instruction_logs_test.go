package sealevel

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func stableLogTestContext() (*ExecutionCtx, *LogRecorder, []InstructionAccount, []byte) {
	txAccounts := NewTransactionAccounts([]accounts.Account{
		{Key: addresses.SystemProgramAddr, Owner: addresses.NativeLoaderAddr, Lamports: 1, Executable: true},
		{Key: solana.PublicKey{31}, Owner: addresses.SystemProgramAddr, Lamports: 100},
		{Key: solana.PublicKey{32}, Owner: addresses.SystemProgramAddr, Lamports: 100},
	})
	tx := NewTransactionCtx(*txAccounts, 5, 64)
	logger := new(LogRecorder)
	ctx := &ExecutionCtx{TransactionContext: tx, ComputeMeter: cu.NewComputeMeterDefault(), Log: logger, SkipTimingMetrics: true}
	instructionAccounts := []InstructionAccount{
		{IndexInTransaction: 1, IndexInCaller: 1, IndexInCallee: 0, IsSigner: true, IsWritable: true},
		{IndexInTransaction: 2, IndexInCaller: 2, IndexInCallee: 1, IsWritable: true},
	}
	data := make([]byte, 12)
	binary.LittleEndian.PutUint32(data, SystemProgramInstrTypeTransfer)
	binary.LittleEndian.PutUint64(data[4:], 1)
	return ctx, logger, instructionAccounts, data
}

func TestStableInstructionLogsSuccessFailureAndNesting(t *testing.T) {
	program := solana.PublicKey(addresses.SystemProgramAddr).String()
	ctx, log, instructionAccounts, data := stableLogTestContext()
	require.NoError(t, ctx.ProcessInstruction(data, instructionAccounts, []uint64{0}))
	require.Equal(t, []string{"Program " + program + " invoke [1]", "Program " + program + " success"}, log.Logs)
	ctx, log, instructionAccounts, _ = stableLogTestContext()
	require.ErrorIs(t, ctx.ProcessInstruction([]byte{255}, instructionAccounts, []uint64{0}), InstrErrInvalidInstructionData)
	require.Equal(t, []string{"Program " + program + " invoke [1]", "Program " + program + " failed: invalid instruction data"}, log.Logs)
	ctx, log, instructionAccounts, data = stableLogTestContext()
	parent, err := ctx.TransactionContext.NextInstructionCtx()
	require.NoError(t, err)
	parentAccounts := append(append([]InstructionAccount(nil), instructionAccounts...), InstructionAccount{IndexInTransaction: 0, IndexInCaller: 0, IndexInCallee: 2})
	parent.Configure([]uint64{0}, parentAccounts, nil)
	require.NoError(t, ctx.Push())
	require.NoError(t, ctx.NativeInvoke(Instruction{ProgramId: addresses.SystemProgramAddr, Data: data, Accounts: []AccountMeta{
		{Pubkey: solana.PublicKey{31}, IsSigner: true, IsWritable: true},
		{Pubkey: solana.PublicKey{32}, IsWritable: true},
	}}, nil))
	require.Equal(t, []string{"Program " + program + " invoke [2]", "Program " + program + " success"}, log.Logs)
	require.Equal(t, uint64(1), ctx.StackHeight())
}

func TestStableInstructionLogsExcludeUnresolvedPushAndPrecompile(t *testing.T) {
	ctx, log, instructionAccounts, data := stableLogTestContext()
	ctx.TransactionContext.Accounts.Accounts[0].Owner = solana.PublicKey{99}
	require.ErrorIs(t, ctx.ProcessInstruction(data, instructionAccounts, []uint64{0}), InstrErrUnsupportedProgramId)
	require.Empty(t, log.Logs)
	ctx, log, instructionAccounts, data = stableLogTestContext()
	parent, err := ctx.TransactionContext.NextInstructionCtx()
	require.NoError(t, err)
	parent.Configure([]uint64{0}, instructionAccounts, nil)
	require.NoError(t, ctx.Push())
	ctx.TransactionContext.InstructionTraceCapacity = 1
	require.ErrorIs(t, ctx.ProcessInstruction(data, instructionAccounts, []uint64{0}), InstrErrCallDepth)
	require.Empty(t, log.Logs)
	for _, precompile := range []solana.PublicKey{addresses.Ed25519PrecompileAddr, addresses.Secp256kPrecompileAddr, addresses.Secp256r1PrecompileAddr} {
		ctx, log, _, _ = stableLogTestContext()
		ctx.TransactionContext.Accounts.Accounts[0].Key = precompile
		ctx.TransactionContext.AccountKeys[0] = precompile
		_ = ctx.ProcessInstruction([]byte{0, 0}, nil, []uint64{0})
		require.Empty(t, log.Logs, "precompile dispatch has no stable invocation logs")
	}
}

type rejectingStableLogger struct{}

func (rejectingStableLogger) Log(string) { panic("disabled stable log was emitted") }

func TestStableInstructionLogsDisabledAvoidsFormatting(t *testing.T) {
	ctx, _, instructionAccounts, data := stableLogTestContext()
	ctx.SkipStableLogs = true
	run := func() {
		ctx.TransactionContext.InstructionTrace = ctx.TransactionContext.InstructionTrace[:1]
		ctx.TransactionContext.InstructionStack = ctx.TransactionContext.InstructionStack[:0]
		ctx.TransactionContext.Accounts.Accounts[1].Lamports = 100
		ctx.TransactionContext.Accounts.Accounts[2].Lamports = 100
		ctx.ComputeMeter = cu.NewComputeMeterDefault()
		if err := ctx.ProcessInstruction(data, instructionAccounts, []uint64{0}); err != nil {
			panic(err)
		}
	}
	ctx.Log = nil
	baseline := testing.AllocsPerRun(100, run)
	ctx.Log = rejectingStableLogger{}
	disabled := testing.AllocsPerRun(100, run)
	require.Equal(t, baseline, disabled, "discard policy adds no formatting allocations")
	require.LessOrEqual(t, disabled, float64(6), "Go 1.27 native execution baseline excludes stable-message formatting")
	t.Logf("disabled native instruction allocations: %g", disabled)
}

func TestStableInstructionFailureFormatting(t *testing.T) {
	for _, check := range []struct {
		err  error
		want string
	}{
		{InstrErrInvalidInstructionData, "invalid instruction data"},
		{fmt.Errorf("wrapped: %w", InstrErrInsufficientFunds), "insufficient funds for instruction"},
		{InstrErrCustomCode{Code: 42}, "custom program error: 0x2a"},
		{fmt.Errorf("wrapped: %w", InstrErrCustomCode{Code: 0}), "custom program error: 0x0"},
		{SystemProgErrResultWithNegativeLamports, "custom program error: 0x1"},
		{fmt.Errorf("wrapped: %w", SystemProgErrResultWithNegativeLamports), "custom program error: 0x1"},
	} {
		require.Equal(t, check.want, instructionErrorLogMessage(check.err))
	}
	for _, instructionError := range instructionErrTargets {
		if instructionError == InstrErrCustom {
			continue
		}
		code, ok := solanaErrCode(instructionError)
		require.True(t, ok)
		require.NotEmpty(t, instructionErrorLogMessages[code])
		require.Equal(t, instructionErrorLogMessages[code], instructionErrorLogMessage(instructionError))
	}
}
