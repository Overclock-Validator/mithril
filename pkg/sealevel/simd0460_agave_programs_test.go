package sealevel

import (
	"bytes"
	"encoding/binary"
	"testing"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// These small SBF programs exercise the account ABI through the same loader
// path as Agave's programs/sbf/tests/programs.rs integration tests. The three
// configurations distinguish the legacy flat input from SIMD-0460's copied
// and directly mapped account regions.
type agaveSBFMapping struct {
	name   string
	vasa   bool
	direct bool
}

var agaveSBFMappings = []agaveSBFMapping{
	{name: "legacy_flat"},
	{name: "vasa_copied", vasa: true},
	{name: "vasa_direct", vasa: true, direct: true},
}

func agaveSBFContext(t *testing.T, mapping agaveSBFMapping, data []byte) *ExecutionCtx {
	t.Helper()
	ctx := serializationTestContext(t, 1)
	if mapping.vasa {
		ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	}
	if mapping.direct {
		ctx.Features.EnableFeature(features.AccountDataDirectMapping, 0)
	}
	ctx.TransactionContext.Accounts.Accounts[1].Data = bytes.Clone(data)
	ctx.TransactionContext.ComputeBudgetLimits = &ComputeBudgetLimits{UpdatedHeapBytes: 32 * 1024}
	ctx.ComputeMeter = cu.NewComputeMeter(100_000)
	return ctx
}

func agaveSBFSlot(op, dst, src uint8, off int16, imm uint32) sbpf.Slot {
	return sbpf.Slot(op) | sbpf.Slot(dst)<<8 | sbpf.Slot(src)<<12 | sbpf.Slot(uint16(off))<<16 | sbpf.Slot(imm)<<32
}

func agaveSBFProgram(t *testing.T, slots ...sbpf.Slot) *sbpf.Program {
	t.Helper()
	slots = append(slots, agaveSBFSlot(sbpf.OpMov64Imm, 0, 0, 0, 0), agaveSBFSlot(sbpf.OpExit, 0, 0, 0, 0))
	program := &sbpf.Program{Text: slots, TextVA: sbpf.VaddrProgram, TextBytes: make([]byte, 8*len(slots))}
	for i, slot := range slots {
		binary.LittleEndian.PutUint64(program.TextBytes[i*8:], uint64(slot))
	}
	require.NoError(t, program.Verify())
	return program
}

func agaveSBFSetDataLengthProgram(t *testing.T, length uint64, more ...sbpf.Slot) *sbpf.Program {
	t.Helper()
	var slots []sbpf.Slot
	if length <= 0x7fff_ffff {
		slots = append(slots, agaveSBFSlot(sbpf.OpStdw, 1, 0, 88, uint32(length)))
	} else {
		slots = append(slots,
			agaveSBFSlot(sbpf.OpLddw, 2, 0, 0, uint32(length)),
			agaveSBFSlot(0, 0, 0, 0, uint32(length>>32)),
			agaveSBFSlot(sbpf.OpStxdw, 1, 2, 88, 0),
		)
	}
	return agaveSBFProgram(t, append(slots, more...)...)
}

// Agave's test_deny_access_beyond_current_length reads three bytes into a
// zero-length readonly or writable account. Without VASA the read reaches the
// serialized realloc padding; with VASA it must fail with a specific error.
func TestAgaveSBFReadBeyondCurrentLength(t *testing.T) {
	program := agaveSBFProgram(t, agaveSBFSlot(sbpf.OpLdxb, 2, 1, 99, 0))
	for _, mapping := range agaveSBFMappings {
		for _, writable := range []bool{false, true} {
			name := mapping.name + "/readonly"
			if writable {
				name = mapping.name + "/writable"
			}
			t.Run(name, func(t *testing.T) {
				ctx := agaveSBFContext(t, mapping, nil)
				instr, err := ctx.TransactionContext.CurrentInstructionCtx()
				require.NoError(t, err)
				instr.InstructionAccounts[0].IsWritable = writable
				err = executeLoadedProgram(ctx, program, executionSyscalls(ctx))
				if !mapping.vasa {
					require.NoError(t, err)
				} else if writable {
					require.ErrorIs(t, err, InstrErrInvalidRealloc)
				} else {
					require.ErrorIs(t, err, InstrErrAccountDataTooSmall)
				}
				require.Empty(t, ctx.TransactionContext.Accounts.Accounts[1].Data)
			})
		}
	}
}

// Agave's readonly, executable, and ownership cases all reach account data
// through a real SBF store. The expected error is determined by the current
// account permissions, including for a store into reserved growth space.
func TestAgaveSBFAccountDataWritePermissions(t *testing.T) {
	for _, mapping := range agaveSBFMappings[1:] {
		for _, tc := range []struct {
			name       string
			offset     int16
			readonly   bool
			executable bool
			external   bool
			want       error
		}{
			{name: "readonly_existing", offset: 96, readonly: true, want: InstrErrReadonlyDataModified},
			{name: "readonly_growth", offset: 100, readonly: true, want: InstrErrReadonlyDataModified},
			{name: "executable_readonly", offset: 96, readonly: true, executable: true, want: InstrErrReadonlyDataModified},
			{name: "external_owner_existing", offset: 96, external: true, want: InstrErrExternalAccountDataModified},
			{name: "external_owner_growth", offset: 100, external: true, want: InstrErrExternalAccountDataModified},
		} {
			t.Run(mapping.name+"/"+tc.name, func(t *testing.T) {
				ctx := agaveSBFContext(t, mapping, []byte{0, 0, 0, 0})
				ctx.Features.EnableFeature(features.RemoveAccountsExecutableFlagChecks, 0)
				account := ctx.TransactionContext.Accounts.Accounts[1]
				account.Executable = tc.executable
				if tc.external {
					account.Owner = solana.PublicKey{0xee}
				}
				if tc.readonly {
					instr, err := ctx.TransactionContext.CurrentInstructionCtx()
					require.NoError(t, err)
					instr.InstructionAccounts[0].IsWritable = false
				}
				program := agaveSBFProgram(t, agaveSBFSlot(sbpf.OpStb, 1, 0, tc.offset, 0xa5))
				err := executeLoadedProgram(ctx, program, executionSyscalls(ctx))
				require.ErrorIs(t, err, tc.want)
				require.Equal(t, []byte{0, 0, 0, 0}, account.Data)
			})
		}
	}
}

// Agave's test_deplete_cost_meter_with_access_violation checks that a VM
// access fault consumes the rest of the instruction's CU budget. The failed
// account write must still report the account-specific error.
func TestAgaveSBFAccountAccessViolationDepletesComputeMeter(t *testing.T) {
	program := agaveSBFProgram(t, agaveSBFSlot(sbpf.OpStb, 1, 0, 96, 0xa5))
	for _, mapping := range agaveSBFMappings[1:] {
		t.Run(mapping.name, func(t *testing.T) {
			ctx := agaveSBFContext(t, mapping, []byte{0})
			ctx.ComputeMeter = cu.NewComputeMeter(10_000)
			instr, err := ctx.TransactionContext.CurrentInstructionCtx()
			require.NoError(t, err)
			instr.InstructionAccounts[0].IsWritable = false
			err = executeLoadedProgram(ctx, program, executionSyscalls(ctx))
			require.ErrorIs(t, err, InstrErrReadonlyDataModified)
			require.Zero(t, ctx.ComputeMeter.Remaining())
			require.Equal(t, []byte{0}, ctx.TransactionContext.Accounts.Accounts[1].Data)
		})
	}
}

// Agave excludes syscall errors from the VM-failure meter depletion rule.
// This keeps that exception explicit when the access-violation case is fixed.
func TestAgaveSBFSyscallErrorPreservesRemainingComputeMeter(t *testing.T) {
	program := agaveSBFProgram(t,
		agaveSBFSlot(sbpf.OpMov64Imm, 1, 0, 0, 0),
		agaveSBFSlot(sbpf.OpMov64Imm, 2, 0, 0, 0),
		agaveSBFSlot(sbpf.OpMov64Imm, 3, 0, 0, 1),
		agaveSBFSlot(sbpf.OpCall, 0, 0, 0, hash_sol_memcpy_),
	)
	for _, mapping := range agaveSBFMappings[1:] {
		t.Run(mapping.name, func(t *testing.T) {
			ctx := agaveSBFContext(t, mapping, nil)
			ctx.ComputeMeter = cu.NewComputeMeter(10_000)
			err := executeLoadedProgram(ctx, program, executionSyscalls(ctx))
			require.ErrorIs(t, err, InstrErrProgramFailedToComplete)
			require.NotZero(t, ctx.ComputeMeter.Remaining())
			require.Less(t, ctx.ComputeMeter.Remaining(), uint64(10_000))
		})
	}
}

// Agave's realloc test checks zero initialization, maximum growth, and a
// 64-bit store that straddles the old data length. These run through the
// serializer, VM, and deserializer rather than calling resize helpers.
func TestAgaveSBFReallocWithoutCPI(t *testing.T) {
	for _, mapping := range agaveSBFMappings {
		if mapping.vasa {
			t.Run(mapping.name+"/readonly_resize", func(t *testing.T) {
				ctx := agaveSBFContext(t, mapping, []byte{1, 2, 3, 4, 5, 6})
				instr, err := ctx.TransactionContext.CurrentInstructionCtx()
				require.NoError(t, err)
				instr.InstructionAccounts[0].IsWritable = false
				err = executeLoadedProgram(ctx, agaveSBFSetDataLengthProgram(t, 0), executionSyscalls(ctx))
				require.ErrorIs(t, err, InstrErrReadonlyDataModified)
				require.Equal(t, []byte{1, 2, 3, 4, 5, 6}, ctx.TransactionContext.Accounts.Accounts[1].Data)
			})
		}
		t.Run(mapping.name+"/shrink_to_zero", func(t *testing.T) {
			ctx := agaveSBFContext(t, mapping, []byte{1, 2, 3, 4, 5, 6})
			require.NoError(t, executeLoadedProgram(ctx, agaveSBFSetDataLengthProgram(t, 0), executionSyscalls(ctx)))
			require.Empty(t, ctx.TransactionContext.Accounts.Accounts[1].Data)
			require.EqualValues(t, -6, ctx.TransactionContext.AccountsResizeDelta)
		})
		t.Run(mapping.name+"/zero_initialized_growth", func(t *testing.T) {
			ctx := agaveSBFContext(t, mapping, []byte{1, 2, 3, 4, 5, 6})
			require.NoError(t, executeLoadedProgram(ctx, agaveSBFSetDataLengthProgram(t, 8), executionSyscalls(ctx)))
			require.Equal(t, []byte{1, 2, 3, 4, 5, 6, 0, 0}, ctx.TransactionContext.Accounts.Accounts[1].Data)
			require.EqualValues(t, 2, ctx.TransactionContext.AccountsResizeDelta)
		})
		// Agave's ZERO_INIT SBF program calls AccountInfo::realloc twice within
		// one invocation. Raw ABI length stores cannot exercise that SDK branch;
		// two loader invocations test the corresponding runtime resize path.
		t.Run(mapping.name+"/shrink_then_regrow_zeroes_truncated_bytes", func(t *testing.T) {
			ctx := agaveSBFContext(t, mapping, []byte{1, 2, 3, 4, 0xa5, 0xb6, 0xc7, 0xd8})
			require.NoError(t, executeLoadedProgram(ctx, agaveSBFSetDataLengthProgram(t, 4), executionSyscalls(ctx)))
			require.Equal(t, []byte{1, 2, 3, 4}, ctx.TransactionContext.Accounts.Accounts[1].Data)
			require.EqualValues(t, -4, ctx.TransactionContext.AccountsResizeDelta)
			require.NoError(t, executeLoadedProgram(ctx, agaveSBFSetDataLengthProgram(t, 8), executionSyscalls(ctx)))
			require.Equal(t, []byte{1, 2, 3, 4, 0, 0, 0, 0}, ctx.TransactionContext.Accounts.Accounts[1].Data)
			require.Zero(t, ctx.TransactionContext.AccountsResizeDelta)
		})
		t.Run(mapping.name+"/store_across_old_length", func(t *testing.T) {
			ctx := agaveSBFContext(t, mapping, []byte{1, 2, 3, 4, 5, 6})
			program := agaveSBFSetDataLengthProgram(t, 8,
				agaveSBFSlot(sbpf.OpLddw, 2, 0, 0, 0x55667788),
				agaveSBFSlot(0, 0, 0, 0, 0x11223344),
				agaveSBFSlot(sbpf.OpStxdw, 1, 2, 96, 0),
			)
			require.NoError(t, executeLoadedProgram(ctx, program, executionSyscalls(ctx)))
			require.Equal(t, []byte{0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11}, ctx.TransactionContext.Accounts.Accounts[1].Data)
			require.EqualValues(t, 2, ctx.TransactionContext.AccountsResizeDelta)
		})
		t.Run(mapping.name+"/realloc_and_assign", func(t *testing.T) {
			ctx := agaveSBFContext(t, mapping, nil)
			program := agaveSBFSetDataLengthProgram(t, MaxPermittedDataIncrease,
				agaveSBFSlot(sbpf.OpStdw, 1, 0, 48, 0),
				agaveSBFSlot(sbpf.OpStdw, 1, 0, 56, 0),
				agaveSBFSlot(sbpf.OpStdw, 1, 0, 64, 0),
				agaveSBFSlot(sbpf.OpStdw, 1, 0, 72, 0),
			)
			require.NoError(t, executeLoadedProgram(ctx, program, executionSyscalls(ctx)))
			account := ctx.TransactionContext.Accounts.Accounts[1]
			require.Equal(t, a.SystemProgramAddr, account.Owner)
			require.Len(t, account.Data, MaxPermittedDataIncrease)
			require.Equal(t, make([]byte, MaxPermittedDataIncrease), account.Data)
		})
		if mapping.vasa {
			t.Run(mapping.name+"/near_max_to_max", func(t *testing.T) {
				ctx := agaveSBFContext(t, mapping, make([]byte, MaxPermittedDataLength-0x100))
				require.NoError(t, executeLoadedProgram(ctx, agaveSBFSetDataLengthProgram(t, MaxPermittedDataLength), executionSyscalls(ctx)))
				account := ctx.TransactionContext.Accounts.Accounts[1]
				require.Len(t, account.Data, MaxPermittedDataLength)
				require.Zero(t, account.Data[len(account.Data)-1])
				require.EqualValues(t, 0x100, ctx.TransactionContext.AccountsResizeDelta)
			})
		}
		for _, tc := range []struct {
			name   string
			length uint64
		}{
			{name: "over_one_instruction_increase", length: 6 + MaxPermittedDataIncrease + 1},
			{name: "over_maximum_account_length", length: MaxPermittedDataLength + 1},
			{name: "overflowed_length", length: ^uint64(0)},
		} {
			t.Run(mapping.name+"/"+tc.name, func(t *testing.T) {
				ctx := agaveSBFContext(t, mapping, []byte{1, 2, 3, 4, 5, 6})
				err := executeLoadedProgram(ctx, agaveSBFSetDataLengthProgram(t, tc.length), executionSyscalls(ctx))
				require.ErrorIs(t, err, InstrErrInvalidRealloc)
				require.Equal(t, []byte{1, 2, 3, 4, 5, 6}, ctx.TransactionContext.Accounts.Accounts[1].Data)
			})
		}
		if mapping.vasa {
			t.Run(mapping.name+"/exhausted_transaction_budget", func(t *testing.T) {
				ctx := agaveSBFContext(t, mapping, []byte{1, 2, 3, 4, 5, 6})
				ctx.TransactionContext.AccountsResizeDelta = maxAccountDataGrowthPerTransaction
				err := executeLoadedProgram(ctx, agaveSBFSetDataLengthProgram(t, 7), executionSyscalls(ctx))
				require.ErrorIs(t, err, InstrErrMaxAccountsDataAllocationsExceeded)
				require.Equal(t, []byte{1, 2, 3, 4, 5, 6}, ctx.TransactionContext.Accounts.Accounts[1].Data)
				require.EqualValues(t, maxAccountDataGrowthPerTransaction, ctx.TransactionContext.AccountsResizeDelta)
			})
		}
	}
}

func agaveSBFMemoryVM(t *testing.T, ctx *ExecutionCtx, deprecated bool) (*sbpf.Interpreter, uint64) {
	t.Helper()
	var input []byte
	var metas []serializedAcctMetadata
	var regions []sbpf.InputRegion
	var err error
	if deprecated {
		ctx.TransactionContext.Accounts.Accounts[0].Owner = a.BpfLoaderDeprecatedAddr
		input, _, metas, regions, err = serializeParametersUnaligned(ctx)
	} else {
		input, _, metas, regions, err = serializeParametersAligned(ctx)
	}
	require.NoError(t, err)
	vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram}, &sbpf.VMOpts{
		Input: input, InputRegions: regions, Context: ctx, ComputeMeter: &ctx.ComputeMeter,
		HeapMax: 32 * 1024, DisableStackFrameGaps: true,
	})
	t.Cleanup(vm.Finish)
	return vm, metas[0].vmDataAddr
}

// Agave's test_mem_syscalls_overlap_account_begin_or_end runs these 16
// operations against a real account. VASA makes account data a distinct VM
// region, so none of the translations may span its beginning or reserved end.
// Without VASA each ABI has a single flat input buffer and permits the same
// ranges. ABI-v0 has no resize affordance; its zero-length account has no data
// region, so the operations at the end of that region can also succeed.
func TestAgaveSBFMemorySyscallsAtAccountBoundaries(t *testing.T) {
	const heap = sbpf.VaddrHeap
	endOffset := func(start, end, offset uint64) uint64 {
		if end-start < offset {
			return start
		}
		return end - offset
	}
	compare := func(vm sbpf.VM, left, right, n uint64) error {
		_, err := SyscallMemcmpImpl(vm, left, right, n, heap+2048)
		return err
	}
	copyData := func(vm sbpf.VM, dst, src, n uint64) error {
		_, err := SyscallMemcpyImpl(vm, dst, src, n)
		return err
	}
	moveData := func(vm sbpf.VM, dst, src, n uint64) error {
		_, err := SyscallMemmoveImpl(vm, dst, src, n)
		return err
	}
	setData := func(vm sbpf.VM, dst, value, n uint64) error {
		_, err := SyscallMemsetImpl(vm, dst, value, n)
		return err
	}
	cases := []struct {
		name string
		call func(sbpf.VM, uint64, uint64) error
	}{
		{"memcmp_begin_left", func(vm sbpf.VM, start, end uint64) error {
			length := uint64(500)
			if end-start < MaxPermittedDataIncrease { // ABI-v0 uses a 90-byte copy.
				length = 90
			}
			return compare(vm, start-8, heap, length)
		}},
		{"memcmp_begin_right", func(vm sbpf.VM, start, _ uint64) error { return compare(vm, heap, start-9, 12) }},
		{"memset_begin", func(vm sbpf.VM, start, _ uint64) error { return setData(vm, start-2, 3, 3) }},
		{"memcpy_begin_source", func(vm sbpf.VM, start, _ uint64) error { return copyData(vm, heap, start-3, 10) }},
		{"memmove_begin_source", func(vm sbpf.VM, start, _ uint64) error { return moveData(vm, heap, start-3, 10) }},
		{"memcpy_begin_destination", func(vm sbpf.VM, start, _ uint64) error { return copyData(vm, start-3, heap, 10) }},
		{"memmove_begin_destination", func(vm sbpf.VM, start, _ uint64) error { return moveData(vm, start-3, heap, 10) }},
		{"memmove_begin_reverse", func(vm sbpf.VM, start, _ uint64) error { return moveData(vm, start, start-3, 10) }},
		{"memcmp_end_right", func(vm sbpf.VM, start, end uint64) error { return compare(vm, heap, endOffset(start, end, 8), 16) }},
		{"memcmp_end_left", func(vm sbpf.VM, start, end uint64) error { return compare(vm, endOffset(start, end, 7), heap, 15) }},
		{"memset_end", func(vm sbpf.VM, start, end uint64) error { return setData(vm, endOffset(start, end, 2), 0, 3) }},
		{"memcpy_end_source", func(vm sbpf.VM, start, end uint64) error { return copyData(vm, heap, endOffset(start, end, 3), 10) }},
		{"memmove_end_source", func(vm sbpf.VM, start, end uint64) error { return moveData(vm, heap, endOffset(start, end, 3), 10) }},
		{"memcpy_end_destination", func(vm sbpf.VM, start, end uint64) error { return copyData(vm, endOffset(start, end, 3), heap, 10) }},
		{"memmove_end_destination", func(vm sbpf.VM, start, end uint64) error { return moveData(vm, endOffset(start, end, 3), heap, 10) }},
		{"memmove_end_reverse", func(vm sbpf.VM, start, end uint64) error { return moveData(vm, end, endOffset(start, end, 3), 10) }},
	}
	for _, mapping := range agaveSBFMappings {
		for _, deprecated := range []bool{false, true} {
			for _, dataLen := range []int{0, 1024} {
				for index, tc := range cases {
					name := mapping.name + "/"
					if deprecated {
						name += "deprecated/"
					} else {
						name += "aligned/"
					}
					if dataLen == 0 {
						name += "empty/"
					} else {
						name += "1024_bytes/"
					}
					t.Run(name+tc.name, func(t *testing.T) {
						ctx := agaveSBFContext(t, mapping, bytes.Repeat([]byte{0x42}, dataLen))
						vm, start := agaveSBFMemoryVM(t, ctx, deprecated)
						end := start + uint64(dataLen)
						if !deprecated {
							end += MaxPermittedDataIncrease
						}
						err := tc.call(vm, start, end)
						if mapping.vasa && !(deprecated && dataLen == 0 && index >= 8) {
							var badAccess sbpf.ExcBadAccess
							require.ErrorAs(t, err, &badAccess)
						} else {
							require.NoError(t, err)
						}
					})
				}
			}
		}
	}
}
