package sealevel

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyscallLogRejectsInvalidUTF8(t *testing.T) {
	vm := newBls12_381SyscallTestVM(4)
	vm.mem[0] = 0xff

	_, err := SyscallLogImpl(vm, 0, 1)
	require.ErrorIs(t, err, SyscallErrInvalidString)
	assert.Equal(t, uint64(1_000_000-cu.CUSyscallBaseCost), vm.ComputeMeter().Remaining())
}

type discardingTestLogger struct{ lines int }

func (l *discardingTestLogger) Log(string)         { l.lines++ }
func (l *discardingTestLogger) DiscardsLogs() bool { return true }

// A logger that discards its lines lets the log syscalls skip formatting them.
// Nothing a program can observe may change: the result, the error and the
// compute units charged must match a recording logger's on every input,
// including the ones that fail partway.
func TestLogSyscallsWithDiscardingLoggerMatchRecording(t *testing.T) {
	const memSize = 256
	setup := func(vm *bls12_381SyscallTestVM) {
		copy(vm.mem[0:], "hello, log")
		vm.mem[20] = 0xff // invalid UTF-8 at 20
		for i := 32; i < 64; i++ {
			vm.mem[i] = byte(i)
		}
		// Slice descriptors for sol_log_data at 96: two valid, then one whose
		// data lies outside memory.
		for i, d := range [][2]uint64{{0, 10}, {32, 32}, {memSize - 4, 64}} {
			binary.LittleEndian.PutUint64(vm.mem[96+16*i:], d[0])
			binary.LittleEndian.PutUint64(vm.mem[96+16*i+8:], d[1])
		}
	}
	cases := []struct {
		name  string
		lines int // lines a recording logger receives
		call  func(vm *bls12_381SyscallTestVM) (uint64, error)
	}{
		{"log", 1, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogImpl(vm, 0, 10) }},
		{"log empty", 1, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogImpl(vm, 0, 0) }},
		{"log invalid utf8", 0, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogImpl(vm, 16, 8) }},
		{"log out of bounds", 0, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogImpl(vm, memSize-4, 8) }},
		{"log beyond budget", 0, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogImpl(vm, 0, 2_000_000) }},
		{"log_64", 1, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLog64Impl(vm, 1, 2, 3, 4, 5) }},
		{"log_compute_units", 1, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogCUsImpl(vm) }},
		{"log_pubkey", 1, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogPubkeyImpl(vm, 32) }},
		{"log_pubkey out of bounds", 0, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogPubkeyImpl(vm, memSize-8) }},
		{"log_data none", 1, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogDataImpl(vm, 96, 0) }},
		{"log_data two slices", 1, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogDataImpl(vm, 96, 2) }},
		{"log_data bad third slice", 0, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogDataImpl(vm, 96, 3) }},
		{"log_data descriptors out of bounds", 0, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogDataImpl(vm, memSize-8, 2) }},
		{"log_data length overflow", 0, func(vm *bls12_381SyscallTestVM) (uint64, error) { return SyscallLogDataImpl(vm, 96, 1<<61) }},
	}
	for _, tc := range cases {
		for _, budget := range []uint64{1_000_000, 150, 101, 100, 99, 1, 0} {
			rec := newBls12_381SyscallTestVM(memSize)
			rec.ctx.ComputeMeter = cu.NewComputeMeter(budget)
			setup(rec)
			wantRet, wantErr := tc.call(rec)

			dis := newBls12_381SyscallTestVM(memSize)
			dis.ctx.ComputeMeter = cu.NewComputeMeter(budget)
			logger := new(discardingTestLogger)
			dis.ctx.Log = logger
			setup(dis)
			gotRet, gotErr := tc.call(dis)

			require.Equalf(t, wantRet, gotRet, "%s budget %d: result", tc.name, budget)
			require.Equalf(t, wantErr, gotErr, "%s budget %d: error", tc.name, budget)
			require.Equalf(t, rec.ComputeMeter().Remaining(), dis.ComputeMeter().Remaining(), "%s budget %d: compute units", tc.name, budget)
			require.Zerof(t, logger.lines, "%s budget %d: the discarding logger was given a line", tc.name, budget)
			if budget == 1_000_000 {
				require.Lenf(t, rec.ctx.Log.(*LogRecorder).Logs, tc.lines, "%s: lines recorded", tc.name)
				require.Equalf(t, tc.lines == 0, wantErr != nil, "%s: the case must fail exactly when it logs nothing", tc.name)
			}
		}
	}
}
