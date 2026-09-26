package sealevel

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemainingComputeUnitsRegistration(t *testing.T) {
	const name = "sol_remaining_compute_units"
	assert.Equal(t, uint32(0xedef5aee), sbpf.SymbolHash(name))
	assert.Equal(t, "5TuppMutoyzhUSfuYdhgzD47F92GL1g89KpCZQKqedxP", solana.PublicKey(features.RemainingComputeUnitsSyscallEnabled.Address).String())
	assert.Contains(t, features.AllFeatureGates, features.RemainingComputeUnitsSyscallEnabled)

	ft := features.NewFeaturesDefault()
	for _, deploy := range []bool{false, true} {
		_, ok := Syscalls(ft, deploy, sbpf.SymbolHash(name))
		assert.False(t, ok)
	}
	ft.EnableFeature(features.RemainingComputeUnitsSyscallEnabled, 0)
	for _, deploy := range []bool{false, true} {
		syscall, ok := Syscalls(ft, deploy, sbpf.SymbolHash(name))
		require.True(t, ok)
		require.NotNil(t, syscall)
	}
}

func TestRemainingComputeUnitsChargesBeforeReturning(t *testing.T) {
	for _, tc := range []struct {
		name   string
		budget uint64
		want   uint64
		fail   bool
	}{
		{name: "remaining", budget: 1_000, want: 900},
		{name: "exact cost", budget: 100, want: 0},
		{name: "insufficient", budget: 99, fail: true},
		{name: "empty", budget: 0, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &ExecutionCtx{ComputeMeter: cu.NewComputeMeter(tc.budget)}
			vm := sbpf.NewInterpreter(&sbpf.Program{}, &sbpf.VMOpts{Context: ctx, ComputeMeter: &ctx.ComputeMeter})
			defer vm.Finish()
			remaining, err := SyscallRemainingComputeUnits.Invoke(vm, 0, 0, 0, 0, 0)
			if tc.fail {
				require.ErrorIs(t, err, InstrErrComputationalBudgetExceeded)
				assert.Zero(t, ctx.ComputeMeter.Remaining())
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.want, remaining)
				assert.Equal(t, tc.want, ctx.ComputeMeter.Remaining())
			}
		})
	}
}

func TestRemainingComputeUnitsInterpreterCall(t *testing.T) {
	ft := features.NewFeaturesDefault()
	ft.EnableFeature(features.RemainingComputeUnitsSyscallEnabled, 0)
	for _, version := range []sbpfver.SbpfVersion{{Version: sbpfver.SbpfVersionV0}, {Version: sbpfver.SbpfVersionV3}} {
		ctx := &ExecutionCtx{ComputeMeter: cu.NewComputeMeter(1_000)}
		program := &sbpf.Program{
			SbpfVersion: version,
			Text: []sbpf.Slot{
				sbpf.Slot(sbpf.OpCall) | sbpf.Slot(uint64(0xedef5aee)<<32),
				sbpf.Slot(sbpf.OpExit),
			},
		}
		vm := sbpf.NewInterpreter(program, &sbpf.VMOpts{
			Context: ctx, ComputeMeter: &ctx.ComputeMeter,
			Syscalls: func(hash uint32) (sbpf.Syscall, bool) { return Syscalls(ft, false, hash) },
		})
		ret, _, err := vm.Run()
		vm.Finish()
		require.NoError(t, err)
		assert.Equal(t, uint64(899), ret, "call instruction and syscall are charged before the observed balance")
		assert.Equal(t, uint64(898), ctx.ComputeMeter.Remaining(), "exit is charged after the observed balance")
	}
}
