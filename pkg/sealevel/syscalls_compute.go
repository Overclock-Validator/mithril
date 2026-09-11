package sealevel

import (
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
)

// SyscallRemainingComputeUnitsImpl returns the balance after charging the syscall,
// matching Agave's sol_remaining_compute_units runtime behavior.
func SyscallRemainingComputeUnitsImpl(vm sbpf.VM) (uint64, error) {
	execCtx := executionCtx(vm)
	if err := execCtx.ComputeMeter.Consume(cu.CUSyscallBaseCost); err != nil {
		return syscallCuErr()
	}
	return syscallSuccess(execCtx.ComputeMeter.Remaining())
}

var SyscallRemainingComputeUnits = sbpf.SyscallFunc0(SyscallRemainingComputeUnitsImpl)
