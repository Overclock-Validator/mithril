package sealevel

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/stretchr/testify/require"
)

func TestSiblingHeaderWriteIsClearedOnFinish(t *testing.T) {
	old := sbpf.UsePool
	sbpf.UsePool = true
	t.Cleanup(func() { sbpf.UsePool = old })
	for _, addr := range []uint64{sbpf.VaddrStack + 128, sbpf.VaddrHeap + 128} {
		ctx := &ExecutionCtx{ComputeMeter: cu.NewComputeMeter(100000), TransactionContext: &TransactionCtx{
			InstructionTrace: []InstructionCtx{{Data: []byte{1, 2, 3}}, {}, {}}, InstructionStack: []uint64{1},
		}}
		vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram}, &sbpf.VMOpts{HeapMax: 32768, Context: ctx, ComputeMeter: &ctx.ComputeMeter})
		// Keep a read-only view so the test itself does not mark the header dirty.
		header, err := vm.Translate(addr, ProcessedSiblingInstructionSize, false)
		require.NoError(t, err)
		require.Equal(t, make([]byte, 16), header)
		found, err := SyscallGetProcessedSiblingInstructionImpl(vm, 0, addr, 0, 0, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(1), found)
		require.Equal(t, byte(3), header[0])
		vm.Finish()
		// Inspect before allocating another VM: this is deterministic and does not
		// depend on sync.Pool choosing a particular backing buffer on the next Get.
		require.Equal(t, make([]byte, 16), header)
	}
}

func TestSiblingHeaderRejectsReadOnlyDestination(t *testing.T) {
	data := make([]byte, 16)
	vm, ctx := newMemSyscallVM(t, nil, []sbpf.InputRegion{{RegionSize: 16, AddressSpaceReserved: 16, Data: data}})
	ctx.TransactionContext = &TransactionCtx{InstructionTrace: []InstructionCtx{{Data: []byte{1, 2, 3}}, {}, {}}, InstructionStack: []uint64{1}}
	_, err := SyscallGetProcessedSiblingInstructionImpl(vm, 0, sbpf.VaddrInput, 0, 0, 0)
	require.Error(t, err)
	require.Equal(t, make([]byte, 16), data)
}
