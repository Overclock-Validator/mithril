package sbpf

import (
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"testing"
)

// VMOpts.Syscalls determines dispatch, including when the same loaded program
// is invoked with a registry different from the one used for pre-resolution.
func TestFastLoopInvocationRegistryPrecedence(t *testing.T) {
	const hash = uint32(0x12345678)
	p := mkProgram([]Slot{
		slot(OpCall, 0, 0, 0, hash),
		slot(OpExit, 0, 0, 0, 0),
		slot(OpMov64Imm, 0, 0, 0, 42),
		slot(OpExit, 0, 0, 0, 0),
	}, 0)
	p.Funcs[hash] = 2
	resolveCallTargetsIfSupported(p)
	registry := SyscallRegistry(func(h uint32) (Syscall, bool) {
		if h != hash {
			return nil, false
		}
		return SyscallFunc5(func(VM, uint64, uint64, uint64, uint64, uint64) (uint64, error) { return 99, nil }), true
	})
	prev := useFastLoop
	defer func() { useFastLoop = prev }()
	for _, fast := range []bool{false, true} {
		useFastLoop = fast
		meter := cu.NewComputeMeter(100)
		ip := NewInterpreter(p, &VMOpts{HeapMax: 4096, Syscalls: registry, ComputeMeter: &meter})
		ret, used, err := ip.Run()
		ip.Finish()
		if err != nil || ret != 99 || used != 2 || meter.Remaining() != 98 {
			t.Fatalf("fast=%v: ret=%d used=%d remaining=%d err=%v; invocation syscall must take precedence", fast, ret, used, meter.Remaining(), err)
		}
	}
}
