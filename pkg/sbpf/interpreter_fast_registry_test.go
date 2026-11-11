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

// A legacy internal call may skip the registry only while the registry it was
// checked against is the one in use: the answer follows the registry's
// identity and generation, a registry that claims one of the program's call
// immediates keeps every call on the lookup, and a program is not trusted
// without an identity.
func TestLegacyCallsFollowRegistryIdentity(t *testing.T) {
	const fn, other = uint32(0x12345678), uint32(0x0badcafe)
	p := mkProgram([]Slot{
		slot(OpCall, 0, 0, 0, fn),
		slot(OpExit, 0, 0, 0, 0),
		slot(OpMov64Imm, 0, 0, 0, 42),
		slot(OpExit, 0, 0, 0, 0),
	}, 0)
	p.Funcs[fn] = 2
	p.ResolveCallTargets()
	registryFor := func(hash uint32) (SyscallRegistry, *int) {
		lookups := new(int)
		return func(h uint32) (Syscall, bool) {
			*lookups++
			if h != hash {
				return nil, false
			}
			return SyscallFunc5(func(VM, uint64, uint64, uint64, uint64, uint64) (uint64, error) { return 99, nil }), true
		}, lookups
	}
	run := func(name string, registry SyscallRegistry, id any, gen uint64, wantRet, wantUsed uint64, wantFast bool) {
		t.Helper()
		for _, mode := range []string{"run", "fast", "asm"} {
			prevFast, prevAsm := useFastLoop, useAsmLoop
			useFastLoop, useAsmLoop = mode != "run", mode == "asm"
			meter := cu.NewComputeMeter(100)
			ip := NewInterpreter(p, &VMOpts{HeapMax: 4096, Syscalls: registry, SyscallsID: id, SyscallsGen: gen, ComputeMeter: &meter})
			if got := ip.fastCalls != nil; got != wantFast {
				t.Errorf("%s (%s): fastCalls set = %t, want %t", name, mode, got, wantFast)
			}
			ret, used, err := ip.Run()
			ip.Finish()
			useFastLoop, useAsmLoop = prevFast, prevAsm
			if err != nil || ret != wantRet || used != wantUsed {
				t.Errorf("%s (%s): ret=%d used=%d err=%v, want ret=%d used=%d", name, mode, ret, used, err, wantRet, wantUsed)
			}
		}
	}
	type id struct{ n int }

	clean, cleanLookups := registryFor(other)
	run("no identity", clean, nil, 0, 42, 4, false)
	if *cleanLookups == 0 {
		t.Fatal("without an identity the registry must be asked at the call")
	}
	run("identity, registry does not know the function", clean, id{1}, 7, 42, 4, true)
	checked := *cleanLookups
	run("same identity again", clean, id{1}, 7, 42, 4, true)
	if *cleanLookups != checked {
		t.Fatalf("the registry was asked %d more times under an identity already checked", *cleanLookups-checked)
	}

	// The same program under registries that claim the immediate: by a new
	// identity, and by a new generation of the old one.
	shadow, _ := registryFor(fn)
	run("new identity, registry claims the immediate", shadow, id{2}, 7, 99, 2, false)
	run("back to the first identity", clean, id{1}, 7, 42, 4, true)
	run("new generation, registry claims the immediate", shadow, id{1}, 8, 99, 2, false)
	run("first generation again", clean, id{1}, 7, 42, 4, true)

	// A table that does not cover the text exactly is never used.
	p.CallTargets = p.CallTargets[:len(p.Text)-1]
	p.callCheck.Store(nil)
	if p.unshadowedCallTargets(id{3}, 0, clean) != nil {
		t.Fatal("a call table shorter than the text was accepted")
	}
}

// A legacy internal call taken without the registry must fail exactly as one
// taken through it: on a full shadow stack, with the same error and CU.
func TestLegacyCallDepthMatchesRun(t *testing.T) {
	const fn = uint32(0x12345678)
	p := mkProgram([]Slot{
		slot(OpAdd64Imm, 1, 0, 0, 1),
		slot(OpCall, 0, 0, 0, fn), // recurses until the shadow stack is full
		slot(OpExit, 0, 0, 0, 0),
	}, 0)
	p.Funcs[fn] = 0
	p.ResolveCallTargets()
	for _, budget := range []uint64{1, 2, 3, 50, 127, 128, 129, 1000} {
		requireSameWithAndWithoutFastLoop(t, "recursion", p, budget, 1)
	}
}
