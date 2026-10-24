package sbpf

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
)

// These tests replace the pools only while no other test is running. Returning
// known holders from New makes reuse deterministic even if sync.Pool drops items
// (in particular under the race detector).
func isolateInvocationPools(t *testing.T) {
	t.Helper()
	oldHeap, oldMem, oldShadow, oldUse := heapPool, stackMemPool, stackShadowPool, UsePool
	t.Cleanup(func() {
		heapPool, stackMemPool, stackShadowPool, UsePool = oldHeap, oldMem, oldShadow, oldUse
	})
	UsePool = true
	heapPool = &sync.Pool{New: func() any { return &byteBuffer{data: newHeap()} }}
	stackMemPool = &sync.Pool{New: func() any { return &byteBuffer{data: newStackMem()} }}
	stackShadowPool = &sync.Pool{New: func() any { return &frameBuffer{data: newStackShadow()} }}
}

func reuseInvocationBuffers(vm *Interpreter) {
	heapPool = &sync.Pool{New: func() any { return vm.heapBuffer }}
	stackMemPool = &sync.Pool{New: func() any { return vm.stack.memBuffer }}
	stackShadowPool = &sync.Pool{New: func() any { return vm.stack.shadowBuffer }}
}

func newBufferTestVM(version uint32, gapsDisabled bool, heapMax int) *Interpreter {
	meter := cu.NewComputeMeter(100_000)
	return NewInterpreter(&Program{SbpfVersion: sbpfver.SbpfVersion{Version: version}},
		&VMOpts{HeapMax: heapMax, DisableStackFrameGaps: gapsDisabled, ComputeMeter: &meter})
}

func assertZeroStack(t *testing.T, mem []byte, shadow []Frame) {
	t.Helper()
	for i, v := range mem {
		if v != 0 {
			t.Fatalf("stack byte %d retained %#x", i, v)
		}
	}
	for i, f := range shadow {
		if f != (Frame{}) {
			t.Fatalf("shadow frame %d retained %+v", i, f)
		}
	}
}

func TestInvocationBufferReuse(t *testing.T) {
	isolateInvocationPools(t)
	heapSizes := []int{32 << 10, 256 << 10, 512 << 10, 4 << 10, 256 << 10}
	for i, version := range []uint32{0, 1, 2, 3, 0} {
		t.Run(fmt.Sprintf("v%d/heap%d", version, heapSizes[i]), func(t *testing.T) {
			vm := newBufferTestVM(version, i == 4, heapSizes[i])
			if len(vm.heap) != heapSizes[i] || len(vm.stack.mem) != StackMax || len(vm.stack.shadow) != 1 {
				t.Fatal("buffer sizes changed")
			}
			if !bytes.Equal(vm.heap, make([]byte, len(vm.heap))) {
				t.Fatal("reused heap is not zero")
			}
			assertZeroStack(t, vm.stack.mem, vm.stack.shadow[1:StackDepth])
			wantFP := uint64(VaddrStack + StackFrameSize)
			if version == 1 || version == 2 {
				wantFP = VaddrStack + StackMax
			}
			if vm.stack.GetFramePtr() != wantFP {
				t.Fatal("initial frame pointer changed")
			}
			// Write through VM translation so existing dirty-range cleanup sees it.
			heap, err := vm.Translate(VaddrHeap, uint64(len(vm.heap)), true)
			if err != nil {
				t.Fatal(err)
			}
			for j := range heap {
				heap[j] = 0x5a
			}
			for j := 0; j < StackDepth; j++ {
				addr := VaddrStack + uint64(j*StackFrameSize)
				if vm.stack.stackFrameGaps {
					addr = VaddrStack + uint64(j*StackFrameSize*2)
				}
				frame, err := vm.Translate(addr, StackFrameSize, true)
				if err != nil {
					t.Fatal(err)
				}
				for k := range frame {
					frame[k] = 0xa5
				}
			}
			regs := &[16]uint64{0, 0, 0, 0, 0, 0, 6, 7, 8, 9, wantFP}
			for depth := 1; depth < StackDepth; depth++ {
				if !vm.stack.Push(regs, int64(depth)) {
					t.Fatal("push failed")
				}
			}
			mem, shadow := vm.stack.mem, vm.stack.shadow[:StackDepth]
			vm.Finish()
			assertZeroStack(t, mem, shadow)
			reuseInvocationBuffers(vm)
		})
	}
}

func TestInvocationBuffersNested(t *testing.T) {
	isolateInvocationPools(t)
	parent := newBufferTestVM(0, false, 32<<10)
	defer parent.Finish()
	if err := parent.Write64(VaddrStack, 123); err != nil {
		t.Fatal(err)
	}
	if err := parent.Write64(VaddrHeap, 456); err != nil {
		t.Fatal(err)
	}
	for _, version := range []uint32{0, 1, 2, 3} {
		child := newBufferTestVM(version, false, 256<<10)
		if child.heapBuffer == parent.heapBuffer || child.stack.memBuffer == parent.stack.memBuffer || child.stack.shadowBuffer == parent.stack.shadowBuffer {
			t.Fatal("nested VM shares an active buffer")
		}
		if err := child.Write64(VaddrStack, 789); err != nil {
			t.Fatal(err)
		}
		if err := child.Write64(VaddrHeap, 987); err != nil {
			t.Fatal(err)
		}
		child.Finish()
		if v, err := parent.Read64(VaddrStack); v != 123 || err != nil {
			t.Fatalf("parent stack: %d %v", v, err)
		}
		if v, err := parent.Read64(VaddrHeap); v != 456 || err != nil {
			t.Fatalf("parent heap: %d %v", v, err)
		}
	}
}

func TestInvocationBuffersWithoutPool(t *testing.T) {
	isolateInvocationPools(t)
	UsePool = false
	vm := newBufferTestVM(0, false, 32<<10)
	if len(vm.heap) != 256<<10 || vm.heapBuffer != nil || vm.stack.memBuffer != nil || vm.stack.shadowBuffer != nil {
		t.Fatal("non-pooled allocation behavior changed")
	}
	if err := vm.Write64(VaddrStack, 123); err != nil {
		t.Fatal(err)
	}
	vm.Finish()
	if v, err := vm.Read64(VaddrStack); v != 123 || err != nil {
		t.Fatal("non-pooled Finish behavior changed")
	}
}

func TestInvocationBuffersExecutionError(t *testing.T) {
	isolateInvocationPools(t)
	code := []Slot{testSlot(OpMov64Imm, 0, 0, 0, 42), testSlot(OpStxdw, 10, 0, -8, 0), testSlot(0xff, 0, 0, 0, 0)}
	meter := cu.NewComputeMeter(100_000)
	vm := NewInterpreter(&Program{Text: code, TextBytes: testSlotsToBytes(code), TextVA: VaddrProgram}, &VMOpts{HeapMax: 32 << 10, ComputeMeter: &meter})
	mem, shadow := vm.stack.mem, vm.stack.shadow[:StackDepth]
	func() {
		defer vm.Finish()
		if _, _, err := vm.Run(); err == nil {
			t.Fatal("expected execution error")
		}
		if v, err := vm.Read64(VaddrStack + StackFrameSize - 8); v != 42 || err != nil {
			t.Fatal("program did not write stack before error")
		}
	}()
	assertZeroStack(t, mem, shadow)
	reuseInvocationBuffers(vm)
	next := newBufferTestVM(3, false, 32<<10)
	defer next.Finish()
	assertZeroStack(t, next.stack.mem, next.stack.shadow[1:StackDepth])
}
