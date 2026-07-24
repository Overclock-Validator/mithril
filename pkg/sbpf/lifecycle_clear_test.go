package sbpf

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
	"github.com/stretchr/testify/require"
)

// These tests guard the pool zero-invariant that bounded clearing relies on:
// after Finish returns pooled stack/heap memory, a subsequent execution that
// only READS the same addresses must observe zeros. A regression here is a
// consensus bug (a program could read another program's leftover memory).

func newTestInterp(t *testing.T, program *Program, heapMax int) (*Interpreter, *cu.ComputeMeter) {
	t.Helper()
	meter := cu.NewComputeMeter(1 << 40)
	ip := NewInterpreter(program, &VMOpts{
		HeapMax:      heapMax,
		Syscalls:     func(uint32) (Syscall, bool) { return nil, false },
		ComputeMeter: &meter,
	})
	return ip, &meter
}

// storeProgram writes writeVal to the given VA (stack/heap) via r2, then exits.
func storeProgram(ver uint32, baseVA uint64, off int16, writeVal uint32) *Program {
	text := emitLddw(1, baseVA)
	text = append(text,
		testSlot(OpMov64Imm, 2, 0, 0, writeVal),
		testSlot(OpStxdw, 1, 2, off, 0),
		testSlot(OpExit, 0, 0, 0, 0),
	)
	return benchProgram(ver, text, nil)
}

// loadProgram loads 8 bytes from the given VA into r0 (no write), then exits.
func loadProgram(ver uint32, baseVA uint64, off int16) *Program {
	text := emitLddw(1, baseVA)
	text = append(text,
		testSlot(OpLdxdw, 0, 1, off, 0),
		testSlot(OpExit, 0, 0, 0, 0),
	)
	return benchProgram(ver, text, nil)
}

func TestPoolReuseHeapZeroed(t *testing.T) {
	if !UsePool {
		t.Skip("pool disabled")
	}
	const heapMax = 64 * 1024
	// Program A writes a sentinel deep into the heap.
	progA := storeProgram(sbpfver.SbpfVersionV0, VaddrHeap+0x8000, 0, 0xDEADBEEF)
	ipA, _ := newTestInterp(t, progA, heapMax)
	retA, _, err := ipA.Run()
	require.NoError(t, err)
	_ = retA
	ipA.Finish() // returns heap to the pool; must be fully zeroed

	// Program B reads the same heap address without writing. If clearing is
	// broken, it observes A's sentinel instead of zero.
	progB := loadProgram(sbpfver.SbpfVersionV0, VaddrHeap+0x8000, 0)
	ipB, _ := newTestInterp(t, progB, heapMax)
	ret, _, err := ipB.Run()
	require.NoError(t, err)
	require.Equal(t, uint64(0), ret, "heap leaked a prior execution's write")
	ipB.Finish()
}

func TestPoolReuseStackZeroed(t *testing.T) {
	if !UsePool {
		t.Skip("pool disabled")
	}
	// r10 points just past the first frame; write near the top of the frame.
	// Program A writes a sentinel to [r10-8].
	textA := []Slot{
		testSlot(OpMov64Imm, 2, 0, 0, 0xCAFEBABE),
		testSlot(OpStxdw, 10, 2, -8, 0),
		testSlot(OpExit, 0, 0, 0, 0),
	}
	progA := benchProgram(sbpfver.SbpfVersionV0, textA, nil)
	ipA, _ := newTestInterp(t, progA, 4096)
	_, _, err := ipA.Run()
	require.NoError(t, err)
	ipA.Finish()

	// Program B reads [r10-8] without writing.
	textB := []Slot{
		testSlot(OpLdxdw, 0, 10, -8, 0),
		testSlot(OpExit, 0, 0, 0, 0),
	}
	progB := benchProgram(sbpfver.SbpfVersionV0, textB, nil)
	ipB, _ := newTestInterp(t, progB, 4096)
	ret, _, err := ipB.Run()
	require.NoError(t, err)
	require.Equal(t, uint64(0), ret, "stack leaked a prior execution's write")
	ipB.Finish()
}

// TestPoolReuseZeroedFuzz drains and reuses the pool many times with varied
// write offsets, then verifies a fresh interpreter's backing memory is fully
// zero across the whole capacity (not just the touched prefix).
func TestPoolReuseZeroedFuzz(t *testing.T) {
	if !UsePool {
		t.Skip("pool disabled")
	}
	offsets := []uint64{0, 8, 0x1000, 0x4000, 0x8000, 0xF000}
	for _, off := range offsets {
		prog := storeProgram(sbpfver.SbpfVersionV0, VaddrHeap+off, 0, 0x11223344)
		ip, _ := newTestInterp(t, prog, 64*1024)
		_, _, err := ip.Run()
		require.NoError(t, err)
		ip.Finish()
	}
	// A fresh interpreter should get an all-zero heap and stack.
	ip, _ := newTestInterp(t, benchProgram(sbpfver.SbpfVersionV0, []Slot{testSlot(OpExit, 0, 0, 0, 0)}, nil), 64*1024)
	for i := range ip.heap {
		require.Zerof(t, ip.heap[i], "heap[%d] not zero after pool reuse", i)
	}
	for i := range ip.stack.mem {
		require.Zerof(t, ip.stack.mem[i], "stack.mem[%d] not zero after pool reuse", i)
	}
	ip.Finish()
}

// TestFinishZeroesReturnedBuffers deterministically verifies (without relying on
// sync.Pool reuse probability) that Finish restores the full-zero contract on
// the exact backing buffers it returns to the pool: it captures the buffers
// after a write-heavy Run, calls Finish, then asserts every byte across the
// whole capacity is zero.
func TestFinishZeroesReturnedBuffers(t *testing.T) {
	if !UsePool {
		t.Skip("pool disabled")
	}
	text := emitLddw(3, VaddrHeap+0x3000)
	text = append(text,
		testSlot(OpMov64Imm, 2, 0, 0, 0xABCD),
		testSlot(OpStxdw, 3, 2, 0, 0),   // heap[0x3000] = sentinel
		testSlot(OpStxdw, 10, 2, -8, 0), // stack top = sentinel
		testSlot(OpExit, 0, 0, 0, 0),
	)
	prog := benchProgram(sbpfver.SbpfVersionV0, text, nil)
	ip, _ := newTestInterp(t, prog, 64*1024)
	_, _, err := ip.Run()
	require.NoError(t, err)
	require.NotZero(t, ip.heapDirtyEnd, "heap write should have advanced dirtyEnd")
	require.NotZero(t, ip.stack.dirtyEnd, "stack write should have advanced dirtyEnd")

	// Full-capacity views of the exact buffers Finish returns to the pool.
	heapBuf := ip.heap[:cap(ip.heap)]
	stackBuf := ip.stack.mem[:cap(ip.stack.mem)]
	ip.Finish()

	for i := range heapBuf {
		if heapBuf[i] != 0 {
			t.Fatalf("heap buffer byte %d = %#x, not zero after Finish", i, heapBuf[i])
		}
	}
	for i := range stackBuf {
		if stackBuf[i] != 0 {
			t.Fatalf("stack buffer byte %d = %#x, not zero after Finish", i, stackBuf[i])
		}
	}
}

// TestDirtyEndTracksSyscallStyleWrite verifies a Translate(write=true) that the
// caller fills afterward is still zeroed on Finish (the memset syscall pattern).
func TestDirtyEndTracksSyscallStyleWrite(t *testing.T) {
	if !UsePool {
		t.Skip("pool disabled")
	}
	prog := benchProgram(sbpfver.SbpfVersionV0, []Slot{testSlot(OpExit, 0, 0, 0, 0)}, nil)
	ip, _ := newTestInterp(t, prog, 64*1024)
	// Translate a heap range for writing, then fill it (as a syscall would).
	mem, err := ip.Translate(VaddrHeap+0x2000, 128, true)
	require.NoError(t, err)
	for i := range mem {
		mem[i] = 0x7F
	}
	require.GreaterOrEqual(t, ip.heapDirtyEnd, uint64(0x2000+128), "dirtyEnd must cover a write-translated range")
	ip.Finish()

	// Next reuse must be zeroed at that range.
	ip2, _ := newTestInterp(t, prog, 64*1024)
	mem2, err := ip2.Translate(VaddrHeap+0x2000, 128, false)
	require.NoError(t, err)
	for i := range mem2 {
		require.Zerof(t, mem2[i], "heap[%d] leaked after syscall-style write", 0x2000+i)
	}
	ip2.Finish()
}
