package sbpf

import (
	"math"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
)

// Stack is the VM's call frame stack.
//
// # Memory stack
//
// The memory stack resides in addressable memory at VaddrStack.
//
// It is split into statically sized stack frames (StackFrameSize).
// Each frame stores spilled function arguments and local variables.
// The frame pointer (r10) points to the highest address in the current frame.
//
// New frames get allocated upwards.
// Each frame is followed by a gap of size StackFrameSize.
//
//	[0x1_0000_0000]: Frame
//	[0x1_0000_1000]: Gap
//	[0x1_0000_2000]: Frame
//	[0x1_0000_3000]: Gap
//	...
//
// # Shadow stack
//
// The shadow stack is not directly accessible from SBF.
// It stores return addresses and caller-preserved registers.
type Stack struct {
	mem                []byte
	sp                 uint64
	shadow             []Frame
	dynamicStackFrames bool
	stackFrameGaps     bool

	// written is the high-water mark, in bytes from the base of mem, of
	// everything this run has written to the memory stack. Finish clears only
	// that prefix before returning mem to the pool.
	//
	// The safety argument is inductive. A pooled buffer is handed out fully
	// zeroed: newStackMem allocates zeroed memory, and every buffer that
	// re-enters the pool has had everything it wrote cleared. So clearing the
	// prefix restores the all-zero state the next run relies on, and a program
	// still cannot observe a previous program's bytes.
	//
	// This depends on every write to mem passing through markWritten.
	// Interpreter.translateInternal is the only non-test caller of GetFrame, and
	// nothing else writes mem, so that choke point holds.
	//
	// Over-marking is safe and only costs a larger clear; under-marking would
	// leak. Where the two are in tension, prefer marking more.
	written uint64
}

// markWritten records that [end-size, end) has been written, where end is a
// byte offset into mem. The branch stops being taken once a run settles into
// its frames, so the common case is a load and a well-predicted compare.
func (s *Stack) markWritten(end uint64) {
	if end > s.written {
		s.written = end
	}
}

// Frame is an entry on the shadow stack.
type Frame struct {
	FramePtr uint64
	NVRegs   [4]uint64
	RetAddr  int64
}

// StackFrameSize is the addressable memory within a stack frame.
//
// Note that this constant cannot be changed trivially.
const (
	StackFrameSize          = 0x1000
	StackMax                = 64 * StackFrameSize
	DynamicStackFramesAlign = 64
)

// StackDepth is the max frame count of the stack.
const StackDepth = 64

var GapMask = uint64(0xFFFFFFFFFFFFF000)

func newStackMem() []byte {
	return make([]byte, StackMax)
}

func newStackShadow() []Frame {
	return make([]Frame, 1, StackDepth)
}

var (
	// Also applies to interpreter heap.
	UsePool = true

	stackMemPool = &sync.Pool{New: func() interface{} {
		return newStackMem()
	}}

	stackShadowPool = &sync.Pool{New: func() interface{} {
		return newStackShadow()
	}}
)

func NewStack(sbpfVer sbpfver.SbpfVersion, disableStackFrameGaps bool) Stack {
	var m []byte
	var sh []Frame
	if UsePool {
		m = stackMemPool.Get().([]byte)
		sh = stackShadowPool.Get().([]Frame)
	} else {
		m = newStackMem()
		sh = newStackShadow()
	}

	s := Stack{
		mem:    m,
		sp:     VaddrStack,
		shadow: sh,
	}

	var sz uint64
	if sbpfVer.DynamicStackFrames() {
		sz = StackMax
		s.dynamicStackFrames = true
	} else {
		sz = StackFrameSize
		s.stackFrameGaps = sbpfVer.StackFrameGaps() && !disableStackFrameGaps
	}

	s.shadow[0] = Frame{
		FramePtr: VaddrStack + sz,
	}
	return s
}

func (s *Stack) Finish() {
	if UsePool {
		s.mem = s.mem[:StackMax]
		// Only the prefix this run wrote can be non-zero, so only that prefix
		// needs restoring before the buffer goes back to the pool. Clearing all
		// 256 KiB was 68% of interpreter setup cost, and across the vm-programs
		// corpus 46% of runs write no stack at all and 95% write at most one
		// 4 KiB frame. See the `written` field for the safety argument.
		//
		// min guards the clear against a mark that somehow exceeds the buffer;
		// markWritten's callers cannot produce one, since off+size is bounded by
		// StackMax, but a wrong clear length here would be a silent leak.
		clear(s.mem[:min(s.written, StackMax)])
		stackMemPool.Put(s.mem)
		s.shadow = s.shadow[:StackDepth]
		clear(s.shadow)
		s.shadow = s.shadow[:1]
		stackShadowPool.Put(s.shadow)
	}
}

// GetFramePtr returns the current frame pointer.
func (s *Stack) GetFramePtr() uint64 {
	return s.shadow[len(s.shadow)-1].FramePtr
}

// frameOffset resolves a stack virtual address to a physical offset into mem,
// without materialising a slice. GetFrame is the slice-returning form kept for
// callers that want one; the interpreter's translate path uses this instead,
// because building a slice header per memory access -- 30.6% of instructions on
// real mainnet traffic -- only to take the address of its first byte is pure
// overhead.
//
// ok is false for an unmapped address: a gap under frame-gap addressing, or an
// offset past the end of the stack. The returned offset is already gap-remapped
// so it indexes mem directly.
func (s *Stack) frameOffset(addr uint32) (uint64, bool) {
	off := uint64(addr & math.MaxUint32)

	if !s.dynamicStackFrames && s.stackFrameGaps {
		if (addr/StackFrameSize)%2 == 1 {
			return 0, false
		}
		off = ((off & GapMask) >> 1) | (off & ^GapMask)
	}

	if off > StackMax {
		return 0, false
	}
	return off, true
}

// GetFrame returns underlying memory as a slice for a given stack address
func (s *Stack) GetFrame(addr uint32) []byte {
	off := uint64(addr & math.MaxUint32)

	if !s.dynamicStackFrames {
		if s.stackFrameGaps {
			// disallow addressing a gap
			hi := addr / StackFrameSize
			if hi%2 == 1 {
				return nil
			}

			// account for gapping in virtual addr space but not in the underlying memory
			off = ((off & GapMask) >> 1) | (off & ^GapMask)
		}
	}

	if off > StackMax {
		return nil
	} else {
		return s.mem[off:]
	}
}

// Push allocates a new call frame.
//
// Saves the given nonvolatile regs, return address,
// and current frame pointer.
// Returns the new frame pointer.
func (s *Stack) Push(regs []uint64, ret int64) bool {
	if ok := len(s.shadow) < cap(s.shadow); !ok {
		return false
	}

	frame := Frame{RetAddr: ret}
	copy(frame.NVRegs[:], regs[6:10])
	frame.FramePtr = regs[10]

	s.shadow = append(s.shadow, frame)

	if !s.dynamicStackFrames {
		if s.stackFrameGaps {
			regs[10] += StackFrameSize * 2
		} else {
			regs[10] += StackFrameSize
		}
	}

	return true
}

// Pop exits the last call frame.
//
// Restores saved nonvolatile regs into provided slice.
// Returns saved return address and returns true upon success,
// and returns false if no call frames are left.
func (s *Stack) Pop(regs []uint64) (int64, bool) {
	if len(s.shadow) <= 1 {
		return 0, false
	}

	var frame Frame
	frame, s.shadow = s.shadow[len(s.shadow)-1], s.shadow[:len(s.shadow)-1]

	copy(regs[6:10], frame.NVRegs[:])
	regs[10] = frame.FramePtr

	return frame.RetAddr, true
}
