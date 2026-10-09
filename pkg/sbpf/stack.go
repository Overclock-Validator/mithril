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
	// dirtyLo/dirtyHi bound the physical byte range of mem that may have been
	// written during this execution. Finish only has to zero this range before
	// returning the buffer to the pool.
	dirtyLo  uint64
	dirtyHi  uint64
	maxDepth int
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
		mem:     m,
		sp:      VaddrStack,
		shadow:  sh,
		dirtyLo: StackMax,
		dirtyHi: 0,
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
		if s.dirtyHi > s.dirtyLo {
			clear(s.mem[s.dirtyLo:s.dirtyHi])
		}
		stackMemPool.Put(s.mem)
		s.shadow = s.shadow[:StackDepth]
		clear(s.shadow[:max(s.maxDepth, 1)])
		s.shadow = s.shadow[:1]
		stackShadowPool.Put(s.shadow)
	}
}

// GetFramePtr returns the current frame pointer.
func (s *Stack) GetFramePtr() uint64 {
	return s.shadow[len(s.shadow)-1].FramePtr
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

// MarkDirty records that physical stack bytes [off, off+size) may be written.
func (s *Stack) MarkDirty(lo, hi uint64) {
	if hi <= lo {
		return
	}
	s.dirtyLo = min(s.dirtyLo, lo)
	s.dirtyHi = max(s.dirtyHi, hi)
}

// Push allocates a new call frame.
//
// Saves the given nonvolatile regs, return address,
// and current frame pointer.
// Returns the new frame pointer.
func (s *Stack) Push(regs *[16]uint64, ret int64) bool {
	n := len(s.shadow)
	if n >= cap(s.shadow) {
		return false
	}
	// Write the frame in place (no temporary Frame value / copy) to avoid
	// store-forwarding stalls in this very hot path.
	s.shadow = s.shadow[:n+1]
	f := &s.shadow[n]
	f.RetAddr = ret
	f.NVRegs[0] = regs[6]
	f.NVRegs[1] = regs[7]
	f.NVRegs[2] = regs[8]
	f.NVRegs[3] = regs[9]
	f.FramePtr = regs[10]
	if n+1 > s.maxDepth {
		s.maxDepth = n + 1
	}

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
func (s *Stack) Pop(regs *[16]uint64) (int64, bool) {
	n := len(s.shadow)
	if n <= 1 {
		return 0, false
	}
	f := &s.shadow[n-1]
	regs[6] = f.NVRegs[0]
	regs[7] = f.NVRegs[1]
	regs[8] = f.NVRegs[2]
	regs[9] = f.NVRegs[3]
	regs[10] = f.FramePtr
	s.shadow = s.shadow[:n-1]
	return f.RetAddr, true
}
