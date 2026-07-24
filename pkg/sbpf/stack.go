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

	// dirtyEnd is the high-water mark (physical offset into mem, past gap
	// compression) of any write performed during this execution. Finish only
	// needs to zero mem[:dirtyEnd] before returning it to the pool, since the
	// remainder of the backing array is already zero (see the pool invariant
	// documented at stackMemPool). maxDepth is the analogous peak for shadow.
	dirtyEnd uint64
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

	// stackMemPool invariant: every []byte in this pool is fully zero across
	// its entire capacity (StackMax). This holds because pool.New allocates a
	// zeroed slice, every guest-visible stack write flows through
	// Interpreter.translateInternal (which records the write high-water mark in
	// Stack.dirtyEnd), and Finish zeroes exactly mem[:dirtyEnd] before Put.
	// Callers therefore may skip re-zeroing on Get. Any new code path that
	// writes into stack memory WITHOUT going through translateInternal breaks
	// this invariant and must update dirtyEnd itself.
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
	s.maxDepth = 1
	return s
}

func (s *Stack) Finish() {
	if UsePool {
		// Zero only the touched prefix; the rest of mem is already zero (see
		// the stackMemPool invariant), so this restores the full-zero contract.
		if s.dirtyEnd > StackMax {
			s.dirtyEnd = StackMax
		}
		clear(s.mem[:s.dirtyEnd])
		stackMemPool.Put(s.mem)

		if s.maxDepth > StackDepth {
			s.maxDepth = StackDepth
		}
		s.shadow = s.shadow[:s.maxDepth]
		clear(s.shadow)
		s.shadow = s.shadow[:1]
		stackShadowPool.Put(s.shadow)
	}
}

// GetFramePtr returns the current frame pointer.
func (s *Stack) GetFramePtr() uint64 {
	return s.shadow[len(s.shadow)-1].FramePtr
}

// frameOffset maps a stack virtual address (low 32 bits) to a physical offset
// into the backing memory, applying gap compression for gapped-frame versions.
// ok is false if the address lands in a gap or is out of range.
func (s *Stack) frameOffset(addr uint32) (off uint64, ok bool) {
	off = uint64(addr & math.MaxUint32)

	if !s.dynamicStackFrames {
		if s.stackFrameGaps {
			// disallow addressing a gap
			hi := addr / StackFrameSize
			if hi%2 == 1 {
				return 0, false
			}

			// account for gapping in virtual addr space but not in the underlying memory
			off = ((off & GapMask) >> 1) | (off & ^GapMask)
		}
	}

	if off > StackMax {
		return 0, false
	}
	return off, true
}

// GetFrame returns underlying memory as a slice for a given stack address
func (s *Stack) GetFrame(addr uint32) []byte {
	off, ok := s.frameOffset(addr)
	if !ok {
		return nil
	}
	return s.mem[off:]
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
	if len(s.shadow) > s.maxDepth {
		s.maxDepth = len(s.shadow)
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
