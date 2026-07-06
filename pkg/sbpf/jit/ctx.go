// Package jit compiles SBF programs to amd64 machine code.
//
// Compiled code never calls back into Go: syscalls, translation slow
// paths and faults store an exit reason in the ExecContext and return
// to the Go dispatch loop, which handles the event and may re-enter at
// a resume point. Programs containing instructions the compiler does
// not support fall back to the interpreter entirely.
package jit

import (
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/cu"
)

// Exit reasons written by compiled code.
const (
	exitExited    = 0 // SBF exit with empty call stack; result in Regs[0]
	exitOOCU      = 1 // compute budget exhausted at block ExitPC
	exitDivZero   = 2 // division by zero at ExitPC
	exitOverrun   = 3 // execution ran off the end of text
	exitBadAccess = 4 // out-of-bounds or wrong-permission memory access
	exitCallDepth = 5 // SBF call stack exceeded 64 frames
	exitSyscall   = 6 // yield to Go to run a syscall, then resume
)

// ExecContext is the shared state between Go and compiled code. Field
// offsets are hardcoded in the trampoline (enter_amd64.s) and the code
// emitter; TestExecContextLayout pins them.
type ExecContext struct {
	Regs [11]uint64 // 0x00: SBF r0..r10

	HostSP  uint64 // 0x58
	HostBP  uint64 // 0x60
	HostBX  uint64 // 0x68
	HostR12 uint64 // 0x70
	HostR13 uint64 // 0x78
	HostR14 uint64 // 0x80: g register
	HostR15 uint64 // 0x88

	NativeStackTop uint64 // 0x90: initial RSP for compiled code
	Resume         uint64 // 0x98: native address to CALL
	ExitReason     uint64 // 0xa0
	ExitPC         uint64 // 0xa8: SBF pc for faults
	CuDue          uint64 // 0xb0: instructions charged since entry
	CuLeft         uint64 // 0xb8: budget snapshot

	// Memory regions, keyed by the top 32 bits of a virtual address.
	// Base is the host address of the region's byte 0; Len its length.
	// A zero Len means the region is absent (every access faults).
	RoBase    uint64 // 0xc0
	RoLen     uint64 // 0xc8
	StackBase uint64 // 0xd0
	StackLen  uint64 // 0xd8
	HeapBase  uint64 // 0xe0
	HeapLen   uint64 // 0xe8
	InputBase uint64 // 0xf0
	InputLen  uint64 // 0xf8

	// CallDepth mirrors the interpreter's shadow-stack depth: it starts
	// at 1 (the entry frame) and a call is refused once it reaches 64.
	CallDepth uint64 // 0x100

	// SyscallHash and ResumeOff are written at a syscall exit: the hash
	// selects the syscall, ResumeOff is the native code offset to resume
	// at once Go has run it.
	SyscallHash uint64 // 0x108
	ResumeOff   uint64 // 0x110

	// Go-side only (offsets below are never touched by native code).
	nativeStack []byte
	meter       *cu.ComputeMeter
	// keepalive pins region-backing slices for the lifetime of a Run so
	// their base pointers in the context stay valid.
	keepalive [][]byte
}

// Context field offsets used by the emitter.
const (
	offRegs           = 0x00
	offExitReason     = 0xa0
	offExitPC         = 0xa8
	offCuDue          = 0xb0
	offCuLeft         = 0xb8
	offRoBase         = 0xc0
	offRoLen          = 0xc8
	offStackBase      = 0xd0
	offStackLen       = 0xd8
	offHeapBase       = 0xe0
	offHeapLen        = 0xe8
	offInputBase      = 0xf0
	offInputLen       = 0xf8
	offCallDepth      = 0x100
	offSyscallHash    = 0x108
	offResumeOff      = 0x110
	offNativeStackTop = 0x90
)

const nativeStackSize = 64 << 10

func newExecContext() *ExecContext {
	ctx := &ExecContext{nativeStack: make([]byte, nativeStackSize)}
	base := uintptr(unsafe.Pointer(&ctx.nativeStack[0]))
	ctx.NativeStackTop = uint64((base + nativeStackSize) &^ 15)
	return ctx
}
