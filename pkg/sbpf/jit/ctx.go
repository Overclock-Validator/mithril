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
	exitExited  = 0 // SBF exit with empty call stack; result in Regs[0]
	exitOOCU    = 1 // compute budget exhausted at block ExitPC
	exitDivZero = 2 // division by zero at ExitPC
	exitOverrun = 3 // execution ran off the end of text
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

	// Go-side only (offsets below are never touched by native code).
	nativeStack []byte
	meter       *cu.ComputeMeter
}

// Context field offsets used by the emitter.
const (
	offRegs       = 0x00
	offExitReason = 0xa0
	offExitPC     = 0xa8
	offCuDue      = 0xb0
	offCuLeft     = 0xb8
)

const nativeStackSize = 64 << 10

func newExecContext() *ExecContext {
	ctx := &ExecContext{nativeStack: make([]byte, nativeStackSize)}
	base := uintptr(unsafe.Pointer(&ctx.nativeStack[0]))
	ctx.NativeStackTop = uint64((base + nativeStackSize) &^ 15)
	return ctx
}
