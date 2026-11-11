package sbpf

import (
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/gagliardetto/solana-go"
)

// VM is the virtual machine abstraction, implemented by each executor.
type VM interface {
	VMContext() any

	HeapMax() uint64
	HeapSize() uint64
	UpdateHeapSize(size uint64)

	Translate(addr uint64, size uint64, write bool) ([]byte, error)

	DueInstrCount() uint64
	PrevInstrMeter() uint64
	SetPrevInstrMeter(num uint64)
	ComputeMeter() *cu.ComputeMeter

	Read(addr uint64, p []byte) error
	Read8(addr uint64) (uint8, error)
	Read16(addr uint64) (uint16, error)
	Read32(addr uint64) (uint32, error)
	Read64(addr uint64) (uint64, error)

	Write(addr uint64, p []byte) error
	Write8(addr uint64, x uint8) error
	Write16(addr uint64, x uint16) error
	Write32(addr uint64, x uint32) error
	Write64(addr uint64, x uint64) error
}

// VMOpts specifies virtual machine parameters.
type VMOpts struct {
	// Machine parameters
	HeapMax  int
	Syscalls SyscallRegistry
	// SyscallsID and SyscallsGen optionally identify what Syscalls answers:
	// any two VMs given equal non-nil SyscallsID and equal SyscallsGen must be
	// given registries that agree on every hash, and the registry must not
	// change while the VM runs. A Program then remembers, per identity, that
	// none of its internal call targets names a syscall, and legacy internal
	// calls stop consulting the registry. SyscallsID must be comparable.
	SyscallsID    any
	SyscallsGen   uint64
	Tracer        TraceSink
	EnableTracing bool

	// Execution parameters
	Context        any // passed to syscalls
	MaxCU          int
	ComputeMeter   *cu.ComputeMeter
	Input          []byte // mapped at VaddrInput
	InputRegions   []InputRegion
	InputDataVaddr uint64 // VM address of instruction data within Input (SIMD-0321)
	// DisableStackFrameGaps is used by SIMD-0460 virtual address space adjustments.
	DisableStackFrameGaps bool

	// Debug
	ProgramId   solana.PublicKey
	TxSignature solana.Signature
}

type InputRegion struct {
	Offset               uint64
	HostOffset           uint64
	RegionSize           uint64
	AddressSpaceReserved uint64
	Writable             bool
	AccountIndex         int
	Data                 []byte
	OnWrite              func(region *InputRegion, requestedLen uint64) error
}

type Exception struct {
	PC     int64
	Detail error
}

func (e *Exception) Error() string {
	return fmt.Sprintf("exception at %d: %s", e.PC, e.Detail)
}

func (e *Exception) Unwrap() error {
	return e.Detail
}

// Exception codes.
var (
	ExcDivideByZero   = errors.New("divide by zero at BPF instruction")
	ExcDivideOverflow = errors.New("divide overflow")
	ExcOutOfCU        = errors.New("compute unit overrun")
	ExcCallDepth      = errors.New("call depth exceeded")
	ExcInvalidInstr   = errors.New("invalid instruction - feature not enabled")

	ExcUnsupportedInstruction = errors.New("unsupported BPF instruction")
	ExcExecutionOverrun       = errors.New("attempted to execute past the end of the text segment")
)

type ExcBadAccess struct {
	Addr   uint64
	Size   uint64
	Write  bool
	Reason string
}

func NewExcBadAccess(addr uint64, size uint64, write bool, reason string) ExcBadAccess {
	return ExcBadAccess{
		Addr:   addr,
		Size:   size,
		Write:  write,
		Reason: reason,
	}
}

func (e ExcBadAccess) Error() string {
	return fmt.Sprintf("bad memory access at %#x (size=%d write=%v), reason: %s", e.Addr, e.Size, e.Write, e.Reason)
}

type ExcCallDest struct {
	Imm uint32
}

func (e ExcCallDest) Error() string {
	return fmt.Sprintf("unknown symbol or syscall 0x%08x", e.Imm)
}

type ExcSyscallError struct {
	Err error
}

func (e ExcSyscallError) Error() string {
	return fmt.Sprintf("syscall error: %s", e.Err)
}

func (e ExcSyscallError) Unwrap() error {
	return e.Err
}
