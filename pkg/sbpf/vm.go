package sbpf

import (
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
	HeapMax       int
	Syscalls      SyscallRegistry
	Tracer        TraceSink
	EnableTracing bool

	// Execution parameters
	Context      any // passed to syscalls
	MaxCU        int
	ComputeMeter *cu.ComputeMeter
	Input        []byte // mapped at VaddrInput

	// Debug
	ProgramId   solana.PublicKey
	TxSignature solana.Signature
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
