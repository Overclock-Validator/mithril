package jit

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// execMem holds a finalized machine-code mapping.
type execMem struct {
	code []byte
}

// newExecMem copies code into a fresh anonymous mapping and flips it to
// read+execute (W^X).
func newExecMem(code []byte) (*execMem, error) {
	mem, err := unix.Mmap(-1, 0, len(code),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		return nil, fmt.Errorf("jit: mmap: %w", err)
	}
	copy(mem, code)
	if err := unix.Mprotect(mem, unix.PROT_READ|unix.PROT_EXEC); err != nil {
		unix.Munmap(mem)
		return nil, fmt.Errorf("jit: mprotect: %w", err)
	}
	return &execMem{code: mem}, nil
}

func (m *execMem) addr(off int32) uint64 {
	return uint64(uintptr(unsafeSliceAddr(m.code))) + uint64(off)
}

func (m *execMem) free() {
	if m.code != nil {
		unix.Munmap(m.code)
		m.code = nil
	}
}
