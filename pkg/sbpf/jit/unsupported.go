//go:build !(linux && amd64)

package jit

import "errors"

// Supported reports whether this platform has a JIT backend.
const Supported = false

type execMem struct{ code []byte }

func newExecMem([]byte) (*execMem, error) {
	return nil, errors.New("jit: unsupported platform")
}

func (m *execMem) addr(int32) uint64 { return 0 }
func (m *execMem) free()             {}

func enter(*ExecContext) { panic("jit: unsupported platform") }
