package sbpf

import (
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
)

// Program is a loaded SBF program.
type Program struct {
	RO          []byte // read-only segment containing text and ELFs
	Text        []Slot
	TextVA      uint64
	Entrypoint  uint64 // PC
	Funcs       map[uint32]int64
	SbpfVersion sbpfver.SbpfVersion
}

// Verify runs the static bytecode verifier.
func (p *Program) Verify() error {
	return NewVerifier(p).VerifyProgram()
}
