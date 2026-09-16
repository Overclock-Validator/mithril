package sbpf

import (
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
)

// Program is a loaded SBF program.
type Program struct {
	RO          []byte // read-only segment containing text and ELFs
	TextBytes   []byte
	Text        []Slot
	TextVA      uint64
	Entrypoint  uint64 // PC
	Funcs       map[uint32]int64
	SbpfVersion sbpfver.SbpfVersion

	// CallTargets[pc] holds the resolved internal function target for a
	// `call imm` slot at pc (non-static-syscall versions), or -1.
	CallTargets []int64
}

// ResolveCallTargets precomputes CallTargets from Funcs so the interpreter
// does not need a map lookup per call instruction.
func (p *Program) ResolveCallTargets() {
	if p.SbpfVersion.EnableStaticSyscalls() {
		p.CallTargets = nil
		return
	}
	targets := make([]int64, len(p.Text))
	for pc, slot := range p.Text {
		targets[pc] = -1
		if slot.Op() == OpCall {
			if t, ok := p.Funcs[slot.Uimm()]; ok {
				targets[pc] = t
			}
		}
	}
	p.CallTargets = targets
}

func (p *Program) MemoryBytes() uint64 {
	if p == nil {
		return 0
	}
	total := uint64(len(p.RO)) + uint64(len(p.Text))*8
	if len(p.RO) == 0 {
		total += uint64(len(p.TextBytes))
	}
	if len(p.Funcs) > 0 {
		total += uint64(len(p.Funcs)) * 32
	}
	return total
}

// Verify runs the static bytecode verifier.
func (p *Program) Verify() error {
	return NewVerifier(p).VerifyProgram()
}
