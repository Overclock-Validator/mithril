package sbpf

import (
	"encoding/binary"
	"math"
	"sync/atomic"
	"unsafe"

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
	CallTargets []int32

	// callCheck remembers the last answer of unshadowedCallTargets.
	callCheck atomic.Pointer[callCheck]
}

// callCheck is the result of checking a program's internal call targets
// against one syscall registry, identified by (id, gen).
type callCheck struct {
	id    any
	gen   uint64
	clean bool
}

// unshadowedCallTargets returns CallTargets if the interpreter may take an
// internal call from it without asking the registry first, and nil otherwise.
//
// A legacy `call imm` names a syscall if the invocation's registry knows the
// immediate, and only otherwise the internal function resolved at load time.
// Under the registry identified by (id, gen) (see VMOpts.SyscallsID) that
// answer is fixed, so it is checked once: if the registry knows none of the
// immediates that have an internal target, every slot with CallTargets[pc] >= 0
// is an internal call. If it knows even one, the program keeps the lookup.
func (p *Program) unshadowedCallTargets(id any, gen uint64, registry SyscallRegistry) []int32 {
	if id == nil || registry == nil || len(p.CallTargets) == 0 || len(p.CallTargets) != len(p.Text) {
		return nil
	}
	c := p.callCheck.Load()
	if c == nil || c.gen != gen || c.id != id {
		c = &callCheck{id: id, gen: gen, clean: true}
		for pc, target := range p.CallTargets {
			if target < 0 {
				continue
			}
			if _, isSyscall := registry(p.Text[pc].Uimm()); isSyscall {
				c.clean = false
				break
			}
		}
		p.callCheck.Store(c)
	}
	if !c.clean {
		return nil
	}
	return p.CallTargets
}

// ResolveCallTargets precomputes CallTargets from Funcs so the interpreter
// does not need a map lookup per call instruction.
func (p *Program) ResolveCallTargets() {
	if p.SbpfVersion.EnableStaticSyscalls() {
		p.CallTargets = nil
		return
	}
	targets := make([]int32, len(p.Text))
	for pc, slot := range p.Text {
		targets[pc] = -1
		if slot.Op() == OpCall {
			if t, ok := p.Funcs[slot.Uimm()]; ok {
				// Loaded ELF targets fit. Keep the map path available for
				// programs constructed with a wider instruction index.
				if t > math.MaxInt32 {
					p.CallTargets = nil
					return
				}
				if t >= 0 {
					targets[pc] = int32(t)
				}
			}
		}
	}
	p.CallTargets = targets
}

// SetLoadedText installs loader-owned instruction bytes after all relocations.
// The resulting program and its byte and instruction views are immutable.
func (p *Program) SetLoadedText(text []byte) {
	if len(text)%SlotSize != 0 {
		panic("instruction text must contain whole slots")
	}
	p.TextBytes = text
	if len(text) == 0 {
		p.Text = nil
		return
	}
	ptr := unsafe.Pointer(unsafe.SliceData(text))
	if binary.NativeEndian.Uint16([]byte{1, 0}) == 1 && uintptr(ptr)%unsafe.Alignof(Slot(0)) == 0 {
		// Slot contains only a uint64. The view lies wholly inside the original
		// allocation, and the slice keeps that allocation live.
		p.Text = unsafe.Slice((*Slot)(ptr), len(text)/SlotSize)
		return
	}
	p.Text = make([]Slot, len(text)/SlotSize)
	for i := range p.Text {
		p.Text[i] = Slot(binary.LittleEndian.Uint64(text[i*SlotSize:]))
	}
}

func (p *Program) MemoryBytes() uint64 {
	if p == nil {
		return 0
	}
	total := uint64(len(p.RO)) + uint64(len(p.CallTargets))*4
	// Legacy text is a subrange of RO; strict ELF text is separately allocated.
	if len(p.TextBytes) > 0 && !containsText(p.RO, p.TextBytes) {
		total += uint64(len(p.TextBytes))
	}
	if len(p.Text) > 0 && (len(p.TextBytes) != len(p.Text)*SlotSize ||
		unsafe.Pointer(unsafe.SliceData(p.Text)) != unsafe.Pointer(unsafe.SliceData(p.TextBytes))) {
		total += uint64(len(p.Text)) * SlotSize
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

// containsText checks containment without computing a pointer past the allocation.
func containsText(ro, text []byte) bool {
	if len(text) > len(ro) {
		return false
	}
	start := uintptr(unsafe.Pointer(unsafe.SliceData(ro)))
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(text)))
	return addr >= start && addr-start <= uintptr(len(ro)-len(text))
}
