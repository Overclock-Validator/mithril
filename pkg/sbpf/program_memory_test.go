package sbpf

import "testing"

func TestProgramMemoryIncludesResolvedCalls(t *testing.T) {
	p := &Program{Text: make([]Slot, 20)}
	before := p.MemoryBytes()
	p.ResolveCallTargets()
	if got := p.MemoryBytes() - before; got != 20*4 {
		t.Fatalf("resolved-call cache bytes = %d, want 80", got)
	}
}

func TestWideCallTargetUsesFunctionMap(t *testing.T) {
	const hash = 7
	p := &Program{Text: []Slot{slot(OpCall, 0, 0, 0, hash)}, Funcs: map[uint32]int64{hash: 1 << 32}}
	p.ResolveCallTargets()
	if p.CallTargets != nil {
		t.Fatal("wide call target must use the function map")
	}
}
