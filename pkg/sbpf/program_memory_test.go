package sbpf

import "testing"

func TestProgramMemoryIncludesResolvedCalls(t *testing.T) {
	p := &Program{Text: make([]Slot, 20)}
	before := p.MemoryBytes()
	p.ResolveCallTargets()
	if got := p.MemoryBytes() - before; got != 20*8 {
		t.Fatalf("resolved-call cache bytes = %d, want 160", got)
	}
}
