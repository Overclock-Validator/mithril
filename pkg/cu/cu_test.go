package cu

import (
	"errors"
	"testing"
)

// TestConsumeOneMatchesConsumeOfOne is the guard on ConsumeOne's reason for
// existing. It is only allowed to be a cheaper spelling of Consume(1), because
// the sBPF interpreter charges one unit per instruction through it and a
// disagreement would change the compute units a transaction reports and the
// budget its later instructions run on.
//
// The interesting values are the boundary (0 and 1 remaining, where the
// exhaustion branch flips) and the disabled meter, which must mutate nothing.
func TestConsumeOneMatchesConsumeOfOne(t *testing.T) {
	for _, budget := range []uint64{0, 1, 2, 3, 200_000} {
		for _, disabled := range []bool{false, true} {
			reference := ComputeMeter{computeMeter: budget, startingBalance: budget, disable: disabled}
			candidate := reference

			refErr := reference.Consume(1)
			gotErr := candidate.ConsumeOne()

			if !errors.Is(gotErr, refErr) || !errors.Is(refErr, gotErr) {
				t.Fatalf("budget=%d disabled=%v: error mismatch: Consume(1)=%v ConsumeOne()=%v",
					budget, disabled, refErr, gotErr)
			}
			if reference != candidate {
				t.Fatalf("budget=%d disabled=%v: meter state mismatch: Consume(1)=%+v ConsumeOne()=%+v",
					budget, disabled, reference, candidate)
			}
		}
	}
}

// TestConsumeOneDrainsIdentically walks a meter all the way to exhaustion and
// past it through both spellings, so a divergence that only appears after
// repeated calls -- rather than on the first one -- still fails.
func TestConsumeOneDrainsIdentically(t *testing.T) {
	const budget = 8

	reference := NewComputeMeter(budget)
	candidate := NewComputeMeter(budget)

	for step := 0; step < budget+3; step++ {
		refErr := reference.Consume(1)
		gotErr := candidate.ConsumeOne()

		if !errors.Is(gotErr, refErr) || !errors.Is(refErr, gotErr) {
			t.Fatalf("step %d: error mismatch: Consume(1)=%v ConsumeOne()=%v", step, refErr, gotErr)
		}
		if reference.Used() != candidate.Used() || reference.Remaining() != candidate.Remaining() {
			t.Fatalf("step %d: used %d vs %d, remaining %d vs %d",
				step, reference.Used(), candidate.Used(),
				reference.Remaining(), candidate.Remaining())
		}
	}

	if reference.Remaining() != 0 || reference.Used() != budget {
		t.Fatalf("reference meter did not drain: used=%d remaining=%d", reference.Used(), reference.Remaining())
	}
}

// TestConsumeOneStaysInlinable pins the property the optimisation depends on,
// in the only way a test can: by documenting the check. ConsumeOne must cost 20
// or less in the inliner's model, because Interpreter.Run is a "big" function
// and Go caps callees inlined into a big caller at 20. Consume itself costs 22,
// which is why it was a real call on every interpreted instruction.
//
// Verify with:
//
//	go build -gcflags=-m=2 ./pkg/cu/ 2>&1 | grep ConsumeOne
//
// and confirm the call site still inlines with:
//
//	go build -gcflags=-m ./pkg/sbpf/ 2>&1 | grep ConsumeOne
func TestConsumeOneStaysInlinable(t *testing.T) {
	t.Skip("documentation only: inline cost is a compiler property, checked via -gcflags=-m=2")
}
