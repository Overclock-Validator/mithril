package cu

import (
	"errors"
)

var ErrComputeExceeded = errors.New("Compute exceeded")

type ComputeMeter struct {
	computeMeter    uint64
	startingBalance uint64
	disable         bool
}

func NewComputeMeter(budget uint64) ComputeMeter {
	return ComputeMeter{computeMeter: budget, startingBalance: budget}
}

func NewComputeMeterDefault() ComputeMeter {
	return ComputeMeter{computeMeter: 200000, startingBalance: 200000}
}

func (cm *ComputeMeter) Consume(cost uint64) error {
	if cm.disable {
		return nil
	}

	if cm.computeMeter < cost {
		cm.computeMeter = 0
		return ErrComputeExceeded
	}
	cm.computeMeter -= cost
	return nil
}

// ConsumeOne charges a single unit. It exists to be inlinable, not to be
// different: it is Consume(1) with the two operations that a cost of one makes
// dead removed.
//
// Consume costs 22 in the inliner's model. The sBPF interpreter's Run is a
// "big" function -- 10044, far past the threshold -- and Go only inlines
// callees costing 20 or less into a big caller, so Consume missed by two and
// every interpreted instruction paid a real cross-package call. Worse than the
// call itself, it clobbered registers, so the loop spilled and reloaded pc and
// the instruction counter around it on every iteration.
//
// Equivalence with Consume(1), which the compute-unit parity tests depend on:
//
//   - disabled: both return nil and mutate nothing;
//   - computeMeter == 0: `computeMeter < 1` holds, so Consume stores zero over a
//     value already zero and returns the error. Dropping a store that cannot
//     change the field leaves the same state and the same error;
//   - computeMeter >= 1: `computeMeter -= 1` and `computeMeter--` are the same
//     operation.
//
// Keep this at or under 20. `go build -gcflags=-m=2 ./pkg/cu/` prints the cost.
func (cm *ComputeMeter) ConsumeOne() error {
	if cm.disable {
		return nil
	}
	if cm.computeMeter == 0 {
		return ErrComputeExceeded
	}
	cm.computeMeter--
	return nil
}

func (cm *ComputeMeter) Used() uint64 {
	return cm.startingBalance - cm.computeMeter
}

func (cm *ComputeMeter) Remaining() uint64 {
	return cm.computeMeter
}

func (cm *ComputeMeter) Disable() {
	cm.disable = true
}

func (cm *ComputeMeter) Enable() {
	cm.disable = false
}
