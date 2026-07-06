package jit

import (
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
)

// Run executes the compiled program with interpreter-equivalent
// register setup and compute accounting. Return values mirror
// Interpreter.Run.
func (c *Compiled) Run(meter *cu.ComputeMeter) (ret uint64, cuConsumed uint64, err error) {
	ctx := newExecContext()
	ctx.meter = meter

	// Register setup matches the interpreter for a v0 program.
	ctx.Regs[1] = sbpf.VaddrInput
	ctx.Regs[10] = sbpf.VaddrStack + sbpf.StackFrameSize

	initial := meter.Remaining()
	if meter.Disabled() {
		ctx.CuLeft = math.MaxUint64
	} else {
		ctx.CuLeft = initial
	}
	ctx.Resume = c.mem.addr(c.entryOff)

	enter(ctx)

	switch ctx.ExitReason {
	case exitExited:
		meter.Consume(ctx.CuDue)
		return ctx.Regs[0], initial - meter.Remaining(), nil
	case exitOOCU:
		// Matches the interpreter: the meter zeroes and the raw
		// ErrComputeExceeded is returned with the full consumption.
		err = meter.Consume(ctx.CuDue)
		return 0, initial - meter.Remaining(), err
	case exitDivZero:
		meter.Consume(ctx.CuDue - uint64(c.refundAfter[ctx.ExitPC]))
		return 0, 0, &sbpf.Exception{
			PC:     int64(ctx.ExitPC) + 1, // div/mod cases advance pc before erroring
			Detail: fmt.Errorf("%w:", sbpf.ExcDivideByZero),
		}
	case exitOverrun:
		meter.Consume(ctx.CuDue)
		return 0, 0, &sbpf.Exception{
			PC:     int64(ctx.ExitPC),
			Detail: fmt.Errorf("%w:", sbpf.ExcExecutionOverrun),
		}
	default:
		panic(fmt.Sprintf("jit: unknown exit reason %d", ctx.ExitReason))
	}
}
