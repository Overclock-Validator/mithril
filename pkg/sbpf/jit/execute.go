package jit

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
)

// Memory supplies the host buffers backing a program's virtual address
// regions. Stack must be sbpf.StackMax bytes. A nil region faults on
// access.
type Memory struct {
	Ro    []byte
	Stack []byte
	Heap  []byte
	Input []byte
}

func regionAddr(ctx *ExecContext, b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}
	ctx.keepalive = append(ctx.keepalive, b)
	return uint64(uintptr(unsafe.Pointer(&b[0])))
}

// Run executes the compiled program with interpreter-equivalent
// register setup and compute accounting. registry and vm resolve and
// service syscalls; vm must share mem's backing buffers so syscall
// memory effects are visible to compiled code. Return values mirror
// Interpreter.Run.
func (c *Compiled) Run(meter *cu.ComputeMeter, mem *Memory, registry sbpf.SyscallRegistry, vm sbpf.VM) (ret uint64, cuConsumed uint64, err error) {
	ctx := newExecContext()
	ctx.meter = meter

	ctx.RoBase, ctx.RoLen = regionAddr(ctx, mem.Ro), uint64(len(mem.Ro))
	ctx.StackBase, ctx.StackLen = regionAddr(ctx, mem.Stack), uint64(len(mem.Stack))
	ctx.HeapBase, ctx.HeapLen = regionAddr(ctx, mem.Heap), uint64(len(mem.Heap))
	ctx.InputBase, ctx.InputLen = regionAddr(ctx, mem.Input), uint64(len(mem.Input))

	// Register setup matches the interpreter for a v0 program.
	ctx.Regs[1] = sbpf.VaddrInput
	ctx.Regs[10] = sbpf.VaddrStack + sbpf.StackFrameSize
	ctx.CallDepth = 1 // entry frame

	initial := meter.Remaining()
	refreshBudget := func() {
		if meter.Disabled() {
			ctx.CuLeft = math.MaxUint64
		} else {
			ctx.CuLeft = meter.Remaining()
		}
	}
	refreshBudget()
	ctx.Resume = c.mem.addr(c.entryOff)
	ctx.EnterSP = ctx.NativeStackTop // syscall yields overwrite with their RSP

	for {
		enter(ctx)
		if ctx.ExitReason != exitSyscall {
			break
		}
		// Settle the block's compute charge before the syscall consumes
		// its own, mirroring the interpreter.
		meter.Consume(ctx.CuDue)
		ctx.CuDue = 0
		sc, ok := registry(uint32(ctx.SyscallHash))
		if !ok {
			return 0, 0, &sbpf.Exception{
				PC:     int64(ctx.ExitPC),
				Detail: fmt.Errorf("%w:", sbpf.ExcCallDest{Imm: uint32(ctx.SyscallHash)}),
			}
		}
		r0, serr := sc.Invoke(vm, ctx.Regs[1], ctx.Regs[2], ctx.Regs[3], ctx.Regs[4], ctx.Regs[5])
		if serr != nil {
			return 0, 0, &sbpf.Exception{
				PC:     int64(ctx.ExitPC),
				Detail: fmt.Errorf("%w:", sbpf.ExcSyscallError{Err: serr}),
			}
		}
		ctx.Regs[0] = r0
		refreshBudget()
		ctx.Resume = c.mem.addr(int32(ctx.ResumeOff))
	}

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
	case exitBadAccess:
		meter.Consume(ctx.CuDue - uint64(c.refundAfter[ctx.ExitPC]))
		return 0, 0, &sbpf.Exception{
			PC:     int64(ctx.ExitPC),
			Detail: fmt.Errorf("%w:", sbpf.NewExcBadAccess(0, 0, false, "jit")),
		}
	case exitOverrun:
		meter.Consume(ctx.CuDue)
		return 0, 0, &sbpf.Exception{
			PC:     int64(ctx.ExitPC),
			Detail: fmt.Errorf("%w:", sbpf.ExcExecutionOverrun),
		}
	case exitCallDepth:
		meter.Consume(ctx.CuDue - uint64(c.refundAfter[ctx.ExitPC]))
		return 0, 0, &sbpf.Exception{
			PC:     int64(ctx.ExitPC),
			Detail: fmt.Errorf("%w:", sbpf.ExcCallDepth),
		}
	default:
		panic(fmt.Sprintf("jit: unknown exit reason %d", ctx.ExitReason))
	}
}
