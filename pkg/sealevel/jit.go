package sealevel

import (
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/jit"
)

// EnableJIT turns on tiered native compilation of SBF programs: a
// program interpreting its JITThreshold-th execution is compiled and
// runs native from then on, sharing the interpreter's memory and
// syscalls. Off by default.
var EnableJIT bool

// JITThreshold is the execution count at which a program is compiled.
var JITThreshold uint64 = 2

// JIT execution counters for reporting, only maintained when EnableJIT.
var (
	JITExecs       atomic.Uint64
	JITInterpExecs atomic.Uint64
	JITCompiled    atomic.Uint64
)

// maybeJITProgram returns compiled code for this execution or nil to
// use the interpreter. The threshold-crossing execution kicks off
// compilation in the background so it never blocks transaction
// processing; executions keep interpreting until the code is stored.
func maybeJITProgram(entry *accountsdb.ProgramCacheEntry, program *sbpf.Program, registry sbpf.SyscallRegistry, opts *sbpf.VMOpts) *jit.Compiled {
	if !EnableJIT || !jit.Supported || entry == nil || len(opts.InputRegions) != 0 {
		return nil
	}
	if compiled := entry.Jit.Load(); compiled != nil {
		return compiled
	}
	if entry.JitFailed.Load() || entry.JitExecs.Add(1) != JITThreshold {
		return nil
	}
	stackGaps := !opts.DisableStackFrameGaps
	go func() {
		isSyscall := func(u uint32) bool {
			_, ok := registry(u)
			return ok
		}
		compiled, err := jit.Compile(program, stackGaps, isSyscall)
		if err != nil {
			entry.JitFailed.Store(true)
			return
		}
		JITCompiled.Add(1)
		entry.Jit.Store(compiled)
	}()
	return nil
}
