//go:build sbpfprofile

package sbpf

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

// Where interpreted execution actually spends its time, measured on real
// traffic rather than on the conformance corpus.
//
// The corpus is fuzz-derived: its opcode mix is 31% lddw and 8% mod64, while a
// real mainnet program's code is 23% ldxdw, 18% stxdw and almost no modulo. Any
// conclusion about what is worth optimising -- dispatch, memory translation, or
// the syscall bodies -- drawn from that corpus is drawn from the wrong
// distribution. This exists to replace those guesses with replay measurements.
//
// Output goes to MITHRIL_SBPF_PROFILE_OUT, rewritten in full every
// profileDumpInterval, so a long replay can be inspected while it runs and the
// last write is always complete. Nothing is printed to stdout: node stat output
// is deliberately left untouched.

const sbpfProfileEnabled = true

const profileDumpInterval = 30 * time.Second

var prof struct {
	mu       sync.Mutex
	runs     uint64
	instrs   uint64
	runNanos int64
	opcodes  [256]uint64
	syscalls map[uint32]*syscallStat
	// topRunNanos counts only outermost Run calls, so it excludes children
	// re-entered through CPI; runNanos counts every Run at every depth.
	topRunNanos int64
	nestedRuns  uint64
	started     time.Time
}

type syscallStat struct {
	calls uint64
	nanos int64
}

// runDepth splits Run time into top-level program execution and execution
// nested inside a CPI. CPI syscalls are 19.9% of block execution, but their
// measured time includes the whole child invocation, so without this split
// there is no way to tell CPI plumbing -- argument translation, account
// serialisation, permission checks -- from the child program simply running.
//
// Deliberately a plain counter, not goroutine-local: it is only meaningful on a
// single-threaded replay (txpar=1), which is the configuration this split is
// measured in, because that is also the only configuration where run time and
// block wall-clock are directly comparable. Under concurrency it is noise, and
// the report says so.
var runDepth int

func profileNow() int64 { return time.Now().UnixNano() }

// profileInstruction runs once per dispatched instruction. The lock would be
// ruinous here, so it is deliberately absent: replay executes transactions on
// several goroutines, and a lost increment under contention costs a rounding
// error on a distribution, which is not worth serialising the innermost loop
// of the interpreter to prevent. Treat the histogram as a sample, not a count.
func profileInstruction(op uint8) {
	prof.opcodes[op]++
	prof.instrs++
}

func profileRunEnter() int {
	prof.mu.Lock()
	runDepth++
	d := runDepth
	prof.mu.Unlock()
	return d
}

func profileRun(startNanos int64) { profileRunAt(1, startNanos) }

func profileRunAt(depth int, startNanos int64) {
	elapsed := time.Now().UnixNano() - startNanos
	prof.mu.Lock()
	runDepth--
	prof.runs++
	prof.runNanos += elapsed
	if depth == 1 {
		prof.topRunNanos += elapsed
	} else {
		prof.nestedRuns++
	}
	prof.mu.Unlock()
}

// profileSyscall is locked because it runs once per syscall rather than once
// per instruction, so contention is orders of magnitude rarer, and the map
// needs it regardless.
func profileSyscall(num uint32, startNanos int64) {
	elapsed := time.Now().UnixNano() - startNanos
	prof.mu.Lock()
	if prof.syscalls == nil {
		prof.syscalls = make(map[uint32]*syscallStat)
	}
	s := prof.syscalls[num]
	if s == nil {
		s = new(syscallStat)
		prof.syscalls[num] = s
	}
	s.calls++
	s.nanos += elapsed
	prof.mu.Unlock()
}

func init() {
	path := os.Getenv("MITHRIL_SBPF_PROFILE_OUT")
	if path == "" {
		return
	}
	prof.started = time.Now()
	go func() {
		for {
			time.Sleep(profileDumpInterval)
			if err := writeProfile(path); err != nil {
				fmt.Fprintf(os.Stderr, "sbpf profile: %v\n", err)
			}
		}
	}()
}

func writeProfile(path string) error {
	prof.mu.Lock()
	runs, instrs, runNanos := prof.runs, prof.instrs, prof.runNanos
	topRunNanos, nestedRuns := prof.topRunNanos, prof.nestedRuns
	ops := prof.opcodes
	sys := make(map[uint32]syscallStat, len(prof.syscalls))
	for k, v := range prof.syscalls {
		sys[k] = *v
	}
	prof.mu.Unlock()

	if runs == 0 {
		return nil
	}

	var b []byte
	add := func(format string, args ...any) { b = append(b, fmt.Sprintf(format, args...)...) }

	var sysNanos int64
	var sysCalls uint64
	for _, s := range sys {
		sysNanos += s.nanos
		sysCalls += s.calls
	}

	add("# sBPF execution profile, %s elapsed\n", time.Since(prof.started).Round(time.Second))
	add("runs                 %d\n", runs)
	add("instructions         %d\n", instrs)
	add("instructions_per_run %.1f\n", float64(instrs)/float64(runs))
	add("run_ms               %.1f\n", float64(runNanos)/1e6)
	add("syscall_ms           %.1f\n", float64(sysNanos)/1e6)
	add("syscall_calls        %d\n", sysCalls)
	add("top_level_run_ms     %.1f\n", float64(topRunNanos)/1e6)
	add("nested_runs          %d\n", nestedRuns)
	// Only meaningful single-threaded; see runDepth.
	add("nested_run_ms        %.1f\n", float64(runNanos-topRunNanos)/1e6)
	// The headline: how much of the interpreter's own time is spent
	// interpreting, versus inside native syscall bodies it merely calls.
	if runNanos > 0 {
		add("syscall_pct_of_run   %.1f\n", 100*float64(sysNanos)/float64(runNanos))
		add("ns_per_instruction   %.2f\n", float64(runNanos-sysNanos)/float64(max64(instrs, 1)))
	}

	add("\n# syscalls by total time\n")
	type sysRow struct {
		num uint32
		syscallStat
	}
	var rows []sysRow
	for n, s := range sys {
		rows = append(rows, sysRow{n, s})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].nanos > rows[j].nanos })
	add("%-12s %10s %12s %10s %8s\n", "syscall", "calls", "total_ms", "ns/call", "%of_sys")
	for _, r := range rows {
		pct := 0.0
		if sysNanos > 0 {
			pct = 100 * float64(r.nanos) / float64(sysNanos)
		}
		add("0x%08x %10d %12.2f %10.0f %7.1f%%\n",
			r.num, r.calls, float64(r.nanos)/1e6, float64(r.nanos)/float64(max64(r.calls, 1)), pct)
	}

	add("\n# opcodes by frequency\n")
	type opRow struct {
		op uint8
		n  uint64
	}
	var orows []opRow
	for i, n := range ops {
		if n != 0 {
			orows = append(orows, opRow{uint8(i), n})
		}
	}
	sort.Slice(orows, func(i, j int) bool { return orows[i].n > orows[j].n })
	var ip Interpreter
	add("%-6s %-14s %12s %8s\n", "op", "name", "count", "share")
	for _, r := range orows {
		add("0x%02x   %-14s %12d %7.2f%%\n",
			r.op, ip.GetOpcodeName(r.op), r.n, 100*float64(r.n)/float64(max64(instrs, 1)))
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
