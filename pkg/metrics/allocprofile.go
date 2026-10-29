package metrics

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"
	"time"
)

// AllocProfiler attributes allocation to code paths without per-goroutine
// counters. The runtime already samples allocations by call stack (about one
// sample per runtime.MemProfileRate bytes) for heap profiles; this diffs that
// cumulative profile between calls and reports the stacks that allocated the
// most in the window. The stack names the task: os.ReadDir under
// CompactOnce under the compactor goroutine is compaction's listing.
//
// The runtime profile may lag by up to two GC cycles.
type AllocProfiler struct {
	prev    map[string]allocRecord
	records []runtime.MemProfileRecord
	at      time.Time
}

type allocRecord struct {
	bytes, objects int64
	stack          []uintptr
}

func NewAllocProfiler() *AllocProfiler {
	p := &AllocProfiler{at: time.Now()}
	p.prev = p.read()
	return p
}

func (p *AllocProfiler) read() map[string]allocRecord {
	n, ok := runtime.MemProfile(p.records, true)
	for !ok {
		p.records = make([]runtime.MemProfileRecord, n+n/4+64)
		n, ok = runtime.MemProfile(p.records, true)
	}
	out := make(map[string]allocRecord, n)
	for i := range p.records[:n] {
		r := &p.records[i]
		stack := append([]uintptr(nil), r.Stack()...) // r is reused by the next read
		key := stackKey(stack)
		prev := out[key]
		out[key] = allocRecord{bytes: prev.bytes + r.AllocBytes, objects: prev.objects + r.AllocObjects, stack: stack}
	}
	return out
}

// AllocSite is one call stack's estimated allocation in a window.
type AllocSite struct {
	BytesPerSec float64
	// Allocator is the first frame outside the runtime; Caller is the first
	// Mithril frame at or above it; Root is the goroutine's entry function.
	Allocator, Caller, Root string
}

func (s AllocSite) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%.1f MiB/s %s", s.BytesPerSec/(1<<20), s.Allocator)
	if s.Caller != "" && s.Caller != s.Allocator {
		fmt.Fprintf(&b, " <- %s", s.Caller)
	}
	if s.Root != "" && s.Root != s.Caller && s.Root != s.Allocator {
		fmt.Fprintf(&b, " <- .. %s", s.Root)
	}
	return b.String()
}

// Sample returns the sampled allocation rate of the window and its top call
// stacks, merged by (allocator, caller, root).
func (p *AllocProfiler) Sample(top int) (totalBytesPerSec float64, sites []AllocSite) {
	cur := p.read()
	now := time.Now()
	seconds := now.Sub(p.at).Seconds()
	if seconds <= 0 {
		seconds = math.SmallestNonzeroFloat64
	}
	merged := map[[3]string]float64{}
	var total float64
	for key, r := range cur {
		old := p.prev[key]
		dBytes, dObjects := r.bytes-old.bytes, r.objects-old.objects
		if dBytes <= 0 || dObjects <= 0 {
			continue
		}
		est := unbiasedBytes(dBytes, dObjects)
		total += est
		allocator, caller, root := describeStack(r.stack)
		merged[[3]string{allocator, caller, root}] += est
	}
	p.prev, p.at = cur, now
	for k, bytes := range merged {
		sites = append(sites, AllocSite{BytesPerSec: bytes / seconds, Allocator: k[0], Caller: k[1], Root: k[2]})
	}
	sort.Slice(sites, func(i, j int) bool { return sites[i].BytesPerSec > sites[j].BytesPerSec })
	if len(sites) > top {
		sites = sites[:top]
	}
	return total / seconds, sites
}

// unbiasedBytes scales sampled bytes to an estimate of all bytes allocated,
// as runtime/pprof does for heap profiles: an allocation of size s is sampled
// with probability 1-exp(-s/rate).
func unbiasedBytes(bytes, objects int64) float64 {
	rate := runtime.MemProfileRate
	if rate <= 1 {
		return float64(bytes)
	}
	avg := float64(bytes) / float64(objects)
	scale := 1 / (1 - math.Exp(-avg/float64(rate)))
	return float64(bytes) * scale
}

const mithrilModule = "github.com/Overclock-Validator/mithril/"

func describeStack(stack []uintptr) (allocator, caller, root string) {
	frames := runtime.CallersFrames(stack)
	for {
		f, more := frames.Next()
		name := shortFunc(f.Function)
		if allocator == "" && !strings.HasPrefix(f.Function, "runtime.") {
			allocator = name
		}
		if caller == "" && strings.HasPrefix(f.Function, mithrilModule) {
			caller = name
		}
		if f.Function != "" && f.Function != "runtime.goexit" {
			root = name
		}
		if !more {
			break
		}
	}
	return allocator, caller, root
}

func shortFunc(name string) string {
	name = strings.TrimPrefix(name, mithrilModule)
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	return name
}

func stackKey(stack []uintptr) string {
	var b strings.Builder
	for _, pc := range stack {
		fmt.Fprintf(&b, "%x,", pc)
	}
	return b.String()
}
