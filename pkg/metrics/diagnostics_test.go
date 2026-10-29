package metrics

import (
	"errors"
	"fmt"
	rtmetrics "runtime/metrics"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// freshName unregisters any lock or task left by an earlier run of the same
// test in this process, so its statistics and warning limiter start clean.
func freshName(t *testing.T) string {
	t.Helper()
	name := t.Name()
	registryMu.Lock()
	delete(locks, name)
	delete(tasks, name)
	registryMu.Unlock()
	return name
}

func captureWarnings(t *testing.T) *[]string {
	t.Helper()
	var mu sync.Mutex
	var got []string
	previous := DiagnosticsWarn
	DiagnosticsWarn = func(format string, args ...any) {
		mu.Lock()
		got = append(got, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	t.Cleanup(func() { DiagnosticsWarn = previous })
	return &got
}

// A contended Lock records how long it waited and who held the lock; a long
// hold warns with the holder's name; both reach the summary.
func TestTimedMutexReportsWaitAndHold(t *testing.T) {
	warnings := captureWarnings(t)
	var m TimedMutex
	m.Bind(Lock(freshName(t), 5*time.Millisecond, 20*time.Millisecond))

	m.LockAs("compaction")
	acquired := make(chan struct{})
	go func() {
		m.LockAs("fold")
		m.Unlock()
		close(acquired)
	}()
	time.Sleep(30 * time.Millisecond)
	m.Unlock()
	<-acquired

	require.Len(t, *warnings, 2)
	both := strings.Join(*warnings, "\n") // the waiter and the unlocker race to log first
	assert.Contains(t, both, "held ")
	assert.Contains(t, both, "by compaction")
	assert.Contains(t, both, "waited")
	assert.Contains(t, both, "holder: compaction")
	summary := DiagnosticsSummary()
	assert.Contains(t, summary, "lock "+t.Name()+" contended=1")
	assert.Contains(t, summary, "(behind compaction)")
	assert.Contains(t, summary, "writes=2")
	assert.Contains(t, DiagnosticsSummary(), "lock "+t.Name()+" contended=0", "the summary resets its window")
}

// An unbound mutex records nothing and still locks.
func TestTimedMutexZeroValue(t *testing.T) {
	var m TimedMutex
	var rw TimedRWMutex
	m.Lock()
	m.Unlock()
	rw.RLock()
	rw.RUnlock()
	rw.Lock()
	rw.Unlock()
}

// Readers blocked behind a writer record their wait; uncontended readers do not.
func TestTimedRWMutexReaderWait(t *testing.T) {
	warnings := captureWarnings(t)
	var m TimedRWMutex
	stat := Lock(freshName(t), 5*time.Millisecond, time.Hour)
	m.Bind(stat)
	m.RLock()
	m.RUnlock()
	assert.Zero(t, stat.contended.Load(), "an uncontended reader takes the fast path")

	m.LockAs("rewind")
	done := make(chan struct{})
	go func() {
		m.RLock()
		m.RUnlock()
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	m.Unlock()
	<-done
	assert.Equal(t, uint64(1), stat.contended.Load())
	require.Len(t, *warnings, 1)
	assert.Contains(t, (*warnings)[0], "holder: rewind")
}

// Over-budget runs warn with their detail, are rate limited, and failed runs
// are counted in the summary.
func TestBackgroundTaskBudgetAndFailures(t *testing.T) {
	warnings := captureWarnings(t)
	task := Background(freshName(t), 10*time.Millisecond)
	task.Observe(time.Millisecond, func() string { panic("detail is only built for over-budget runs") })
	task.Observe(50*time.Millisecond, func() string { return "moved 3 files" })
	task.Observe(60*time.Millisecond, func() string { return "suppressed" })
	run := task.Start()
	run.EndErr(errors.New("missing manifest"), nil)

	require.Len(t, *warnings, 1, "one warning per interval")
	assert.Contains(t, (*warnings)[0], "over its 10ms budget: moved 3 files")
	summary := DiagnosticsSummary()
	assert.Contains(t, summary, "task "+t.Name()+" runs=4 FAILED=1")
	assert.Contains(t, summary, "max=60ms")
}

func TestWarnLimiterCountsSuppressed(t *testing.T) {
	var l warnLimiter
	now := time.Unix(1000, 0)
	ok, n := l.allow(now, time.Second)
	assert.True(t, ok)
	assert.Zero(t, n)
	ok, _ = l.allow(now.Add(100*time.Millisecond), time.Second)
	assert.False(t, ok)
	ok, n = l.allow(now.Add(2*time.Second), time.Second)
	assert.True(t, ok)
	assert.Equal(t, uint64(1), n)
	assert.True(t, strings.Contains(suppressedNote(n), "1 similar"))
}

// Quantiles come from the observations added in the window only.
func TestHistogramDelta(t *testing.T) {
	buckets := []float64{0, 0.001, 0.010, 0.100, 1}
	old := []uint64{5, 0, 0, 0}
	cur := &rtmetrics.Float64Histogram{Counts: []uint64{5 + 90, 9, 1, 0}, Buckets: buckets}
	p99, maximum := histogramDelta(old, cur, 0.99)
	assert.Equal(t, 10*time.Millisecond, p99)
	assert.Equal(t, 100*time.Millisecond, maximum)
	p50, _ := histogramDelta(old, cur, 0.5)
	assert.Equal(t, time.Millisecond, p50)
	none, _ := histogramDelta(cur.Counts, cur, 0.99)
	assert.Zero(t, none, "an empty window reports nothing")
}

func TestRuntimeSamplerWindow(t *testing.T) {
	s := NewRuntimeSampler()
	sink := make([][]byte, 0, 1024)
	for i := 0; i < 1024; i++ {
		sink = append(sink, make([]byte, 64<<10))
	}
	time.Sleep(10 * time.Millisecond)
	w := s.Sample()
	_ = sink
	assert.Positive(t, w.Seconds)
	assert.Positive(t, w.AllocBytesPerSec, "64 MiB was allocated in the window")
	assert.GreaterOrEqual(t, w.GCCPUFraction, 0.0)
	assert.LessOrEqual(t, w.GCCPUFraction, 1.0)
	assert.Contains(t, w.String(), "sched_latency p99=")
}

// A level reports its window high-water mark against its limit.
func TestLevelReportsHighWaterAgainstLimit(t *testing.T) {
	registryMu.Lock()
	delete(levels, t.Name())
	registryMu.Unlock()
	l := NewLevel(t.Name())
	l.Observe(300, 1000)
	l.Observe(800, 1000)
	l.Observe(100, 1000)
	assert.Contains(t, DiagnosticsSummary(), "level "+t.Name()+" max=800 limit=1000 (80%)")
	assert.Contains(t, DiagnosticsSummary(), "level "+t.Name()+" max=0", "the window resets")
}
