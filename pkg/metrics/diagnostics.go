package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// Always-on diagnostics for work outside the per-block replay timers: lock
// waits and holds, background task durations, and (runtime.go) Go scheduler,
// GC and allocation pressure. Readers try the lock before recording contention;
// writers also timestamp and record their holds. A
// periodic summary line goes to the log file, and anything over its budget
// logs a rate-limited warning naming what held the time.

// DiagnosticsWarn is where over-budget warnings go (replaced in tests).
var DiagnosticsWarn = func(format string, args ...any) { mlog.Log.Warnf(format, args...) }

// warnLimiter allows one warning per interval and counts the rest.
type warnLimiter struct {
	last       atomic.Int64
	suppressed atomic.Uint64
}

func (l *warnLimiter) allow(now time.Time, every time.Duration) (bool, uint64) {
	last := l.last.Load()
	if now.UnixNano()-last < int64(every) || !l.last.CompareAndSwap(last, now.UnixNano()) {
		l.suppressed.Add(1)
		return false, 0
	}
	return true, l.suppressed.Swap(0)
}

const diagnosticsWarnEvery = 10 * time.Second

// windowMax tracks the largest value since the last summary and who caused it.
type windowMax struct {
	mu    sync.Mutex
	value time.Duration
	label string
}

func (w *windowMax) observe(d time.Duration, label string) {
	w.mu.Lock()
	if d > w.value {
		w.value, w.label = d, label
	}
	w.mu.Unlock()
}

func (w *windowMax) take() (time.Duration, string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	d, l := w.value, w.label
	w.value, w.label = 0, ""
	return d, l
}

// LockStat aggregates one named lock across every instance that uses it.
type LockStat struct {
	name          string
	waitBudget    time.Duration
	holdBudget    time.Duration
	contended     atomic.Uint64
	waitTotal     atomic.Int64
	maxWait       windowMax
	maxHold       windowMax
	waitWarnings  warnLimiter
	holdWarnings  warnLimiter
	writeAcquires atomic.Uint64
}

var (
	registryMu sync.Mutex
	locks      = map[string]*LockStat{}
	tasks      = map[string]*BackgroundTask{}
	levels     = map[string]*Level{}
)

// Level tracks the high-water mark of a quantity that has a limit, such as
// memory held before a cap stops replay, so creeping toward the limit shows in
// every summary before it bites.
type Level struct {
	name  string
	max   atomic.Uint64
	limit atomic.Uint64
}

// NewLevel returns the shared gauge for a named level.
func NewLevel(name string) *Level {
	registryMu.Lock()
	defer registryMu.Unlock()
	if l := levels[name]; l != nil {
		return l
	}
	l := &Level{name: name}
	levels[name] = l
	return l
}

// Observe records the current value and limit (0: no limit).
func (l *Level) Observe(value, limit uint64) {
	l.limit.Store(limit)
	for {
		old := l.max.Load()
		if value <= old || l.max.CompareAndSwap(old, value) {
			return
		}
	}
}

// Lock returns the shared statistics for a named lock. Waits over waitBudget
// and write holds over holdBudget log a warning.
func Lock(name string, waitBudget, holdBudget time.Duration) *LockStat {
	registryMu.Lock()
	defer registryMu.Unlock()
	if s := locks[name]; s != nil {
		return s
	}
	s := &LockStat{name: name, waitBudget: waitBudget, holdBudget: holdBudget}
	locks[name] = s
	return s
}

func (s *LockStat) recordWait(d time.Duration, blocker string) {
	s.contended.Add(1)
	s.waitTotal.Add(int64(d))
	s.maxWait.observe(d, blocker)
	if d < s.waitBudget {
		return
	}
	if ok, suppressed := s.waitWarnings.allow(time.Now(), diagnosticsWarnEvery); ok {
		DiagnosticsWarn("lock %s: waited %s to acquire (holder: %s)%s", s.name, d.Round(time.Millisecond), labelOr(blocker, "reader or unnamed"), suppressedNote(suppressed))
	}
}

func (s *LockStat) recordHold(d time.Duration, holder string) {
	s.writeAcquires.Add(1)
	s.maxHold.observe(d, holder)
	if d < s.holdBudget {
		return
	}
	if ok, suppressed := s.holdWarnings.allow(time.Now(), diagnosticsWarnEvery); ok {
		DiagnosticsWarn("lock %s: held %s by %s%s", s.name, d.Round(time.Millisecond), labelOr(holder, "an unnamed writer"), suppressedNote(suppressed))
	}
}

// TimedMutex is a sync.Mutex that reports contention and hold times to a
// LockStat. The zero value works and records nothing until Bind is called.
// Uncontended Lock takes the TryLock fast path but still timestamps the
// acquisition, so hold times cover every acquire.
type TimedMutex struct {
	mu       sync.Mutex
	stat     *LockStat
	holder   atomic.Pointer[string]
	acquired time.Time
}

// Bind attaches statistics; call before the mutex is shared.
func (m *TimedMutex) Bind(s *LockStat) { m.stat = s }

func (m *TimedMutex) Lock() { m.LockAs("") }

// LockAs acquires the lock and names the holder in later reports.
func (m *TimedMutex) LockAs(holder string) {
	if m.stat == nil {
		m.mu.Lock()
		return
	}
	if !m.mu.TryLock() {
		blocker := m.holderName()
		start := time.Now()
		m.mu.Lock()
		m.stat.recordWait(time.Since(start), blocker)
	}
	m.acquired = time.Now()
	m.holder.Store(&holder)
}

func (m *TimedMutex) Unlock() {
	if m.stat == nil {
		m.mu.Unlock()
		return
	}
	held := time.Since(m.acquired)
	holder := m.holderName()
	m.holder.Store(nil)
	m.mu.Unlock()
	m.stat.recordHold(held, holder)
}

func (m *TimedMutex) holderName() string {
	if p := m.holder.Load(); p != nil {
		return *p
	}
	return ""
}

// TimedRWMutex is a sync.RWMutex with the same reporting for writers and
// contended readers. Uncontended RLock costs one TryRLock; reader hold times
// are not tracked.
type TimedRWMutex struct {
	mu       sync.RWMutex
	stat     *LockStat
	holder   atomic.Pointer[string]
	acquired time.Time
}

func (m *TimedRWMutex) Bind(s *LockStat) { m.stat = s }

func (m *TimedRWMutex) Lock() { m.LockAs("") }

func (m *TimedRWMutex) LockAs(holder string) {
	if m.stat == nil {
		m.mu.Lock()
		return
	}
	if !m.mu.TryLock() {
		blocker := m.holderName()
		start := time.Now()
		m.mu.Lock()
		m.stat.recordWait(time.Since(start), blocker)
	}
	m.acquired = time.Now()
	m.holder.Store(&holder)
}

func (m *TimedRWMutex) Unlock() {
	if m.stat == nil {
		m.mu.Unlock()
		return
	}
	held := time.Since(m.acquired)
	holder := m.holderName()
	m.holder.Store(nil)
	m.mu.Unlock()
	m.stat.recordHold(held, holder)
}

func (m *TimedRWMutex) RLock() {
	if m.stat == nil || m.mu.TryRLock() {
		if m.stat == nil {
			m.mu.RLock()
		}
		return
	}
	blocker := m.holderName()
	start := time.Now()
	m.mu.RLock()
	m.stat.recordWait(time.Since(start), blocker)
}

func (m *TimedRWMutex) RUnlock() { m.mu.RUnlock() }

func (m *TimedRWMutex) holderName() string {
	if p := m.holder.Load(); p != nil {
		return *p
	}
	return ""
}

// BackgroundTask accumulates the runs of one kind of background work.
type BackgroundTask struct {
	name     string
	budget   time.Duration
	runs     atomic.Uint64
	failures atomic.Uint64
	total    atomic.Int64
	maxRun   windowMax
	warnings warnLimiter
}

// Background returns the shared accumulator for a named task. Runs longer
// than budget log a warning with the run's detail.
func Background(name string, budget time.Duration) *BackgroundTask {
	registryMu.Lock()
	defer registryMu.Unlock()
	if t := tasks[name]; t != nil {
		return t
	}
	t := &BackgroundTask{name: name, budget: budget}
	tasks[name] = t
	return t
}

// BackgroundRun is one timed run; End it exactly once.
type BackgroundRun struct {
	task  *BackgroundTask
	start time.Time
}

func (t *BackgroundTask) Start() BackgroundRun { return BackgroundRun{task: t, start: time.Now()} }

// End records the run. detail, if non-nil, is called only for an over-budget
// run, to describe what it did.
func (r BackgroundRun) End(detail func() string) time.Duration {
	d := time.Since(r.start)
	r.task.Observe(d, detail)
	return d
}

// EndErr is End for a run that can fail; failed runs are counted separately
// so work that never succeeds (a compaction stuck on a missing manifest) shows
// in every summary.
func (r BackgroundRun) EndErr(err error, detail func() string) time.Duration {
	if err != nil {
		r.task.failures.Add(1)
	}
	return r.End(detail)
}

// Observe records a duration measured elsewhere, with the same budget check.
func (t *BackgroundTask) Observe(d time.Duration, detail func() string) {
	t.runs.Add(1)
	t.total.Add(int64(d))
	t.maxRun.observe(d, "")
	if d >= t.budget {
		if ok, suppressed := t.warnings.allow(time.Now(), diagnosticsWarnEvery); ok {
			extra := ""
			if detail != nil {
				extra = ": " + detail()
			}
			DiagnosticsWarn("%s: %s over its %s budget%s%s", t.name, d.Round(time.Millisecond), t.budget, extra, suppressedNote(suppressed))
		}
	}
}

// DiagnosticsSummary returns one line for locks and background tasks since the
// previous call: contended acquires, total and longest wait, longest write
// hold and its holder, and task run counts, totals and the longest run.
func DiagnosticsSummary() string {
	registryMu.Lock()
	lockList := make([]*LockStat, 0, len(locks))
	for _, s := range locks {
		lockList = append(lockList, s)
	}
	taskList := make([]*BackgroundTask, 0, len(tasks))
	for _, t := range tasks {
		taskList = append(taskList, t)
	}
	registryMu.Unlock()
	registryMu.Lock()
	levelList := make([]*Level, 0, len(levels))
	for _, l := range levels {
		levelList = append(levelList, l)
	}
	registryMu.Unlock()
	sort.Slice(levelList, func(i, j int) bool { return levelList[i].name < levelList[j].name })
	sort.Slice(lockList, func(i, j int) bool { return lockList[i].name < lockList[j].name })
	sort.Slice(taskList, func(i, j int) bool { return taskList[i].name < taskList[j].name })

	var b strings.Builder
	for _, s := range lockList {
		wait, waitFor := s.maxWait.take()
		hold, holder := s.maxHold.take()
		fmt.Fprintf(&b, " | lock %s contended=%d wait_total=%s wait_max=%s", s.name, s.contended.Swap(0), time.Duration(s.waitTotal.Swap(0)).Round(time.Microsecond), wait.Round(time.Microsecond))
		if waitFor != "" {
			fmt.Fprintf(&b, "(behind %s)", waitFor)
		}
		fmt.Fprintf(&b, " writes=%d hold_max=%s", s.writeAcquires.Swap(0), hold.Round(time.Microsecond))
		if holder != "" {
			fmt.Fprintf(&b, "(%s)", holder)
		}
	}
	for _, t := range taskList {
		longest, _ := t.maxRun.take()
		fmt.Fprintf(&b, " | task %s runs=%d", t.name, t.runs.Swap(0))
		if failed := t.failures.Swap(0); failed > 0 {
			fmt.Fprintf(&b, " FAILED=%d", failed)
		}
		fmt.Fprintf(&b, " total=%s max=%s", time.Duration(t.total.Swap(0)).Round(time.Millisecond), longest.Round(time.Millisecond))
	}
	for _, l := range levelList {
		peak, limit := l.max.Swap(0), l.limit.Load()
		fmt.Fprintf(&b, " | level %s max=%d", l.name, peak)
		if limit > 0 {
			fmt.Fprintf(&b, " limit=%d (%.0f%%)", limit, 100*float64(peak)/float64(limit))
		}
	}
	return strings.TrimPrefix(b.String(), " | ")
}

func labelOr(label, fallback string) string {
	if label == "" {
		return fallback
	}
	return label
}

func suppressedNote(n uint64) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf(" (%d similar warnings suppressed)", n)
}
