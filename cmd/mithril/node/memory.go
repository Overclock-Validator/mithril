package node

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/replay"
)

var nodeMemoryLimit int64
var heapProfileThreshold uint64

// transactionStatusCleanupTask budgets the cleanup after each fold, which
// lists the accounts directory.
var transactionStatusCleanupTask = metrics.Background("node.transaction_status_cleanup", 250*time.Millisecond)

// Runtime pressure worth a warning on its own: runnable goroutines waiting this
// long for a CPU, or GC taking this share of CPU, over a summary window.
const (
	diagnosticsSummaryEvery = time.Minute
	allocationSummaryEvery  = 5 * time.Minute
	allocationSummarySites  = 8
	schedLatencyWarnP99     = 10 * time.Millisecond
	gcCPUFractionWarn       = 0.25
)

func memoryMiB(key string, fallback uint64, allowZero bool) (uint64, error) {
	if !config.IsSet(key) {
		return fallback, nil
	}
	value := config.GetInt64(key)
	if value < 0 || (!allowZero && value == 0) || value > (1<<63-1)>>20 {
		return 0, fmt.Errorf("%s must be a valid %sMiB count", key, map[bool]string{true: "nonnegative ", false: "positive "}[allowZero])
	}
	return uint64(value) << 20, nil
}

func configureMemoryBudgets() error {
	fold, err := memoryMiB("storage.fold_max_mb", replay.DefaultFoldMaxBytes, false)
	if err != nil {
		return err
	}
	retained, err := memoryMiB("storage.working_set_max_mb", replay.DefaultWorkingSetMaxBytes, false)
	if err != nil {
		return err
	}
	if fold > retained {
		return fmt.Errorf("storage.fold_max_mb must not exceed storage.working_set_max_mb")
	}
	limit, err := memoryMiB("tuning.memory_limit_mb", 0, true)
	if err != nil {
		return err
	}
	threshold, err := memoryMiB("tuning.pprof.heap_profile_threshold_mb", 0, true)
	if err != nil {
		return err
	}
	replay.FoldMaxBytes, replay.WorkingSetMaxBytes = fold, retained
	nodeMemoryLimit, heapProfileThreshold = int64(limit), threshold
	return nil
}

// Sample independently of replay, so a slow fold or stalled slot cannot hide
// memory growth. Profiles are opt-in, rate-limited, and rotate over three files
// in the run directory. Do not force GC: that would perturb the workload being
// diagnosed. Heap profiles describe allocations as of the latest completed GC.
func startMemoryDiagnostics(parent context.Context, dir string, threshold uint64) func() {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		var lastProfile time.Time
		profileIndex := 0
		sampler := metrics.NewRuntimeSampler()
		allocations := metrics.NewAllocProfiler()
		lastSummary, lastAllocationSummary := time.Now(), time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				used := mem.Sys - mem.HeapReleased
				mlog.Log.FileOnlyf("runtime memory: heap_bytes=%d go_bytes=%d next_gc_bytes=%d gc_total=%d memory_limit_bytes=%d", mem.HeapAlloc, used, mem.NextGC, mem.NumGC, debug.SetMemoryLimit(-1))
				if now.Sub(lastSummary) >= diagnosticsSummaryEvery {
					lastSummary = now
					logDiagnosticsSummary(sampler.Sample())
				}
				if now.Sub(lastAllocationSummary) >= allocationSummaryEvery {
					lastAllocationSummary = now
					logAllocationSummary(allocations)
				}
				if threshold == 0 || used < threshold || dir == "" || now.Sub(lastProfile) < time.Minute {
					continue
				}
				lastProfile = now // rate-limit failed writes too
				path := filepath.Join(dir, fmt.Sprintf("heap-pressure-%d.pprof", profileIndex%3))
				if err := writeHeapProfile(path); err != nil {
					mlog.Log.Warnf("memory diagnostics: %v", err)
					continue
				}
				profileIndex++
				mlog.Log.Infof("memory diagnostics: heap profile saved to %s (heap=%d MiB Go=%d MiB)", path, mem.HeapAlloc>>20, used>>20)
			}
		}
	}()
	return func() { cancel(); <-done }
}

func writeHeapProfile(path string) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	err = pprof.WriteHeapProfile(f)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp, path)
}

// logDiagnosticsSummary writes one line per window: Go runtime pressure, lock
// contention and holds, and background task totals, and warns when runtime
// pressure alone could explain slow replay.
func logDiagnosticsSummary(w metrics.RuntimeWindow) {
	mlog.Log.FileOnlyf("runtime diagnostics: %s | %s", w, metrics.DiagnosticsSummary())
	if w.SchedLatencyP99 >= schedLatencyWarnP99 || w.GCCPUFraction >= gcCPUFractionWarn {
		mlog.Log.Warnf("runtime pressure over the last %.0fs: %s", w.Seconds, w)
	}
}

// logAllocationSummary writes the window's top allocating call stacks, from the
// runtime's built-in allocation sampling, so a task that churns memory can be
// named without per-goroutine counters.
func logAllocationSummary(p *metrics.AllocProfiler) {
	total, sites := p.Sample(allocationSummarySites)
	parts := make([]string, len(sites))
	for i, site := range sites {
		parts[i] = site.String()
	}
	mlog.Log.FileOnlyf("allocation profile: %.0f MiB/s sampled | top: %s", total/(1<<20), strings.Join(parts, " | "))
}
