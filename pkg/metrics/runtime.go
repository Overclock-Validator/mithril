package metrics

import (
	"fmt"
	"math"
	rtmetrics "runtime/metrics"
	"time"
)

// RuntimeSampler reports Go runtime pressure per window from runtime/metrics,
// without stopping the world: how long runnable goroutines waited for a CPU
// (scheduler latency), the share of CPU spent in GC, stop-the-world pauses,
// the allocation rate, and time goroutines spent blocked on sync mutexes.
// Metrics missing from the running Go version are skipped.
type RuntimeSampler struct {
	samples []rtmetrics.Sample
	prev    []sampleCopy
	at      time.Time
}

// sampleCopy keeps a sample's value past the next Read, which may reuse the
// histogram it returned.
type sampleCopy struct {
	f      float64
	u      uint64
	counts []uint64
}

const (
	rtSchedLatency = "/sched/latencies:seconds"
	rtGCPauses     = "/sched/pauses/total/gc:seconds"
	rtGCCPU        = "/cpu/classes/gc/total:cpu-seconds"
	rtTotalCPU     = "/cpu/classes/total:cpu-seconds"
	rtIdleCPU      = "/cpu/classes/idle:cpu-seconds"
	rtAllocBytes   = "/gc/heap/allocs:bytes"
	rtMutexWait    = "/sync/mutex/wait/total:seconds"
)

func NewRuntimeSampler() *RuntimeSampler {
	supported := map[string]bool{}
	for _, d := range rtmetrics.All() {
		supported[d.Name] = true
	}
	s := &RuntimeSampler{}
	for _, name := range []string{rtSchedLatency, rtGCPauses, rtGCCPU, rtTotalCPU, rtIdleCPU, rtAllocBytes, rtMutexWait} {
		if supported[name] {
			s.samples = append(s.samples, rtmetrics.Sample{Name: name})
		}
	}
	rtmetrics.Read(s.samples)
	s.prev = copySamples(s.samples)
	s.at = time.Now()
	return s
}

// RuntimeWindow is one window's runtime pressure.
type RuntimeWindow struct {
	Seconds          float64
	SchedLatencyP99  time.Duration
	SchedLatencyMax  time.Duration // upper bound of the highest non-empty bucket
	GCCPUFraction    float64       // estimated fraction of non-idle Go/runtime CPU
	GCPauseMax       time.Duration
	AllocBytesPerSec float64
	MutexWaitPerSec  float64 // seconds blocked per wall second, summed over goroutines
}

// Sample reads the runtime and returns the window since the previous call.
func (s *RuntimeSampler) Sample() RuntimeWindow {
	rtmetrics.Read(s.samples)
	now := time.Now()
	w := RuntimeWindow{Seconds: now.Sub(s.at).Seconds()}
	if w.Seconds <= 0 {
		w.Seconds = math.SmallestNonzeroFloat64
	}
	var gcCPU, totalCPU, idleCPU float64
	for i, cur := range s.samples {
		old := s.prev[i]
		switch cur.Name {
		case rtSchedLatency:
			w.SchedLatencyP99, w.SchedLatencyMax = histogramDelta(old.counts, cur.Value.Float64Histogram(), 0.99)
		case rtGCPauses:
			_, w.GCPauseMax = histogramDelta(old.counts, cur.Value.Float64Histogram(), 0.99)
		case rtGCCPU:
			gcCPU = cur.Value.Float64() - old.f
		case rtTotalCPU:
			totalCPU = cur.Value.Float64() - old.f
		case rtIdleCPU:
			idleCPU = cur.Value.Float64() - old.f
		case rtAllocBytes:
			w.AllocBytesPerSec = float64(cur.Value.Uint64()-old.u) / w.Seconds
		case rtMutexWait:
			w.MutexWaitPerSec = (cur.Value.Float64() - old.f) / w.Seconds
		}
	}
	w.GCCPUFraction = nonIdleGCCPUFraction(gcCPU, totalCPU, idleCPU)
	s.prev = copySamples(s.samples)
	s.at = now
	return w
}

// The runtime's total is available GOMAXPROCS capacity, including idle time.
// Clamp noisy or empty estimate windows rather than report negative or >100%.
func nonIdleGCCPUFraction(gcCPU, totalCPU, idleCPU float64) float64 {
	active := totalCPU - idleCPU
	if active <= 0 {
		return 0
	}
	return min(1, max(0, gcCPU/active))
}

func (w RuntimeWindow) String() string {
	return fmt.Sprintf("sched_latency p99=%s max<=%s | gc_cpu=%.1f%% gc_pause_max<=%s | alloc=%.0f MiB/s | mutex_wait=%.3f s/s",
		w.SchedLatencyP99.Round(time.Microsecond), w.SchedLatencyMax.Round(time.Microsecond), 100*w.GCCPUFraction,
		w.GCPauseMax.Round(time.Microsecond), w.AllocBytesPerSec/(1<<20), w.MutexWaitPerSec)
}

// histogramDelta returns the q quantile and the maximum (bucket upper bounds)
// of the observations added between two cumulative histograms.
func histogramDelta(old []uint64, cur *rtmetrics.Float64Histogram, q float64) (quantile, maximum time.Duration) {
	if cur == nil || len(cur.Counts) == 0 {
		return 0, 0
	}
	delta := make([]uint64, len(cur.Counts))
	var total uint64
	for i := range cur.Counts {
		delta[i] = cur.Counts[i]
		if i < len(old) {
			delta[i] -= old[i]
		}
		total += delta[i]
	}
	if total == 0 {
		return 0, 0
	}
	upper := func(i int) time.Duration {
		b := cur.Buckets[i+1]
		if math.IsInf(b, 1) {
			b = cur.Buckets[i]
		}
		return time.Duration(b * float64(time.Second))
	}
	target := uint64(math.Ceil(q * float64(total)))
	var seen uint64
	for i, c := range delta {
		seen += c
		if seen >= target && quantile == 0 {
			quantile = upper(i)
		}
		if c > 0 {
			maximum = upper(i)
		}
	}
	return quantile, maximum
}

func copySamples(in []rtmetrics.Sample) []sampleCopy {
	out := make([]sampleCopy, len(in))
	for i, sample := range in {
		switch sample.Value.Kind() {
		case rtmetrics.KindFloat64:
			out[i].f = sample.Value.Float64()
		case rtmetrics.KindUint64:
			out[i].u = sample.Value.Uint64()
		case rtmetrics.KindFloat64Histogram:
			out[i].counts = append([]uint64(nil), sample.Value.Float64Histogram().Counts...)
		}
	}
	return out
}
