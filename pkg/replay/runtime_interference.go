package replay

import (
	"math"
	"runtime/metrics"
	"time"

	mmetrics "github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

// Runtime metrics the replay loop samples once per row. All are cumulative,
// so each row records the change since the previous row.
const (
	rtGCCycles     = "/gc/cycles/total:gc-cycles"
	rtGCPauseCPU   = "/cpu/classes/gc/pause:cpu-seconds"
	rtGCAssistCPU  = "/cpu/classes/gc/mark/assist:cpu-seconds"
	rtGCPauses     = "/sched/pauses/total/gc:seconds"
	rtSchedLatency = "/sched/latencies:seconds"
)

// runtimeInterferenceSampler reads runtime/metrics into a reused buffer. It
// is owned by the replay loop goroutine.
type runtimeInterferenceSampler struct {
	samples []metrics.Sample
	prev    runtimeReading
	primed  bool
}

type runtimeReading struct {
	gcCycles             uint64
	gcPauseCPU, assist   float64
	pauses, schedLatency histogramCounts
	kernelDrops          uint64
}

// histogramCounts copies a runtime histogram's cumulative bucket counts.
type histogramCounts struct {
	counts  []uint64
	buckets []float64 // len(counts)+1 boundaries, as runtime/metrics gives them
}

func newRuntimeInterferenceSampler() *runtimeInterferenceSampler {
	names := []string{rtGCCycles, rtGCPauseCPU, rtGCAssistCPU, rtGCPauses, rtSchedLatency}
	s := &runtimeInterferenceSampler{samples: make([]metrics.Sample, len(names))}
	for i, name := range names {
		s.samples[i].Name = name
	}
	return s
}

func (s *runtimeInterferenceSampler) read() runtimeReading {
	metrics.Read(s.samples)
	var r runtimeReading
	for _, sample := range s.samples {
		v := sample.Value
		switch sample.Name {
		case rtGCCycles:
			if v.Kind() == metrics.KindUint64 {
				r.gcCycles = v.Uint64()
			}
		case rtGCPauseCPU:
			if v.Kind() == metrics.KindFloat64 {
				r.gcPauseCPU = v.Float64()
			}
		case rtGCAssistCPU:
			if v.Kind() == metrics.KindFloat64 {
				r.assist = v.Float64()
			}
		case rtGCPauses:
			r.pauses = copyHistogram(v)
		case rtSchedLatency:
			r.schedLatency = copyHistogram(v)
		}
	}
	r.kernelDrops = turbine.KernelShredDrops()
	return r
}

func copyHistogram(v metrics.Value) histogramCounts {
	if v.Kind() != metrics.KindFloat64Histogram {
		return histogramCounts{}
	}
	h := v.Float64Histogram()
	// Buckets is shared and immutable across reads; Counts is overwritten by
	// the next read, so copy it.
	return histogramCounts{counts: append([]uint64(nil), h.Counts...), buckets: h.Buckets}
}

// record writes the runtime activity since the previous call into rec. The
// first call only establishes the baseline.
func (s *runtimeInterferenceSampler) record(rec *mmetrics.RuntimeInterference) {
	cur := s.read()
	prev, primed := s.prev, s.primed
	s.prev, s.primed = cur, true
	if !primed {
		return
	}
	rec.GCCycles = cur.gcCycles - prev.gcCycles
	addSeconds(&rec.GCPauseCPU, cur.gcPauseCPU-prev.gcPauseCPU)
	addSeconds(&rec.GCAssistCPU, cur.assist-prev.assist)
	if bound, _, ok := newSamplesBound(prev.pauses, cur.pauses, 0); ok {
		addSeconds(&rec.GCPauseMax, bound)
	}
	if bound, over, ok := newSamplesBound(prev.schedLatency, cur.schedLatency, 1e-3); ok {
		addSeconds(&rec.SchedLatencyMax, bound)
		rec.SchedLatencyOver1ms = over
	}
	rec.KernelShredDrops = cur.kernelDrops - prev.kernelDrops
}

func addSeconds(t *mmetrics.Timing, seconds float64) {
	if seconds > 0 && !math.IsInf(seconds, 0) && !math.IsNaN(seconds) {
		t.AddTiming(time.Duration(seconds * 1e9))
	}
}

// newSamplesBound returns the upper boundary of the highest bucket that gained
// samples between two readings, and how many new samples lie in buckets whose
// lower boundary is at least threshold. An unbounded top bucket reports its
// lower boundary. ok is false when nothing new was observed.
func newSamplesBound(prev, cur histogramCounts, threshold float64) (bound float64, over uint64, ok bool) {
	if len(cur.counts) == 0 || len(cur.buckets) != len(cur.counts)+1 {
		return 0, 0, false
	}
	for i := len(cur.counts) - 1; i >= 0; i-- {
		var before uint64
		if i < len(prev.counts) {
			before = prev.counts[i]
		}
		if cur.counts[i] <= before {
			continue
		}
		gained := cur.counts[i] - before
		if !ok {
			bound = cur.buckets[i+1]
			if math.IsInf(bound, 1) {
				bound = cur.buckets[i]
			}
			ok = true
		}
		if threshold > 0 && cur.buckets[i] >= threshold {
			over += gained
		}
	}
	return bound, over, ok
}
