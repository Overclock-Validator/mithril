package replay

import (
	"math"
	"runtime"
	"runtime/metrics"
	"testing"
	"time"

	mmetrics "github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/stretchr/testify/require"
)

func tipPathSum(r *mmetrics.TipPath) uint64 {
	return r.FullToReady.SumNanoseconds + r.WaitParentArrival.SumNanoseconds + r.WaitParentReplay.SumNanoseconds +
		r.LoopUntilAdmitted.SumNanoseconds + r.WaitUnclassified.SumNanoseconds + r.AdmittedToReplay.SumNanoseconds +
		r.ReplayToHandoff.SumNanoseconds + r.Unattributed.SumNanoseconds
}

func TestRecordTipPath(t *testing.T) {
	ms := int64(time.Millisecond)
	full := int64(1_700_000_000) * int64(time.Second)
	at := func(offsetMs int64) time.Time { return time.Unix(0, full+offsetMs*ms) }
	base := tipPathInput{
		slot: 11, parentSlot: 10,
		receivedNanos: full - 2*ms,
		fullNanos:     full,
		fullToReady:   3 * time.Millisecond, haveReady: true,
		admittedAt:  at(20),
		replayStart: at(21),
		handedOffAt: at(30),
	}

	t.Run("parent arrived late, then replayed", func(t *testing.T) {
		in := base
		in.parent = streamingFrontierMark{slot: 10, fullNanos: full + 8*ms, replayedAt: at(15)}
		var r mmetrics.TipPath
		recordTipPath(&r, in)
		require.Equal(t, full-2*ms, r.ShredReceivedNanos)
		require.Equal(t, uint64(2*ms), r.ReceiveToFull.SumNanoseconds)
		require.Equal(t, uint64(3*ms), r.FullToReady.SumNanoseconds)
		require.Equal(t, uint64(5*ms), r.WaitParentArrival.SumNanoseconds, "ready at 3, parent full at 8")
		require.Equal(t, uint64(7*ms), r.WaitParentReplay.SumNanoseconds, "parent replayed at 15")
		require.Equal(t, uint64(5*ms), r.LoopUntilAdmitted.SumNanoseconds, "admitted at 20")
		require.Equal(t, uint64(1*ms), r.AdmittedToReplay.SumNanoseconds)
		require.Equal(t, uint64(9*ms), r.ReplayToHandoff.SumNanoseconds)
		require.Zero(t, r.Unattributed.Count)
		require.Equal(t, uint64(30*ms), tipPathSum(&r), "the intervals add up to FullToReplayed")
	})

	t.Run("parent already replayed before this block was full", func(t *testing.T) {
		in := base
		in.parent = streamingFrontierMark{slot: 10, fullNanos: full - 400*ms, replayedAt: at(-100)}
		var r mmetrics.TipPath
		recordTipPath(&r, in)
		require.Zero(t, r.WaitParentArrival.Count)
		require.Zero(t, r.WaitParentReplay.Count)
		require.Equal(t, uint64(17*ms), r.LoopUntilAdmitted.SumNanoseconds)
		require.Equal(t, uint64(30*ms), tipPathSum(&r))
	})

	t.Run("previous block is not the parent", func(t *testing.T) {
		in := base
		in.parent = streamingFrontierMark{slot: 9, fullNanos: full + 8*ms, replayedAt: at(15)}
		var r mmetrics.TipPath
		recordTipPath(&r, in)
		require.Equal(t, uint64(17*ms), r.WaitUnclassified.SumNanoseconds)
		require.Zero(t, r.WaitParentArrival.Count)
		require.Equal(t, uint64(30*ms), tipPathSum(&r))
	})

	t.Run("unknown ready instant leaves the rest unattributed", func(t *testing.T) {
		in := base
		in.haveReady = false
		var r mmetrics.TipPath
		recordTipPath(&r, in)
		require.Equal(t, uint64(30*ms), r.Unattributed.SumNanoseconds)
		require.Equal(t, uint64(30*ms), tipPathSum(&r))
	})

	t.Run("no shreds or no hand-off records nothing", func(t *testing.T) {
		for _, in := range []tipPathInput{{}, {fullNanos: full}, {fullNanos: full, handedOffAt: at(-1)}} {
			var r mmetrics.TipPath
			recordTipPath(&r, in)
			require.Equal(t, mmetrics.TipPath{}, r)
		}
	})

	t.Run("receive time after full is not recorded as a duration", func(t *testing.T) {
		in := base
		in.receivedNanos = full + ms
		var r mmetrics.TipPath
		recordTipPath(&r, in)
		require.Zero(t, r.ReceiveToFull.Count)
	})
}

func TestRuntimeInterferenceSampler(t *testing.T) {
	s := newRuntimeInterferenceSampler()
	var first mmetrics.RuntimeInterference
	s.record(&first)
	require.Equal(t, mmetrics.RuntimeInterference{}, first, "the first call only sets the baseline")

	runtime.GC()
	runtime.GC()
	var second mmetrics.RuntimeInterference
	s.record(&second)
	require.GreaterOrEqual(t, second.GCCycles, uint64(2))
	require.Equal(t, uint64(1), second.GCPauseMax.Count, "forced GCs pause the world")
}

func TestNewSamplesBound(t *testing.T) {
	buckets := []float64{0, 1e-4, 1e-3, 1e-2, math.Inf(1)}
	prev := histogramCounts{counts: []uint64{5, 5, 0, 0}, buckets: buckets}
	cur := histogramCounts{counts: []uint64{9, 5, 2, 0}, buckets: buckets}
	bound, over, ok := newSamplesBound(prev, cur, 1e-3)
	require.True(t, ok)
	require.Equal(t, 1e-2, bound, "highest bucket with new samples is [1ms, 10ms)")
	require.Equal(t, uint64(2), over)

	cur.counts[3] = 1
	bound, over, _ = newSamplesBound(prev, cur, 1e-3)
	require.Equal(t, 1e-2, bound, "the unbounded top bucket reports its lower boundary")
	require.Equal(t, uint64(3), over)

	_, _, ok = newSamplesBound(cur, cur, 0)
	require.False(t, ok)
}

// The metric names exist in this Go version: a renamed metric would read as
// KindBad and silently record zeros.
func TestRuntimeMetricNamesExist(t *testing.T) {
	known := map[string]bool{}
	for _, d := range metrics.All() {
		known[d.Name] = true
	}
	for _, name := range []string{rtGCCycles, rtGCPauseCPU, rtGCAssistCPU, rtGCPauses, rtSchedLatency} {
		require.True(t, known[name], name)
	}
}
