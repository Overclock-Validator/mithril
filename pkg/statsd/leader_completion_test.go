package statsd

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestSignedDeadlineMarginPreservesNegativeAndZeroSamples(t *testing.T) {
	observer, err := metricsCollection.histograms[BlockProductionDeadlineMargin].GetMetricWithLabelValues()
	require.NoError(t, err)
	read := func() *dto.Histogram {
		metric := &dto.Metric{}
		require.NoError(t, observer.(prometheus.Metric).Write(metric))
		return metric.GetHistogram()
	}
	before := read()
	for _, duration := range []time.Duration{-12 * time.Millisecond, 0, 8 * time.Millisecond} {
		require.NoError(t, SignedDuration(BlockProductionDeadlineMargin, duration, nil))
	}
	after := read()
	require.Equal(t, before.GetSampleCount()+3, after.GetSampleCount())
	require.InDelta(t, -0.004, after.GetSampleSum()-before.GetSampleSum(), 1e-12)
	for i, bucket := range after.GetBucket() {
		var want uint64
		switch bucket.GetUpperBound() {
		case -0.010:
			want = 1 // the late sample retained its sign
		case 0:
			want = 2 // exact deadline is distinguished from positive headroom
		case 0.010:
			want = 3
		default:
			continue
		}
		require.Equal(t, want, bucket.GetCumulativeCount()-before.GetBucket()[i].GetCumulativeCount())
	}
	// Callers cannot accidentally make an ordinary duration histogram signed.
	require.ErrorContains(t, SignedDuration(BlockProductionCompletion, -time.Millisecond, nil), "not registered for signed")
	require.ErrorContains(t, Duration(BlockProductionDeadlineMargin, -time.Millisecond, nil), "cannot observe negative")
	require.ErrorContains(t, SignedDuration(BlockProductionDeadlineMargin, 0, []string{"unexpected"}), "labels")
	require.Equal(t, after.GetSampleCount(), read().GetSampleCount(), "invalid observations must not alter the histogram")
}
