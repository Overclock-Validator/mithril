package statsd

import (
	"testing"
	"time"

	mithrilmetrics "github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func TestInitializeStatsdMetrics(t *testing.T) {
	//metricsCollection := InitializeStatsdMetrics()
	// Check that PreprocessBlock is in histograms
	if _, ok := metricsCollection.histograms[PreprocessBlock]; !ok {
		t.Errorf("Expected PreprocessBlock to be in histograms")
	}
	// Check that Epoch is in gauges
	if _, ok := metricsCollection.gauges[Epoch]; !ok {
		t.Errorf("Expected Epoch to be in gauges")
	}

	// Check that SnapshotTarBytesRead is in counters
	if _, ok := metricsCollection.counters[SnapshotTarBytesRead]; !ok {
		t.Errorf("Expected SnapshotTarBytesRead to be in counters")
	}
}

func TestThatEveryMetricHasLabelsAndType(t *testing.T) {
	for metric := range MetricToType {
		if _, ok := MetricToLabels[metric]; !ok {
			t.Errorf("Metric %v is missing labels in MetricToLabels", metric)
		}
	}

	for metric := range MetricToLabels {
		if _, ok := MetricToType[metric]; !ok {
			t.Errorf("Metric %v is missing type in MetricToType", metric)
		}
	}
}

func TestCountWithoutLabelValues(t *testing.T) {

	val := float64(5)
	Count(SnapshotTarBytesRead, int64(val), nil)
	counterVec := metricsCollection.counters[SnapshotTarBytesRead]
	metric, _ := counterVec.GetMetricWithLabelValues([]string{}...)
	// check that the counter value is 5
	m := &dto.Metric{}
	metric.Write(m)
	assert.Equal(t, m.GetCounter().GetValue(), val, "Counter value should be 5")
}

func TestCountWithLabelValues(t *testing.T) {
	val := float64(10)
	Count(TestCount, int64(val), []string{"testLabel"})
	counterVec := metricsCollection.counters[TestCount]
	metric, _ := counterVec.GetMetricWithLabelValues("testLabel")
	// check that the counter value is 10
	m := &dto.Metric{}
	metric.Write(m)
	assert.Equal(t, m.GetCounter().GetValue(), val, "Counter value should be 10")
}

func TestGaugeWithLabelValues(t *testing.T) {
	val := float64(15)
	Gauge(SnapshotWorkerPoolUtilization, val, []string{"testLabel"})
	gaugeVec := metricsCollection.gauges[SnapshotWorkerPoolUtilization]
	metric, _ := gaugeVec.GetMetricWithLabelValues([]string{"testLabel"}...)
	// check that the gauge value is 15
	m := &dto.Metric{}
	metric.Write(m)
	assert.Equal(t, m.GetGauge().GetValue(), val, "Gauge value should be 15")
}

func TestGaugeWithoutLabelValues(t *testing.T) {
	val := float64(20)
	Gauge(Epoch, val, nil)
	gaugeVec := metricsCollection.gauges[Epoch]
	metric, _ := gaugeVec.GetMetricWithLabelValues([]string{}...)
	// check that the gauge value is 20
	m := &dto.Metric{}
	metric.Write(m)
	assert.Equal(t, m.GetGauge().GetValue(), val, "Gauge value should be 20")
}

func TestTimingWithLabelValues(t *testing.T) {
	val := uint64(25)
	Timing(PreprocessBlock, val, []string{"testLabel"})
	histogramVec := metricsCollection.histograms[PreprocessBlock]
	metric, _ := histogramVec.GetMetricWithLabelValues([]string{"testLabel"}...)
	// check that the histogram value is 25
	mcollector := metric.(prometheus.Metric)
	m := &dto.Metric{}
	mcollector.Write(m)

	assert.Equal(t, m.GetHistogram().GetSampleCount(), uint64(1), "Histogram sample count should be 1")
	assert.Equal(t, uint64(m.GetHistogram().GetSampleSum()), val, "Histogram sample sum should be 25")
}

func TestTimingWithoutLabelValues(t *testing.T) {
	val := uint64(30)
	Timing(TaskIndexEntryCommitterLatency, val, []string{})
	histogramVec := metricsCollection.histograms[TaskIndexEntryCommitterLatency]
	metric, _ := histogramVec.GetMetricWithLabelValues([]string{}...)
	// check that the histogram value is 30
	mcollector := metric.(prometheus.Metric)
	m := &dto.Metric{}
	mcollector.Write(m)

	assert.Equal(t, m.GetHistogram().GetSampleCount(), uint64(1), "Histogram sample count should be 1")
	assert.Equal(t, uint64(m.GetHistogram().GetSampleSum()), val, "Histogram sample sum should be 30")
}

func TestDurationRecordsSecondsInCustomBuckets(t *testing.T) {
	duration := 125 * time.Millisecond
	histogramVec := metricsCollection.histograms[BlockProductionParentReadyAge]
	metric, err := histogramVec.GetMetricWithLabelValues("initial")
	assert.NoError(t, err)
	mcollector := metric.(prometheus.Metric)
	before := &dto.Metric{}
	assert.NoError(t, mcollector.Write(before))

	assert.NoError(t, Duration(BlockProductionParentReadyAge, duration, []string{"initial"}))
	after := &dto.Metric{}
	assert.NoError(t, mcollector.Write(after))

	assert.Equal(t, before.GetHistogram().GetSampleCount()+1, after.GetHistogram().GetSampleCount())
	assert.InDelta(t, duration.Seconds(), after.GetHistogram().GetSampleSum()-before.GetHistogram().GetSampleSum(), 1e-12)
	criticalBucketCount := func(metric *dto.Metric) (uint64, bool) {
		for _, bucket := range metric.GetHistogram().GetBucket() {
			if bucket.GetUpperBound() == duration.Seconds() {
				return bucket.GetCumulativeCount(), true
			}
		}
		return 0, false
	}
	beforeCount, beforeFound := criticalBucketCount(before)
	afterCount, afterFound := criticalBucketCount(after)
	assert.True(t, beforeFound && afterFound, "missing the 125ms leader cutoff bucket")
	assert.Equal(t, beforeCount+1, afterCount)
}

func TestDurationRejectsNegativeDurationWithoutObservation(t *testing.T) {
	histogram := metricsCollection.histograms[BlockProductionParentReadyAge]
	metric, err := histogram.GetMetricWithLabelValues("initial")
	assert.NoError(t, err)
	before := &dto.Metric{}
	assert.NoError(t, metric.(prometheus.Metric).Write(before))

	err = Duration(BlockProductionParentReadyAge, -time.Millisecond, []string{"initial"})
	assert.ErrorContains(t, err, "cannot observe negative duration")

	after := &dto.Metric{}
	assert.NoError(t, metric.(prometheus.Metric).Write(after))
	assert.Equal(t, before.GetHistogram().GetSampleCount(), after.GetHistogram().GetSampleCount())
	assert.Equal(t, before.GetHistogram().GetSampleSum(), after.GetHistogram().GetSampleSum())
}

func TestDurationRejectsMissingWrongAndBadLabelMetrics(t *testing.T) {
	tests := []struct {
		name   string
		metric Metric
		labels []string
		want   string
	}{
		{
			name:   "missing metric",
			metric: Metric{"missing_duration_metric_seconds"},
			want:   "is not registered",
		},
		{
			name:   "wrong metric type",
			metric: SlotReplays,
			want:   "non-histogram type",
		},
		{
			name:   "legacy unit histogram",
			metric: PreprocessBlock,
			want:   "is not registered for duration observations",
		},
		{
			name:   "bad label count",
			metric: BlockProductionStartAttempt,
			want:   "labels:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Duration(tt.metric, time.Millisecond, tt.labels)
			assert.ErrorContains(t, err, tt.want)
		})
	}
}

func TestTurbinePipelineDurationMetricsUseSecondsAndBoundedSchema(t *testing.T) {
	metrics := []Metric{
		TurbineShredCollection,
		TurbineBlockCompletionQueueDelay,
		TurbineBlockDecode,
		TurbineTransactionParse,
		TurbineTransactionSigverify,
		TurbineEarlyTransactionParse,
		TurbineEarlyTransactionSigverify,
		TurbineEarlyPreparationWait,
		TurbineFullToReady,
		TurbineReplayAdmission,
	}
	duration := 25 * time.Millisecond
	for _, metric := range metrics {
		t.Run(metric.String(), func(t *testing.T) {
			assert.Equal(t, []string{}, MetricToLabels[metric])
			assert.Equal(t, TimingT, MetricToType[metric])
			buckets, ok := MetricToBuckets[metric]
			assert.True(t, ok)
			assert.Equal(t, turbinePipelineDurationBuckets, buckets)

			histogram, err := metricsCollection.histograms[metric].GetMetricWithLabelValues()
			assert.NoError(t, err)
			before := &dto.Metric{}
			assert.NoError(t, histogram.(prometheus.Metric).Write(before))
			assert.NoError(t, Duration(metric, duration, nil))
			after := &dto.Metric{}
			assert.NoError(t, histogram.(prometheus.Metric).Write(after))

			assert.Equal(t, before.GetHistogram().GetSampleCount()+1, after.GetHistogram().GetSampleCount())
			assert.InDelta(t, duration.Seconds(), after.GetHistogram().GetSampleSum()-before.GetHistogram().GetSampleSum(), 1e-12)
		})
	}
}

func TestTurbineEarlyVerifiedTransactionsHasBoundedCountSchema(t *testing.T) {
	assert.Equal(t, CountT, MetricToType[TurbineEarlyVerifiedTransactions])
	assert.Equal(t, []string{}, MetricToLabels[TurbineEarlyVerifiedTransactions])
}

func TestBlockProductionMetricLabelsStayBounded(t *testing.T) {
	assert.Equal(t, []string{"outcome", "reason"}, MetricToLabels[BlockProductionLeaderSlots])
	assert.Equal(t, []string{"outcome", "terminal", "cause"}, MetricToLabels[BlockProductionLeaderSlotTerminals])
	assert.Equal(t, []string{"activation", "status"}, MetricToLabels[BlockProductionParentReady])
	assert.Equal(t, []string{"activation"}, MetricToLabels[BlockProductionParentReadyAge])
	assert.Equal(t, []string{"phase"}, MetricToLabels[BlockProductionStartCutoffLate])
	assert.Equal(t, []string{"outcome"}, MetricToLabels[BlockProductionStartDecisionTickDeliveryLag])
	assert.Equal(t, []string{"outcome"}, MetricToLabels[BlockProductionStartDecisionTickWork])
	assert.Equal(t, []string{"result"}, MetricToLabels[BlockProductionStartAttempt])
}

func TestBlockReplayMetrics(t *testing.T) {

	// Instantiate mithrilmetrics.BlockReplay
	blockReplay := &mithrilmetrics.BlockReplay{
		Slot: 12345,
	}

	// Add some timings
	blockReplay.PreprocessBlock.AddTiming(time.Millisecond * 100)
	blockReplay.LoadBlockAccounts.AddTiming(time.Millisecond * 200)
	blockReplay.TxLoop.AddTiming(time.Millisecond * 300)
	// Sanity test to ensure that the function completes without error
	SendBlockReplayMetrics(*blockReplay)
}
