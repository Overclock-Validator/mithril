package statsd

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	mithrilmetrics "github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func TestMetricsHandlerExposesOnlyMetrics(t *testing.T) {
	previousDefault := http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()
	http.DefaultServeMux.HandleFunc("/debug/pprof/", func(http.ResponseWriter, *http.Request) {})
	http.DefaultServeMux.HandleFunc("/setcpuprofilerate", func(http.ResponseWriter, *http.Request) {})
	t.Cleanup(func() { http.DefaultServeMux = previousDefault })

	handler := newMetricsHandler()

	metrics := httptest.NewRecorder()
	handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metrics.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want 200", metrics.Code)
	}
	if got := metrics.Header().Get(mithrilEndpointHeader); got != mithrilMetricsEndpoint {
		t.Fatalf("GET /metrics identity = %q, want %q", got, mithrilMetricsEndpoint)
	}

	for _, path := range []string{"/debug/pprof/", "/setcpuprofilerate"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotFound {
			t.Errorf("GET %s status = %d, want 404", path, response.Code)
		}
	}
}

func TestMetricsServerRejectsOccupiedAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	if err := startMetricsServer(listener.Addr().String()); err == nil {
		t.Fatal("startMetricsServer() error = nil, want occupied-address error")
	}
}

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

func TestBeginSnapshotBootstrap(t *testing.T) {
	before := time.Now().Unix()
	finish := BeginSnapshotBootstrap()
	after := time.Now().Unix()
	t.Cleanup(finish)

	readGauge := func(metric Metric) float64 {
		m := &dto.Metric{}
		if err := metricsCollection.gauges[metric].WithLabelValues().Write(m); err != nil {
			t.Fatal(err)
		}
		return m.GetGauge().GetValue()
	}
	if got := readGauge(SnapshotBootstrapActive); got != 1 {
		t.Fatalf("bootstrap active = %v, want 1", got)
	}
	if got := int64(readGauge(SnapshotBootstrapStartedAt)); got < before || got > after {
		t.Fatalf("bootstrap start = %d, want [%d,%d]", got, before, after)
	}
	finish()
	if got := readGauge(SnapshotBootstrapActive); got != 0 {
		t.Fatalf("bootstrap active after finish = %v, want 0", got)
	}
}

func TestSendTurbineReceiverMetrics(t *testing.T) {
	readGauge := func(metric Metric) float64 {
		m := &dto.Metric{}
		if err := metricsCollection.gauges[metric].WithLabelValues().Write(m); err != nil {
			t.Fatal(err)
		}
		return m.GetGauge().GetValue()
	}
	readCounter := func(metric Metric, labels ...string) float64 {
		m := &dto.Metric{}
		if err := metricsCollection.counters[metric].WithLabelValues(labels...).Write(m); err != nil {
			t.Fatal(err)
		}
		return m.GetCounter().GetValue()
	}

	beforePackets := readCounter(TurbinePacketsReceived)
	beforeData := readCounter(TurbineDataShredsReceived)
	beforeBlocks := readCounter(TurbineBlocksEmitted)
	beforeRejected := map[string]float64{}
	for _, reason := range []string{"parse", "signature", "missing_leader", "assembly"} {
		beforeRejected[reason] = readCounter(TurbineShredsRejected, reason)
	}

	first := TurbineReceiverSnapshot{
		Packets:         10,
		DataShreds:      7,
		ParseErrors:     1,
		SignatureErrors: 2,
		MissingLeaders:  3,
		AssemblyErrors:  4,
		BlocksEmitted:   5,
		LastPacketUnix:  1_700_000_000,
		LastDataSlot:    123,
		LastBlockUnix:   1_700_000_001,
		LastBlockSlot:   122,
		ActiveSlots:     6,
	}
	SendTurbineReceiverMetrics(first, TurbineReceiverSnapshot{}, true)

	if got := readCounter(TurbinePacketsReceived) - beforePackets; got != 10 {
		t.Errorf("packet delta = %v, want 10", got)
	}
	if got := readCounter(TurbineDataShredsReceived) - beforeData; got != 7 {
		t.Errorf("data-shred delta = %v, want 7", got)
	}
	if got := readCounter(TurbineBlocksEmitted) - beforeBlocks; got != 5 {
		t.Errorf("block delta = %v, want 5", got)
	}
	for reason, want := range map[string]float64{
		"parse": 1, "signature": 2, "missing_leader": 3, "assembly": 4,
	} {
		if got := readCounter(TurbineShredsRejected, reason) - beforeRejected[reason]; got != want {
			t.Errorf("%s rejection delta = %v, want %v", reason, got, want)
		}
	}
	for metric, want := range map[Metric]float64{
		TurbineReceiverActive:       1,
		TurbineAssemblerActiveSlots: 6,
		TurbineLastPacketTimestamp:  1_700_000_000,
		TurbineLastDataSlot:         123,
		TurbineLastBlockTimestamp:   1_700_000_001,
		TurbineLastBlockSlot:        122,
	} {
		if got := readGauge(metric); got != want {
			t.Errorf("%s = %v, want %v", metric, got, want)
		}
	}

	second := first
	second.Packets += 4
	second.DataShreds += 3
	second.BlocksEmitted += 2
	second.ParseErrors++
	SendTurbineReceiverMetrics(second, first, true)
	if got := readCounter(TurbinePacketsReceived) - beforePackets; got != 14 {
		t.Errorf("packet total after delta = %v, want 14", got)
	}
	if got := readCounter(TurbineShredsRejected, "parse") - beforeRejected["parse"]; got != 2 {
		t.Errorf("parse total after delta = %v, want 2", got)
	}

	reset := TurbineReceiverSnapshot{Packets: 2, DataShreds: 1, BlocksEmitted: 1}
	SendTurbineReceiverMetrics(reset, second, false)
	if got := readCounter(TurbinePacketsReceived) - beforePackets; got != 16 {
		t.Errorf("packet total after receiver reset = %v, want 16", got)
	}
	if got := readGauge(TurbineReceiverActive); got != 0 {
		t.Errorf("receiver active after stop = %v, want 0", got)
	}
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
