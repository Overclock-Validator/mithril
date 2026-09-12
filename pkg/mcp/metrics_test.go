package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

const sampleMetrics = `# HELP slot Current slot
# TYPE slot gauge
slot 285000042

# HELP epoch Current epoch
# TYPE epoch gauge
epoch 660

# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes 123456789
# TYPE process_virtual_memory_bytes gauge
process_virtual_memory_bytes 987654321

# TYPE snapshot_bootstrap_active gauge
snapshot_bootstrap_active 1
# TYPE snapshot_bootstrap_started_timestamp_seconds gauge
snapshot_bootstrap_started_timestamp_seconds 1784509200

# HELP slot_replay_duration_ms Replay duration
# TYPE slot_replay_duration_ms histogram
slot_replay_duration_ms_bucket{le="1"} 0
slot_replay_duration_ms_bucket{le="2"} 0
slot_replay_duration_ms_bucket{le="5"} 0
slot_replay_duration_ms_bucket{le="10"} 0
slot_replay_duration_ms_bucket{le="20"} 0
slot_replay_duration_ms_bucket{le="50"} 800
slot_replay_duration_ms_bucket{le="100"} 950
slot_replay_duration_ms_bucket{le="200"} 990
slot_replay_duration_ms_bucket{le="400"} 1000
slot_replay_duration_ms_bucket{le="800"} 1000
slot_replay_duration_ms_bucket{le="1600"} 1000
slot_replay_duration_ms_bucket{le="3200"} 1000
slot_replay_duration_ms_bucket{le="6400"} 1000
slot_replay_duration_ms_bucket{le="10000"} 1000
slot_replay_duration_ms_bucket{le="+Inf"} 1000
slot_replay_duration_ms_sum 55200
slot_replay_duration_ms_count 1000

# HELP txs_per_block Transactions per block
# TYPE txs_per_block histogram
txs_per_block_bucket{le="10"} 10
txs_per_block_bucket{le="50"} 90
txs_per_block_bucket{le="+Inf"} 100
txs_per_block_sum 2500
txs_per_block_count 100

# HELP slot_replays Total slots replayed
# TYPE slot_replays counter
slot_replays 42

# HELP snapshot_worker_pool_utilization Pool utilization
# TYPE snapshot_worker_pool_utilization gauge
snapshot_worker_pool_utilization{task="index_entry_committer"} 0.42
snapshot_worker_pool_utilization{task="index_entry_builder"} 0.71
snapshot_worker_pool_utilization{task="append_vec_copying"} 0.30
`

func TestParseCannedMetrics(t *testing.T) {
	sum, err := parseMetrics(sampleMetrics)
	if err != nil {
		t.Fatalf("parseMetrics: %v", err)
	}
	if sum.Slot == nil || *sum.Slot != 285_000_042 {
		t.Errorf("slot = %v, want 285000042", sum.Slot)
	}
	if sum.Epoch == nil || *sum.Epoch != 660 {
		t.Errorf("epoch = %v, want 660", sum.Epoch)
	}
	if sum.ProcessRSSBytes == nil || *sum.ProcessRSSBytes != 123456789 {
		t.Errorf("process RSS = %v, want 123456789", sum.ProcessRSSBytes)
	}
	if sum.ProcessVirtualBytes == nil || *sum.ProcessVirtualBytes != 987654321 {
		t.Errorf("process virtual memory = %v, want 987654321", sum.ProcessVirtualBytes)
	}
	if sum.SnapshotBootstrapActive == nil || !*sum.SnapshotBootstrapActive || sum.SnapshotBootstrapStartedTimestampSeconds == nil || *sum.SnapshotBootstrapStartedTimestampSeconds != 1784509200 {
		t.Errorf("bootstrap metrics = active %v, started %v", sum.SnapshotBootstrapActive, sum.SnapshotBootstrapStartedTimestampSeconds)
	}
	if sum.SlotReplays == nil || *sum.SlotReplays != 42 {
		t.Errorf("slot_replays = %v, want 42", sum.SlotReplays)
	}
	// Max across the three worker-pool tasks (0.42, 0.71, 0.30) = 0.71.
	if sum.SnapshotWorkerPoolUtil == nil || math.Abs(sum.SnapshotWorkerPoolUtil.LastReportedMax-0.71) > 0.001 || !sum.SnapshotWorkerPoolUtil.MayBeStale {
		t.Errorf("worker pool util = %v, want 0.71", sum.SnapshotWorkerPoolUtil)
	}
	if sum.SlotReplayMsP50 == nil || math.Abs(*sum.SlotReplayMsP50-38.75) > 0.01 {
		t.Errorf("p50 = %v, want 38.75", sum.SlotReplayMsP50)
	}
	if sum.SlotReplayMsP99 == nil || math.Abs(*sum.SlotReplayMsP99-200) > 0.01 {
		t.Errorf("p99 = %v, want 200", sum.SlotReplayMsP99)
	}
	if sum.TxsPerBlockMean == nil || math.Abs(*sum.TxsPerBlockMean-25) > 0.001 {
		t.Errorf("txs_per_block mean = %v, want 25", sum.TxsPerBlockMean)
	}
	encoded, err := json.Marshal(sum)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"txs_per_block_mean":25`) {
		t.Fatalf("txs_per_block projection is not truthfully named: %s", encoded)
	}
	if !strings.Contains(string(encoded), `"snapshot_worker_pool_utilization":{"last_reported_max":0.71,"may_be_stale":true}`) {
		t.Fatalf("snapshot worker projection is not honest about freshness: %s", encoded)
	}
}

func TestParseTurbineMetrics(t *testing.T) {
	body := `# TYPE turbine_receiver_active gauge
turbine_receiver_active 1
# TYPE turbine_packets_received_total counter
turbine_packets_received_total 100
# TYPE turbine_data_shreds_received_total counter
turbine_data_shreds_received_total 70
# TYPE turbine_blocks_emitted_total counter
turbine_blocks_emitted_total 12
# TYPE turbine_shreds_rejected_total counter
turbine_shreds_rejected_total{reason="parse"} 2
turbine_shreds_rejected_total{reason="signature"} 3
turbine_shreds_rejected_total{reason="missing_leader"} 4
turbine_shreds_rejected_total{reason="assembly"} 5
turbine_shreds_rejected_total{reason="future_reason"} 9
# TYPE turbine_assembler_active_slots gauge
turbine_assembler_active_slots 6
# TYPE turbine_last_packet_timestamp_seconds gauge
turbine_last_packet_timestamp_seconds 1700000000
# TYPE turbine_last_data_slot gauge
turbine_last_data_slot 123
# TYPE turbine_last_block_timestamp_seconds gauge
turbine_last_block_timestamp_seconds 1700000001
# TYPE turbine_last_block_slot gauge
turbine_last_block_slot 122
`
	sum, err := parseMetrics(body)
	if err != nil {
		t.Fatal(err)
	}
	if sum.TurbineReceiverActive == nil || !*sum.TurbineReceiverActive {
		t.Fatalf("receiver active = %v, want true", sum.TurbineReceiverActive)
	}
	assertUint := func(name string, got *uint64, want uint64) {
		t.Helper()
		if got == nil || *got != want {
			t.Errorf("%s = %v, want %d", name, got, want)
		}
	}
	assertUint("packets", sum.TurbinePacketsReceived, 100)
	assertUint("data shreds", sum.TurbineDataShredsReceived, 70)
	assertUint("blocks", sum.TurbineBlocksEmitted, 12)
	assertUint("active slots", sum.TurbineAssemblerActiveSlots, 6)
	assertUint("last packet time", sum.TurbineLastPacketTimestampSeconds, 1_700_000_000)
	assertUint("last data slot", sum.TurbineLastDataSlot, 123)
	assertUint("last block time", sum.TurbineLastBlockTimestampSeconds, 1_700_000_001)
	assertUint("last block slot", sum.TurbineLastBlockSlot, 122)
	if rejected := sum.TurbineShredsRejected; rejected == nil {
		t.Fatal("typed rejection summary is absent")
	} else {
		assertUint("parse rejections", rejected.Parse, 2)
		assertUint("signature rejections", rejected.Signature, 3)
		assertUint("missing-leader rejections", rejected.MissingLeader, 4)
		assertUint("assembly rejections", rejected.Assembly, 5)
		if rejected.SamplesOmitted != 1 {
			t.Errorf("omitted rejection samples = %d, want 1", rejected.SamplesOmitted)
		}
	}
	for _, name := range []string{
		"turbine_receiver_active",
		"turbine_packets_received_total",
		"turbine_data_shreds_received_total",
		"turbine_blocks_emitted_total",
		"turbine_shreds_rejected_total",
		"turbine_assembler_active_slots",
		"turbine_last_packet_timestamp_seconds",
		"turbine_last_data_slot",
		"turbine_last_block_timestamp_seconds",
		"turbine_last_block_slot",
	} {
		if _, ok := sum.Other[name]; ok {
			t.Errorf("typed metric %q duplicated in other", name)
		}
	}
	lookup := lookupMetric(sum, "turbine_last_block_slot")
	if lookup.Error != "" || lookup.Value != float64(122) {
		t.Fatalf("last-block lookup = %+v", lookup)
	}
	lookup = lookupMetric(sum, "turbine_shreds_rejected_total")
	rejections, ok := lookup.Value.(map[string]any)
	if lookup.Error != "" || !ok ||
		rejections["parse"] != float64(2) ||
		rejections["assembly"] != float64(5) ||
		rejections["samples_omitted"] != float64(1) {
		t.Fatalf("rejection lookup = %+v", lookup)
	}
}

func TestParseTurbineMetricsRejectsInvalidScalars(t *testing.T) {
	for _, test := range []struct {
		name  string
		body  string
		valid func(*MetricsSummary) bool
	}{
		{"invalid active", "turbine_receiver_active 2\n", func(sum *MetricsSummary) bool { return sum.TurbineReceiverActive != nil }},
		{"negative counter", "turbine_packets_received_total -1\n", func(sum *MetricsSummary) bool { return sum.TurbinePacketsReceived != nil }},
		{"negative rejection", "turbine_shreds_rejected_total{reason=\"parse\"} -1\n", func(sum *MetricsSummary) bool {
			return sum.TurbineShredsRejected != nil && sum.TurbineShredsRejected.Parse != nil
		}},
		{"fractional slot", "turbine_last_data_slot 1.5\n", func(sum *MetricsSummary) bool { return sum.TurbineLastDataSlot != nil }},
		{"NaN timestamp", "turbine_last_packet_timestamp_seconds NaN\n", func(sum *MetricsSummary) bool { return sum.TurbineLastPacketTimestampSeconds != nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			sum, err := parseMetrics(test.body)
			if err != nil {
				t.Fatal(err)
			}
			if test.valid(sum) {
				t.Fatal("invalid Turbine scalar was accepted")
			}
		})
	}
}

func TestTurbineRejectionsSurviveGenericMetricBounds(t *testing.T) {
	var body strings.Builder
	for i := 0; i <= maxOtherMetricSamples; i++ {
		fmt.Fprintf(&body, "a_metric_%04d %d\n", i, i)
	}
	body.WriteString("turbine_shreds_rejected_total{reason=\"assembly\"} 7\n")

	sum, err := parseMetrics(body.String())
	if err != nil {
		t.Fatal(err)
	}
	if !sum.OtherTruncated {
		t.Fatal("generic metric summary did not reach its bound")
	}
	if sum.TurbineShredsRejected == nil ||
		sum.TurbineShredsRejected.Assembly == nil ||
		*sum.TurbineShredsRejected.Assembly != 7 {
		t.Fatalf("typed rejection summary = %+v, want assembly=7", sum.TurbineShredsRejected)
	}
}

func scrapePrometheusSummary(t *testing.T) *MetricsSummary {
	t.Helper()
	recorder := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("Prometheus scrape status = %d", recorder.Code)
	}
	sum, err := parseMetrics(recorder.Body.String())
	if err != nil {
		t.Fatal(err)
	}
	return sum
}

func uint64OrZero(v *uint64) uint64 {
	if v == nil {
		return 0
	}
	return *v
}

func parseRejections(sum *MetricsSummary) uint64 {
	if sum.TurbineShredsRejected == nil {
		return 0
	}
	return uint64OrZero(sum.TurbineShredsRejected.Parse)
}

func TestTurbinePublisherReachesMCPMetricsSummary(t *testing.T) {
	before := scrapePrometheusSummary(t)
	current := statsd.TurbineReceiverSnapshot{
		Packets:        21,
		DataShreds:     13,
		BlocksEmitted:  8,
		ParseErrors:    1,
		LastPacketUnix: 1_700_000_000,
		LastDataSlot:   123,
		LastBlockUnix:  1_700_000_001,
		LastBlockSlot:  122,
		ActiveSlots:    5,
	}
	statsd.SendTurbineReceiverMetrics(current, statsd.TurbineReceiverSnapshot{}, true)
	after := scrapePrometheusSummary(t)

	for name, values := range map[string]struct {
		before *uint64
		after  *uint64
		want   uint64
	}{
		"packets":     {before.TurbinePacketsReceived, after.TurbinePacketsReceived, current.Packets},
		"data shreds": {before.TurbineDataShredsReceived, after.TurbineDataShredsReceived, current.DataShreds},
		"blocks":      {before.TurbineBlocksEmitted, after.TurbineBlocksEmitted, current.BlocksEmitted},
	} {
		if got := uint64OrZero(values.after) - uint64OrZero(values.before); got != values.want {
			t.Errorf("%s publisher delta = %d, want %d", name, got, values.want)
		}
	}
	if got := parseRejections(after) - parseRejections(before); got != current.ParseErrors {
		t.Errorf("parse rejection delta = %d, want %d", got, current.ParseErrors)
	}
	if after.TurbineReceiverActive == nil || !*after.TurbineReceiverActive ||
		uint64OrZero(after.TurbineAssemblerActiveSlots) != uint64(current.ActiveSlots) ||
		uint64OrZero(after.TurbineLastPacketTimestampSeconds) != uint64(current.LastPacketUnix) ||
		uint64OrZero(after.TurbineLastDataSlot) != current.LastDataSlot ||
		uint64OrZero(after.TurbineLastBlockTimestampSeconds) != uint64(current.LastBlockUnix) ||
		uint64OrZero(after.TurbineLastBlockSlot) != current.LastBlockSlot {
		t.Fatalf("published Turbine gauges were not preserved: %+v", after)
	}
}

func TestHistogramPercentileFiniteBucketBoundary(t *testing.T) {
	for _, test := range []struct {
		name          string
		finiteCount   uint64
		sampleSum     float64
		wantAvailable bool
	}{
		{"overflow only", 1, 50_000, false},
		{"last finite bucket", 100, 500, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := fmt.Sprintf(`# TYPE slot_replay_duration_ms histogram
slot_replay_duration_ms_bucket{le="10"} %d
slot_replay_duration_ms_bucket{le="+Inf"} 100
slot_replay_duration_ms_sum %g
slot_replay_duration_ms_count 100
`, test.finiteCount, test.sampleSum)
			sum, err := parseMetrics(body)
			if err != nil {
				t.Fatal(err)
			}
			if !test.wantAvailable {
				if sum.SlotReplayMsP50 != nil || sum.SlotReplayMsP99 != nil {
					t.Fatalf("overflow-only quantiles must be unavailable, got p50=%v p99=%v", sum.SlotReplayMsP50, sum.SlotReplayMsP99)
				}
				return
			}
			if sum.SlotReplayMsP99 == nil || *sum.SlotReplayMsP99 <= 0 || *sum.SlotReplayMsP99 > 10 {
				t.Fatalf("finite-bucket p99 = %v, want value in (0,10]", sum.SlotReplayMsP99)
			}
		})
	}
}

func testHistogramFamily(sampleCount uint64, sampleSum float64, bounds []float64, counts []uint64) *dto.MetricFamily {
	histogram := &dto.Histogram{SampleCount: &sampleCount, SampleSum: &sampleSum}
	for i := range bounds {
		upperBound := bounds[i]
		cumulativeCount := counts[i]
		histogram.Bucket = append(histogram.Bucket, &dto.Bucket{
			UpperBound:      &upperBound,
			CumulativeCount: &cumulativeCount,
		})
	}
	return &dto.MetricFamily{Metric: []*dto.Metric{{Histogram: histogram}}}
}

func TestHistogramProjectionsRejectMalformedStructure(t *testing.T) {
	tests := []struct {
		name        string
		sampleCount uint64
		bounds      []float64
		counts      []uint64
	}{
		{"NaN bound", 2, []float64{math.NaN(), math.Inf(1)}, []uint64{1, 2}},
		{"negative infinity bound", 2, []float64{math.Inf(-1), math.Inf(1)}, []uint64{1, 2}},
		{"negative finite bound", 2, []float64{-1, math.Inf(1)}, []uint64{1, 2}},
		{"duplicate bound", 2, []float64{10, 10, math.Inf(1)}, []uint64{1, 1, 2}},
		{"decreasing cumulative count", 3, []float64{10, 20, math.Inf(1)}, []uint64{2, 1, 3}},
		{"bucket exceeds sample count", 2, []float64{10, math.Inf(1)}, []uint64{3, 2}},
		{"terminal count mismatch", 3, []float64{10, math.Inf(1)}, []uint64{1, 2}},
		{"missing infinity bucket", 2, []float64{10, 20}, []uint64{1, 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			family := testHistogramFamily(test.sampleCount, 10, test.bounds, test.counts)
			if value, ok := histogramPercentile(family, 0.5); ok {
				t.Fatalf("malformed histogram percentile = %v, want unavailable", value)
			}
			if value, ok := histogramMean(family); ok {
				t.Fatalf("malformed histogram mean = %v, want unavailable", value)
			}
		})
	}

	missingBound := testHistogramFamily(1, 1, []float64{math.Inf(1)}, []uint64{1})
	missingBound.Metric[0].Histogram.Bucket[0].UpperBound = nil
	if value, ok := histogramPercentile(missingBound, 0.5); ok {
		t.Fatalf("missing-bound percentile = %v, want unavailable", value)
	}

	for _, sampleSum := range []float64{-1, math.NaN(), math.Inf(1)} {
		family := testHistogramFamily(2, sampleSum, []float64{10, math.Inf(1)}, []uint64{1, 2})
		if value, ok := histogramPercentile(family, 0.5); ok {
			t.Errorf("sample sum %v produced percentile %v", sampleSum, value)
		}
		if value, ok := histogramMean(family); ok {
			t.Errorf("sample sum %v produced mean %v", sampleSum, value)
		}
	}
}

func TestHistogramPercentileNeverReturnsNonFinite(t *testing.T) {
	// Exercise interpolation near float64's finite ceiling; every successful
	// projection must remain finite.
	family := testHistogramFamily(2, math.MaxFloat64,
		[]float64{math.MaxFloat64 / 2, math.MaxFloat64, math.Inf(1)},
		[]uint64{1, 2, 2},
	)
	for _, percentile := range []float64{0.25, 0.75, 1} {
		value, ok := histogramPercentile(family, percentile)
		if !ok || !isFinite(value) {
			t.Errorf("percentile %v = %v,%v; want a finite value", percentile, value, ok)
		}
	}
	for _, percentile := range []float64{0, -0.1, 1.1, math.NaN(), math.Inf(1)} {
		if value, ok := histogramPercentile(family, percentile); ok {
			t.Errorf("invalid percentile %v returned %v", percentile, value)
		}
	}
}

func TestParseEmptyBody(t *testing.T) {
	sum, err := parseMetrics("")
	if err != nil {
		t.Fatal(err)
	}
	if sum.Slot != nil || sum.Epoch != nil {
		t.Errorf("empty body should yield nil slot/epoch, got %v/%v", sum.Slot, sum.Epoch)
	}
}

func TestParseUnknownGoesToOther(t *testing.T) {
	sum, err := parseMetrics("# TYPE custom_metric untyped\ncustom_metric 123\n")
	if err != nil {
		t.Fatal(err)
	}
	got, ok := sum.Other["custom_metric"]
	if !ok || len(got) == 0 || got[0].Value != 123 {
		t.Errorf("custom_metric not in other as expected: %+v", sum.Other)
	}
}

func TestParseMalformedReturnsError(t *testing.T) {
	if _, err := parseMetrics("metric{le=\"50\" 100\n"); err == nil {
		t.Error("expected error on unclosed label braces")
	}
}

func TestParseMetricsSanitizesSecretBearingParseError(t *testing.T) {
	secret := "PARSE_ERROR_SECRET_SENTINEL"
	body := `metric{token="` + secret + strings.Repeat("x", 8*1024)
	_, err := parseMetrics(body)
	if err == nil {
		t.Fatal("expected malformed metrics to fail")
	}
	got := err.Error()
	if strings.Contains(got, secret) || strings.Contains(got, strings.Repeat("x", 128)) {
		t.Fatalf("parse error exposed endpoint-controlled content: %q", got)
	}
	if len(got) > 128 || !strings.Contains(got, "line 1") {
		t.Fatalf("parse error is not useful and bounded: %q", got)
	}
}

func TestParseMetricsBoundsHighCardinalityOther(t *testing.T) {
	const extraSamples = 250
	total := maxOtherMetricSamples + extraSamples
	var body strings.Builder
	body.WriteString("# TYPE custom_metric gauge\n")
	for i := 0; i < total; i++ {
		fmt.Fprintf(&body, "custom_metric{id=%q,node=%q} %d\n", fmt.Sprintf("%06d", i), "mithril", i)
	}

	sum, err := parseMetrics(body.String())
	if err != nil {
		t.Fatal(err)
	}
	if !sum.OtherTruncated || sum.OtherSamplesRetained <= 0 || sum.OtherSamplesRetained > maxOtherMetricSamples {
		t.Fatalf("unexpected retention metadata: retained=%d omitted=%d truncated=%v", sum.OtherSamplesRetained, sum.OtherSamplesOmitted, sum.OtherTruncated)
	}
	if sum.OtherSamplesRetained+sum.OtherSamplesOmitted != total {
		t.Fatalf("retained + omitted = %d, want %d", sum.OtherSamplesRetained+sum.OtherSamplesOmitted, total)
	}
	encoded, err := json.Marshal(sum.Other)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > maxOtherMetricBytes {
		t.Fatalf("encoded other metrics = %d bytes, exceeds %d-byte bound", len(encoded), maxOtherMetricBytes)
	}

	// Selection at the bound must not depend on randomized Go map iteration.
	again, err := parseMetrics(body.String())
	if err != nil {
		t.Fatal(err)
	}
	encodedAgain, err := json.Marshal(again.Other)
	if err != nil {
		t.Fatal(err)
	}
	if string(encodedAgain) != string(encoded) {
		t.Fatal("bounded sample selection is not deterministic")
	}

	lookup := lookupMetric(sum, "metric_sorted_after_retained_families")
	if !lookup.SummaryTruncated || lookup.OtherSamplesOmitted == 0 || lookup.Error != "metric not found in bounded summary" {
		t.Fatalf("lookup did not disclose inconclusive bounded search: %+v", lookup)
	}
}

func TestParseMetricsBoundsModelVisibleLabels(t *testing.T) {
	var labels strings.Builder
	for i := 0; i < maxMetricLabelsPerSample+5; i++ {
		if i > 0 {
			labels.WriteByte(',')
		}
		fmt.Fprintf(&labels, "label_%02d=%q", i, strings.Repeat("v", maxMetricLabelValueBytes+50))
	}
	body := "# TYPE custom_metric gauge\ncustom_metric{" + labels.String() + "} 1\n"
	sum, err := parseMetrics(body)
	if err != nil {
		t.Fatal(err)
	}
	samples := sum.Other["custom_metric"]
	if len(samples) != 1 {
		t.Fatalf("samples = %+v", samples)
	}
	if !samples[0].LabelsTruncated || len(samples[0].Labels) > maxMetricLabelsPerSample {
		t.Fatalf("labels were not bounded transparently: %+v", samples[0])
	}
	for name, value := range samples[0].Labels {
		if len(name) > maxMetricLabelNameBytes || len(value) > maxMetricLabelValueBytes {
			t.Fatalf("label exceeds visible bound: %q=%q", name, value)
		}
	}
}

func TestParseMetricsOmitsUnsafeNames(t *testing.T) {
	const (
		familySecret = "FAMILYNAME987654"
		labelSecret  = "LABELNAME987654"
		valueSecret  = "LABELVALUE987654"
		tokenSecret  = "TOKENVALUE987654"
		nestedName   = "ABC123OPAQUE"
		nestedValue  = "RANDOM987654"
		standalone   = "STANDALONE987"
		encodedValue = "V6w7X8y9"
		unsafeSuffix = "Q7x8R9y0"
	)
	body := "# TYPE safe_metric gauge\n" +
		`safe_metric{safe="token=` + valueSecret + `",api_key_` + labelSecret + `="visible",foo_token_value="` + tokenSecret + `",status="session_token_` + standalone + `",feature="ReplaceSplTokenWithPToken",encoded="{\"client\\u0053ecret\":\"` + encodedValue + `\"}"} 1` + "\n" +
		"# TYPE token_balance gauge\n" +
		`token_balance{spl_token_balance="visible",note="foo_token_` + nestedName + `=` + nestedValue + `"} 3` + "\n" +
		"# TYPE api_key_" + familySecret + " gauge\n" +
		"api_key_" + familySecret + " 2\n" +
		"# TYPE token_balance_" + unsafeSuffix + " gauge\n" +
		"token_balance_" + unsafeSuffix + " 4\n"
	sum, err := parseMetrics(body)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(sum)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{familySecret, labelSecret, valueSecret, tokenSecret, nestedName, nestedValue, standalone, encodedValue, unsafeSuffix, `client\u0053ecret`} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("metrics summary leaks %q: %s", secret, encoded)
		}
	}
	samples := sum.Other["safe_metric"]
	if len(samples) != 1 || !samples[0].LabelsTruncated || samples[0].Labels["safe"] != "token=[REDACTED]" {
		t.Fatalf("unsafe label metadata was not reported safely: %+v", samples)
	}
	if samples[0].Labels["status"] != "[REDACTED]" || samples[0].Labels["feature"] != "ReplaceSplTokenWithPToken" {
		t.Fatalf("standalone label-value redaction changed domain text or leaked a credential: %+v", samples[0])
	}
	if sum.OtherSamplesOmitted != 2 || !sum.OtherTruncated {
		t.Fatalf("unsafe family omission was not reported: %+v", sum)
	}
	if samples := sum.Other["token_balance"]; len(samples) != 1 || samples[0].Labels["spl_token_balance"] != "visible" || samples[0].Labels["note"] != "[REDACTED]=[REDACTED]" {
		t.Fatalf("token balance metric metadata was incorrectly redacted: %+v", samples)
	}

	raw, _ := boundedRawMetrics(body, DefaultOutputBudgetBytes)
	for _, secret := range []string{familySecret, labelSecret, valueSecret, tokenSecret, nestedName, nestedValue, standalone, encodedValue, unsafeSuffix, `client\\u0053ecret`} {
		if strings.Contains(raw, secret) {
			t.Fatalf("raw metrics leak %q: %s", secret, raw)
		}
	}
	if !strings.Contains(raw, `token_balance{spl_token_balance="visible",note="[REDACTED]=[REDACTED]"} 3`) {
		t.Fatalf("raw metrics lost token balance identifiers: %s", raw)
	}
}

func TestParseMetricsRejectsNonLegacyNames(t *testing.T) {
	if _, err := parseMetrics("{\"token$value\"} 1\n"); err == nil {
		t.Fatal("non-legacy metric name was accepted")
	}
}

func TestBoundedRawMetricsFitsDefaultWireBudget(t *testing.T) {
	secret := "RAW_METRICS_SECRET_SENTINEL"
	body := "# HELP custom_metric token=" + secret + " " + strings.Repeat("<", 2*maxRawMetricsBytes) + "\n# TYPE custom_metric gauge\ncustom_metric 1\n"
	if _, err := parseMetrics(body); err != nil {
		t.Fatalf("raw-output fixture must be valid metrics: %v", err)
	}
	raw, truncated := boundedRawMetrics(body, DefaultOutputBudgetBytes)
	if !truncated || len(raw) > maxRawMetricsBytes {
		t.Fatalf("raw bound failed: bytes=%d truncated=%v", len(raw), truncated)
	}
	if strings.Contains(raw, secret) {
		t.Fatal("raw output exposed a credential")
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	// StructuredContent and the SDK compatibility TextContent each carry a
	// JSON representation. Leave room for the bounded summary and envelope.
	if 2*len(encoded) >= DefaultOutputBudgetBytes {
		t.Fatalf("raw JSON copies consume %d bytes, default wire budget is %d", 2*len(encoded), DefaultOutputBudgetBytes)
	}

	smallBudget := 16 * 1024
	small, smallTruncated := boundedRawMetrics(body, smallBudget)
	if !smallTruncated || len(small) > smallBudget/rawMetricsWireReserve {
		t.Fatalf("raw output did not scale down with configured budget: bytes=%d truncated=%v", len(small), smallTruncated)
	}
}

func TestBoundedRawMetricsPreservesLineStructure(t *testing.T) {
	const body = "# HELP first Authorization: Custom FIRST_SECRET\n" +
		"# TYPE first gauge\n" +
		"first 1\n" +
		"# HELP second token=SECOND_SECRET\n" +
		"# TYPE second gauge\n" +
		"second 2\n" +
		"# HELP third api\\nkey=THIRD_SECRET session\\ntoken\\nABC123=FOURTH_SECRET\n" +
		"# TYPE third gauge\n" +
		"third 3\n"
	if _, err := parseMetrics(body); err != nil {
		t.Fatalf("raw-output fixture must be valid metrics: %v", err)
	}

	raw, truncated := boundedRawMetrics(body, DefaultOutputBudgetBytes)
	if truncated {
		t.Fatal("short raw output was marked truncated")
	}
	if got, want := strings.Count(raw, "\n"), strings.Count(body, "\n"); got != want {
		t.Fatalf("raw line separators = %d, want %d: %q", got, want, raw)
	}
	for _, secret := range []string{"FIRST_SECRET", "SECOND_SECRET", "THIRD_SECRET", "FOURTH_SECRET"} {
		if strings.Contains(raw, secret) {
			t.Fatalf("raw output exposed %s: %q", secret, raw)
		}
	}
	for _, want := range []string{"Authorization: [REDACTED]\n", "token=[REDACTED]\n", `api\nkey=[REDACTED]`, `[REDACTED]=[REDACTED]`, "# TYPE first gauge\nfirst 1\n"} {
		if !strings.Contains(raw, want) {
			t.Fatalf("raw output lost %q: %q", want, raw)
		}
	}
	if twice, _ := boundedRawMetrics(raw, DefaultOutputBudgetBytes); twice != raw {
		t.Fatalf("raw metrics redaction is not idempotent: once %q, twice %q", raw, twice)
	}
}

func TestBoundedRawMetricsReportsSanitizerTruncation(t *testing.T) {
	for _, test := range []struct {
		name, assignments string
	}{
		{"spaced", strings.Repeat("a=0 ", 2048)},
		{"chained", strings.Repeat("a=", 2048)},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := "# HELP dense " + test.assignments + "token=DENSE_TAIL_SECRET\n# TYPE dense gauge\ndense 1\n"
			if _, err := parseMetrics(body); err != nil {
				t.Fatalf("raw-output fixture must be valid metrics: %v", err)
			}
			raw, truncated := boundedRawMetrics(body, DefaultOutputBudgetBytes)
			if !truncated {
				t.Fatal("sanitizer suffix truncation was not reported")
			}
			if strings.Contains(raw, "DENSE_TAIL_SECRET") || !strings.Contains(raw, "[REDACTED]") {
				t.Fatalf("dense raw metrics were not bounded safely: %q", raw)
			}
		})
	}
}

func TestScrapeMetricsNearCapFitsDefaultWireBudget(t *testing.T) {
	secret := "NEAR_CAP_RAW_SECRET_SENTINEL"
	var body strings.Builder
	body.WriteString("# HELP custom_metric token=" + secret + " " + strings.Repeat("<", 2*maxRawMetricsBytes) + "\n")
	body.WriteString("# TYPE custom_metric gauge\n")
	for i := 0; i < maxOtherMetricSamples+250; i++ {
		fmt.Fprintf(&body, "custom_metric{id=%q,node=%q} %d\n", fmt.Sprintf("%06d", i), "mithril", i)
	}
	metricsBody := body.String()
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(mithrilEndpointHeader, mithrilMetricsEndpoint)
		_, _ = w.Write([]byte(metricsBody))
	}))
	defer endpoint.Close()

	session := startInMemorySession(t, Config{
		Profile:           ProfileDiagnostic,
		MetricsURL:        endpoint.URL,
		RPCURL:            endpoint.URL,
		OutputBudgetBytes: DefaultOutputBudgetBytes,
	})
	text, isErr := callToolText(t, session, "mithril_scrape_metrics", map[string]any{"include_raw": true})
	if isErr {
		t.Fatalf("near-cap bounded scrape was rejected by wire budget: %s", text)
	}
	for _, want := range []string{`"raw_truncated":true`, `"other_truncated":true`, `"other_samples_omitted":`} {
		if !strings.Contains(text, want) {
			t.Fatalf("near-cap response missing %s: %s", want, text)
		}
	}
	if strings.Contains(text, secret) {
		t.Fatal("near-cap response exposed raw metric secret")
	}
}

func TestMetricRejectsOversizedNameBeforeFetch(t *testing.T) {
	var requests atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte("slot 1\n"))
	}))
	defer endpoint.Close()

	session := startInMemorySession(t, Config{MetricsURL: endpoint.URL, RPCURL: endpoint.URL})
	text, isErr := callToolText(t, session, "mithril_metric", map[string]any{
		"metric": strings.Repeat("m", maxMetricNameLength+1),
	})
	if !isErr || !strings.Contains(text, "metric name too long") {
		t.Fatalf("oversized metric name: isError=%v text=%q", isErr, text)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("oversized metric name triggered %d endpoint requests", got)
	}
}

func TestMetricRejectsUnsafeNameBeforeFetch(t *testing.T) {
	var requests atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte("slot 1\n"))
	}))
	defer endpoint.Close()

	session := startInMemorySession(t, Config{MetricsURL: endpoint.URL, RPCURL: endpoint.URL})
	text, isErr := callToolText(t, session, "mithril_metric", map[string]any{
		"metric": "api_key_LOOKUP_SECRET",
	})
	if !isErr || !strings.Contains(text, "unsafe display text") {
		t.Fatalf("unsafe metric name: isError=%v text=%q", isErr, text)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("unsafe metric name triggered %d endpoint requests", got)
	}
}

func TestLookupMetric(t *testing.T) {
	sum, _ := parseMetrics(sampleMetrics)
	if out := lookupMetric(sum, "slot"); out.Error != "" || out.Value == nil {
		t.Errorf("lookup slot: %+v", out)
	}
	replay := lookupMetric(sum, "slot_replay_duration_ms")
	replayProjection, ok := replay.Value.(map[string]*float64)
	if replay.Error != "" || !ok || replayProjection["p50"] == nil || replayProjection["p99"] == nil {
		t.Errorf("lookup real replay histogram family: %+v", replay)
	}
	txs := lookupMetric(sum, "txs_per_block")
	txsProjection, ok := txs.Value.(map[string]*float64)
	if txs.Error != "" || !ok || txsProjection["mean"] == nil || *txsProjection["mean"] != 25 {
		t.Errorf("lookup real tx histogram family: %+v", txs)
	}
	if out := lookupMetric(sum, "txs_per_block_mean"); out.Error != "" || out.Value == nil {
		t.Errorf("lookup projected tx mean: %+v", out)
	}
	snapshot := lookupMetric(sum, "snapshot_worker_pool_utilization")
	snapshotValue, ok := snapshot.Value.(map[string]any)
	if snapshot.Error != "" || !ok || snapshotValue["last_reported_max"] != 0.71 || snapshotValue["may_be_stale"] != true {
		t.Errorf("lookup snapshot worker projection: %+v", snapshot)
	}
	if out := lookupMetric(sum, "does_not_exist"); out.Error == "" {
		t.Errorf("lookup missing metric should report error, got %+v", out)
	}
	absent := &MetricsSummary{Other: map[string][]Sample{}}
	if out := lookupMetric(absent, "slot"); out.Error != "metric not found" || out.Value != nil {
		t.Errorf("lookup absent known metric should report error, got %+v", out)
	}
}

func TestMetricToolSupportsPrometheusHistogramFamilyNames(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(mithrilEndpointHeader, mithrilMetricsEndpoint)
		_, _ = w.Write([]byte(sampleMetrics))
	}))
	defer endpoint.Close()

	session := startInMemorySession(t, Config{MetricsURL: endpoint.URL, RPCURL: endpoint.URL})
	tests := []struct {
		name string
		want []string
	}{
		{"slot_replay_duration_ms", []string{`"metric":"slot_replay_duration_ms"`, `"p50":38.75`, `"p99":`}},
		{"txs_per_block", []string{`"metric":"txs_per_block"`, `"mean":25`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			text, isError := callToolText(t, session, "mithril_metric", map[string]any{"metric": test.name})
			if isError {
				t.Fatalf("metric tool returned an error: %s", text)
			}
			for _, want := range test.want {
				if !strings.Contains(text, want) {
					t.Fatalf("metric result missing %s: %s", want, text)
				}
			}
		})
	}
}

func TestMetricToolFindsFamilyOmittedFromBoundedSummary(t *testing.T) {
	var body strings.Builder
	for i := 0; i < maxOtherMetricSamples; i++ {
		fmt.Fprintf(&body, "metric_%04d %d\n", i, i)
	}
	body.WriteString("zz_requested_metric 42\n")

	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(mithrilEndpointHeader, mithrilMetricsEndpoint)
		_, _ = w.Write([]byte(body.String()))
	}))
	defer endpoint.Close()

	session := startInMemorySession(t, Config{MetricsURL: endpoint.URL, RPCURL: endpoint.URL})
	text, isError := callToolText(t, session, "mithril_metric", map[string]any{"metric": "zz_requested_metric"})
	if isError {
		t.Fatalf("metric tool returned an error: %s", text)
	}
	for _, want := range []string{`"metric":"zz_requested_metric"`, `"name":"zz_requested_metric"`, `"value":42`} {
		if !strings.Contains(text, want) {
			t.Fatalf("metric result missing %s: %s", want, text)
		}
	}
}

func TestMetricToolDoesNotBypassKnownMetricValidation(t *testing.T) {
	for _, value := range []string{"-1", "1.5"} {
		t.Run(value, func(t *testing.T) {
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(mithrilEndpointHeader, mithrilMetricsEndpoint)
				fmt.Fprintf(w, "slot %s\n", value)
			}))
			defer endpoint.Close()

			session := startInMemorySession(t, Config{MetricsURL: endpoint.URL, RPCURL: endpoint.URL})
			text, _ := callToolText(t, session, "mithril_metric", map[string]any{"metric": "slot"})
			if !strings.Contains(text, `"error":"metric not found"`) || strings.Contains(text, `"value":`+value) {
				t.Fatalf("invalid known slot escaped semantic validation: %s", text)
			}
		})
	}
}

func TestFetchMetricsRejectsUnmarkedEndpoint(t *testing.T) {
	for _, identity := range []string{"", mithrilPprofEndpoint} {
		t.Run(identity, func(t *testing.T) {
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if identity != "" {
					w.Header().Set(mithrilEndpointHeader, identity)
				}
				_, _ = w.Write([]byte("slot 1\n"))
			}))
			defer endpoint.Close()

			if _, err := fetchMetricsText(context.Background(), endpoint.URL); err == nil || !strings.Contains(err.Error(), "expected Mithril service") {
				t.Fatalf("fetchMetricsText identity %q error = %v", identity, err)
			}
		})
	}
}

func TestParseMetricsSurfacesFormerlyHiddenFamilies(t *testing.T) {
	// bank_hash / preprocess_block / accounts_delta_hash are real families that
	// were being silently dropped; they must now appear in Other.
	body := "# TYPE bank_hash gauge\nbank_hash 42\n# TYPE preprocess_block gauge\npreprocess_block 7\n"
	sum, err := parseMetrics(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for _, name := range []string{"bank_hash", "preprocess_block"} {
		if _, ok := sum.Other[name]; !ok {
			t.Errorf("%s must be surfaced in Other, not dropped", name)
		}
	}
}

func TestParseMetricsHandlesNonFinite(t *testing.T) {
	body := `# TYPE slot gauge
slot NaN
# TYPE weird summary
weird{quantile="0.5"} NaN
weird_sum NaN
weird_count 3
# TYPE ok_gauge gauge
ok_gauge 123
`
	sum, err := parseMetrics(body)
	if err != nil {
		t.Fatalf("parseMetrics should tolerate NaN, got %v", err)
	}
	if sum.Slot != nil {
		t.Errorf("NaN slot should be absent, got %v", *sum.Slot)
	}
	// The whole summary must marshal without error (encoding/json rejects NaN).
	if _, err := json.Marshal(sum); err != nil {
		t.Errorf("summary with NaN inputs must still marshal: %v", err)
	}
	// The finite gauge still shows up in other.
	if _, ok := sum.Other["ok_gauge"]; !ok {
		t.Error("finite ok_gauge should be in other")
	}
	// The summary's finite count survives; its NaN sum is dropped.
	if got := sum.Other["weird"]; got == nil {
		t.Error("summary count should be preserved in other")
	}
}

func TestParseMetricsRedactsSensitiveLabelKeys(t *testing.T) {
	sum, err := parseMetrics("# TYPE custom gauge\ncustom{token=\"TOPSECRET\",api_key=\"KEYSECRET\",node=\"safe\"} 1\n")
	if err != nil {
		t.Fatal(err)
	}
	samples := sum.Other["custom"]
	if len(samples) != 1 {
		t.Fatalf("custom samples = %+v", samples)
	}
	if samples[0].Labels["token"] != "[REDACTED]" || samples[0].Labels["api_key"] != "[REDACTED]" || samples[0].Labels["node"] != "safe" {
		t.Fatalf("sensitive label redaction failed: %+v", samples[0].Labels)
	}
}

func TestSafeUint64(t *testing.T) {
	for _, invalid := range []float64{-1, 1.5, math.Inf(1), math.NaN(), math.Exp2(64)} {
		if _, ok := safeUint64(invalid); ok {
			t.Errorf("%v should be rejected", invalid)
		}
	}
	if u, ok := safeUint64(42); !ok || u != 42 {
		t.Errorf("42 -> %d,%v", u, ok)
	}
}
