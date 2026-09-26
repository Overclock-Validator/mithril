package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maxMetricsResponseBytes  = 4 * 1024 * 1024 // Bound expfmt's in-memory object amplification.
	maxMetricLines           = 25_000          // CPU/allocation guard; real Mithril output is far smaller.
	maxMetricNameLength      = 512             // bound the user-supplied lookup key.
	maxOtherMetricSamples    = 1_000
	maxOtherMetricBytes      = 64 * 1024
	maxMetricFamilyNameBytes = 256
	maxMetricLabelsPerSample = 32
	maxMetricLabelNameBytes  = 128
	maxMetricLabelValueBytes = 512
	maxRawMetricsBytes       = 64 * 1024
	rawMetricsWireReserve    = 20
)

// fetchMetricsText validates and pins the endpoint, then reads a capped body.
func fetchMetricsText(ctx context.Context, rawURL string) (string, error) {
	u, err := validateURL(rawURL)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", sanitizeHTTPError(err)
	}
	resp, err := doPinnedRequest(ctx, req, outboundHTTPTimeout)
	if err != nil {
		return "", sanitizeHTTPError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}
	if err := requireMithrilEndpoint(resp, mithrilMetricsEndpoint); err != nil {
		return "", err
	}
	body, err := readCappedBody(resp, maxMetricsResponseBytes)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// Sample is a single parsed Prometheus metric sample.
type Sample struct {
	Name            string            `json:"name"`
	Labels          map[string]string `json:"labels"`
	LabelsTruncated bool              `json:"labels_truncated,omitempty"`
	Value           float64           `json:"value"`
}

// SnapshotWorkerPoolUtilizationSummary is the maximum value last reported by
// any snapshot worker pool. The producer does not reliably reset this gauge.
type SnapshotWorkerPoolUtilizationSummary struct {
	LastReportedMax float64 `json:"last_reported_max"`
	MayBeStale      bool    `json:"may_be_stale"`
}

// TurbineShredRejectionsSummary keeps the receiver's fixed rejection reasons
// outside the bounded generic metric set.
type TurbineShredRejectionsSummary struct {
	Parse          *uint64 `json:"parse"`
	Signature      *uint64 `json:"signature"`
	MissingLeader  *uint64 `json:"missing_leader"`
	Assembly       *uint64 `json:"assembly"`
	SamplesOmitted int     `json:"samples_omitted"`
}

// MetricsSummary is a structured summary of Mithril's Prometheus metrics.
// Well-known fields remain present as null when a metric is absent.
type MetricsSummary struct {
	Slot                                     *uint64                               `json:"slot"`
	Epoch                                    *uint64                               `json:"epoch"`
	TurbineReceiverActive                    *bool                                 `json:"turbine_receiver_active"`
	TurbinePacketsReceived                   *uint64                               `json:"turbine_packets_received_total"`
	TurbineDataShredsReceived                *uint64                               `json:"turbine_data_shreds_received_total"`
	TurbineBlocksEmitted                     *uint64                               `json:"turbine_blocks_emitted_total"`
	TurbineShredsRejected                    *TurbineShredRejectionsSummary        `json:"turbine_shreds_rejected_total"`
	TurbineAssemblerActiveSlots              *uint64                               `json:"turbine_assembler_active_slots"`
	TurbineLastPacketTimestampSeconds        *uint64                               `json:"turbine_last_packet_timestamp_seconds"`
	TurbineLastDataSlot                      *uint64                               `json:"turbine_last_data_slot"`
	TurbineLastBlockTimestampSeconds         *uint64                               `json:"turbine_last_block_timestamp_seconds"`
	TurbineLastBlockSlot                     *uint64                               `json:"turbine_last_block_slot"`
	ProcessRSSBytes                          *uint64                               `json:"process_resident_memory_bytes"`
	ProcessVirtualBytes                      *uint64                               `json:"process_virtual_memory_bytes"`
	SlotReplayMsP50                          *float64                              `json:"slot_replay_duration_ms_p50"`
	SlotReplayMsP99                          *float64                              `json:"slot_replay_duration_ms_p99"`
	TxsPerBlockMean                          *float64                              `json:"txs_per_block_mean"`
	SlotReplays                              *uint64                               `json:"slot_replays"`
	SnapshotTarBytesRead                     *uint64                               `json:"snapshot_tar_bytes_read"`
	SnapshotBootstrapActive                  *bool                                 `json:"snapshot_bootstrap_active"`
	SnapshotBootstrapStartedTimestampSeconds *uint64                               `json:"snapshot_bootstrap_started_timestamp_seconds"`
	SnapshotWorkerPoolUtil                   *SnapshotWorkerPoolUtilizationSummary `json:"snapshot_worker_pool_utilization"`
	Other                                    map[string][]Sample                   `json:"other"`
	OtherSamplesRetained                     int                                   `json:"other_samples_retained"`
	OtherSamplesOmitted                      int                                   `json:"other_samples_omitted"`
	OtherTruncated                           bool                                  `json:"other_truncated"`
}

// knownMetricFamilies are summarized or excluded instead of repeated in Other.
var knownMetricFamilies = map[string]bool{
	"slot": true, "epoch": true, "slot_replays": true,
	"turbine_receiver_active": true, "turbine_packets_received_total": true,
	"turbine_data_shreds_received_total": true, "turbine_blocks_emitted_total": true,
	"turbine_shreds_rejected_total":  true,
	"turbine_assembler_active_slots": true, "turbine_last_packet_timestamp_seconds": true,
	"turbine_last_data_slot": true, "turbine_last_block_timestamp_seconds": true,
	"turbine_last_block_slot":       true,
	"process_resident_memory_bytes": true, "process_virtual_memory_bytes": true,
	"snapshot_tar_bytes_read": true, "snapshot_bootstrap_active": true,
	"snapshot_bootstrap_started_timestamp_seconds": true, "snapshot_worker_pool_utilization": true,
	"slot_replay_duration_ms": true, "txs_per_block": true,
}

func isFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func metricIdentifierSafeForDisplay(name string, maxBytes int) bool {
	return len(name) <= maxBytes && !isSensitiveFieldName(name) && redactUntrustedText(name) == name
}

// safeUint64 converts a Prometheus float to uint64, rejecting NaN/Inf/negative
// (a raw cast of those is implementation-defined).
func safeUint64(v float64) (uint64, bool) {
	if !isFinite(v) || v < 0 || math.Trunc(v) != v || v >= math.Exp2(64) {
		return 0, false
	}
	return uint64(v), true
}

func familyUint64(families map[string]*dto.MetricFamily, name string) *uint64 {
	value, ok := familyValue(families, name)
	if !ok {
		return nil
	}
	result, ok := safeUint64(value)
	if !ok {
		return nil
	}
	return &result
}

func turbineShredRejections(families map[string]*dto.MetricFamily) *TurbineShredRejectionsSummary {
	family := families["turbine_shreds_rejected_total"]
	if family == nil {
		return nil
	}
	summary := &TurbineShredRejectionsSummary{}
	for _, metric := range family.Metric {
		if len(metric.Label) != 1 || metric.Label[0].GetName() != "reason" {
			summary.SamplesOmitted++
			continue
		}
		value, ok := metricValue(metric)
		if !ok {
			summary.SamplesOmitted++
			continue
		}
		result, ok := safeUint64(value)
		if !ok {
			summary.SamplesOmitted++
			continue
		}
		switch metric.Label[0].GetValue() {
		case "parse":
			summary.Parse = &result
		case "signature":
			summary.Signature = &result
		case "missing_leader":
			summary.MissingLeader = &result
		case "assembly":
			summary.Assembly = &result
		default:
			summary.SamplesOmitted++
		}
	}
	if summary.Parse == nil && summary.Signature == nil &&
		summary.MissingLeader == nil && summary.Assembly == nil &&
		summary.SamplesOmitted == 0 {
		return nil
	}
	return summary
}

// parseMetrics builds a MetricsSummary with Prometheus's expfmt parser.
func parseMetrics(body string) (*MetricsSummary, error) {
	lineCount := strings.Count(body, "\n")
	if len(body) > 0 && body[len(body)-1] != '\n' {
		lineCount++
	}
	if lineCount > maxMetricLines {
		return nil, fmt.Errorf("too many metric lines: exceeded %d limit", maxMetricLines)
	}

	p := expfmt.NewTextParser(model.LegacyValidation)
	families, err := p.TextToMetricFamilies(strings.NewReader(body))
	if err != nil {
		return nil, sanitizeMetricsParseError(err)
	}

	sum := &MetricsSummary{Other: map[string][]Sample{}}

	// Prometheus allows values that cannot be represented as uint64.
	if v, ok := familyValue(families, "slot"); ok {
		if u, ok := safeUint64(v); ok {
			sum.Slot = &u
		}
	}
	if v, ok := familyValue(families, "epoch"); ok {
		if u, ok := safeUint64(v); ok {
			sum.Epoch = &u
		}
	}
	if v, ok := familyValue(families, "turbine_receiver_active"); ok && (v == 0 || v == 1) {
		active := v == 1
		sum.TurbineReceiverActive = &active
	}
	sum.TurbinePacketsReceived = familyUint64(families, "turbine_packets_received_total")
	sum.TurbineDataShredsReceived = familyUint64(families, "turbine_data_shreds_received_total")
	sum.TurbineBlocksEmitted = familyUint64(families, "turbine_blocks_emitted_total")
	sum.TurbineShredsRejected = turbineShredRejections(families)
	sum.TurbineAssemblerActiveSlots = familyUint64(families, "turbine_assembler_active_slots")
	sum.TurbineLastPacketTimestampSeconds = familyUint64(families, "turbine_last_packet_timestamp_seconds")
	sum.TurbineLastDataSlot = familyUint64(families, "turbine_last_data_slot")
	sum.TurbineLastBlockTimestampSeconds = familyUint64(families, "turbine_last_block_timestamp_seconds")
	sum.TurbineLastBlockSlot = familyUint64(families, "turbine_last_block_slot")
	if v, ok := familyValue(families, "process_resident_memory_bytes"); ok {
		if u, ok := safeUint64(v); ok {
			sum.ProcessRSSBytes = &u
		}
	}
	if v, ok := familyValue(families, "process_virtual_memory_bytes"); ok {
		if u, ok := safeUint64(v); ok {
			sum.ProcessVirtualBytes = &u
		}
	}
	if v, ok := familyValue(families, "slot_replays"); ok {
		if u, ok := safeUint64(v); ok {
			sum.SlotReplays = &u
		}
	}
	if v, ok := familyValue(families, "snapshot_tar_bytes_read"); ok {
		if u, ok := safeUint64(v); ok {
			sum.SnapshotTarBytesRead = &u
		}
	}
	if v, ok := familyValue(families, "snapshot_bootstrap_active"); ok && (v == 0 || v == 1) {
		active := v == 1
		sum.SnapshotBootstrapActive = &active
	}
	if v, ok := familyValue(families, "snapshot_bootstrap_started_timestamp_seconds"); ok {
		if u, ok := safeUint64(v); ok {
			sum.SnapshotBootstrapStartedTimestampSeconds = &u
		}
	}
	// Real Mithril emits one value per task, but the producer may leave its last
	// value in place after work ends. Preserve the maximum without calling it current.
	if v, ok := familyValueMax(families, "snapshot_worker_pool_utilization"); ok && isFinite(v) {
		sum.SnapshotWorkerPoolUtil = &SnapshotWorkerPoolUtilizationSummary{
			LastReportedMax: v,
			MayBeStale:      true,
		}
	}
	if v, ok := histogramPercentile(families["slot_replay_duration_ms"], 0.50); ok {
		sum.SlotReplayMsP50 = &v
	}
	if v, ok := histogramPercentile(families["slot_replay_duration_ms"], 0.99); ok {
		sum.SlotReplayMsP99 = &v
	}
	if v, ok := histogramMean(families["txs_per_block"]); ok {
		sum.TxsPerBlockMean = &v
	}

	// Map iteration order must not decide which samples survive the bound.
	// Sort before truncation so map iteration cannot affect the result.
	familyNames := make([]string, 0, len(families))
	for name := range families {
		familyNames = append(familyNames, name)
	}
	sort.Strings(familyNames)
	otherBytes := 2 // JSON encoding of the initially empty object: {}.
	otherBudgetExhausted := false
	for _, name := range familyNames {
		if knownMetricFamilies[name] {
			continue
		}
		f := families[name]
		for _, m := range f.Metric {
			values := metricSampleValues(name, m)
			if len(values) == 0 {
				continue
			}
			if !metricIdentifierSafeForDisplay(name, maxMetricFamilyNameBytes) || sum.OtherSamplesRetained >= maxOtherMetricSamples || otherBudgetExhausted {
				sum.OtherSamplesOmitted += len(values)
				continue
			}

			labels, labelsTruncated := labelMap(m)
			for _, value := range values {
				sample := Sample{
					Name:            value.name,
					Labels:          labels,
					LabelsTruncated: labelsTruncated,
					Value:           value.value,
				}
				if sum.OtherSamplesRetained >= maxOtherMetricSamples || otherBudgetExhausted {
					sum.OtherSamplesOmitted++
					continue
				}
				if !appendBoundedMetricSample(sum.Other, name, sample, &otherBytes) {
					sum.OtherSamplesOmitted++
					otherBudgetExhausted = true
					continue
				}
				sum.OtherSamplesRetained++
			}
		}
	}
	sum.OtherTruncated = sum.OtherSamplesOmitted > 0
	return sum, nil
}

// sanitizeMetricsParseError retains only the parser's line number. expfmt may
// include an attacker-controlled metric name or label value in ParseError.Msg,
// so even redacting familiar credential patterns is not sufficient to make the
// original message safe to expose to an MCP client.
func sanitizeMetricsParseError(err error) error {
	var parseErr expfmt.ParseError
	if errors.As(err, &parseErr) && parseErr.Line > 0 {
		return fmt.Errorf("failed to parse metrics payload at line %d", parseErr.Line)
	}
	return errors.New("failed to parse metrics payload")
}

type metricSampleValue struct {
	name  string
	value float64
}

func metricSampleValues(name string, m *dto.Metric) []metricSampleValue {
	if v, ok := metricValue(m); ok {
		if isFinite(v) {
			return []metricSampleValue{{name: name, value: v}}
		}
		return nil
	}
	if h := m.Histogram; h != nil {
		values := []metricSampleValue{{name: name + "_count", value: float64(h.GetSampleCount())}}
		if isFinite(h.GetSampleSum()) {
			values = append(values, metricSampleValue{name: name + "_sum", value: h.GetSampleSum()})
		}
		return values
	}
	if s := m.Summary; s != nil {
		// Summaries can emit a NaN sum before their first observation. The count
		// remains useful, while non-finite values cannot be JSON encoded.
		values := []metricSampleValue{{name: name + "_count", value: float64(s.GetSampleCount())}}
		if isFinite(s.GetSampleSum()) {
			values = append(values, metricSampleValue{name: name + "_sum", value: s.GetSampleSum()})
		}
		return values
	}
	return nil
}

func appendBoundedMetricSample(other map[string][]Sample, family string, sample Sample, encodedBytes *int) bool {
	encodedSample, err := json.Marshal(sample)
	if err != nil {
		return false
	}

	delta := len(encodedSample) + 1 // comma before another sample in an existing array
	if _, exists := other[family]; !exists {
		encodedFamily, err := json.Marshal(family)
		if err != nil {
			return false
		}
		// Optional object comma, encoded key, colon, and the two array brackets.
		delta = len(encodedFamily) + len(encodedSample) + 3
		if len(other) > 0 {
			delta++
		}
	}
	if *encodedBytes > maxOtherMetricBytes-delta {
		return false
	}
	other[family] = append(other[family], sample)
	*encodedBytes += delta
	return true
}

func labelMap(m *dto.Metric) (map[string]string, bool) {
	labels := map[string]string{}
	pairs := append([]*dto.LabelPair(nil), m.GetLabel()...)
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].GetName() < pairs[j].GetName() })
	truncated := len(pairs) > maxMetricLabelsPerSample
	for i, l := range pairs {
		if i >= maxMetricLabelsPerSample {
			break
		}
		name := l.GetName()
		if len(name) > maxMetricLabelNameBytes || redactUntrustedText(name) != name {
			truncated = true
			continue
		}
		if isSensitiveFieldName(name) {
			if isPlainSensitiveFieldName(name) {
				labels[name] = "[REDACTED]"
			} else {
				truncated = true
			}
			continue
		}
		value := redactUntrustedText(l.GetValue())
		var valueTruncated bool
		value, valueTruncated = truncateUTF8Bytes(value, maxMetricLabelValueBytes)
		truncated = truncated || valueTruncated
		labels[name] = value
	}
	return labels, truncated
}

// metricValue extracts a scalar value from a gauge/counter/untyped metric.
func metricValue(m *dto.Metric) (float64, bool) {
	switch {
	case m.Gauge != nil:
		return m.Gauge.GetValue(), true
	case m.Counter != nil:
		return m.Counter.GetValue(), true
	case m.Untyped != nil:
		return m.Untyped.GetValue(), true
	}
	return 0, false
}

func familyValue(fams map[string]*dto.MetricFamily, name string) (float64, bool) {
	f := fams[name]
	if f == nil || len(f.Metric) == 0 {
		return 0, false
	}
	return metricValue(f.Metric[0])
}

func familyValueMax(fams map[string]*dto.MetricFamily, name string) (float64, bool) {
	f := fams[name]
	if f == nil {
		return 0, false
	}
	var max float64
	found := false
	for _, m := range f.Metric {
		if v, ok := metricValue(m); ok && isFinite(v) && (!found || v > max) {
			max, found = v, true
		}
	}
	return max, found
}

type histogramBucket struct {
	upperBound      float64
	cumulativeCount uint64
}

// validatedHistogram sorts a histogram's buckets by upper bound and verifies
// the invariants required by percentile and mean projections. Cumulative
// bucket counts may remain flat, but must never decrease or exceed the sample
// count. A terminal +Inf bucket must account for every sample.
func validatedHistogram(h *dto.Histogram) ([]histogramBucket, uint64, bool) {
	if h == nil || h.SampleCount == nil || h.GetSampleCount() == 0 ||
		h.SampleSum == nil || !isFinite(h.GetSampleSum()) || h.GetSampleSum() < 0 ||
		len(h.Bucket) == 0 {
		return nil, 0, false
	}

	total := h.GetSampleCount()
	buckets := make([]histogramBucket, 0, len(h.Bucket))
	for _, b := range h.Bucket {
		if b == nil || b.UpperBound == nil || b.CumulativeCount == nil {
			return nil, 0, false
		}
		upperBound := b.GetUpperBound()
		if math.IsNaN(upperBound) || math.IsInf(upperBound, -1) || upperBound < 0 {
			return nil, 0, false
		}
		buckets = append(buckets, histogramBucket{
			upperBound:      upperBound,
			cumulativeCount: b.GetCumulativeCount(),
		})
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].upperBound < buckets[j].upperBound })

	var previousCount uint64
	for i, bucket := range buckets {
		if i > 0 && bucket.upperBound <= buckets[i-1].upperBound {
			return nil, 0, false
		}
		if bucket.cumulativeCount < previousCount || bucket.cumulativeCount > total {
			return nil, 0, false
		}
		previousCount = bucket.cumulativeCount
	}
	last := buckets[len(buckets)-1]
	if !math.IsInf(last.upperBound, 1) || last.cumulativeCount != total {
		return nil, 0, false
	}
	return buckets, total, true
}

// histogramPercentile estimates a percentile from validated histogram buckets
// using linear interpolation. Malformed histograms and non-finite projections
// are unavailable rather than being presented as plausible latency values.
func histogramPercentile(f *dto.MetricFamily, p float64) (float64, bool) {
	if f == nil || len(f.Metric) == 0 || f.Metric[0].Histogram == nil || !isFinite(p) || p <= 0 || p > 1 {
		return 0, false
	}
	buckets, sampleCount, ok := validatedHistogram(f.Metric[0].Histogram)
	if !ok {
		return 0, false
	}
	total := float64(sampleCount)
	target := p * total
	if !isFinite(total) || !isFinite(target) || target <= 0 {
		return 0, false
	}

	previousBound := 0.0
	var previousCount uint64
	for _, b := range buckets {
		if float64(b.cumulativeCount) >= target {
			// The quantile lies only in the +Inf overflow bucket. Returning the
			// last finite upper bound would turn an unbounded latency into a small,
			// plausible number and is unsafe for health decisions.
			if math.IsInf(b.upperBound, 1) {
				return 0, false
			}
			countDelta := b.cumulativeCount - previousCount
			if countDelta == 0 {
				return 0, false
			}
			fraction := (target - float64(previousCount)) / float64(countDelta)
			if !isFinite(fraction) || fraction < 0 || fraction > 1 {
				return 0, false
			}
			value := previousBound + fraction*(b.upperBound-previousBound)
			if !isFinite(value) {
				return 0, false
			}
			return value, true
		}
		if !math.IsInf(b.upperBound, 1) {
			previousBound = b.upperBound
		}
		previousCount = b.cumulativeCount
	}
	return 0, false
}

// histogramMean computes a histogram's mean from its sum and count.
func histogramMean(f *dto.MetricFamily) (float64, bool) {
	if f == nil || len(f.Metric) == 0 || f.Metric[0].Histogram == nil {
		return 0, false
	}
	h := f.Metric[0].Histogram
	_, sampleCount, ok := validatedHistogram(h)
	if !ok {
		return 0, false
	}
	count := float64(sampleCount)
	mean := h.GetSampleSum() / count
	if !isFinite(mean) || mean < 0 {
		return 0, false
	}
	return mean, true
}

type scrapeMetricsInput struct {
	IncludeRaw bool `json:"include_raw,omitempty" jsonschema:"include the raw metrics text in the response"`
}

type scrapeMetricsOutput struct {
	SourceURL        string          `json:"source_url"`
	Summary          *MetricsSummary `json:"summary"`
	Raw              string          `json:"raw,omitempty"`
	RawSourceBytes   int             `json:"raw_source_bytes,omitempty"`
	RawReturnedBytes int             `json:"raw_returned_bytes,omitempty"`
	RawTruncated     bool            `json:"raw_truncated,omitempty"`
}

type metricInput struct {
	Metric string `json:"metric" jsonschema:"the Prometheus metric name to look up"`
}

type metricOutput struct {
	Metric              string `json:"metric"`
	Value               any    `json:"value,omitempty"`
	Error               string `json:"error,omitempty"`
	SummaryTruncated    bool   `json:"summary_truncated,omitempty"`
	OtherSamplesOmitted int    `json:"other_samples_omitted,omitempty"`
}

// boundedRawMetrics reserves for JSON escaping and the SDK's compatibility
// TextContent copy of structured output. At the default 1 MiB wire budget, the
// raw slice is about 51 KiB even though the endpoint body may be several MiB.
func boundedRawMetrics(body string, outputBudget int) (string, bool) {
	limit := maxRawMetricsBytes
	if budgetLimit := outputBudget / rawMetricsWireReserve; budgetLimit < limit {
		limit = budgetLimit
	}
	if limit < 0 {
		limit = 0
	}
	prefix, sourceTruncated := truncateUTF8Bytes(body, limit)
	redactedText, sanitizerTruncated := redactUntrustedMultilineWithTruncation(prefix)
	bounded, redactionTruncated := truncateUTF8Bytes(redactedText, limit)
	return bounded, sourceTruncated || sanitizerTruncated || redactionTruncated
}

func registerMetricsTools(server *mcpsdk.Server, cfg Config) {
	scrapeTool := &mcpsdk.Tool{
		Name:        "mithril_scrape_metrics",
		Annotations: annReadOnlyNetwork,
		Description: "Summarize Mithril's Prometheus metrics, including slot, epoch, and replay latency. Snapshot worker utilization is last-reported and may be stale.",
	}
	if cfg.Profile == ProfileDiagnostic {
		scrapeTool.Description += " In this profile, include_raw returns a bounded, secret-redacted prefix."
	} else {
		// The handler type retains IncludeRaw for the diagnostic profile, but a
		// least-privilege catalog must not advertise an argument it will reject.
		scrapeTool.InputSchema = map[string]any{
			"type":                 "object",
			"additionalProperties": false,
		}
	}
	addTool(server, cfg, scrapeTool, func(ctx context.Context, _ *mcpsdk.CallToolRequest, in scrapeMetricsInput) (*mcpsdk.CallToolResult, scrapeMetricsOutput, error) {
		url := cfg.MetricsURL
		safe := sanitizeURLForDisplay(url)
		body, err := fetchMetricsText(ctx, url)
		if err != nil {
			return nil, scrapeMetricsOutput{}, fmt.Errorf("failed to fetch metrics from %s: %w", safe, err)
		}
		summary, err := parseMetrics(body)
		if err != nil {
			return nil, scrapeMetricsOutput{}, err
		}
		out := scrapeMetricsOutput{SourceURL: safe, Summary: summary}
		if in.IncludeRaw {
			out.RawSourceBytes = len(body)
			out.Raw, out.RawTruncated = boundedRawMetrics(body, cfg.OutputBudgetBytes)
			out.RawReturnedBytes = len(out.Raw)
		}
		return nil, out, nil
	})

	addTool(server, cfg, &mcpsdk.Tool{
		Name:         "mithril_metric",
		Annotations:  annReadOnlyNetwork,
		OutputSchema: dynamicObjectOutputSchema,
		Description:  "Read one Prometheus metric family or summary projection by name. Use the base family name, not _bucket, _sum, or _count suffixes.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, in metricInput) (*mcpsdk.CallToolResult, metricOutput, error) {
		if len(in.Metric) > maxMetricNameLength {
			return nil, metricOutput{}, fmt.Errorf("metric name too long: %d exceeds %d chars", len(in.Metric), maxMetricNameLength)
		}
		if !metricIdentifierSafeForDisplay(in.Metric, maxMetricNameLength) {
			return nil, metricOutput{}, errors.New("metric name contains unsafe display text")
		}
		url := cfg.MetricsURL
		safe := sanitizeURLForDisplay(url)
		body, err := fetchMetricsText(ctx, url)
		if err != nil {
			return nil, metricOutput{}, fmt.Errorf("failed to fetch metrics from %s: %w", safe, err)
		}
		summary, err := parseMetrics(body)
		if err != nil {
			return nil, metricOutput{}, err
		}
		out := lookupMetric(summary, in.Metric)
		if out.Error == "" || knownMetricFamilies[in.Metric] {
			return nil, out, nil
		}
		return nil, lookupMetricFamily(body, in.Metric), nil
	})
}

// lookupMetricFamily is the fallback for real Prometheus family names that the
// bounded summary projects or omits. Keep the one-family result bounded and
// apply the same label redaction as the summary path.
func lookupMetricFamily(body, name string) metricOutput {
	p := expfmt.NewTextParser(model.LegacyValidation)
	families, err := p.TextToMetricFamilies(strings.NewReader(body))
	if err != nil {
		return metricOutput{Metric: name, Error: "failed to parse metrics payload"}
	}
	family := families[name]
	if family == nil {
		return metricOutput{Metric: name, Error: "metric not found"}
	}

	samples := map[string][]Sample{}
	encodedBytes := 2
	omitted := 0
	for _, metric := range family.Metric {
		values := metricSampleValues(name, metric)
		labels, labelsTruncated := labelMap(metric)
		for _, value := range values {
			sample := Sample{
				Name:            value.name,
				Labels:          labels,
				LabelsTruncated: labelsTruncated,
				Value:           value.value,
			}
			if len(samples[name]) >= maxOtherMetricSamples ||
				!appendBoundedMetricSample(samples, name, sample, &encodedBytes) {
				omitted++
			}
		}
	}
	if len(samples[name]) == 0 {
		return metricOutput{Metric: name, Error: "metric contains no finite samples", OtherSamplesOmitted: omitted}
	}
	return metricOutput{
		Metric:              name,
		Value:               samples[name],
		SummaryTruncated:    omitted > 0,
		OtherSamplesOmitted: omitted,
	}
}

// lookupMetric searches named projections and the bounded Other map.
func lookupMetric(summary *MetricsSummary, name string) metricOutput {
	// The full scrape projects these real histogram family names into bounded
	// aggregates. Preserve the names operators see in /metrics while making the
	// projection explicit in the returned value.
	switch name {
	case "slot_replay_duration_ms":
		if summary.SlotReplayMsP50 == nil && summary.SlotReplayMsP99 == nil {
			return metricOutput{Metric: name, Error: "metric not found"}
		}
		return metricOutput{Metric: name, Value: map[string]*float64{
			"p50": summary.SlotReplayMsP50,
			"p99": summary.SlotReplayMsP99,
		}}
	case "txs_per_block":
		if summary.TxsPerBlockMean == nil {
			return metricOutput{Metric: name, Error: "metric not found"}
		}
		return metricOutput{Metric: name, Value: map[string]*float64{
			"mean": summary.TxsPerBlockMean,
		}}
	}

	raw, _ := json.Marshal(summary)
	var top map[string]json.RawMessage
	_ = json.Unmarshal(raw, &top)
	if v, ok := top[name]; ok {
		var val any
		_ = json.Unmarshal(v, &val)
		if val == nil {
			return metricOutput{Metric: name, Error: "metric not found"}
		}
		return metricOutput{Metric: name, Value: val}
	}
	var wrap struct {
		Other map[string]json.RawMessage `json:"other"`
	}
	_ = json.Unmarshal(raw, &wrap)
	if v, ok := wrap.Other[name]; ok {
		var val any
		_ = json.Unmarshal(v, &val)
		return metricOutput{
			Metric:              name,
			Value:               val,
			SummaryTruncated:    summary.OtherTruncated,
			OtherSamplesOmitted: summary.OtherSamplesOmitted,
		}
	}
	err := "metric not found"
	if summary.OtherTruncated {
		err = "metric not found in bounded summary"
	}
	return metricOutput{
		Metric:              name,
		Error:               err,
		SummaryTruncated:    summary.OtherTruncated,
		OtherSamplesOmitted: summary.OtherSamplesOmitted,
	}
}
