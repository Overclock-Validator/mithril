package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/progress"
	corestate "github.com/Overclock-Validator/mithril/pkg/state"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const healthyDiagnoseMetrics = `# TYPE slot gauge
slot 100
# TYPE epoch gauge
epoch 1
# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes 1048576
# TYPE process_virtual_memory_bytes gauge
process_virtual_memory_bytes 2097152
`

func collectDiagnoseHostWithRoomyDisk(ctx context.Context, cfg Config, metrics *MetricsSummary, state *ShutdownStateSummary) hostHealthOutput {
	return collectHostHealthWith(ctx, cfg.normalized(), metrics, state, func(path string) *progress.DiskInfo {
		return &progress.DiskInfo{Path: path, UsedBytes: 10, TotalBytes: 100}
	}, time.Unix(1_700_000_000, 0))
}

func runDiagnosisForTest(ctx context.Context, cfg Config, in diagnoseInput) diagnoseOutput {
	return runDiagnosisWithHostCollector(ctx, cfg, in, collectDiagnoseHostWithRoomyDisk)
}

func writeDiagnoseFixture(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func newDiagnoseFixture(t *testing.T, state map[string]any, replayMs uint64, metricsBody string) Config {
	t.Helper()
	if _, ok := state["state_schema_version"]; !ok {
		state["state_schema_version"] = corestate.CurrentStateSchemaVersion
	}
	if metricsBody == "" {
		metricsBody = healthyDiagnoseMetrics
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			w.Header().Set(mithrilEndpointHeader, mithrilMetricsEndpoint)
			_, _ = w.Write([]byte(metricsBody))
		case "/rpc":
			var request struct {
				Method string `json:"method"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if request.Method != "getEpochInfo" {
				http.Error(w, "unexpected RPC method", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"absoluteSlot":100,"blockHeight":90,"epoch":1,"slotIndex":10,"slotsInEpoch":432000,"transactionCount":7}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mithril_state.json")
	writeDiagnoseFixture(t, statePath, state)
	if err := os.WriteFile(filepath.Join(dir, "mithril.log"), []byte("I2026-07-13T01:02:03.000Z node.go:1] node running\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var replayLines []string
	for i := 0; i < minReplayHealthSamples; i++ {
		replay := mkEntry(t, uint64(100-minReplayHealthSamples+1+i), completeBlockTimings(replayMs*1_000_000))
		replayData, err := json.Marshal(replay)
		if err != nil {
			t.Fatal(err)
		}
		replayLines = append(replayLines, string(replayData))
	}
	replayPath := filepath.Join(dir, "replay_timings.jsonl")
	if err := os.WriteFile(replayPath, []byte(strings.Join(replayLines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	return Config{
		MetricsURL:      server.URL + "/metrics",
		RPCURL:          server.URL + "/rpc",
		LogDir:          dir,
		StatePath:       statePath,
		ReplayPath:      replayPath,
		ReplayP99WarnMs: 400,
		SlotsBehindWarn: 150,
	}
}

func diagnoseCheck(t *testing.T, out diagnoseOutput, name string) DiagnosticCheck {
	t.Helper()
	for _, check := range out.Checks {
		if check.Name == name {
			return check
		}
	}
	t.Fatalf("diagnostic check %q not found in %+v", name, out.Checks)
	return DiagnosticCheck{}
}

func TestDiagnoseTurbineEvidence(t *testing.T) {
	now := time.Unix(1_700_000_100, 0)
	u64 := func(value uint64) *uint64 { return &value }
	summary := func(active *bool, packetAt, blockAt, dataSlot, blockSlot *uint64) *MetricsSummary {
		return &MetricsSummary{
			TurbineReceiverActive:             active,
			TurbineLastPacketTimestampSeconds: packetAt,
			TurbineLastBlockTimestampSeconds:  blockAt,
			TurbineLastDataSlot:               dataSlot,
			TurbineLastBlockSlot:              blockSlot,
		}
	}
	active, inactive := true, false
	freshPacket := u64(uint64(now.Add(-10 * time.Second).Unix()))
	freshBlock := u64(uint64(now.Add(-20 * time.Second).Unix()))
	stalePacket := u64(uint64(now.Add(-turbineFreshness - time.Second).Unix()))
	staleBlock := u64(uint64(now.Add(-turbineFreshness - time.Second).Unix()))
	future := u64(uint64(now.Add(time.Second).Unix()))
	dataSlot, blockSlot := u64(123), u64(122)

	tests := []struct {
		name    string
		summary *MetricsSummary
		want    string
	}{
		{"missing metrics", nil, checkUnknown},
		{"missing active", summary(nil, freshPacket, freshBlock, dataSlot, blockSlot), checkUnknown},
		{"inactive", summary(&inactive, freshPacket, freshBlock, dataSlot, blockSlot), checkDegraded},
		{"no packets", summary(&active, u64(0), freshBlock, dataSlot, blockSlot), checkUnknown},
		{"invalid packet timestamp", summary(&active, u64(maxUnixTimestamp+1), freshBlock, dataSlot, blockSlot), checkUnknown},
		{"future packet timestamp", summary(&active, future, freshBlock, dataSlot, blockSlot), checkUnknown},
		{"stale packets", summary(&active, stalePacket, freshBlock, dataSlot, blockSlot), checkDegraded},
		{"no blocks", summary(&active, freshPacket, u64(0), dataSlot, blockSlot), checkUnknown},
		{"invalid block timestamp", summary(&active, freshPacket, u64(maxUnixTimestamp+1), dataSlot, blockSlot), checkUnknown},
		{"future block timestamp", summary(&active, freshPacket, future, dataSlot, blockSlot), checkUnknown},
		{"stale blocks", summary(&active, freshPacket, staleBlock, dataSlot, blockSlot), checkDegraded},
		{"missing activity slots", summary(&active, freshPacket, freshBlock, nil, nil), checkUnknown},
		{"fresh activity", summary(&active, freshPacket, freshBlock, dataSlot, blockSlot), checkOK},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, evidence := diagnoseTurbineEvidence(test.summary, now)
			if got != test.want {
				t.Fatalf("status = %q, want %q; evidence=%q", got, test.want, evidence)
			}
			if test.want == checkOK && (!strings.Contains(evidence, "point-in-time") || !strings.Contains(evidence, "does not prove")) {
				t.Fatalf("healthy evidence lacks scope caveat: %q", evidence)
			}
			if test.want == checkOK && (!strings.Contains(evidence, "latest parsed data-shred slot") || strings.Contains(evidence, "ago at data slot")) {
				t.Fatalf("healthy evidence conflates packet and shred observations: %q", evidence)
			}
		})
	}
}

func TestRunDiagnosisEvaluatesTurbineAfterMetricsScrape(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		target := time.Now().Truncate(time.Second).Add(time.Second)
		time.Sleep(time.Until(target.Add(10 * time.Millisecond)))
		w.Header().Set(mithrilEndpointHeader, mithrilMetricsEndpoint)
		_, _ = fmt.Fprintf(w, `# TYPE slot gauge
slot 100
# TYPE turbine_receiver_active gauge
turbine_receiver_active 1
# TYPE turbine_last_packet_timestamp_seconds gauge
turbine_last_packet_timestamp_seconds %d
# TYPE turbine_last_data_slot gauge
turbine_last_data_slot 100
# TYPE turbine_last_block_timestamp_seconds gauge
turbine_last_block_timestamp_seconds %d
# TYPE turbine_last_block_slot gauge
turbine_last_block_slot 100
`, target.Unix(), target.Unix())
	}))
	defer server.Close()

	out := runDiagnosisForTest(context.Background(), Config{
		MetricsURL:  server.URL,
		BlockSource: "turbine",
	}, diagnoseInput{})
	if check := diagnoseCheck(t, out, "turbine_receiver"); check.Status != checkOK {
		t.Fatalf("post-scrape Turbine check = %+v, want ok", check)
	}
}

func TestRunDiagnosisAddsTurbineCheckOnlyForNativeSource(t *testing.T) {
	now := time.Now().Unix()
	metrics := healthyDiagnoseMetrics + fmt.Sprintf(`# TYPE turbine_receiver_active gauge
turbine_receiver_active 1
# TYPE turbine_last_packet_timestamp_seconds gauge
turbine_last_packet_timestamp_seconds %d
# TYPE turbine_last_data_slot gauge
turbine_last_data_slot 100
# TYPE turbine_last_block_timestamp_seconds gauge
turbine_last_block_timestamp_seconds %d
# TYPE turbine_last_block_slot gauge
turbine_last_block_slot 100
`, now, now)
	cfg := newDiagnoseFixture(t, map[string]any{
		"stage":                "ready",
		"last_shutdown_reason": "graceful shutdown (Ctrl+C)",
	}, 100, metrics)

	cfg.BlockSource = "turbine"
	if got := diagnoseCheck(t, runDiagnosisForTest(context.Background(), cfg, diagnoseInput{}), "turbine_receiver").Status; got != checkOK {
		t.Fatalf("native Turbine status = %q, want ok", got)
	}
	cfg.BlockSource = "rpc"
	if got := diagnoseCheck(t, runDiagnosisForTest(context.Background(), cfg, diagnoseInput{}), "turbine_receiver").Status; got != checkSkipped {
		t.Fatalf("RPC-source Turbine status = %q, want skipped", got)
	}
}

func TestRunDiagnosisStateAndShutdownMatrix(t *testing.T) {
	tests := []struct {
		name           string
		stage          string
		reason         string
		wantOverall    string
		wantState      string
		wantShutdown   string
		wantComplete   bool
		corruptionInfo bool
	}{
		{"ready graceful", "ready", "graceful shutdown (Ctrl+C)", diagnosticHealthy, checkOK, checkOK, true, false},
		{"ready no prior shutdown", "ready", "", diagnosticHealthy, checkOK, checkOK, true, false},
		{"ready completed", "ready", "replay completed - reached end slot", diagnosticHealthy, checkOK, checkOK, true, false},
		{"ready after stall", "ready", "block fetch stalled - no RPC progress for 5+ minutes", diagnosticDegraded, checkOK, checkDegraded, true, false},
		{"ready after leader failure", "ready", "leader schedule fetch failed from all RPC endpoints", diagnosticDegraded, checkOK, checkDegraded, true, false},
		{"ready after replay error", "ready", "replay error: bankhash mismatch", diagnosticCritical, checkOK, checkCritical, true, false},
		{"corrupted", "corrupted", "graceful shutdown (Ctrl+C)", diagnosticCritical, checkCritical, checkOK, true, true},
		{"building", "building", "graceful shutdown (Ctrl+C)", diagnosticDegraded, checkDegraded, checkOK, true, false},
		{"downloading", "downloading", "graceful shutdown (Ctrl+C)", diagnosticDegraded, checkDegraded, checkOK, true, false},
		{"future stage", "future-stage", "graceful shutdown (Ctrl+C)", diagnosticUnknown, checkUnknown, checkOK, false, false},
		{"missing stage", "", "graceful shutdown (Ctrl+C)", diagnosticUnknown, checkUnknown, checkOK, false, false},
		{"future shutdown reason", "ready", "future shutdown category", diagnosticUnknown, checkOK, checkUnknown, false, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := map[string]any{"last_shutdown_reason": test.reason}
			if test.stage != "" {
				state["stage"] = test.stage
			}
			if test.corruptionInfo {
				state["corruption_reason"] = "bankhash marker mismatch"
			}
			cfg := newDiagnoseFixture(t, state, 100, "")
			out := runDiagnosisForTest(context.Background(), cfg, diagnoseInput{})
			if out.Status != test.wantOverall {
				t.Fatalf("overall status = %q, want %q; checks=%+v", out.Status, test.wantOverall, out.Checks)
			}
			if got := diagnoseCheck(t, out, "state").Status; got != test.wantState {
				t.Errorf("state status = %q, want %q", got, test.wantState)
			}
			if got := diagnoseCheck(t, out, "shutdown").Status; got != test.wantShutdown {
				t.Errorf("shutdown status = %q, want %q", got, test.wantShutdown)
			}
			if out.EvidenceComplete != test.wantComplete {
				t.Errorf("evidence_complete = %v, want %v", out.EvidenceComplete, test.wantComplete)
			}
		})
	}
}

func TestDiagnoseStateEvidence(t *testing.T) {
	slot := func(value uint64) *uint64 { return &value }
	tests := []struct {
		name    string
		summary *ShutdownStateSummary
		status  string
		text    []string
	}{
		{
			name: "clear",
			summary: &ShutdownStateSummary{
				SchemaSupported:          true,
				AlpenglowEvidence:        &AlpenglowFinalityEvidenceSummary{},
				ReplayDivergenceEvidence: &ReplayDivergenceEvidenceSummary{},
			},
			status: checkOK,
			text:   []string{"no unresolved"},
		},
		{
			name: "Alpenglow finality",
			summary: &ShutdownStateSummary{
				SchemaSupported: true,
				AlpenglowEvidence: &AlpenglowFinalityEvidenceSummary{
					Count: 2, ConflictCount: 1, EarliestSlot: slot(110), LatestSlot: slot(120),
				},
				ReplayDivergenceEvidence: &ReplayDivergenceEvidenceSummary{},
			},
			status: checkCritical,
			text:   []string{"count=2", "slots 110-120", "conflicts=1", "exact finality match", "operator triage"},
		},
		{
			name: "replay divergence",
			summary: &ShutdownStateSummary{
				SchemaSupported:   true,
				AlpenglowEvidence: &AlpenglowFinalityEvidenceSummary{},
				ReplayDivergenceEvidence: &ReplayDivergenceEvidenceSummary{
					Count: 2, EarliestSlot: slot(130), LatestSlot: slot(140),
				},
			},
			status: checkCritical,
			text:   []string{"count=2", "slots 130-140", "folds are blocked", "operator triage"},
		},
		{name: "unsupported schema", summary: &ShutdownStateSummary{}, status: checkUnknown, text: []string{"supported state schema"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, evidence := diagnoseStateEvidence(test.summary)
			if status != test.status {
				t.Fatalf("status = %q, want %q; evidence=%s", status, test.status, evidence)
			}
			for _, want := range test.text {
				if !strings.Contains(evidence, want) {
					t.Errorf("state-evidence check missing %q: %s", want, evidence)
				}
			}
		})
	}
}

func TestRunDiagnosisDivergenceIsCritical(t *testing.T) {
	cfg := newDiagnoseFixture(t, map[string]any{
		"stage":                "ready",
		"last_shutdown_reason": "graceful shutdown (Ctrl+C)",
	}, 100, "")
	consensusDir := filepath.Join(cfg.LogDir, "consensus")
	if err := os.MkdirAll(consensusDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeDiagnoseFixture(t, filepath.Join(consensusDir, "bankhash_mismatch_slot_100.json"), map[string]any{
		"type":             "bankhash_mismatch",
		"checked_slot":     100,
		"our_bankhash":     testHash,
		"winning_bankhash": testOtherHash,
		"policy":           "halt",
		"run_id":           "run-test",
		"created_at":       "2026-07-13T00:00:00Z",
	})

	out := runDiagnosisForTest(context.Background(), cfg, diagnoseInput{})
	if out.Status != diagnosticCritical {
		t.Fatalf("status = %q, want critical; checks=%+v", out.Status, out.Checks)
	}
	check := diagnoseCheck(t, out, "divergence_artifacts")
	if got := check.Status; got != checkCritical {
		t.Fatalf("divergence artifact status = %q, want critical", got)
	}
	if !strings.Contains(check.Evidence, divergenceRecoveryGuidance) {
		t.Fatalf("divergence recovery guidance = %q, want %q", check.Evidence, divergenceRecoveryGuidance)
	}
	if len(out.DivergenceArtifacts) != 1 || out.DivergenceArtifacts[0].CheckedSlot == nil || *out.DivergenceArtifacts[0].CheckedSlot != 100 {
		t.Fatalf("divergence artifacts = %+v", out.DivergenceArtifacts)
	}
}

func TestRunDiagnosisHistoricalErrorsDoNotPermanentlyDegrade(t *testing.T) {
	cfg := newDiagnoseFixture(t, map[string]any{
		"stage":                "ready",
		"last_shutdown_reason": "graceful shutdown (Ctrl+C)",
	}, 100, "")
	var log strings.Builder
	for i := 0; i < 6; i++ {
		fmt.Fprintf(&log, "E2026-07-13T01:02:%02d.000Z node.go:%d] failure %d\n", i, i+1, i)
	}
	if err := os.WriteFile(filepath.Join(cfg.LogDir, "mithril.log"), []byte(log.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	out := runDiagnosisForTest(context.Background(), cfg, diagnoseInput{})
	if out.Status != diagnosticHealthy {
		t.Fatalf("status = %q, want healthy snapshot; checks=%+v", out.Status, out.Checks)
	}
	if got := diagnoseCheck(t, out, "logs").Status; got != checkOK {
		t.Fatalf("logs status = %q, want ok historical evidence", got)
	}
	if len(out.RecentErrors) != 6 {
		t.Fatalf("recent errors = %d, want 6", len(out.RecentErrors))
	}
}

func TestRunDiagnosisReportsLogTotalAndIncompleteScan(t *testing.T) {
	cfg := newDiagnoseFixture(t, map[string]any{
		"stage":                "ready",
		"last_shutdown_reason": "graceful shutdown (Ctrl+C)",
	}, 100, "")
	var log strings.Builder
	for i := 0; i < 25; i++ {
		fmt.Fprintf(&log, "E2026-07-13T01:02:%02d.000Z node.go:%d] failure %d\n", i, i+1, i)
	}
	log.WriteString(strings.Repeat("x", maxLogLineBytes+1))
	log.WriteByte('\n')
	if err := os.WriteFile(filepath.Join(cfg.LogDir, "mithril.log"), []byte(log.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	out := runDiagnosisForTest(context.Background(), cfg, diagnoseInput{})
	check := diagnoseCheck(t, out, "logs")
	if len(out.RecentErrors) != 20 || !strings.Contains(check.Evidence, "contains 25") ||
		!strings.Contains(check.Evidence, "oversized_lines=1") || !strings.Contains(check.Evidence, "source_changed_during_scan=false") {
		t.Fatalf("log diagnosis = recent:%d evidence:%q", len(out.RecentErrors), check.Evidence)
	}
	if out.EvidenceComplete {
		t.Fatal("diagnosis marked skipped oversized log evidence complete")
	}
}

func TestIncompleteLogScanEvidenceReportsSourceChange(t *testing.T) {
	evidence := incompleteLogScanEvidence(LogScanMeta{SourceChangedDuringScan: true})
	if !strings.Contains(evidence, "source_changed_during_scan=true") {
		t.Fatalf("source change is not auditable: %q", evidence)
	}
}

func TestRunDiagnosisConfiguredSourcesUnavailableIsUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "offline", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	missingRoot := filepath.Join(t.TempDir(), "missing")
	cfg := Config{
		MetricsURL:      server.URL,
		RPCURL:          server.URL,
		StatePath:       filepath.Join(missingRoot, "state.json"),
		LogDir:          missingRoot,
		ReplayPath:      filepath.Join(missingRoot, "replay.jsonl"),
		ReplayP99WarnMs: 400,
	}
	out := runDiagnosisForTest(context.Background(), cfg, diagnoseInput{})
	if out.Status != diagnosticUnknown || out.EvidenceComplete {
		t.Fatalf("status/evidence = %q/%v, want unknown/false", out.Status, out.EvidenceComplete)
	}
	for _, name := range []string{"metrics", "rpc", "state", "state_evidence", "shutdown", "logs", "divergence_artifacts", "replay"} {
		if got := diagnoseCheck(t, out, name).Status; got != checkUnknown {
			t.Errorf("%s status = %q, want unknown", name, got)
		}
	}
}

func TestRunDiagnosisReferenceRPCUnavailableIsUnknown(t *testing.T) {
	cfg := newDiagnoseFixture(t, map[string]any{
		"stage":                "ready",
		"last_shutdown_reason": "graceful shutdown (Ctrl+C)",
	}, 100, "")
	reference := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "offline", http.StatusServiceUnavailable)
	}))
	reference.Close()
	cfg.ReferenceRPCURL = reference.URL

	out := runDiagnosisForTest(context.Background(), cfg, diagnoseInput{})
	if out.Status != diagnosticUnknown || out.EvidenceComplete {
		t.Fatalf("status/evidence = %q/%v, want unknown/false; checks=%+v", out.Status, out.EvidenceComplete, out.Checks)
	}
	if got := diagnoseCheck(t, out, "cross_check").Status; got != checkUnknown {
		t.Fatalf("cross-check status = %q, want unknown", got)
	}
}

func TestRunDiagnosisFormatsExtremeAheadDistanceWithoutOverflow(t *testing.T) {
	epochServer := func(slot uint64) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":{"absoluteSlot":%d,"blockHeight":0,"epoch":0,"slotIndex":0,"slotsInEpoch":1,"transactionCount":0}}`, slot)
		}))
	}
	local := epochServer(math.MaxUint64)
	defer local.Close()
	reference := epochServer(0)
	defer reference.Close()

	out := runDiagnosisForTest(context.Background(), Config{
		RPCURL:          local.URL,
		ReferenceRPCURL: reference.URL,
		SlotsBehindWarn: 150,
	}, diagnoseInput{})
	check := diagnoseCheck(t, out, "cross_check")
	if check.Status != checkDegraded || !strings.Contains(check.Evidence, "18446744073709551615 slots ahead") {
		t.Fatalf("extreme ahead evidence = status:%q evidence:%q", check.Status, check.Evidence)
	}
	if strings.Contains(check.Evidence, "node is -") {
		t.Fatalf("extreme ahead evidence overflowed: %q", check.Evidence)
	}
}

func TestRunDiagnosisUsesReplayBlockTotalNotOverflowedPrometheusP99(t *testing.T) {
	metrics := healthyDiagnoseMetrics + `# TYPE slot_replay_duration_ms histogram
slot_replay_duration_ms_bucket{le="10"} 1
slot_replay_duration_ms_bucket{le="+Inf"} 100
slot_replay_duration_ms_sum 50000
slot_replay_duration_ms_count 100
`
	cfg := newDiagnoseFixture(t, map[string]any{
		"stage":                "ready",
		"last_shutdown_reason": "graceful shutdown (Ctrl+C)",
	}, 500, metrics)

	out := runDiagnosisForTest(context.Background(), cfg, diagnoseInput{})
	if out.Status != diagnosticDegraded {
		t.Fatalf("status = %q, want degraded; checks=%+v", out.Status, out.Checks)
	}
	if got := diagnoseCheck(t, out, "metrics").Status; got != checkOK {
		t.Errorf("metrics status = %q, want ok", got)
	}
	if got := diagnoseCheck(t, out, "replay").Status; got != checkDegraded {
		t.Errorf("replay status = %q, want degraded", got)
	}
	if out.MetricsSnapshot == nil || out.MetricsSnapshot.SlotReplayMsP99 != nil {
		t.Fatalf("overflowed Prometheus p99 must be unavailable, snapshot=%+v", out.MetricsSnapshot)
	}
	if out.ReplayStats == nil || out.ReplayStats.TimingField != timingBlockTotal || out.ReplayStats.P99Ms != 500 {
		t.Fatalf("replay stats = %+v, want block_total p99=500", out.ReplayStats)
	}
}

func TestRunDiagnosisFailsClosedOnIncompleteNewestReplayRecord(t *testing.T) {
	cfg := newDiagnoseFixture(t, map[string]any{
		"stage":                "ready",
		"last_shutdown_reason": "graceful shutdown (Ctrl+C)",
	}, 100, "")
	f, err := os.OpenFile(cfg.ReplayPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"Slot":999`); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	out := runDiagnosisForTest(context.Background(), cfg, diagnoseInput{})
	if got := diagnoseCheck(t, out, "replay").Status; got != checkUnknown {
		t.Fatalf("replay status = %q, want unknown", got)
	}
	if out.EvidenceComplete || out.ReplayMeta == nil || !out.ReplayMeta.IncompleteTail {
		t.Fatalf("incomplete tail not surfaced: evidence=%v meta=%+v", out.EvidenceComplete, out.ReplayMeta)
	}
}

func TestDiagnoseReplayEvidenceFailsClosedWhenSourceChanges(t *testing.T) {
	status, evidence := diagnoseReplayEvidence(ReplayStats{
		FieldFound: true,
		Count:      minReplayHealthSamples,
		P99Ms:      1,
	}, ReplayMeta{SourceChangedDuringScan: true}, DefaultReplayP99WarnMs)
	if status != checkUnknown || !strings.Contains(evidence, "changed while it was read") {
		t.Fatalf("changed replay evidence = status:%q evidence:%q", status, evidence)
	}
}

func TestRunDiagnosisOptionalChecksCanBeSkipped(t *testing.T) {
	cfg := newDiagnoseFixture(t, map[string]any{
		"stage":                "ready",
		"last_shutdown_reason": "graceful shutdown (Ctrl+C)",
	}, 100, "")
	no := false
	out := runDiagnosisForTest(context.Background(), cfg, diagnoseInput{IncludeLogs: &no, IncludeReplayTrend: &no})
	if out.Status != diagnosticHealthy {
		t.Fatalf("status = %q, want healthy; checks=%+v", out.Status, out.Checks)
	}
	if got := diagnoseCheck(t, out, "logs").Status; got != checkSkipped {
		t.Errorf("logs status = %q, want skipped", got)
	}
	if got := diagnoseCheck(t, out, "replay").Status; got != checkSkipped {
		t.Errorf("replay status = %q, want skipped", got)
	}
}

func callDiagnoseOverProtocol(t *testing.T, cfg Config) diagnoseOutput {
	t.Helper()
	cfg.Profile = ProfileDiagnostic
	cfg = cfg.normalized()
	server := mcpsdk.NewServer(&mcpsdk.Implementation{
		Name:    "mithril-diagnose-test",
		Version: "0",
	}, &mcpsdk.ServerOptions{})
	registerDiagnoseToolWithHostCollector(server, cfg, collectDiagnoseHostWithRoomyDisk)
	session := connectServerForTest(t, server, "diagnose-test")
	text, isError := callToolText(t, session, "mithril_diagnose", nil)
	if isError {
		t.Fatalf("diagnose returned tool error: %s", text)
	}
	if strings.TrimSpace(text) == "" {
		t.Fatal("diagnose returned no JSON text content")
	}
	var out diagnoseOutput
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("decode protocol output: %v\n%s", err, text)
	}
	return out
}

func TestRunDiagnosisReusesFetchedMetricsAndStateForHost(t *testing.T) {
	var metricRequests atomic.Int32
	metrics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		metricRequests.Add(1)
		w.Header().Set(mithrilEndpointHeader, mithrilMetricsEndpoint)
		_, _ = w.Write([]byte(healthyDiagnoseMetrics))
	}))
	defer metrics.Close()

	statePath := filepath.Join(t.TempDir(), "mithril_state.json")
	writeDiagnoseFixture(t, statePath, map[string]any{
		"state_schema_version": corestate.CurrentStateSchemaVersion,
		"stage":                "ready",
	})
	cfg := Config{MetricsURL: metrics.URL, StatePath: statePath}
	var collectorCalls int
	var receivedMetrics *MetricsSummary
	var receivedState *ShutdownStateSummary
	out := runDiagnosisWithHostCollector(context.Background(), cfg, diagnoseInput{}, func(ctx context.Context, gotCfg Config, gotMetrics *MetricsSummary, gotState *ShutdownStateSummary) hostHealthOutput {
		collectorCalls++
		receivedMetrics = gotMetrics
		receivedState = gotState
		if err := os.Remove(statePath); err != nil {
			t.Fatalf("remove state at host boundary: %v", err)
		}
		return collectHostHealth(ctx, gotCfg, gotMetrics, gotState)
	})

	if metricRequests.Load() != 1 || collectorCalls != 1 {
		t.Fatalf("metrics requests/host collections = %d/%d, want 1/1", metricRequests.Load(), collectorCalls)
	}
	if receivedMetrics == nil || receivedMetrics != out.MetricsSnapshot || receivedMetrics.ProcessRSSBytes == nil {
		t.Fatalf("host did not receive the parsed metrics snapshot: received=%p output=%p", receivedMetrics, out.MetricsSnapshot)
	}
	if receivedState == nil || receivedState != out.ShutdownState || receivedState.Stage == nil || *receivedState.Stage != "ready" {
		t.Fatalf("host did not receive the parsed state summary: received=%p output=%p", receivedState, out.ShutdownState)
	}
	if out.HostHealth == nil || !out.HostHealth.Bootstrap.StateFound || out.HostHealth.Bootstrap.Assessment != "ready" {
		t.Fatalf("host did not retain state after its source was removed: %+v", out.HostHealth)
	}
}

func TestDiagnoseProtocolReturnsAuditableChecks(t *testing.T) {
	cfg := newDiagnoseFixture(t, map[string]any{
		"stage":                "ready",
		"last_shutdown_reason": "graceful shutdown (Ctrl+C)",
	}, 100, "")
	out := callDiagnoseOverProtocol(t, cfg)
	if out.Status != diagnosticHealthy || !out.EvidenceComplete {
		t.Fatalf("protocol status/evidence = %q/%v", out.Status, out.EvidenceComplete)
	}
	if out.AssessmentScope != "point_in_time_snapshot" || out.SafeForAutomation {
		t.Fatalf("diagnostic overstated its automation authority: scope=%q safe=%v", out.AssessmentScope, out.SafeForAutomation)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, out.ObservedAt)
	if err != nil || observedAt.Location() != time.UTC {
		t.Fatalf("observed_at = %q, want an RFC3339 UTC timestamp: %v", out.ObservedAt, err)
	}
	for _, name := range []string{"metrics", "rpc", "state", "state_evidence", "shutdown", "logs", "divergence_artifacts", "replay", "cross_check"} {
		_ = diagnoseCheck(t, out, name)
	}
	if evidence := diagnoseCheck(t, out, "divergence_artifacts").Evidence; !strings.Contains(evidence, "does not prove") || !strings.Contains(evidence, "verification") {
		t.Fatalf("empty divergence scan overstates its evidence: %q", evidence)
	}
	if out.RPCSnapshot == nil || out.RPCSnapshot.AbsoluteSlot != 100 {
		t.Fatalf("rpc_snapshot = %+v", out.RPCSnapshot)
	}
	if out.ReplayStats == nil || out.ReplayStats.TimingField != timingBlockTotal {
		t.Fatalf("replay_stats = %+v", out.ReplayStats)
	}
	if evidence := diagnoseCheck(t, out, "replay").Evidence; !strings.Contains(evidence, "complete records") || !strings.Contains(evidence, "do not prove") || !strings.Contains(evidence, "wall-clock") || !strings.Contains(evidence, "asynchronous") {
		t.Fatalf("replay evidence omits measurement caveat: %q", evidence)
	}
	if len(out.Findings) != 1 || out.Findings[0].Category != "overall" {
		t.Fatalf("healthy findings = %+v", out.Findings)
	}
	if !strings.Contains(out.Findings[0].Message, "consensus activity") || !strings.Contains(out.Findings[0].Message, "slot progress") || !strings.Contains(out.Findings[0].Message, "rewards") {
		t.Fatalf("healthy snapshot caveat is missing: %+v", out.Findings)
	}
}

func TestDiagnoseRejectsUnsupportedStateSchema(t *testing.T) {
	cfg := newDiagnoseFixture(t, map[string]any{
		"state_schema_version": 1,
		"stage":                "ready",
	}, 100, "")
	out := runDiagnosisForTest(context.Background(), cfg, diagnoseInput{})
	if got := diagnoseCheck(t, out, "state").Status; got != checkCritical {
		t.Fatalf("state check = %q, want critical", got)
	}
	if got := diagnoseCheck(t, out, "state_evidence").Status; got != checkUnknown {
		t.Fatalf("state evidence check = %q, want unknown for unsupported schema", got)
	}
	if out.Status != diagnosticCritical || out.ShutdownState == nil || out.ShutdownState.SchemaSupported {
		t.Fatalf("unsupported state schema was not surfaced: %+v", out)
	}
}

func TestMergeDiagnosticStatusPrecedence(t *testing.T) {
	steps := []struct {
		current, check, want string
	}{
		{diagnosticHealthy, checkDegraded, diagnosticDegraded},
		{diagnosticDegraded, checkUnknown, diagnosticUnknown},
		{diagnosticUnknown, checkDegraded, diagnosticUnknown},
		{diagnosticUnknown, checkCritical, diagnosticCritical},
		{diagnosticCritical, checkUnknown, diagnosticCritical},
	}
	for _, step := range steps {
		t.Run(fmt.Sprintf("%s+%s", step.current, step.check), func(t *testing.T) {
			if got := mergeDiagnosticStatus(step.current, step.check); got != step.want {
				t.Fatalf("merge = %q, want %q", got, step.want)
			}
		})
	}
}
