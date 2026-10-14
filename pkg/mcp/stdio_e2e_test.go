package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	corestate "github.com/Overclock-Validator/mithril/pkg/state"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	stdioE2EHelperEnv = "MITHRIL_MCP_STDIO_E2E_HELPER"
	stdioE2ESlot      = uint64(500)

	stdioE2EMetricsPathSecret     = "STDIO_METRICS_PATH_SECRET"
	stdioE2EMetricsQuerySecret    = "STDIO_METRICS_QUERY_SECRET"
	stdioE2EMetricLabelSecret     = "STDIO_METRIC_LABEL_SECRET"
	stdioE2EMetricRawSecret       = "STDIO_METRIC_RAW_SECRET"
	stdioE2ERPCPathSecret         = "STDIO_RPC_PATH_SECRET"
	stdioE2ERPCQuerySecret        = "STDIO_RPC_QUERY_SECRET"
	stdioE2EReferencePathSecret   = "STDIO_REFERENCE_PATH_SECRET"
	stdioE2EReferenceQuerySecret  = "STDIO_REFERENCE_QUERY_SECRET"
	stdioE2EPprofQuerySecret      = "STDIO_PPROF_QUERY_SECRET"
	stdioE2ELogSecret             = "STDIO_LOG_SECRET"
	stdioE2ELogBearerSecret       = "STDIO_LOG_BEARER_SECRET"
	stdioE2EStatePathSecret       = "STDIO_STATE_PATH_SECRET"
	stdioE2EStateQuerySecret      = "STDIO_STATE_QUERY_SECRET"
	stdioE2EReplaySecret          = "STDIO_REPLAY_SECRET"
	stdioE2EDivergencePathSecret  = "STDIO_DIVERGENCE_PATH_SECRET"
	stdioE2EDivergenceQuerySecret = "STDIO_DIVERGENCE_QUERY_SECRET"
)

// TestMCPStdioHelperProcess is entered only by the subprocess launched from
// TestMCPStdioSubprocessE2E. os.Exit avoids the testing package writing "PASS"
// to stdout after Serve returns; stdout must remain an MCP-only byte stream.
func TestMCPStdioHelperProcess(_ *testing.T) {
	if os.Getenv(stdioE2EHelperEnv) != "1" {
		return
	}
	if err := Serve(context.Background(), ConfigFromEnv()); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "stdio MCP helper failed: %s\n", redactUntrustedText(err.Error()))
		os.Exit(2)
	}
	os.Exit(0)
}

type stdioE2EFixture struct {
	env     []string
	secrets []string
}

func newStdioE2EFixture(t *testing.T) stdioE2EFixture {
	t.Helper()

	observedUnix := time.Now().Unix()
	metricsBody := fmt.Sprintf(`# HELP slot Current slot
# TYPE slot gauge
slot 500
# HELP epoch Current epoch
# TYPE epoch gauge
epoch 7
# HELP slot_replays Replayed slots
# TYPE slot_replays counter
slot_replays 12
# HELP slot_replay_duration_ms Replay duration
# TYPE slot_replay_duration_ms histogram
slot_replay_duration_ms_bucket{le="1"} 0
slot_replay_duration_ms_bucket{le="2"} 0
slot_replay_duration_ms_bucket{le="5"} 10
slot_replay_duration_ms_bucket{le="10"} 50
slot_replay_duration_ms_bucket{le="20"} 70
slot_replay_duration_ms_bucket{le="50"} 95
slot_replay_duration_ms_bucket{le="100"} 100
slot_replay_duration_ms_bucket{le="200"} 100
slot_replay_duration_ms_bucket{le="400"} 100
slot_replay_duration_ms_bucket{le="800"} 100
slot_replay_duration_ms_bucket{le="1600"} 100
slot_replay_duration_ms_bucket{le="3200"} 100
slot_replay_duration_ms_bucket{le="6400"} 100
slot_replay_duration_ms_bucket{le="10000"} 100
slot_replay_duration_ms_bucket{le="+Inf"} 100
slot_replay_duration_ms_sum 2000
slot_replay_duration_ms_count 100
# HELP process_resident_memory_bytes Resident memory
# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes 104857600
# HELP process_virtual_memory_bytes Virtual memory
# TYPE process_virtual_memory_bytes gauge
process_virtual_memory_bytes 536870912
# HELP snapshot_bootstrap_active Snapshot bootstrap state
# TYPE snapshot_bootstrap_active gauge
snapshot_bootstrap_active 0
# TYPE turbine_receiver_active gauge
turbine_receiver_active 1
# TYPE turbine_last_packet_timestamp_seconds gauge
turbine_last_packet_timestamp_seconds %d
# TYPE turbine_last_data_slot gauge
turbine_last_data_slot 500
# TYPE turbine_last_block_timestamp_seconds gauge
turbine_last_block_timestamp_seconds %d
# TYPE turbine_last_block_slot gauge
turbine_last_block_slot 500
# HELP custom_metric token=%s
# TYPE custom_metric gauge
custom_metric{source="https://metrics.invalid/%s?api-key=label-value"} 1
`, observedUnix, observedUnix, stdioE2EMetricRawSecret, stdioE2EMetricLabelSecret)

	metricsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("metrics method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/metrics/"+stdioE2EMetricsPathSecret {
			t.Errorf("metrics path = %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("api-key"); got != stdioE2EMetricsQuerySecret {
			t.Errorf("metrics query credential = %q", got)
		}
		w.Header().Set(mithrilEndpointHeader, mithrilMetricsEndpoint)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, metricsBody)
	}))
	t.Cleanup(metricsServer.Close)

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("RPC method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/rpc/"+stdioE2ERPCPathSecret {
			t.Errorf("RPC path = %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("api-key"); got != stdioE2ERPCQuerySecret {
			t.Errorf("RPC query credential = %q", got)
		}
		var request struct {
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode RPC request: %v", err)
			return
		}
		var result any
		switch request.Method {
		case "getEpochInfo":
			result = map[string]any{
				"absoluteSlot": stdioE2ESlot, "blockHeight": uint64(800), "epoch": uint64(7),
				"slotIndex": uint64(20), "slotsInEpoch": uint64(100), "transactionCount": uint64(12_345),
			}
		case "getBankHash":
			if len(request.Params) != 1 || string(request.Params[0]) != "500" {
				t.Errorf("getBankHash params = %s", request.Params)
			}
			result = testHash
		case "getLatestBlockhash":
			result = map[string]any{
				"context": map[string]any{"slot": stdioE2ESlot},
				"value": map[string]any{
					"blockhash": testHash, "lastValidBlockHeight": uint64(900),
				},
			}
		case "getBlockHeight":
			result = uint64(800)
		case "getAccountInfo":
			result = map[string]any{
				"context": map[string]any{"apiVersion": "mithril-e2e", "slot": stdioE2ESlot},
				"value": map[string]any{
					"data": []string{"AQID", "base64"}, "executable": false,
					"lamports": uint64(9_007_199_254_740_993), "owner": "11111111111111111111111111111111",
					"rentEpoch": ^uint64(0), "space": uint64(3),
				},
			}
		case "simulateTransaction":
			if len(request.Params) != 2 || string(request.Params[0]) != `"AQ=="` {
				t.Errorf("simulateTransaction params = %s", request.Params)
			}
			var config map[string]any
			if len(request.Params) == 2 {
				if err := json.Unmarshal(request.Params[1], &config); err != nil {
					t.Errorf("decode simulation config: %v", err)
				}
			}
			if config["encoding"] != "base64" || config["sigVerify"] != false || config["replaceRecentBlockhash"] != true {
				t.Errorf("simulation defaults = %#v", config)
			}
			if _, ok := config["commitment"]; ok {
				t.Errorf("simulation sent unsupported commitment config: %#v", config)
			}
			result = map[string]any{
				"context": map[string]any{"slot": stdioE2ESlot},
				"value": map[string]any{
					"err": nil, "fee": uint64(9_007_199_254_740_993),
					"logs": []string{"simulation complete"}, "unitsConsumed": uint64(321),
				},
			}
		default:
			t.Errorf("unexpected local RPC method %q", request.Method)
			result = map[string]any{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	}))
	t.Cleanup(rpcServer.Close)

	referenceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/reference/"+stdioE2EReferencePathSecret {
			t.Errorf("reference RPC path = %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("api-key"); got != stdioE2EReferenceQuerySecret {
			t.Errorf("reference RPC query credential = %q", got)
		}
		var request struct {
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode reference RPC request: %v", err)
			return
		}
		if request.Method != "getEpochInfo" {
			t.Errorf("reference RPC method = %q", request.Method)
		}
		if len(request.Params) != 1 || !bytes.Contains(request.Params[0], []byte(`"commitment":"confirmed"`)) {
			t.Errorf("reference getEpochInfo params = %s", request.Params)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":{"absoluteSlot":500,"blockHeight":800,"epoch":7}}`)
	}))
	t.Cleanup(referenceServer.Close)

	pprofServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(mithrilEndpointHeader, mithrilPprofEndpoint)
		switch r.URL.Path {
		case "/debug/pprof/heap":
			if r.URL.RawQuery != "" {
				t.Errorf("heap query = %q, want empty", r.URL.RawQuery)
			}
			_, _ = io.WriteString(w, "heap-profile")
		case "/debug/pprof/profile":
			if r.URL.Query().Get("seconds") != "1" {
				t.Errorf("profile seconds = %q", r.URL.Query().Get("seconds"))
			}
			_, _ = io.WriteString(w, "cpu-profile")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(pprofServer.Close)

	root := t.TempDir()
	logDir := filepath.Join(root, "logs")
	rewardsDir := filepath.Join(logDir, "rewards")
	consensusDir := filepath.Join(logDir, "consensus")
	accountsDir := filepath.Join(root, "accounts")
	snapshotsDir := filepath.Join(root, "snapshots")
	shredstoreDir := filepath.Join(root, "shredstore")
	for _, dir := range []string{logDir, rewardsDir, consensusDir, accountsDir, snapshotsDir, shredstoreDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create fixture directory %s: %v", dir, err)
		}
	}

	stdioE2EWriteFile(t, filepath.Join(logDir, "mithril.log"), strings.Join([]string{
		"I2026-07-13T01:02:03.000Z node.go:10] startup complete",
		"W2026-07-13T01:02:04.000Z node.go:11] token=" + stdioE2ELogSecret + " warning observed",
		"E2026-07-13T01:02:05.000Z node.go:12] Authorization: Bearer " + stdioE2ELogBearerSecret,
	}, "\n")+"\n")

	statePath := filepath.Join(root, "mithril_state.json")
	stdioE2EWriteJSON(t, statePath, map[string]any{
		"state_schema_version": corestate.CurrentStateSchemaVersion, "stage": "ready", "last_slot": stdioE2ESlot, "last_epoch": 7,
		"last_bankhash": testHash, "last_block_height": 800, "last_shutdown_reason": "graceful shutdown (Ctrl+C)",
		"last_rooted_slot": 490, "last_rooted_bankhash": testHash,
		"alpenglow_finality_evidence": []map[string]any{
			{"slot": 499, "conflict": true},
		},
		"replay_divergence_evidence": []map[string]any{
			{"slot": 498, "tx_index": 2, "kind": "tx_record", "detail": stdioE2EStatePathSecret},
		},
		"operator_note": "https://state.invalid/" + stdioE2EStatePathSecret + "?api-key=" + stdioE2EStateQuerySecret,
	})

	replayPath := filepath.Join(root, "replay_timings.jsonl")
	replayLines := make([]string, 0, minReplayHealthSamples)
	for i := 0; i < minReplayHealthSamples; i++ {
		replay := map[string]any{"Slot": 500 - minReplayHealthSamples + 1 + i}
		for _, field := range blockFields {
			replay[field] = TimingField{}
		}
		replay["TxLoop"] = TimingField{Count: 1, SumNanoseconds: uint64(20_000_000 + i*100_000)}
		replay["note"] = "token=" + stdioE2EReplaySecret
		data, err := json.Marshal(replay)
		if err != nil {
			t.Fatal(err)
		}
		replayLines = append(replayLines, string(data))
	}
	stdioE2EWriteFile(t, replayPath, strings.Join(replayLines, "\n")+"\n")

	stdioE2EWriteJSON(t, filepath.Join(consensusDir, "bankhash_mismatch_slot_500.json"), map[string]any{
		"type": "bankhash_mismatch", "checked_slot": stdioE2ESlot, "our_bankhash": testHash,
		"winning_bankhash": "4vJ9JU1bJJE96FWSJKvHsmmF3drisEAs5XFWmZ7BvC7Y", "policy": "halt", "run_id": "stdio-e2e", "created_at": "2026-07-13T00:00:00Z",
		"operator_note": "https://divergence.invalid/" + stdioE2EDivergencePathSecret + "?api-key=" + stdioE2EDivergenceQuerySecret,
	})

	calculatedName := "epoch_boundary_calculated_rewards_slot_500"
	writeRewardFixture(t, rewardsDir, calculatedName+".json", calculatedRewardFixture(stdioE2ESlot, 7))
	writeRewardFixture(t, rewardsDir, calculatedName+".csv", "record_type,lamports\n")
	writeRewardFixture(t, rewardsDir, "epoch_boundary_voting_rewards_slot_500.json", votingRewardFixture(stdioE2ESlot, 7))

	metricsURL := metricsServer.URL + "/metrics/" + stdioE2EMetricsPathSecret + "?api-key=" + stdioE2EMetricsQuerySecret
	rpcURL := rpcServer.URL + "/rpc/" + stdioE2ERPCPathSecret + "?api-key=" + stdioE2ERPCQuerySecret
	referenceURL := referenceServer.URL + "/reference/" + stdioE2EReferencePathSecret + "?api-key=" + stdioE2EReferenceQuerySecret
	pprofURL := pprofServer.URL + "/ignored?api-key=" + stdioE2EPprofQuerySecret

	return stdioE2EFixture{
		env: []string{
			stdioE2EHelperEnv + "=1",
			"MITHRIL_MCP_PROFILE=diagnostic",
			"MITHRIL_METRICS_URL=" + metricsURL,
			"MITHRIL_RPC_URL=" + rpcURL,
			"MITHRIL_LOG_DIR=" + logDir,
			"MITHRIL_ACCOUNTS_PATH=" + accountsDir,
			"MITHRIL_SNAPSHOTS_PATH=" + snapshotsDir,
			"MITHRIL_SHREDSTORE_PATH=" + shredstoreDir,
			"MITHRIL_STATE_PATH=" + statePath,
			"MITHRIL_REPLAY_PATH=" + replayPath,
			"MITHRIL_PPROF_URL=" + pprofURL,
			"MITHRIL_REFERENCE_RPC_URL=" + referenceURL,
			"MITHRIL_REPLAY_P99_WARN_MS=400",
			"MITHRIL_SLOTS_BEHIND_WARN=5",
			"MITHRIL_BLOCK_SOURCE=turbine",
			"MITHRIL_MCP_MAX_CONCURRENT=64",
			"MITHRIL_MCP_RATE_PER_SECOND=10000",
			"MITHRIL_MCP_RATE_BURST=128",
			"MITHRIL_MCP_OUTPUT_BUDGET_BYTES=4194304",
		},
		secrets: []string{
			stdioE2EMetricsPathSecret, stdioE2EMetricsQuerySecret, stdioE2EMetricLabelSecret, stdioE2EMetricRawSecret,
			stdioE2ERPCPathSecret, stdioE2ERPCQuerySecret, stdioE2EReferencePathSecret, stdioE2EReferenceQuerySecret,
			stdioE2EPprofQuerySecret,
			stdioE2ELogSecret, stdioE2ELogBearerSecret, stdioE2EStatePathSecret, stdioE2EStateQuerySecret,
			stdioE2EReplaySecret, stdioE2EDivergencePathSecret, stdioE2EDivergenceQuerySecret,
			"SUPER_PATH_SECRET", "SUPER_QUERY_SECRET", "SUPER_ERROR_SECRET",
		},
	}
}

func stdioE2EWriteFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}

func stdioE2EWriteJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal fixture %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}

type stdioE2EToolCase struct {
	arguments    map[string]any
	expectedText []string
}

func stdioE2EToolCases() map[string]stdioE2EToolCase {
	return map[string]stdioE2EToolCase{
		"mithril_mcp_info":             {arguments: map[string]any{}, expectedText: []string{`"server_version":"` + serverVersion + `"`, `"profile":"diagnostic"`, `"max_concurrent":64`, `"rate_burst":128`, `"max_input_frame_bytes":65536`, `"input_frames_per_second":1000`, `"input_frame_burst":128`, `"reference_rpc_configured":true`}},
		"mithril_scrape_metrics":       {arguments: map[string]any{"include_raw": true}, expectedText: []string{`"slot":500`, `"epoch":7`, `"raw":`, "[REDACTED]"}},
		"mithril_metric":               {arguments: map[string]any{"metric": "slot"}, expectedText: []string{`"metric":"slot"`, `"value":500`}},
		"mithril_get_slot_info":        {arguments: map[string]any{}, expectedText: []string{`"absolute_slot":500`, `"block_height":800`, `"epoch":7`}},
		"mithril_get_bank_hash":        {arguments: map[string]any{"slot": stdioE2ESlot}, expectedText: []string{`"slot":500`, `"bank_hash":"` + testHash + `"`}},
		"mithril_get_latest_blockhash": {arguments: map[string]any{}, expectedText: []string{`"slot":500`, `"last_valid_block_height":900`, `"status":"ready"`, `"consistency":"node_reported_non_atomic"`, `"finality":"local_unfinalized"`}},
		"mithril_get_block_height":     {arguments: map[string]any{}, expectedText: []string{`"block_height":800`, `"finality":"local_unfinalized"`}},
		"mithril_get_account_info":     {arguments: map[string]any{"pubkey": "11111111111111111111111111111111", "data_length": uint64(3)}, expectedText: []string{`"found":true`, `"lamports":"9007199254740993"`, `"rent_epoch":"18446744073709551615"`, `"data_length":3`}},
		"mithril_simulate_transaction": {arguments: map[string]any{"transaction": "AQ=="}, expectedText: []string{`"slot":500`, `"err":null`, `"fee":9007199254740993`, `"unitsConsumed":321`, `"simulation complete"`}},
		"mithril_tail_log":             {arguments: map[string]any{"lines": 10, "level": "info"}, expectedText: []string{`"count":3`, `"startup complete"`, "[REDACTED]"}},
		"mithril_grep_log":             {arguments: map[string]any{"pattern": "startup", "max_matches": 10}, expectedText: []string{`"total_matches":1`, `"returned":1`, `"startup complete"`}},
		"mithril_read_shutdown_state":  {arguments: map[string]any{}, expectedText: []string{`"found":true`, `"schema_supported":true`, `"last_rooted_slot":490`, `"alpenglow_finality_evidence_summary":{`, `"conflict_count":1`, `"earliest_slot":499`, `"replay_divergence_evidence_summary":{`, `"earliest_slot":498`, `"parsed_shutdown_reason":"Normal"`, `"is_error_shutdown":false`, `"omitted_extra_fields":["operator_note"]`}},
		"mithril_read_replay_timings":  {arguments: map[string]any{"last_n": minReplayHealthSamples, "timing_field": "block_total"}, expectedText: []string{`"timing_field":"block_total"`, `"field_found":true`, `"count":20`, `"source_layout":"flat"`, "[REDACTED]"}},
		"mithril_pprof_heap":           {arguments: map[string]any{}, expectedText: []string{`"profile_bytes_b64":"aGVhcC1wcm9maWxl"`, `"size_bytes":12`}},
		"mithril_pprof_profile":        {arguments: map[string]any{"seconds": 1}, expectedText: []string{`"profile_bytes_b64":"Y3B1LXByb2ZpbGU="`, `"size_bytes":11`}},
		"mithril_cross_check_slot":     {arguments: map[string]any{"commitment": "confirmed"}, expectedText: []string{`"mithril_slot":500`, `"reference_slot":500`, `"slots_behind":0`, `"status":"in_sync"`}},
		"mithril_read_divergence":      {arguments: map[string]any{}, expectedText: []string{`"diverged":true`, `"count":1`, `"scan_complete":true`, `"checked_slot":500`, `"omitted_extra_fields":["operator_note"]`}},
		"mithril_read_rewards":         {arguments: map[string]any{"slot": stdioE2ESlot}, expectedText: []string{`"found":true`, `"artifact_state":"complete"`, `"verification_state":"reference_matched"`, `"summary_only":true`}},
		"mithril_diagnose":             {arguments: map[string]any{}, expectedText: []string{`"status":"critical"`, `"evidence_complete":true`, `"name":"turbine_receiver","status":"ok"`, `"name":"state_evidence","status":"critical"`, `"name":"divergence_artifacts","status":"critical"`, "[REDACTED]"}},
		"mithril_host_health":          {arguments: map[string]any{}, expectedText: []string{`"assessment_scope":"point_in_time_host_snapshot"`, `"node_rss_bytes":104857600`, `"assessment":"ready"`}},
	}
}

func TestMCPStdioSubprocessE2E(t *testing.T) {
	fixture := newStdioE2EFixture(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}

	cmd := exec.Command(executable, "-test.run=^TestMCPStdioHelperProcess$")
	cmd.Env = append([]string(nil), fixture.env...)
	var telemetry bytes.Buffer
	cmd.Stderr = &telemetry

	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "mithril-stdio-e2e", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcpsdk.CommandTransport{Command: cmd, TerminateDuration: 5 * time.Second}, nil)
	if err != nil {
		t.Fatalf("connect to stdio subprocess: %v; stderr=%s", err, telemetry.String())
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = session.Close()
		}
	})

	initialized := session.InitializeResult()
	if initialized == nil || initialized.ServerInfo == nil || initialized.ServerInfo.Name != "mithril" {
		t.Fatalf("unexpected initialize result: %+v", initialized)
	}
	if initialized.ProtocolVersion != "2025-11-25" || initialized.Capabilities == nil || initialized.Capabilities.Tools == nil {
		t.Fatalf("initialize did not advertise tool capability: %+v", initialized)
	}
	if !strings.Contains(initialized.Instructions, `"diagnostic"`) {
		t.Errorf("initialize instructions do not identify diagnostic profile: %q", initialized.Instructions)
	}

	listed, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list tools over stdio: %v", err)
	}
	if listed.NextCursor != "" {
		t.Errorf("unexpected tools pagination cursor %q", listed.NextCursor)
	}
	toolCases := stdioE2EToolCases()
	wantNames := make([]string, 0, len(toolCases))
	for name := range toolCases {
		wantNames = append(wantNames, name)
	}
	sort.Strings(wantNames)

	gotNames := make([]string, 0, len(listed.Tools))
	for _, tool := range listed.Tools {
		gotNames = append(gotNames, tool.Name)
	}
	sort.Strings(gotNames)
	if len(gotNames) != len(wantNames) || strings.Join(gotNames, "\n") != strings.Join(wantNames, "\n") {
		t.Fatalf("stdio tool catalog mismatch\n got (%d): %v\nwant (%d): %v", len(gotNames), gotNames, len(wantNames), wantNames)
	}

	var allResults bytes.Buffer
	_ = json.NewEncoder(&allResults).Encode(initialized)
	_ = json.NewEncoder(&allResults).Encode(listed)
	for _, tool := range listed.Tools {
		name := tool.Name
		toolCase := toolCases[name]
		t.Run(name, func(t *testing.T) {
			result, callErr := session.CallTool(ctx, &mcpsdk.CallToolParams{Name: name, Arguments: toolCase.arguments})
			if callErr != nil {
				t.Fatalf("protocol call failed: %v", callErr)
			}
			if result.IsError {
				t.Fatalf("tool returned IsError: %s", toolResultText(result))
			}
			text := toolResultText(result)
			if strings.TrimSpace(text) == "" || result.StructuredContent == nil {
				t.Fatalf("tool did not return both text and structured content: %+v", result)
			}
			for _, fragment := range toolCase.expectedText {
				if !strings.Contains(text, fragment) {
					t.Errorf("result missing %q: %s", fragment, text)
				}
			}
			encoded, marshalErr := json.Marshal(result)
			if marshalErr != nil {
				t.Fatalf("marshal result for secret scan: %v", marshalErr)
			}
			allResults.Write(encoded)
			allResults.WriteByte('\n')
		})
	}
	if err := session.Close(); err != nil {
		t.Fatalf("clean stdio disconnect: %v; stderr=%s", err, telemetry.String())
	}
	closed = true
	if cmd.ProcessState == nil || !cmd.ProcessState.Exited() || !cmd.ProcessState.Success() {
		t.Fatalf("helper did not exit cleanly after stdin close: %+v", cmd.ProcessState)
	}

	for _, secret := range fixture.secrets {
		if strings.Contains(allResults.String(), secret) {
			t.Errorf("secret %q leaked into an MCP result", secret)
		}
		if strings.Contains(telemetry.String(), secret) {
			t.Errorf("secret %q leaked into server telemetry", secret)
		}
	}

	telemetryCounts := make(map[string]int)
	scanner := bufio.NewScanner(strings.NewReader(telemetry.String()))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// The Go coverage runtime (and, in production, process-level logging)
		// may also write non-MCP diagnostics to stderr. Telemetry itself remains
		// newline-delimited JSON and stdout remains MCP-only.
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var event toolCallTelemetry
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("decode child telemetry: %v\nstderr=%s", err, telemetry.String())
		}
		telemetryCounts[event.Tool]++
		if event.Event != "mcp_tool_call" || event.Profile != ProfileDiagnostic || event.Status != "ok" || event.ResponseBytes <= 0 {
			t.Errorf("unexpected telemetry event: %+v", event)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan child telemetry: %v", err)
	}
	if len(telemetryCounts) != len(wantNames) {
		t.Errorf("telemetry covered %d tools, want %d: %v", len(telemetryCounts), len(wantNames), telemetryCounts)
	}
	for _, name := range wantNames {
		if telemetryCounts[name] != 1 {
			t.Errorf("telemetry count for %q = %d, want 1", name, telemetryCounts[name])
		}
	}

}

func TestMCPStdioSubprocessRejectsOversizedInputFrame(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestMCPStdioHelperProcess$")
	cmd.Env = []string{stdioE2EHelperEnv + "=1"}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("create helper stdin: %v", err)
	}
	cmd.Stdout = io.Discard
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start stdio helper: %v", err)
	}

	// The frame has no newline within the allowed prefix, so the
	// server must reject it before the SDK can decode or queue the raw JSON.
	_, writeErr := io.WriteString(stdin, `"`+strings.Repeat("x", maxStdioInputFrameBytes)+"\n")
	_ = stdin.Close()
	if writeErr != nil && !errors.Is(writeErr, io.ErrClosedPipe) {
		// A platform may report EPIPE directly once the bounded reader closes;
		// that is an expected consequence, so defer the authoritative assertion
		// to the subprocess exit and stderr below.
		t.Logf("oversized frame write ended after server rejection: %v", writeErr)
	}
	waitErr := cmd.Wait()
	if ctx.Err() != nil {
		t.Fatalf("oversized-frame helper did not terminate promptly: %v", ctx.Err())
	}
	if waitErr == nil {
		t.Fatal("oversized input frame unexpectedly produced a clean MCP shutdown")
	}
	if !strings.Contains(stderr.String(), errStdioInputFrameTooLarge.Error()) {
		t.Fatalf("helper stderr does not report the frame limit: %s", stderr.String())
	}
}
