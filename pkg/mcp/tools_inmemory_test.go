package mcp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestGetAccountInfoInputValidation(t *testing.T) {
	var requestCount atomic.Int32
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		var req struct {
			Params []json.RawMessage `json:"params"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &req); err != nil || len(req.Params) != 2 {
			t.Errorf("unexpected request body: %s", body)
		}
		var cfg map[string]any
		json.Unmarshal(req.Params[1], &cfg)
		if cfg["encoding"] != "base64" {
			t.Errorf("expected default encoding base64 on the wire, got %v", cfg["encoding"])
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},"value":{"data":["AQID","base64"],"executable":false,"lamports":9007199254740993,"owner":"11111111111111111111111111111111","rentEpoch":18446744073709551615,"space":3}}}`))
	}))
	defer rpc.Close()
	session := startInMemorySession(t, Config{RPCURL: rpc.URL, MetricsURL: rpc.URL})

	// Short pubkey rejected before any network call.
	text, isErr := callToolText(t, session, "mithril_get_account_info", map[string]any{"pubkey": "short"})
	if !isErr || !strings.Contains(text, "invalid pubkey") {
		t.Errorf("short pubkey: isError=%v text=%q", isErr, text)
	}
	// Unknown encoding rejected.
	text, isErr = callToolText(t, session, "mithril_get_account_info",
		map[string]any{"pubkey": strings.Repeat("1", 32), "encoding": "jsonParsed"})
	if !isErr || !strings.Contains(text, "encoding must be") {
		t.Errorf("bad encoding: isError=%v text=%q", isErr, text)
	}
	if got := requestCount.Load(); got != 0 {
		t.Fatalf("invalid account-info inputs made %d RPC requests, want none", got)
	}
	// Valid call defaults to base64 on the wire (asserted in the handler above).
	if text, isErr = callToolText(t, session, "mithril_get_account_info",
		map[string]any{"pubkey": strings.Repeat("1", 32)}); isErr {
		t.Errorf("valid call failed: %q", text)
	} else if !strings.Contains(text, `"lamports":"9007199254740993"`) || !strings.Contains(text, `"rent_epoch":"18446744073709551615"`) {
		t.Errorf("large integers were not preserved as decimal strings: %q", text)
	}
	if got := requestCount.Load(); got != 1 {
		t.Fatalf("valid account-info input made %d RPC requests, want one", got)
	}
}

func TestRPCBoundaryValidation(t *testing.T) {
	session := startInMemorySession(t, Config{RPCURL: "http://127.0.0.1:1/", MetricsURL: "http://127.0.0.1:1/"})
	if text, isErr := callToolText(t, session, "mithril_get_bank_hash", map[string]any{"slot": uint64(maxJSONExactInteger + 1)}); !isErr || !strings.Contains(text, "exact-integer limit") {
		t.Fatalf("unsafe bank-hash slot: isError=%v text=%q", isErr, text)
	}
	for _, test := range []struct {
		name string
		args map[string]any
		want string
	}{
		{"invalid simulation encoding", map[string]any{"transaction": "AQ==", "encoding": "hex"}, "encoding must be"},
		{"conflicting simulation options", map[string]any{"transaction": "AQ==", "sig_verify": true}, "cannot be combined"},
		{"unsafe simulation slot", map[string]any{"transaction": "AQ==", "min_context_slot": uint64(maxJSONExactInteger + 1)}, "exact-integer limit"},
		{"oversized simulation", map[string]any{"transaction": strings.Repeat("A", maxBase64TransactionChars+1)}, "transaction exceeds"},
		{"invalid base64 simulation", map[string]any{"transaction": "%%%"}, "not valid base64"},
		{"empty simulation", map[string]any{"transaction": ""}, "must not be empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if text, isErr := callToolText(t, session, "mithril_simulate_transaction", test.args); !isErr || !strings.Contains(text, test.want) {
				t.Fatalf("isError=%v text=%q, want %q", isErr, text, test.want)
			}
		})
	}
}

func TestSimulationSingleFlightLeavesOtherToolsAvailable(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	var simulations atomic.Int32
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode RPC request: %v", err)
			return
		}
		if request.Method != "simulateTransaction" {
			t.Errorf("unexpected RPC method %q", request.Method)
			return
		}
		if simulations.Add(1) == 1 {
			entered <- struct{}{}
			<-release
		}
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},"value":{"err":null}}}`)
	}))
	defer rpc.Close()

	session := startInMemorySession(t, Config{RPCURL: rpc.URL, MetricsURL: rpc.URL, MaxConcurrent: 4, RatePerSecond: 100, RateBurst: 100})
	type outcome struct {
		result *mcpsdk.CallToolResult
		err    error
	}
	firstDone := make(chan outcome, 1)
	go func() {
		result, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{
			Name: "mithril_simulate_transaction", Arguments: map[string]any{"transaction": "AQ=="},
		})
		firstDone <- outcome{result: result, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first simulation did not reach the RPC server")
	}

	started := time.Now()
	second, err := session.CallTool(t.Context(), &mcpsdk.CallToolParams{
		Name: "mithril_simulate_transaction", Arguments: map[string]any{"transaction": "AQ=="},
	})
	if err != nil {
		t.Fatalf("second simulation protocol error: %v", err)
	}
	if !second.IsError || !strings.Contains(toolResultText(second), "already running") {
		t.Fatalf("second simulation = isError:%v text:%q, want busy tool error", second.IsError, toolResultText(second))
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("busy simulation took %v, want fail-fast response", elapsed)
	}

	info, err := session.CallTool(t.Context(), &mcpsdk.CallToolParams{Name: "mithril_mcp_info", Arguments: map[string]any{}})
	if err != nil || info.IsError {
		t.Fatalf("info tool while simulation runs = result:%+v error:%v", info, err)
	}
	close(release)
	select {
	case first := <-firstDone:
		if first.err != nil || first.result == nil || first.result.IsError {
			t.Fatalf("first simulation = result:%+v error:%v", first.result, first.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first simulation did not finish after release")
	}
	if got := simulations.Load(); got != 1 {
		t.Fatalf("RPC simulation calls = %d, want one", got)
	}
}

func TestSimulationOutputRedactsEndpointControlledStrings(t *testing.T) {
	const (
		urlSecret   = "SIMULATION_URL_SECRET"
		querySecret = "SIMULATION_QUERY_SECRET"
		tokenSecret = "SIMULATION_TOKEN_SECRET"
		keySecret   = "SIMULATION_KEY_SECRET"
	)
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},"value":{"err":null,"fee":9007199254740993,"logs":["https://rpc.example/`+urlSecret+`?api-key=`+querySecret+`"],"nested":{"authorization":"Bearer `+tokenSecret+`","api_key_`+keySecret+`":0}}}}`)
	}))
	defer rpc.Close()

	session := startInMemorySession(t, Config{RPCURL: rpc.URL, MetricsURL: rpc.URL})
	result, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{
		Name: "mithril_simulate_transaction", Arguments: map[string]any{"transaction": "AQ=="},
	})
	if err != nil || result.IsError {
		t.Fatalf("simulation result = %+v, error = %v", result, err)
	}
	structured, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	text := toolResultText(result)
	for _, output := range []string{text, string(structured)} {
		for _, secret := range []string{urlSecret, querySecret, tokenSecret, keySecret} {
			if strings.Contains(output, secret) {
				t.Fatalf("simulation output leaks %q: %s", secret, output)
			}
		}
	}
	if !strings.Contains(text, "9007199254740993") {
		t.Fatalf("simulation text lost exact integer: %s", text)
	}
}

func TestReadRewardsSlotFoundFlag(t *testing.T) {
	logDir := t.TempDir()
	rewards := filepath.Join(logDir, "rewards")
	if err := os.MkdirAll(rewards, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rewards, "epoch_boundary_voting_rewards_slot_100.json"),
		[]byte(votingRewardFixture(100, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	session := startInMemorySession(t, Config{LogDir: logDir, MetricsURL: "http://127.0.0.1:1/", RPCURL: "http://127.0.0.1:1/"})

	text, isErr := callToolText(t, session, "mithril_read_rewards", map[string]any{"slot": 100})
	if isErr || !strings.Contains(text, `"found":true`) || !strings.Contains(text, `"artifact_state":"partial"`) {
		t.Errorf("slot 100: isError=%v text=%q", isErr, text)
	}
	text, isErr = callToolText(t, session, "mithril_read_rewards", map[string]any{"slot": 999})
	if isErr || !strings.Contains(text, `"found":false`) {
		t.Errorf("slot 999: isError=%v text=%q", isErr, text)
	}
}
