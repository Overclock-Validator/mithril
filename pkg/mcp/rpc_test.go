package mcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DataDog/zstd"
	mrbase58 "github.com/mr-tron/base58"
)

const testHash = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"

func newRPCClientWithResponse(t *testing.T, response string) *mithrilRPCClient {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, response)
	}))
	t.Cleanup(srv.Close)

	client, err := newMithrilRPCClient(srv.URL)
	if err != nil {
		t.Fatalf("new Mithril RPC client: %v", err)
	}
	return client
}

func TestValidateAccountDataEncodingsAndLength(t *testing.T) {
	raw := []byte{1, 2, 3}
	compressed, err := zstd.Compress(nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, value, encoding string
		length                uint64
		wantErr               bool
	}{
		{"base64", base64.StdEncoding.EncodeToString(raw), "base64", 3, false},
		{"base58", mrbase58.Encode(raw), "base58", 3, false},
		{"zstd", base64.StdEncoding.EncodeToString(compressed), "base64+zstd", 3, false},
		{"bad alphabet", "%%%", "base64", 3, true},
		{"wrong length", base64.StdEncoding.EncodeToString(raw), "base64", 2, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateAccountData(test.value, test.encoding, test.length)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateAccountData() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestNewRPCClientRejectsSSRF(t *testing.T) {
	if _, err := newMithrilRPCClient("http://169.254.169.254/"); err == nil {
		t.Error("expected metadata IP to be rejected")
	}
	if _, err := newMithrilRPCClient("http://127.0.0.1:8899"); err != nil {
		t.Errorf("localhost should be allowed: %v", err)
	}
}

func TestGetSlotInfoSemanticLabels(t *testing.T) {
	client := newRPCClientWithResponse(t, `{"jsonrpc":"2.0","id":1,"result":{"absoluteSlot":285,"blockHeight":250,"epoch":1,"slotIndex":5,"slotsInEpoch":100,"transactionCount":99}}`)
	info, err := client.getSlotInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Consistency != "node_reported_non_atomic" || info.Finality != "local_unfinalized" {
		t.Fatalf("slot info semantic labels missing: %+v", info)
	}
}

func TestGetLatestBlockhashParse(t *testing.T) {
	c := newRPCClientWithResponse(t, `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":285},"value":{"blockhash":"`+testHash+`","lastValidBlockHeight":435}}}`)
	bh, err := c.getLatestBlockhash(context.Background())
	if err != nil || bh.Slot != 285 || bh.Blockhash != testHash || bh.LastValidBlockHeight == nil || *bh.LastValidBlockHeight != 435 || bh.Status != "ready" {
		t.Fatalf("getLatestBlockhash = %+v, %v", bh, err)
	}
	if bh.Consistency != "node_reported_non_atomic" || bh.Finality != "local_unfinalized" {
		t.Fatalf("semantic labels missing: %+v", bh)
	}
}

func TestGetLatestBlockhashReportsStartupNotReady(t *testing.T) {
	c := newRPCClientWithResponse(t, `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":0},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":150}}}`)
	bh, err := c.getLatestBlockhash(context.Background())
	if err != nil {
		t.Fatalf("getLatestBlockhash startup response: %v", err)
	}
	if bh.Status != "not_ready" || bh.Blockhash != "" || bh.Slot != 0 || bh.LastValidBlockHeight != nil {
		t.Fatalf("getLatestBlockhash startup response = %+v", bh)
	}
	encoded, err := json.Marshal(bh)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "blockhash") || strings.Contains(string(encoded), "last_valid_block_height") {
		t.Fatalf("not-ready response exposes unavailable values: %s", encoded)
	}
}

func TestSimConfigUsesRPCFieldNamesAndDefaults(t *testing.T) {
	slot := uint64(123)
	got := (simulationConfig{
		Encoding:               "base64",
		SigVerify:              false,
		ReplaceRecentBlockhash: true,
		MinContextSlot:         &slot,
	}).toJSON()
	for key, want := range map[string]any{
		"encoding": "base64", "sigVerify": false,
		"replaceRecentBlockhash": true, "minContextSlot": uint64(123),
	} {
		if got[key] != want {
			t.Errorf("simulation config %s = %v, want %v", key, got[key], want)
		}
	}
	if _, ok := got["commitment"]; ok {
		t.Error("simulation config advertises unsupported commitment semantics")
	}
}

func TestValidateEncodedTransaction(t *testing.T) {
	for _, test := range []struct {
		name        string
		transaction string
		encoding    string
		wantErr     bool
	}{
		{"base64", "AQ==", "base64", false},
		{"base58", "2", "base58", false},
		{"empty", "", "base64", true},
		{"bad base64", "%%%", "base64", true},
		{"bad base58", "0", "base58", true},
		{"unknown encoding", "AQ==", "hex", true},
		{"decoded base64 too large", base64.StdEncoding.EncodeToString(make([]byte, maxTransactionBytes+1)), "base64", true},
		{"decoded base58 too large", strings.Repeat("1", maxTransactionBytes+1), "base58", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateEncodedTransaction(test.transaction, test.encoding)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateEncodedTransaction() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestValidateSimulationResult(t *testing.T) {
	for _, test := range []struct {
		name    string
		result  string
		wantErr bool
	}{
		{"valid", `{"context":{"slot":1},"value":{"err":null,"fee":9007199254740993}}`, false},
		{"not object", `[]`, true},
		{"missing context", `{"value":{}}`, true},
		{"missing value", `{"context":{}}`, true},
		{"null context", `{"context":null,"value":{}}`, true},
		{"missing context slot", `{"context":{},"value":{"err":null}}`, true},
		{"wrong context slot type", `{"context":{"slot":"1"},"value":{"err":null}}`, true},
		{"negative context slot", `{"context":{"slot":-1},"value":{"err":null}}`, true},
		{"missing value err", `{"context":{"slot":1},"value":{}}`, true},
		{"scalar value", `{"context":{"slot":1},"value":1}`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateSimulationResult(json.RawMessage(test.result))
			if (err != nil) != test.wantErr {
				t.Fatalf("validateSimulationResult() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestSimulateTransactionRejectsOversizedInputBeforeFetch(t *testing.T) {
	client, err := newMithrilRPCClient("http://127.0.0.1:1/")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		encoding string
		limit    int
	}{
		{"base64", maxBase64TransactionChars},
		{"base58", maxBase58TransactionChars},
	} {
		if _, err := client.simulateTransaction(context.Background(), strings.Repeat("A", test.limit+1), simulationConfig{Encoding: test.encoding}); err == nil || !strings.Contains(err.Error(), "limit") {
			t.Errorf("oversized %s transaction error = %v", test.encoding, err)
		}
	}
}

func TestRPCCallValidatesResponseEnvelope(t *testing.T) {
	tests := []struct {
		name     string
		response string
		want     string
	}{
		{"missing version", `{"id":1,"result":1}`, "jsonrpc version"},
		{"wrong version", `{"jsonrpc":"1.0","id":1,"result":1}`, "jsonrpc version"},
		{"missing id", `{"jsonrpc":"2.0","result":1}`, "does not match request id 1"},
		{"wrong id", `{"jsonrpc":"2.0","id":2,"result":1}`, "does not match request id 1"},
		{"string id", `{"jsonrpc":"2.0","id":"1","result":1}`, "does not match request id 1"},
		{"both result and error", `{"jsonrpc":"2.0","id":1,"result":1,"error":{"code":-1,"message":"bad"}}`, "exactly one"},
		{"neither result nor error", `{"jsonrpc":"2.0","id":1}`, "exactly one"},
		{"null error", `{"jsonrpc":"2.0","id":1,"error":null}`, "invalid error object"},
		{"error missing code", `{"jsonrpc":"2.0","id":1,"error":{"message":"bad"}}`, "invalid error object"},
		{"error missing message", `{"jsonrpc":"2.0","id":1,"error":{"code":-1}}`, "invalid error object"},
		{"valid error envelope", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"backend failed"}}`, "RPC error -32000: backend failed"},
		{"null result", `{"jsonrpc":"2.0","id":1,"result":null}`, "null result"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newRPCClientWithResponse(t, tt.response)
			if _, err := client.call(context.Background(), "getBlockHeight", []any{}); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestRPCParseErrorsDoNotEchoLargeNumericTokens(t *testing.T) {
	token := strings.Repeat("9", 64*1024)
	client := newRPCClientWithResponse(t, `{"jsonrpc":`+token+`,"id":1,"result":{}}`)
	_, err := client.call(context.Background(), "getBlockHeight", []any{})
	if err == nil || err.Error() != "failed to parse RPC response" || strings.Contains(err.Error(), token[:64]) {
		t.Fatalf("RPC envelope parse error exposed input: %v", err)
	}

	client = newRPCClientWithResponse(t, `{"jsonrpc":"2.0","id":1,"result":{"absoluteSlot":`+token+`,"blockHeight":1,"epoch":1,"slotIndex":1,"slotsInEpoch":1,"transactionCount":1}}`)
	_, err = client.getSlotInfo(context.Background())
	if err == nil || err.Error() != "failed to parse getEpochInfo response" || strings.Contains(err.Error(), token[:64]) {
		t.Fatalf("typed RPC parse error exposed input: %v", err)
	}
}

func TestRPCRequiredFieldValidation(t *testing.T) {
	tests := []struct {
		name   string
		result string
		call   func(*mithrilRPCClient) error
	}{
		{
			name:   "epoch info missing fields",
			result: `{}`,
			call: func(c *mithrilRPCClient) error {
				_, err := c.getSlotInfo(context.Background())
				return err
			},
		},
		{
			name:   "epoch info missing transaction count",
			result: `{"absoluteSlot":285,"blockHeight":250,"epoch":1,"slotIndex":5,"slotsInEpoch":100}`,
			call: func(c *mithrilRPCClient) error {
				_, err := c.getSlotInfo(context.Background())
				return err
			},
		},
		{
			name:   "latest blockhash missing value",
			result: `{"context":{"slot":1}}`,
			call: func(c *mithrilRPCClient) error {
				_, err := c.getLatestBlockhash(context.Background())
				return err
			},
		},
		{
			name:   "latest blockhash missing context slot",
			result: `{"context":{},"value":{"blockhash":"` + testHash + `","lastValidBlockHeight":2}}`,
			call: func(c *mithrilRPCClient) error {
				_, err := c.getLatestBlockhash(context.Background())
				return err
			},
		},
		{
			name:   "latest blockhash missing last valid height",
			result: `{"context":{"slot":1},"value":{"blockhash":"` + testHash + `"}}`,
			call: func(c *mithrilRPCClient) error {
				_, err := c.getLatestBlockhash(context.Background())
				return err
			},
		},
		{
			name:   "account context missing slot",
			result: `{"context":{},"value":null}`,
			call: func(c *mithrilRPCClient) error {
				_, err := c.getAccountInfo(context.Background(), "11111111111111111111111111111111", "base64", accountDataSlice{})
				return err
			},
		},
		{
			name:   "account value missing lamports",
			result: `{"context":{"slot":1},"value":{"data":["","base64"],"executable":false,"owner":"11111111111111111111111111111111","rentEpoch":0,"space":0}}`,
			call: func(c *mithrilRPCClient) error {
				_, err := c.getAccountInfo(context.Background(), "11111111111111111111111111111111", "base64", accountDataSlice{})
				return err
			},
		},
		{
			name:   "account value missing rent epoch",
			result: `{"context":{"slot":1},"value":{"data":["","base64"],"executable":false,"lamports":0,"owner":"11111111111111111111111111111111","space":0}}`,
			call: func(c *mithrilRPCClient) error {
				_, err := c.getAccountInfo(context.Background(), "11111111111111111111111111111111", "base64", accountDataSlice{})
				return err
			},
		},
		{
			name:   "account value missing space",
			result: `{"context":{"slot":1},"value":{"data":["","base64"],"executable":false,"lamports":0,"owner":"11111111111111111111111111111111","rentEpoch":0}}`,
			call: func(c *mithrilRPCClient) error {
				_, err := c.getAccountInfo(context.Background(), "11111111111111111111111111111111", "base64", accountDataSlice{})
				return err
			},
		},
		{
			name:   "invalid bank hash",
			result: `"short"`,
			call: func(c *mithrilRPCClient) error {
				_, err := c.getBankHash(context.Background(), 1)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newRPCClientWithResponse(t, `{"jsonrpc":"2.0","id":1,"result":`+tt.result+`}`)
			if err := tt.call(c); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestGetAccountInfoPreservesUint64AndSlices(t *testing.T) {
	var request struct {
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		if err := json.Unmarshal(body, &request); err != nil {
			t.Error(err)
			return
		}
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":{"context":{"apiVersion":"mithril","slot":7},"value":{"data":["AQID","base64"],"executable":false,"lamports":9007199254740993,"owner":"11111111111111111111111111111111","rentEpoch":18446744073709551615,"space":10000}}}`)
	}))
	defer srv.Close()
	c, err := newMithrilRPCClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.getAccountInfo(context.Background(), "11111111111111111111111111111111", "base64", accountDataSlice{Offset: 8, Length: 64})
	if err != nil {
		t.Fatal(err)
	}
	if got.Value == nil || got.Value.Lamports == nil || *got.Value.Lamports != 9007199254740993 || got.Value.RentEpoch == nil || *got.Value.RentEpoch != ^uint64(0) {
		t.Fatalf("integer precision lost: %+v", got.Value)
	}
	if request.Method != "getAccountInfo" || len(request.Params) != 2 {
		t.Fatalf("unexpected RPC request: %+v", request)
	}
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(request.Params[1], &cfg); err != nil {
		t.Fatal(err)
	}
	var dataSlice accountDataSlice
	if err := json.Unmarshal(cfg["dataSlice"], &dataSlice); err != nil {
		t.Fatal(err)
	}
	if dataSlice.Offset != 8 || dataSlice.Length != 64 {
		t.Fatalf("dataSlice = %+v", dataSlice)
	}
}

func TestGetAccountInfoSanitizesAndBoundsAPIVersion(t *testing.T) {
	secret := "API_VERSION_SECRET"
	apiVersion := "token=" + secret + ";" + strings.Repeat("v", maxRPCAPIVersionBytes+32)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"result": map[string]any{
				"context": map[string]any{"apiVersion": apiVersion, "slot": 7},
				"value":   nil,
			},
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer srv.Close()

	c, err := newMithrilRPCClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.getAccountInfo(context.Background(), "11111111111111111111111111111111", "base64", accountDataSlice{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Context == nil {
		t.Fatal("sanitized response context is nil")
	}
	if strings.Contains(got.Context.APIVersion, secret) {
		t.Fatalf("apiVersion leaked a credential: %q", got.Context.APIVersion)
	}
	if len(got.Context.APIVersion) > maxRPCAPIVersionBytes {
		t.Fatalf("apiVersion length = %d, want at most %d bytes", len(got.Context.APIVersion), maxRPCAPIVersionBytes)
	}
	if !strings.Contains(got.Context.APIVersion, "[REDACTED]") {
		t.Fatalf("apiVersion did not preserve a redaction marker: %q", got.Context.APIVersion)
	}
}

func TestGetAccountInfoRejectsImpossibleAccountSpace(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, fmt.Sprintf(
			`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":7},"value":{"data":["","base64"],"executable":false,"lamports":0,"owner":"11111111111111111111111111111111","rentEpoch":0,"space":%d}}}`,
			maxAccountSpaceBytes+1,
		))
	}))
	defer srv.Close()
	session := startInMemorySession(t, Config{RPCURL: srv.URL, MetricsURL: srv.URL})
	zero := uint64(0)
	text, isError := callToolText(t, session, "mithril_get_account_info", map[string]any{
		"pubkey": "11111111111111111111111111111111", "data_length": zero,
	})
	if !isError || !strings.Contains(text, "account space above Mithril's") {
		t.Fatalf("oversized account space = isError:%v text:%q", isError, text)
	}
}

func TestValidatePubkeyAndHash(t *testing.T) {
	if err := validatePubkey("11111111111111111111111111111111"); err != nil {
		t.Fatalf("system program pubkey must be valid: %v", err)
	}
	for _, bad := range []string{"", strings.Repeat("1", 31), strings.Repeat("z", 32), "0OIl", strings.Repeat("1", 4096)} {
		if err := validatePubkey(bad); err == nil {
			t.Errorf("validatePubkey(%q) unexpectedly passed", bad)
		}
	}
	if err := validateHash(testHash); err != nil {
		t.Fatalf("test hash invalid: %v", err)
	}
	if err := validateHash("11111111111111111111111111111111"); err == nil {
		t.Error("all-zero hash must be rejected")
	}
}
