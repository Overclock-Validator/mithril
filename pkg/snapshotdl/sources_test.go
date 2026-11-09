package snapshotdl

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const (
	knownSourceIdentity   = "7Np41oeYqPefeNQEHSv1UDhYrehxin3NStELsSKCT4K2"
	unknownSourceIdentity = "GdnSyH3YtwcxFvQrVVJMm1JhTS4QVX7MFsX56uJLUfiZ"
)

func clusterDiscoveryServer(t *testing.T, nodes []map[string]any, requests *atomic.Int32) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("unexpected snapshot request to discovery RPC: %s %s", r.Method, r.URL)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		var result any
		switch request.Method {
		case "getSlot":
			result = 1000
		case "getClusterNodes":
			requests.Add(1)
			result = nodes
		default:
			t.Errorf("unexpected RPC method %q", request.Method)
			http.Error(w, "unexpected method", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
	}))
	t.Cleanup(server.Close)
	return server
}

func TestKnownValidatorDiscoveryFiltersIdentitiesAndConfiguredRPCs(t *testing.T) {
	var requests atomic.Int32
	server := clusterDiscoveryServer(t, []map[string]any{
		{"pubkey": unknownSourceIdentity, "rpc": "127.0.0.1:1234", "version": "4.0.0"},
		{"pubkey": knownSourceIdentity, "rpc": "127.0.0.1:5678", "version": "3.1.0"},
		{"pubkey": knownSourceIdentity, "rpc": nil},
	}, &requests)
	cfg := DefaultSnapshotConfig()
	cfg.RPCAddresses = []string{server.URL}
	cfg.KnownValidators = []string{knownSourceIdentity}
	cfg.OnlyKnownRPC = true
	nodes, err := discoverSnapshotRPCNodes(context.Background(), cfg, cfg.toInternalConfig(""), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].Address != "127.0.0.1:5678" || nodes[0].Version != "3.1.0" || nodes[0].IsStatic {
		t.Fatalf("eligible nodes = %+v; only the known validator's advertised RPC is allowed", nodes)
	}
	if requests.Load() != 1 {
		t.Fatalf("cluster discovery requests = %d, want 1", requests.Load())
	}
}

func TestKnownValidatorDiscoveryTriesNextDiscoveryEndpoint(t *testing.T) {
	var firstRequests, secondRequests atomic.Int32
	first := clusterDiscoveryServer(t, []map[string]any{
		{"pubkey": unknownSourceIdentity, "rpc": "127.0.0.1:1234"},
	}, &firstRequests)
	second := clusterDiscoveryServer(t, []map[string]any{
		{"pubkey": knownSourceIdentity, "rpc": "127.0.0.1:5678", "version": "3.1.0"},
	}, &secondRequests)
	cfg := DefaultSnapshotConfig()
	cfg.RPCAddresses = []string{first.URL, second.URL}
	cfg.KnownValidators = []string{knownSourceIdentity}
	cfg.OnlyKnownRPC = true
	nodes, err := discoverSnapshotRPCNodes(context.Background(), cfg, cfg.toInternalConfig(""), first.URL)
	if err != nil || len(nodes) != 1 || nodes[0].Address != "127.0.0.1:5678" {
		t.Fatalf("fallback discovery: nodes=%+v, err=%v", nodes, err)
	}
	if firstRequests.Load() != 1 || secondRequests.Load() != 1 {
		t.Fatalf("discovery request counts = %d, %d; want 1, 1", firstRequests.Load(), secondRequests.Load())
	}
}

func TestKnownValidatorRestrictionFailsClosedAcrossSnapshotPaths(t *testing.T) {
	var sourceRequests, discoveryRequests atomic.Int32
	untrustedSource := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceRequests.Add(1)
		http.Error(w, "untrusted snapshot source", http.StatusForbidden)
	}))
	defer untrustedSource.Close()
	discovery := clusterDiscoveryServer(t, []map[string]any{
		{"pubkey": unknownSourceIdentity, "rpc": untrustedSource.URL, "version": "3.1.0"},
	}, &discoveryRequests)
	cfg := DefaultSnapshotConfig()
	cfg.RPCAddresses = []string{discovery.URL}
	cfg.KnownValidators = []string{knownSourceIdentity}
	cfg.OnlyKnownRPC = true
	tests := []struct {
		name string
		run  func() error
	}{
		{"full URL", func() error {
			_, _, _, err := GetSnapshotURL(context.Background(), cfg)
			return err
		}},
		{"full URL with info", func() error {
			_, err := GetSnapshotURLWithInfo(context.Background(), cfg)
			return err
		}},
		{"full download", func() error {
			_, _, _, err := DownloadSnapshotWithConfig(context.Background(), t.TempDir(), cfg)
			return err
		}},
		{"incremental download", func() error {
			_, _, _, err := DownloadIncrementalSnapshotWithConfig(t.TempDir(), 1000, 900, cfg)
			return err
		}},
		{"incremental same-source and fallback", func() error {
			_, _, _, err := GetIncrementalSnapshotURL(untrustedSource.URL+"/snapshot-900-hash.tar.zst", 1000, 900, cfg)
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); err == nil || !strings.Contains(err.Error(), "no known-validator snapshot RPC nodes") {
				t.Fatalf("expected closed source restriction, got %v", err)
			}
			if got := sourceRequests.Load(); got != 0 {
				t.Fatalf("contacted untrusted snapshot source %d times", got)
			}
		})
	}
	if got := discoveryRequests.Load(); got != int32(len(tests)) {
		t.Fatalf("discovery requests = %d, want %d", got, len(tests))
	}
}

func TestValidateKnownValidators(t *testing.T) {
	for _, tt := range []struct {
		identities []string
		onlyKnown  bool
		wantError  bool
	}{
		{nil, false, false},
		{nil, true, true},
		{[]string{knownSourceIdentity}, true, false},
		{[]string{"not-a-pubkey"}, true, true},
		{[]string{""}, true, true},
	} {
		t.Run(fmt.Sprintf("%v/%v", tt.identities, tt.onlyKnown), func(t *testing.T) {
			cfg := SnapshotConfig{KnownValidators: tt.identities, OnlyKnownRPC: tt.onlyKnown}
			if err := cfg.ValidateKnownValidators(); (err != nil) != tt.wantError {
				t.Fatalf("ValidateKnownValidators() = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}
