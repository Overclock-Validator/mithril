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
		case "getIdentity":
			result = map[string]any{"identity": unknownSourceIdentity}
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

func TestKnownValidatorDiscoveryFallsBackToVerifiedConfiguredIdentity(t *testing.T) {
	for _, tt := range []struct {
		name          string
		identity      any
		identityError bool
		version       any
		versionError  bool
		wantVersion   bool
		wantNode      bool
	}{
		{name: "known identity", identity: knownSourceIdentity, version: "4.4.1", wantVersion: true, wantNode: true},
		{name: "unknown identity", identity: unknownSourceIdentity},
		{name: "invalid identity", identity: "not-a-pubkey"},
		{name: "missing identity"},
		{name: "identity RPC failure", identityError: true},
		{name: "missing version", identity: knownSourceIdentity, wantVersion: true},
		{name: "empty version", identity: knownSourceIdentity, version: "", wantVersion: true},
		{name: "version RPC failure", identity: knownSourceIdentity, versionError: true, wantVersion: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var identityRequests, versionRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.RequestURI() != "/validator/rpc?route=snapshot" {
					t.Errorf("configured endpoint path/query changed: %s", r.URL)
				}
				var request struct {
					ID     any    `json:"id"`
					Method string `json:"method"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Errorf("unexpected request before source admission: %s %s", r.Method, r.URL)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				response := map[string]any{"jsonrpc": "2.0", "id": request.ID}
				rpcError := false
				switch request.Method {
				case "getClusterNodes":
					rpcError = true // Agave minimal RPC omits this method.
				case "getIdentity":
					identityRequests.Add(1)
					rpcError = tt.identityError
					if tt.identity != nil {
						response["result"] = map[string]any{"identity": tt.identity}
					}
				case "getVersion":
					versionRequests.Add(1)
					rpcError = tt.versionError
					if tt.version != nil {
						response["result"] = map[string]any{"solana-core": tt.version}
					}
				default:
					t.Errorf("unexpected RPC method before source admission: %s", request.Method)
				}
				if rpcError {
					response["error"] = map[string]any{"code": -32601, "message": "Method not found"}
					delete(response, "result")
				} else if _, ok := response["result"]; !ok {
					response["result"] = nil
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			endpoint := server.URL + "/validator/rpc?route=snapshot"
			cfg := DefaultSnapshotConfig()
			// The preferred URL and repeated configuration must not create
			// duplicate candidates or repeated identity checks.
			cfg.RPCAddresses = []string{endpoint, endpoint}
			cfg.KnownValidators = []string{knownSourceIdentity}
			cfg.OnlyKnownRPC = true
			nodes, err := discoverSnapshotRPCNodes(context.Background(), cfg, cfg.toInternalConfig(""), endpoint)
			if tt.wantNode {
				if err != nil || len(nodes) != 1 || nodes[0].Address != endpoint || nodes[0].Version != "4.4.1" || nodes[0].IsStatic {
					t.Fatalf("verified configured source = %+v, err=%v", nodes, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "no known-validator snapshot RPC nodes") || len(nodes) != 0 {
				t.Fatalf("source restriction failed open: nodes=%+v, err=%v", nodes, err)
			}
			if identityRequests.Load() != 1 {
				t.Fatalf("identity requests = %d, want 1", identityRequests.Load())
			}
			wantVersionRequests := int32(0)
			if tt.wantVersion {
				wantVersionRequests = 1
			}
			if versionRequests.Load() != wantVersionRequests {
				t.Fatalf("version requests = %d, want %d", versionRequests.Load(), wantVersionRequests)
			}
		})
	}
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
