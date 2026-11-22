package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard/cluster"
)

func TestMainnetClusterProjection(t *testing.T) {
	now := time.Now()
	s := clusterTestSnapshot(now, 2)
	s.Profile, s.Network, s.GenesisHash = "mainnet", "mainnet-beta", cluster.MainnetGenesisHash
	s.Providers = []cluster.ProviderStatus{
		{ID: "rpc", SourceURL: "https://api.mainnet-beta.solana.com", State: "fresh", CapturedAt: now, SourceAt: &now, Epoch: s.Epoch, GenesisHash: s.GenesisHash, Binding: "rpc_genesis_before_after", SourceTimeKind: "response", RowCount: "2"},
		{ID: "solstat", State: "unavailable", Reason: "not_collected", CapturedAt: now, RowCount: "0"},
		{ID: "titan", State: "unavailable", Reason: "not_collected", CapturedAt: now, RowCount: "0"},
		{ID: "geoip", SourceURL: "https://db-ip.com/", State: "unavailable", Reason: "not_collected", CapturedAt: now, RowCount: "0"},
		{ID: "puffin", State: "unavailable", Reason: "not_collected", CapturedAt: now, RowCount: "0"},
	}
	for i := range s.Validators {
		s.Validators[i].DirectorySource = "rpc"
		s.Validators[i].Name, s.Validators[i].CountryCode, s.Validators[i].CountrySource = "", "", ""
		s.Validators[i].Version = "4.1.0"
	}
	if err := cluster.Validate(s); err != nil {
		t.Fatalf("invalid Mainnet fixture: %v", err)
	}
	view := projectCluster(&s, "g", "fresh", "", now)
	if view.metadata.Profile != "mainnet" || view.metadata.Network != "mainnet-beta" {
		t.Fatalf("wrong network metadata: %+v", view.metadata)
	}
	var versions clusterVersions
	if err := json.Unmarshal(view.versionsJSON, &versions); err != nil {
		t.Fatal(err)
	}
	if versions.SourceRows != 2 || len(versions.Versions) != 1 || versions.Versions[0].Version != "4.1.0" || versions.Versions[0].Rows != 2 {
		t.Fatalf("wrong version projection: %+v", versions)
	}
	response := httptest.NewRecorder()
	testClusterReader(view).handler(func(*http.Request) bool { return true }, clusterVersionsRoute)(response, httptest.NewRequest("GET", "http://local/api/v1/cluster-versions", nil))
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &versions) != nil || versions.Network != "mainnet-beta" || versions.SourceRows != 2 {
		t.Fatalf("wrong version response: %d %s", response.Code, response.Body.String())
	}
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := cluster.WriteFile(path, s); err != nil {
		t.Fatal(err)
	}
	reader := testClusterReader(nil)
	reader.clockOrigin = now
	reader.poll(filepath.Join(t.TempDir(), "missing.json"), now)
	if actual := reader.latest.Load(); actual.metadata.State != "unavailable" || actual.metadata.Network != "unknown" || actual.metadata.Profile != "unknown" {
		t.Fatalf("missing source should not claim a network: %+v", actual.metadata)
	}
	reader.poll(path, now)
	if actual := reader.latest.Load(); actual.metadata.State != "fresh" || actual.metadata.Network != "mainnet-beta" || len(actual.rows) != 2 {
		t.Fatalf("wrong saved Mainnet snapshot: %+v", actual.metadata)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	reader.poll(path, now.Add(time.Second))
	if actual := reader.latest.Load(); actual.metadata.State != "stale" || actual.metadata.Network != "mainnet-beta" {
		t.Fatalf("wrong state after snapshot loss: %+v", actual.metadata)
	}
}

func TestClusterMapObservedRPCNodes(t *testing.T) {
	now := time.Now()
	for _, profile := range []string{"community", "mainnet"} {
		t.Run(profile, func(t *testing.T) {
			s := clusterTestSnapshot(now, 2)
			if profile == "mainnet" {
				s.Profile, s.Network, s.GenesisHash = "mainnet", "mainnet-beta", cluster.MainnetGenesisHash
				s.Providers = []cluster.ProviderStatus{
					{ID: "rpc", SourceURL: "https://api.mainnet-beta.solana.com", State: "fresh", CapturedAt: now, SourceAt: &now, Epoch: s.Epoch, GenesisHash: s.GenesisHash, Binding: "rpc_genesis_before_after", SourceTimeKind: "response", RowCount: "2"},
					{ID: "solstat", State: "unavailable", Reason: "not_collected", CapturedAt: now, RowCount: "0"},
					{ID: "titan", State: "unavailable", Reason: "not_collected", CapturedAt: now, RowCount: "0"},
					{ID: "geoip", SourceURL: "https://db-ip.com/", State: "unavailable", Reason: "not_collected", CapturedAt: now, RowCount: "0"},
					{ID: "puffin", State: "unavailable", Reason: "not_collected", CapturedAt: now, RowCount: "0"},
				}
				for i := range s.Validators {
					s.Validators[i].DirectorySource = "rpc"
					s.Validators[i].Name, s.Validators[i].CountryCode, s.Validators[i].CountrySource = "", "", ""
				}
			}
			observed := now.Add(-time.Minute)
			s.RPCNodes = &cluster.RPCNodes{Advertised: "0", ObservedAt: observed}
			if err := cluster.Validate(s); err != nil {
				t.Fatal(err)
			}
			view := projectClusterAnchored(&s, "generation", "fresh", "", now, now)
			var body clusterMap
			if json.Unmarshal(view.mapJSON, &body) != nil || body.RPCNodes == nil || body.RPCNodes.Advertised != "0" || !body.RPCNodes.ObservedAt.Equal(observed) {
				t.Fatal("known zero/source time lost")
			}
			first := clusterProjectionRemaining(view.leaseStarted, now)
			if first <= 0 || first > 60000 {
				t.Fatalf("own original lease not capped: %d", first)
			}
			later := projectClusterAnchored(&s, "generation", "fresh", "", now.Add(20*time.Second), now)
			if clusterProjectionRemaining(later.leaseStarted, now.Add(20*time.Second)) > first-19000 {
				t.Fatal("same observation lease renewed")
			}
			expired := projectClusterAnchored(&s, "generation", "fresh", "", now.Add(61*time.Second), now)
			body.RPCNodes = nil
			json.Unmarshal(expired.mapJSON, &body)
			if body.RPCNodes != nil {
				t.Fatal("expired optional observation remains current")
			}
			stale := projectCluster(&s, "generation", "stale", "", now)
			body.RPCNodes = nil
			json.Unmarshal(stale.mapJSON, &body)
			if body.RPCNodes != nil {
				t.Fatal("stale parent supplied RPC count")
			}
			s.RPCNodes = nil
			missing := projectCluster(&s, "generation", "fresh", "", now)
			body.RPCNodes = nil
			json.Unmarshal(missing.mapJSON, &body)
			if body.RPCNodes != nil {
				t.Fatal("absent became zero")
			}
		})
	}
}
