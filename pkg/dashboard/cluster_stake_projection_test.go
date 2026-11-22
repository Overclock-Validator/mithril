package dashboard

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard/cluster"
)

func TestClusterMapExactRPCStakeCoverage(t *testing.T) {
	now := time.Now()
	fixture := func() cluster.Snapshot {
		s := clusterTestSnapshot(now, 3)
		s.Profile, s.Network, s.GenesisHash = "mainnet", "mainnet-beta", cluster.MainnetGenesisHash
		s.Providers = []cluster.ProviderStatus{
			{ID: "rpc", SourceURL: "https://api.mainnet-beta.solana.com", State: "fresh", CapturedAt: now, SourceAt: &now, Epoch: s.Epoch, GenesisHash: s.GenesisHash, Binding: "rpc_genesis_before_after", SourceTimeKind: "response", RowCount: "3"},
			{ID: "solstat", State: "unavailable", Reason: "not_collected", CapturedAt: now, RowCount: "0"},
			{ID: "titan", State: "unavailable", Reason: "not_collected", CapturedAt: now, RowCount: "0"},
			{ID: "geoip", SourceURL: "https://db-ip.com/", State: "unavailable", Reason: "not_collected", CapturedAt: now, RowCount: "0"},
			{ID: "puffin", State: "unavailable", Reason: "not_collected", CapturedAt: now, RowCount: "0"},
		}
		for i := range s.Validators {
			row := &s.Validators[i]
			// Every vote account shares one identity, including the delinquent row.
			row.Identity = s.Validators[0].Identity
			row.DirectorySource, row.Name, row.CountryCode, row.CountrySource = "rpc", "", "", ""
			stake, delinquent := "18446744073.709551615", i == 2
			if delinquent {
				stake = "0.000000001"
			}
			row.StakeSOL, row.Delinquent = &stake, &delinquent
		}
		return s
	}
	read := func(s cluster.Snapshot, state string, at time.Time) *struct {
		Current string `json:"current_lamports"`
		Bad     string `json:"delinquent_lamports"`
	} {
		t.Helper()
		var body struct {
			TotalNodes int `json:"total_nodes"`
			Stake      *struct {
				Current string `json:"current_lamports"`
				Bad     string `json:"delinquent_lamports"`
			} `json:"rpc_stake"`
		}
		if err := json.Unmarshal(projectCluster(&s, "generation", state, "", at).mapJSON, &body); err != nil {
			t.Fatal(err)
		}
		if body.Stake != nil && body.TotalNodes != 1 {
			t.Fatalf("vote-account stake aggregation changed unique identity count: got %d nodes, want 1", body.TotalNodes)
		}
		return body.Stake
	}
	s := fixture()
	if err := cluster.Validate(s); err != nil {
		t.Fatal(err)
	}
	if got := read(s, "fresh", now); got == nil || got.Current != "36893488147419103230" || got.Bad != "1" {
		t.Fatalf("exact complete RPC stake = %+v, want current 36893488147419103230, delinquent 1", got)
	}
	for name, change := range map[string]func(*cluster.Snapshot){
		"missing stake":      func(s *cluster.Snapshot) { s.Validators[0].StakeSOL = nil },
		"missing class":      func(s *cluster.Snapshot) { s.Validators[0].Delinquent = nil },
		"incomplete rows":    func(s *cluster.Snapshot) { s.Providers[0].RowCount = "4" },
		"wrong row source":   func(s *cluster.Snapshot) { s.Validators[0].DirectorySource = "solstat" },
		"wrong RPC binding":  func(s *cluster.Snapshot) { s.Providers[0].Binding = "unverified" },
		"fractional lamport": func(s *cluster.Snapshot) { value := "0.0000000001"; s.Validators[0].StakeSOL = &value },
		"per-row overflow":   func(s *cluster.Snapshot) { value := "18446744073.709551616"; s.Validators[0].StakeSOL = &value },
	} {
		t.Run(name, func(t *testing.T) {
			s := fixture()
			change(&s)
			if got := read(s, "fresh", now); got != nil {
				t.Fatalf("incomplete or invalid RPC stake was projected: %+v", got)
			}
		})
	}
	if read(fixture(), "stale", now) != nil || read(fixture(), "fresh", now.Add(cluster.MaxAge+time.Second)) != nil || read(clusterTestSnapshot(now, 3), "fresh", now) != nil {
		t.Fatal("stale or community data projected as exact RPC stake")
	}
}
