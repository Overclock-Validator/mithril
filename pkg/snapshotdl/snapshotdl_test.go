package snapshotdl

import (
	"testing"

	snapconfig "github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/config"
	snaprpc "github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/rpc"
)

func TestSnapshotNodeBlacklistMatchesCommonEndpointForms(t *testing.T) {
	blacklist := newSnapshotNodeBlacklist([]string{
		"http://203.0.113.10:8899/",
		"198.51.100.24:8899",
		"bad-snapshot-node.example.com",
	})

	tests := []struct {
		name     string
		endpoint string
		want     bool
	}{
		{
			name:     "full rpc url",
			endpoint: "http://203.0.113.10:8899",
			want:     true,
		},
		{
			name:     "snapshot url from same source",
			endpoint: "https://203.0.113.10:8899/snapshot-123-abc.tar.zst",
			want:     true,
		},
		{
			name:     "host port",
			endpoint: "http://198.51.100.24:8899",
			want:     true,
		},
		{
			name:     "hostname",
			endpoint: "http://bad-snapshot-node.example.com:8899",
			want:     true,
		},
		{
			name:     "allowed node",
			endpoint: "http://good-snapshot-node.example.com:8899",
			want:     false,
		},
		{
			name:     "empty endpoint",
			endpoint: "",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := blacklist.contains(tt.endpoint); got != tt.want {
				t.Fatalf("contains(%q) = %v, want %v", tt.endpoint, got, tt.want)
			}
		})
	}
}

func TestFilterSnapshotRPCNodes(t *testing.T) {
	blacklist := newSnapshotNodeBlacklist([]string{"203.0.113.10", "bad.example.com"})
	nodes := []snaprpc.RPCNode{
		{Address: "http://203.0.113.10:8899"},
		{Address: "http://good.example.com:8899"},
		{Address: "http://bad.example.com:8899"},
	}

	filtered, skipped := filterSnapshotRPCNodes(nodes, blacklist)
	if skipped != 2 {
		t.Fatalf("skipped = %d, want 2", skipped)
	}
	if len(filtered) != 1 {
		t.Fatalf("len(filtered) = %d, want 1", len(filtered))
	}
	if filtered[0].Address != "http://good.example.com:8899" {
		t.Fatalf("remaining node = %q, want good.example.com", filtered[0].Address)
	}
}

func TestAddConfiguredSnapshotRPCNodes(t *testing.T) {
	nodes := []snaprpc.RPCNode{
		{Address: "http://public.example.com:8899", Version: "cluster"},
	}

	got, added := addConfiguredSnapshotRPCNodes(nodes, "https://rpc.example.com", []string{
		"https://rpc.example.com",
		"http://public.example.com:8899",
	})
	if added != 1 {
		t.Fatalf("added = %d, want 1", added)
	}
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	if got[1].Address != "https://rpc.example.com" {
		t.Fatalf("configured address = %q", got[1].Address)
	}
	if got[1].Version != "999.0.0-configured" {
		t.Fatalf("configured version = %q", got[1].Version)
	}
	if !got[1].IsStatic {
		t.Fatalf("configured RPC was not marked static")
	}
}

func TestFilterByIncrementalBaseMatchAllowsGenesisFullWithBaseZeroIncremental(t *testing.T) {
	results := []snaprpc.NodeResult{
		{
			RPC:      "http://young-testnet.example.com:8899",
			FullURL:  "http://young-testnet.example.com:8899/snapshot-0-hash.tar.zst",
			FullSlot: 0,
			HasInc:   true,
			IncBase:  0,
			IncSlot:  21102,
			IncURL:   "http://young-testnet.example.com:8899/incremental-snapshot-0-21102-hash.tar.zst",
		},
	}

	filtered, stats := filterByIncrementalBaseMatch(results, 0, 0) // freshness disabled: legacy base-match semantics
	if len(filtered) != 1 {
		t.Fatalf("len(filtered) = %d, want 1", len(filtered))
	}
	if stats.totalWithFull != 1 || stats.totalWithFullSlotZero != 1 {
		t.Fatalf("full stats = %d/%d, want 1/1", stats.totalWithFull, stats.totalWithFullSlotZero)
	}
	if stats.matchingFullSlots != 1 {
		t.Fatalf("matchingFullSlots = %d, want 1", stats.matchingFullSlots)
	}
}

func TestNormalizeGenesisFullSnapshotsForRankingUsesSyntheticSlot(t *testing.T) {
	results := []snaprpc.NodeResult{
		{
			RPC:      "http://young-testnet.example.com:8899",
			FullURL:  "http://young-testnet.example.com:8899/snapshot-0-hash.tar.zst",
			FullSlot: 0,
			HasInc:   true,
			IncBase:  0,
		},
	}

	normalized := normalizeGenesisFullSnapshotsForRanking(results)
	if results[0].FullSlot != 0 {
		t.Fatalf("original FullSlot mutated to %d", results[0].FullSlot)
	}
	if normalized[0].FullSlot != 1 {
		t.Fatalf("normalized FullSlot = %d, want synthetic slot 1", normalized[0].FullSlot)
	}
}

func TestRelaxedSnapshotProbeConfigUsesTinyStage1Sample(t *testing.T) {
	cfg := DefaultSnapshotConfig().toInternalConfig("")
	relaxed := relaxedSnapshotProbeConfig(cfg)

	if relaxed.Stage1WarmKiB != 0 {
		t.Fatalf("Stage1WarmKiB = %d, want 0", relaxed.Stage1WarmKiB)
	}
	if relaxed.Stage1WindowKiB != 64 {
		t.Fatalf("Stage1WindowKiB = %d, want 64", relaxed.Stage1WindowKiB)
	}
	if relaxed.Stage1Windows != 1 {
		t.Fatalf("Stage1Windows = %d, want 1", relaxed.Stage1Windows)
	}
}

func TestRelaxedIncrementalSnapshotProbeConfigThreshold(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	tests := []struct {
		name string
		in   int
		want int
	}{
		{name: "ten thousand floor", in: 2_000, want: 10_000},
		{name: "observed production threshold", in: 10_000, want: 20_000},
		{name: "larger custom threshold", in: 20_000, want: 40_000},
		{name: "overflow saturates", in: maxInt, want: maxInt},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultSnapshotConfig().toInternalConfig("")
			cfg.IncrementalThreshold = tt.in
			cfg.Stage1Concurrency = 37
			cfg.TCPTimeoutMs = 1_234

			relaxed := relaxedIncrementalSnapshotProbeConfig(cfg)
			if relaxed.IncrementalThreshold != tt.want {
				t.Fatalf("IncrementalThreshold = %d, want %d", relaxed.IncrementalThreshold, tt.want)
			}
			if relaxed.Stage1Concurrency != cfg.Stage1Concurrency {
				t.Fatalf("Stage1Concurrency = %d, want preserved value %d", relaxed.Stage1Concurrency, cfg.Stage1Concurrency)
			}
			if relaxed.TCPTimeoutMs != cfg.TCPTimeoutMs {
				t.Fatalf("TCPTimeoutMs = %d, want preserved value %d", relaxed.TCPTimeoutMs, cfg.TCPTimeoutMs)
			}
			if cfg.IncrementalThreshold != tt.in {
				t.Fatalf("input config mutated: IncrementalThreshold = %d, want %d", cfg.IncrementalThreshold, tt.in)
			}
		})
	}
}

func TestRankIncrementalSnapshotNodesStrictSuccessDoesNotRetry(t *testing.T) {
	cfg := DefaultSnapshotConfig().toInternalConfig("")
	cfg.IncrementalThreshold = 10_000
	stats := snaprpc.NewProbeStats()
	results := []snaprpc.NodeResult{{
		RPC:      "http://snapshot.example.com:8899",
		FullSlot: 408_345,
		HasInc:   true,
		IncBase:  408_345,
		IncSlot:  420_408,
	}}
	wantBest := []string{results[0].RPC}
	wantRanked := []snaprpc.RankedNode{{Result: results[0]}}
	calls := 0

	best, ranked := rankIncrementalSnapshotNodes(
		results, cfg, stats, 408_345, 420_660,
		func(
			gotResults []snaprpc.NodeResult,
			gotCfg snapconfig.Config,
			gotStats *snaprpc.ProbeStats,
			minSlot int64,
			referenceSlot int,
		) ([]string, []snaprpc.RankedNode) {
			calls++
			if gotStats != stats {
				t.Fatalf("strict stats pointer changed")
			}
			if gotCfg.IncrementalThreshold != cfg.IncrementalThreshold {
				t.Fatalf("strict IncrementalThreshold = %d, want %d", gotCfg.IncrementalThreshold, cfg.IncrementalThreshold)
			}
			if minSlot != 408_345 || referenceSlot != 420_660 {
				t.Fatalf("slot arguments = (%d, %d), want (408345, 420660)", minSlot, referenceSlot)
			}
			if len(gotResults) != 1 || gotResults[0].RPC != results[0].RPC {
				t.Fatalf("unexpected ranking results input: %+v", gotResults)
			}
			return wantBest, wantRanked
		},
	)

	if calls != 1 {
		t.Fatalf("sorter calls = %d, want 1", calls)
	}
	if len(best) != 1 || best[0] != wantBest[0] {
		t.Fatalf("best nodes = %v, want %v", best, wantBest)
	}
	if len(ranked) != 1 || ranked[0].Result.RPC != wantRanked[0].Result.RPC {
		t.Fatalf("ranked nodes = %+v, want %+v", ranked, wantRanked)
	}
}

func TestRankIncrementalSnapshotNodesStrictFailureRelaxedSuccess(t *testing.T) {
	cfg := DefaultSnapshotConfig().toInternalConfig("")
	cfg.MaxRTTMs = 200
	cfg.FullThreshold = 150_000
	cfg.IncrementalThreshold = 10_000
	cfg.Stage1TimeoutMS = 3_000
	cfg.Stage1WarmKiB = 512
	cfg.Stage1WindowKiB = 512
	cfg.Stage1Windows = 4
	cfg.Stage1Concurrency = 37
	cfg.Stage2TopK = 8
	cfg.Stage2MinRatio = 0.6
	cfg.TCPTimeoutMs = 1_234
	strictStats := snaprpc.NewProbeStats()
	results := []snaprpc.NodeResult{{
		RPC:      "http://snapshot.example.com:8899",
		FullSlot: 408_345,
		HasInc:   true,
		IncBase:  408_345,
		IncSlot:  420_408,
	}}
	wantBest := []string{results[0].RPC}
	wantRanked := []snaprpc.RankedNode{{Result: results[0]}}
	calls := 0

	best, ranked := rankIncrementalSnapshotNodes(
		results, cfg, strictStats, 408_345, 420_660,
		func(
			_ []snaprpc.NodeResult,
			gotCfg snapconfig.Config,
			gotStats *snaprpc.ProbeStats,
			_ int64,
			_ int,
		) ([]string, []snaprpc.RankedNode) {
			calls++
			switch calls {
			case 1:
				if gotStats != strictStats {
					t.Fatalf("strict stats pointer changed")
				}
				if gotCfg.IncrementalThreshold != 10_000 || gotCfg.MaxRTTMs != 200 {
					t.Fatalf("strict config was unexpectedly relaxed: %+v", gotCfg)
				}
				return nil, nil
			case 2:
				if gotStats == nil {
					t.Fatal("relaxed stats is nil")
				}
				if gotStats == strictStats {
					t.Fatal("relaxed pass reused strict stats")
				}
				if gotCfg.MaxRTTMs != 1_000 || gotCfg.FullThreshold != 1_000_000 || gotCfg.IncrementalThreshold != 20_000 {
					t.Fatalf("relaxed filters = rtt:%d full:%d incremental:%d, want 1000/1000000/20000",
						gotCfg.MaxRTTMs, gotCfg.FullThreshold, gotCfg.IncrementalThreshold)
				}
				if gotCfg.Stage1TimeoutMS != 8_000 || gotCfg.Stage1WarmKiB != 0 || gotCfg.Stage1WindowKiB != 64 || gotCfg.Stage1Windows != 1 {
					t.Fatalf("relaxed Stage 1 = timeout:%d sample:%d+%dx%dKiB, want 8000 and 0+1x64KiB",
						gotCfg.Stage1TimeoutMS, gotCfg.Stage1WarmKiB, gotCfg.Stage1Windows, gotCfg.Stage1WindowKiB)
				}
				if gotCfg.Stage2TopK != 16 || gotCfg.Stage2MinRatio != 0 {
					t.Fatalf("relaxed Stage 2 = top-k:%d min-ratio:%f, want 16/0", gotCfg.Stage2TopK, gotCfg.Stage2MinRatio)
				}
				if gotCfg.Stage1Concurrency != cfg.Stage1Concurrency || gotCfg.TCPTimeoutMs != cfg.TCPTimeoutMs {
					t.Fatalf("unrelated settings changed: concurrency=%d tcp-timeout=%d", gotCfg.Stage1Concurrency, gotCfg.TCPTimeoutMs)
				}
				return wantBest, wantRanked
			default:
				t.Fatalf("unexpected sorter call %d", calls)
				return nil, nil
			}
		},
	)

	if calls != 2 {
		t.Fatalf("sorter calls = %d, want 2", calls)
	}
	if len(best) != 1 || best[0] != wantBest[0] {
		t.Fatalf("best nodes = %v, want %v", best, wantBest)
	}
	if len(ranked) != 1 || ranked[0].Result.RPC != wantRanked[0].Result.RPC {
		t.Fatalf("ranked nodes = %+v, want %+v", ranked, wantRanked)
	}
	if cfg.IncrementalThreshold != 10_000 || cfg.MaxRTTMs != 200 {
		t.Fatalf("input config was mutated: %+v", cfg)
	}
}

func TestRankIncrementalSnapshotNodesNormalizesNilStrictStats(t *testing.T) {
	cfg := DefaultSnapshotConfig().toInternalConfig("")
	results := []snaprpc.NodeResult{{RPC: "http://snapshot.example.com:8899"}}
	calls := 0

	best, ranked := rankIncrementalSnapshotNodes(
		results, cfg, nil, 1, 2,
		func(
			_ []snaprpc.NodeResult,
			_ snapconfig.Config,
			gotStats *snaprpc.ProbeStats,
			_ int64,
			_ int,
		) ([]string, []snaprpc.RankedNode) {
			calls++
			if gotStats == nil {
				t.Fatal("strict stats is nil")
			}
			return []string{results[0].RPC}, []snaprpc.RankedNode{{Result: results[0]}}
		},
	)

	if calls != 1 || len(best) != 1 || len(ranked) != 1 {
		t.Fatalf("calls/best/ranked = %d/%d/%d, want 1/1/1", calls, len(best), len(ranked))
	}
}
