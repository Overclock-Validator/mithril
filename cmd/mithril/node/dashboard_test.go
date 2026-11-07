package node

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

func TestReplayProjectionCopiesChainActivity(t *testing.T) {
	at := time.Now().UTC()
	p := replay.ProgressSnapshot{ChainActivity: replay.ChainActivitySummary{SampledAt: at, Available: true, AnchorSlot: 0, LastSlot: 4,
		StartTime: 100, EndTime: 112, Blocks: 3, Transactions: sealevel.BankTransactionCounts{Total: 1<<53 + 1, NonVote: 5, Failed: 3, NonVoteFailed: 2, ComputeUnits: 1<<53 + 2}}}
	r := replayMeasurement(p)
	want := dashboard.ChainActivity{SampledAt: at, Available: true, AnchorSlot: 0, LastSlot: 4, StartTime: 100, EndTime: 112,
		Blocks: 3, Transactions: 1<<53 + 1, NonVoteTransactions: 5, FailedTransactions: 3, FailedNonVote: 2, ComputeUnits: 1<<53 + 2}
	if r.ChainActivity == nil || *r.ChainActivity != want {
		t.Fatalf("bad activity mapping: %+v", r.ChainActivity)
	}
	p.ChainActivity.Transactions.Total = 0
	if *r.ChainActivity != want {
		t.Fatal("published activity changed with its producer")
	}
	p.ChainActivity = replay.ChainActivitySummary{}
	if replayMeasurement(p).ChainActivity != nil {
		t.Fatal("unobserved activity was fabricated")
	}
}

func TestReplayProjectionPreservesAllCompletedWork(t *testing.T) {
	now := time.Now().UTC()
	p := replay.ProgressSnapshot{Attempt: 2, StartedAt: now.Add(-time.Minute), UpdatedAt: now,
		Phase: "running", ExecutedBlocks: 4, ExecutedTransactions: 1<<53 + 1,
		ExecutedComputeUnits: 1<<53 + 2, Rewinds: 1, WorkCountersSaturated: true,
		HasCompleted: true, Completed: replay.CompletedBlock{At: now, Slot: 9, Transactions: 2, ComputeUnits: 3}}
	r := replayMeasurement(p)
	if r.ExecutedTransactions != p.ExecutedTransactions || r.ExecutedComputeUnits != p.ExecutedComputeUnits || !r.WorkCountersSaturated || r.Completed.Transactions != 2 || r.Rewinds != 1 {
		t.Fatalf("work totals changed or confused with latest block: %+v", r)
	}
	if r.DurableSlot != nil || r.ReadyAt != nil || r.ResumeParent != nil {
		t.Fatal("unobserved progress was fabricated")
	}
	p.ExecutedTransactions, p.Completed.Transactions = 0, 0
	if r.ExecutedTransactions != 1<<53+1 || r.Completed.Transactions != 2 {
		t.Fatal("published replay sample changed with source")
	}
	b, err := json.Marshal(r)
	if err != nil || !strings.Contains(string(b), `"executed_transactions":"9007199254740993"`) || !strings.Contains(string(b), `"executed_compute_units":"9007199254740994"`) {
		t.Fatalf("work counter precision: %s, %v", b, err)
	}
}

func TestBlockSourceProjectionPreservesUnknownAndSession(t *testing.T) {
	now := time.Date(2026, 10, 30, 9, 30, 0, 123, time.FixedZone("offset", 19800))
	s := blockstream.NetworkSnapshot{SampledAt: now, SourceStartedAt: now, ConfiguredSource: "turbine", NextSlot: 1<<53 + 1}
	m := blockSourceMeasurement(s)
	if m.Turbine != nil || m.RepairCatchupFrom != nil || m.RepairCatchupUntil != nil || m.RPCBlockFetchAllowed {
		t.Fatal("unknown receiver or inactive repair range was fabricated")
	}
	s.RepairCatchupFrom, s.RepairCatchupUntil = 90, 100
	s.Turbine = &blockstream.TurbineObservation{SampledAt: now, StartedAt: now, Session: 2, Packets: 1<<53 + 1}
	m = blockSourceMeasurement(s)
	if m.Turbine.Repair != nil || m.Turbine.LastPacketAt != nil || m.Turbine.Session != 2 || *m.RepairCatchupFrom != 90 || *m.RepairCatchupUntil != 100 {
		t.Fatalf("invalid receiver projection: %+v", m)
	}
	s.Turbine.RepairEnabled, s.Turbine.LastPacketAt = true, now
	s.Turbine.RepairRequests, s.Turbine.RepairResponses = 9, 4
	s.Turbine.Frontiers = turbine.ShredFrontiers{HasObserved: true, HasReconstructed: true, ObservedSlot: 1<<53 + 1}
	s.Turbine.BlocksEmitted, s.Turbine.RecoveredData, s.Turbine.AssemblyErrors, s.Turbine.ActiveSlots = 20, 21, 22, 23
	s.Turbine.MissingLeaders, s.Turbine.ShredVersionMismatch = 27, 1<<53+3
	s.Turbine.RepairOutstanding, s.Turbine.RepairPeers, s.Turbine.RepairResponding = 24, 25, 26
	next := blockSourceMeasurement(s)
	if m.Turbine.Repair != nil || next.Turbine.Repair.Requests != 9 || next.Turbine.Repair.Responses != 4 {
		t.Fatal("sample was mutated or repair counters changed")
	}
	d := next.Turbine.Diagnostics
	if m.Turbine.Diagnostics.ObservedHeaderSlot != nil || d.ObservedHeaderSlot == nil || *d.ObservedHeaderSlot != 1<<53+1 || d.ReconstructedSlot == nil || *d.ReconstructedSlot != 0 ||
		d.BlockEmissionAttempts != 20 || d.RecoveredDataShreds != 21 || d.AssemblyErrors != 22 || d.ActiveSlots != 23 ||
		d.Admission == nil || d.Admission.MissingLeaders != 27 || d.Admission.ShredVersionMismatch != 1<<53+3 ||
		d.RepairOutstanding != 24 || d.RepairPeers != 25 || d.RepairRespondingPeers != 26 {
		t.Fatalf("incorrect receiver diagnostic mapping: %+v", d)
	}
	s.Turbine.Frontiers.ObservedSlot = 1
	if *d.ObservedHeaderSlot != 1<<53+1 {
		t.Fatal("diagnostic frontier aliases its source")
	}
	for _, at := range []time.Time{next.SampledAt, next.StartedAt, next.Turbine.SampledAt, next.Turbine.StartedAt, *next.Turbine.LastPacketAt} {
		if !at.Equal(now) || at.Location() != time.UTC {
			t.Errorf("non-UTC network time: %s", at)
		}
	}
	b, err := json.Marshal(next)
	if err != nil || !strings.Contains(string(b), `"next_slot":"9007199254740993"`) || !strings.Contains(string(b), `"packets":"9007199254740993"`) {
		t.Fatalf("network counter precision: %s, %v", b, err)
	}
}

func TestBlockSourcePeerPayloadPresenceAndCopy(t *testing.T) {
	at := time.Now().UTC()
	s := blockstream.NetworkSnapshot{SampledAt: at, SourceStartedAt: at, LiveConnected: true,
		Turbine: &blockstream.TurbineObservation{SampledAt: at, StartedAt: at, Session: 1}}
	if blockSourceMeasurement(s).Turbine.Diagnostics.PeerPayloadBytes != nil {
		t.Fatal("absent retransmitter became known-zero traffic")
	}
	s.Turbine.RetransmitEnabled = true
	zero := blockSourceMeasurement(s).Turbine.Diagnostics.PeerPayloadBytes
	if zero == nil || *zero != 0 {
		t.Fatal("initialized relay lost known-zero traffic")
	}
	s.Turbine.PeerPayloadBytes = ^uint64(0)
	m := blockSourceMeasurement(s)
	b, err := json.Marshal(m)
	if err != nil || !strings.Contains(string(b), `"peer_payload_bytes":"18446744073709551615"`) {
		t.Fatalf("peer byte precision: %s, %v", b, err)
	}
	s.Turbine.PeerPayloadBytes = 1
	if *m.Turbine.Diagnostics.PeerPayloadBytes != ^uint64(0) || !m.Turbine.SampledAt.Equal(at) {
		t.Fatal("peer payload projection aliases or redates its source")
	}
	s.Turbine.RetransmitEnabled = false
	if blockSourceMeasurement(s).Turbine.Diagnostics.PeerPayloadBytes != nil {
		t.Fatal("unavailable relay retained a byte measurement")
	}
}

func TestGossipProjectionCopiesCountersAndNullableTimes(t *testing.T) {
	now := time.Date(2026, 10, 30, 9, 30, 0, 123, time.FixedZone("offset", 19800))
	s := blockstream.NetworkSnapshot{SampledAt: now, SourceStartedAt: now, LiveConnected: true}
	if m := blockSourceMeasurement(s); m.Gossip != nil {
		t.Fatal("missing gossip sample was fabricated")
	}
	s.Gossip = &gossip.MonitoringSnapshot{SampledAt: now.Add(-10 * time.Second), StartedAt: now.Add(-time.Minute),
		Stats: gossip.Stats{RxPackets: 1<<53 + 1, TxPackets: 1<<53 + 2, RxDecodeErrors: 3, TxErrors: 4,
			AcceptedContacts: 5, Peers: 6, RepairPeers: 7, AlpenglowPeers: 8}}
	m := blockSourceMeasurement(s)
	want := &dashboard.Gossip{SampledAt: s.Gossip.SampledAt.UTC(), StartedAt: s.Gossip.StartedAt.UTC(),
		ReceivedPackets: 1<<53 + 1, SentPackets: 1<<53 + 2, DecodeErrors: 3, SendErrors: 4,
		AcceptedContacts: 5, KnownPeers: 6, RepairPeers: 7, AlpenglowPeers: 8}
	if !reflect.DeepEqual(m.Gossip, want) {
		t.Fatalf("gossip = %+v, want %+v", m.Gossip, want)
	}
	s.Gossip.Stats.LastRxUnix, s.Gossip.Stats.LastTxUnix = now.Unix(), now.Unix()-1
	next := blockSourceMeasurement(s)
	if m.Gossip.LastReceivedAt != nil || m.Gossip.LastSentAt != nil || next.Gossip.LastReceivedAt == nil || next.Gossip.LastSentAt == nil {
		t.Fatal("unknown timestamps became zero or old projection changed")
	}
	if !next.Gossip.LastReceivedAt.Equal(time.Unix(now.Unix(), 0)) || !next.Gossip.LastSentAt.Equal(time.Unix(now.Unix()-1, 0)) {
		t.Fatalf("Unix timestamp conversion changed instants: %+v", next.Gossip)
	}
	for _, at := range []time.Time{next.Gossip.SampledAt, next.Gossip.StartedAt, *next.Gossip.LastReceivedAt, *next.Gossip.LastSentAt} {
		if at.Location() != time.UTC {
			t.Errorf("non-UTC gossip observation: %s", at)
		}
	}
	s.Gossip.Stats.RxPackets, s.Gossip.Stats.Peers, s.Gossip.Stats.LastRxUnix = 0, 0, 0
	if !reflect.DeepEqual(m.Gossip, want) || next.Gossip.ReceivedPackets != 1<<53+1 || next.Gossip.KnownPeers != 6 {
		t.Fatal("source mutation changed a copied gossip observation")
	}
	data, err := json.Marshal(m.Gossip)
	if err != nil || !strings.Contains(string(data), `"received_packets":"9007199254740993"`) || !strings.Contains(string(data), `"sent_packets":"9007199254740994"`) || !strings.Contains(string(data), `"last_received_at":null`) || !strings.Contains(string(data), `"last_sent_at":null`) {
		t.Fatalf("gossip precision/null contract: %s, %v", data, err)
	}
}

func TestGossipProjectionCopiesBoundedPeerMetadata(t *testing.T) {
	at := time.Now().UTC()
	g := &gossip.MonitoringSnapshot{SampledAt: at, StartedAt: at.Add(-time.Minute), PeerCount: 9}
	g.Peers[0] = gossip.PeerObservation{Pubkey: gossip.Pubkey{1}, Version: gossip.Version{Major: 3, Minor: 1, Patch: 2, Client: 15},
		ShredVersion: 4321, ReceivedAt: at, TVU: true, Alpenglow: true}
	m := blockSourceMeasurement(blockstream.NetworkSnapshot{Gossip: g})
	if len(m.Gossip.RecentPeers) != 8 || m.Gossip.RecentPeers[0].Version != "3.1.2" || m.Gossip.RecentPeers[0].Client != 15 || m.Gossip.RecentPeers[0].ShredVersion != 4321 || !m.Gossip.RecentPeers[0].TVU {
		t.Fatalf("wrong or unbounded peer mapping: %+v", m.Gossip.RecentPeers)
	}
	g.Peers[0] = gossip.PeerObservation{}
	if m.Gossip.RecentPeers[0].Version != "3.1.2" || !m.Gossip.RecentPeers[0].ReceivedAt.Equal(at) {
		t.Fatal("mapped contacts alias owner cache")
	}
}

func TestGossipProjectionNormalizesPackedBaseVersion(t *testing.T) {
	// Agave 965aee8e version/src/v4.rs:100-143 packs the prerelease tag
	// into minor's upper two bits and its number into the wire patch.
	for _, tc := range []struct {
		name                string
		major, minor, patch uint16
		want                string
	}{
		{"stable", 4, 3, 7, "4.3.7"},
		{"rc_zero", 4, 3 | 1<<14, 0, "4.3.0"},
		{"rc", 4, 3 | 1<<14, 5, "4.3.0"},
		{"beta", 4, 3 | 2<<14, 9, "4.3.0"},
		{"alpha", 4, 3 | 3<<14, 11, "4.3.0"},
		{"stable_maximum", 65535, 0x3fff, 65535, "65535.16383.65535"},
		{"alpha_maximum", 65535, 65535, 65535, "65535.16383.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at := time.Unix(123, 0).UTC()
			g := &gossip.MonitoringSnapshot{SampledAt: at, StartedAt: at, PeerCount: 1}
			g.Peers[0] = gossip.PeerObservation{Pubkey: gossip.Pubkey{1},
				Version:      gossip.Version{Major: tc.major, Minor: tc.minor, Patch: tc.patch, Commit: 42, FeatureSet: 43, Client: 15},
				ShredVersion: 4321, ReceivedAt: at, TVU: true, Alpenglow: true}
			before := *g
			m := blockSourceMeasurement(blockstream.NetworkSnapshot{Gossip: g})
			if *g != before {
				t.Fatal("version projection mutated the raw gossip observation")
			}
			if m.Gossip == nil || len(m.Gossip.RecentPeers) != 1 {
				t.Fatalf("peer projection missing: %+v", m.Gossip)
			}
			got := m.Gossip.RecentPeers[0]
			if got.Version != tc.want {
				t.Errorf("wire version %d.%d.%d projected as %q, want base version %q", tc.major, tc.minor, tc.patch, got.Version, tc.want)
			}
			if got.Client != 15 || got.ShredVersion != 4321 || !got.ReceivedAt.Equal(at) || !got.TVU || !got.Alpenglow {
				t.Fatalf("version normalization changed other peer metadata: %+v", got)
			}
		})
	}
}

func TestDashboardTimestampsUseUTC(t *testing.T) {
	now := time.Date(2026, 10, 30, 9, 30, 0, 123, time.FixedZone("offset", 19800))
	d := &nodeDashboard{}
	d.observeRuntime(now, runtime.MemStats{}, &metrics.RuntimeWindow{Seconds: 60})
	s := d.snapshot()
	c := consensusMeasurement(consensus.MonitoringSnapshot{
		Receiver: &alpenglow.ReceiverStats{LastMessageAt: now},
		Voting:   &consensus.VotingObservation{At: now, LastNetworkLandedAt: now},
	}, now)
	for _, at := range []time.Time{s.Runtime.SampledAt, s.Runtime.Pressure.SampledAt, s.Resources.SampledAt,
		c.SampledAt, *c.Receiver.LastMessageAt, c.Voting.SampledAt, *c.Voting.LastNetworkLandedAt} {
		if !at.Equal(now) || at.Location() != time.UTC {
			t.Errorf("timestamp is not the same instant in UTC: %s", at)
		}
	}
}

func TestDashboardDisabledIsNoOp(t *testing.T) {
	d := startNodeDashboard(context.Background(), time.Now().UTC(), "", "", "run", "alpenglow", "verifying", true, false, false)
	if d != nil {
		t.Fatal("disabled dashboard created a feed")
	}
	d.bootstrap("genesis")
	d.replaying(nil)
	d.stopping()
	d.close()
	d.storagePaths("", "")
	d.observeRuntime(time.Now(), runtime.MemStats{}, nil)
	d.consensusStarted(nil)
	d.selectedRecovery(nil)
	d.rpcStarted(nil)
}

func TestDashboardInvalidGroupDoesNotFallBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.sock")
	d := startNodeDashboard(context.Background(), time.Now().UTC(), path,
		"mithril-dashboard-group-does-not-exist", "run", "alpenglow", "verifying", true, false, false)
	if d != nil {
		d.close()
		t.Fatal("invalid feed group fell back to a private feed")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("failed group setup left a socket: %v", err)
	}
}

func TestRecoverySelectionDoesNotReadLiveMutableState(t *testing.T) {
	d := &nodeDashboard{}
	s := &state.MithrilState{SnapshotSlot: 50, LastSlot: 70, LastBankhash: "bank", LastRootedSlot: 60, LastRootedBankhash: "root"}
	d.selectedRecovery(s)
	s.LastSlot, s.LastBankhash = 80, "new"
	r := d.snapshot().Recovery
	if *r.StoredSlot != 70 || *r.StoredBankhash != "bank" || *r.RootedSlot != 60 || r.SnapshotSlot != 50 {
		t.Fatalf("selection mutated: %+v", r)
	}
	d.selectedRecovery(&state.MithrilState{SnapshotSlot: 90})
	r = d.snapshot().Recovery
	if r.StoredSlot != nil || r.RootedSlot != nil || r.StoredBankhash != nil {
		t.Fatal("new seed retained old recovery data")
	}
}

func TestConsensusProjectionDoesNotInventEligibilityOrFinality(t *testing.T) {
	m := consensusMeasurement(consensus.MonitoringSnapshot{VotingEnabled: true, Voting: &consensus.VotingObservation{}}, time.Now())
	if m.FinalizedSlot != nil || m.FinalizedBlockID != nil || m.Receiver != nil || m.Voting.KeyEligible != nil || m.Voting.Epoch != nil || m.Voting.LastNetworkLandedSlot != nil {
		t.Fatalf("unobserved data became zero: %+v", m)
	}
	m = consensusMeasurement(consensus.MonitoringSnapshot{VotingEnabled: true, Voting: &consensus.VotingObservation{HasEpochSet: true, Eligible: true, Rank: 0, Stake: 100}}, time.Now())
	if m.Voting.KeyEligible == nil || !*m.Voting.KeyEligible || m.Voting.Rank == nil || *m.Voting.Rank != 0 || *m.Voting.AdmittedStake != 100 {
		t.Fatal("valid zero rank is unavailable")
	}
}

func TestDashboardRuntimeResourcesAreImmutableAndPrivate(t *testing.T) {
	d := &nodeDashboard{}
	privatePath := t.TempDir()
	d.storagePaths(privatePath, privatePath+"/missing")
	now := time.Now().UTC()
	w := &metrics.RuntimeWindow{Seconds: 60, SchedLatencyP99: 2 * time.Millisecond, GCCPUFraction: .2}
	d.observeRuntime(now, runtime.MemStats{HeapAlloc: 25, Sys: 100, HeapReleased: 40, NumGC: 3}, w)
	first := d.snapshot()
	if first.Runtime.GoBytes != 60 || first.Runtime.GCCount != 3 || first.Runtime.Pressure.SchedulerP99Seconds != .002 {
		t.Fatalf("incorrect projection: %+v", first.Runtime)
	}
	if first.Resources.Accounts == nil || first.Resources.Accounts.TotalBytes == 0 || first.Resources.Ledger != nil || first.Resources.CPUSeconds == nil {
		t.Fatalf("missing filesystem reported as healthy: %+v", first.Resources)
	}
	w.GCCPUFraction = .9
	d.observeRuntime(now.Add(10*time.Second), runtime.MemStats{HeapAlloc: 50, Sys: 100}, nil)
	second := d.snapshot()
	if first.Runtime.HeapBytes != 25 || second.Runtime.HeapBytes != 50 || second.Runtime.Pressure.GCCPUFraction != .2 || !second.Runtime.Pressure.SampledAt.Equal(now) {
		t.Fatal("immutable sample or independent pressure timestamp changed")
	}
	b, err := json.Marshal(second)
	if err != nil || strings.Contains(string(b), privatePath) {
		t.Fatalf("resource projection leaked a path or failed: %s, %v", b, err)
	}
	if filesystemSample("") != nil {
		t.Fatal("unconfigured filesystem must be unknown")
	}
}

func TestDashboardLifecycleAndIdentitySwap(t *testing.T) {
	oldFrontier := global.ReplayFrontier()
	t.Cleanup(func() { global.SetReplayFrontier(oldFrontier) })
	global.SetReplayFrontier(91)
	d := &nodeDashboard{state: dashboard.Snapshot{RunID: "run", Mode: "validator", Phase: "starting"}}
	if d.snapshot().ReplayFrontier != nil {
		t.Fatal("starting node exposed an old frontier")
	}
	d.bootstrap("genesis")
	s := d.snapshot()
	if s.Phase != "bootstrap" || s.GenesisHash == nil || *s.GenesisHash != "genesis" || s.ReplayFrontier != nil {
		t.Fatalf("invalid bootstrap snapshot: %+v", s)
	}
	holder, err := identity.NewEphemeralHolder()
	if err != nil {
		t.Fatal(err)
	}
	d.replaying(holder)
	s = d.snapshot()
	if s.IdentityKey == nil || *s.IdentityKey != holder.PublicKey().String() || s.ReplayFrontier == nil || *s.ReplayFrontier != 91 {
		t.Fatalf("invalid replay snapshot: %+v", s)
	}
	next, err := identity.NewEphemeralHolder()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Swap(next.Current().PrivateKey()); err != nil {
		t.Fatal(err)
	}
	if got := d.snapshot().IdentityKey; got == nil || *got != next.PublicKey().String() {
		t.Fatal("dashboard retained old identity after a swap")
	}
	global.SetReplayFrontier(80)
	if got := d.snapshot().ReplayFrontier; got == nil || *got != 80 {
		t.Fatal("dashboard hid a recovery rewind")
	}
	d.stopping()
	if d.snapshot().ReplayFrontier != nil {
		t.Fatal("stopping node exposed live progress")
	}
}

func TestDashboardSnapshotConcurrentLifecycle(t *testing.T) {
	d := &nodeDashboard{state: dashboard.Snapshot{RunID: "run", Mode: "verifying", Phase: "starting"}}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 1000 {
				d.bootstrap("genesis")
				d.replaying(nil)
				d.snapshot()
				d.stopping()
			}
		})
	}
	wg.Wait()
}
