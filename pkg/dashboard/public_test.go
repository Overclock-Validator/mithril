package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestPublicCollectorThroughputAndOversizedProjection(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	defer c.client.CloseIdleConnections()
	at := time.Now().UTC()
	s := throughputFixture(at)
	read := func() PublicSnapshot {
		t.Helper()
		set := c.latest.Load()
		if set == nil {
			t.Fatal("missing public frame")
		}
		var p PublicSnapshot
		if err := json.Unmarshal(set.publicJSON, &p); err != nil {
			t.Fatal(err)
		}
		var frame struct{ Value json.RawMessage }
		if err := json.Unmarshal(set.messages[0], &frame); err != nil || string(frame.Value) != string(set.publicJSON) {
			t.Fatal("HTTP and WebSocket projections differ")
		}
		return p
	}
	for i := 0; i < throughputSamples; i++ {
		s.SampledAt = at.Add(time.Duration(i) * time.Second)
		s.Replay.SampledAt = s.SampledAt
		s.Replay.ExecutedTransactions = uint64(i * 7)
		s.Replay.ExecutedComputeUnits = uint64(i * 11)
		setFixture(t, body, s)
		c.poll(context.Background(), s.SampledAt)
		p := read()
		if p.SourceState != "fresh" || p.Throughput == nil {
			t.Fatalf("missing current throughput: %+v", p)
		}
		if i == 0 && p.Throughput.TransactionsPerSecond != nil {
			t.Fatal("baseline fabricated a rate")
		}
		if i > 0 && (p.Throughput.TransactionsPerSecond == nil || *p.Throughput.TransactionsPerSecond != 7 || *p.Throughput.ComputeUnitsPerSecond != 11) {
			t.Fatalf("incorrect measured rate: %+v", p.Throughput)
		}
	}
	if p := read(); len(p.Throughput.History) != throughputSamples-1 {
		t.Fatalf("missing bounded history: %+v", p.Throughput)
	}

	// The private feed still fits, but the public projection plus its retained
	// history does not. An old fresh response must not survive this failure.
	s.SampledAt = s.SampledAt.Add(time.Second)
	s.Replay.SampledAt = s.SampledAt
	s.Version = ""
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	s.Version = strings.Repeat("v", MaxSnapshotBytes-len(b)-512)
	b, err = json.Marshal(s)
	if err != nil || len(b) > MaxSnapshotBytes || s.validate() != nil {
		t.Fatalf("invalid feed fixture: bytes=%d, err=%v", len(b), err)
	}
	probe := c.throughput
	v := probe.Observe(&s, "fresh", s.SampledAt)
	if _, err := encodePublicSnapshot(&s, "fresh", s.SampledAt, &v, nil, nil); err == nil {
		t.Fatal("fixture did not exceed the public history budget")
	}
	body.Store(&b)
	c.poll(context.Background(), s.SampledAt)
	if p := read(); p.SourceState != "unavailable" || p.Node != nil || p.Throughput != nil || c.last != nil || c.throughput.count != 0 {
		t.Fatalf("failed projection retained source data: %+v", p)
	}
	failedAt := s.SampledAt
	s.Version = "dev"
	setFixture(t, body, s)
	c.poll(context.Background(), failedAt)
	if p := read(); p.SourceState != "unavailable" || p.Node != nil || c.last != nil {
		t.Fatalf("projection failure accepted changed data at the same sample time: %+v", p)
	}
	s.SampledAt = failedAt.Add(-time.Second)
	s.Replay.SampledAt = s.SampledAt
	setFixture(t, body, s)
	c.poll(context.Background(), failedAt)
	if p := read(); p.SourceState != "unavailable" || p.Node != nil || c.last != nil {
		t.Fatalf("projection failure lost same-run timestamp guard: %+v", p)
	}
	s.SampledAt = failedAt.Add(time.Second)
	s.Replay.SampledAt = s.SampledAt
	setFixture(t, body, s)
	c.poll(context.Background(), s.SampledAt)
	if p := read(); p.SourceState != "fresh" || p.Throughput == nil || p.Throughput.Samples != 1 || p.Throughput.TransactionsPerSecond != nil {
		t.Fatalf("recovery reused old rate coverage: %+v", p)
	}
	body.Store(nil)
	c.poll(context.Background(), s.SampledAt.Add(time.Second))
	if p := read(); p.SourceState != "stale" || p.Throughput == nil || p.Throughput.State != "unavailable" || p.Throughput.Samples != 0 {
		t.Fatalf("source loss retained current rates: %+v", p)
	}
}

func publicTestSnapshot(now time.Time) Snapshot {
	s := fullTestSnapshot(now)
	s.Mode, s.RPCConfigured = "validator", true
	identity := "11111111111111111111111111111111"
	s.IdentityKey = &identity
	s.Consensus.Voting.Identity = identity
	s.BlockSource.LiveConnected = true
	s.Replay.WorkCountersSaturated = true
	return s
}

func publicValue(t *testing.T, s *Snapshot, state string, now time.Time) (PublicSnapshot, []byte) {
	t.Helper()
	b, err := encodePublicSnapshot(s, state, now, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var value PublicSnapshot
	if err := json.Unmarshal(b, &value); err != nil {
		t.Fatal(err)
	}
	return value, b
}

func TestPublicFieldAllowlist(t *testing.T) {
	now := sizingTime(time.Now())
	s := publicTestSnapshot(now)
	_, b := publicValue(t, &s, "fresh", now)
	var value map[string]json.RawMessage
	if err := json.Unmarshal(b, &value); err != nil {
		t.Fatal(err)
	}
	// Every object has an exact allowlist: future private fields cannot leak
	// through an embedded private model without failing this test.
	objects := map[string]string{
		"":                                    "schema_version source_state sampled_at collector_at node replay consensus voting rpc input throughput pipeline startup",
		"node":                                "run_id identity genesis_hash version commit configured_network configured_mode alpenglow rpc_configured phase elapsed_nanoseconds",
		"replay":                              "state sampled_at attempt started_at phase frontier executed_blocks executed_transactions executed_compute_units work_counters_saturated rewinds durable_slot completed certificate_outcomes leader_outcomes next_known_assignment chain_activity",
		"replay.chain_activity":               "state sampled_at provenance time_source interval",
		"replay.completed":                    "at slot bankhash block_height epoch slot_index slots_in_epoch transactions compute_units execution_seconds wait_seconds",
		"consensus":                           "state sampled_at notarized_slot direct_finalized_slot direct_finalized_block_id voting_enabled receiver",
		"consensus.receiver":                  "datagrams_received messages_decoded decode_errors read_errors connections_rejected rate_limited_datagrams last_message_at",
		"voting":                              "state sampled_at activity_scope identity vote_account epoch root_slot key_eligible admitted_stake_lamports total_admitted_stake_lamports votes_cast_this_run network_landed_votes last_network_landed_slot last_network_landed_at joined_live",
		"rpc":                                 "state sampled_at serving published_slot published_at",
		"startup":                             "started_at recovery replay input snapshot_archive",
		"startup.snapshot_archive":            "state sampled_at attempt started_at phase full_snapshot_slot input_kind compressed_input_bytes compressed_reported_total account_payload_bytes",
		"startup.recovery":                    "selected_at snapshot_slot stored_slot rooted_slot",
		"startup.replay":                      "state sampled_at attempt started_at initialized_at start_slot resume_parent",
		"startup.input":                       "state sampled_at started_at configured_source rpc_block_fetch_allowed repair_pending repair_from repair_until",
		"input":                               "state sampled_at started_at next_slot live_connected live_active turbine gossip",
		"input.turbine":                       "state sampled_at session started_at packets data_shreds coding_shreds signature_errors last_packet_at diagnostics",
		"input.turbine.diagnostics":           "observed_header_slot reconstructed_slot block_emission_attempts recovered_data_shreds parse_errors assembly_errors active_slots evicted_slots ignored_old_shreds signature_verifications signature_cache_hits peer_payload_bytes admission useful_repair_shreds spooled_repair_shreds repair",
		"input.turbine.diagnostics.admission": "missing_leaders shred_version_mismatch",
		"input.turbine.diagnostics.repair":    "requests responses late_responses timeouts errors outstanding peers responding_peers",
		"input.gossip":                        "state sampled_at started_at received_packets sent_packets decode_errors send_errors accepted_contacts known_peers repair_peers alpenglow_peers",
		"pipeline":                            "state sampled_at started_at ingress_packets ingress_bytes ingress_full_drops passed_dedup_packets invalid_wire_drops unsupported_version_drops duplicate_drops verified_packets invalid_signature_drops scheduler_buffered_total scheduler_queue_depth bank_admissions buffer_capacity_drops cost_rejections execution_rejections quic_open_connections quic_active_streams quic_stream_buffer_drops scheduler_expired_removals scheduler_receive_evictions",
	}
	for path, fields := range objects {
		object := value
		if path != "" {
			for _, key := range strings.Split(path, ".") {
				var nested map[string]json.RawMessage
				if err := json.Unmarshal(object[key], &nested); err != nil || nested == nil {
					t.Fatalf("%s: missing object %s: %v", path, key, err)
				}
				object = nested
			}
		}
		var got []string
		for key := range object {
			got = append(got, key)
		}
		want := strings.Fields(fields)
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s keys: got %v, want %v", path, got, want)
		}
	}
	if len(b) > MaxSnapshotBytes {
		t.Fatalf("public response exceeds budget: %d", len(b))
	}
	t.Logf("complete public response: %d/%d bytes", len(b), MaxSnapshotBytes)
}

func TestPublicProjectionDoesNotForwardPrivateFields(t *testing.T) {
	now := time.Now().UTC()
	s := publicTestSnapshot(now)
	canary := "private-diagnostic-must-not-be-published"
	s.Network, s.BlockSource.ConfiguredSource = canary, canary
	s.Recovery.StoredBankhash = &canary
	s.RPC.Methods[0].Name = canary
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var future map[string]json.RawMessage
	if err := json.Unmarshal(b, &future); err != nil {
		t.Fatal(err)
	}
	future["new_private_field"], err = json.Marshal(canary)
	if err != nil {
		t.Fatal(err)
	}
	b, err = json.Marshal(future)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Snapshot
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	p, b := publicValue(t, &decoded, "fresh", now)
	if strings.Contains(string(b), canary) || p.Node.Network != "unknown" {
		t.Fatal("private data crossed the public projection")
	}
	if p.Replay.ExecutedTransactions != math.MaxUint64 || p.Replay.ExecutedComputeUnits != math.MaxUint64 || !p.Replay.WorkCountersSaturated {
		t.Fatal("cumulative replay counters lost precision or saturation")
	}
	for _, field := range []string{"executed_blocks", "executed_transactions", "executed_compute_units", "packets", "votes_cast_this_run", "received_packets", "scheduler_buffered_total", "scheduler_queue_depth", "quic_open_connections"} {
		if !strings.Contains(string(b), `"`+field+`":"18446744073709551615"`) {
			t.Errorf("%s not encoded as a decimal string", field)
		}
	}
}

func TestPublicAvailabilityAndRoleChanges(t *testing.T) {
	now := time.Now().UTC()
	s := publicTestSnapshot(now)
	stale, _ := publicValue(t, &s, "stale", now)
	for _, state := range []string{stale.Replay.State, stale.Consensus.State, stale.Voting.State, stale.RPC.State, stale.Input.State, stale.Input.Turbine.State, stale.Input.Gossip.State, stale.Pipeline.State} {
		if state != "stale" {
			t.Fatalf("stale feed advertised a %s group", state)
		}
	}
	for _, state := range []string{"fresh", "stale", "unavailable"} {
		p, _ := publicValue(t, nil, state, now)
		if p.SourceState != "unavailable" || p.Node != nil || p.Replay != nil || p.SampledAt != nil || p.Pipeline != nil || p.Input != nil {
			t.Fatal("missing feed fabricated data")
		}
	}
	p, _ := publicValue(t, &s, "unavailable", now)
	if p.Node != nil || p.Replay != nil || p.SampledAt != nil || p.Pipeline != nil || p.Input != nil {
		t.Fatal("unavailable source published observations")
	}
	if _, err := encodePublicSnapshot(&s, "healthy", now, nil, nil, nil); err == nil {
		t.Fatal("invented source state accepted")
	}
	s.Mode = "verifying"
	p, _ = publicValue(t, &s, "fresh", now)
	if p.Voting != nil || p.Pipeline != nil || p.Replay == nil || p.Input == nil || p.RPC == nil || p.Input.Gossip == nil {
		t.Fatal("non-voting node lost useful data or advertised voting")
	}
	s.Mode, s.RPCConfigured, s.Alpenglow = "validator", false, false
	p, _ = publicValue(t, &s, "fresh", now)
	if p.RPC != nil || p.Consensus != nil || p.Voting != nil {
		t.Fatal("disabled capability retained old observations")
	}
	s.Alpenglow = true
	s.Consensus.Voting.Identity = "old-identity"
	p, _ = publicValue(t, &s, "fresh", now)
	if p.Voting != nil {
		t.Fatal("old identity's voting observations attributed to new identity")
	}
	s.Consensus.Voting.Identity = *s.IdentityKey
	s.BlockSource.LiveConnected = false
	p, _ = publicValue(t, &s, "fresh", now)
	if p.Input.Turbine != nil || p.Input.Gossip != nil || p.Voting.ActivityScope != "voter_instance" {
		t.Fatal("disconnected input retained live counters or voting scope was lost")
	}
}

func TestPublicConfiguredNetworkRecognizesAlpenglow(t *testing.T) {
	now := time.Now().UTC()
	s := publicTestSnapshot(now)
	for _, tc := range []struct{ configured, want string }{
		{"alpenglow", "alpenglow"}, {"mainnet", "mainnet"}, {"mainnet-beta", "mainnet-beta"}, {"private-network", "unknown"},
	} {
		s.Network = tc.configured
		p, _ := publicValue(t, &s, "fresh", now)
		if p.Node == nil || p.Node.Network != tc.want {
			t.Fatalf("configured network %q: got %+v, want %q", tc.configured, p.Node, tc.want)
		}
	}
}

func TestPublicObservationTimes(t *testing.T) {
	now := time.Now().UTC()
	for _, at := range []time.Time{{}, now.Add(2 * time.Second)} {
		s := publicTestSnapshot(now)
		s.Replay.SampledAt, s.Consensus.SampledAt, s.RPC.SampledAt, s.BlockSource.SampledAt = at, at, at, at
		s.Pipeline.SampledAt = at
		p, _ := publicValue(t, &s, "fresh", now)
		if p.Replay != nil || p.Consensus != nil || p.Voting != nil || p.RPC != nil || p.Input != nil || p.Pipeline != nil {
			t.Fatal("undated or future group published")
		}
		s = publicTestSnapshot(now)
		s.Replay.Completed.At, s.Consensus.Voting.SampledAt, s.BlockSource.Turbine.SampledAt = at, at, at
		s.BlockSource.Gossip.SampledAt = at
		p, _ = publicValue(t, &s, "fresh", now)
		if p.Replay.Completed != nil || p.Voting != nil || p.Input.Turbine != nil || p.Input.Gossip != nil {
			t.Fatal("undated or future nested observation published")
		}
	}
	for _, age := range []time.Duration{30 * time.Second, 30*time.Second + time.Nanosecond} {
		s := publicTestSnapshot(now)
		s.Replay.SampledAt = now.Add(-age)
		s.Pipeline.SampledAt, s.BlockSource.Gossip.SampledAt = s.Replay.SampledAt, s.Replay.SampledAt
		p, _ := publicValue(t, &s, "fresh", now)
		want := "fresh"
		if age > 30*time.Second {
			want = "stale"
		}
		if p.Replay.State != want || p.Pipeline.State != want || p.Input.Gossip.State != want || p.Replay.ExecutedTransactions != s.Replay.ExecutedTransactions {
			t.Fatal("producer age was hidden or stale observations lost")
		}
	}
}

func TestPublicCompletedBlockBoundToReplayAttempt(t *testing.T) {
	now := time.Now().UTC()
	s := publicTestSnapshot(now)
	s.Replay.StartedAt = now.Add(-3 * time.Second)
	s.Replay.SampledAt = now.Add(-time.Second)
	for _, at := range []time.Time{s.Replay.StartedAt.Add(-time.Nanosecond), s.Replay.SampledAt.Add(time.Nanosecond)} {
		s.Replay.Completed.At = at
		p, _ := publicValue(t, &s, "fresh", now)
		if p.Replay == nil || p.Replay.Completed != nil {
			t.Fatalf("completion outside replay attempt was published: %+v", p.Replay)
		}
	}
	s.Replay.Completed.At = s.Replay.SampledAt
	p, _ := publicValue(t, &s, "fresh", now)
	if p.Replay == nil || p.Replay.Completed == nil {
		t.Fatal("valid completed block was dropped")
	}
}

func TestPublicRPCPublishedSlotRequiresCoherentTime(t *testing.T) {
	now := time.Now().UTC()
	timePtr := func(at time.Time) *time.Time { return &at }
	for _, tc := range []struct {
		name string
		at   *time.Time
		slot *uint64
		want bool
	}{
		{"known slot zero", &now, new(uint64), true},
		{"missing time", nil, new(uint64), false},
		{"missing slot", &now, nil, false},
		{"future time", timePtr(now.Add(2 * time.Second)), new(uint64), false},
		{"before run", timePtr(now.Add(-2 * time.Hour)), new(uint64), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := publicTestSnapshot(now)
			s.RPC.PublishedSlot, s.RPC.PublishedAt = tc.slot, tc.at
			p, _ := publicValue(t, &s, "fresh", now)
			if p.RPC == nil || (p.RPC.PublishedSlot != nil) != tc.want || (p.RPC.PublishedAt != nil) != tc.want {
				t.Fatalf("publication pair: %+v", p.RPC)
			}
		})
	}
	s := publicTestSnapshot(now)
	s.RPC.SampledAt = now.Add(-time.Second)
	s.RPC.PublishedSlot, s.RPC.PublishedAt = new(uint64), &now
	p, _ := publicValue(t, &s, "fresh", now)
	if p.RPC == nil || p.RPC.PublishedSlot != nil || p.RPC.PublishedAt != nil {
		t.Fatalf("publication after the sample accepted: %+v", p.RPC)
	}
}

func TestPublicPipelineGaugesAndIndependentCounters(t *testing.T) {
	now := time.Now().UTC()
	s := publicTestSnapshot(now)
	s.Pipeline = &Pipeline{SampledAt: now, StartedAt: now.Add(-time.Minute),
		Ingress:   PipelineIngress{EnqueuedPackets: 5, EnqueuedBytes: 7, DroppedFull: 11},
		Dedup:     PipelineDedup{OutPackets: 13, DroppedSanitize: 17, DroppedUnsupportedVersion: 19, DroppedDedup: 23},
		Sigverify: PipelineSigverify{VerifiedPackets: 29, DroppedSigverify: 31},
		Scheduler: PipelineScheduler{BufferedTotal: 37, Accepted: 41, DroppedCapacity: 43, DroppedCost: 47, DroppedExecution: 53},
	}
	p, _ := publicValue(t, &s, "fresh", now)
	want := &PublicPipeline{PublicObservation: publicObservation(now, "fresh", now), StartedAt: s.Pipeline.StartedAt,
		IngressPackets: 5, IngressBytes: 7, IngressFullDrops: 11, PassedDedupPackets: 13,
		InvalidWireDrops: 17, UnsupportedVersionDrops: 19, DuplicateDrops: 23,
		VerifiedPackets: 29, InvalidSignatureDrops: 31, SchedulerBufferedTotal: 37,
		BankAdmissions: 41, BufferCapacityDrops: 43, CostRejections: 47, ExecutionRejections: 53,
	}
	if !reflect.DeepEqual(p.Pipeline, want) {
		t.Fatalf("pipeline mapping: got %+v, want %+v", p.Pipeline, want)
	}
	// Missing gauges stay null; measured zeros are not missing. Independent
	// counters need not balance and must not be clamped into a waterfall.
	zero := uint64(0)
	s.Pipeline.Scheduler.QueueDepth = &zero
	s.Pipeline.QUIC = &PipelineQUIC{StreamBufferDrops: 59}
	p, _ = publicValue(t, &s, "fresh", now)
	if p.Pipeline.SchedulerQueueDepth == nil || *p.Pipeline.SchedulerQueueDepth != 0 || p.Pipeline.QUICOpenConnections == nil || *p.Pipeline.QUICOpenConnections != 0 || p.Pipeline.QUICActiveStreams == nil || *p.Pipeline.QUICActiveStreams != 0 || p.Pipeline.QUICStreamBufferDrops == nil || *p.Pipeline.QUICStreamBufferDrops != 59 {
		t.Fatalf("missing or wrong measured gauge: %+v", p.Pipeline)
	}
}
