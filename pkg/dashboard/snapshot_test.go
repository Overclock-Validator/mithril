package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

// Fill every group, respecting producer collection bounds. This is a conservative
// source-shaped size envelope, not a semantically coherent observation: cross-field
// sums and unknown/known combinations are checked by separate projection fixtures.
// Free-form build/network labels have a tested allowance, not a production cap.
// Size tests pass sizingTime; transport/boundary tests retain their exact input.
func fullTestSnapshot(now time.Time) Snapshot {
	s := testSnapshot(now)
	s.Version, s.Commit, s.Network = "1"+strings.Repeat("<", 63), strings.Repeat("<", 64), strings.Repeat("<", 64)
	var hash solana.Hash
	for i := range hash {
		hash[i] = 255
	}
	key, frontier := hash.String(), uint64(math.MaxUint64)
	s.GenesisHash, s.IdentityKey, s.ReplayFrontier = &key, &key, &frontier
	elapsed := uint64(math.MaxInt64 - 1)
	s.ElapsedNanoseconds = &elapsed
	var fill func(reflect.Value)
	fill = func(v reflect.Value) {
		if v.Type() == reflect.TypeOf(time.Time{}) {
			v.Set(reflect.ValueOf(now))
			return
		}
		switch v.Kind() {
		case reflect.Pointer:
			v.Set(reflect.New(v.Type().Elem()))
			fill(v.Elem())
		case reflect.Struct:
			for i := range v.NumField() {
				fill(v.Field(i))
			}
		case reflect.Array, reflect.Slice:
			if v.Kind() == reflect.Slice {
				var count int
				switch v.Type() {
				case reflect.TypeFor[[]EpochOutcome]():
					count = 2 // replay.EpochOutcomeSummary.Epochs is [2].
				case reflect.TypeFor[[]Peer]():
					count = 8 // gossip.MonitoringSnapshot.Peers is [8].
				case reflect.TypeFor[[]RPCMethod]():
					count = 9 // rpcserver registers exactly nine monitored wrappers.
				default:
					panic("add the producer bound for private snapshot slice " + v.Type().String())
				}
				v.Set(reflect.MakeSlice(v.Type(), count, count))
			}
			for i := range v.Len() {
				fill(v.Index(i))
			}
		case reflect.String:
			v.SetString(strings.Repeat("x", 64))
		case reflect.Uint64:
			v.SetUint(math.MaxUint64)
		case reflect.Uint32:
			v.SetUint(math.MaxUint32)
		case reflect.Uint16:
			v.SetUint(math.MaxUint16)
		case reflect.Int:
			v.SetInt(math.MaxInt)
		case reflect.Int64:
			v.SetInt(math.MaxInt64)
		case reflect.Float64:
			// Fixed-point formatting near 1e-6 is wider than MaxFloat64's
			// exponent form for these nonnegative source measurements.
			v.SetFloat(0.0000012345678901234567)
		}
	}
	for _, field := range []string{"Runtime", "Resources", "Replay", "Consensus", "RPC", "Recovery", "BlockSource", "Pipeline"} {
		fill(reflect.ValueOf(&s).Elem().FieldByName(field))
	}
	for i, name := range []string{"getAccountInfo", "getBankHash", "getBlockHeight", "getEpochInfo", "getGenesisHash", "getInflationReward", "getLatestBlockhash", "sendTransaction", "simulateTransaction"} {
		s.RPC.Methods[i].Name = name
		for bucket := range s.RPC.Methods[i].DurationBuckets {
			// The adapter uses strconv.FormatUint on the source's [6]uint64.
			s.RPC.Methods[i].DurationBuckets[bucket] = strconv.FormatUint(math.MaxUint64, 10)
		}
	}
	s.SnapshotArchive = maximumSnapshotArchive(now)
	s.BlockSource.Reception.RetainedSlots = 64
	d := s.BlockSource.Turbine.Diagnostics
	d.ActiveSlots, d.RepairOutstanding, d.RepairPeers, d.RepairRespondingPeers = math.MaxInt32, math.MaxInt32, math.MaxInt32, math.MaxInt32
	return s
}

func sizingTime(at time.Time) time.Time {
	return at.UTC().Truncate(time.Second).Add(time.Second - time.Nanosecond)
}

func assertSizingTimestamps(t *testing.T, value any) {
	t.Helper()
	var check func(reflect.Value)
	check = func(v reflect.Value) {
		if v.Kind() == reflect.Pointer {
			if !v.IsNil() {
				check(v.Elem())
			}
			return
		}
		if v.Type() == reflect.TypeFor[time.Time]() {
			at := v.Interface().(time.Time)
			encoded, err := at.MarshalJSON()
			if err != nil || len(at.Format(time.RFC3339Nano)) != 30 || len(encoded) != 32 {
				t.Fatalf("sizing timestamp lost full precision: %s (%v)", encoded, err)
			}
			return
		}
		switch v.Kind() {
		case reflect.Struct:
			for i := range v.NumField() {
				check(v.Field(i))
			}
		case reflect.Array, reflect.Slice:
			for i := range v.Len() {
				check(v.Index(i))
			}
		}
	}
	check(reflect.ValueOf(value))
}

func TestFullSnapshotUsesProducerCollectionAndNumericBounds(t *testing.T) {
	s := fullTestSnapshot(sizingTime(time.Now()))
	assertSizingTimestamps(t, s)
	if len(s.Replay.EpochOutcomes.Epochs) != 2 || len(s.BlockSource.Gossip.RecentPeers) != 8 || len(s.RPC.Methods) != 9 ||
		s.Runtime.Goroutines != math.MaxInt || s.Resources.Host.CPU.OnlineCPUs != math.MaxUint32 || s.BlockSource.Gossip.RecentPeers[0].Client != math.MaxUint16 || *s.Replay.AccountFacts.StoredCommissions.BlockRevenueBPS != math.MaxUint16 {
		t.Fatal("size fixture no longer covers source collection/numeric bounds")
	}
	if f := s.Replay.AccountFacts; f.StoredVoteHeader == nil || *f.StoredVoteHeader.PendingDelegatorRewards != math.MaxUint64 ||
		f.FundingRules == nil || *f.FundingRules.ShortfallLamports != math.MaxUint64 || f.EpochStakeSets == nil ||
		f.EpochStakeSets.Next == nil || f.EpochStakeSets.Next.Generation != math.MaxUint64 || f.EpochStakeSets.Payer == nil {
		t.Fatal("size fixture omitted account extension bounds")
	}
	for _, method := range s.RPC.Methods {
		for _, bucket := range method.DurationBuckets {
			if n, err := strconv.ParseUint(bucket, 10, 64); err != nil || n != math.MaxUint64 || len(bucket) != 20 {
				t.Fatalf("not a maximum source uint64 bucket: %q", bucket)
			}
		}
	}
}

func TestOversizedValidSnapshotRejectedByFeedAndCollector(t *testing.T) {
	at := time.Now().UTC()
	s := testSnapshot(at)
	// Build labels are not globally length-bounded. Escaping, not raw string
	// length, decides the actual wire limit; never truncate labels to fit.
	s.Version = "1" + strings.Repeat("<", MaxPrivateSnapshotBytes/6+1)
	b, err := json.Marshal(s)
	if err != nil || len(b) <= MaxPrivateSnapshotBytes || s.validate() != nil || !json.Valid(b) {
		t.Fatalf("invalid oversize control: bytes=%d, err=%v", len(b), err)
	}
	path := socketPath(t)
	feed, err := ListenFeed(context.Background(), path, func() Snapshot { return s })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	c := newCollector(path)
	t.Cleanup(c.client.CloseIdleConnections)
	response, err := c.client.Get("http://node/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("feed published an oversized snapshot")
	}
	fixturePath, body := fixtureFeed(t)
	body.Store(&b)
	reader := newCollector(fixturePath)
	t.Cleanup(reader.client.CloseIdleConnections)
	reader.poll(context.Background(), at)
	if reader.last != nil || statusFields(t, reader.latest.Load())["source_state"] != "unavailable" {
		t.Fatal("collector accepted oversized valid JSON")
	}
}

func TestCompleteSnapshotFitsFeedBudgetAndProjectsEveryGroup(t *testing.T) {
	now := sizingTime(time.Now())
	s := fullTestSnapshot(now)
	s.Mode = "validator"
	assertSizingTimestamps(t, s)
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > MaxPrivateSnapshotBytes {
		t.Fatalf("full snapshot is %d bytes, budget is %d", len(b), MaxPrivateSnapshotBytes)
	}
	t.Logf("full snapshot: %d/%d bytes", len(b), MaxPrivateSnapshotBytes)
	var decoded Snapshot
	if err := json.Unmarshal(b, &decoded); err != nil || decoded.Resources == nil {
		t.Fatalf("resource group lost: %v", err)
	}
	if a := decoded.SnapshotArchive; a == nil || a.Attempt != math.MaxUint64 || a.FullSnapshotSlot != math.MaxUint64 ||
		a.CompressedInputBytes != math.MaxUint64 || a.AccountPayloadBytes != math.MaxUint64 ||
		a.CompressedReportedTotal == nil || *a.CompressedReportedTotal != math.MaxUint64 {
		t.Fatal("maximum-width snapshot archive lost")
	}
	if decoded.Consensus.NotarizedSlot == nil || *decoded.Consensus.NotarizedSlot != math.MaxUint64 {
		t.Fatal("maximum-width accepted certificate frontier lost")
	}
	if decoded.BlockSource.Turbine.Diagnostics.PeerPayloadBytes == nil || *decoded.BlockSource.Turbine.Diagnostics.PeerPayloadBytes != math.MaxUint64 {
		t.Fatal("maximum-width peer payload bytes lost")
	}
	f, err := frames(&s, "fresh", now)
	if err != nil {
		t.Fatal(err)
	}
	values := decodedFrames(t, &frameSet{messages: f})
	for _, group := range []string{"runtime", "resources", "replay", "consensus", "rpc", "recovery", "block_source", "pipeline", "snapshot_archive"} {
		if value := values["mithril."+group]; len(value) == 0 || string(value) == "null" {
			t.Fatalf("group %s lost in projection", group)
		}
	}
}

func TestPrivateSnapshotAbovePublicLimitPassesFeedAndCollector(t *testing.T) {
	now := sizingTime(time.Now())
	s := fullTestSnapshot(now)
	s.SnapshotArchive, s.IncrementalArchive = nil, maximumIncrementalArchive(now)
	// Network labels are unbounded input; the public projection normalizes
	// unknown labels. Exercise the private limit independently of optional data.
	s.Network = ""
	base, err := json.Marshal(s)
	if err != nil || len(base) >= MaxSnapshotBytes {
		t.Fatalf("invalid size fixture: %d bytes, %v", len(base), err)
	}
	s.Network = strings.Repeat("n", MaxSnapshotBytes-len(base)+128)
	b, err := json.Marshal(s)
	if err != nil || len(b) <= MaxSnapshotBytes || len(b) > MaxPrivateSnapshotBytes || s.validate() != nil {
		t.Fatalf("fixture does not exercise separate limits: %d bytes, %v", len(b), err)
	}
	path := socketPath(t)
	feed, err := ListenFeed(context.Background(), path, func() Snapshot { return s })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	c := newCollector(path)
	t.Cleanup(c.client.CloseIdleConnections)
	c.poll(context.Background(), now)
	if c.last == nil || statusFields(t, c.latest.Load())["source_state"] != "fresh" {
		t.Fatal("bounded private snapshot was not accepted")
	}
	if c.last.BlockSource.Turbine.Diagnostics.Admission == nil {
		t.Fatal("collector lost receiver admission counters")
	}
	public := newCollector(path)
	public.public = true
	t.Cleanup(public.client.CloseIdleConnections)
	public.poll(context.Background(), now)
	var projected PublicSnapshot
	if public.last == nil || public.latest.Load() == nil ||
		json.Unmarshal(public.latest.Load().publicJSON, &projected) != nil ||
		projected.SourceState != "fresh" || projected.Node == nil || projected.Node.Network != "unknown" {
		t.Fatal("public collector did not normalize the long private network label")
	}
}

func testSnapshot(now time.Time) Snapshot {
	return Snapshot{
		SchemaVersion: SchemaVersion, RunID: "test-run", SampledAt: now,
		StartedAt: now.Add(-time.Hour), Version: "dev", Commit: "abc123",
		Network: "alpenglow", Alpenglow: true, Mode: "verifying", Phase: "replaying",
	}
}

func decodedFrames(t *testing.T, set *frameSet) map[string]json.RawMessage {
	t.Helper()
	values := make(map[string]json.RawMessage)
	for _, b := range set.messages {
		var m struct {
			Topic string          `json:"topic"`
			Key   string          `json:"key"`
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		values[m.Topic+"."+m.Key] = m.Value
	}
	return values
}

func statusFields(t *testing.T, set *frameSet) map[string]any {
	t.Helper()
	var status map[string]any
	if err := json.Unmarshal(decodedFrames(t, set)["mithril.status"], &status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestFramesPreservePrecisionAndUnknownValues(t *testing.T) {
	now := time.Date(2026, 10, 29, 8, 0, 0, 123456789, time.UTC)
	s := testSnapshot(now)
	frontier := uint64(1<<53 + 1)
	s.ReplayFrontier = &frontier
	b, err := frames(&s, "fresh", now)
	if err != nil {
		t.Fatal(err)
	}
	set := &frameSet{messages: b}
	status := statusFields(t, set)
	if status["replay_frontier"] != "9007199254740993" {
		t.Fatalf("frontier lost precision: %v", status)
	}
	values := decodedFrames(t, set)
	if string(values["summary.server_time_nanos"]) != `"1793260800123456789"` {
		t.Fatalf("unexpected server time: %s", values["summary.server_time_nanos"])
	}
	if string(values["summary.identity_key"]) != "null" {
		t.Fatal("missing identity is not null")
	}
	if string(values["mithril.runtime"]) != "null" || string(values["mithril.resources"]) != "null" {
		t.Fatal("missing measurements must not fabricate healthy zeroes")
	}
	if string(values["summary.cluster"]) != `"unknown"` {
		t.Fatal("custom network must not be mislabeled as an FD cluster")
	}
	for name := range values {
		if strings.Contains(name, "completed_slot") || strings.Contains(name, "vote_slot") || strings.Contains(name, "finalized") {
			t.Fatalf("unsupported measurement: %s", name)
		}
	}
	frontier = 0
	b, _ = frames(&s, "fresh", now)
	if statusFields(t, &frameSet{messages: b})["replay_frontier"] != "0" {
		t.Fatal("known zero was hidden")
	}
	b, _ = frames(nil, "unavailable", now)
	status = statusFields(t, &frameSet{messages: b})
	if status["source_state"] != "unavailable" || status["mode"] != nil || status["rpc_configured"] != nil || status["replay_frontier"] != nil {
		t.Fatalf("unknown source fabricated values: %v", status)
	}
}

func TestResourceCountersKeepIntegerPrecision(t *testing.T) {
	now := time.Now().UTC()
	s := testSnapshot(now)
	value := uint64(1<<53 + 1)
	s.Runtime = &Runtime{SampledAt: now, HeapBytes: value}
	s.Resources = &Resources{SampledAt: now, RSSBytes: &value, Root: &Filesystem{TotalBytes: value}}
	b, err := frames(&s, "fresh", now)
	if err != nil {
		t.Fatal(err)
	}
	values := decodedFrames(t, &frameSet{messages: b})
	for _, key := range []string{"mithril.runtime", "mithril.resources"} {
		if !strings.Contains(string(values[key]), `"9007199254740993"`) {
			t.Fatalf("counter lost precision: %s", values[key])
		}
	}
}

func TestClusterUsesGenesisNotConfiguredNetwork(t *testing.T) {
	now := time.Now().UTC()
	for _, tt := range []struct{ genesis, want string }{
		{"", "unknown"},
		{"custom-genesis", "unknown"},
		{"5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d", "mainnet-beta"},
		{"EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG", "devnet"},
		{"4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY", "testnet"},
	} {
		for _, configured := range []string{"mainnet-beta", "alpenglow", "development"} {
			s := testSnapshot(now)
			s.Network = configured
			if tt.genesis != "" {
				s.GenesisHash = &tt.genesis
			}
			f, err := frames(&s, "fresh", now)
			if err != nil {
				t.Fatal(err)
			}
			values := decodedFrames(t, &frameSet{messages: f})
			if got := string(values["summary.cluster"]); got != `"`+tt.want+`"` {
				t.Errorf("genesis=%q, configured=%q: got %s, want %q", tt.genesis, configured, got, tt.want)
			}
			if got := statusFields(t, &frameSet{messages: f})["network"]; got != configured {
				t.Errorf("configured label changed: got %v, want %s", got, configured)
			}
		}
	}
}

func TestSnapshotValidation(t *testing.T) {
	now := time.Now().UTC()
	for name, change := range map[string]func(*Snapshot){
		"version":    func(s *Snapshot) { s.SchemaVersion++ },
		"run":        func(s *Snapshot) { s.RunID = "" },
		"long run":   func(s *Snapshot) { s.RunID = strings.Repeat("x", 129) },
		"start":      func(s *Snapshot) { s.StartedAt = time.Time{} },
		"time order": func(s *Snapshot) { s.StartedAt = now.Add(time.Second) },
		"mode":       func(s *Snapshot) { s.Mode = "healthy" },
		"phase":      func(s *Snapshot) { s.Phase = "caught-up" },
	} {
		t.Run(name, func(t *testing.T) {
			s := testSnapshot(now)
			if err := s.validate(); err != nil {
				t.Fatal(err)
			}
			change(&s)
			if s.validate() == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
}
