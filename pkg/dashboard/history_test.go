package dashboard

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

func testHistoryRecord(at time.Time, value uint64) historyRecord {
	text := strconv.FormatUint(value, 10)
	r := historyRecord{Version: 1, At: at.Truncate(time.Minute).Unix(), ObservedAt: at.UnixMilli(), SourceAt: at.UnixMilli(),
		Binding: historyBinding{Genesis: "genesis", Identity: "identity", Run: "run"}, Role: "verifying", Attempt: 1, ReplayStartedAt: at.Add(-time.Hour).UnixMilli()}
	r.Values[0], r.GroupAt[0] = &text, at.UnixMilli()
	return r
}

func TestHistoryRecordExactIntegersAndCorruption(t *testing.T) {
	r := testHistoryRecord(time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC), math.MaxUint64)
	line, err := encodeHistoryRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeHistoryRecord(line)
	if err != nil || *got.Values[0] != "18446744073709551615" {
		t.Fatalf("round trip: %v %v", got, err)
	}
	for _, bad := range [][]byte{line[:len(line)-1], append(append([]byte{}, line...), '\n'), bytes.Replace(line, []byte("18446744073709551615"), []byte("18446744073709551614"), 1)} {
		if _, err := decodeHistoryRecord(bad); err == nil {
			t.Fatal("invalid line accepted")
		}
	}
	for _, value := range []string{"01", "-1", "18446744073709551616", "NaN"} {
		r.Values[0] = &value
		if _, err := encodeHistoryRecord(r); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestChainHistoryExtractionCompatibilityAndGaps(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	for _, role := range []string{"validator", "verifying"} {
		p := PublicSnapshot{SourceState: "fresh", SampledAt: &now,
			Node: &PublicNode{RunID: "run", Mode: role},
			Replay: &PublicReplay{PublicObservation: PublicObservation{State: "fresh", SampledAt: now}, Attempt: 1, StartedAt: now.Add(-time.Minute),
				ChainActivity: &PublicChainActivity{PublicObservation: PublicObservation{State: "fresh", SampledAt: now}, Provenance: "selected_and_folded", TimeSource: "bank_clock",
					Interval: &PublicActivityInterval{StartTime: 1000, EndTime: 1010, Transactions: 20, NonVoteTransactions: 18,
						FailedTransactions: 3, FailedNonVote: 2, ComputeUnits: 3000}}}}
		r, ok := historyPublicRecord(p, now, false)
		want := [historyChainMetrics]string{"2", "0.1", "0.1", "1.6", "0.2", "300"}
		if !ok || r.Version != 2 || r.Chain == nil || r.Chain.Values != want {
			t.Fatalf("%s: selected-chain rates: %+v", role, r)
		}
		line, err := encodeHistoryRecord(r)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeHistoryRecord(line)
		if err != nil || got.Chain == nil || got.Chain.Values != want {
			t.Fatalf("%s: round trip: %+v %v", role, got, err)
		}
		old := testHistoryRecord(now.Add(-time.Minute), 1)
		old.Binding = r.Binding
		old.Role = r.Role
		old.Attempt, old.ReplayStartedAt = r.Attempt, r.ReplayStartedAt
		oldLine, err := encodeHistoryRecord(old)
		if err != nil || bytes.Contains(oldLine, []byte(`"chain"`)) {
			t.Fatalf("legacy encoding changed: %s %v", oldLine, err)
		}
		if _, err := decodeHistoryRecord(oldLine); err != nil {
			t.Fatalf("legacy record rejected: %v", err)
		}
		bins := historyBuckets([]historyRecord{old, got}, r.Binding, historyBaseMetrics, historyRange{Name: "test", Seconds: 120, BucketSeconds: 60}, now)
		if bins[0].Count != 0 || !bins[0].Break || bins[1].Last == nil || *bins[1].Last != "2" {
			t.Fatalf("old record should remain unknown: %+v", bins)
		}
		p.Replay.ChainActivity.State = "stale"
		stale, ok := historyPublicRecord(p, now, false)
		if !ok || stale.Version != 1 || stale.Chain != nil {
			t.Fatal("stale interval recorded")
		}
		p.Replay.ChainActivity.State = "fresh"
		p.Replay.ChainActivity.Interval.FailedNonVote = 99
		invalid, ok := historyPublicRecord(p, now, false)
		if !ok || invalid.Version != 1 || invalid.Chain != nil {
			t.Fatal("invalid partition recorded")
		}
		p.Replay.ChainActivity.Interval.Transactions = 9007199254741001
		p.Replay.ChainActivity.Interval.NonVoteTransactions = 9007199254741000
		p.Replay.ChainActivity.Interval.FailedTransactions = 9007199254741000
		p.Replay.ChainActivity.Interval.FailedNonVote = 9007199254740999
		large, ok := historyPublicRecord(p, now, false)
		if !ok || large.Chain == nil || large.Chain.Values[2] != "0.1" || large.Chain.Values[3] != "0.1" {
			t.Fatal("subtraction lost precision before rate conversion")
		}
	}
}

func TestChainHistoryRejectsInvalidVersionAndValues(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	valid := testHistoryRecord(now, 1)
	valid.Version = 2
	valid.Chain = &historyChainRecord{At: now.UnixMilli(), Values: [historyChainMetrics]string{"0", "0", "0", "0", "0", "0"}}
	if _, err := encodeHistoryRecord(valid); err != nil {
		t.Fatal(err)
	}
	for _, damage := range []func(*historyRecord){
		func(r *historyRecord) { r.Version = 1 },
		func(r *historyRecord) { r.Chain = nil },
		func(r *historyRecord) { r.Chain.At = 0 },
		func(r *historyRecord) { r.Chain.Values[0] = "-0" },
		func(r *historyRecord) { r.Chain.Values[0] = "NaN" },
	} {
		r := valid
		chain := *valid.Chain
		r.Chain = &chain
		damage(&r)
		if _, err := encodeHistoryRecord(r); err == nil {
			t.Fatal("invalid extended history record accepted")
		}
	}
}

func TestHistoryMaximumRecordAndResponseBudgets(t *testing.T) {
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	r := testHistoryRecord(at, math.MaxUint64)
	r.Binding = historyBinding{Genesis: strings.Repeat("g", 128), Identity: strings.Repeat("i", 128), Run: strings.Repeat("r", 128)}
	r.Vote = strings.Repeat("v", 128)
	r.Attempt, r.Rewinds = math.MaxUint64, math.MaxUint64
	for i := range r.Values {
		v := "18446744073709551615"
		if historyMetrics[i].Kind == "number" {
			v = "1.7976931348623157e+308"
		}
		r.Values[i], r.GroupAt[i] = &v, at.UnixMilli()
	}
	line, err := encodeHistoryRecord(r)
	if err != nil {
		t.Fatalf("maximum record: %v", err)
	}
	if len(line)*1440 > historySegmentBytes {
		t.Fatalf("daily budget: %d", len(line)*1440)
	}
	r.Version = 2
	r.Chain = &historyChainRecord{At: at.UnixMilli()}
	for i := range r.Chain.Values {
		r.Chain.Values[i] = "1.7976931348623157e+308"
	}
	line, err = encodeHistoryRecord(r)
	if err != nil || len(line)*1440 > historySegmentBytes {
		t.Fatalf("maximum extended daily record: %d %v", len(line), err)
	}
	var records []historyRecord
	for i := 0; i < 1440; i++ {
		v := r
		v.At = at.Add(time.Duration(i) * time.Minute).Unix()
		v.ObservedAt = v.At * 1000
		v.SourceAt = v.ObservedAt
		records = append(records, v)
	}
	for _, span := range historyRanges {
		buckets := historyBuckets(records, r.Binding, 0, span, at.Add(1439*time.Minute))
		b, err := json.Marshal(buckets)
		if err != nil || len(b) > MaxSnapshotBytes-2048 || len(buckets) > 96 {
			t.Fatalf("response budget %s: %d %v", span.Name, len(b), err)
		}
	}
}

func TestHistoryBucketsBindingResetAndGaps(t *testing.T) {
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	a := testHistoryRecord(at, 9007199254740992)
	b := a
	b.At += 60
	b.ObservedAt += 60000
	v := "9007199254740993"
	b.Values[0] = &v
	span := historyRange{Name: "test", Seconds: 900, BucketSeconds: 900}
	bins := historyBuckets([]historyRecord{a, b}, a.Binding, 0, span, at.Add(time.Minute))
	if bins[0].Count != 2 || *bins[0].Max != v || !bins[0].Break {
		t.Fatalf("partial/exact: %+v", bins)
	}
	b.Binding.Run = "new-run"
	bins = historyBuckets([]historyRecord{a, b}, a.Binding, 0, span, at.Add(time.Minute))
	if bins[0].Count != 2 || bins[0].Last != nil || !bins[0].Break {
		t.Fatalf("joined runs: %+v", bins)
	}
	b.Binding.Identity = "other"
	bins = historyBuckets([]historyRecord{a, b}, a.Binding, 0, span, at.Add(time.Minute))
	if bins[0].Count != 1 {
		t.Fatal("joined identities")
	}
	a.Binding.Genesis = ""
	b = a
	b.Binding.Run = "other"
	if a.Binding.matches(b.Binding) {
		t.Fatal("joined unbound runs")
	}
}

func TestHistoryExtractionDisclosureAndFreshness(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	s := fullTestSnapshot(now)
	s.IdentityKey, s.GenesisHash = new("identity"), new("genesis")
	c := newCollector("/unused")
	c.public = true
	c.publicOptions.Resources = true
	encoded, err := c.encode(&s, "fresh", now)
	if err != nil {
		t.Fatal(err)
	}
	var p PublicSnapshot
	if err := json.Unmarshal(encoded.publicJSON, &p); err != nil {
		t.Fatal(err)
	}
	p.Resources = &PublicResources{State: "fresh", SampledAt: &now, RSSBytes: new(uint64(123)), CPU: &PublicCPURate{WindowEnd: now, PercentOneCore: 42}}
	r, ok := historyPublicRecord(p, now, true)
	if !ok || r.Values[5] == nil || r.Values[6] == nil {
		t.Fatalf("fresh resources: %+v", r)
	}
	r, ok = historyPublicRecord(p, now, false)
	if !ok || r.Values[5] != nil || r.Values[6] != nil {
		t.Fatal("resource disclosure")
	}
	p.SourceState = "stale"
	r, ok = historyPublicRecord(p, now, true)
	if !ok {
		t.Fatal("stale record lost")
	}
	for _, value := range r.Values {
		if value != nil {
			t.Fatal("stale value persisted")
		}
	}
}

func TestHistoryStartupWithoutReplayStartRetainsOtherGroups(t *testing.T) {
	now := time.Now().UTC()
	p := PublicSnapshot{SourceState: "fresh", SampledAt: &now,
		Node:      &PublicNode{RunID: "starting-run", Mode: "verifying"},
		Replay:    &PublicReplay{PublicObservation: PublicObservation{State: "fresh", SampledAt: now}},
		Resources: &PublicResources{State: "fresh", SampledAt: &now, RSSBytes: new(uint64(123))}}
	r, ok := historyPublicRecord(p, now, true)
	if !ok || r.Values[6] == nil || *r.Values[6] != "123" {
		t.Fatal("unknown replay start suppressed an independently valid resource sample")
	}
}
