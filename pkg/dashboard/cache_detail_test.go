package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func cacheDetailFixture(now time.Time) (Snapshot, CacheDetailCapture) {
	s := testSnapshot(now.UTC())
	empty, small := uint64(3), uint64(4)
	d := CacheDetail{SchemaVersion: 1, Family: "accounts_cache", Scope: "accountsdb_read_cache_gauges", State: "fresh",
		Binding: CacheDetailBinding{RunID: s.RunID, NodeStartedAt: s.StartedAt, CacheGeneration: 1}, CapturedAt: now.UTC(),
		Vote:    CacheVoteDetail{CapacityEntries: 2500},
		Program: CacheProgramDetail{CapacityCostUnits: 1024, CostUnitBytes: 1 << 20},
		Common: CacheCommonDetail{Layout: "fixed", ConfiguredBudgetBytes: 805306368,
			FixedEmptyCapacityEntries: 64, FixedSmallCapacityEntries: 128, LargeCapacityBytes: 721420288,
			FixedEmptyOccupiedEntries: &empty, FixedSmallOccupiedEntries: &small}}
	return s, CacheDetailCapture{Detail: d, FreshUntil: now.Add(CacheDetailFreshFor)}
}

func cacheInsertionWeightsFixture(program, large uint64) *CacheInsertionWeights {
	return &CacheInsertionWeights{Scope: "approximate_stored_insertion_weight", ProgramCostUnits: &program, CommonLargeAdmissionBytes: &large}
}

func cacheProgramSetsFixture(count uint64) *CacheProgramSets {
	return &CacheProgramSets{Scope: "accepted_program_cache_set_calls", Count: &count}
}

func TestCacheInsertionWeightsCanonical(t *testing.T) {
	_, capture := cacheDetailFixture(time.Now())
	capture.Detail.Activity = &CacheActivityDetail{Instance: 1, InsertionWeights: cacheInsertionWeightsFixture(0, math.MaxUint64)}
	for _, faulted := range []bool{false, true} {
		w := capture.Detail.Activity.InsertionWeights
		if faulted {
			w.ProgramCostUnits, w.ProgramReason = nil, "counter_fault"
		}
		body, err := json.Marshal(capture.Detail)
		if err != nil {
			t.Fatal(err)
		}
		if decoded, err := decodeCacheDetail(body); err != nil || !reflect.DeepEqual(decoded, capture.Detail) {
			t.Fatalf("insertion weights changed: %+v, %v", decoded.Activity, err)
		}
		for _, invalid := range [][]byte{
			bytes.Replace(body, []byte(`"insertion_weights":{`), []byte(`"insertion_weights":null,"ignored":{`), 1),
			bytes.Replace(body, []byte(`"scope":"approximate_stored_insertion_weight"`), []byte(`"scope":"other"`), 1),
			bytes.Replace(body, []byte(`"common_large_reason":""`), []byte(`"common_large_reason":"counter_fault"`), 1),
			bytes.Replace(body, []byte(`"common_large_admission_bytes":"18446744073709551615"`), []byte(`"common_large_admission_bytes":null`), 1),
			bytes.Replace(body, []byte(`"common_large_admission_bytes":"18446744073709551615"`), []byte(`"common_large_admission_bytes":"18446744073709551616"`), 1),
			bytes.Replace(body, []byte(`"common_large_reason":""`), []byte(`"common_large_reason":"","common_large_reason":""`), 1),
			bytes.Replace(body, []byte(`"insertion_weights":{`), []byte(`"insertion_weights":{"extra":0,`), 1),
		} {
			if _, err := decodeCacheDetail(invalid); err == nil {
				t.Fatalf("noncanonical insertion weights accepted: %s", invalid)
			}
		}
	}
	capture.Detail.Activity.InsertionWeights = nil
	body, err := json.Marshal(capture.Detail)
	if err != nil || bytes.Contains(body, []byte("insertion_weights")) {
		t.Fatal("older observation fabricated insertion weights")
	}
	if _, err := decodeCacheDetail(body); err != nil {
		t.Fatal("older producer rejected", err)
	}
	// There is no window in this fixture. Insert a null child in its activity.
	explicitNull := bytes.Replace(body, []byte(`"instance":"1",`), []byte(`"instance":"1","insertion_weights":null,`), 1)
	if _, err := decodeCacheDetail(explicitNull); err == nil {
		t.Fatal("explicit null optional child accepted")
	}
}

func TestCacheActivityWindowValidation(t *testing.T) {
	now := time.Now().UTC()
	_, capture := cacheDetailFixture(now)
	lookup := CacheLookupDetail{Hits: 8, Misses: 2}
	delta := CacheLookupDelta{Hits: 4, Misses: 1}
	capture.Detail.Activity = &CacheActivityDetail{Instance: 1, Vote: lookup, Program: lookup, CommonLarge: lookup,
		Window: &CacheActivityWindow{StartedAt: now.Add(-time.Second), EndedAt: now, DurationNanoseconds: uint64(time.Second),
			Vote: delta, Program: delta, CommonLarge: delta}}
	if err := validateCacheDetail(capture.Detail); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*CacheDetail){
		"zero_duration":  func(d *CacheDetail) { d.Activity.Window.DurationNanoseconds = 0 },
		"long_duration":  func(d *CacheDetail) { d.Activity.Window.DurationNanoseconds = uint64(time.Minute) + 1 },
		"early_start":    func(d *CacheDetail) { d.Activity.Window.StartedAt = d.Binding.NodeStartedAt.Add(-time.Nanosecond) },
		"changed_end":    func(d *CacheDetail) { d.Activity.Window.EndedAt = now.Add(-time.Nanosecond) },
		"vote_hits":      func(d *CacheDetail) { d.Activity.Window.Vote.Hits = 9 },
		"vote_misses":    func(d *CacheDetail) { d.Activity.Window.Vote.Misses = 3 },
		"program_hits":   func(d *CacheDetail) { d.Activity.Window.Program.Hits = 9 },
		"program_misses": func(d *CacheDetail) { d.Activity.Window.Program.Misses = 3 },
		"large_hits":     func(d *CacheDetail) { d.Activity.Window.CommonLarge.Hits = 9 },
		"large_misses":   func(d *CacheDetail) { d.Activity.Window.CommonLarge.Misses = 3 },
	} {
		t.Run(name, func(t *testing.T) {
			d := capture.Detail
			activity, window := *d.Activity, *d.Activity.Window
			d.Activity, activity.Window = &activity, &window
			change(&d)
			if err := validateCacheDetail(d); err == nil {
				t.Fatal("invalid observed cache interval accepted")
			}
		})
	}
	// UTC metadata is not the source clock used for the duration.
	capture.Detail.Activity.Window.StartedAt = now
	if err := validateCacheDetail(capture.Detail); err != nil {
		t.Fatal("wall-clock adjustment invalidated a monotonic interval", err)
	}
}

func TestCacheDetailRealFeedAndDefaults(t *testing.T) {
	snapshot, capture := cacheDetailFixture(time.Now())
	var calls, statusCalls atomic.Int64
	path := socketPath(t)
	f, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot {
		statusCalls.Add(1)
		return snapshot
	}, DetailSamplers{Cache: func(at time.Time) (CacheDetailCapture, error) {
		if at == at.UTC() {
			t.Error("feed stripped the original monotonic clock")
		}
		calls.Add(1)
		return capture, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.Close)
	client := newCollector(path).client
	t.Cleanup(client.CloseIdleConnections)
	status, body, header := replayDetailHTTP(t, client, "GET", "http://node/details/cache", "")
	d, err := decodeCacheDetail([]byte(body))
	if status != 200 || err != nil || !reflect.DeepEqual(d, capture.Detail) || len(body) > MaxCacheDetailBytes {
		t.Fatalf("complete cache document: status=%d error=%v body=%s", status, err, body)
	}
	remaining, err := strconv.Atoi(header.Get(replayDetailRemainingHeader))
	if err != nil || remaining <= 0 || remaining > 30000 || header.Get("Cache-Control") != "no-store" || header.Get("Content-Type") != "application/json" {
		t.Fatal("invalid cache or original-deadline headers")
	}
	for range 20 {
		code, next, h := replayDetailHTTP(t, client, "GET", "http://node/details/cache", "")
		left, err := strconv.Atoi(h.Get(replayDetailRemainingHeader))
		if code != 200 || next != body || err != nil || left > remaining {
			t.Fatal("cached request changed or renewed the source capture")
		}
		remaining = left
	}
	status, ordinary, _ := replayDetailHTTP(t, client, "GET", "http://node/snapshot", "")
	var decoded Snapshot
	if status != 200 || len(ordinary) > MaxSnapshotBytes || json.Unmarshal([]byte(ordinary), &decoded) != nil || decoded.validate() != nil || strings.Contains(ordinary, "cache_generation") {
		t.Fatal("cache route changed the ordinary status document")
	}
	for _, request := range []struct{ method, path, body string }{
		{"POST", "/details/cache", ""}, {"GET", "/details/cache?", ""}, {"GET", "/details/cache?x=1", ""},
		{"GET", "/details/cache", "x"}, {"GET", "/details/%63ache", ""},
	} {
		code, result, h := replayDetailHTTP(t, client, request.method, "http://node"+request.path, request.body)
		if code != 400 || result != `{"schema_version":1,"family":"accounts_cache","state":"unavailable","reason":"invalid_request"}` || len(result) > 512 || h.Get(replayDetailRemainingHeader) != "" {
			t.Fatalf("invalid request accepted: %d %s", code, result)
		}
	}
	if f.cache == nil || f.details != nil || f.network != nil || f.server.MaxHeaderBytes != 4096 || f.server.ReadTimeout != 2*time.Second || f.server.WriteTimeout != 2*time.Second {
		t.Fatal("cache option changed existing workers or server bounds")
	}
	f.Close()
	// Compare after joining the owner; slow CI may legitimately span more ticks.
	if calls.Load() != statusCalls.Load() {
		t.Fatal("viewers invoked a sampler outside accepted status cycles")
	}
	for _, constructor := range []func(string) (*Feed, error){
		func(path string) (*Feed, error) {
			return ListenFeed(context.Background(), path, func() Snapshot { return snapshot })
		},
		func(path string) (*Feed, error) {
			return ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{})
		},
		func(path string) (*Feed, error) {
			return ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot { return snapshot }, nil)
		},
	} {
		path := socketPath(t)
		feed, err := constructor(path)
		if err != nil {
			t.Fatal(err)
		}
		client := newCollector(path).client
		code, _, _ := replayDetailHTTP(t, client, "GET", "http://node/details/cache", "")
		client.CloseIdleConnections()
		feed.Close()
		if code != 404 || feed.cache != nil {
			t.Fatal("default feed enabled cache details")
		}
	}
}

func TestCacheDetailCanonicalAndBounds(t *testing.T) {
	_, capture := cacheDetailFixture(time.Now())
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCacheDetail(body); err != nil {
		t.Fatal(err)
	}
	wide := capture.Detail
	wide.Binding.RunID = strings.Repeat("<", 128)
	genesis := "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4S6DYF4wW"
	wide.Binding.GenesisHash, wide.Binding.CacheGeneration = &genesis, math.MaxUint64-1
	at := time.Date(9999, 12, 31, 23, 59, 59, 123456789, time.UTC)
	wide.Binding.NodeStartedAt, wide.CapturedAt = at, at
	wide.Vote = CacheVoteDetail{CapacityEntries: math.MaxUint64, EstimatedEntries: math.MaxUint64}
	wide.Program = CacheProgramDetail{CapacityCostUnits: math.MaxUint64, CostUnitBytes: 1 << 20, EstimatedEntries: math.MaxUint64}
	wide.Common = CacheCommonDetail{Layout: "fixed", ConfiguredBudgetBytes: math.MaxUint64,
		FixedEmptyCapacityEntries: math.MaxUint64, FixedSmallCapacityEntries: math.MaxUint64,
		LargeCapacityBytes: math.MaxUint64 - 1, LargeEstimatedEntries: math.MaxUint64}
	occupied := uint64(math.MaxUint64)
	wide.Common.FixedEmptyOccupiedEntries, wide.Common.FixedSmallOccupiedEntries = &occupied, &occupied
	lookup := CacheLookupDetail{Hits: math.MaxUint64, Misses: math.MaxUint64, Evictions: math.MaxUint64, EvictionWeight: math.MaxUint64}
	wide.Activity = &CacheActivityDetail{Instance: math.MaxUint64 - 1, Vote: lookup, Program: lookup, CommonLarge: lookup,
		Fixed: &CacheFixedActivityDetail{EmptyHits: math.MaxUint64, SmallHits: math.MaxUint64, EmptyReplacements: math.MaxUint64, SmallReplacements: math.MaxUint64}}
	delta := CacheLookupDelta{Hits: math.MaxUint64, Misses: math.MaxUint64}
	wide.Binding.NodeStartedAt = at.Add(-time.Minute)
	wide.Activity.Window = &CacheActivityWindow{StartedAt: at.Add(-time.Minute), EndedAt: at, DurationNanoseconds: uint64(time.Minute), Vote: delta, Program: delta, CommonLarge: delta}
	wide.Activity.InsertionWeights = cacheInsertionWeightsFixture(math.MaxUint64, math.MaxUint64)
	wide.Activity.ProgramSets = cacheProgramSetsFixture(math.MaxUint64 - 1)
	refused := uint64(math.MaxUint64 - 1)
	wide.Activity.ProgramRefusals = &CacheProgramSets{Scope: "oversized_program_cache_set_refusals", Count: &refused}
	for _, layout := range []string{"fixed", "flat"} {
		wide.Common.Layout = layout
		if layout == "flat" {
			occupied = 0
			wide.Common.FixedEmptyCapacityEntries, wide.Common.FixedSmallCapacityEntries = 0, 0
			wide.Common.LargeCapacityBytes = wide.Common.ConfiguredBudgetBytes
			wide.Activity.Fixed = &CacheFixedActivityDetail{}
		}
		encoded, err := json.Marshal(wide)
		if err != nil || len(encoded) > MaxCacheDetailBytes {
			t.Fatalf("maximum-width %s body: %d %v", layout, len(encoded), err)
		}
		if decoded, err := decodeCacheDetail(encoded); err != nil || !reflect.DeepEqual(decoded, wide) {
			t.Fatalf("maximum-width %s changed: %v", layout, err)
		}
		t.Logf("maximum-width %s private cache body: %d/%d bytes", layout, len(encoded), MaxCacheDetailBytes)
		family, err := preparePublicCacheDetail(wide, at, at.Add(time.Second))
		if err != nil {
			t.Fatalf("maximum-width %s public family: %v", layout, err)
		}
		var total int
		for _, body := range family.bodies {
			total += len(body)
		}
		t.Logf("maximum-width %s public family: %d/%d bytes", layout, total, cacheDetailFamilyBytes)
	}
	for _, bad := range [][]byte{
		append([]byte(" "), body...), append(append([]byte{}, body...), []byte("{}")...),
		bytes.Replace(body, []byte(`"schema_version":1`), []byte(`"schema_version":1,"schema_version":1`), 1),
		bytes.Replace(body, []byte(`"reason":"",`), nil, 1),
		bytes.Replace(body, []byte(`"binding":{`), []byte(`"binding":{"unknown":0,`), 1),
		bytes.Replace(body, []byte(`"genesis_hash":null,`), nil, 1),
		bytes.Replace(body, []byte(`"cache_generation":"1"`), []byte(`"cache_generation":"01"`), 1),
		bytes.Replace(body, []byte(`"capacity_entries":"2500"`), []byte(`"capacity_entries":2500`), 1),
		bytes.Replace(body, []byte(`"estimated_entries":"0"`), []byte(`"estimated_entries":null`), 1),
		bytes.Replace(body, []byte(`"estimated_entries":"0"`), []byte(`"estimated_entries":"-1"`), 1),
		bytes.Replace(body, []byte(`"estimated_entries":"0"`), []byte(`"estimated_entries":"18446744073709551616"`), 1),
		bytes.Replace(body, []byte(`"program":{`), []byte(`"program":{"unknown":0,`), 1),
		bytes.Replace(body, []byte(`"fixed_empty_capacity_entries":"64",`), nil, 1),
		bytes.Replace(body, []byte(`"large_estimated_entries":"0"`), []byte(`"large_estimated_entries":"0","large_estimated_entries":"0"`), 1),
		bytes.Replace(body, []byte(`"family":"accounts_cache"`), []byte(`"family":"accounts_\u0063ache"`), 1),
		bytes.Replace(body, []byte(`Z"`), []byte(`+00:00"`), 1),
		bytes.Repeat([]byte(" "), MaxCacheDetailBytes+1),
	} {
		if _, err := decodeCacheDetail(bad); err == nil {
			t.Fatalf("invalid cache JSON accepted: %s", bad[:min(len(bad), 120)])
		}
	}
	for _, mutate := range []func(*CacheDetail){
		func(d *CacheDetail) { d.Binding.CacheGeneration = 0 },
		func(d *CacheDetail) { d.Binding.CacheGeneration = math.MaxUint64 },
		func(d *CacheDetail) { d.Binding.RunID = strings.Repeat("a", 129) },
		func(d *CacheDetail) { d.Binding.RunID = "\xff" },
		func(d *CacheDetail) { d.Binding.GenesisHash = new(string) },
		func(d *CacheDetail) { d.CapturedAt = d.Binding.NodeStartedAt.Add(-time.Nanosecond) },
		func(d *CacheDetail) { d.CapturedAt = time.Time{} },
		func(d *CacheDetail) { d.Vote.CapacityEntries = 0 },
		func(d *CacheDetail) { d.Program.CapacityCostUnits = 0 },
		func(d *CacheDetail) { d.Program.CostUnitBytes = 1024 },
		func(d *CacheDetail) { d.Common.Layout = "unknown" },
		func(d *CacheDetail) { d.Common.FixedEmptyCapacityEntries = 0 },
		func(d *CacheDetail) { d.Common.FixedSmallCapacityEntries = 0 },
		func(d *CacheDetail) { d.Common.LargeCapacityBytes = 0 },
		func(d *CacheDetail) { d.Common.LargeCapacityBytes = d.Common.ConfiguredBudgetBytes },
		func(d *CacheDetail) { d.Common.ConfiguredBudgetBytes = 0 },
		func(d *CacheDetail) { d.Common.Layout = "flat" },
		func(d *CacheDetail) { d.Common.FixedEmptyOccupiedEntries = nil },
		func(d *CacheDetail) { d.Common.FixedSmallOccupiedEntries = nil },
		func(d *CacheDetail) { d.Common.FixedEmptyCapacityEntries = 1 },
		func(d *CacheDetail) { d.Common.FixedSmallCapacityEntries = 1 },
	} {
		_, c := cacheDetailFixture(time.Now())
		mutate(&c.Detail)
		if validateCacheDetail(c.Detail) == nil {
			t.Fatal("invalid source cache shape accepted")
		}
	}
	// Counts are approximate, not utilization constrained to a configured cap.
	_, c := cacheDetailFixture(time.Now())
	c.Detail.Vote.EstimatedEntries, c.Detail.Program.EstimatedEntries, c.Detail.Common.LargeEstimatedEntries = math.MaxUint64, math.MaxUint64, math.MaxUint64
	if validateCacheDetail(c.Detail) != nil {
		t.Fatal("approximate counts above capacity rejected")
	}
}

func TestCacheDetailSourcePublicationAndRefusals(t *testing.T) {
	now := time.Now()
	snapshot, capture := cacheDetailFixture(now)
	genesis := "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4S6DYF4wW"
	snapshot.GenesisHash, capture.Detail.Binding.GenesisHash = &genesis, &genesis
	capture.FreshUntil = now.Add(20 * time.Second)
	s := &cacheDetailSource{sample: func(time.Time) (CacheDetailCapture, error) { return capture, nil }}
	read := func() (int, string, string) {
		w := httptest.NewRecorder()
		s.serveHTTP(w, httptest.NewRequest("GET", "/details/cache", nil))
		return w.Code, w.Body.String(), w.Header().Get(replayDetailRemainingHeader)
	}
	s.update(snapshot, now)
	status, original, _ := read()
	if status != 200 {
		t.Fatal(original)
	}
	deadline := s.latest.Load().freshUntil
	capture.FreshUntil = now.Add(25 * time.Second)
	s.update(snapshot, time.Now())
	if s.latest.Load().freshUntil != deadline {
		t.Fatal("identical capture renewed its deadline")
	}
	genesis = "caller-mutated-private-data"
	capture.Detail.Vote.EstimatedEntries = 99
	_, body, _ := read()
	if body != original || *s.gate.Load().binding.GenesisHash == genesis {
		t.Fatal("caller-owned fields changed the cached publication or gate")
	}
	genesis = "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4S6DYF4wW"
	capture.Detail.Binding.CacheGeneration++
	s.update(snapshot, time.Now())
	_, body, _ = read()
	if !strings.Contains(body, `"cache_generation":"2"`) || !strings.Contains(body, `"estimated_entries":"99"`) {
		t.Fatal("same-run generation replacement was not published")
	}
	for _, mutate := range []func(*Snapshot){
		func(s *Snapshot) { s.RunID = "replacement" },
		func(s *Snapshot) { s.StartedAt = s.StartedAt.Add(time.Second) },
		func(s *Snapshot) { s.GenesisHash = nil },
	} {
		changed := snapshot
		mutate(&changed)
		g := cacheGate(changed, time.Now())
		s.gate.Store(&g)
		code, result, h := read()
		if code != 503 || !strings.Contains(result, `"reason":"binding_changed"`) || h != "" || strings.Contains(result, "capacity_entries") {
			t.Fatal("current status binding did not fence old gauges")
		}
	}
	for _, reason := range []string{"waiting_source", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "binding_changed", "response_limit", "invalid_request", "private-path-secret", "unsupported_source"} {
		s.sample = func(time.Time) (CacheDetailCapture, error) { return CacheDetailCapture{}, errors.New(reason) }
		s.update(snapshot, time.Now())
		code, result, h := read()
		want := cacheDetailReason(reason)
		if code != 503 || result != `{"schema_version":1,"family":"accounts_cache","state":"unavailable","reason":"`+want+`"}` || len(result) > 512 || h != "" || s.latest.Load().body != "" {
			t.Fatalf("source refusal retained data or leaked error: %d %s", code, result)
		}
	}
	for _, observedAt := range []time.Time{time.Now().Add(-StaleAfter - time.Second), time.Now().Add(time.Second), {}} {
		g := cacheGate(snapshot, observedAt)
		s.gate.Store(&g)
		code, result, h := read()
		if code != 503 || !strings.Contains(result, g.failure(time.Now())) || h != "" {
			t.Fatal("old source failure overrode current status gate")
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*CacheDetailCapture)
	}{
		{"zero deadline", func(c *CacheDetailCapture) { c.FreshUntil = time.Time{} }},
		{"expired deadline", func(c *CacheDetailCapture) { c.FreshUntil = time.Now().Add(-time.Second) }},
		{"extended deadline", func(c *CacheDetailCapture) { c.FreshUntil = time.Now().Add(31 * time.Second) }},
		{"ancient capture", func(c *CacheDetailCapture) { c.Detail.CapturedAt = time.Now().Add(-CacheDetailFreshFor).UTC() }},
		{"future capture", func(c *CacheDetailCapture) { c.Detail.CapturedAt = time.Now().Add(2 * time.Second).UTC() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s.sample = func(at time.Time) (CacheDetailCapture, error) {
				_, c := cacheDetailFixture(at)
				c.Detail.Binding = capture.Detail.Binding
				test.mutate(&c)
				return c, nil
			}
			s.update(snapshot, time.Now())
			if s.latest.Load().reason != "source_time_invalid" {
				t.Fatalf("invalid clock accepted: %+v", s.latest.Load())
			}
		})
	}
}

func TestCacheDetailExpiryAndConcurrentOwnerCapture(t *testing.T) {
	snapshot, capture := cacheDetailFixture(time.Now())
	capture.FreshUntil = time.Now().Add(30 * time.Millisecond)
	s := &cacheDetailSource{sample: func(time.Time) (CacheDetailCapture, error) { return capture, nil }}
	s.update(snapshot, time.Now())
	time.Sleep(time.Until(capture.FreshUntil) + time.Millisecond)
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest("GET", "/details/cache", nil))
	if w.Code != 503 || !strings.Contains(w.Body.String(), `"reason":"status_stale"`) || w.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatal("expired owner capture remained fresh")
	}
	var deadline time.Time
	path := socketPath(t)
	f, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{Cache: func(time.Time) (CacheDetailCapture, error) {
		time.Sleep(10 * time.Millisecond)
		_, current := cacheDetailFixture(time.Now())
		current.Detail.Binding = capture.Detail.Binding
		deadline = current.FreshUntil
		return current, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.Close)
	client := newCollector(path).client
	t.Cleanup(client.CloseIdleConnections)
	code, body, _ := replayDetailHTTP(t, client, "GET", "http://node/details/cache", "")
	if code != 200 {
		t.Fatalf("post-cycle-start owner capture rejected: %d %s", code, body)
	}
	f.Close()
	if f.cache.latest.Load().freshUntil != deadline {
		t.Fatal("post-copy acceptance changed the owner deadline")
	}
}

func TestCacheDetailRejectedStatusDoesNotSample(t *testing.T) {
	var calls atomic.Int64
	path := socketPath(t)
	f, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return Snapshot{} }, DetailSamplers{Cache: func(time.Time) (CacheDetailCapture, error) {
		calls.Add(1)
		return CacheDetailCapture{}, errors.New("should not run")
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.Close)
	client := newCollector(path).client
	t.Cleanup(client.CloseIdleConnections)
	code, body, h := replayDetailHTTP(t, client, "GET", "http://node/details/cache", "")
	f.Close()
	if calls.Load() != 0 || f.cache.latest.Load() != nil || f.cache.gate.Load() != nil || code != 503 || !strings.Contains(body, `"reason":"waiting_source"`) || h.Get(replayDetailRemainingHeader) != "" {
		t.Fatal("rejected status invoked or published a cache capture")
	}
}
