package dashboard

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func replayDetailTestGate(c ReplayDetailCore, now time.Time) replayDetailGate {
	b := c.Binding
	g := replayDetailGate{RunID: b.RunID, NodeStartedAt: b.NodeStartedAt, Attempt: b.Attempt, Rewinds: b.Rewinds, AttemptStartedAt: b.AttemptStartedAt, ObservedAt: now, State: "fresh", stamp: statusStamp{nodeEpoch: 1, familyEpoch: 1}}
	if b.GenesisHash != nil {
		g.HasGenesis, g.GenesisHash = true, *b.GenesisHash
	}
	return g
}

func replayDetailTestPrepared(t *testing.T, c ReplayDetailCore) *replayDetailPrepared {
	t.Helper()
	p, err := prepareReplayDetail(c, c.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func replayDetailAssertReply(t *testing.T, got replayDetailReply, status int, reason string) {
	t.Helper()
	if got.status != status || reason != "" && !strings.Contains(got.body, `"reason":"`+reason+`"`) || len(got.body) > MaxSnapshotBytes {
		t.Fatalf("reply status=%d body=%s, want status=%d reason=%s", got.status, got.body, status, reason)
	}
}

func TestReplayDetailCachePagesAndDetachedEncoding(t *testing.T) {
	for _, count := range []int{0, 1, 3, 256} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			core := replayDetailTestCore(count)
			key := "CktRuQ2mttgRGkXJtyksdKHjUdc2C4TgDzyB98oEzy8"
			core.Binding.GenesisHash = &key
			p := replayDetailTestPrepared(t, core)
			var cache replayDetailCache
			now := core.CapturedAt.Add(time.Second)
			gate := replayDetailTestGate(core, now)
			if !cache.canPublish(gate, now) || cache.publish(p, gate, now) != nil {
				t.Fatal("publication rejected")
			}
			got := cache.lookup("", false, gate, now)
			replayDetailAssertReply(t, got, 200, "")
			if got.remainingMilliseconds != 29000 {
				t.Fatal("capture freshness was reset at publication")
			}
			var m ReplayDetailManifest
			if err := json.Unmarshal([]byte(got.body), &m); err != nil {
				t.Fatal(err)
			}
			canonical, err := json.Marshal(m)
			if err != nil || string(canonical) != got.body || m.Count != count || m.PageCount != (count+1)/2 || m.RetainUntil != core.CapturedAt.Add(30*time.Second) {
				t.Fatal("manifest shape, count or advisory date changed")
			}
			rows := make([]ReplayDetailRecord, 0, count)
			for page := 0; page < m.PageCount; page++ {
				query := "generation=" + m.Generation + "&page=" + strconv.Itoa(page)
				response := cache.lookup(query, false, gate, now)
				replayDetailAssertReply(t, response, 200, "")
				var wire ReplayDetailPage
				if err := json.Unmarshal([]byte(response.body), &wire); err != nil {
					t.Fatal(err)
				}
				body, err := json.Marshal(wire)
				if err != nil || string(body) != response.body || wire.Page != page || wire.Offset != page*2 || wire.Generation != m.Generation || !sameReplayDetailBinding(wire.Binding, m.Binding) || len(wire.Records) != min(2, count-page*2) {
					t.Fatal("page metadata or typed canonical encoding changed")
				}
				if page+1 == m.PageCount && wire.NextPage != nil || page+1 < m.PageCount && (wire.NextPage == nil || *wire.NextPage != page+1) {
					t.Fatal("page continuation changed")
				}
				rows = append(rows, wire.Records...)
			}
			if !reflect.DeepEqual(rows, core.Records) {
				t.Fatal("pages lost or changed source rows")
			}
			key = strings.Repeat("1", 32)
			if count > 0 {
				core.Records[0].Timings.SlotReplay.Count++
			}
			if cache.lookup("", false, gate, now).body != got.body || p.binding.GenesisHash == core.Binding.GenesisHash {
				t.Fatal("caller mutation changed publication")
			}
			replayDetailAssertReply(t, cache.lookup("generation="+m.Generation+"&page="+strconv.Itoa(m.PageCount), false, gate, now), 400, "invalid_request")
		})
	}
}

func TestReplayDetailCacheLeasesAndCapacity(t *testing.T) {
	core := replayDetailTestCore(1)
	t0 := core.CapturedAt
	var cache replayDetailCache
	var prepared [3]*replayDetailPrepared
	var installed [3]*replayDetailPrepared
	for i := range prepared {
		now := t0.Add(time.Duration(i) * 10 * time.Second)
		core.CapturedAt = now
		prepared[i] = replayDetailTestPrepared(t, core)
		gate := replayDetailTestGate(core, now)
		if !cache.canPublish(gate, now) || cache.publish(prepared[i], gate, now) != nil {
			t.Fatal("three bounded captures should fit")
		}
		installed[i] = cache.items[i].prepared
	}
	now := t0.Add(25 * time.Second)
	gate := replayDetailTestGate(core, now)
	if cache.canPublish(gate, now) {
		t.Fatal("unexpired full holder advertised capacity")
	}
	core.CapturedAt = now
	if err := cache.publish(replayDetailTestPrepared(t, core), gate, now); err == nil || err.Error() != "generation_not_ready" {
		t.Fatal("commit did not recheck capacity")
	}
	if err := cache.publish(prepared[0], gate, now); err != nil {
		t.Fatal("exact-token retry failed", err)
	}
	if cache.items[0].holdUntil != t0.Add(30*time.Second) || cache.items[0].prepared != installed[0] {
		t.Fatal("retry renewed or replaced retained generation")
	}
	for i, p := range prepared {
		replayDetailAssertReply(t, cache.lookup("generation="+p.generation+"&page=0", false, gate, now), 200, "")
		if cache.items[i].prepared != installed[i] {
			t.Fatal("request replaced cached bytes")
		}
	}
	now = t0.Add(30 * time.Second)
	gate.ObservedAt = now
	replayDetailAssertReply(t, cache.lookup("generation="+prepared[0].generation+"&page=0", false, gate, now), 409, "source_generation_expired")
	if !cache.canPublish(gate, now) {
		t.Fatal("expired holder not released")
	}
	core.CapturedAt = now
	if err := cache.publish(replayDetailTestPrepared(t, core), gate, now); err != nil {
		t.Fatal(err)
	}
	if len(cache.items) != 3 || cache.items[0].prepared != installed[1] || cache.items[1].prepared != installed[2] {
		t.Fatal("catch-up publication evicted an unexpired generation")
	}
}

func TestReplayDetailCacheFreshnessAndGate(t *testing.T) {
	core := replayDetailTestCore(1)
	t0 := core.CapturedAt
	p := replayDetailTestPrepared(t, core)
	var cache replayDetailCache
	gate := replayDetailTestGate(core, t0)
	if err := cache.publish(p, gate, t0); err != nil {
		t.Fatal(err)
	}
	for _, elapsed := range []time.Duration{0, time.Millisecond, 17 * time.Second, 17*time.Second + time.Nanosecond, 30 * time.Second} {
		now := t0.Add(elapsed)
		gate.ObservedAt = now
		reply := cache.lookup("", false, gate, now)
		if elapsed <= 17*time.Second {
			replayDetailAssertReply(t, reply, 200, "")
			if reply.remainingMilliseconds != (30*time.Second - elapsed).Milliseconds() {
				t.Fatal("remaining header did not use conservative millisecond floor")
			}
		} else {
			replayDetailAssertReply(t, reply, 503, "generation_not_ready")
		}
	}
	var delayed replayDetailCache
	now := t0.Add(17*time.Second + time.Nanosecond)
	gate.ObservedAt = now
	if err := delayed.publish(p, gate, now); err == nil || delayed.items[0] != nil {
		t.Fatal("uncollectable candidate consumed a holder")
	}
	now = t0.Add(17 * time.Second)
	gate.ObservedAt = now
	if err := delayed.publish(p, gate, now); err != nil || delayed.items[0].holdUntil != now.Add(30*time.Second) {
		t.Fatal("hold did not begin after encoding at commit")
	}
	for _, reason := range []string{"", "status_stale", "source_unavailable", "source_invalid"} {
		gate.State, gate.Reason = "stale", reason
		reply := delayed.lookup("", false, gate, now)
		replayDetailAssertReply(t, reply, 200, "")
		if !strings.Contains(reply.body, `"state":"stale"`) || reply.remainingMilliseconds != 13000 {
			t.Fatal("stale metadata renewed or relabelled capture")
		}
	}
	gate = replayDetailTestGate(core, t0)
	replayDetailAssertReply(t, cache.lookup("", false, gate, t0.Add(StaleAfter+time.Nanosecond)), 503, "status_stale")
	replayDetailAssertReply(t, cache.lookup("", false, gate, t0.Add(-time.Nanosecond)), 503, "source_time_invalid")
	for _, change := range []func(*replayDetailGate){
		func(g *replayDetailGate) { g.RunID += "-new" },
		func(g *replayDetailGate) { g.NodeStartedAt = g.NodeStartedAt.Add(time.Second) },
		func(g *replayDetailGate) { g.Attempt++ },
		func(g *replayDetailGate) { g.Rewinds++ },
		func(g *replayDetailGate) { g.AttemptStartedAt = g.AttemptStartedAt.Add(time.Second) },
		func(g *replayDetailGate) { g.HasGenesis, g.GenesisHash = true, strings.Repeat("1", 32) },
	} {
		changed := gate
		change(&changed)
		replayDetailAssertReply(t, cache.lookup("generation="+p.generation+"&page=0", false, changed, t0), 409, "binding_changed")
		if err := cache.publish(p, changed, t0); err == nil {
			t.Fatal("mismatched current gate accepted at commit")
		}
	}
	gate = replayDetailTestGate(core, t0)
	core.Binding.SourceGeneration++
	core.Binding.ResetAt = t0.Add(time.Second)
	core.CapturedAt = t0.Add(time.Second)
	gate.ObservedAt = core.CapturedAt
	if err := cache.publish(replayDetailTestPrepared(t, core), gate, core.CapturedAt); err != nil {
		t.Fatal(err)
	}
	replayDetailAssertReply(t, cache.lookup("generation="+p.generation+"&page=0", false, gate, core.CapturedAt), 409, "source_generation_expired")
	if err := cache.publish(p, gate, core.CapturedAt); err == nil {
		t.Fatal("source generation regressed")
	}
}

func TestReplayDetailCacheStrictQueriesAndFailures(t *testing.T) {
	generation := strings.Repeat("a", 64)
	for _, query := range []string{"generation=" + generation + "&page=0", "page=127&generation=" + generation} {
		g, _, pageRequest, ok := parseReplayDetailQuery(query, false)
		if !ok || !pageRequest || g != generation {
			t.Fatal("valid parameter order rejected")
		}
	}
	for _, query := range []string{
		"generation=" + generation, "page=0", "generation=" + generation + "&page=00", "generation=" + generation + "&page=128",
		"generation=" + generation + "&page=-1", "generation=" + generation + "&page=+1", "generation=" + generation + "&page=%31",
		"%67eneration=" + generation + "&page=0", "generation=" + strings.ToUpper(generation) + "&page=0",
		"generation=" + generation[:63] + "/&page=0", "generation=" + generation[:63] + ":&page=0", "generation=" + generation[:63] + "g&page=0",
		"generation=" + generation + "&page=0&extra=1", "generation=" + generation + "&generation=" + generation,
		"page=0&page=1", "generation=" + generation + "&page=1;extra=1", "generation=" + generation + "&page=1.0",
		"generation=" + strings.Repeat("a", 130) + "&page=0", "generation=" + generation + "&page=", "generation=&page=0",
	} {
		if _, _, _, ok := parseReplayDetailQuery(query, false); ok {
			t.Fatalf("invalid query accepted: %q", query)
		}
	}
	if _, _, _, ok := parseReplayDetailQuery("", true); ok {
		t.Fatal("empty forced query accepted")
	}
	core := replayDetailTestCore(0)
	now := core.CapturedAt
	gate := replayDetailTestGate(core, now)
	var cache replayDetailCache
	replayDetailAssertReply(t, cache.lookup("", false, gate, now), 503, "waiting_source")
	replayDetailAssertReply(t, (*replayDetailCache)(nil).lookup("", false, gate, now), 404, "capture_disabled")
	for _, reason := range []string{"capture_disabled", "unsupported_source", "waiting_source", "source_unavailable", "source_invalid", "source_time_invalid"} {
		gate.State, gate.Reason = "unavailable", reason
		status := 503
		if reason == "capture_disabled" || reason == "unsupported_source" {
			status = 404
		}
		replayDetailAssertReply(t, cache.lookup("", false, gate, now), status, reason)
		if cache.canPublish(gate, now) {
			t.Fatal("unavailable gate allowed capture")
		}
	}
	gate.Reason = "/private/secret"
	if strings.Contains(cache.lookup("", false, gate, now).body, "secret") {
		t.Fatal("caller error leaked")
	}
	if _, err := prepareReplayDetail(core, now.Add(time.Nanosecond)); err == nil {
		t.Fatal("capture clock does not match immutable core date")
	}
	core.Records = nil
	if _, err := prepareReplayDetail(core, now); err == nil {
		t.Fatal("invalid core prepared")
	}
}

func TestReplayDetailPrivateDTOsAreConcreteAndBounded(t *testing.T) {
	core := replayDetailTestCore(256)
	maximumReplayOptionalMetadata(&core)
	core.Binding.RunID = strings.Repeat("<", 128)
	core.Binding.SourceGeneration, core.Binding.Attempt, core.Binding.Rewinds = ^uint64(0), ^uint64(0), ^uint64(0)
	core.FirstSequence, core.LastSequence, core.Evicted = ^uint64(0)-255, ^uint64(0), ^uint64(0)-256
	hash := "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG"
	core.Binding.GenesisHash = &hash
	var fill func(reflect.Value)
	fill = func(v reflect.Value) {
		if v.Type() == reflect.TypeFor[time.Time]() {
			return
		}
		switch v.Kind() {
		case reflect.Struct:
			for i := range v.NumField() {
				fill(v.Field(i))
			}
		case reflect.Uint64:
			v.SetUint(^uint64(0))
		}
	}
	for i := range core.Records {
		fill(reflect.ValueOf(&core.Records[i]).Elem())
		core.Records[i].Sequence, core.Records[i].CompletedIndex = core.FirstSequence+uint64(i), core.FirstSequence+uint64(i)
		core.Records[i].Attempt, core.Records[i].Rewinds = core.Binding.Attempt, core.Binding.Rewinds
		core.Records[i].Bankhash = hash
		core.Records[i].RetentionLimits.Observed = true
		core.Records[i].Streaming.DiscardObserved = true
		core.Records[i].Streaming.LastDiscardReason = "generation_gone"
	}
	p := replayDetailTestPrepared(t, core)
	if len(p.pages) != 128 || p.bytes > replayDetailFamilyBytes {
		t.Fatal("full retained family exceeds bounds")
	}
	for _, page := range p.pages {
		if len(page) > MaxSnapshotBytes {
			t.Fatal("page exceeds existing response bound")
		}
	}
	// A whole-family response is intentionally not exposed; only bounded pages.
	if len(p.pages[0]) < 20000 || !strings.Contains(p.pages[0], `"count":"18446744073709551615"`) {
		t.Fatal("width test lost the raw timing columns")
	}
	var page ReplayDetailPage
	if err := json.Unmarshal([]byte(p.pages[0]), &page); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(page)
	if err != nil || !bytes.Equal(canonical, []byte(p.pages[0])) {
		t.Fatal("private typed page did not canonically round trip")
	}
	t.Logf("prepared pages=%d first_page_bytes=%d retained_encoding_bytes=%d", len(p.pages), len(p.pages[0]), p.bytes)
}

func TestReplayDetailCacheConcurrentLookups(t *testing.T) {
	core := replayDetailTestCore(3)
	now := core.CapturedAt
	p := replayDetailTestPrepared(t, core)
	gate := replayDetailTestGate(core, now)
	var cache replayDetailCache
	if err := cache.publish(p, gate, now); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				if cache.lookup("generation="+p.generation+"&page=0", false, gate, now).status != 200 {
					t.Error("cached lookup failed")
				}
				if err := cache.publish(p, gate, now); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if cache.items[1] != nil || cache.items[0].holdUntil != now.Add(30*time.Second) {
		t.Fatal("concurrent idempotent publish duplicated or renewed generation")
	}
	query := "generation=" + p.generation + "&page=0"
	if allocations := testing.AllocsPerRun(100, func() { cache.lookup(query, false, gate, now) }); allocations != 0 {
		t.Fatalf("cached page selection allocated %g objects", allocations)
	}
}

func TestReplayDetailCacheOwnsInstalledPublication(t *testing.T) {
	core := replayDetailTestCore(3)
	genesis := "CktRuQ2mttgRGkXJtyksdKHjUdc2C4TgDzyB98oEzy8"
	core.Binding.GenesisHash = &genesis
	now := core.CapturedAt
	p := replayDetailTestPrepared(t, core)
	gate := replayDetailTestGate(core, now)
	var cache replayDetailCache
	if err := cache.publish(p, gate, now); err != nil {
		t.Fatal(err)
	}
	query := "generation=" + p.generation + "&page=0"
	wantManifest, wantPage := cache.lookup("", false, gate, now), cache.lookup(query, false, gate, now)
	if wantManifest.status != 200 || wantPage.status != 200 {
		t.Fatal("baseline publication missing")
	}
	mutateCaller := func() {
		for i := range p.manifests {
			p.manifests[i] = `{"corrupt":true}`
		}
		for i := range p.pages {
			p.pages[i] = `{"corrupt":true}`
		}
		*p.binding.GenesisHash = strings.Repeat("1", 32)
		p.binding.RunID = "changed"
		p.binding.NodeStartedAt = p.binding.NodeStartedAt.Add(time.Second)
		p.binding.SourceGeneration++
		p.binding.Attempt++
		p.binding.Rewinds++
		p.binding.AttemptStartedAt = p.binding.AttemptStartedAt.Add(time.Second)
		p.binding.ResetAt = p.binding.ResetAt.Add(time.Second)
		p.generation = strings.Repeat("f", 64)
		p.capturedAt = p.capturedAt.Add(time.Second)
		p.lastSequence, p.bytes = 0, 0
	}
	mutateCaller()
	if got := cache.lookup("", false, gate, now); got != wantManifest {
		t.Fatal("caller mutation changed installed manifest")
	}
	if got := cache.lookup(query, false, gate, now); got != wantPage {
		t.Fatal("caller mutation changed installed page")
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			mutateCaller()
		}
	})
	for range 4 {
		wg.Go(func() {
			for range 100 {
				if cache.lookup("", false, gate, now) != wantManifest || cache.lookup(query, false, gate, now) != wantPage {
					t.Error("installed publication retained caller-owned state")
				}
			}
		})
	}
	wg.Wait()
}

func TestReplayDetailCacheCaptureClockAndRejectedGate(t *testing.T) {
	core := replayDetailTestCore(1)
	captured := time.Now()
	core.CapturedAt = captured.UTC()
	p, err := prepareReplayDetail(core, captured)
	if err != nil || p.capturedAt != captured {
		t.Fatal("capture lost its original clock")
	}
	gate := replayDetailTestGate(core, captured)
	var cache replayDetailCache
	if err := cache.publish(p, gate, captured.Add(time.Second)); err != nil || cache.items[0].publishedAt != captured.Add(time.Second) || cache.items[0].holdUntil != captured.Add(31*time.Second) {
		t.Fatal("publication lost its original clock")
	}
	for _, change := range []func(*replayDetailGate){
		func(g *replayDetailGate) { g.RunID = "" },
		func(g *replayDetailGate) { g.RunID = string([]byte{255}) },
		func(g *replayDetailGate) { g.NodeStartedAt = time.Time{} },
		func(g *replayDetailGate) { g.AttemptStartedAt = time.Time{} },
		func(g *replayDetailGate) { g.ObservedAt = time.Time{} },
		func(g *replayDetailGate) { g.Attempt = 0 },
		func(g *replayDetailGate) { g.GenesisHash = "unmarked" },
		func(g *replayDetailGate) { g.HasGenesis, g.GenesisHash = true, "bad" },
		func(g *replayDetailGate) { g.State = "other" },
		func(g *replayDetailGate) { g.Reason = "private error" },
		func(g *replayDetailGate) { g.State, g.Reason = "stale", "private error" },
	} {
		invalid := gate
		change(&invalid)
		if cache.canPublish(invalid, captured) || cache.publish(p, invalid, captured) == nil {
			t.Fatal("invalid gate allowed publication")
		}
		replayDetailAssertReply(t, cache.lookup("", false, invalid, captured), 503, "source_invalid")
	}
	if cache.publish(nil, gate, captured) == nil {
		t.Fatal("nil candidate accepted")
	}
}
