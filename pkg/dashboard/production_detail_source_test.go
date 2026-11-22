package dashboard

import (
	"context"
	"errors"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func productionSourceFixture(now time.Time) (Snapshot, ProductionDetailCapture) {
	d := productionDetailFixture()
	genesis := d.Binding.GenesisHash
	d.Binding.RunID = "production-source-run"
	d.Binding.NodeStartedAt = now.Add(-time.Hour).UTC()
	d.Binding.ProducerStartedAt = now.Add(-30 * time.Minute).UTC()
	d.CapturedAt = now.UTC()
	// Historical evidence is not ordered against node or producer wall dates.
	d.LastTerminal.ObservedAt = now.Add(-2 * time.Hour).UTC()
	s := testSnapshot(now.UTC())
	s.RunID, s.StartedAt, s.GenesisHash = d.Binding.RunID, d.Binding.NodeStartedAt, &genesis
	s.Mode, s.Alpenglow = "validator", true
	return s, ProductionDetailCapture{Detail: d, FreshUntil: now.Add(20 * time.Second)}
}

func readProductionSource(s *productionDetailSource) (int, string, string) {
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest("GET", "/details/production", nil))
	return w.Code, w.Body.String(), w.Header().Get(replayDetailRemainingHeader)
}

func TestProductionDetailSourcePublicationGenerationAndOwnership(t *testing.T) {
	now := time.Now()
	snapshot, capture := productionSourceFixture(now)
	s := &productionDetailSource{sample: func(time.Time) (ProductionDetailCapture, error) { return capture, nil }}
	s.update(snapshot, now)
	status, original, header := readProductionSource(s)
	remaining, err := strconv.Atoi(header)
	if status != 200 || err != nil || remaining <= 0 || remaining > 30000 {
		t.Fatalf("fresh production detail: status=%d remaining=%q body=%s", status, header, original)
	}
	decoded, err := decodeProductionDetail([]byte(original))
	if err != nil || decoded.LastTerminal == nil || !decoded.LastTerminal.ObservedAt.Before(decoded.Binding.NodeStartedAt) {
		t.Fatalf("historical terminal changed or rejected: %v", err)
	}
	parentSlot := capture.Detail.LastTerminal.Bank.ParentSlot
	capture.Detail.LastTerminal.Bank.ParentSlot++
	if _, body, _ := readProductionSource(s); body != original {
		t.Fatal("caller-owned nested data changed the cached publication")
	}
	capture.Detail.LastTerminal.Bank.ParentSlot = parentSlot

	deadline := s.latest.Load().freshUntil
	capture.FreshUntil = now.Add(25 * time.Second)
	s.update(snapshot, time.Now())
	if !s.latest.Load().freshUntil.Equal(deadline) {
		t.Fatal("same capture renewed its original deadline")
	}
	s.sample = func(time.Time) (ProductionDetailCapture, error) {
		return ProductionDetailCapture{}, errors.New("source_unavailable")
	}
	s.update(snapshot, time.Now())
	s.sample = func(time.Time) (ProductionDetailCapture, error) { return capture, nil }
	s.update(snapshot, time.Now())
	if !s.latest.Load().freshUntil.Equal(deadline) {
		t.Fatal("failure cycle renewed an unchanged capture")
	}
	capture.Detail.ProducerState = "stopped"
	s.update(snapshot, time.Now())
	if p := s.latest.Load(); p.reason != "source_invalid" || p.body != "" {
		t.Fatal("conflicting same-capture body was published")
	}
	// Restore a valid body and advance the producer generation.
	capture.Detail.ProducerState = "running"
	capture.Detail.Binding.ProducerGeneration = 2
	capture.Detail.Binding.ProducerStartedAt = now.Add(-20 * time.Minute).UTC()
	capture.Detail.CapturedAt = time.Now().UTC()
	capture.FreshUntil = time.Now().Add(20 * time.Second)
	s.update(snapshot, time.Now())
	if p := s.latest.Load(); p.reason != "" || p.binding.ProducerGeneration != 2 {
		t.Fatalf("new producer generation was not published: %+v", p)
	}
	capture.Detail.ProducerState = "stopped"
	capture.Detail.CapturedAt = capture.Detail.CapturedAt.Add(time.Nanosecond)
	capture.FreshUntil = time.Now().Add(20 * time.Second)
	s.update(snapshot, time.Now())
	if p := s.latest.Load(); p.reason != "" || !strings.Contains(p.body, `"producer_state":"stopped"`) {
		t.Fatal("same generation and start could not publish a newer owner copy")
	}
	inapplicable := snapshot
	inapplicable.Mode = "verifying"
	s.update(inapplicable, time.Now())
	if p := s.latest.Load(); p.reason != "not_applicable" || s.highwaterGen != 2 {
		t.Fatal("role change cleared the same-node producer generation")
	}

	s.sample = func(time.Time) (ProductionDetailCapture, error) {
		return ProductionDetailCapture{}, errors.New("source_unavailable")
	}
	s.update(snapshot, time.Now())
	if s.highwaterGen != 2 {
		t.Fatal("failure cycle cleared the producer generation high-water")
	}
	capture.Detail.Binding.ProducerGeneration = 1
	capture.Detail.CapturedAt = time.Now().UTC()
	capture.FreshUntil = time.Now().Add(20 * time.Second)
	s.sample = func(time.Time) (ProductionDetailCapture, error) { return capture, nil }
	s.update(snapshot, time.Now())
	if p := s.latest.Load(); p.reason != "binding_changed" || p.body != "" {
		t.Fatal("older same-run producer generation was accepted")
	}
	capture.Detail.Binding.ProducerGeneration = 2
	capture.Detail.Binding.ProducerStartedAt = capture.Detail.Binding.ProducerStartedAt.Add(time.Second)
	s.update(snapshot, time.Now())
	if s.latest.Load().reason != "binding_changed" {
		t.Fatal("same generation changed its producer start")
	}

	// A different accepted node binding starts an independent generation space.
	snapshot.RunID = "replacement-node"
	snapshot.StartedAt = snapshot.StartedAt.Add(time.Minute)
	capture.Detail.Binding.RunID = snapshot.RunID
	capture.Detail.Binding.NodeStartedAt = snapshot.StartedAt
	capture.Detail.Binding.ProducerGeneration = 1
	capture.Detail.Binding.ProducerStartedAt = time.Now().Add(-time.Minute).UTC()
	capture.Detail.CapturedAt = time.Now().UTC()
	capture.FreshUntil = time.Now().Add(20 * time.Second)
	s.update(snapshot, time.Now())
	if p := s.latest.Load(); p.reason != "" || s.highwaterGen != 1 {
		t.Fatalf("node replacement did not reset producer high-water: %+v", p)
	}
}

func TestProductionDetailSourceShortenedLease(t *testing.T) {
	now := time.Now()
	snapshot, capture := productionSourceFixture(now)
	s := &productionDetailSource{sample: func(time.Time) (ProductionDetailCapture, error) { return capture, nil }}
	s.update(snapshot, now)
	if p := s.latest.Load(); p == nil || p.reason != "" {
		t.Fatal("initial capture was not accepted")
	}
	capture.FreshUntil = now.Add(10 * time.Second)
	s.update(snapshot, time.Now())
	if p := s.latest.Load(); p.reason != "" || !p.freshUntil.Equal(capture.FreshUntil) {
		t.Fatal("unchanged capture ignored a shortened source deadline")
	}
	shortened := capture.FreshUntil
	capture.FreshUntil = now.Add(20 * time.Second)
	s.update(snapshot, time.Now())
	if p := s.latest.Load(); p.reason != "" || !p.freshUntil.Equal(shortened) {
		t.Fatal("later observation renewed a shortened source deadline")
	}
}

func TestProductionDetailSourceBindingLossRetainsAcceptedGeneration(t *testing.T) {
	for _, kind := range []string{"unknown_genesis", "different_node_source_failure", "different_node_invalid_capture"} {
		t.Run(kind, func(t *testing.T) {
			now := time.Now()
			snapshot, capture := productionSourceFixture(now)
			capture.Detail.Binding.ProducerGeneration = 2
			s := &productionDetailSource{sample: func(time.Time) (ProductionDetailCapture, error) { return capture, nil }}
			s.update(snapshot, now)
			if s.highwaterGen != 2 || s.latest.Load().reason != "" {
				t.Fatal("initial generation not established")
			}
			uncertain := snapshot
			if kind == "unknown_genesis" {
				uncertain.GenesisHash = nil
			} else {
				uncertain.RunID = "unverified-other-node"
				if kind == "different_node_source_failure" {
					s.sample = func(time.Time) (ProductionDetailCapture, error) {
						return ProductionDetailCapture{}, errors.New("source_unavailable")
					}
				}
			}
			s.update(uncertain, time.Now())
			if s.latest.Load().reason == "" {
				t.Fatal("uncertain binding was published")
			}
			capture.Detail.Binding.ProducerGeneration = 1
			capture.Detail.CapturedAt = time.Now().UTC()
			capture.FreshUntil = time.Now().Add(20 * time.Second)
			s.sample = func(time.Time) (ProductionDetailCapture, error) { return capture, nil }
			s.update(snapshot, time.Now())
			if p := s.latest.Load(); p.reason != "binding_changed" || p.body != "" || s.highwaterGen != 2 {
				t.Fatalf("generation rollback accepted after %s: reason=%q generation=%d body_present=%t", kind, p.reason, s.highwaterGen, p.body != "")
			}
		})
	}
}

func TestProductionDetailSourceBindingLossRetainsCapture(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		now := time.Now()
		snapshot, capture := productionSourceFixture(now)
		s := &productionDetailSource{sample: func(time.Time) (ProductionDetailCapture, error) { return capture, nil }}
		s.update(snapshot, now)
		original := s.latest.Load()
		unknown := snapshot
		unknown.GenesisHash = nil
		s.update(unknown, time.Now())
		if s.latest.Load().reason != "waiting_source" {
			t.Fatal("unknown genesis was published")
		}
		if conflict {
			capture.Detail.ProducerState = "stopped"
		} else {
			capture.FreshUntil = now.Add(25 * time.Second)
		}
		s.update(snapshot, time.Now())
		if p := s.latest.Load(); conflict {
			if p.reason != "source_invalid" || p.body != "" {
				t.Fatal("unknown binding erased the same-capture content guard")
			}
		} else if p.reason != "" || p.body != original.body || !p.freshUntil.Equal(original.freshUntil) {
			t.Fatal("unknown binding renewed an unchanged capture")
		}
	}
}

func TestProductionDetailSourceClockRoleAndCurrentGate(t *testing.T) {
	completed := time.Date(2026, 11, 1, 12, 0, 0, 123456789, time.UTC)
	snapshot, base := productionSourceFixture(completed)
	base.FreshUntil = completed.Add(20 * time.Second)
	for name, mutate := range map[string]func(*ProductionDetailCapture){
		"node future": func(c *ProductionDetailCapture) {
			c.Detail.Binding.NodeStartedAt = completed.Add(time.Second + time.Nanosecond)
		},
		"producer future": func(c *ProductionDetailCapture) {
			c.Detail.Binding.ProducerStartedAt = completed.Add(time.Second + time.Nanosecond)
		},
		"capture future": func(c *ProductionDetailCapture) { c.Detail.CapturedAt = completed.Add(time.Second + time.Nanosecond) },
		"event future": func(c *ProductionDetailCapture) {
			c.Detail.LastTerminal.ObservedAt = completed.Add(time.Second + time.Nanosecond)
		},
		"event after copy": func(c *ProductionDetailCapture) {
			c.Detail.CapturedAt = completed.Add(-time.Second)
			c.Detail.LastTerminal.ObservedAt = completed.Add(time.Nanosecond)
		},
		"expired": func(c *ProductionDetailCapture) { c.FreshUntil = completed },
		"extended": func(c *ProductionDetailCapture) {
			c.FreshUntil = completed.Add(ProductionDetailFreshFor + time.Nanosecond)
		},
		"lease beyond declared copy": func(c *ProductionDetailCapture) {
			c.Detail.CapturedAt = completed.Add(-25 * time.Second)
			c.FreshUntil = completed.Add(ProductionDetailFreshFor)
		},
		"old copy": func(c *ProductionDetailCapture) { c.Detail.CapturedAt = completed.Add(-ProductionDetailFreshFor) },
	} {
		t.Run(name, func(t *testing.T) {
			capture := base
			terminal, bank, costs := *base.Detail.LastTerminal, *base.Detail.LastTerminal.Bank, *base.Detail.LastTerminal.Costs
			terminal.Bank, terminal.Costs = &bank, &costs
			capture.Detail.LastTerminal = &terminal
			mutate(&capture)
			if !productionDetailTimeInvalid(capture, completed) {
				t.Fatal("invalid production clock accepted")
			}
		})
	}
	if productionDetailTimeInvalid(base, completed) {
		t.Fatal("old historical terminal made a fresh owner copy unhealthy")
	}
	boundary := base
	boundary.FreshUntil = boundary.Detail.CapturedAt.Add(ProductionDetailFreshFor)
	if productionDetailTimeInvalid(boundary, completed) {
		t.Fatal("exact maximum owner lease was rejected")
	}

	snapshot, base = productionSourceFixture(time.Now())
	var calls atomic.Int64
	s := &productionDetailSource{}
	s.sample = func(time.Time) (ProductionDetailCapture, error) {
		calls.Add(1)
		changed := snapshot
		changed.Mode = "verifying"
		g := productionGate(changed, time.Now())
		s.gate.Store(&g)
		capture := base
		capture.Detail.CapturedAt = time.Now().UTC()
		capture.FreshUntil = time.Now().Add(20 * time.Second)
		return capture, nil
	}
	s.update(snapshot, time.Now())
	if p := s.latest.Load(); p.reason != "not_applicable" || p.body != "" {
		t.Fatal("current role gate did not fence a copy completed under an old role")
	}
	for _, change := range []func(*Snapshot){
		func(s *Snapshot) { s.Mode = "verifying" },
		func(s *Snapshot) { s.Alpenglow = false },
		func(s *Snapshot) { s.GenesisHash = nil },
	} {
		before := calls.Load()
		status := snapshot
		change(&status)
		s.update(status, time.Now())
		want := "not_applicable"
		if status.GenesisHash == nil && status.Mode == "validator" && status.Alpenglow {
			want = "waiting_source"
		}
		if calls.Load() != before || s.latest.Load().reason != want {
			t.Fatal("inapplicable or unbound status invoked the producer owner")
		}
	}
}

func TestProductionDetailRealFeedCachedRouteAndDefaults(t *testing.T) {
	now := time.Now()
	snapshot, capture := productionSourceFixture(now)
	var statusCalls, detailCalls atomic.Int64
	path := socketPath(t)
	f, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot {
		statusCalls.Add(1)
		return snapshot
	}, DetailSamplers{Production: func(time.Time) (ProductionDetailCapture, error) {
		detailCalls.Add(1)
		return capture, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.Close)
	client := newCollector(path).client
	t.Cleanup(client.CloseIdleConnections)
	status, body, header := replayDetailHTTP(t, client, "GET", "http://node/details/production", "")
	remaining, err := strconv.Atoi(header.Get(replayDetailRemainingHeader))
	if status != 200 || err != nil || remaining <= 0 || remaining > 30000 || header.Get("Cache-Control") != "no-store" ||
		header.Get("Content-Type") != "application/json" {
		t.Fatalf("private production route: status=%d remaining=%q body=%s", status, header.Get(replayDetailRemainingHeader), body)
	}
	if d, err := decodeProductionDetail([]byte(body)); err != nil || d.Binding.ProducerGeneration != 1 {
		t.Fatalf("private production body changed: %v", err)
	}
	before := detailCalls.Load()
	for range 20 {
		code, next, h := replayDetailHTTP(t, client, "GET", "http://node/details/production", "")
		left, err := strconv.Atoi(h.Get(replayDetailRemainingHeader))
		if code != 200 || next != body || err != nil || left > remaining {
			t.Fatal("viewer changed or renewed the cached production document")
		}
		remaining = left
	}
	if detailCalls.Load() != before {
		t.Fatal("viewer requests invoked the producer sampler")
	}
	code, ordinary, _ := replayDetailHTTP(t, client, "GET", "http://node/snapshot", "")
	if code != 200 || strings.Contains(ordinary, "producer_generation") || strings.Contains(ordinary, "last_terminal") {
		t.Fatal("production detail changed the ordinary status document")
	}
	for _, request := range []struct{ method, path, body string }{
		{"POST", "/details/production", ""}, {"GET", "/details/production?", ""},
		{"GET", "/details/production?x=1", ""}, {"GET", "/details/production", "x"},
		{"GET", "/details/%70roduction", ""},
	} {
		code, result, h := replayDetailHTTP(t, client, request.method, "http://node"+request.path, request.body)
		if code != 400 || result != `{"schema_version":1,"family":"production","state":"unavailable","reason":"invalid_request"}` ||
			len(result) > 512 || h.Get(replayDetailRemainingHeader) != "" {
			t.Fatalf("invalid production request accepted: %d %s", code, result)
		}
	}
	if f.production == nil || f.details != nil || f.network != nil || f.cache != nil {
		t.Fatal("production option changed another detail worker")
	}
	f.Close()
	if detailCalls.Load() != statusCalls.Load() {
		t.Fatal("producer sampler did not stay on accepted status cycles")
	}

	path = socketPath(t)
	plain, err := ListenFeed(context.Background(), path, func() Snapshot { return snapshot })
	if err != nil {
		t.Fatal(err)
	}
	client = newCollector(path).client
	code, _, _ = replayDetailHTTP(t, client, "GET", "http://node/details/production", "")
	client.CloseIdleConnections()
	plain.Close()
	if code != 404 || plain.production != nil {
		t.Fatal("default feed enabled production details")
	}
}

func TestProductionDetailFixedErrorsAndExpiry(t *testing.T) {
	now := time.Now()
	snapshot, capture := productionSourceFixture(now)
	s := &productionDetailSource{sample: func(time.Time) (ProductionDetailCapture, error) { return capture, nil }}
	for _, reason := range []string{"waiting_source", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "binding_changed", "response_limit", "invalid_request", "not_applicable", "private-path-secret"} {
		s.sample = func(time.Time) (ProductionDetailCapture, error) { return ProductionDetailCapture{}, errors.New(reason) }
		s.update(snapshot, time.Now())
		code, result, header := readProductionSource(s)
		want := productionDetailReason(reason)
		if code != 503 || result != `{"schema_version":1,"family":"production","state":"unavailable","reason":"`+want+`"}` ||
			len(result) > 512 || header != "" || s.latest.Load().body != "" {
			t.Fatalf("source error leaked or retained production data: %d %s", code, result)
		}
	}
	s.sample = func(time.Time) (ProductionDetailCapture, error) { return capture, nil }
	s.update(snapshot, time.Now())
	p := *s.latest.Load()
	p.freshUntil = time.Now().Add(-time.Nanosecond)
	s.latest.Store(&p)
	code, result, header := readProductionSource(s)
	if code != 503 || !strings.Contains(result, `"reason":"status_stale"`) || header != "" || strings.Contains(result, "producer_generation") {
		t.Fatal("expired production document remained available")
	}
	for _, observedAt := range []time.Time{time.Now().Add(-StaleAfter - time.Second), time.Now().Add(time.Second), {}} {
		g := productionGate(snapshot, observedAt)
		s.gate.Store(&g)
		code, result, header = readProductionSource(s)
		if code != 503 || !strings.Contains(result, `"reason":"`+g.failure(time.Now())+`"`) || header != "" {
			t.Fatal("stored source state overrode the current status clock")
		}
	}
}
