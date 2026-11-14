package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

func machineSourceFixture(t *testing.T, now time.Time, generation uint64) (Snapshot, MachineDetailCapture) {
	t.Helper()
	d, _, _, _ := maximumMachineDetail(t)
	d.Binding.RunID, d.Binding.NodeStartedAt, d.Binding.SourceGeneration = "machine-source", now.Add(-time.Hour).UTC(), generation
	d.CapturedAt = now.UTC()
	d.RSSBreakdown = &ProcessRSSBreakdown{SampledAt: now.UTC(), AnonymousBytes: 1048576, FileBytes: 0, SharedBytes: 2048}
	p, err := PrepareMachineDetail(d)
	if err != nil {
		t.Fatal(err)
	}
	s := testSnapshot(now.UTC())
	s.RunID, s.StartedAt, s.GenesisHash = d.Binding.RunID, d.Binding.NodeStartedAt, &d.Binding.GenesisHash
	return s, MachineDetailCapture{Prepared: p, FreshUntil: now.Add(MachineDetailFreshFor)}
}

func machineSourceRead(s *machineDetailSource, query string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest(http.MethodGet, machineDetailPath+query, nil))
	return w
}

func TestMachineSourceRealFeed(t *testing.T) {
	snapshot, capture := machineSourceFixture(t, time.Now(), 1)
	var calls, statuses atomic.Int64
	path := socketPath(t)
	f, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { statuses.Add(1); return snapshot },
		DetailSamplers{Machine: func(now time.Time) (MachineDetailCapture, error) {
			if now == now.Round(0) {
				t.Error("Feed removed the original monotonic sample clock")
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
	code, body, header := replayDetailHTTP(t, client, "GET", "http://node/details/machine", "")
	remaining, parseErr := strconv.Atoi(header.Get(replayDetailRemainingHeader))
	if code != 200 || body != capture.Prepared.Manifest() || parseErr != nil || remaining < 9000 || remaining > 20000 {
		t.Fatalf("manifest %d %s", code, body)
	}
	for i := 0; i < 14; i++ {
		want, ok := capture.Prepared.Page(i)
		if !ok {
			t.Fatal("fixture no longer covers full fourteen-page topology")
		}
		code, body, h := replayDetailHTTP(t, client, "GET", "http://node/details/machine?generation="+capture.Prepared.Generation()+"&page="+strconv.Itoa(i), "")
		if code != 200 || body != want || h.Get(replayDetailRemainingHeader) != "" || len(body) > MaxMachinePageBytes ||
			i == 0 && !strings.Contains(body, `"anonymous_bytes":"1048576","file_bytes":"0","shared_bytes":"2048"`) {
			t.Fatalf("page %d: %d", i, code)
		}
	}
	if _, ok := capture.Prepared.Page(14); ok {
		t.Fatal("fixture changed")
	}
	code, ordinary, _ := replayDetailHTTP(t, client, "GET", "http://node/snapshot", "")
	if code != 200 || strings.Contains(ordinary, `"family":"machine"`) || f.network != nil || f.server.MaxHeaderBytes != 4096 {
		t.Fatal("machine changed ordinary feed or worker policy")
	}
	f.Close()
	if calls.Load() != statuses.Load() {
		t.Fatal("viewers called sampler outside accepted status cycles")
	}
	for _, constructor := range []func(string) (*Feed, error){
		func(path string) (*Feed, error) {
			return ListenFeed(context.Background(), path, func() Snapshot { return snapshot })
		},
		func(path string) (*Feed, error) {
			return ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{})
		},
	} {
		path := socketPath(t)
		feed, err := constructor(path)
		if err != nil {
			t.Fatal(err)
		}
		c := newCollector(path).client
		code, _, _ := replayDetailHTTP(t, c, "GET", "http://node/details/machine", "")
		c.CloseIdleConnections()
		feed.Close()
		if code != 404 || feed.machine != nil {
			t.Fatal("default enabled machine source")
		}
	}
}

func TestMachineSourceNoRenewalAndPublicationCounter(t *testing.T) {
	now := time.Now()
	snapshot, first := machineSourceFixture(t, now, 1)
	capture := first
	s := newMachineDetailSource(func(time.Time) (MachineDetailCapture, error) { return capture, nil })
	s.update(context.Background(), snapshot, now)
	original := s.items[0]
	capture.FreshUntil = time.Now().Add(MachineDetailFreshFor)
	s.update(context.Background(), snapshot, time.Now())
	if s.items[0].freshUntil != original.freshUntil || s.items[0].holdUntil != original.holdUntil {
		t.Fatal("same token renewed original clocks")
	}
	// Rewind changes replay, not the machine binding.
	snapshot.Replay = &Replay{Attempt: 3, Rewinds: 9, StartedAt: now.UTC(), Phase: "replaying"}
	s.update(context.Background(), snapshot, time.Now())
	if machineSourceRead(s, "").Code != 200 {
		t.Fatal("replay rewind invalidated machine")
	}
	// Keep this node binding while wall time reverses; the publication counter
	// advances and the original raw deadline remains independent.
	d, err := NewMachineDetail(firstBinding(t, first.Prepared), now.Add(-time.Second), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	d.Binding.SourceGeneration = 2
	p, err := PrepareMachineDetail(d)
	if err != nil {
		t.Fatal(err)
	}
	capture = MachineDetailCapture{p, now.Add(MachineDetailFreshFor)}
	s.update(context.Background(), snapshot, time.Now())
	if s.sourceGeneration != 2 || s.items[1].prepared != first.Prepared || s.items[0].prepared != p {
		t.Fatal("wall reversal or previous generation lost")
	}
	oldPage := s.lookup(first.Prepared.Generation(), 0, true, time.Now())
	if oldPage.status != 200 {
		t.Fatal("compatible prior page was revoked")
	}
	// A different token at the same counter cannot replace an accepted capture.
	d.CapturedAt = now.UTC()
	p2, err := PrepareMachineDetail(d)
	if err != nil {
		t.Fatal(err)
	}
	capture.Prepared = p2
	s.update(context.Background(), snapshot, time.Now())
	if machineSourceRead(s, "").Code != 409 || s.items[0].prepared != p {
		t.Fatal("same counter replaced a different token")
	}
	d.Binding.SourceGeneration = 3
	p3, err := PrepareMachineDetail(d)
	if err != nil {
		t.Fatal(err)
	}
	capture.Prepared = p3
	s.update(context.Background(), snapshot, time.Now())
	if s.items[1].prepared != p || s.lookup(first.Prepared.Generation(), 0, true, time.Now()).status != 409 {
		t.Fatal("cache does not retain exactly current and previous")
	}
	capture = first
	capture.FreshUntil = time.Now().Add(MachineDetailFreshFor)
	s.update(context.Background(), snapshot, time.Now())
	if s.items[0].prepared != p3 || machineSourceRead(s, "").Code != 409 {
		t.Fatal("evicted A returned after B/C")
	}
	// A transient error must not erase the remembered clocks for the current token.
	original = s.items[0]
	s.sample = func(time.Time) (MachineDetailCapture, error) { return MachineDetailCapture{}, errors.New("failed") }
	s.update(context.Background(), snapshot, time.Now())
	s.sample = func(time.Time) (MachineDetailCapture, error) {
		return MachineDetailCapture{p3, time.Now().Add(MachineDetailFreshFor)}, nil
	}
	s.update(context.Background(), snapshot, time.Now())
	if s.items[0].freshUntil != original.freshUntil || s.items[0].holdUntil != original.holdUntil {
		t.Fatal("recovery renewed identical capture")
	}
	boundary := original.holdUntil.Add(-machineDetailMinimum + time.Nanosecond)
	s.gate.observedAt = boundary
	if s.lookup("", 0, false, boundary).status == 200 || s.lookup(p3.Generation(), 0, true, boundary).status != 200 {
		t.Fatal("nine-second manifest floor revoked historical pages or admitted a short transfer")
	}
}

func firstBinding(t *testing.T, p *PreparedMachineDetail) MachineDetailBinding {
	t.Helper()
	var m MachineDetailManifest
	if err := json.Unmarshal([]byte(p.Manifest()), &m); err != nil {
		t.Fatal(err)
	}
	return m.Binding
}

func TestMachineSourceFailureExpiryAndContext(t *testing.T) {
	for _, mode := range []string{"failure", "expired", "old status", "run", "start", "genesis", "stopping", "unknown genesis", "future lease", "future capture", "cancel", "closed"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now()
			snapshot, capture := machineSourceFixture(t, now, 1)
			var failed bool
			s := newMachineDetailSource(func(time.Time) (MachineDetailCapture, error) {
				if failed {
					return MachineDetailCapture{}, errors.New("secret /private/canary")
				}
				return capture, nil
			})
			s.update(context.Background(), snapshot, now)
			pageQuery := "?generation=" + capture.Prepared.Generation() + "&page=0"
			switch mode {
			case "failure":
				failed = true
				s.update(context.Background(), snapshot, time.Now())
			case "expired":
				s.items[0].holdUntil = time.Now().Add(-time.Second)
			case "old status":
				s.gate.observedAt = time.Now().Add(-StaleAfter - time.Second)
			case "run":
				snapshot.RunID += "-new"
				s.update(context.Background(), snapshot, time.Now())
			case "start":
				snapshot.StartedAt = snapshot.StartedAt.Add(time.Second)
				s.update(context.Background(), snapshot, time.Now())
			case "genesis":
				hash := "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4S6DYF4wW"
				snapshot.GenesisHash = &hash
				s.update(context.Background(), snapshot, time.Now())
			case "stopping":
				snapshot.Phase = "stopping"
				s.update(context.Background(), snapshot, time.Now())
			case "unknown genesis":
				snapshot.GenesisHash = nil
				s.update(context.Background(), snapshot, time.Now())
			case "future lease":
				capture.FreshUntil = time.Now().Add(MachineDetailFreshFor + time.Second)
				s.update(context.Background(), snapshot, time.Now())
			case "future capture":
				d, err := NewMachineDetail(firstBinding(t, capture.Prepared), time.Now().Add(2*time.Second), nil, nil, nil, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				d.Binding.SourceGeneration = 2
				capture.Prepared, err = PrepareMachineDetail(d)
				if err != nil {
					t.Fatal(err)
				}
				s.update(context.Background(), snapshot, time.Now())
			case "cancel":
				ctx, cancel := context.WithCancel(context.Background())
				s.sample = func(time.Time) (MachineDetailCapture, error) { cancel(); return capture, nil }
				s.items = [2]machineDetailEntry{}
				s.update(ctx, snapshot, time.Now())
			case "closed":
				s.sample = func(time.Time) (MachineDetailCapture, error) { s.close(); return capture, nil }
				s.update(context.Background(), snapshot, time.Now())
			}
			w := machineSourceRead(s, "")
			if w.Code == 200 || w.Header().Get(replayDetailRemainingHeader) != "" || w.Body.Len() > 512 || strings.Contains(w.Body.String(), "canary") {
				t.Fatalf("invalid root: %d %s", w.Code, w.Body.String())
			}
			if (mode == "future capture" || mode == "future lease") && !strings.Contains(w.Body.String(), "source_time_invalid") {
				t.Fatal("clock fixture did not reach time validation")
			}
			if mode == "failure" || mode == "future lease" || mode == "future capture" {
				if machineSourceRead(s, pageQuery).Code != 200 {
					t.Fatal("source failure revoked compatible historical page")
				}
			} else if machineSourceRead(s, pageQuery).Code == 200 {
				t.Fatal("incompatible or expired page survived")
			}
		})
	}
}

func TestMachineDetailQueryGrammar(t *testing.T) {
	digest := strings.Repeat("a", 64)
	for _, tc := range []struct {
		query string
		force bool
		valid bool
	}{
		{"", false, true}, {"", true, false},
		{"generation=" + digest + "&page=15", false, true},
		{"page=0&generation=" + digest, false, true},
		{"generation=" + digest + "&page=16", false, false},
		{"generation=" + digest + "&page=127", false, false},
		{"generation=" + digest + "&page=128", false, false},
		{"generation=" + digest + "&page=01", false, false},
		{"generation=" + strings.ToUpper(digest) + "&page=0", false, false},
		{"generation=" + digest + "&page=0&page=1", false, false},
		{"generation=" + digest + "&page=%30", false, false},
		{"generation=" + digest + "&unknown=0", false, false},
	} {
		_, _, _, valid := parseMachineDetailQuery(tc.query, tc.force)
		if valid != tc.valid {
			t.Errorf("query %q force=%t: valid=%t", tc.query, tc.force, valid)
		}
	}
	for page := 0; page < 128; page++ {
		_, parsed, isPage, valid := parseMachineDetailQuery("generation="+digest+"&page="+strconv.Itoa(page), false)
		if valid != (page < machineMaxPages) || valid && (!isPage || parsed != page) {
			t.Errorf("page %d: parsed=%d pageRequest=%t valid=%t", page, parsed, isPage, valid)
		}
	}
}

func TestMachineSourceGrammarAndAdmission(t *testing.T) {
	now := time.Now()
	snapshot, capture := machineSourceFixture(t, now, 1)
	s := newMachineDetailSource(func(time.Time) (MachineDetailCapture, error) { return capture, nil })
	s.update(context.Background(), snapshot, now)
	for _, path := range []string{"/details/machine?", "/details/%6dachine", "/details/machine/", "/details/machine?x=1", "/details/machine?generation=" + capture.Prepared.Generation() + "&page=16", "/details/machine?generation=" + capture.Prepared.Generation() + "&page=00"} {
		w := httptest.NewRecorder()
		s.serveHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 400 {
			t.Fatalf("noncanonical path accepted: %s", path)
		}
	}
	for _, method := range []string{"POST", "HEAD"} {
		w := httptest.NewRecorder()
		s.serveHTTP(w, httptest.NewRequest(method, machineDetailPath, nil))
		if w.Code != 400 {
			t.Fatal(method)
		}
	}
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest("GET", machineDetailPath, strings.NewReader("x")))
	if w.Code != 400 {
		t.Fatal("request body admitted")
	}
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var joined sync.WaitGroup
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }); joined.Wait() }
	defer unblock()
	for i := range 2 {
		joined.Add(1)
		go func() {
			defer joined.Done()
			sink := &machineTestSink{header: make(http.Header), onFirstWrite: func() { entered <- struct{}{}; <-release }}
			s.serveHTTP(sink, httptest.NewRequest("GET", machineDetailPath, nil))
		}()
		<-entered
		if i == 0 {
			d, err := NewMachineDetail(firstBinding(t, capture.Prepared), now, nil, nil, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			d.Binding.SourceGeneration = 2
			capture.Prepared, err = PrepareMachineDetail(d)
			if err != nil {
				t.Fatal(err)
			}
			s.update(context.Background(), snapshot, time.Now())
		}
	}
	for _, generation := range []uint64{3, 4} {
		d, err := NewMachineDetail(firstBinding(t, capture.Prepared), now, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		d.Binding.SourceGeneration = generation
		capture.Prepared, err = PrepareMachineDetail(d)
		if err != nil {
			t.Fatal(err)
		}
		s.update(context.Background(), snapshot, time.Now())
	}
	if s.items[0].binding.SourceGeneration != 4 || s.items[1].binding.SourceGeneration != 3 {
		t.Fatal("retired handlers prevented bounded replacement")
	}
	if machineSourceRead(s, "").Code != 429 {
		t.Fatal("more than two cached replies admitted")
	}
	s.close()
	unblock()
	if len(s.active) != 0 || s.items != [2]machineDetailEntry{} {
		t.Fatal("retired reply admission leaked")
	}
	// The source retains exactly two Prepared pointers; handlers only retain a
	// selected string, never another DTO or a snapshot of the family cache.
	if unsafe.Sizeof(machineDetailSource{}) > 1024 || cap(s.active) != 2 {
		t.Fatal("source headers exceed the bounded holder allowance")
	}
}

func TestMachineSourceHolderBudget(t *testing.T) {
	d, topology, affinity, links := maximumMachineDetail(t)
	p, err := PrepareMachineDetail(d)
	if err != nil {
		t.Fatal(err)
	}
	owned := func(v any) uintptr { return reflect.TypeOf(v).Size() + machineDynamicBytes(reflect.ValueOf(v)) }
	// Same five-family model as the typed maximum, now including the concrete
	// Feed/cache/reply headers and bounded manifest decode/conversion scratch.
	modeled := 2*owned(d) + 5*owned(*p) + owned(topology) + owned(affinity) + owned(links) + owned(maximumMachineRoutes(topology, affinity, links)) + 2*MaxMachineDetailBytes + MaxMachinePageBytes
	identitySize := unsafe.Sizeof(struct {
		Index uint32
		Name  string
	}{})
	modeled += 256*(identitySize+15) + 2*256*(unsafe.Sizeof(metrics.LinkTopology{})+identitySize) + 64<<10 + 64<<10
	modeled += unsafe.Sizeof(machineDetailSource{}) + unsafe.Sizeof(Feed{}) + 2*unsafe.Sizeof(machineDetailReply{}) + 2*MaxMachineManifestBytes
	if modeled > 4<<20 {
		t.Fatalf("source holder model exceeds4MiB: %d", modeled)
	}
	t.Logf("source model with cache/Feed/replies/metadata scratch=%d/4194304; not RSS", modeled)
}

func TestMachineSourceFeedCloseJoinsAndRejectedStatus(t *testing.T) {
	snapshot, capture := machineSourceFixture(t, time.Now(), 1)
	var invalid atomic.Bool
	var calls atomic.Int64
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	f, err := ListenFeedWithDetails(context.Background(), socketPath(t), "", func() Snapshot {
		s := snapshot
		if invalid.Load() {
			s.Mode = "invalid"
		}
		return s
	},
		DetailSamplers{Machine: func(time.Time) (MachineDetailCapture, error) {
			if calls.Add(1) == 2 {
				close(entered)
				<-release
			}
			return capture, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unblock(); f.Close() }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("second ordinary Feed callback did not run")
	}
	done := make(chan struct{})
	go func() { f.Close(); close(done) }()
	select {
	case <-done:
		t.Fatal("Close did not join callback")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close failed to finish")
	}
	if !f.machine.closed || f.machine.items != [2]machineDetailEntry{} {
		t.Fatal("late callback committed after close")
	}
	invalid.Store(true)
	before := calls.Load()
	f2, err := ListenFeedWithDetails(context.Background(), socketPath(t), "", func() Snapshot { s := snapshot; s.Mode = "invalid"; return s }, DetailSamplers{Machine: func(time.Time) (MachineDetailCapture, error) { calls.Add(1); return capture, nil }})
	if err != nil {
		t.Fatal(err)
	}
	f2.Close()
	if calls.Load() != before {
		t.Fatal("rejected status invoked source")
	}
}
