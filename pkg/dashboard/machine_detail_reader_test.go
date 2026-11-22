package dashboard

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

func machineReaderPages(p *PreparedMachineDetail) []string {
	return slices.Clone(p.pages)
}

func TestMachineReaderCanonicalAssembly(t *testing.T) {
	d, _, _, _ := maximumMachineDetail(t)
	d.RSSBreakdown = &ProcessRSSBreakdown{SampledAt: d.CapturedAt, AnonymousBytes: math.MaxUint64, SharedBytes: 2048}
	p, err := PrepareMachineDetail(d)
	if err != nil {
		t.Fatal(err)
	}
	m, err := decodeMachineManifest([]byte(p.Manifest()))
	if err != nil || len(m.Pages) != 14 {
		t.Fatal("maximum manifest", err)
	}
	if err := verifyMachinePages(m, machineReaderPages(p)); err != nil {
		t.Fatal("maximum assembly", err)
	}
	for _, name := range []string{"duplicate", "extra", "null_pages", "counter_zero", "counter_exhausted", "offset", "page_count", "uppercase_digest"} {
		t.Run(name, func(t *testing.T) {
			copy := m
			copy.Pages = slices.Clone(m.Pages)
			switch name {
			case "null_pages":
				copy.Pages = nil
			case "counter_zero":
				copy.Binding.SourceGeneration = 0
			case "counter_exhausted":
				copy.Binding.SourceGeneration = math.MaxUint64
			case "offset":
				copy.Pages[1].Offset++
			case "page_count":
				copy.Pages[1].Count++
			case "uppercase_digest":
				copy.Pages[1].Digest = strings.ToUpper(copy.Pages[1].Digest)
			}
			raw, err := json.Marshal(copy)
			if err != nil {
				t.Fatal(err)
			}
			if name == "duplicate" {
				raw = []byte(strings.Replace(string(raw), `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1))
			}
			if name == "extra" {
				raw = append(raw[:len(raw)-1], []byte(`,"extra":0}`)...)
			}
			if _, err := decodeMachineManifest(raw); err == nil {
				t.Fatal("accepted invalid manifest")
			}
		})
	}
	for _, name := range []string{"tamper", "missing", "mixed", "full_digest"} {
		t.Run(name, func(t *testing.T) {
			pages, manifest := machineReaderPages(p), m
			switch name {
			case "tamper":
				pages[1] += " "
			case "missing":
				pages = pages[:len(pages)-1]
			case "mixed":
				pages[1] = pages[2]
			case "full_digest":
				manifest.Generation = strings.Repeat("a", 64)
			}
			if verifyMachinePages(manifest, pages) == nil {
				t.Fatal("accepted incomplete or mixed family")
			}
		})
	}
	at := time.Now()
	topology := metrics.TopologyObservation{Generation: 1, CPUState: metrics.TopologyUnsupported, NUMAState: metrics.TopologyUnsupported}
	affinity := metrics.ThreadAffinityObservation{Generation: 1, State: metrics.TopologyUnsupported}
	links := metrics.LinkTopologyObservation{Generation: 1, State: metrics.TopologyUnsupported}
	d, err = NewMachineDetail(d.Binding, at, &topology, &affinity, &links, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err = PrepareMachineDetail(d)
	if err != nil {
		t.Fatal(err)
	}
	m, err = decodeMachineManifest([]byte(p.Manifest()))
	if err != nil || len(m.Pages) != 1 || verifyMachinePages(m, machineReaderPages(p)) != nil {
		t.Fatal("empty arrays did not reassemble as []")
	}
	if machineArraysBounded([]byte(`[`+strings.Repeat(`0,`, 256)+`0]`)) || machineArraysBounded([]byte(strings.Repeat("[", 14)+"0"+strings.Repeat("]", 14))) {
		t.Fatal("typed allocation preflight admitted excessive arrays/depth")
	}
}

func TestMachineReaderExplicitOptInAndNoProbe(t *testing.T) {
	for _, options := range []struct {
		origin string
		public PublicOptions
	}{
		{"", PublicOptions{MachineDetails: true}},
		{"", PublicOptions{Resources: true, MachineDetails: true}},
		{"https://stats.example", PublicOptions{MachineDetails: true}},
	} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if err := serveProfile(context.Background(), "/unused.sock", listener, options.origin, options.public); err == nil ||
			!strings.Contains(err.Error(), "machine details require") {
			t.Fatalf("machine reader accepted missing disclosure: %+v, %v", options, err)
		}
	}
	for _, resources := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "resources_only"}[resources], func(t *testing.T) {
			// This strict legacy fixture fails on any private request other than
			// GET /snapshot, including an automatic /details/machine probe.
			path, body := fixtureFeed(t)
			snapshot := fullTestSnapshot(time.Now().UTC())
			snapshot.Resources = resourceFixture(snapshot.SampledAt).Resources
			setFixture(t, body, snapshot)
			address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: resources})
			defer replayDetailStopPublic(t, cancel, done)
			status, features, _ := replayDetailPublicRequest(t, address, "/api/v1/features?include=advanced_status", "stats.example", "")
			if status != 200 || string(features) != `{"schema_version":1,"history":false,"cluster":false}` {
				t.Fatalf("machine capability changed existing resource-only defaults: %d %s", status, features)
			}
			if status, _, _ := replayDetailPublicRequest(t, address, machineDetailPublicPath, "stats.example", ""); status != 404 {
				t.Fatal("machine route enabled without its explicit option")
			}
		})
	}
}

func TestMachineReaderRealFeedAndDisclosure(t *testing.T) {
	snapshot, capture := machineSourceFixture(t, time.Now(), 1)
	var calls, statuses atomic.Int64
	path := socketPath(t)
	f, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot {
		statuses.Add(1)
		s := snapshot
		s.SampledAt = time.Now().UTC()
		return s
	}, DetailSamplers{Machine: func(time.Time) (MachineDetailCapture, error) { calls.Add(1); return capture, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, MachineDetails: true})
	defer replayDetailStopPublic(t, cancel, done)
	read := func(path string) (int, string, http.Header) {
		req, err := http.NewRequest("GET", address+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "stats.example"
		res, err := (&http.Client{Timeout: time.Second}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, err := io.ReadAll(io.LimitReader(res.Body, MaxMachinePageBytes+1))
		if err != nil || len(body) > MaxMachinePageBytes {
			t.Fatal("response bound", err)
		}
		return res.StatusCode, string(body), res.Header
	}
	var body string
	var header http.Header
	deadline := time.Now().Add(7 * time.Second)
	for {
		status, b, h := read(machineDetailPublicPath)
		if status == 200 {
			body, header = b, h
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("public family did not arrive: %d %s", status, b)
		}
		time.Sleep(100 * time.Millisecond)
	}
	var public PublicMachineManifest
	if json.Unmarshal([]byte(body), &public) != nil || public.State != "fresh" || public.Generation != capture.Prepared.Generation() || public.CollectorAt.IsZero() || len(public.Pages) != 14 {
		t.Fatal("public envelope", body)
	}
	remaining, err := strconv.Atoi(header.Get(replayDetailRemainingHeader))
	if err != nil || remaining < 1 || remaining > 20000 || header.Get(statusRemainingHeader) == "" {
		t.Fatal("original source remainder", header)
	}
	private, err := decodeMachineManifest([]byte(capture.Prepared.Manifest()))
	if err != nil || public.Binding != private.Binding || !public.CapturedAt.Equal(private.CapturedAt) || public.Bytes != private.Bytes || !reflect.DeepEqual(public.Pages, private.Pages) {
		t.Fatal("public projection changed source metadata")
	}
	for i := range public.Pages {
		status, body, h := read(machineDetailPublicPath + "?generation=" + public.Generation + "&page=" + strconv.Itoa(i))
		want, _ := capture.Prepared.Page(i)
		if status != 200 || body != want || h.Get(replayDetailRemainingHeader) != "" || h.Get(statusRemainingHeader) == "" {
			t.Fatalf("public page %d differs", i)
		}
	}
	status, feature, _ := read("/api/v1/features?include=advanced_status")
	if status != 200 || !strings.Contains(feature, `"machine_details":true`) {
		t.Fatal("missing disclosure capability", feature)
	}
	status, _, _ = replayDetailPublicRequest(t, address, machineDetailPublicPath, "wrong.example", "")
	if status != 403 {
		t.Fatal("Host gate")
	}
	for _, invalid := range []string{"/api/v1/details//machine", "/api/v1/details/%6dachine", "/api/v1/details/machine/"} {
		status, _, _ = replayDetailPublicRequest(t, address, invalid, "stats.example", "")
		if status != 400 {
			t.Fatalf("noncanonical machine path %q: got %d", invalid, status)
		}
	}
	f.Close()
	if calls.Load() != statuses.Load() {
		t.Fatal("viewers sampled the owner")
	}
	addressOff, cancelOff, doneOff := replayDetailPublicServer(t, path, PublicOptions{})
	defer replayDetailStopPublic(t, cancelOff, doneOff)
	status, _, _ = replayDetailPublicRequest(t, addressOff, machineDetailPublicPath, "stats.example", "")
	if status != 404 {
		t.Fatal("default machine route enabled")
	}
	status, featureBody, _ := replayDetailPublicRequest(t, addressOff, "/api/v1/features?include=advanced_status", "stats.example", "")
	if status != 200 || strings.Contains(string(featureBody), `"machine_details"`) {
		t.Fatal("default machine capability enabled")
	}
}

func TestMachineReaderLeaseAndContext(t *testing.T) {
	now := time.Now()
	snapshot, capture := machineSourceFixture(t, now, 1)
	d, err := NewMachineDetail(firstBinding(t, capture.Prepared), now, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := PrepareMachineDetail(d)
	if err != nil {
		t.Fatal(err)
	}
	var current atomic.Pointer[PreparedMachineDetail]
	current.Store(p)
	var mode atomic.Int32
	path := socketPath(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	client := newCollector(path).client
	r, err := newMachineDetailReader(client)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	defer r.client.CloseIdleConnections()
	r.observeStatus(&snapshot, "fresh", "", now, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if mode.Load() == 1 {
			w.WriteHeader(503)
			_, _ = io.WriteString(w, machineDetailFailure("source_unavailable").body)
			return
		}
		p := current.Load()
		if req.URL.RawQuery == "" {
			w.Header().Set(replayDetailRemainingHeader, "20000")
			_, _ = io.WriteString(w, p.Manifest())
			return
		}
		if mode.Load() == 2 {
			other := snapshot
			other.RunID = "other"
			r.observeStatus(&other, "fresh", "", time.Now(), 2)
			r.observeStatus(&snapshot, "fresh", "", time.Now(), 3)
		}
		_, page, _, _ := parseMachineDetailQuery(req.URL.RawQuery, false)
		body, _ := p.Page(page)
		_, _ = io.WriteString(w, body)
	})}
	joined := make(chan error, 1)
	go func() { joined <- server.Serve(listener) }()
	defer func() { _ = server.Close(); <-joined }()
	r.collect(context.Background())
	first := r.items[0]
	if first == nil {
		t.Fatal("initial collection", r.reason)
	}
	lease, hold, root := first.until, first.holdUntil, first.roots[0]
	r.observeStatus(&snapshot, "fresh", "", time.Now(), 2)
	if r.lookup("", 0, false, time.Now()).status == 200 || r.lookup(p.Generation(), 0, true, time.Now()).status == 200 {
		t.Fatal("status recovery exposed an old generation without revalidation")
	}
	r.collect(context.Background())
	if r.items[0] != first || first.stamp.nodeEpoch != 2 || first.until != lease || first.holdUntil != hold ||
		r.lookup(p.Generation(), 0, true, time.Now()).status != 200 {
		t.Fatal("validated machine recovery renewed its page hold or failed to rebind")
	}
	mode.Store(1)
	r.collect(context.Background())
	stale := r.lookup("", 0, false, time.Now())
	if stale.status != 200 || stale.remainingMilliseconds != 0 || !strings.Contains(stale.body, `"state":"stale"`) {
		t.Fatal("failed poll did not preserve dated stale family")
	}
	mode.Store(0)
	r.collect(context.Background())
	if first != r.items[0] || first.until != lease || first.holdUntil != hold || first.roots[0] != root {
		t.Fatal("same capture renewed")
	}
	for _, generation := range []uint64{2, 3} {
		d.Binding.SourceGeneration = generation
		d.CapturedAt = now.Add(-time.Second).UTC()
		next, err := PrepareMachineDetail(d)
		if err != nil {
			t.Fatal(err)
		}
		current.Store(next)
		r.collect(context.Background())
	}
	if r.sourceGeneration != 3 {
		t.Fatal("wall-clock reversal blocked a newer publication", r.reason)
	}
	current.Store(p)
	r.collect(context.Background())
	if r.sourceGeneration != 3 || r.reason != "binding_changed" {
		t.Fatal("evicted A regained a lease")
	}
	mode.Store(2)
	d.Binding.SourceGeneration = 4
	next, err := PrepareMachineDetail(d)
	if err != nil {
		t.Fatal(err)
	}
	current.Store(next)
	r.collect(context.Background())
	if r.items[0] != nil {
		t.Fatal("accepted A -> B -> A candidate")
	}
	mode.Store(0)
	r.collect(context.Background())
	if r.items[0] == nil {
		t.Fatal("new context could not recover")
	}
	oldRevision := r.revision
	r.observeStatus(&snapshot, "fresh", "", time.Now(), 3)
	if r.revision != oldRevision {
		t.Fatal("ordinary status refresh invalidated transfer")
	}
	expired := r.items[0].holdUntil
	r.observeStatus(&snapshot, "fresh", "", expired, 3)
	got := r.lookup(next.Generation(), 0, true, expired)
	if got.status != 409 {
		t.Fatal("expired pages admitted")
	}
	r.observeStatus(&snapshot, "stale", "status_stale", time.Now(), 4)
	if r.lookup("", 0, false, time.Now()).status != 503 {
		t.Fatal("status failure did not dominate")
	}
	r.observeStatus(&snapshot, "fresh", "", time.Now(), 5)
	r.revision = math.MaxUint64 - 1
	other := snapshot
	other.RunID = "exhausted"
	r.observeStatus(&other, "fresh", "", time.Now(), 6)
	if !r.closed || r.items != [2]*machinePublicFamily{} {
		t.Fatal("revision exhausted without retiring")
	}
}

func TestMachineReaderCachedRepliesAndBudget(t *testing.T) {
	now := time.Now()
	d, _, _, _ := maximumMachineDetail(t)
	p, err := PrepareMachineDetail(d)
	if err != nil {
		t.Fatal(err)
	}
	m, err := decodeMachineManifest([]byte(p.Manifest()))
	if err != nil {
		t.Fatal(err)
	}
	family, err := preparePublicMachine(m, machineReaderPages(p), sha256.Sum256([]byte(p.Manifest())), now, now.Add(20*time.Second), now.Add(20*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	r, err := newMachineDetailReader(newCollector("/unused.sock").client)
	if err != nil {
		t.Fatal(err)
	}
	defer r.client.CloseIdleConnections()
	r.gate = machineDetailGate{binding: m.Binding, observedAt: now, stamp: statusStamp{1, 1}}
	r.revision, family.stamp = 1, r.gate.stamp
	r.items[0] = family
	admit := func(machineReaderReply) (time.Duration, bool) { return time.Second, true }
	for _, path := range []string{machineDetailPublicPath + "?", machineDetailPublicPath + "/", "/api/v1/details/%6dachine", machineDetailPublicPath + "?x=1", machineDetailPublicPath + "?generation=" + m.Generation + "&page=16"} {
		w := httptest.NewRecorder()
		r.serveHTTP(w, httptest.NewRequest("GET", path, nil), admit)
		if w.Code != 400 {
			t.Fatal("invalid grammar", path, w.Code)
		}
	}
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var wg sync.WaitGroup
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }); wg.Wait() }
	defer finish()
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sink := &machineTestSink{header: make(http.Header), onFirstWrite: func() { entered <- struct{}{}; <-release }}
			r.serveHTTP(sink, httptest.NewRequest("GET", machineDetailPublicPath+"?generation="+m.Generation+"&page=1", nil), admit)
		}()
		<-entered
	}
	w := httptest.NewRecorder()
	r.serveHTTP(w, httptest.NewRequest("GET", machineDetailPublicPath, nil), admit)
	if w.Code != 429 {
		t.Fatal("third active reply admitted")
	}
	r.close()
	finish()
	if len(r.active) != 0 {
		t.Fatal("reply admission leaked")
	}
	owned := func(v any) uintptr { return reflect.TypeOf(v).Size() + machineDynamicBytes(reflect.ValueOf(v)) }
	// Valid assembly: current/previous/incoming encoded families, one typed
	// detail, bounded encoder workspaces and two retired selected-page strings.
	validPeak := 3*owned(*family) + owned(d) + 2*MaxMachineDetailBytes + 6*MaxMachinePageBytes + 2*MaxMachineManifestBytes + unsafe.Sizeof(machineDetailReader{})
	// Refused input is checked before full-body encoding. Count a complete
	// capped incoming page set, all decoded string bytes and fixed typed rows.
	refusalPeak := 2*owned(*family) + 2*MaxMachineDetailBytes + owned(d) + 6*MaxMachinePageBytes + 2*MaxMachineManifestBytes + unsafe.Sizeof(machineDetailReader{})
	if max(validPeak, refusalPeak) > 4<<20 {
		t.Fatalf("reader holder model exceeds4MiB: valid=%d refused=%d", validPeak, refusalPeak)
	}
	t.Logf("modeled reader peaks valid=%d refused=%d/4194304; not RSS", validPeak, refusalPeak)
}

func TestMachineReaderFinalAdmissionAndPageHold(t *testing.T) {
	setup := func(t *testing.T) (*collector, *machineDetailReader, string) {
		t.Helper()
		now := time.Now()
		snapshot, capture := machineSourceFixture(t, now, 1)
		manifest, err := decodeMachineManifest([]byte(capture.Prepared.Manifest()))
		if err != nil {
			t.Fatal(err)
		}
		family, err := preparePublicMachine(manifest, machineReaderPages(capture.Prepared), sha256.Sum256([]byte(capture.Prepared.Manifest())), now, now.Add(20*time.Second), now.Add(20*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		r, err := newMachineDetailReader(newCollector("/unused.sock").client)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.client.CloseIdleConnections)
		stamp := r.observeStatus(&snapshot, "fresh", "", now, 1)
		family.stamp, r.items[0] = stamp, family
		c := &collector{clockOrigin: now}
		frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
		frame.fence.nodeEpoch, frame.fence.families[statusMachine] = stamp.nodeEpoch, stamp.familyEpoch
		c.latest.Store(frame)
		return c, r, manifest.Generation
	}
	request := func(r *machineDetailReader, path string, admit func(machineReaderReply) (time.Duration, bool)) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(http.MethodGet, path, nil), admit)
		return response
	}
	t.Run("root and page leases", func(t *testing.T) {
		c, r, generation := setup(t)
		admit := func(reply machineReaderReply) (time.Duration, bool) { return c.admitMachineDetail(r, reply) }
		root := request(r, machineDetailPublicPath, admit)
		detail, detailErr := strconv.ParseInt(root.Header().Get(replayDetailRemainingHeader), 10, 64)
		status, statusErr := strconv.ParseInt(root.Header().Get(statusRemainingHeader), 10, 64)
		page := request(r, machineDetailPublicPath+"?generation="+generation+"&page=0", admit)
		if root.Code != 200 || detailErr != nil || statusErr != nil || status <= 0 || detail <= status ||
			page.Code != 200 || page.Header().Get(statusRemainingHeader) == "" || page.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatal("root and page did not retain their independent status and detail leases")
		}
		if noGate := request(r, machineDetailPublicPath, nil); noGate.Code != 503 {
			t.Fatal("missing status admission exposed a machine family")
		}
	})
	t.Run("retained page after rotation", func(t *testing.T) {
		c, r, generation := setup(t)
		response := request(r, machineDetailPublicPath+"?generation="+generation+"&page=0", func(reply machineReaderReply) (time.Duration, bool) {
			r.mu.Lock()
			next := *reply.family
			r.items[1], r.items[0] = reply.family, &next
			r.mu.Unlock()
			return c.admitMachineDetail(r, reply)
		})
		if response.Code != 200 || response.Header().Get(statusRemainingHeader) == "" {
			t.Fatal("a still-held compatible previous page was rejected")
		}
	})
	t.Run("held page after source failure", func(t *testing.T) {
		c, r, generation := setup(t)
		r.mu.Lock()
		r.reason = "source_unavailable"
		r.mu.Unlock()
		admit := func(reply machineReaderReply) (time.Duration, bool) { return c.admitMachineDetail(r, reply) }
		root := request(r, machineDetailPublicPath, admit)
		page := request(r, machineDetailPublicPath+"?generation="+generation+"&page=0", admit)
		if root.Code != 200 || !strings.Contains(root.Body.String(), `"state":"stale"`) || root.Header().Get(replayDetailRemainingHeader) != "" ||
			page.Code != 200 || page.Header().Get(statusRemainingHeader) == "" {
			t.Fatal("source failure lost the stale root or a still-held page")
		}
	})
	for _, name := range []string{"source failure", "family replaced", "detail shortened", "page hold expired", "gate changed", "parent status expired"} {
		t.Run(name, func(t *testing.T) {
			c, r, generation := setup(t)
			path := machineDetailPublicPath
			if name == "page hold expired" {
				path += "?generation=" + generation + "&page=0"
			}
			response := request(r, path, func(reply machineReaderReply) (time.Duration, bool) {
				r.mu.Lock()
				switch name {
				case "source failure":
					r.reason = "source_unavailable"
				case "family replaced":
					next := *reply.family
					r.items[0] = &next
				case "detail shortened":
					reply.family.until = time.Now().Add(time.Second)
				case "page hold expired":
					reply.family.holdUntil = time.Now().Add(-time.Second)
				case "gate changed":
					r.gate.stamp = statusStamp{2, 2}
				}
				r.mu.Unlock()
				if name == "parent status expired" {
					frame := *c.latest.Load()
					frame.freshUntil = time.Now().Add(-time.Second)
					c.latest.Store(&frame)
				}
				return c.admitMachineDetail(r, reply)
			})
			if response.Code != 503 || response.Header().Get(statusRemainingHeader) != "" || response.Header().Get(replayDetailRemainingHeader) != "" {
				t.Fatal("changed or expired machine admission remained fresh")
			}
		})
	}
}

func TestMachineReaderSupportsBothRoles(t *testing.T) {
	now := time.Now()
	snapshot, _ := machineSourceFixture(t, now, 1)
	r, err := newMachineDetailReader(newCollector("/unused.sock").client)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.client.CloseIdleConnections)
	var first statusStamp
	for _, role := range []string{"verifying", "validator"} {
		snapshot.Mode = role
		stamp := r.observeStatus(&snapshot, "fresh", "", time.Now(), 1)
		if !stamp.valid() || r.gate.failure(time.Now()) != "" || first.valid() && stamp != first {
			t.Fatalf("%s did not retain a compatible machine status gate", role)
		}
		first = stamp
	}
}
