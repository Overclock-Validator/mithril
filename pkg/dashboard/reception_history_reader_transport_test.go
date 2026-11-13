package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This is a real Unix HTTP peer, not a reader/source callback mock. Its only
// mutable fixtures are protected separately from the collector being tested.
type receptionCollectorPeer struct {
	mu           sync.Mutex
	root         ReceptionHistoryRoot
	metadata     map[string]*PreparedReceptionMetadata
	chunks       []PreparedReceptionChunk
	remaining    string
	paths        []string
	bytes        int
	mutate       func(string, *receptionHistoryReply)
	active, peak atomic.Int64
}

func newReceptionCollectorPeer(t *testing.T, m ReceptionMetadata, chunks []PreparedReceptionChunk) (*receptionCollectorPeer, *receptionHistoryReader, *collector) {
	t.Helper()
	f := &receptionCollectorPeer{metadata: make(map[string]*PreparedReceptionMetadata), remaining: "1000"}
	f.set(t, m, chunks)
	path := replayDetailReaderServer(t, f.serve)
	c := newCollector(path)
	c.public, c.publicOptions = true, PublicOptions{Resources: true, ReplayDetails: true, NetworkDetails: true, ReceptionHistory: true}
	c.poll(context.Background(), time.Now().UTC())
	r, err := newReceptionHistoryReader(c.client, &c.receptionGate)
	if err != nil {
		t.Fatal(err)
	}
	r.admitStatus = func(selected receptionPublicSelection) (time.Duration, bool) {
		return c.admitReceptionHistory(r, selected)
	}
	t.Cleanup(c.client.CloseIdleConnections)
	t.Cleanup(r.client.CloseIdleConnections)
	return f, r, c
}

func (f *receptionCollectorPeer) set(t *testing.T, m ReceptionMetadata, chunks []PreparedReceptionChunk) {
	t.Helper()
	p, err := PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	root := receptionTestRoot(t, m)
	root.ConsumedThrough = m.StartupAtCapture.PrefixCutoff
	if len(chunks) > 0 {
		first, last := chunks[0].descriptor.FirstSequence, chunks[len(chunks)-1].descriptor.LastSequence
		root.SourceBounds, root.CacheBounds = ReceptionSequenceBounds{First: &first, Last: &last}, ReceptionSequenceBounds{First: &first, Last: &last}
		root.ConsumedThrough = max(root.ConsumedThrough, last)
	}
	f.mu.Lock()
	f.root, f.chunks = root, chunks
	f.metadata[p.manifest.Digest] = p
	f.mu.Unlock()
}

func (f *receptionCollectorPeer) serve(w http.ResponseWriter, request *http.Request) {
	active := f.active.Add(1)
	defer f.active.Add(-1)
	for old := f.peak.Load(); active > old && !f.peak.CompareAndSwap(old, active); old = f.peak.Load() {
	}
	f.mu.Lock()
	f.paths = append(f.paths, request.URL.RequestURI())
	if request.URL.Path == "/snapshot" {
		s := testSnapshot(time.Now().UTC())
		s.RunID, s.StartedAt, s.GenesisHash, s.Replay = f.root.Binding.RunID, f.root.Binding.NodeStartedAt, &f.root.Binding.GenesisHash, nil
		body, err := json.Marshal(s)
		f.mu.Unlock()
		if err != nil {
			http.Error(w, "invalid fixture", 500)
			return
		}
		_, _ = w.Write(body)
		return
	}
	q, status := parseReceptionHistoryRequest(request)
	reply := receptionHistoryReply{status: 200}
	if status != 0 {
		reply = receptionHistoryError(status, "invalid_request", "", ReceptionSequenceBounds{}, nil)
	} else if q.kind == "root" {
		body, err := PrepareReceptionHistoryRoot(f.root)
		if err != nil {
			reply = receptionHistoryError(503, "source_invalid", "", ReceptionSequenceBounds{}, nil)
		} else {
			reply.body = body
		}
		if f.root.State == "fresh" {
			reply.remaining, _ = strconv.ParseInt(f.remaining, 10, 64)
		}
	} else if q.recording != f.root.RecordingID {
		reply = receptionHistoryError(409, "binding_changed", f.root.RecordingID, f.root.CacheBounds, nil)
	} else {
		switch q.kind {
		case "index":
			index := ReceptionHistoryIndex{RecordingID: q.recording, After: q.after, CacheBounds: f.root.CacheBounds, Chunks: []ReceptionChunkDescriptor{}}
			for _, p := range f.chunks {
				if p.descriptor.LastSequence > q.after && len(index.Chunks) < 8 {
					index.Chunks = append(index.Chunks, p.descriptor)
				}
			}
			if len(index.Chunks) != 0 && index.Chunks[0].FirstSequence > q.after && index.Chunks[0].FirstSequence-q.after > 1 {
				next := index.Chunks[0].FirstSequence
				reply = receptionHistoryError(409, "cache_gap", q.recording, index.CacheBounds, &next)
			} else {
				var err error
				reply.body, err = PrepareReceptionHistoryIndex(index)
				if err != nil {
					reply = receptionHistoryError(503, "source_invalid", q.recording, index.CacheBounds, nil)
				}
			}
		case "chunk":
			reply = receptionHistoryError(409, "chunk_unavailable", q.recording, f.root.CacheBounds, nil)
			for _, p := range f.chunks {
				if p.descriptor.Digest == q.digest {
					reply = receptionHistoryReply{status: 200, body: p.body, etag: q.digest}
					break
				}
			}
		case "metadata":
			reply = receptionHistoryError(409, "metadata_expired", q.recording, f.root.CacheBounds, nil)
			if p := f.metadata[q.digest]; p != nil {
				for i, d := range p.manifest.Pages {
					if d.Kind == q.metadataKind && d.Page == q.page {
						reply = receptionHistoryReply{status: 200, body: p.pages[i], etag: d.Digest}
						break
					}
				}
			}
		}
	}
	mutate := f.mutate
	f.mu.Unlock()
	if mutate != nil {
		mutate(request.URL.RequestURI(), &reply)
	}
	w.Header().Set("Content-Type", "application/json")
	if reply.remaining != 0 {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(reply.remaining, 10))
	}
	if reply.etag != "" {
		w.Header().Set("ETag", `"`+reply.etag+`"`)
	}
	w.WriteHeader(reply.status)
	n, _ := io.WriteString(w, reply.body)
	f.mu.Lock()
	f.bytes += n
	f.mu.Unlock()
}

func (f *receptionCollectorPeer) counters() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.paths), f.bytes
}

func TestReceptionCollectorRealUnixFixedTokenAndBudgets(t *testing.T) {
	m := maximumReceptionMetadata()
	m.Binding = receptionCollectorMetadata().Binding
	m.Revision, m.StartupAtCapture = 1, ReceptionStartup{State: "open"}
	var chunks []PreparedReceptionChunk
	for i := range 8 {
		chunks = append(chunks, receptionChunkFixture(t, uint64(1+i*128), 128, true))
	}
	f, r, c := newReceptionCollectorPeer(t, m, chunks)
	firstToken := f.root.Metadata.Digest
	var body string
	for cycle := 0; cycle < 40; cycle++ {
		before, bytesBefore := f.counters()
		previousCursor := r.cursor
		c.observeReceptionHistoryStatus("fresh", time.Now())
		r.collect(context.Background())
		after, bytesAfter := f.counters()
		if after-before > 14 || bytesAfter-bytesBefore > receptionReaderCycleBytes || f.peak.Load() > 2 {
			t.Fatal("cycle envelope exceeded", after-before, bytesAfter-bytesBefore, f.peak.Load())
		}
		if r.cursor < previousCursor || r.cursor > 1024 || r.cursor%128 != 0 {
			t.Fatal("invalid chunk progress", previousCursor, r.cursor)
		}
		body, _, _ = r.rootBody(time.Now())
		if body != "" && r.pending != nil && r.pending.manifest.Digest == firstToken {
			t.Fatal("partial metadata root published")
		}
		if body != "" {
			published, err := decodeReceptionHistoryRoot([]byte(body))
			if err != nil || published.Metadata.Digest != firstToken || published.Metadata.Revision != 1 || len(published.Metadata.Pages) != 52 {
				t.Fatal("invalid or incomplete published metadata", err)
			}
			for _, page := range published.Metadata.Pages {
				data, descriptor, ok := r.cache.MetadataPage(published.RecordingID, published.Metadata.Digest, page.Kind, page.Page, time.Now())
				if !ok || descriptor != page || receptionDigest([]byte(data)) != page.Digest {
					t.Fatal("published metadata page missing or invalid", page.Kind, page.Page)
				}
			}
		}
		if body == "" && r.pending != nil {
			if r.pending.manifest.Digest != firstToken {
				t.Fatal("pending metadata changed token")
			}
			m.Revision++
			m.PublishedAt = m.PublishedAt.Add(-time.Second)
			f.set(t, m, chunks)
		}
		if body != "" && r.cursor == 1024 {
			break
		}
	}
	body, _, reason := r.rootBody(time.Now())
	root, err := decodeReceptionHistoryRoot([]byte(body))
	if reason != "" || err != nil || root.Metadata.Digest != firstToken || root.Metadata.Revision != 1 || len(root.Metadata.Pages) != 52 || r.cursor != 1024 {
		t.Fatal("latest roots starved/relabelled fixed token", reason, err, r.cursor)
	}
	if root.Metadata.PublishedAt.Equal(root.PublishedAt) {
		t.Fatal("old metadata was redated as current")
	}
	got, err := r.cache.Cursor(context.Background(), root.RecordingID, 0)
	if err != nil || got.Count != 128 || got.ScannedThrough != 128 {
		t.Fatal("reader retained rows differ", err)
	}
	for _, p := range chunks {
		body, ok := r.cache.ChunkBody(root.RecordingID, p.descriptor.Digest)
		if !ok || body != p.body {
			t.Fatal("original chunk changed")
		}
	}
	// A newer closed marker is compatible with older open pages, but their
	// frozen startup state must not be relabeled closed coverage.
	f.mu.Lock()
	f.root.Startup = ReceptionStartup{State: "closed", PrefixCutoff: 1024, Marker: &ReceptionMarker{At: m.PublishedAt, SourceGeneration: 1, Bankhash: m.Binding.GenesisHash, ReplayAttempt: 1, CompletedIndex: 1}}
	f.root.Closed, f.root.State = true, "closed"
	f.mu.Unlock()
	r.collect(context.Background())
	body, until, reason := r.rootBody(time.Now())
	root, err = decodeReceptionHistoryRoot([]byte(body))
	if reason != "" || err != nil || !until.IsZero() || root.Startup.State != "closed" || root.Metadata.StartupAtCapture.State != "open" {
		t.Fatal("historical metadata/terminal root conflated", err)
	}
}

func TestReceptionCollectorRealUnixCrossPageRefusal(t *testing.T) {
	for _, kind := range []string{"source_equal", "source_reversed", "range_overlap", "range_adjacent"} {
		t.Run(kind, func(t *testing.T) {
			m := receptionCollectorMetadata()
			bad := receptionCrossPageTestBundle(t, m, kind)
			f, r, _ := newReceptionCollectorPeer(t, m, nil)
			f.mu.Lock()
			f.root.Metadata = bad.Manifest()
			f.metadata[bad.manifest.Digest] = bad
			f.mu.Unlock()
			r.collect(context.Background())
			if r.cache == nil || r.cache.metadataRevision != 0 || r.pending != nil || r.expiredMetadata != bad.manifest.Digest || r.publication.body != "" {
				t.Fatal("collector installed invalid complete metadata or did not reach final validation")
			}
			if body, _, _ := r.rootBody(time.Now()); body != "" {
				t.Fatal("invalid complete metadata became a current root")
			}
			f.mu.Lock()
			pages := 0
			for _, path := range f.paths {
				if strings.Contains(path, "/metadata/") {
					pages++
				}
			}
			f.mu.Unlock()
			if pages != 2 {
				t.Fatal("did not validate both individually valid pages over Unix", pages)
			}
		})
	}
}

func TestReceptionCollectorRealUnixGapOutrunAndMaxCursor(t *testing.T) {
	m := receptionCollectorMetadata()
	chunks := []PreparedReceptionChunk{receptionChunkFixture(t, 10, 2, false), receptionChunkFixture(t, 14, 1, false)}
	f, r, c := newReceptionCollectorPeer(t, m, chunks)
	r.collect(context.Background()) // explicit index gap0 -> first10
	if r.cursor != 9 || r.cacheGaps != 1 {
		t.Fatal("exact initial gap not acknowledged")
	}
	r.collect(context.Background())
	if r.cursor != 14 || r.cacheGaps != 2 || r.last.Startup.Losses != (ReceptionLosses{}) {
		t.Fatal("gap became source loss or cursor stopped", r.cursor, r.cacheGaps)
	}
	read, err := r.cache.Cursor(context.Background(), r.last.RecordingID, 10)
	if err == nil || read.NextAvailable == nil || *read.NextAvailable != 14 {
		t.Fatal("interior availability gap disappeared", err)
	}
	// Simulate a source that outruns both adapters without changing protocol
	// counters. The next exact range still advances after explicit refusal.
	chunks = []PreparedReceptionChunk{receptionChunkFixture(t, 100, 1, false)}
	m.Revision++
	f.set(t, m, chunks)
	c.observeReceptionHistoryStatus("fresh", time.Now())
	r.collect(context.Background())
	r.collect(context.Background())
	if r.cursor != 100 {
		t.Fatal("out-run stopped cursor progress", r.cursor)
	}
	// A missing tail without an exact next sequence must not skip the source's
	// broader consumed frontier. A later successful index can recover it.
	f.mu.Lock()
	f.root.ConsumedThrough = 200
	f.mutate = func(path string, reply *receptionHistoryReply) {
		if strings.Contains(path, "/index/") {
			*reply = receptionHistoryError(409, "cache_gap", f.root.RecordingID, ReceptionSequenceBounds{}, nil)
		}
	}
	f.mu.Unlock()
	r.collect(context.Background())
	if r.cursor != 100 {
		t.Fatal("unknown gap tail skipped future node imports")
	}
	f.mu.Lock()
	f.mutate = nil
	f.mu.Unlock()
	chunks = []PreparedReceptionChunk{receptionChunkFixture(t, math.MaxUint64, 1, true)}
	m.Revision++
	f.set(t, m, chunks)
	r.collect(context.Background())
	r.collect(context.Background())
	if r.cursor != math.MaxUint64 {
		t.Fatal("MaxUint64 import overflowed", r.cursor)
	}
	r.collect(context.Background())
	if r.cursor != math.MaxUint64 {
		t.Fatal("future cursor wrapped")
	}
}

func TestReceptionCollectorRealUnixPartialFailures(t *testing.T) {
	for _, mode := range []string{"chunk429", "chunk-corrupt", "metadata409", "metadata429", "metadata-corrupt", "metadata-etag", "root-remaining", "root-unknown", "root404"} {
		t.Run(mode, func(t *testing.T) {
			m := receptionCollectorMetadata()
			m.Sources = []ReceptionSource{{Generation: 1, StartedAt: m.PublishedAt, Kind: "live"}}
			chunks := []PreparedReceptionChunk{receptionChunkFixture(t, 1, 1, false), receptionChunkFixture(t, 2, 1, false)}
			f, r, _ := newReceptionCollectorPeer(t, m, chunks)
			id := f.root.RecordingID
			f.mu.Lock()
			f.mutate = func(path string, reply *receptionHistoryReply) {
				switch {
				case mode == "chunk429" && strings.Contains(path, "/chunk/"):
					*reply = receptionHistoryError(429, "rate_limited", id, ReceptionSequenceBounds{}, nil)
				case mode == "chunk-corrupt" && strings.Contains(path, "/chunk/"):
					reply.body = "{}"
				case mode == "metadata409" && strings.Contains(path, "/metadata/"):
					*reply = receptionHistoryError(409, "metadata_expired", id, ReceptionSequenceBounds{}, nil)
				case mode == "metadata429" && strings.Contains(path, "/metadata/"):
					*reply = receptionHistoryError(429, "rate_limited", id, ReceptionSequenceBounds{}, nil)
				case mode == "metadata-corrupt" && strings.Contains(path, "/metadata/"):
					reply.body = "{}"
				case mode == "metadata-etag" && strings.Contains(path, "/metadata/"):
					reply.etag = strings.Repeat("0", 64)
				case mode == "root-remaining" && path == receptionHistoryPath:
					reply.remaining = receptionHistoryFreshFor.Milliseconds() + 1
				case mode == "root-unknown" && path == receptionHistoryPath:
					reply.body = strings.TrimSuffix(reply.body, "}") + `,"unknown":0}`
				case mode == "root404" && path == receptionHistoryPath:
					reply.status, reply.body, reply.remaining = 404, "not found", 0
				}
			}
			f.mu.Unlock()
			r.collect(context.Background())
			if r.failure == "" {
				t.Fatal("failed transport accepted")
			}
			if strings.HasPrefix(mode, "chunk") && r.cursor != 0 {
				t.Fatal("failed earlier chunk silently skipped")
			}
			if strings.HasPrefix(mode, "metadata") && r.publication.body != "" {
				t.Fatal("partial/corrupt metadata published")
			}
			if mode == "metadata409" && r.pending != nil {
				t.Fatal("expired fixed token retained")
			}
			if mode == "root404" && r.failure != "unsupported_source" {
				t.Fatal("unsupported source classification")
			}
			f.mu.Lock()
			f.mutate = nil
			f.mu.Unlock()
			r.collect(context.Background())
			if r.cursor != 2 || r.publication.body == "" || r.failure != "" {
				t.Fatal("same bounded next cycle did not recover", r.failure)
			}
		})
	}
}

func TestReceptionCollectorRealUnixOriginalDeadlineAndABA(t *testing.T) {
	for _, kind := range []string{"late-headers", "late-body", "ABA", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			m := receptionCollectorMetadata()
			root := receptionTestRoot(t, m)
			body, err := PrepareReceptionHistoryRoot(root)
			if err != nil {
				t.Fatal(err)
			}
			entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			path := replayDetailReaderServer(t, func(w http.ResponseWriter, req *http.Request) {
				defer close(exited)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set(replayDetailRemainingHeader, "1")
				if kind == "late-body" {
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
				}
				close(entered)
				select {
				case <-release:
				case <-req.Context().Done():
					return
				}
				_, _ = io.WriteString(w, body)
			})
			r, c := receptionCollectorForTest(t, root)
			fixed, err := newReceptionHistoryReader(newCollector(path).client, &c.receptionGate)
			if err != nil {
				t.Fatal(err)
			}
			r.client.CloseIdleConnections()
			r = fixed
			t.Cleanup(func() { unblock(); r.client.CloseIdleConnections() })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() { r.collect(ctx); close(done) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("request did not start")
			}
			if kind == "ABA" {
				c.observeReceptionHistoryStatus("unavailable", time.Now())
				c.observeReceptionHistoryStatus("fresh", time.Now())
			}
			if kind == "cancel" {
				cancel()
			}
			if strings.HasPrefix(kind, "late") {
				time.Sleep(3 * time.Millisecond)
			}
			unblock()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("cycle did not join")
			}
			select {
			case <-exited:
			case <-time.After(time.Second):
				t.Fatal("Unix handler not canceled")
			}
			if r.cache != nil || r.publication.body != "" {
				t.Fatal("late/ABA/canceled response committed")
			}
		})
	}
}

func TestReceptionCollectorReadBudgetAndHeaderRefusals(t *testing.T) {
	budget := &receptionCycleBudget{}
	budget.bytes.Store(receptionReaderCycleBytes - 3)
	buf := make([]byte, 8)
	rd := receptionBudgetReader{strings.NewReader("abcdef"), budget}
	if n, err := rd.Read(buf); n != 3 || err != nil {
		t.Fatal("remaining read budget not clipped", n, err)
	}
	if n, err := rd.Read(buf); n != 0 || err == nil || budget.bytes.Load() != receptionReaderCycleBytes {
		t.Fatal("budget read past cap")
	}
	for _, kind := range []string{"oversized", "encoding", "content-type", "duplicate-remainder", "data-remainder", "leading-zero", "error-extra", "error-next-zero", "error-next-outside"} {
		t.Run(kind, func(t *testing.T) {
			path := replayDetailReaderServer(t, func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				body := `{}`
				switch kind {
				case "oversized":
					body = strings.Repeat("x", MaxReceptionRootBytes+1)
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
				case "content-type":
					w.Header().Set("Content-Type", "text/plain")
				case "duplicate-remainder":
					w.Header().Add(replayDetailRemainingHeader, "1000")
					w.Header().Add(replayDetailRemainingHeader, "1000")
				case "data-remainder":
					w.Header().Set(replayDetailRemainingHeader, "1000")
				case "leading-zero":
					w.Header().Set(replayDetailRemainingHeader, "01")
				case "error-extra":
					w.WriteHeader(409)
					body = strings.TrimSuffix(receptionHistoryError(409, "cache_gap", "", ReceptionSequenceBounds{}, nil).body, "}") + `,"extra":true}`
				case "error-next-zero":
					w.WriteHeader(409)
					zero := uint64(0)
					body = receptionHistoryError(409, "cache_gap", "", ReceptionSequenceBounds{}, &zero).body
				case "error-next-outside":
					w.WriteHeader(409)
					first, last, next := uint64(1), uint64(2), uint64(3)
					body = receptionHistoryError(409, "cache_gap", strings.Repeat("a", 64), ReceptionSequenceBounds{First: &first, Last: &last}, &next).body
				}
				_, _ = io.WriteString(w, body)
			})
			var gate receptionReaderStatus
			r, err := newReceptionHistoryReader(newCollector(path).client, &gate)
			if err != nil {
				t.Fatal(err)
			}
			defer r.client.CloseIdleConnections()
			response := r.fetch(context.Background(), &receptionCycleBudget{}, receptionHistoryPath, "", MaxReceptionRootBytes, kind != "data-remainder")
			want := "source_invalid"
			if kind == "oversized" {
				want = "response_limit"
			}
			if response.reason != want {
				t.Fatalf("invalid HTTP boundary reason=%q, want %q", response.reason, want)
			}
		})
	}
}

func TestReceptionCollectorLeaseHeaderBoundary(t *testing.T) {
	f, r, _ := newReceptionCollectorPeer(t, receptionCollectorMetadata(), nil)
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"3000", true},
		{"3001", false},
	} {
		f.mu.Lock()
		f.remaining = tc.value
		f.mu.Unlock()
		response := r.fetch(context.Background(), &receptionCycleBudget{}, receptionHistoryPath, "", MaxReceptionRootBytes, true)
		if tc.valid && (response.reason != "" || response.remaining != receptionHistoryFreshFor) || !tc.valid && response.reason != "source_invalid" {
			t.Fatalf("remaining %s: reason=%q duration=%v", tc.value, response.reason, response.remaining)
		}
	}
}

func TestReceptionCollectorMalformedGapCannotAdvanceCursor(t *testing.T) {
	m := receptionCollectorMetadata()
	chunks := []PreparedReceptionChunk{receptionChunkFixture(t, 1, 3, false)}
	f, r, _ := newReceptionCollectorPeer(t, m, chunks)
	id := f.root.RecordingID
	f.mu.Lock()
	f.mutate = func(path string, reply *receptionHistoryReply) {
		if strings.Contains(path, "/index/") {
			first, last, next := uint64(1), uint64(2), uint64(3)
			*reply = receptionHistoryError(409, "cache_gap", id, ReceptionSequenceBounds{First: &first, Last: &last}, &next)
		}
	}
	f.mu.Unlock()
	r.collect(context.Background())
	if r.last.ConsumedThrough < 3 {
		t.Fatal("fixture did not admit the wider source frontier")
	}
	if r.failure != "source_invalid" || r.cursor != 0 || r.cacheGaps != 0 {
		t.Fatalf("malformed gap became an acknowledged interval: failure=%q cursor=%d gaps=%d", r.failure, r.cursor, r.cacheGaps)
	}
	index, _, ok := r.cache.Index(id, 0)
	if !ok || len(index.Chunks) != 0 {
		t.Fatal("malformed gap installed later data")
	}
}

func TestReceptionCollectorServerOptInRoutesAndJoin(t *testing.T) {
	for _, missing := range []string{"origin", "resources", "replay", "network"} {
		t.Run(missing, func(t *testing.T) {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			options := PublicOptions{Resources: true, ReplayDetails: true, NetworkDetails: true, ReceptionHistory: true}
			origin := "https://dashboard.test"
			switch missing {
			case "origin":
				origin = ""
			case "resources":
				options.Resources = false
			case "replay":
				options.ReplayDetails = false
			case "network":
				options.NetworkDetails = false
			}
			if err := serveProfile(context.Background(), "/unused.sock", l, origin, options); err == nil {
				t.Fatal("missing prerequisite allowed")
			}
		})
	}
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			m := receptionCollectorMetadata()
			f := &receptionCollectorPeer{metadata: make(map[string]*PreparedReceptionMetadata), remaining: "1000"}
			f.set(t, m, nil)
			entered := make(chan struct{}, 1)
			var active atomic.Int64
			path := replayDetailReaderServer(t, func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == receptionHistoryPath {
					active.Add(1)
					defer active.Add(-1)
					select {
					case entered <- struct{}{}:
					default:
					}
					<-req.Context().Done()
					return
				}
				f.serve(w, req)
			})
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			options := PublicOptions{Resources: true, ReplayDetails: true, NetworkDetails: true, ReceptionHistory: enabled}
			go func() { done <- serveProfile(ctx, path, l, "https://dashboard.test", options) }()
			t.Cleanup(func() { cancel() })
			client := &http.Client{Timeout: time.Second}
			t.Cleanup(client.CloseIdleConnections)
			req, _ := http.NewRequest(http.MethodGet, "http://"+l.Addr().String()+"/api/v1/details/reception", nil)
			req.Host = "dashboard.test"
			response, err := client.Do(req)
			if err != nil {
				cancel()
				<-done
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			want := http.StatusNotFound
			if enabled {
				want = http.StatusServiceUnavailable // The owned source request is deliberately blocked.
			}
			if response.StatusCode != want {
				t.Fatal("public route opt-in/pending state", response.StatusCode, want)
			}
			if enabled {
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("independent worker did not start")
				}
			} else {
				select {
				case <-entered:
					t.Fatal("default-off made reception request")
				case <-time.After(300 * time.Millisecond):
				}
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("server did not join worker")
			}
			deadline := time.Now().Add(time.Second)
			for active.Load() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if active.Load() != 0 {
				t.Fatal("owned Unix request survived cancellation")
			}
		})
	}
}

func TestReceptionCollectorRequestStartsAndCycleDeadline(t *testing.T) {
	// A nil client would panic if either rejected request reached Do. Neither
	// refusal is allowed to start I/O, regardless of transport configuration.
	budget := &receptionCycleBudget{}
	budget.starts.Store(receptionReaderCycleStarts)
	r := &receptionHistoryReader{}
	response := r.fetch(context.Background(), budget, receptionHistoryPath, "", MaxReceptionRootBytes, true)
	if response.reason != "response_limit" {
		t.Fatal("fifteenth request started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	budget = &receptionCycleBudget{}
	r.fetch(ctx, budget, receptionHistoryPath, "", MaxReceptionRootBytes, true)
	if budget.starts.Load() != 0 {
		t.Fatal("canceled cycle started request")
	}
}

func TestReceptionCollectorRealUnixCycleDeadlineKeepsValidatedPrefix(t *testing.T) {
	m := receptionCollectorMetadata()
	for i := range 192 {
		m.Sources = append(m.Sources, ReceptionSource{Generation: uint64(i + 1), StartedAt: m.PublishedAt, Kind: "live"})
	}
	f := &receptionCollectorPeer{metadata: make(map[string]*PreparedReceptionMetadata), remaining: "1000"}
	f.set(t, m, nil)
	var slow atomic.Bool
	slow.Store(true)
	var started atomic.Int64
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.Contains(req.URL.Path, "/metadata/") {
			request := started.Add(1)
			if slow.Load() {
				if request >= 3 {
					<-req.Context().Done()
					return
				}
				timer := time.NewTimer(70 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-req.Context().Done():
					return
				}
			}
		}
		f.serve(w, req)
	})
	r, c := receptionCollectorForTest(t, f.root)
	r.client.CloseIdleConnections()
	var err error
	r, err = newReceptionHistoryReader(newCollector(path).client, &c.receptionGate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.client.CloseIdleConnections)
	start := time.Now()
	r.collect(context.Background())
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatal("cycle deadline reset or unbounded", elapsed)
	}
	if r.pending == nil {
		t.Fatal("deadline discarded pending metadata")
	}
	completed := 0
	for i := range 3 {
		if r.pending.pages[i].body != "" {
			if completed != i {
				t.Fatal("metadata prefix has a gap")
			}
			completed++
		}
	}
	requests := started.Load()
	if completed < 1 || completed >= 3 || r.publication.body != "" || requests < int64(completed) || requests > int64(completed+1) {
		t.Fatalf("deadline prefix: completed=%d published=%t requests=%d", completed, r.publication.body != "", requests)
	}
	until := r.until
	slow.Store(false)
	r.collect(context.Background())
	if r.publication.body == "" || r.pending != nil || started.Load() != requests+int64(3-completed) || r.until != until {
		t.Fatal("next cycle restarted pages or renewed source")
	}
	// A single response cannot spend the whole cycle budget: the100ms child
	// timeout also applies when the parent has much more time remaining.
	entered, stopped := make(chan struct{}), make(chan struct{})
	path2 := replayDetailReaderServer(t, func(w http.ResponseWriter, req *http.Request) { close(entered); <-req.Context().Done(); close(stopped) })
	one, err := newReceptionHistoryReader(newCollector(path2).client, &c.receptionGate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(one.client.CloseIdleConnections)
	start = time.Now()
	response := one.fetch(context.Background(), &receptionCycleBudget{}, receptionHistoryPath, "", MaxReceptionRootBytes, true)
	if response.reason == "" || time.Since(start) > 400*time.Millisecond {
		t.Fatal("individual request timeout missing")
	}
	select {
	case <-entered:
	default:
		t.Fatal("timeout test never reached Unix peer")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("timed-out request remained active")
	}
}

func TestReceptionCollectorActualFeedSixSecondMetadataHold(t *testing.T) {
	m := maximumReceptionMetadata()
	m.Binding = receptionCollectorMetadata().Binding
	m.StartupAtCapture = ReceptionStartup{State: "open"}
	var changed atomic.Bool
	sample := ReceptionHistorySampler{Metadata: func() (ReceptionHistoryCapture, error) {
		at := time.Now()
		value := m
		value.PublishedAt = at.UTC()
		value.Sources = append([]ReceptionSource(nil), m.Sources...)
		value.Sources[0].Retired = changed.Load()
		return ReceptionHistoryCapture{Metadata: value, PublishedAt: at}, nil
	}, ChunkBody: func(ReceptionRecordingBinding, ReceptionChunkDescriptor) (string, bool) { return "", false }}
	snapshot := func() Snapshot {
		s := testSnapshot(time.Now().UTC())
		s.RunID, s.StartedAt, s.GenesisHash = m.Binding.RunID, m.Binding.NodeStartedAt, &m.Binding.GenesisHash
		return s
	}
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", snapshot, DetailSamplers{Reception: &sample})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	c := newCollector(path)
	c.public, c.publicOptions = true, PublicOptions{Resources: true, ReplayDetails: true, NetworkDetails: true, ReceptionHistory: true}
	t.Cleanup(c.client.CloseIdleConnections)
	var first ReceptionHistoryRoot
	waitReceptionTest(t, func() bool {
		status, body, _ := receptionHTTPTest(t, c.client, receptionHistoryPath)
		if status != 200 {
			return false
		}
		first, err = decodeReceptionHistoryRoot([]byte(body))
		return err == nil
	})
	feed.reception.mu.RLock()
	cache := feed.reception.cache
	cache.mu.RLock()
	installedAt := cache.metadataInstalledAt
	cache.mu.RUnlock()
	feed.reception.mu.RUnlock()
	if len(first.Metadata.Pages) != 52 || installedAt.IsZero() {
		t.Fatal("actual source maximum token missing")
	}
	// Start shortly before the real six-second boundary, without rewriting a
	// clock or token. A bounded transfer checks that the previous token remains
	// available while the source replaces its current one.
	if delay := time.Until(installedAt.Add(receptionMetadataHold - 600*time.Millisecond)); delay > 0 {
		time.Sleep(delay)
	}
	c.poll(context.Background(), time.Now().UTC())
	r, err := newReceptionHistoryReader(c.client, &c.receptionGate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.client.CloseIdleConnections)
	transferStarted := time.Now()
	r.collect(context.Background())
	if r.pending == nil || r.pending.manifest.Digest != first.Metadata.Digest {
		t.Fatal("first fixed transfer missing")
	}
	changed.Store(true)
	var body, reason string
	for time.Since(transferStarted) < receptionMetadataHold {
		time.Sleep(receptionHistoryInterval)
		c.poll(context.Background(), time.Now().UTC())
		r.collect(context.Background())
		body, _, reason = r.rootBody(time.Now())
		if body != "" {
			break
		}
	}
	if elapsed := time.Since(transferStarted); body == "" || elapsed >= receptionMetadataHold {
		t.Fatal("actual fixed-token transfer exceeded six-second bound", elapsed, reason)
	}
	root, err := decodeReceptionHistoryRoot([]byte(body))
	if reason != "" || err != nil || root.Metadata.Digest != first.Metadata.Digest || root.Metadata.Revision != 1 {
		t.Fatal("actual retained previous token did not finish", reason, err)
	}
	status, sourceBody, _ := receptionHTTPTest(t, c.client, receptionHistoryPath)
	latest, err := decodeReceptionHistoryRoot([]byte(sourceBody))
	if status != 200 || err != nil || latest.Metadata.Revision != 2 || latest.Metadata.Digest == first.Metadata.Digest {
		t.Fatal("actual node6s replacement not witnessed", status, err)
	}
	feed.reception.mu.RLock()
	cache = feed.reception.cache
	cache.mu.RLock()
	currentAt, currentDigest := cache.metadataInstalledAt, cache.currentMetadata.manifest.Digest
	cache.mu.RUnlock()
	feed.reception.mu.RUnlock()
	if currentDigest != latest.Metadata.Digest || currentAt.Sub(installedAt) < receptionMetadataHold {
		t.Fatal("source token changed before hold", currentAt.Sub(installedAt))
	}
	page := first.Metadata.Pages[0]
	status, pageBody, _ := receptionHTTPTest(t, c.client, receptionHistoryPath+"/metadata/"+first.RecordingID+"/"+first.Metadata.Digest+"/"+page.Kind+"/"+strconv.Itoa(page.Page))
	if status != 200 || receptionDigest([]byte(pageBody)) != page.Digest {
		t.Fatal("actual previous token retired before reader completion")
	}
}
