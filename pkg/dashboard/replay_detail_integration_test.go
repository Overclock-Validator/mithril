package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func replayDetailPublicServer(t *testing.T, path string, options PublicOptions) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveProfile(ctx, path, listener, "https://stats.example", options) }()
	return "http://" + listener.Addr().String(), cancel, done
}

func replayDetailPublicRequest(t *testing.T, address, path, host, origin string) (int, []byte, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, address+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	response, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxSnapshotBytes+1))
	if err != nil || len(body) > MaxSnapshotBytes {
		t.Fatalf("public detail body: bytes=%d error=%v", len(body), err)
	}
	return response.StatusCode, body, response.Header
}

func replayDetailStopPublic(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("dashboard did not join status and detail workers")
	}
}

func TestReplayDetailPublicRequiresResourceDisclosure(t *testing.T) {
	for _, tc := range []struct {
		origin string
		opts   PublicOptions
	}{
		{"", PublicOptions{ReplayDetails: true}},
		{"", PublicOptions{Resources: true, ReplayDetails: true}},
		{"https://stats.example", PublicOptions{ReplayDetails: true}},
	} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if err := serveProfile(context.Background(), "/unused.sock", listener, tc.origin, tc.opts); err == nil || !strings.Contains(err.Error(), "resource disclosure") {
			t.Fatalf("detail disclosure accepted: origin=%q options=%+v error=%v", tc.origin, tc.opts, err)
		}
	}
}

func TestReplayDetailCollectorPublishesDetachedReceiptGate(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public, c.publicOptions = true, PublicOptions{Resources: true, ReplayDetails: true}
	t.Cleanup(c.client.CloseIdleConnections)
	core := replayDetailLiveCore(1)
	genesis := "CktRuQ2mttgRGkXJtyksdKHjUdc2C4TgDzyB98oEzy8"
	core.Binding.GenesisHash = &genesis
	s := replayDetailLiveStatus(core)
	setFixture(t, body, s)
	c.poll(context.Background(), time.Now().UTC())
	g := c.detailGate.Load()
	if g == nil || g.State != "fresh" || g.Reason != "" || !g.matches(core.Binding) || g.failure(time.Now()) != "" {
		t.Fatalf("accepted status did not publish detail context: %+v", g)
	}
	if g.ObservedAt == g.ObservedAt.Round(0) {
		t.Fatal("detail status receipt lost its local monotonic clock")
	}
	before := *g
	*c.last.GenesisHash = "changed-after-publication"
	if *c.detailGate.Load() != before {
		t.Fatal("detail gate retained a mutable status pointer")
	}
	// Restore the published fixture before checking source-loss behaviour.
	setFixture(t, body, s)
	c.poll(context.Background(), time.Now().UTC())
	body.Store(nil)
	c.poll(context.Background(), time.Now().UTC())
	g = c.detailGate.Load()
	if g.State != "stale" || g.Reason != "source_unavailable" || !g.matches(core.Binding) {
		t.Fatalf("source failure lost dated context: %+v", g)
	}
	bad := []byte("{")
	body.Store(&bad)
	c.poll(context.Background(), time.Now().UTC())
	g = c.detailGate.Load()
	if g.State != "stale" || g.Reason != "source_invalid" {
		t.Fatalf("malformed source did not downgrade detail context: %+v", g)
	}
	s.RunID, s.Replay = "replacement-run", nil
	setFixture(t, body, s)
	c.poll(context.Background(), time.Now().UTC())
	g = c.detailGate.Load()
	if g.RunID != s.RunID || g.State != "unavailable" || g.Reason != "waiting_source" || g.matches(core.Binding) {
		t.Fatalf("new run inherited an old replay context: %+v", g)
	}
	disabled := newCollector(path)
	t.Cleanup(disabled.client.CloseIdleConnections)
	disabled.poll(context.Background(), time.Now().UTC())
	if disabled.detailGate.Load() != nil {
		t.Fatal("default collector enabled detail publication")
	}
}

func TestReplayDetailPublicRouteOptInAndCachedPages(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			core := replayDetailLiveCore(3)
			var captures atomic.Int64
			path := socketPath(t)
			feed, err := ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot {
				return replayDetailLiveStatus(core)
			}, func(at time.Time) (ReplayDetailCore, error) {
				captures.Add(1)
				copy := core
				copy.CapturedAt = at.UTC()
				return copy, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(feed.Close)
			client := newCollector(path)
			t.Cleanup(client.client.CloseIdleConnections)
			replayDetailWaitManifest(t, client.client, "http://node/details/replay")
			address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: enabled})
			t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
			status, body, _ := replayDetailPublicRequest(t, address, "/api/v1/features?include=advanced_status", "stats.example", "")
			var features map[string]json.RawMessage
			if status != 200 || json.Unmarshal(body, &features) != nil || string(features["replay_details"]) != map[bool]string{false: "", true: "true"}[enabled] {
				t.Fatalf("detail discovery: %d %s", status, body)
			}
			if !enabled {
				status, _, _ = replayDetailPublicRequest(t, address, "/api/v1/details/replay", "stats.example", "")
				if status != 404 {
					t.Fatalf("default detail route is enabled: %d", status)
				}
				return
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				status, body, _ = replayDetailPublicRequest(t, address, "/api/v1/details/replay", "stats.example", "")
				if status == 200 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("reader did not install detail family: %d %s", status, body)
				}
				time.Sleep(10 * time.Millisecond)
			}
			var manifest PublicReplayDetailManifest
			if json.Unmarshal(body, &manifest) != nil || manifest.Count != 3 || manifest.PageCount != 2 || manifest.ReaderMissedRows != 0 {
				t.Fatalf("public detail manifest: %s", body)
			}
			query := "/api/v1/details/replay?generation=" + manifest.Generation + "&page=1"
			for range 8 {
				status, body, headers := replayDetailPublicRequest(t, address, query, "stats.example", "https://stats.example")
				var page PublicReplayDetailPage
				if status != 200 || json.Unmarshal(body, &page) != nil || len(page.Records) != 1 || !replayDetailRecordsEqual(page.Records[0], core.Records[2]) || headers.Get("Cache-Control") != "no-store" {
					t.Fatalf("cached public page: %d %s", status, body)
				}
			}
			for _, tc := range []struct {
				path, host, origin string
				want               int
			}{
				{query, "attacker.example", "", 403},
				{query, "stats.example", "https://attacker.example", 403},
				{"/api/v1/details/replay?page=0", "stats.example", "", 400},
				{"/api/v1/details/replay?", "stats.example", "", 400},
				{"/api/v1/details/%72eplay", "stats.example", "", 400},
				{"/api/v1/details/replay/", "stats.example", "", 404},
			} {
				status, _, _ := replayDetailPublicRequest(t, address, tc.path, tc.host, tc.origin)
				if status != tc.want {
					t.Fatalf("%s: got %d, want %d", tc.path, status, tc.want)
				}
			}
			if captures.Load() != 1 {
				t.Fatal("public requests triggered source capture")
			}
		})
	}
}

func TestReplayDetailPublicSlowCollectionKeepsStatusAndJoins(t *testing.T) {
	core := replayDetailLiveCore(256)
	core.CapturedAt = time.Now().UTC()
	prepared, err := prepareReplayDetail(core, core.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	var statusPolls, activePages atomic.Int64
	entered := make(chan struct{})
	hold := make(chan struct{})
	held := make(chan struct{})
	var first, firstHeld sync.Once
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/snapshot" {
			statusPolls.Add(1)
			_ = json.NewEncoder(w).Encode(replayDetailLiveStatus(core))
			return
		}
		if r.URL.Path == "/details/replay" && r.URL.RawQuery != "" {
			activePages.Add(1)
			defer activePages.Add(-1)
			first.Do(func() { close(entered) })
			select {
			case <-hold:
				firstHeld.Do(func() { close(held) })
				<-r.Context().Done()
				return
			default:
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
		replayDetailReaderReply(w, r, prepared)
	})
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: true})
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			replayDetailStopPublic(t, cancel, done)
		}
	})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("detail collection did not start")
	}
	initial := statusPolls.Load()
	deadline := time.Now().Add(3 * time.Second)
	for statusPolls.Load() < initial+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if statusPolls.Load() < initial+2 {
		t.Fatal("detail pages blocked independent status polling")
	}
	close(hold)
	select {
	case <-held:
	case <-time.After(3 * time.Second):
		t.Fatal("detail collection did not reach a held page")
	}
	status, body, _ := replayDetailPublicRequest(t, address, "/api/v1/status", "stats.example", "")
	var snapshot PublicSnapshot
	if status != 200 || json.Unmarshal(body, &snapshot) != nil || snapshot.SourceState != "fresh" {
		t.Fatalf("status stopped advancing during detail transfer: %d %s", status, body)
	}
	if activePages.Load() == 0 {
		t.Fatal("slow detail fixture was not still transferring")
	}
	replayDetailStopPublic(t, cancel, done)
	stopped = true
	deadline = time.Now().Add(time.Second)
	for activePages.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if activePages.Load() != 0 {
		t.Fatal("shutdown left detail requests running")
	}
}

func TestReplayDetailPublicViewersDoNotFetchSource(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			core := replayDetailLiveCore(2)
			core.CapturedAt = time.Now().UTC()
			prepared, err := prepareReplayDetail(core, core.CapturedAt)
			if err != nil {
				t.Fatal(err)
			}
			var detailRequests atomic.Int64
			path := replayDetailReaderServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/snapshot" {
					_ = json.NewEncoder(w).Encode(replayDetailLiveStatus(core))
					return
				}
				if r.URL.Path == "/details/slots" || r.URL.Path == "/details/schedule" || r.URL.Path == "/details/selected" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				detailRequests.Add(1)
				replayDetailReaderReply(w, r, prepared)
			})
			address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: enabled})
			t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
			query := "/api/v1/details/replay"
			if enabled {
				deadline := time.Now().Add(3 * time.Second)
				for {
					status, body, _ := replayDetailPublicRequest(t, address, query, "stats.example", "")
					if status == 200 {
						var m PublicReplayDetailManifest
						if json.Unmarshal(body, &m) != nil {
							t.Fatal("invalid public manifest")
						}
						query += "?generation=" + m.Generation + "&page=0"
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("reader did not collect source")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			for range 8 {
				status, _, _ := replayDetailPublicRequest(t, address, query, "stats.example", "")
				if status != map[bool]int{false: 404, true: 200}[enabled] {
					t.Fatalf("detail page status: %d", status)
				}
			}
			want := int64(0)
			if enabled {
				want = 2 // One manifest and one page in the initial collection.
			}
			if got := detailRequests.Load(); got != want {
				t.Fatalf("viewer/default path reached Unix detail source: got %d requests, want %d", got, want)
			}
		})
	}
}
