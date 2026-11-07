package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReplayDetailPublicBundle(t *testing.T) {
	now := time.Now()
	core := replayDetailReaderWindow(ReplayDetailCapacity, 1, now)
	maximumReplayOptionalMetadata(&core)
	r := replayDetailReaderFixture(t, core, now, "/unused")
	replayDetailReaderAccept(t, r, core, now, 30*time.Second)
	family := r.items[0]
	if len(family.pages) != 128 {
		t.Fatal("fixture does not cover all 256 rows and 128 pages")
	}
	path := "/api/v1/details/replay?generation=" + family.generation + "&bundle=1"
	response := httptest.NewRecorder()
	r.serveHTTP(response, httptest.NewRequest(http.MethodGet, path, nil), allowReplayStatusForTest)
	var body struct {
		Manifest json.RawMessage   `json:"manifest"`
		Pages    []json.RawMessage `json:"pages"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil ||
		string(body.Manifest) != family.manifests[0] || len(body.Pages) != 128 {
		t.Fatalf("bundle status=%d did not preserve the cached complete family", response.Code)
	}
	for i, page := range body.Pages {
		if string(page) != family.pages[i] {
			t.Fatalf("bundle changed cached page %d", i)
		}
	}
	canonical, err := json.Marshal(body)
	if err != nil || !bytes.Equal(canonical, response.Body.Bytes()) || response.Body.Len() > replayDetailFamilyBytes ||
		response.Header().Get("Content-Length") != strconv.Itoa(response.Body.Len()) {
		t.Fatal("bundle is noncanonical or exceeds its exact bounded content length")
	}
	remaining, err := strconv.ParseInt(response.Header().Get(replayDetailRemainingHeader), 10, 64)
	if err != nil || remaining < 13000 || remaining > family.sourceFreshUntil.Sub(now).Milliseconds() {
		t.Fatal("bundle extended or omitted the original fresh lease")
	}
	// The old route bodies remain byte-identical, not bundle-shaped.
	for _, old := range []struct{ query, want string }{
		{"", family.manifests[0]},
		{"?generation=" + family.generation + "&page=127", family.pages[127]},
	} {
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/replay"+old.query, nil), allowReplayStatusForTest)
		if response.Code != http.StatusOK || response.Body.String() != old.want {
			t.Fatal("existing manifest/page route changed")
		}
	}
	// Streaming writes retain the individual cached strings, not one aggregate body.
	sink := &replayBundleSink{header: make(http.Header)}
	r.serveHTTP(sink, httptest.NewRequest(http.MethodGet, path, nil), allowReplayStatusForTest)
	if sink.status != http.StatusOK || sink.bytes != response.Body.Len() || sink.maxWrite > MaxSnapshotBytes || sink.stringWrites != 3+2*len(family.pages) {
		t.Fatalf("bundle did not stream bounded cached parts: %+v", sink)
	}
	t.Logf("complete bundle=%d bytes, %d cached pages", response.Body.Len(), len(family.pages))
}

type replayBundleSink struct {
	header       http.Header
	status       int
	bytes        int
	maxWrite     int
	stringWrites int
	onHeader     func()
	once         sync.Once
	onFirstWrite func()
}

func (s *replayBundleSink) Header() http.Header {
	if s.onHeader != nil {
		s.once.Do(s.onHeader)
	}
	return s.header
}

func (s *replayBundleSink) WriteHeader(status int) { s.status = status }
func (s *replayBundleSink) Write([]byte) (int, error) {
	return 0, errors.New("bundle must use the cached string writer")
}
func (s *replayBundleSink) WriteString(body string) (int, error) {
	if s.onFirstWrite != nil {
		s.once.Do(s.onFirstWrite)
	}
	s.bytes += len(body)
	s.maxWrite = max(s.maxWrite, len(body))
	s.stringWrites++
	return len(body), nil
}

func TestReplayDetailPublicBundleRefusals(t *testing.T) {
	for _, name := range []string{"invalid", "missing", "expired", "reset", "reset at headers", "stale", "oversize", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			core := replayDetailReaderWindow(2, 1, now)
			r := replayDetailReaderFixture(t, core, now, "/unused")
			replayDetailReaderAccept(t, r, core, now, 30*time.Second)
			family := r.items[0]
			query := "generation=" + family.generation + "&bundle=1"
			want := http.StatusOK
			switch name {
			case "invalid":
				for _, bad := range []string{"bundle=1", query + "&page=0", query + "&bundle=1", strings.Replace(query, "bundle=1", "bundle=01", 1), "generation=" + strings.Repeat("A", 64) + "&bundle=1"} {
					response := httptest.NewRecorder()
					r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/replay?"+bad, nil), allowReplayStatusForTest)
					if response.Code != http.StatusBadRequest {
						t.Fatalf("invalid closed query accepted: %s", bad)
					}
				}
				return
			case "missing":
				query, want = "generation="+strings.Repeat("f", 64)+"&bundle=1", http.StatusConflict
			case "expired":
				family.holdUntil, want = now.Add(-time.Second), http.StatusConflict
			case "reset":
				gate := *r.gate.Load()
				gate.Attempt++
				r.gate.Store(&gate)
				want = http.StatusConflict
			case "stale":
				gate := *r.gate.Load()
				gate.State, gate.Reason = "stale", "source_invalid"
				r.gate.Store(&gate)
				want = http.StatusServiceUnavailable
			case "oversize":
				family.pages = make([]string, 128)
				for i := range family.pages {
					family.pages[i] = strings.Repeat(" ", MaxSnapshotBytes)
				}
				want = http.StatusServiceUnavailable
			case "cancelled":
				request := httptest.NewRequest(http.MethodGet, "/api/v1/details/replay?"+query, nil)
				ctx, cancel := context.WithCancel(request.Context())
				cancel()
				sink := &replayBundleSink{header: make(http.Header)}
				r.serveHTTP(sink, request.WithContext(ctx), allowReplayStatusForTest)
				if sink.bytes != 0 || len(r.active) != 0 {
					t.Fatal("cancelled bundle wrote data or leaked admission")
				}
				return
			}
			if name == "reset at headers" {
				sink := &replayBundleSink{header: make(http.Header), onHeader: func() {
					gate := *r.gate.Load()
					gate.Attempt++
					r.gate.Store(&gate)
				}}
				r.serveHTTP(sink, httptest.NewRequest(http.MethodGet, "/api/v1/details/replay?"+query, nil), allowReplayStatusForTest)
				if sink.status != http.StatusConflict || sink.header.Get(replayDetailRemainingHeader) != "" || sink.header.Get("Content-Length") != "" {
					t.Fatal("header-time reset reused the first admission")
				}
				return
			}
			response := httptest.NewRecorder()
			r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/replay?"+query, nil), allowReplayStatusForTest)
			if response.Code != want || response.Header().Get(replayDetailRemainingHeader) != "" {
				t.Fatalf("%s status=%d or fake fresh lease", name, response.Code)
			}
			if name == "stale" && !strings.Contains(response.Body.String(), `"reason":"status_stale"`) {
				t.Fatal("stale parent status did not refuse the bundle")
			}
		})
	}
}

func TestReplayDetailPublicBundleAdmission(t *testing.T) {
	now := time.Now()
	core := replayDetailReaderWindow(1, 1, now)
	r := replayDetailReaderFixture(t, core, now, "/unused")
	replayDetailReaderAccept(t, r, core, now, 30*time.Second)
	path := "/api/v1/details/replay?generation=" + r.cursor.generation + "&bundle=1"
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var viewers sync.WaitGroup
	for range 4 {
		viewers.Go(func() {
			sink := &replayBundleSink{header: make(http.Header), onFirstWrite: func() { entered <- struct{}{}; <-release }}
			r.serveHTTP(sink, httptest.NewRequest(http.MethodGet, path, nil), allowReplayStatusForTest)
		})
	}
	for range 4 {
		<-entered
	}
	response := httptest.NewRecorder()
	r.serveHTTP(response, httptest.NewRequest(http.MethodGet, path, nil), allowReplayStatusForTest)
	close(release)
	viewers.Wait()
	if response.Code != http.StatusTooManyRequests || len(r.active) != 0 || response.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatal("bundle exceeded the existing four-viewer admission or leaked a permit")
	}
}
