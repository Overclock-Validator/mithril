package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestSelectedWindowBundleAdmission(t *testing.T) {
	now := time.Now()
	core := replayDetailLiveCore(0)
	window := selectedWindowFixture(256)
	window.Binding, window.CapturedAt, window.ObservedAt = core.Binding, now.UTC(), now.UTC()
	prepared, err := prepareSelectedWindow(window)
	if err != nil {
		t.Fatal(err)
	}
	body, err := selectedWindowBundle(prepared)
	if err != nil {
		t.Fatal(err)
	}
	publication := &replayDetailPrepared{binding: window.Binding, generation: prepared.digest,
		capturedAt: now, pages: prepared.pages, bytes: prepared.bytes + len(body), selectedBundle: body}
	for i := range publication.manifests {
		publication.manifests[i] = prepared.manifest
	}
	var gate atomic.Pointer[replayDetailGate]
	g := replayGate(replayDetailLiveStatus(core), now)
	g.revision, g.stamp = 1, statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	r := &selectedWindowReader{gate: &gate, revision: 1, limit: rate.NewLimiter(16, 32), active: make(chan struct{}, 4)}
	if err := r.cache.publish(publication, g, now); err != nil {
		t.Fatal(err)
	}
	read := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, path, nil), allowReplayStatusForTest)
		return w
	}
	response := read("/api/v1/details/selected-window")
	var got struct {
		Manifest selectedWindowManifest `json:"manifest"`
		Pages    []selectedWindowPage   `json:"pages"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &got) != nil ||
		got.Manifest.Digest != prepared.digest || len(got.Pages) != 6 || got.Pages[0].Page != 26 || got.Pages[5].Page != 31 ||
		response.Header().Get(replayDetailRemainingHeader) == "" || response.Header().Get(statusRemainingHeader) == "" {
		t.Fatalf("bundle missing recent pages or leases: %d", response.Code)
	}
	if read("/api/v1/details/selected-window?extra=1").Code != http.StatusBadRequest {
		t.Fatal("bundle accepted an unexpected query")
	}
	if read("/api/v1/details/selected-window?generation="+prepared.digest+"&page=0").Code != http.StatusBadRequest {
		t.Fatal("bundle accepted a page request")
	}
	// A complete bundle needs no later page requests, but keeps the original expiry.
	window.CapturedAt, window.ObservedAt = now.Add(-20*time.Second).UTC(), now.Add(-20*time.Second).UTC()
	older, err := prepareSelectedWindow(window)
	if err != nil {
		t.Fatal(err)
	}
	olderBundle, err := selectedWindowBundle(older)
	if err != nil {
		t.Fatal(err)
	}
	p := &replayDetailPrepared{binding: window.Binding, generation: older.digest, capturedAt: window.CapturedAt,
		pages: older.pages, bytes: older.bytes + len(olderBundle), selectedBundle: olderBundle}
	for i := range p.manifests {
		p.manifests[i] = older.manifest
	}
	r.cache = replayDetailCache{}
	oldGate := g
	oldGate.ObservedAt = window.CapturedAt
	if err := r.cache.publish(p, oldGate, window.CapturedAt); err != nil {
		t.Fatal(err)
	}
	g.ObservedAt = time.Now()
	gate.Store(&g)
	response = read("/api/v1/details/selected-window")
	remaining, _ := strconv.ParseInt(response.Header().Get(replayDetailRemainingHeader), 10, 64)
	if response.Code != http.StatusOK || remaining <= 0 || remaining > 10000 {
		t.Fatalf("complete bundle lost its original usable lease: status=%d remaining=%d", response.Code, remaining)
	}
	if read("/api/v1/details/selected").Code != http.StatusServiceUnavailable {
		t.Fatal("paginated manifest lost its minimum retrieval budget")
	}
	futureGate := g
	futureGate.ObservedAt = now.Add(11 * time.Second)
	if r.cache.lookupWithBudget("", false, futureGate, futureGate.ObservedAt, time.Millisecond).status == http.StatusOK {
		t.Fatal("bundle outlived its original source or retention deadline")
	}
	other := g
	other.revision, other.stamp = 2, statusStamp{nodeEpoch: 2, familyEpoch: 2}
	gate.Store(&other)
	if read("/api/v1/details/selected-window").Code == http.StatusOK {
		t.Fatal("old node generation remained visible")
	}
}

func TestSelectedWindowBundleRanges(t *testing.T) {
	for _, count := range []int{0, 1, 48, 49, 256} {
		window := selectedWindowFixture(count)
		prepared, err := prepareSelectedWindow(window)
		if err != nil {
			t.Fatal(err)
		}
		body, err := selectedWindowBundle(prepared)
		if err != nil || len(body) > selectedWindowBundleMaxBytes {
			t.Fatalf("count %d: bundle size=%d err=%v", count, len(body), err)
		}
		var got struct {
			Manifest selectedWindowManifest `json:"manifest"`
			Pages    []selectedWindowPage   `json:"pages"`
		}
		if err := json.Unmarshal([]byte(body), &got); err != nil {
			t.Fatal(err)
		}
		wantPages := min(len(prepared.pages), 6)
		if got.Manifest.Count != count || got.Manifest.PageCount != len(prepared.pages) ||
			got.Manifest.Digest != prepared.digest || len(got.Pages) != wantPages {
			t.Fatalf("count %d: incorrect manifest or page count", count)
		}
		for i, page := range got.Pages {
			number := len(prepared.pages) - wantPages + i
			if page.Page != number || page.Offset != number*selectedWindowPageSize ||
				page.Digest != got.Manifest.Digest || !sameReplayDetailBinding(page.Binding, got.Manifest.Binding) {
				t.Fatalf("count %d: invalid page %d", count, number)
			}
		}
	}
}
