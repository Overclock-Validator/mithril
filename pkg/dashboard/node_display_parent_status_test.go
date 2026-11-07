package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard/cluster"
)

func TestNodeDisplayAdmissionRequiresExactFreshParent(t *testing.T) {
	raw := time.Now()
	wall := raw.UTC()
	snapshot, view := nodeDisplayFixture(t, wall)
	c := &collector{clockOrigin: raw}
	parent := &frameSet{freshUntil: raw.Add(2 * time.Second), clockOrigin: raw, clockUncertain: &c.clockUncertain}
	parent.lease = newStatusFrameLease(parent.freshUntil)
	c.latest.Store(parent)
	owner := &nodeDisplayOwner{}
	revision := owner.observeStatus(&snapshot, "fresh", wall, raw)
	frame, err := owner.prepare(&snapshot, view, wall, raw, revision)
	if err != nil {
		t.Fatal(err)
	}
	frame.parent = parent.lease
	if !owner.publish(frame, raw) {
		t.Fatal("display fixture did not publish")
	}
	owner.admit = func(frame nodeDisplayFrame) (time.Duration, bool) { return c.admitNodeDisplay(owner, frame) }
	handler := owner.handler(func(*http.Request) bool { return true })
	request := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		handler(w, httptest.NewRequest(http.MethodGet, nodeDisplayPublicPath, nil))
		return w
	}
	w := request()
	remaining, err := strconv.ParseInt(w.Header().Get(replayDetailRemainingHeader), 10, 64)
	if w.Code != http.StatusOK || w.Body.String() != frame.body || err != nil || remaining <= 0 || remaining > 2000 {
		t.Fatal("display response was not capped by the shorter parent lease")
	}
	// A frame with identical bytes and deadline is still a different parent.
	replacement := *parent
	replacement.lease = newStatusFrameLease(parent.freshUntil)
	c.latest.Store(&replacement)
	if w := request(); w.Body.String() != nodeDisplayUnavailableBody || w.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatal("display borrowed a replacement status frame's lease")
	}
	c.latest.Store(parent)
	// Controlled fixture expiry, with no background goroutine or sleep.
	parent.freshUntil = time.Now().Add(-time.Second)
	if w := request(); w.Body.String() != nodeDisplayUnavailableBody || w.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatal("display outlived its parent status lease")
	}
}

func TestNodeDisplayHTTPExpiresWithBlockedParent(t *testing.T) {
	wall := time.Now().UTC()
	fixture, _ := nodeDisplayFixture(t, wall)
	snapshot := testSnapshot(wall)
	snapshot.IdentityKey, snapshot.GenesisHash = fixture.IdentityKey, fixture.GenesisHash
	snapshot.Replay = &Replay{SampledAt: wall, Completed: fixture.Replay.Completed}
	var block atomic.Bool
	blocked := make(chan struct{})
	var once sync.Once
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, r *http.Request) {
		if block.Load() {
			once.Do(func() { close(blocked) })
			<-r.Context().Done()
			return
		}
		s := snapshot
		s.SampledAt = time.Now().UTC()
		w.Header().Set(statusRemainingHeader, "400")
		_ = json.NewEncoder(w).Encode(s)
	})
	cache := filepath.Join(t.TempDir(), "cluster.json")
	if err := cluster.WriteFile(cache, clusterTestSnapshot(wall, 3)); err != nil {
		t.Fatal(err)
	}
	request := nodeDisplayHTTPServer(t, path, PublicOptions{ClusterFile: cache})
	until := time.Now().Add(4 * time.Second)
	for {
		code, header, body := request(http.MethodGet, nodeDisplayPublicPath, "stats.example", "", "")
		value := nodeDisplayTestValue(t, body)
		if code == http.StatusOK && value.State == "fresh" {
			remaining, err := strconv.ParseInt(header.Get(replayDetailRemainingHeader), 10, 64)
			if err != nil || remaining <= 0 || remaining > 400 {
				t.Fatal("actual display lease exceeded its parent")
			}
			break
		}
		if time.Now().After(until) {
			t.Fatal("actual server did not publish the display fixture")
		}
		time.Sleep(25 * time.Millisecond)
	}
	block.Store(true)
	select {
	case <-blocked:
	case <-time.After(4 * time.Second):
		t.Fatal("next parent status poll did not block")
	}
	// Even if the blocked poll started before the previous lease expired,
	// 500ms exceeds the source's 400ms lease.
	time.Sleep(500 * time.Millisecond)
	code, header, body := request(http.MethodGet, nodeDisplayPublicPath, "stats.example", "", "")
	if code != http.StatusOK || body != nodeDisplayUnavailableBody || header.Get(replayDetailRemainingHeader) != "" {
		t.Fatal("actual display remained fresh while the parent poll was blocked")
	}
}
