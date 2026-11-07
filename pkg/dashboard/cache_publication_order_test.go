package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestCacheDetailPollPublishesGateBeforeStatus(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public, c.publicOptions = true, PublicOptions{Resources: true, CacheDetails: true}
	t.Cleanup(c.client.CloseIdleConnections)

	snapshot, capture := cacheDetailFixture(time.Now())
	identityA, identityB := "identity-a", "identity-b"
	snapshot.IdentityKey = &identityA
	setFixture(t, body, snapshot)
	c.poll(context.Background(), time.Now().UTC())
	frameA, gateA := c.latest.Load(), c.cacheGate.Load()
	if frameA == nil || gateA == nil || !gateA.stamp.valid() || frameA.statusStamp(statusCache) != gateA.stamp {
		t.Fatal("initial status and cache gate were not published together")
	}
	now := time.Now()
	family, err := preparePublicCacheDetail(capture.Detail, now, now.Add(CacheDetailFreshFor))
	if err != nil {
		t.Fatal(err)
	}
	family.stamp = gateA.stamp
	r := &cacheDetailReader{gate: &c.cacheGate, limit: rate.NewLimiter(100, 100)}
	r.latest.Store(family)
	request := func() *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/cache", nil),
			func(reply cacheDetailReply) (time.Duration, bool) { return c.admitCacheDetail(r, reply) })
		return response
	}
	if response := request(); response.Code != http.StatusOK {
		t.Fatalf("initial cache response: got %d, want 200", response.Code)
	}

	snapshot.IdentityKey = &identityB
	snapshot.SampledAt = time.Now().UTC()
	if !snapshot.SampledAt.After(capture.Detail.CapturedAt) {
		snapshot.SampledAt = capture.Detail.CapturedAt.Add(time.Nanosecond)
	}
	setFixture(t, body, snapshot)
	// The display lock pauses the real poll after the new cache gate is stored
	// and before the corresponding status frame is published.
	c.display = &nodeDisplayOwner{}
	c.display.mu.Lock()
	locked := true
	done := make(chan struct{})
	defer func() {
		if locked {
			c.display.mu.Unlock()
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("paused status poll did not exit")
		}
	}()
	go func() {
		defer close(done)
		c.poll(context.Background(), time.Now().UTC())
	}()

	var gateB *cacheDetailGate
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
waitForGate:
	for {
		select {
		case <-done:
			t.Fatal("status poll exited before the paused publication")
		case <-deadline.C:
			t.Fatal("new cache gate was not published before the status frame")
		case <-ticker.C:
			if gate := c.cacheGate.Load(); gate != nil && gate.stamp != gateA.stamp {
				gateB = gate
				break waitForGate
			}
		}
	}
	if !gateB.stamp.valid() || c.latest.Load() != frameA {
		t.Fatal("test did not capture the gate-before-frame interval")
	}
	// A validated re-fetch may publish the same bytes under the new gate. It
	// still must not be served until the matching status frame is visible.
	familyB := *family
	familyB.stamp = gateB.stamp
	r.latest.Store(&familyB)
	if reply := r.lookup(time.Now()); reply.status != http.StatusOK {
		t.Fatalf("new-gate cache selection: got %d, want 200", reply.status)
	}
	if response := request(); response.Code != http.StatusServiceUnavailable || response.Header().Get(statusRemainingHeader) != "" {
		t.Fatalf("cache response crossed unmatched status publication: status=%d", response.Code)
	}

	c.display.mu.Unlock()
	locked = false
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("status poll did not complete after release")
	}
	if frameB := c.latest.Load(); frameB == nil || frameB == frameA || frameB.statusStamp(statusCache) != gateB.stamp {
		t.Fatal("new status frame did not match the cache gate")
	}
	if response := request(); response.Code != http.StatusOK || response.Header().Get(statusRemainingHeader) == "" {
		t.Fatalf("matching cache response unavailable after publication: status=%d", response.Code)
	}
}
