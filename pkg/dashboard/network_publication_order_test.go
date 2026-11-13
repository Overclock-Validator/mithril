package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestNetworkDetailPollPublishesGateBeforeStatus(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public, c.publicOptions = true, PublicOptions{Resources: true, NetworkDetails: true}
	t.Cleanup(c.client.CloseIdleConnections)
	snapshot, capture := networkDetailFixture(time.Now())
	identityA, identityB := "identity-a", "identity-b"
	snapshot.IdentityKey = &identityA
	setFixture(t, body, snapshot)
	c.poll(context.Background(), time.Now().UTC())
	frameA, gateA := c.latest.Load(), c.networkGate.Load()
	if frameA == nil || gateA == nil || !gateA.stamp.valid() || frameA.statusStamp(statusNetwork) != gateA.stamp {
		t.Fatal("initial status and receiver gate were not published together")
	}
	now := time.Now()
	family, err := preparePublicNetworkDetail(capture.Detail, now, now.Add(NetworkDetailFreshFor))
	if err != nil {
		t.Fatal(err)
	}
	family.stamp, family.nodeStamp = gateA.stamp, gateA.nodeStamp
	r := &networkDetailReader{gate: &c.networkGate, limit: rate.NewLimiter(100, 100)}
	r.latest.Store(family)
	r.admit = func(reply networkDetailReply) (time.Duration, bool) {
		return c.admitNetworkDetail(r, reply, statusNetworkNode)
	}
	request := func() *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/network", nil))
		return response
	}
	if response := request(); response.Code != http.StatusOK {
		t.Fatalf("initial network response: got %d, want 200", response.Code)
	}
	snapshot.IdentityKey, snapshot.SampledAt = &identityB, time.Now().UTC()
	if !snapshot.SampledAt.After(capture.Detail.UpdatedAt) {
		snapshot.SampledAt = capture.Detail.UpdatedAt.Add(time.Nanosecond)
	}
	setFixture(t, body, snapshot)
	// The display lock pauses the real poll after the new network gate and
	// before the matching status frame.
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
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	var gateB *networkDetailGate
waitForGate:
	for {
		select {
		case <-done:
			t.Fatal("poll exited before the paused publication")
		case <-deadline.C:
			t.Fatal("new network gate was not published")
		case <-ticker.C:
			if gate := c.networkGate.Load(); gate != nil && gate.stamp != gateA.stamp {
				gateB = gate
				break waitForGate
			}
		}
	}
	if !gateB.stamp.valid() || c.latest.Load() != frameA {
		t.Fatal("test did not reach the gate-before-frame interval")
	}
	familyB := *family
	familyB.stamp, familyB.nodeStamp = gateB.stamp, gateB.nodeStamp
	r.latest.Store(&familyB)
	if reply := r.lookup(time.Now()); reply.status != http.StatusOK {
		t.Fatalf("new-gate selection: got %d, want 200", reply.status)
	}
	if response := request(); response.Code != http.StatusServiceUnavailable || response.Header().Get(statusRemainingHeader) != "" {
		t.Fatalf("network crossed unmatched status publication: status=%d", response.Code)
	}
	c.display.mu.Unlock()
	locked = false
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("status poll did not complete")
	}
	if frameB := c.latest.Load(); frameB == nil || frameB == frameA || frameB.statusStamp(statusNetwork) != gateB.stamp {
		t.Fatal("new status frame did not match the network gate")
	}
	if response := request(); response.Code != http.StatusOK || response.Header().Get(statusRemainingHeader) == "" {
		t.Fatalf("matching network response unavailable after publication: status=%d", response.Code)
	}
}
