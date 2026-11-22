package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestLocalPeerPollPublishesGateBeforeStatus(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public, c.publicOptions = true, PublicOptions{LocalPeers: true}
	t.Cleanup(c.client.CloseIdleConnections)
	now := time.Now()
	snapshot := testSnapshot(now.UTC())
	identityA, identityB := "identity-a", "identity-b"
	snapshot.IdentityKey = &identityA
	detail := localPeerTestDetail(1)
	snapshot.GenesisHash = detail.Binding.GenesisHash
	detail.Binding = NodeNetworkBinding{RunID: snapshot.RunID, NodeStartedAt: snapshot.StartedAt, GenesisHash: snapshot.GenesisHash}
	detail.ClientStartedAt, detail.RunStartedAt = snapshot.StartedAt, snapshot.StartedAt
	detail.CapturedAt, detail.SampledAt, detail.Rows[0].ObservedAt = now.UTC(), now.UTC(), now.UTC()
	prepared, err := PrepareLocalPeers(detail)
	if err != nil {
		t.Fatal(err)
	}
	setFixture(t, body, snapshot)
	c.poll(context.Background(), time.Now().UTC())
	frameA, gateA := c.latest.Load(), c.localPeerGate.Load()
	if frameA == nil || gateA == nil || frameA.statusStamp(statusPeers) != (statusStamp{gateA.nodeEpoch, gateA.revision}) {
		t.Fatal("initial status and peer gate were not published together")
	}
	r := &localPeerReader{gate: &c.localPeerGate, limit: rate.NewLimiter(100, 100), active: make(chan struct{}, 2)}
	publication := &localPeerPublication{body: prepared.Body(), binding: gateA.binding, nodeEpoch: gateA.nodeEpoch,
		revision: gateA.revision, freshUntil: time.Now().Add(30 * time.Second)}
	r.latest.Store(publication)
	r.admit = func(reply localPeerReply) (time.Duration, bool) { return c.admitLocalPeers(r, reply) }
	request := func() *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(http.MethodGet, localPeerPublicPath, nil))
		return response
	}
	if response := request(); response.Code != http.StatusOK {
		t.Fatalf("initial peer response: got %d, want 200", response.Code)
	}
	snapshot.IdentityKey, snapshot.SampledAt = &identityB, time.Now().UTC()
	if !snapshot.SampledAt.After(now.UTC()) {
		snapshot.SampledAt = now.UTC().Add(time.Nanosecond)
	}
	setFixture(t, body, snapshot)
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
	go func() { defer close(done); c.poll(context.Background(), time.Now().UTC()) }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	var gateB *localPeerGate
waitForGate:
	for {
		select {
		case <-done:
			t.Fatal("poll exited before the paused publication")
		case <-deadline.C:
			t.Fatal("new peer gate was not published")
		case <-ticker.C:
			if gate := c.localPeerGate.Load(); gate != nil && (gate.nodeEpoch != gateA.nodeEpoch || gate.revision != gateA.revision) {
				gateB = gate
				break waitForGate
			}
		}
	}
	if c.latest.Load() != frameA {
		t.Fatal("test did not reach the gate-before-frame interval")
	}
	publicationB := *publication
	publicationB.nodeEpoch, publicationB.revision = gateB.nodeEpoch, gateB.revision
	r.latest.Store(&publicationB)
	if response := request(); response.Code != http.StatusServiceUnavailable || response.Header().Get(statusRemainingHeader) != "" {
		t.Fatalf("peer response crossed unmatched status publication: status=%d", response.Code)
	}
	c.display.mu.Unlock()
	locked = false
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("status poll did not complete")
	}
	if frameB := c.latest.Load(); frameB == nil || frameB == frameA ||
		frameB.statusStamp(statusPeers) != (statusStamp{gateB.nodeEpoch, gateB.revision}) {
		t.Fatal("new status frame did not match the peer gate")
	}
	if response := request(); response.Code != http.StatusOK || response.Header().Get(statusRemainingHeader) == "" {
		t.Fatalf("matching peer response unavailable after publication: status=%d", response.Code)
	}
}
