package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestLocalPeerStatusGateRecoveryAndABA(t *testing.T) {
	detail := localPeerTestDetail(1)
	prepared, err := PrepareLocalPeers(detail)
	if err != nil {
		t.Fatal(err)
	}
	status := localPeerLifecycleStatus(detail)
	var gate atomic.Pointer[localPeerGate]
	setGate := func(snapshot Snapshot, epoch uint64) {
		g := advanceLocalPeerStatusGate(gate.Load(), &snapshot, "", time.Now(), epoch)
		gate.Store(&g)
	}
	setGate(status, 1)
	var requests atomic.Int64
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set(replayDetailRemainingHeader, "30000")
		_, _ = w.Write([]byte(prepared.Body()))
	})
	r, err := newLocalPeerReader(newCollector(path).client, &gate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.client.CloseIdleConnections)
	r.limit = rate.NewLimiter(100, 100)
	r.admit = func(localPeerReply) (time.Duration, bool) { return time.Second, true }
	r.collect(context.Background())
	first := r.latest.Load()
	if first == nil {
		t.Fatal("initial peer capture unavailable")
	}
	request := func() *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, localPeerPublicPath, nil))
		return w
	}
	if request().Code != http.StatusOK {
		t.Fatal("initial peer capture not served")
	}
	setGate(status, 2)
	if request().Code != http.StatusServiceUnavailable || r.latest.Load() != first {
		t.Fatal("same-binding recovery exposed an old-epoch peer capture")
	}
	r.collect(context.Background())
	recovered := r.latest.Load()
	if recovered == first || recovered.nodeEpoch != 2 || recovered.freshUntil.After(first.freshUntil) ||
		request().Code != http.StatusOK {
		t.Fatal("validated same capture did not recover under its original deadline")
	}
	other := status
	other.RunID = "replacement-run"
	setGate(other, 3)
	setGate(status, 4)
	if request().Code != http.StatusServiceUnavailable || requests.Load() != 2 {
		t.Fatal("A to B to A exposed an old peer capture without a fetch")
	}
	r.collect(context.Background())
	if current := r.latest.Load(); current.nodeEpoch != 4 || current.freshUntil.After(first.freshUntil) ||
		requests.Load() != 3 || request().Code != http.StatusOK {
		t.Fatal("A to B to A did not recover under the original lease")
	}
}

func TestLocalPeerFinalAdmission(t *testing.T) {
	setup := func(t *testing.T) (*collector, *localPeerReader) {
		t.Helper()
		now := time.Now()
		detail := localPeerTestDetail(1)
		prepared, err := PrepareLocalPeers(detail)
		if err != nil {
			t.Fatal(err)
		}
		c := &collector{clockOrigin: now}
		snapshot := localPeerLifecycleStatus(detail)
		gate := advanceLocalPeerStatusGate(nil, &snapshot, "", now, 1)
		c.localPeerGate.Store(&gate)
		r := &localPeerReader{gate: &c.localPeerGate, limit: rate.NewLimiter(100, 100), active: make(chan struct{}, 2)}
		r.latest.Store(&localPeerPublication{body: prepared.Body(), binding: gate.binding,
			nodeEpoch: gate.nodeEpoch, revision: gate.revision, freshUntil: now.Add(30 * time.Second)})
		frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
		frame.fence.nodeEpoch, frame.fence.families[statusPeers] = gate.nodeEpoch, gate.revision
		c.latest.Store(frame)
		r.admit = func(reply localPeerReply) (time.Duration, bool) { return c.admitLocalPeers(r, reply) }
		return c, r
	}
	request := func(r *localPeerReader) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, localPeerPublicPath, nil))
		return w
	}
	t.Run("independent leases and nil admission", func(t *testing.T) {
		_, r := setup(t)
		w := request(r)
		detail, detailErr := parseDetailRemaining(w.Header().Get(replayDetailRemainingHeader), NetworkDetailFreshFor)
		statusMS, statusErr := strconv.ParseInt(w.Header().Get(statusRemainingHeader), 10, 64)
		if w.Code != http.StatusOK || detailErr != nil || statusErr != nil || statusMS <= 0 ||
			detail <= time.Duration(statusMS)*time.Millisecond {
			t.Fatal("peer and parent status leases were not independent")
		}
		r.admit = nil
		if request(r).Code != http.StatusServiceUnavailable {
			t.Fatal("missing admission exposed peer data")
		}
	})
	t.Run("failure after selection", func(t *testing.T) {
		c, r := setup(t)
		r.admit = func(reply localPeerReply) (time.Duration, bool) {
			g := r.gate.Load()
			r.failure.Store(&localPeerReaderFailure{binding: g.binding, nodeEpoch: g.nodeEpoch, revision: g.revision, reason: "source_unavailable"})
			return c.admitLocalPeers(r, reply)
		}
		if w := request(r); w.Code != http.StatusServiceUnavailable || w.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatal("late source failure exposed the selected peer data")
		}
	})
	t.Run("capture expires after selection", func(t *testing.T) {
		c, r := setup(t)
		r.admit = func(reply localPeerReply) (time.Duration, bool) {
			// Controlled test-only expiry after selection, without a sleep.
			reply.publication.freshUntil = time.Now().Add(-time.Second)
			return c.admitLocalPeers(r, reply)
		}
		if w := request(r); w.Code != http.StatusServiceUnavailable || w.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatal("expired peer capture retained a freshness header")
		}
	})
	t.Run("gate changes after selection", func(t *testing.T) {
		c, r := setup(t)
		r.admit = func(reply localPeerReply) (time.Duration, bool) {
			gate := *r.gate.Load()
			gate.nodeEpoch, gate.revision = 2, gate.revision+1
			r.gate.Store(&gate)
			return c.admitLocalPeers(r, reply)
		}
		if request(r).Code != http.StatusServiceUnavailable {
			t.Fatal("changed gate admitted old peer data")
		}
	})
	t.Run("parent status expired", func(t *testing.T) {
		c, r := setup(t)
		frame := *c.latest.Load()
		frame.freshUntil = time.Now().Add(-time.Second)
		c.latest.Store(&frame)
		if w := request(r); w.Code != http.StatusServiceUnavailable || w.Header().Get(statusRemainingHeader) != "" {
			t.Fatal("expired parent status admitted peer data")
		}
	})
}
