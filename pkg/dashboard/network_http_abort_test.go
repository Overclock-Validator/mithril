package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestNetworkDetailHTTPAbortsWhenAdmissionChangesAfterBody(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	prepared, err := preparePublicNodeNetwork(capture.Detail, now, now.Add(20*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	stamp := statusStamp{1, 1}
	family := &publicNetworkDetailFamily{node: prepared, publishedAt: now, sourceFreshUntil: now.Add(20 * time.Second), stamp: stamp, nodeStamp: stamp}
	c := &collector{clockOrigin: now}
	gate := networkGate(snapshot, now)
	gate.stamp, gate.nodeStamp = stamp, stamp
	c.networkGate.Store(&gate)
	r := &networkDetailReader{gate: &c.networkGate, limit: rate.NewLimiter(100, 100)}
	r.latest.Store(family)
	frame := &frameSet{freshUntil: now.Add(10 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
	frame.fence.nodeEpoch, frame.fence.families[statusNetwork], frame.fence.families[statusNetworkNode] = 1, 1, 1
	c.latest.Store(frame)
	var calls atomic.Int32
	r.admit = func(reply networkDetailReply) (time.Duration, bool) {
		if calls.Add(1) == 2 {
			return 0, false
		}
		return c.admitNetworkDetail(r, reply, statusNetworkNode)
	}
	server := httptest.NewServer(http.HandlerFunc(r.serveHTTP))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/details/network", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(request)
	if err == nil {
		defer response.Body.Close()
		body, readErr := io.ReadAll(response.Body)
		if response.StatusCode == http.StatusOK && readErr == nil && json.Valid(body) {
			t.Fatal("client received a complete successful document after admission failed")
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("test did not reach the second admission: calls=%d request_error=%v", calls.Load(), err)
	}
}
