package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestNetworkCollectorPublishesMatchingStamp(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public, c.publicOptions = true, PublicOptions{Resources: true, NetworkDetails: true}
	t.Cleanup(c.client.CloseIdleConnections)
	snapshot, _ := networkDetailFixture(time.Now())
	firstIdentity, secondIdentity := "identity-a", "identity-b"
	snapshot.IdentityKey = &firstIdentity
	setFixture(t, body, snapshot)
	c.poll(context.Background(), time.Now().UTC())
	firstFrame, firstGate := c.latest.Load(), c.networkGate.Load()
	if firstFrame == nil || firstGate == nil || !firstGate.stamp.valid() || firstFrame.statusStamp(statusNetwork) != firstGate.stamp {
		t.Fatal("initial status and network gate do not match")
	}
	snapshot.IdentityKey, snapshot.SampledAt = &secondIdentity, time.Now().Add(time.Millisecond).UTC()
	setFixture(t, body, snapshot)
	c.poll(context.Background(), time.Now().UTC())
	secondFrame, secondGate := c.latest.Load(), c.networkGate.Load()
	if secondFrame == nil || secondGate == nil || secondGate.stamp == firstGate.stamp || secondFrame.statusStamp(statusNetwork) != secondGate.stamp {
		t.Fatal("replacement status and network gate do not match")
	}
	snapshot.SampledAt = time.Now().Add(time.Hour).UTC()
	setFixture(t, body, snapshot)
	c.poll(context.Background(), time.Now().UTC())
	staleFrame, staleGate := c.latest.Load(), c.networkGate.Load()
	if staleFrame == nil || staleGate == nil || staleGate.reason != "source_invalid" || staleGate.stamp == secondGate.stamp ||
		staleFrame.statusStamp(statusNetwork) != staleGate.stamp {
		t.Fatal("invalid replacement did not fence the previous network capture")
	}
}

func TestNetworkDetailStampRecovery(t *testing.T) {
	now := time.Now()
	snapshot, capture := networkDetailFixture(now)
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int64
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set(replayDetailRemainingHeader, "30000")
		_, _ = w.Write(body)
	})
	r := networkDetailTestReader(t, path, snapshot, now)
	r.collect(context.Background())
	original := r.latest.Load()
	if original == nil || r.lookup(time.Now()).status != http.StatusOK {
		t.Fatal("initial receiver capture unavailable")
	}
	rebind := func(node, receiver uint64, run string) {
		gate := cloneNetworkDetailGate(*r.gate.Load())
		gate.nodeStamp, gate.stamp = statusStamp{node, node}, statusStamp{node, receiver}
		gate.binding.RunID, gate.observedAt = run, time.Now()
		r.gate.Store(&gate)
	}
	rebind(2, 2, snapshot.RunID)
	if r.lookup(time.Now()).status != http.StatusServiceUnavailable || r.latest.Load() != original {
		t.Fatal("same-binding recovery exposed a capture before revalidation")
	}
	r.collect(context.Background())
	recovered := r.latest.Load()
	if recovered == original || recovered.stamp != (statusStamp{2, 2}) ||
		recovered.sourceFreshUntil.After(original.sourceFreshUntil) || !recovered.publishedAt.Equal(original.publishedAt) ||
		recovered.bodies != original.bodies || r.lookup(time.Now()).status != http.StatusOK {
		t.Fatal("validated identical capture lost its original deadline or facts")
	}
	rebind(3, 3, "replacement-run")
	if r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("replacement run exposed earlier receiver data")
	}
	rebind(4, 4, snapshot.RunID)
	if r.lookup(time.Now()).status != http.StatusServiceUnavailable || requests.Load() != 2 {
		t.Fatal("A to B to A exposed an old capture without a fetch")
	}
	r.collect(context.Background())
	returned := r.latest.Load()
	if requests.Load() != 3 || returned.stamp != (statusStamp{4, 4}) ||
		returned.sourceFreshUntil.After(original.sourceFreshUntil) || !returned.publishedAt.Equal(original.publishedAt) ||
		r.lookup(time.Now()).status != http.StatusOK {
		t.Fatal("validated identical A capture did not recover under its original lease")
	}
	expired := *returned
	expired.sourceFreshUntil = time.Now().Add(-time.Second)
	r.latest.Store(&expired)
	rebind(5, 5, snapshot.RunID)
	r.collect(context.Background())
	if r.latest.Load() != &expired || r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("an expired capture was renewed by a later fetch")
	}
}

func TestNodeNetworkSeparateStatusStamps(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int64
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set(replayDetailRemainingHeader, "30000")
		_, _ = w.Write(body)
	})
	r := networkDetailTestReader(t, path, snapshot, now)
	r.collect(context.Background())
	original := r.latest.Load()
	initial := r.lookup(time.Now())
	if original == nil || initial.status != http.StatusOK || initial.nodeScoped || initial.stamp != (statusStamp{1, 1}) {
		t.Fatal("initial receiver capture unavailable")
	}
	// Losing a receiver invalidates only its stamp, not independent node data.
	gate := cloneNetworkDetailGate(*r.gate.Load())
	gate.reason, gate.stamp, gate.observedAt = "source_unavailable", statusStamp{1, 2}, time.Now()
	r.gate.Store(&gate)
	independent := r.lookup(time.Now())
	var view PublicNodeNetworkDetail
	if independent.status != http.StatusOK || !independent.nodeScoped || independent.stamp != gate.nodeStamp ||
		json.Unmarshal([]byte(independent.body+independent.tail), &view) != nil || view.Receiver != nil || view.Interfaces == nil ||
		independent.remainingMilliseconds <= 0 {
		t.Fatal("receiver loss hid or reauthorized independent node facts")
	}
	r.collect(context.Background())
	if current := r.latest.Load(); current == original || current.sourceFreshUntil.After(original.sourceFreshUntil) ||
		!current.publishedAt.Equal(original.publishedAt) || current.node.receiver != nil {
		t.Fatal("receiver-loss refetch changed original node timing or retained receiver")
	}
	gate.reason, gate.stamp, gate.observedAt = "", statusStamp{1, 3}, time.Now()
	r.gate.Store(&gate)
	if reply := r.lookup(time.Now()); reply.status != http.StatusOK || !reply.nodeScoped {
		t.Fatal("receiver returned without revalidation or independent node data disappeared")
	}
	r.collect(context.Background())
	full := r.lookup(time.Now())
	if full.status != http.StatusOK || full.nodeScoped || full.stamp != gate.stamp ||
		r.latest.Load().sourceFreshUntil.After(original.sourceFreshUntil) || requests.Load() != 3 {
		t.Fatal("validated receiver did not return under its original capture deadline")
	}
	// A status observation gap changes the node stamp and fences every child.
	gate.nodeStamp, gate.stamp, gate.observedAt = statusStamp{2, 2}, statusStamp{2, 4}, time.Now()
	r.gate.Store(&gate)
	if r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("node recovery exposed an old-epoch family")
	}
	r.collect(context.Background())
	if reply := r.lookup(time.Now()); reply.status != http.StatusOK || reply.nodeScoped ||
		r.latest.Load().sourceFreshUntil.After(original.sourceFreshUntil) || requests.Load() != 4 {
		t.Fatal("node recovery did not require a validated fetch with the original deadline")
	}
}

func TestNodeNetworkInFlightNodeABAFailsClosed(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.Header().Set(replayDetailRemainingHeader, "30000")
		_, _ = w.Write(body)
	})
	r := networkDetailTestReader(t, path, snapshot, now)
	done := make(chan struct{})
	go func() { r.collect(context.Background()); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("network fetch did not start")
	}
	gate := cloneNetworkDetailGate(*r.gate.Load())
	gate.binding.RunID, gate.nodeStamp, gate.stamp = "replacement-run", statusStamp{2, 2}, statusStamp{2, 2}
	r.gate.Store(&gate)
	gate = cloneNetworkDetailGate(gate)
	gate.binding.RunID, gate.nodeStamp, gate.stamp = snapshot.RunID, statusStamp{3, 3}, statusStamp{3, 3}
	r.gate.Store(&gate)
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("network collector did not finish")
	}
	if r.latest.Load() != nil || r.lookup(time.Now()).status != http.StatusServiceUnavailable {
		t.Fatal("an in-flight node capture survived an A to B to A gate change")
	}
}

func TestNodeNetworkInFlightReceiverChangeKeepsNodeFacts(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.Header().Set(replayDetailRemainingHeader, "30000")
		_, _ = w.Write(body)
	})
	r := networkDetailTestReader(t, path, snapshot, now)
	done := make(chan struct{})
	go func() { r.collect(context.Background()); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("network fetch did not start")
	}
	gate := cloneNetworkDetailGate(*r.gate.Load())
	gate.reason, gate.stamp, gate.observedAt = "source_unavailable", statusStamp{1, 2}, time.Now()
	r.gate.Store(&gate)
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("network collector did not finish")
	}
	reply := r.lookup(time.Now())
	var view PublicNodeNetworkDetail
	if reply.status != http.StatusOK || !reply.nodeScoped || reply.stamp != gate.nodeStamp ||
		json.Unmarshal([]byte(reply.body+reply.tail), &view) != nil || view.Receiver != nil || view.Interfaces == nil {
		t.Fatal("receiver transition lost node facts or exposed the old receiver")
	}
}

func TestNodeNetworkIdenticalRefetchRestoresReceiverAfterInFlightABA(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var requests atomic.Int64
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			close(started)
			<-release
		}
		w.Header().Set(replayDetailRemainingHeader, "30000")
		_, _ = w.Write(body)
	})
	r := networkDetailTestReader(t, path, snapshot, now)
	done := make(chan struct{})
	go func() { r.collect(context.Background()); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("network fetch did not start")
	}
	gate := cloneNetworkDetailGate(*r.gate.Load())
	gate.reason, gate.stamp = "source_unavailable", statusStamp{1, 2}
	r.gate.Store(&gate)
	gate = cloneNetworkDetailGate(gate)
	gate.reason, gate.stamp = "", statusStamp{1, 3}
	r.gate.Store(&gate)
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("network collector did not finish")
	}
	first, reply := r.latest.Load(), r.lookup(time.Now())
	if first == nil || reply.status != http.StatusOK || !reply.nodeScoped {
		t.Fatal("in-flight receiver transition did not retain node-only data")
	}
	r.collect(context.Background())
	current, reply := r.latest.Load(), r.lookup(time.Now())
	if requests.Load() != 2 || current == first || reply.status != http.StatusOK || reply.nodeScoped ||
		current.sourceFreshUntil.After(first.sourceFreshUntil) || !current.publishedAt.Equal(first.publishedAt) {
		t.Fatal("a validated identical refetch did not restore the receiver under the original lease")
	}
}

func TestNetworkReaderSameCaptureKeepsShorterLease(t *testing.T) {
	for _, node := range []bool{false, true} {
		name := "receiver"
		if node {
			name = "node"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			snapshot, receiver := networkDetailFixture(now)
			_, nodeCapture := nodeNetworkSourceFixture(now)
			var detail any = receiver.Detail
			if node {
				detail = nodeCapture.Detail
			}
			body, err := json.Marshal(detail)
			if err != nil {
				t.Fatal(err)
			}
			var remaining atomic.Int64
			remaining.Store(30000)
			path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining.Load(), 10))
				_, _ = w.Write(body)
			})
			r := networkDetailTestReader(t, path, snapshot, now)
			r.collect(context.Background())
			first := r.latest.Load()
			if first == nil {
				t.Fatal("first capture unavailable")
			}
			remaining.Store(5000)
			r.collect(context.Background())
			short := r.latest.Load()
			if short == first || !short.sourceFreshUntil.Before(first.sourceFreshUntil) || !short.publishedAt.Equal(first.publishedAt) {
				t.Fatal("shorter same-capture deadline was ignored")
			}
			remaining.Store(30000)
			r.collect(context.Background())
			if later := r.latest.Load(); later.sourceFreshUntil.After(short.sourceFreshUntil) || !later.publishedAt.Equal(first.publishedAt) {
				t.Fatal("a later identical capture renewed the shortened deadline")
			}
		})
	}
}

func TestNetworkSourceShorterOwnerDeadlineHonored(t *testing.T) {
	for _, node := range []bool{false, true} {
		name := "receiver"
		if node {
			name = "node"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			snapshot, receiver := networkDetailFixture(now)
			_, nodeCapture := nodeNetworkSourceFixture(now)
			source := &networkDetailSource{}
			if node {
				source.nodeSample = func(time.Time) (NodeNetworkCapture, error) { return nodeCapture, nil }
			} else {
				source.sample = func(time.Time) (NetworkDetailCapture, error) { return receiver, nil }
			}
			source.update(snapshot, now)
			first := source.latest.Load()
			if first == nil || first.reason != "" {
				t.Fatal("first source capture unavailable")
			}
			short := now.Add(5 * time.Second)
			receiver.FreshUntil, nodeCapture.FreshUntil = short, short
			source.update(snapshot, time.Now())
			second := source.latest.Load()
			if second.reason != "" || !second.freshUntil.Equal(short) || !second.freshUntil.Before(first.freshUntil) {
				t.Fatal("source did not publish the shortened owner deadline")
			}
		})
	}
}

func TestNetworkDetailFinalAdmission(t *testing.T) {
	setup := func(t *testing.T) (*collector, *networkDetailReader) {
		t.Helper()
		now := time.Now()
		snapshot, capture := nodeNetworkSourceFixture(now)
		prepared, err := preparePublicNodeNetwork(capture.Detail, now, now.Add(30*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		stamp := statusStamp{1, 1}
		family := &publicNetworkDetailFamily{node: prepared, publishedAt: now, sourceFreshUntil: now.Add(30 * time.Second),
			stamp: stamp, nodeStamp: stamp}
		c := &collector{clockOrigin: now}
		gate := networkGate(snapshot, now)
		gate.stamp, gate.nodeStamp = stamp, stamp
		c.networkGate.Store(&gate)
		r := &networkDetailReader{gate: &c.networkGate, limit: rate.NewLimiter(100, 100)}
		r.latest.Store(family)
		frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
		frame.fence.nodeEpoch, frame.fence.families[statusNetwork], frame.fence.families[statusNetworkNode] = 1, 1, 1
		c.latest.Store(frame)
		r.admit = func(reply networkDetailReply) (time.Duration, bool) {
			return c.admitNetworkDetail(r, reply, statusNetworkNode)
		}
		return c, r
	}
	request := func(r *networkDetailReader) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/network", nil))
		return response
	}
	t.Run("independent leases and nil admission", func(t *testing.T) {
		_, r := setup(t)
		response := request(r)
		detail, detailErr := parseNetworkDetailRemaining(response.Header().Get(replayDetailRemainingHeader))
		statusMS, statusErr := strconv.ParseInt(response.Header().Get(statusRemainingHeader), 10, 64)
		if response.Code != http.StatusOK || detailErr != nil || statusErr != nil ||
			statusMS <= 0 || detail <= time.Duration(statusMS)*time.Millisecond {
			t.Fatal("node and detail leases were not independently retained")
		}
		r.admit = nil
		if response := request(r); response.Code != http.StatusServiceUnavailable {
			t.Fatal("missing admission exposed a network family")
		}
	})
	t.Run("failure after selection", func(t *testing.T) {
		c, r := setup(t)
		r.admit = func(reply networkDetailReply) (time.Duration, bool) {
			gate := *r.gate.Load()
			r.failure.Store(&networkDetailReaderFailure{gate: gate, reason: "source_unavailable", nodeScoped: true})
			return c.admitNetworkDetail(r, reply, statusNetworkNode)
		}
		response := request(r)
		if response.Code != http.StatusServiceUnavailable || response.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatal("a late source failure exposed the earlier fresh response")
		}
	})
	t.Run("family replaced after selection", func(t *testing.T) {
		c, r := setup(t)
		r.admit = func(reply networkDetailReply) (time.Duration, bool) {
			family := *reply.family
			family.sourceFreshUntil = time.Now().Add(time.Second)
			r.latest.Store(&family)
			return c.admitNetworkDetail(r, reply, statusNetworkNode)
		}
		if response := request(r); response.Code != http.StatusServiceUnavailable {
			t.Fatal("a replaced family exposed its earlier deadline")
		}
	})
	t.Run("detail expires after selection", func(t *testing.T) {
		c, r := setup(t)
		r.admit = func(reply networkDetailReply) (time.Duration, bool) {
			// The callback runs after lookup in this single-goroutine test.
			reply.family.sourceFreshUntil = time.Now().Add(-time.Second)
			return c.admitNetworkDetail(r, reply, statusNetworkNode)
		}
		response := request(r)
		if response.Code != http.StatusServiceUnavailable || response.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatal("an expired selected detail retained a freshness header")
		}
	})
	t.Run("receiver loss keeps node data", func(t *testing.T) {
		_, r := setup(t)
		gate := cloneNetworkDetailGate(*r.gate.Load())
		gate.reason, gate.stamp = "source_unavailable", statusStamp{1, 2}
		r.gate.Store(&gate)
		response := request(r)
		var view PublicNodeNetworkDetail
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &view) != nil ||
			view.Receiver != nil || view.Interfaces == nil {
			t.Fatal("receiver loss incorrectly hid independent node observations")
		}
	})
	t.Run("changed parent or receiver", func(t *testing.T) {
		c, r := setup(t)
		r.admit = func(reply networkDetailReply) (time.Duration, bool) {
			gate := cloneNetworkDetailGate(*r.gate.Load())
			gate.stamp = statusStamp{1, 2}
			r.gate.Store(&gate)
			return c.admitNetworkDetail(r, reply, statusNetworkNode)
		}
		if response := request(r); response.Code != http.StatusServiceUnavailable {
			t.Fatal("changed receiver admitted the old selected body")
		}
	})
	t.Run("parent status expired", func(t *testing.T) {
		c, r := setup(t)
		frame := *c.latest.Load()
		frame.freshUntil = time.Now().Add(-time.Second)
		c.latest.Store(&frame)
		response := request(r)
		if response.Code != http.StatusServiceUnavailable || response.Header().Get(statusRemainingHeader) != "" {
			t.Fatal("expired parent status admitted a fresh network detail")
		}
	})
	t.Run("source changes between body and tail", func(t *testing.T) {
		c, r := setup(t)
		calls := 0
		r.admit = func(reply networkDetailReply) (time.Duration, bool) {
			calls++
			if calls == 2 {
				return 0, false
			}
			return c.admitNetworkDetail(r, reply, statusNetworkNode)
		}
		defer func() {
			if recover() != http.ErrAbortHandler || calls != 2 {
				t.Fatal("a changed context did not abort the partial response")
			}
		}()
		_ = request(r)
	})
}
