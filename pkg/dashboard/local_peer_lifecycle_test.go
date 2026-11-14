package dashboard

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func localPeerLifecycleStatus(d LocalPeerDetail) Snapshot {
	// This family does not require replay progress or a connected receiver.
	return Snapshot{RunID: d.Binding.RunID, StartedAt: d.Binding.NodeStartedAt,
		GenesisHash: d.Binding.GenesisHash, Phase: "running"}
}

func TestLocalPeerSourceOriginalLeaseAndBinding(t *testing.T) {
	d := localPeerTestDetail(1)
	p, err := PrepareLocalPeers(d)
	require.NoError(t, err)
	now := time.Now()
	capture := LocalPeerCapture{Data: p, FreshUntil: now.Add(20 * time.Second)}
	calls := 0
	s := &localPeerSource{active: make(chan struct{}, 2)}
	s.sample = func(time.Time) (LocalPeerCapture, error) { calls++; return capture, nil }
	status := localPeerLifecycleStatus(d)
	s.observeStatus(status, now)
	s.poll(context.Background())
	require.NotNil(t, s.latest.Load())
	require.Equal(t, capture.FreshUntil, s.latest.Load().freshUntil)
	for range 3 {
		w := httptest.NewRecorder()
		s.serveHTTP(w, httptest.NewRequest("GET", localPeerPath, nil))
		require.Equal(t, 200, w.Code)
		require.Equal(t, p.Body(), w.Body.String())
		remaining, err := strconv.Atoi(w.Header().Get(replayDetailRemainingHeader))
		require.NoError(t, err)
		require.Positive(t, remaining)
		require.LessOrEqual(t, remaining, 20000)
	}
	require.Equal(t, 1, calls, "viewers must not sample the node")

	for _, tc := range []struct {
		name  string
		until time.Time
	}{
		{"expired", now.Add(-time.Second)},
		{"no_monotonic_clock", now.Add(time.Second).UTC()},
		{"over_lease", now.Add(time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture.FreshUntil = tc.until
			s.poll(context.Background())
			require.Equal(t, "source_time_invalid", s.latest.Load().reason)
			w := httptest.NewRecorder()
			s.serveHTTP(w, httptest.NewRequest("GET", localPeerPath, nil))
			require.Equal(t, 503, w.Code)
			require.Empty(t, w.Header().Get(replayDetailRemainingHeader))
		})
	}

	capture.FreshUntil = time.Now().Add(20 * time.Second)
	s.poll(context.Background())
	prior := s.latest.Load()
	s.sample = func(time.Time) (LocalPeerCapture, error) {
		other := status
		other.RunID = "replacement-node"
		s.observeStatus(other, time.Now())
		s.observeStatus(status, time.Now())
		return capture, nil
	}
	s.poll(context.Background())
	require.Same(t, prior, s.latest.Load(), "A-to-B-to-A must discard the in-flight result")
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest("GET", localPeerPath, nil))
	require.Equal(t, 503, w.Code)
	require.Contains(t, w.Body.String(), "binding_changed")

	status.Phase = "stopping"
	s.observeStatus(status, time.Now())
	s.sample = func(time.Time) (LocalPeerCapture, error) { t.Error("stopping node was sampled"); return capture, nil }
	s.poll(context.Background())
	w = httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest("GET", localPeerPath, nil))
	require.Equal(t, 503, w.Code)
	require.Empty(t, w.Header().Get(replayDetailRemainingHeader))
}

func TestLocalPeerSourceCancellationJoinsSample(t *testing.T) {
	d := localPeerTestDetail(0)
	p, err := PrepareLocalPeers(d)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); unblock() })
	s := &localPeerSource{done: make(chan struct{}), sample: func(time.Time) (LocalPeerCapture, error) {
		close(entered)
		<-release
		return LocalPeerCapture{Data: p, FreshUntil: time.Now().Add(time.Second)}, nil
	}}
	s.observeStatus(localPeerLifecycleStatus(d), time.Now())
	go s.run(ctx)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("source did not start its initial sample")
	}
	cancel()
	select {
	case <-s.done:
		t.Fatal("source reported stopped while its callback was still running")
	default:
	}
	unblock()
	select {
	case <-s.done:
	case <-time.After(time.Second):
		t.Fatal("source did not join the completed callback")
	}
	require.Nil(t, s.latest.Load(), "canceled work must not publish")
}

func TestLocalPeerReaderGenerationLeaseAndBindingRealUnix(t *testing.T) {
	type reply struct {
		body      string
		remaining string
		status    int
	}
	var response atomic.Pointer[reply]
	var gate atomic.Pointer[localPeerGate]
	var requests atomic.Int64
	var changeDuring atomic.Bool
	d := localPeerTestDetail(1)
	d.ClientGeneration, d.RunGeneration = 2, 3
	d.PublicationSequence = 2
	status := localPeerLifecycleStatus(d)
	observe := func(s Snapshot, reason string) {
		g := advanceLocalPeerStatusGate(gate.Load(), &s, reason, time.Now(), 1)
		gate.Store(&g)
	}
	observe(status, "")
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.URL.Path != localPeerPath {
			t.Errorf("unexpected node request: %s", request.URL.Path)
		}
		if changeDuring.Swap(false) {
			other := status
			other.RunID += "-intervening"
			observe(other, "")
			observe(status, "")
		}
		v := response.Load()
		if v.remaining != "" {
			w.Header().Set(replayDetailRemainingHeader, v.remaining)
		}
		w.WriteHeader(v.status)
		_, _ = io.WriteString(w, v.body)
	})
	r, err := newLocalPeerReader(newCollector(path).client, &gate)
	require.NoError(t, err)
	r.admit = func(localPeerReply) (time.Duration, bool) { return time.Second, true }
	t.Cleanup(r.client.CloseIdleConnections)
	set := func(value LocalPeerDetail, remaining string) {
		t.Helper()
		p, err := PrepareLocalPeers(value)
		require.NoError(t, err)
		response.Store(&reply{p.Body(), remaining, 200})
	}
	collect := func() { r.collect(context.Background()) }
	set(d, "30000")
	collect()
	require.Nil(t, r.failure.Load())
	first := r.latest.Load()
	require.NotNil(t, first)
	w := httptest.NewRecorder()
	r.serveHTTP(w, httptest.NewRequest("GET", localPeerPublicPath, nil))
	require.Equal(t, 200, w.Code)
	require.Equal(t, first.body, w.Body.String())
	require.Equal(t, int64(1), requests.Load(), "public request must remain cache-only")
	collect()
	require.Nil(t, r.failure.Load())
	require.Equal(t, first.freshUntil, r.latest.Load().freshUntil, "repeat cannot renew")
	set(d, "5000")
	collect()
	require.Nil(t, r.failure.Load())
	short := r.latest.Load()
	require.True(t, short.freshUntil.Before(first.freshUntil))
	response.Store(&reply{`{"schema_version":1,"family":"local_peers","state":"unavailable","reason":"source_unavailable"}`, "", 503})
	collect()
	require.Equal(t, "source_unavailable", r.failure.Load().reason)
	set(d, "30000")
	collect()
	require.Nil(t, r.failure.Load())
	require.Equal(t, short.freshUntil, r.latest.Load().freshUntil, "error must not forget original lease")

	// A higher source sequence gets its own lease, including across UTC rollback.
	// The old body must not become new again merely because its date is greater.
	priorPublication := d
	d.PublicationSequence++
	d.SampledAt = d.SampledAt.Add(-time.Hour)
	d.CapturedAt = d.SampledAt
	set(d, "30000")
	collect()
	require.Nil(t, r.failure.Load())
	accepted := r.latest.Load()
	require.Equal(t, d.PublicationSequence, r.accepted.sequence)
	set(priorPublication, "30000")
	collect()
	require.Equal(t, "source_invalid", r.failure.Load().reason, "A-to-B-to-A must not renew an older sequence")
	require.Same(t, accepted, r.latest.Load())
	for _, tc := range []struct {
		name   string
		change func(*LocalPeerDetail)
		want   string
	}{
		{"older_client", func(d *LocalPeerDetail) { d.ClientGeneration-- }, "binding_changed"},
		{"older_run", func(d *LocalPeerDetail) { d.RunGeneration-- }, "binding_changed"},
		{"client_date", func(d *LocalPeerDetail) { d.ClientStartedAt = d.ClientStartedAt.Add(time.Second) }, "source_invalid"},
		{"run_date", func(d *LocalPeerDetail) { d.RunStartedAt = d.RunStartedAt.Add(time.Second) }, "source_invalid"},
		{"older_sequence", func(d *LocalPeerDetail) { d.PublicationSequence--; d.SampledAt = d.SampledAt.Add(time.Hour) }, "source_invalid"},
		{"same_sequence_mutation", func(d *LocalPeerDetail) { d.CapacityRefusals-- }, "source_invalid"},
		{"same_sequence_date_change", func(d *LocalPeerDetail) { d.SampledAt = d.SampledAt.Add(-time.Minute) }, "source_invalid"},
		{"wrong_node", func(d *LocalPeerDetail) { d.Binding.RunID = "other-node" }, "source_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := d
			tc.change(&bad)
			set(bad, "30000")
			collect()
			require.Equal(t, tc.want, r.failure.Load().reason)
			require.Same(t, accepted, r.latest.Load(), "rejected result replaced last accepted bytes")
		})
	}

	// Generation high-water survives unavailable/recovered status in this node.
	observe(status, "source_unavailable")
	collect()
	observe(status, "")
	set(priorPublication, "30000")
	collect()
	require.Equal(t, "source_invalid", r.failure.Load().reason, "failure must not reset sequence high-water")
	set(d, "30000")
	collect()
	require.Nil(t, r.failure.Load())
	require.Equal(t, accepted.freshUntil, r.latest.Load().freshUntil, "same sequence must preserve its lease across failure")
	d.PublicationSequence = math.MaxUint64
	set(d, "30000")
	collect()
	require.Nil(t, r.failure.Load(), "last nonwrapping publication is representable")
	wrapped := d
	wrapped.PublicationSequence = 1
	set(wrapped, "30000")
	collect()
	require.Equal(t, "source_invalid", r.failure.Load().reason, "same-Run sequence must not wrap")
	old := d
	old.RunGeneration--
	set(old, "30000")
	collect()
	require.Equal(t, "binding_changed", r.failure.Load().reason)
	d.RunGeneration++
	d.PublicationSequence = 1
	d.RunStartedAt = d.RunStartedAt.Add(time.Second)
	set(d, "30000")
	collect()
	require.Nil(t, r.failure.Load())
	d.ClientGeneration++
	d.ClientStartedAt = d.ClientStartedAt.Add(time.Second)
	d.RunGeneration = 1
	set(d, "30000")
	collect()
	require.Nil(t, r.failure.Load(), "new client owns a separate Run counter")
	d.Binding.RunID = "replacement-node"
	d.ClientGeneration, d.RunGeneration = 1, 1
	status = localPeerLifecycleStatus(d)
	observe(status, "")
	set(d, "30000")
	collect()
	require.Nil(t, r.failure.Load(), "new node owns separate source generations")
	accepted = r.latest.Load()
	changeDuring.Store(true)
	collect()
	require.Nil(t, r.failure.Load(), "an old-context failure must not overwrite the new gate")
	require.Same(t, accepted, r.latest.Load(), "in-flight ABA must not replace publication")
	w = httptest.NewRecorder()
	r.serveHTTP(w, httptest.NewRequest("GET", localPeerPublicPath, nil))
	require.Equal(t, 503, w.Code)
	require.Empty(t, w.Header().Get(replayDetailRemainingHeader))
}
