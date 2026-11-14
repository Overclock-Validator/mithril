package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The actual Feed worker and Unix transport are used. Reader collection is
// explicit, as in other detail integration tests, not an accelerated timer.
func TestLocalPeerRealFeedPublicReaderBothRoles(t *testing.T) {
	for _, role := range []string{"validator", "verifying"} {
		t.Run(role, func(t *testing.T) {
			ctx := context.Background()
			path := socketPath(t)
			var samples atomic.Int64
			open := func(run string) (*Feed, *PreparedLocalPeers, time.Time) {
				t.Helper()
				now := time.Now()
				snapshot := testSnapshot(now.UTC())
				snapshot.Mode, snapshot.RunID = role, run
				d := localPeerTestDetail(1)
				snapshot.GenesisHash = d.Binding.GenesisHash
				d.Binding = NodeNetworkBinding{RunID: run, NodeStartedAt: snapshot.StartedAt, GenesisHash: snapshot.GenesisHash}
				d.ClientGeneration, d.RunGeneration = 1, 1
				d.ClientStartedAt, d.RunStartedAt = snapshot.StartedAt, snapshot.StartedAt
				d.CapturedAt, d.SampledAt, d.Rows[0].ObservedAt = now.UTC(), now.UTC(), now.UTC()
				prepared, err := PrepareLocalPeers(d)
				require.NoError(t, err)
				until := now.Add(20 * time.Second)
				feed, err := ListenFeedWithDetails(ctx, path, "", func() Snapshot { return snapshot }, DetailSamplers{
					LocalPeers: func(time.Time) (LocalPeerCapture, error) {
						samples.Add(1)
						return LocalPeerCapture{Data: prepared, FreshUntil: until}, nil
					},
				})
				require.NoError(t, err)
				t.Cleanup(feed.Close)
				require.Eventually(t, func() bool {
					p := feed.localPeers.latest.Load()
					return p != nil && p.reason == ""
				}, 2*time.Second, 5*time.Millisecond, "initial peer publication")
				return feed, prepared, until
			}
			feed, prepared, until := open(role + "-first")
			c := newCollector(path)
			c.public, c.publicOptions = true, PublicOptions{LocalPeers: true}
			t.Cleanup(c.client.CloseIdleConnections)
			c.poll(ctx, time.Now().UTC())
			var status PublicSnapshot
			require.NoError(t, json.Unmarshal(c.latest.Load().publicJSON, &status))
			require.Equal(t, "fresh", status.SourceState)
			require.NotNil(t, status.Node)
			require.Equal(t, role, status.Node.Mode)
			r, err := newLocalPeerReader(c.client, &c.localPeerGate)
			require.NoError(t, err)
			r.admit = func(localPeerReply) (time.Duration, bool) { return time.Second, true }
			t.Cleanup(r.client.CloseIdleConnections)
			r.collect(ctx)
			require.Nil(t, r.failure.Load())
			require.NotNil(t, r.latest.Load())
			require.False(t, r.latest.Load().freshUntil.After(until), "transport must not extend the original source lease")
			request := func(want int) *httptest.ResponseRecorder {
				t.Helper()
				var w *httptest.ResponseRecorder
				// The real limiter remains enabled; it may pace the last assertion.
				require.Eventually(t, func() bool {
					w = httptest.NewRecorder()
					r.serveHTTP(w, httptest.NewRequest(http.MethodGet, localPeerPublicPath, nil))
					return w.Code != http.StatusTooManyRequests
				}, 2*time.Second, 600*time.Millisecond)
				require.Equal(t, want, w.Code, "public peer body: %s", w.Body.String())
				return w
			}
			// Close joins the source worker. Until ordinary status is recollected,
			// the reader may serve only its already-dated, unextended cached copy.
			feed.Close()
			before := samples.Load()
			for range 2 {
				w := request(200)
				require.Equal(t, prepared.Body(), w.Body.String())
				remaining, err := parseDetailRemaining(w.Header().Get(replayDetailRemainingHeader), NetworkDetailFreshFor)
				require.NoError(t, err)
				require.Positive(t, remaining)
				require.LessOrEqual(t, remaining, 20*time.Second)
			}
			require.Equal(t, before, samples.Load(), "viewers must not invoke the source sampler")
			c.poll(ctx, time.Now().UTC())
			r.collect(ctx)
			lost := request(503)
			require.Empty(t, lost.Header().Get(replayDetailRemainingHeader))
			require.Contains(t, lost.Body.String(), "source_unavailable")

			replacement, next, nextUntil := open(role + "-replacement")
			c.poll(ctx, time.Now().UTC())
			fenced := request(503)
			require.Contains(t, fenced.Body.String(), "binding_changed")
			require.Empty(t, fenced.Header().Get(replayDetailRemainingHeader))
			r.collect(ctx)
			require.Nil(t, r.failure.Load())
			require.False(t, r.latest.Load().freshUntil.After(nextUntil))
			replacement.Close()
			before = samples.Load()
			current := request(200)
			require.Equal(t, next.Body(), current.Body.String())
			require.NotEqual(t, prepared.Body(), current.Body.String())
			require.Equal(t, before, samples.Load())
		})
	}
}
