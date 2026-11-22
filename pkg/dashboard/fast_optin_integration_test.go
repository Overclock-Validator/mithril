package dashboard

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestFastExecutedRouteOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			path := socketPath(t)
			core := replayDetailLiveCore(0)
			details := DetailSamplers{Replay: func(time.Time) (ReplayDetailCore, error) { return core, nil }}
			if enabled {
				details.FastExecuted = func() (FastExecuted, bool) { return FastExecuted{}, false }
			}
			feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return replayDetailLiveStatus(core) }, details)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(feed.Close)
			c := newCollector(path)
			t.Cleanup(c.client.CloseIdleConnections)
			resp, err := c.client.Get("http://node/details/recent-executed")
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			want := http.StatusNotFound
			if enabled {
				want = http.StatusServiceUnavailable
			}
			if resp.StatusCode != want {
				t.Fatalf("private route: got %d want %d", resp.StatusCode, want)
			}

			address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: true, FastExecuted: enabled})
			defer replayDetailStopPublic(t, cancel, done)
			status, _, _ := replayDetailPublicRequest(t, address, "/api/v1/details/recent-executed", "stats.example", "https://stats.example")
			if status != want {
				t.Fatalf("public route: got %d want %d", status, want)
			}
		})
	}
	_, err := ListenFeedWithDetails(context.Background(), socketPath(t), "", func() Snapshot { return Snapshot{} }, DetailSamplers{
		FastExecuted: func() (FastExecuted, bool) { return FastExecuted{}, false },
	})
	if err == nil {
		t.Fatal("private fast route accepted without replay details")
	}
}
