package dashboard

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestDetailFeedRequestRateIsBounded(t *testing.T) {
	path := socketPath(t)
	feed, err := ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot {
		return testSnapshot(time.Now().UTC())
	}, func(time.Time) (ReplayDetailCore, error) { return ReplayDetailCore{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	client := newCollector(path).client
	t.Cleanup(client.CloseIdleConnections)
	const requests = detailFeedRequestBurst + detailFeedRequestsPerSecond
	start := time.Now()
	for range requests {
		resp, err := client.Get("http://node/snapshot")
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil || closeErr != nil || resp.StatusCode != 200 {
			t.Fatalf("snapshot: status %d, read %v, close %v", resp.StatusCode, readErr, closeErr)
		}
	}
	floor := time.Duration(requests-detailFeedRequestBurst-1) * time.Second / detailFeedRequestsPerSecond
	if elapsed := time.Since(start); elapsed < floor {
		t.Fatalf("detail feed exceeded its request limit: %v < %v", elapsed, floor)
	}
}
