package dashboard

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func replayDetailV2Fixture(count int) ReplayDetailCore {
	c := replayDetailTestCore(count)
	c.SchemaVersion = 2
	for i := range c.Records {
		loader := &c.Records[i].AccountLoader
		loader.InProgressLookup, loader.SysvarInProgressLookup = nil, nil
		loader.InProgressHits, loader.SysvarInProgressHits = nil, nil
	}
	return c
}

func TestReplayDetailVersionsAndDetachedLegacyFields(t *testing.T) {
	legacy := replayDetailTestCore(1)
	current := replayDetailV2Fixture(1)
	for _, core := range []ReplayDetailCore{legacy, current} {
		if err := ValidateReplayDetailCore(core); err != nil {
			t.Fatal(err)
		}
	}
	bad := replayDetailV2Fixture(1)
	bad.Records[0].AccountLoader.InProgressHits = new(uint64)
	if ValidateReplayDetailCore(bad) == nil {
		t.Fatal("v2 accepted a retired measurement")
	}
	bad = replayDetailTestCore(1)
	bad.Records[0].AccountLoader.InProgressHits = nil
	if ValidateReplayDetailCore(bad) == nil {
		t.Fatal("v1 accepted a missing measurement")
	}
	cloned := cloneReplayDetailRecords(legacy.Records)
	if !replayDetailRecordsEqual(cloned[0], legacy.Records[0]) {
		t.Fatal("equal values in independent copies differ")
	}
	*cloned[0].AccountLoader.InProgressHits = 9
	cloned[0].AccountLoader.InProgressLookup.Count = 7
	if replayDetailRecordsEqual(cloned[0], legacy.Records[0]) {
		t.Fatal("changed legacy values compared equal")
	}
	if *legacy.Records[0].AccountLoader.InProgressHits != 0 || legacy.Records[0].AccountLoader.InProgressLookup.Count != 0 {
		t.Fatal("reader copy aliases legacy measurements")
	}
}

func TestReplayDetailV2RealFeedAndVersionFence(t *testing.T) {
	core := replayDetailV2Fixture(1)
	at := time.Now().UTC().Add(-time.Second)
	core.Binding.NodeStartedAt, core.Binding.AttemptStartedAt, core.Binding.ResetAt = at, at, at
	core.UpdatedAt, core.Records[0].CompletedAt, core.Records[0].ObservedAt = at, at, at
	path := socketPath(t)
	feed, err := ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot { return replayDetailLiveStatus(core) }, func(capturedAt time.Time) (ReplayDetailCore, error) {
		value := core
		value.CapturedAt = capturedAt.UTC()
		return value, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	collector := newCollector(path)
	t.Cleanup(collector.client.CloseIdleConnections)
	m := replayDetailWaitManifest(t, collector.client, "http://node/details/replay")
	if m.SchemaVersion != 2 {
		t.Fatalf("manifest version = %d", m.SchemaVersion)
	}
	status, body, _ := replayDetailHTTP(t, collector.client, "GET", "http://node/details/replay?page=0&generation="+m.Generation, "")
	if status != 200 {
		t.Fatalf("page status = %d", status)
	}
	p, err := decodeReplayDetailPage([]byte(body), m, 0)
	if err != nil {
		t.Fatal(err)
	}
	if p.Records[0].AccountLoader.InProgressHits != nil || p.Records[0].AccountLoader.InProgressLookup != nil {
		t.Fatal("retired measurement became zero")
	}
	p.SchemaVersion = 1
	mixed, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeReplayDetailPage(mixed, m, 0); err == nil {
		t.Fatal("mixed schema page accepted")
	}
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: true})
	t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
	deadline := time.Now().Add(3 * time.Second)
	var public PublicReplayDetailManifest
	for {
		code, data, _ := replayDetailPublicRequest(t, address, "/api/v1/details/replay", "stats.example", "")
		if code == 200 {
			if err := json.Unmarshal(data, &public); err != nil {
				t.Fatal(err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("public replay detail unavailable: %d %s", code, data)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if public.SchemaVersion != 2 {
		t.Fatalf("public version = %d", public.SchemaVersion)
	}
	code, data, _ := replayDetailPublicRequest(t, address, "/api/v1/details/replay?generation="+public.Generation+"&page=0", "stats.example", "https://stats.example")
	var page PublicReplayDetailPage
	if code != 200 || json.Unmarshal(data, &page) != nil || page.SchemaVersion != 2 || len(page.Records) != 1 || page.Records[0].AccountLoader.InProgressHits != nil {
		t.Fatalf("public page did not preserve v2 unavailable fields: %d %s", code, data)
	}
}
