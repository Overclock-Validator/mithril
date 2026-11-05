package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type feedLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *feedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *feedLogBuffer) records(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	data := bytes.Clone(b.b.Bytes())
	b.mu.Unlock()
	var records []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	return records
}

func captureFeedLogs(t *testing.T) *feedLogBuffer {
	t.Helper()
	b := new(feedLogBuffer)
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(b, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return b
}

func TestFeedReportsUnexpectedListenerExit(t *testing.T) {
	logs := captureFeedLogs(t)
	feed, err := ListenFeed(context.Background(), socketPath(t), func() Snapshot { return testSnapshot(time.Now().UTC()) })
	if err != nil {
		t.Fatal(err)
	}
	if err := feed.listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-feed.served:
	case <-time.After(time.Second):
		t.Fatal("listener did not stop")
	}
	feed.Close()
	records := logs.records(t)
	if len(records) != 1 || records[0]["msg"] != "dashboard feed listener stopped unexpectedly" || records[0]["level"] != "WARN" {
		t.Fatalf("unexpected listener exit log: %v", records)
	}
	delete(records[0], "time")
	want := map[string]any{"level": "WARN", "msg": "dashboard feed listener stopped unexpectedly", "component": "dashboard", "entrypoint": "private_feed"}
	if !reflect.DeepEqual(records[0], want) {
		t.Fatalf("unexpected listener log fields: %v", records[0])
	}
	feed, err = ListenFeed(context.Background(), socketPath(t), func() Snapshot { return testSnapshot(time.Now().UTC()) })
	if err != nil {
		t.Fatal(err)
	}
	feed.Close()
	if len(logs.records(t)) != 1 {
		t.Fatal("normal shutdown logged an unexpected failure")
	}
}

func assertFeedLog(t *testing.T, record map[string]any, reason string) {
	t.Helper()
	stamp, ok := record["time"].(string)
	if _, err := time.Parse(time.RFC3339Nano, stamp); !ok || err != nil {
		t.Fatal("log lacks a valid timestamp")
	}
	delete(record, "time")
	want := map[string]any{"component": "dashboard", "operation": "feed_sample", "entrypoint": "private_feed"}
	if reason == "" {
		want["level"], want["msg"] = "INFO", "dashboard feed sampling recovered"
	} else {
		want["level"], want["msg"], want["reason"] = "WARN", "dashboard feed sample rejected", reason
	}
	if reason == "snapshot_oversized" {
		size, ok := record["encoded_bytes"].(float64)
		if !ok || size <= MaxPrivateSnapshotBytes || record["limit_bytes"] != float64(MaxPrivateSnapshotBytes) {
			t.Fatal("oversize log lacks bounded size fields")
		}
		want["encoded_bytes"], want["limit_bytes"] = size, float64(MaxPrivateSnapshotBytes)
	}
	if !reflect.DeepEqual(record, want) {
		t.Fatalf("unexpected log fields: got %v, want %v", record, want)
	}
}

func rejectedFeedSample(reason string) Snapshot {
	s := testSnapshot(time.Now().UTC())
	s.RunID, s.Version = "private-run-do-not-log", "private-version-do-not-log"
	switch reason {
	case "sample_invalid":
		s.Mode = "private-invalid-mode-do-not-log"
	case "marshal_failed":
		nan := math.NaN()
		s.Resources = &Resources{CPUSeconds: &nan}
	case "snapshot_oversized":
		s.Version += strings.Repeat("<", MaxPrivateSnapshotBytes/6+1)
	}
	return s
}

func TestFeedRejectionInitialCategories(t *testing.T) {
	for _, reason := range []string{"sample_invalid", "marshal_failed", "snapshot_oversized"} {
		t.Run(reason, func(t *testing.T) {
			logs := captureFeedLogs(t)
			feed, err := ListenFeed(context.Background(), socketPath(t), func() Snapshot { return rejectedFeedSample(reason) })
			if err != nil {
				t.Fatal(err)
			}
			feed.Close()
			if feed.latest.Load() != nil {
				t.Fatal("initial rejection published a body")
			}
			records := logs.records(t)
			if len(records) != 1 {
				t.Fatalf("initial rejection logs: got %d, want 1", len(records))
			}
			assertFeedLog(t, records[0], reason)
		})
	}
}

func TestFeedRejectionEpisodesPreserveCache(t *testing.T) {
	logs := captureFeedLogs(t)
	var sample atomic.Pointer[Snapshot]
	var calls atomic.Int64
	good := testSnapshot(time.Now().UTC())
	sample.Store(&good)
	path := socketPath(t)
	feed, err := ListenFeed(context.Background(), path, func() Snapshot {
		calls.Add(1)
		return *sample.Load()
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	wait := func(condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(4 * time.Second)
		for !condition() {
			if time.Now().After(deadline) {
				t.Fatal("feed did not reach the expected sampling state")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	if len(logs.records(t)) != 0 {
		t.Fatal("initial success logged a recovery")
	}
	accepted := feed.latest.Load()
	before := bytes.Clone(accepted.body)
	var original Snapshot
	if err := json.Unmarshal(before, &original); err != nil {
		t.Fatal(err)
	}
	oversized := rejectedFeedSample("snapshot_oversized")
	sample.Store(&oversized)
	wait(func() bool { return len(logs.records(t)) == 1 })
	invalid := rejectedFeedSample("sample_invalid")
	sample.Store(&invalid)
	count := calls.Load()
	// Two further sampler entries ensure a changed-reason update has completed.
	wait(func() bool { return calls.Load() >= count+2 })
	if len(logs.records(t)) != 1 || feed.latest.Load() != accepted || !bytes.Equal(feed.latest.Load().body, before) {
		t.Fatal("continued rejection logged again or changed the accepted cache")
	}
	c := newCollector(path)
	t.Cleanup(c.client.CloseIdleConnections)
	c.poll(context.Background(), original.SampledAt.Add(StaleAfter+time.Second))
	if statusFields(t, c.latest.Load())["source_state"] != "stale" {
		t.Fatal("rejected samples refreshed the reader's source state")
	}
	sample.Store(&good)
	wait(func() bool { return len(logs.records(t)) == 2 })
	if feed.latest.Load() == accepted || bytes.Equal(feed.latest.Load().body, before) {
		t.Fatal("accepted recovery did not replace the cached body")
	}
	count = calls.Load()
	wait(func() bool { return calls.Load() >= count+2 })
	if len(logs.records(t)) != 2 {
		t.Fatal("continued success logged another recovery")
	}
	badMarshal := rejectedFeedSample("marshal_failed")
	sample.Store(&badMarshal)
	wait(func() bool { return len(logs.records(t)) == 3 })
	sample.Store(&good)
	wait(func() bool { return len(logs.records(t)) == 4 })
	feed.Close()
	records := logs.records(t)
	if len(records) != 4 {
		t.Fatalf("episode log count: got %d, want 4", len(records))
	}
	for i, reason := range []string{"snapshot_oversized", "", "marshal_failed", ""} {
		assertFeedLog(t, records[i], reason)
	}
}
