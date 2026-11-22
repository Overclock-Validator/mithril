package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"
)

func maximumSnapshotShardFlush(at time.Time) *SnapshotShardFlush {
	end := uint64(math.MaxUint64)
	return &SnapshotShardFlush{Attempt: math.MaxUint64, StartedAt: at, SampledAt: at, Phase: "returned",
		FullSnapshotSlot: math.MaxUint64 - 1, IncrementalSlot: &end, CompletedShards: 1000, TotalShards: 1000, SSTIngestReturnedAt: &at}
}

func TestSnapshotShardFlushProjectionBoundsAndInvalid(t *testing.T) {
	at := sizingTime(time.Now())
	s := fullTestSnapshot(at)
	s.SnapshotArchive, s.IncrementalArchive = nil, nil
	s.SnapshotShardFlush = maximumSnapshotShardFlush(at)
	private, err := json.Marshal(s)
	if err != nil || len(private) > MaxPrivateSnapshotBytes || s.validate() != nil {
		t.Fatalf("complete private shard-stage envelope: %d/%d, %v", len(private), MaxPrivateSnapshotBytes, err)
	}
	p, _ := advancedPublicValue(t, &s, "fresh", at)
	if p.Startup.SnapshotShardFlush == nil || p.Startup.SnapshotShardFlush.CompletedShards != 1000 || p.Startup.IncrementalArchive != nil || p.Startup.SnapshotArchive != nil {
		t.Fatal("public stage was omitted or relabelled as archive bytes")
	}
	*p.Startup.SnapshotShardFlush.IncrementalSlot = 1
	if *s.SnapshotShardFlush.IncrementalSlot != math.MaxUint64 {
		t.Fatal("public projection aliased the source slot")
	}
	if p.Startup.SnapshotShardFlush.SSTIngestReturnedAt == nil || *p.Startup.SnapshotShardFlush.SSTIngestReturnedAt != at {
		t.Fatal("public projection lost the successful ingestion event")
	}
	*p.Startup.SnapshotShardFlush.SSTIngestReturnedAt = time.Time{}
	if *s.SnapshotShardFlush.SSTIngestReturnedAt != at {
		t.Fatal("public projection aliased the source ingestion date")
	}
	t.Logf("maximum complete private shard-stage envelope: %d/%d bytes", len(private), MaxPrivateSnapshotBytes)
	for _, mutate := range []func(*SnapshotShardFlush){
		func(a *SnapshotShardFlush) { a.Attempt = 0 }, func(a *SnapshotShardFlush) { a.TotalShards = 0 },
		func(a *SnapshotShardFlush) { a.TotalShards = 1001 }, func(a *SnapshotShardFlush) { a.CompletedShards = 1001 },
		func(a *SnapshotShardFlush) { a.Phase = "ready" }, func(a *SnapshotShardFlush) { a.IncrementalSlot = &a.FullSnapshotSlot },
		func(a *SnapshotShardFlush) { a.Phase = "active" },
		func(a *SnapshotShardFlush) { a.SSTIngestReturnedAt = &time.Time{} },
		func(a *SnapshotShardFlush) {
			before := a.SampledAt.Add(-time.Nanosecond)
			a.SSTIngestReturnedAt = &before
		},
	} {
		s.SnapshotShardFlush = maximumSnapshotShardFlush(at)
		mutate(s.SnapshotShardFlush)
		if s.validate() == nil || publicStartup(&s, "fresh", at).SnapshotShardFlush != nil {
			t.Fatal("invalid shard observation became public")
		}
	}
	s.SnapshotShardFlush = maximumSnapshotShardFlush(at)
	s.SnapshotArchive = maximumSnapshotArchive(at)
	if s.validate() == nil {
		t.Fatal("archive and shard stages coexisted")
	}
	p, _ = advancedPublicValue(t, &s, "fresh", at)
	if p.Startup.SnapshotArchive != nil || p.Startup.SnapshotShardFlush != nil {
		t.Fatal("conflicting stages were selectively relabelled")
	}
}

func TestSnapshotShardFlushRealFeedCollectorAndOldOmission(t *testing.T) {
	at := time.Now().UTC()
	s := testSnapshot(at)
	s.SnapshotShardFlush = maximumSnapshotShardFlush(at)
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	t.Cleanup(c.client.CloseIdleConnections)
	for i, phase := range []string{"active", "returned", "error"} {
		s.SampledAt = at.Add(time.Duration(i) * time.Millisecond)
		s.SnapshotShardFlush.Attempt = uint64(i + 1)
		s.SnapshotShardFlush.SampledAt = s.SampledAt
		s.SnapshotShardFlush.Phase = phase
		s.SnapshotShardFlush.SSTIngestReturnedAt = nil
		if phase != "active" {
			returned := s.SampledAt
			s.SnapshotShardFlush.SSTIngestReturnedAt = &returned
		}
		raw, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		body.Store(&raw)
		c.poll(context.Background(), s.SampledAt)
		var p PublicSnapshot
		latest := c.latest.Load()
		if latest == nil || json.Unmarshal(latest.advancedPublicJSON, &p) != nil || p.Startup == nil || p.Startup.SnapshotShardFlush == nil || p.Startup.SnapshotShardFlush.Phase != phase {
			t.Fatal("real collector lost the exact source call result")
		}
		got, want := p.Startup.SnapshotShardFlush.SSTIngestReturnedAt, s.SnapshotShardFlush.SSTIngestReturnedAt
		if (got == nil) != (want == nil) || got != nil && !got.Equal(*want) {
			t.Fatal("real collector changed or invented the SST-ingest event date")
		}
	}
	messages, err := frames(&s, "fresh", at)
	if err != nil {
		t.Fatal(err)
	}
	if _, known := decodedFrames(t, &frameSet{messages: messages})["mithril.snapshot_shard_flush"]; !known {
		t.Fatal("private frame was not published")
	}
	observed := s.SnapshotShardFlush.SampledAt
	s.SampledAt = s.SampledAt.Add(6 * time.Second)
	p, _ := advancedPublicValue(t, &s, "fresh", s.SampledAt)
	if p.Startup.SnapshotShardFlush.State != "stale" || p.Startup.SnapshotShardFlush.SampledAt != observed {
		t.Fatal("fresh heartbeat renewed a historical shard-stage clock")
	}
	s.SnapshotShardFlush = nil
	raw, _ := json.Marshal(s)
	body.Store(&raw)
	c.poll(context.Background(), s.SampledAt)
	// Decode into a fresh destination, as an ordinary consumer must.
	var old PublicSnapshot
	if json.Unmarshal(c.latest.Load().publicJSON, &old) != nil || old.Startup.SnapshotShardFlush != nil {
		t.Fatal("old producer inherited the last shard-stage child")
	}
}
