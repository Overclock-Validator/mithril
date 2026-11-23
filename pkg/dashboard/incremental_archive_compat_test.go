package dashboard

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestIncrementalArchiveLegacyDecodeAndRawBodyBoundary(t *testing.T) {
	// These are the old decoder's relevant fields. Unknown incremental_archive
	// input is ignored, and each accepted sample gets a fresh destination.
	type legacySnapshot struct {
		SchemaVersion   int              `json:"schema_version"`
		RunID           string           `json:"run_id"`
		SampledAt       time.Time        `json:"sampled_at"`
		StartedAt       time.Time        `json:"started_at"`
		Mode            string           `json:"mode"`
		Phase           string           `json:"phase"`
		SnapshotArchive *SnapshotArchive `json:"snapshot_archive,omitempty"`
	}
	at := time.Now().UTC()
	s := testSnapshot(at)
	s.SnapshotArchive = maximumSnapshotArchive(at)
	full, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	s.SampledAt = at.Add(time.Millisecond)
	s.SnapshotArchive, s.IncrementalArchive = nil, maximumIncrementalArchive(at)
	incremental, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	for i, raw := range [][]byte{full, incremental} {
		var legacy legacySnapshot
		if json.Unmarshal(raw, &legacy) != nil || legacy.RunID != s.RunID {
			t.Fatal("legacy raw decoding failed")
		}
		projection := publicStartup(&Snapshot{StartedAt: legacy.StartedAt, SampledAt: legacy.SampledAt, SnapshotArchive: legacy.SnapshotArchive}, "fresh", at)
		if i == 0 && projection.SnapshotArchive == nil || i > 0 && projection.SnapshotArchive != nil || projection.IncrementalArchive != nil {
			t.Fatal("legacy decoder relabelled a newer startup stage as full archive bytes")
		}
	}
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	t.Cleanup(c.client.CloseIdleConnections)
	read := func() PublicSnapshot {
		t.Helper()
		set := c.latest.Load()
		var p PublicSnapshot
		if set == nil || json.Unmarshal(set.publicJSON, &p) != nil {
			t.Fatal("missing public state")
		}
		var frame struct{ Value json.RawMessage }
		if len(set.messages) != 1 || json.Unmarshal(set.messages[0], &frame) != nil || string(frame.Value) != string(set.publicJSON) {
			t.Fatal("HTTP/WebSocket projections differ")
		}
		return p
	}
	body.Store(&full)
	c.poll(context.Background(), at)
	if read().Startup.SnapshotArchive == nil {
		t.Fatal("full baseline not accepted")
	}
	// Account for raw wire bytes, before unknown data/whitespace can be dropped.
	boundary := append(append([]byte{}, incremental...), []byte(strings.Repeat(" ", MaxPrivateSnapshotBytes-len(incremental)))...)
	body.Store(&boundary)
	c.poll(context.Background(), s.SampledAt)
	if p := read(); p.SourceState != "fresh" || p.Startup.SnapshotArchive != nil || p.Startup.IncrementalArchive == nil {
		t.Fatal("exact-boundary incremental did not replace the full snapshot")
	}
	oversized := append(append([]byte{}, boundary...), ' ')
	body.Store(&oversized)
	c.poll(context.Background(), s.SampledAt)
	if p := read(); p.SourceState != "stale" || p.Startup.SnapshotArchive != nil || p.Startup.IncrementalArchive == nil || p.Startup.IncrementalArchive.State != "stale" {
		t.Fatal("oversized body did not retain only explicitly stale accepted data")
	}
	// A valid old producer without either child clears retained archive data.
	s.IncrementalArchive = nil
	s.SampledAt = at.Add(2 * time.Millisecond)
	absent, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	body.Store(&absent)
	c.poll(context.Background(), s.SampledAt)
	if p := read(); p.Startup.SnapshotArchive != nil || p.Startup.IncrementalArchive != nil {
		t.Fatal("old producer inherited archive data")
	}
}
