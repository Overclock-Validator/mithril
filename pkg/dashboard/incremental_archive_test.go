package dashboard

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"
)

func maximumIncrementalArchive(now time.Time) *IncrementalArchive {
	a := maximumSnapshotArchive(now)
	a.FullSnapshotSlot--
	return &IncrementalArchive{SnapshotArchive: *a, IncrementalSlot: math.MaxUint64}
}

func TestIncrementalArchiveMaximumEnvelope(t *testing.T) {
	now := sizingTime(time.Now())
	s := fullTestSnapshot(now)
	s.Mode = "validator"
	s.SnapshotArchive, s.IncrementalArchive = nil, maximumIncrementalArchive(now)
	if err := s.validate(); err != nil {
		t.Fatal(err)
	}
	assertSizingTimestamps(t, s)
	private, err := json.Marshal(s)
	if err != nil || len(private) > MaxPrivateSnapshotBytes {
		t.Fatalf("incremental envelope: %d/%d bytes, %v", len(private), MaxPrivateSnapshotBytes, err)
	}
	var decoded Snapshot
	if err := json.Unmarshal(private, &decoded); err != nil || decoded.SnapshotArchive != nil ||
		!reflect.DeepEqual(decoded.IncrementalArchive, s.IncrementalArchive) || decoded.Resources == nil || decoded.Replay == nil ||
		decoded.Consensus == nil || decoded.Pipeline == nil || decoded.RPC == nil || decoded.BlockSource == nil {
		t.Fatal("incremental envelope dropped a current group")
	}
	t.Logf("maximum private incremental envelope: %d/%d bytes", len(private), MaxPrivateSnapshotBytes)
	p, body := publicValue(t, &s, "fresh", now)
	if p.Startup.SnapshotArchive != nil || p.Startup.IncrementalArchive == nil ||
		p.Startup.IncrementalArchive.IncrementalSlot != math.MaxUint64 || len(body) > MaxSnapshotBytes {
		t.Fatal("incremental public projection missing or mislabeled")
	}
	*p.Startup.IncrementalArchive.CompressedReportedTotal = 1
	if *s.IncrementalArchive.CompressedReportedTotal != math.MaxUint64 {
		t.Fatal("public total aliases the source")
	}
	s.SnapshotArchive = maximumSnapshotArchive(now)
	if s.validate() == nil {
		t.Fatal("conflicting archive observations accepted")
	}
	p, _ = publicValue(t, &s, "fresh", now)
	if p.Startup.SnapshotArchive != nil || p.Startup.IncrementalArchive != nil {
		t.Fatal("bypassed validation published conflicting archive counters")
	}
}
