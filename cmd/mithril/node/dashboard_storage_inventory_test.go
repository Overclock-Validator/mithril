package node

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
)

func TestStorageInventoryBindsReturnedCycleToDatabase(t *testing.T) {
	db, _ := dashboardStorageDB(t)
	d := storageNodeFixture()
	if !d.attachStorageOwner(db) {
		t.Fatal("owner attachment failed")
	}
	m := d.configureCompaction(true)
	opened := d.storage.binding.DBOpenedAt
	at := time.Now().Add(-time.Second)
	c := compactionObservation{SessionStartedAt: at, StartedAt: at.Add(time.Millisecond), CompletedAt: at.Add(3 * time.Millisecond),
		Failed: true, Stats: accountsdb.CompactStats{Inventory: accountsdb.CompactionInventoryObservation{
			Known: true, DBOpenedAt: opened, ObservedAt: at.Add(2 * time.Millisecond),
			DataFiles: 1, ManifestNames: 2, LogicalBytes: 3, AllocatedBytes: 4096, AllocatedBytesKnown: true},
			Liveness: accountsdb.CompactionLivenessObservation{Known: true, ScannedCandidates: 1, CompleteCandidates: 1,
				RewriteCandidates: 1, SourceBytes: 3, ExaminedBytes: 3, LiveBytes: 1, DeadBytes: 2}}}
	m.latest.Store(&c)
	capture, err := d.sampleStorageDetail(time.Now())
	if err != nil || capture.Detail.Inventory == nil || capture.Detail.Liveness == nil || !capture.Detail.Inventory.CycleFailed || !capture.Detail.Liveness.CycleFailed ||
		capture.Detail.Inventory.LogicalBytes != 3 || capture.Detail.Inventory.ObservedAt != c.Stats.Inventory.ObservedAt.UTC() {
		t.Fatalf("returned cycle not copied: %+v, %v", capture.Detail.Inventory, err)
	}
	capture.Detail.Inventory.LogicalBytes = 99
	*capture.Detail.Inventory.AllocatedBytes = 99
	if c.Stats.Inventory.LogicalBytes != 3 || c.Stats.Inventory.AllocatedBytes != 4096 {
		t.Fatal("public copy aliases source")
	}
	for _, mutate := range []func(*compactionObservation){
		func(v *compactionObservation) { v.Stats.Inventory.Known = false },
		func(v *compactionObservation) { v.Stats.Inventory.DBOpenedAt = opened.Add(time.Second) },
		func(v *compactionObservation) { v.Stats.Inventory.ObservedAt = time.Time{} },
		func(v *compactionObservation) { v.Stats.Inventory.ObservedAt = v.CompletedAt.Add(time.Nanosecond) },
		func(v *compactionObservation) { v.StartedAt = v.SessionStartedAt.Add(-time.Nanosecond) },
		func(v *compactionObservation) { v.CompletedAt = v.StartedAt.Add(-time.Nanosecond) },
		func(v *compactionObservation) { v.Stats.Inventory.DataFiles = 0 },
		func(v *compactionObservation) { v.Stats.Inventory.AllocatedBytesKnown = false },
	} {
		bad := c
		mutate(&bad)
		m.latest.Store(&bad)
		capture, err := d.sampleStorageDetail(time.Now())
		if err != nil || capture.Detail.Inventory != nil || capture.Detail.Liveness != nil {
			t.Fatal("invalid inventory replaced valid base storage observation")
		}
	}
	bad := c
	bad.Stats.Liveness.LiveBytes = 99
	m.latest.Store(&bad)
	capture, err = d.sampleStorageDetail(time.Now())
	if err != nil || capture.Detail.Inventory == nil || capture.Detail.Liveness != nil {
		t.Fatal("invalid liveness suppressed the valid inventory or remained available")
	}
	c.Stats.Inventory.AllocatedBytesKnown, c.Stats.Inventory.AllocatedBytes = false, 0
	m.latest.Store(&c)
	capture, err = d.sampleStorageDetail(time.Now())
	if err != nil || capture.Detail.Inventory == nil || capture.Detail.Inventory.AllocatedBytes != nil {
		t.Fatal("unknown allocation erased other inventory fields or became zero")
	}
	d.configureCompaction(false)
	capture, err = d.sampleStorageDetail(time.Now())
	if err != nil || capture.Detail.Inventory != nil {
		t.Fatal("disabled compactor fabricated inventory")
	}
}
