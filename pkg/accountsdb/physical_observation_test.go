package accountsdb

import (
	"context"
	"testing"
	"time"
)

func TestPhysicalObservationStopFence(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	r := &physicalUsageRecorder{cancel: cancel}
	now := time.Now()
	value := PhysicalUsage{StartedAt: now.Add(-time.Second), CompletedAt: now, DurationNanoseconds: uint64(time.Second), Directories: 1}
	db := &AccountsDb{storageOpenedAt: now.Add(-time.Minute)}
	db.physicalUsage.Store(r)
	r.latest.Store(&value)
	if _, deadline, ok := db.PhysicalUsageObservation(now.Add(time.Second)); !ok || !deadline.Equal(now.Add(75*time.Second)) {
		t.Fatal("fresh observation rejected")
	}
	db.StopPhysicalUsage()
	// Simulate a scan that had passed its cancellation check before stop.
	r.latest.Store(&value)
	if _, _, ok := db.PhysicalUsageObservation(now.Add(time.Second)); ok {
		t.Fatal("stopped worker resurrected observation")
	}
	if db.EnablePhysicalUsage() {
		t.Fatal("stopped worker reenabled")
	}
}
