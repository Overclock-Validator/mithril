package node

import (
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func TestStorageAccountOperationsWindowAndConcurrentCaptures(t *testing.T) {
	at := time.Now()
	m := &storageDashboardMonitor{binding: dashboard.StorageDetailBinding{DBOpenedAt: at.Add(-time.Hour).UTC()}}
	o := &accountsdb.AccountOperationsObservation{StartedAt: at.Add(-time.Minute), RequestedKeys: 10, CommittedRecords: 3}
	if m.accountOperationsWindow(o, at, at) != nil {
		t.Fatal("one endpoint made a rate")
	}
	o.RequestedKeys, o.CommittedRecords = 20, 5
	w := m.accountOperationsWindow(o, at.Add(time.Second), at.Add(time.Second))
	if w == nil || w.DurationNanoseconds != uint64(time.Second) || w.RequestedKeys != 10 || w.CommittedRecords != 2 {
		t.Fatalf("wrong delta %+v", w)
	}
	if m.accountOperationsWindow(nil, at.Add(2*time.Second), at.Add(2*time.Second)) != nil {
		t.Fatal("missing source made a rate")
	}
	o.RequestedKeys = 19
	if m.accountOperationsWindow(o, at.Add(3*time.Second), at.Add(3*time.Second)) != nil || !m.operationsRetired {
		t.Fatal("regression after a nil gap was not retired")
	}
	o.RequestedKeys = 100
	if m.accountOperationsWindow(o, at.Add(4*time.Second), at.Add(4*time.Second)) != nil {
		t.Fatal("retired source revived")
	}
	m = &storageDashboardMonitor{binding: m.binding}
	o.RequestedKeys = 10
	m.accountOperationsWindow(o, at, at)
	o.RequestedKeys = 20
	if m.accountOperationsWindow(o, at.Add(31*time.Second), at.Add(31*time.Second)) != nil {
		t.Fatal("gap made an oversized rate window")
	}
	o.RequestedKeys = 21
	if w := m.accountOperationsWindow(o, at.Add(32*time.Second), at.Add(32*time.Second)); w == nil || w.RequestedKeys != 1 {
		t.Fatal("fresh post-gap window unavailable")
	}
	if m.accountOperationsWindow(o, at.UTC(), at.UTC()) != nil {
		t.Fatal("wall-only clocks admitted")
	}
	// Exercise actual sampleStorageDetail concurrently. Sampling and baseline
	// update must be under the same owner mutex, not only publication.
	db, _ := dashboardStorageDB(t)
	d := storageNodeFixture()
	if !d.attachStorageOwner(db) {
		t.Fatal("runtime owner did not attach")
	}
	var wg sync.WaitGroup
	failures := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := d.sampleStorageDetail(time.Now())
			if err != nil {
				failures <- err
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	d.mu.Lock()
	retired := d.storage.operationsRetired
	d.mu.Unlock()
	if retired {
		t.Fatal("concurrent ordered captures falsely retired counters")
	}
	d.stopStorageOwner()
	if _, err := d.sampleStorageDetail(time.Now()); err == nil {
		t.Fatal("stopped owner emitted activity")
	}
}
