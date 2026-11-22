package accountsdb

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"time"
)

type physicalUsageRecorder struct {
	cancel  context.CancelFunc
	stopped atomic.Bool
	latest  atomic.Pointer[PhysicalUsage]
}

// EnablePhysicalUsage starts at most one best-effort observer for this DB.
// Its work is opt-in, does not hold DB locks and is never joined on close.
func (db *AccountsDb) EnablePhysicalUsage() bool {
	if db == nil || db.storageOpenedAt.IsZero() || db.storageClosing.Load() {
		return false
	}
	if r := db.physicalUsage.Load(); r != nil {
		return !r.stopped.Load() && !db.storageClosing.Load()
	}
	dirs := db.AccountsDirs()
	if len(dirs) == 0 || dirs[0] == "" {
		return false
	}
	roots := append([]string{filepath.Dir(dirs[0])}, dirs[1:]...)
	ctx, cancel := context.WithCancel(context.Background())
	recorder := &physicalUsageRecorder{cancel: cancel}
	if !db.physicalUsage.CompareAndSwap(nil, recorder) {
		cancel()
		r := db.physicalUsage.Load()
		return r != nil && !r.stopped.Load() && !db.storageClosing.Load()
	}
	if db.storageClosing.Load() {
		recorder.stopped.Store(true)
		cancel()
		return false
	}
	go func() {
		timer := time.NewTimer(0)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				recorder.latest.Store(nil)
				return
			case <-timer.C:
			}
			value, reason := ObservePhysicalUsage(ctx, roots, PhysicalUsageLimits{MaxEntries: 2_000_000, MaxDuration: 5 * time.Second})
			if reason == "" && ctx.Err() == nil && !recorder.stopped.Load() && !db.storageClosing.Load() {
				copy := value
				recorder.latest.Store(&copy)
			} else {
				recorder.latest.Store(nil)
			}
			timer.Reset(60 * time.Second)
		}
	}()
	return true
}

// StopPhysicalUsage cancels the optional worker without waiting for filesystem calls.
func (db *AccountsDb) StopPhysicalUsage() {
	if db != nil {
		if r := db.physicalUsage.Load(); r != nil {
			r.stopped.Store(true)
			r.cancel()
			r.latest.Store(nil)
		}
	}
}

// PhysicalUsageObservation copies a recent historical result and original deadline.
func (db *AccountsDb) PhysicalUsageObservation(now time.Time) (PhysicalUsage, time.Time, bool) {
	if db == nil || db.storageClosing.Load() || now.IsZero() {
		return PhysicalUsage{}, time.Time{}, false
	}
	r := db.physicalUsage.Load()
	if r == nil || r.stopped.Load() {
		return PhysicalUsage{}, time.Time{}, false
	}
	p := r.latest.Load()
	if p == nil || p.CompletedAt.IsZero() || now == now.Round(0) || p.CompletedAt == p.CompletedAt.Round(0) {
		return PhysicalUsage{}, time.Time{}, false
	}
	age := now.Sub(p.CompletedAt)
	wall := now.Round(0).Sub(p.CompletedAt.Round(0))
	if age < 0 || age >= 75*time.Second || wall-age > 250*time.Millisecond || age-wall > 250*time.Millisecond || db.storageClosing.Load() || r.stopped.Load() {
		return PhysicalUsage{}, time.Time{}, false
	}
	return *p, p.CompletedAt.Add(75 * time.Second), true
}
