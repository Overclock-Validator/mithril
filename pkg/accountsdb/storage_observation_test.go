package accountsdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestWriteStallObservationTransitions(t *testing.T) {
	var absent *writeStallRecorder
	absent.begin(time.Now(), "")
	absent.end(time.Now())
	absent.retire()
	if got, ok := absent.snapshot(); ok || got != (WriteStallObservation{}) {
		t.Fatal("absent recorder fabricated idle")
	}
	r := newWriteStallRecorder()
	if got, ok := r.snapshot(); !ok || got != (WriteStallObservation{}) {
		t.Fatalf("subscribed idle record: %+v, %t", got, ok)
	}
	at := time.Now()
	r.end(at)
	r.begin(time.Time{}, "")
	r.begin(at, "memtable count limit reached")
	r.begin(at.Add(time.Second), "different")
	active, _ := r.snapshot()
	if !active.HasEvent || !active.Active || active.InitialReason != "memtable" || active.BeganAt != at || !active.EndedAt.IsZero() || active.DurationKnown {
		t.Fatalf("active record or duplicate begin: %+v", active)
	}
	end := at.Add(time.Millisecond)
	r.end(end)
	r.end(end.Add(time.Second))
	completed, _ := r.snapshot()
	if completed.Active || completed.BeganAt != at || completed.EndedAt != end || !completed.DurationKnown || completed.Duration != time.Millisecond {
		t.Fatalf("completed record or duplicate end: %+v", completed)
	}
	completed.InitialReason = "changed"
	if next, _ := r.snapshot(); next.InitialReason != "memtable" {
		t.Fatal("snapshot aliases recorded state")
	}
	for _, end := range []time.Time{at.Add(-time.Second), at.AddDate(400, 0, 0)} {
		r.begin(at, "unknown internal reason")
		r.end(end)
		if got, _ := r.snapshot(); got.Active || got.DurationKnown || got.Duration != 0 || got.InitialReason != "other" || got.EndedAt != end {
			t.Fatalf("invalid elapsed interval was measured: %+v", got)
		}
	}
	r.begin(at, "L0 file count limit exceeded")
	r.end(at)
	if got, _ := r.snapshot(); got.InitialReason != "l0" || !got.DurationKnown || got.Duration != 0 {
		t.Fatalf("known zero interval: %+v", got)
	}
	r.retire()
	r.begin(at, "")
	r.end(at)
	if _, ok := r.snapshot(); ok {
		t.Fatal("retired record revived")
	}
}

func TestWriteStallObservationConcurrentCopies(t *testing.T) {
	r := newWriteStallRecorder()
	var readers sync.WaitGroup
	for range 3 {
		readers.Go(func() {
			for range 10000 {
				v, ok := r.snapshot()
				if ok && v.HasEvent && !v.Active && (!v.DurationKnown || v.EndedAt.Before(v.BeganAt)) {
					t.Error("reader observed a partial transition")
					return
				}
			}
		})
	}
	at := time.Now()
	for range 1000 {
		r.begin(at, "memtable count limit reached")
		r.end(at.Add(time.Millisecond))
	}
	readers.Wait()
	r.retire()
}

func TestStorageObservationRuntimeLifetime(t *testing.T) {
	for _, db := range []*AccountsDb{nil, {}} {
		if got, ok := db.StorageObservation(); ok || got != (StorageObservation{}) {
			t.Fatal("unopened source fabricated idle")
		}
	}
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "accounts"), 0o755); err != nil {
		t.Fatal(err)
	}
	meta := filepath.Join(dir, "largest_file_id")
	if err := os.WriteFile(meta, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if db, err := OpenDb(dir); err == nil || db != nil {
		t.Fatal("bad metadata published an owner")
	}
	if err := os.WriteFile(meta, make([]byte, 8), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			db.CloseDb()
		}
	}()
	initial, ok := db.StorageObservation()
	if !ok || initial.OpenedAt.IsZero() || initial.Index != (WriteStallObservation{}) || initial.BankHash != (WriteStallObservation{}) {
		t.Fatalf("successful open is not known idle: %+v, %t", initial, ok)
	}
	at := time.Now()
	db.indexWriteStalls.begin(at, "memtable count limit reached")
	db.indexWriteStalls.end(at.Add(time.Millisecond))
	db.bankHashWriteStalls.begin(at, "L0 file count limit exceeded")
	before, _ := db.StorageObservation()
	if !before.Index.HasEvent || before.Index.Active || !before.BankHash.Active || before.BankHash.InitialReason != "l0" {
		t.Fatalf("independent stores: %+v", before)
	}
	oldProgram, oldCommon, oldLayout := ProgramCacheMaxMB, CommonAccountCacheMaxMB, CommonAccountCacheLayout
	ProgramCacheMaxMB, CommonAccountCacheMaxMB, CommonAccountCacheLayout = 1, 1, "flat"
	defer func() {
		ProgramCacheMaxMB, CommonAccountCacheMaxMB, CommonAccountCacheLayout = oldProgram, oldCommon, oldLayout
	}()
	for range 2 {
		db.InitCaches()
		stopObservationCaches(t, db)
		if got, ok := db.StorageObservation(); !ok || got != before {
			t.Fatal("cache initialization reset DB history")
		}
	}
	db.CloseDb()
	closed = true
	if _, ok := db.StorageObservation(); ok {
		t.Fatal("closed DB remained available")
	}
	db.indexWriteStalls.begin(time.Now(), "")
	if _, ok := db.indexWriteStalls.snapshot(); ok {
		t.Fatal("closed callback source revived")
	}
	reopened, err := OpenDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseDb()
	if got, ok := reopened.StorageObservation(); !ok || got.OpenedAt.Equal(initial.OpenedAt) || got.Index.HasEvent || got.BankHash.HasEvent {
		t.Fatalf("reopened DB inherited prior history: %+v, %t", got, ok)
	}
}

func TestWriteStallListenerPreservesDiagnostics(t *testing.T) {
	metrics.DiagnosticsSummary()
	r := newWriteStallRecorder()
	l := pebbleEventListener("storage_source_test", r)
	l.WriteStallBegin(pebble.WriteStallBeginInfo{Reason: "memtable count limit reached"})
	l.WriteStallEnd()
	if got, ok := r.snapshot(); !ok || got.Active || !got.HasEvent || !got.DurationKnown {
		t.Fatal("listener did not update its per-store recorder")
	}
	if summary := metrics.DiagnosticsSummary(); !strings.Contains(summary, "task pebble.storage_source_test.write_stall runs=1 ") {
		t.Fatalf("legacy diagnostic event lost: %s", summary)
	}
	// Callers without an observation keep the original diagnostic behavior.
	l = pebbleEventListener("storage_source_nil_test", nil)
	l.WriteStallBegin(pebble.WriteStallBeginInfo{})
	l.WriteStallEnd()
}

// Blocking TableCreated is only a fixture control, following Pebble's upstream
// TestWriteStallEvents. The production observer performs no wait or DB call.
func TestWriteStallObservationRealPebbleEvents(t *testing.T) {
	for _, delayFlush := range []bool{true, false} {
		t.Run(fmt.Sprintf("delay_flush_%t", delayFlush), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r := newWriteStallRecorder()
			listener := pebbleEventListener("storage_real_test", r)
			released, ended := make(chan struct{}), make(chan struct{})
			var release, endedOnce, delayOnce sync.Once
			var timedOut atomic.Bool
			listener.TableCreated = func(info pebble.TableCreateInfo) {
				if delayFlush == (info.Reason == "flushing") {
					delayOnce.Do(func() {
						select {
						case <-released:
						case <-ctx.Done():
							timedOut.Store(true)
						}
					})
				}
			}
			begin, end := listener.WriteStallBegin, listener.WriteStallEnd
			listener.WriteStallBegin = func(info pebble.WriteStallBeginInfo) { begin(info); release.Do(func() { close(released) }) }
			listener.WriteStallEnd = func() { end(); endedOnce.Do(func() { close(ended) }) }
			db, err := pebble.Open("db", &pebble.Options{FS: vfs.NewMem(), Logger: silentLogger{}, EventListener: listener,
				MemTableSize: 256 << 10, MemTableStopWritesThreshold: 2, L0CompactionThreshold: 2, L0StopWritesThreshold: 2})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				for range 10 {
					if err := db.Set([]byte("a"), nil, pebble.NoSync); err != nil {
						done <- err
						return
					}
					ch, err := db.AsyncFlush()
					if err != nil {
						done <- err
						return
					}
					if !delayFlush {
						select {
						case <-ch:
						case <-ctx.Done():
							done <- ctx.Err()
							return
						}
					}
					select {
					case <-ended:
						done <- nil
						return
					default:
					}
				}
				select {
				case <-ended:
					done <- nil
				case <-ctx.Done():
					done <- ctx.Err()
				}
			}()
			select {
			case err = <-done:
			case <-ctx.Done():
				release.Do(func() { close(released) })
				select {
				case err = <-done:
				case <-time.After(time.Second):
					t.Fatal("fixture writer did not join")
				}
			}
			cancel()
			release.Do(func() { close(released) })
			got, known := r.snapshot()
			closeErr := db.Close()
			r.retire()
			want := "l0"
			if delayFlush {
				want = "memtable"
			}
			if err != nil || closeErr != nil || timedOut.Load() {
				t.Fatalf("fixture: write=%v close=%v callback_timeout=%t", err, closeErr, timedOut.Load())
			}
			if !known || !got.HasEvent || got.Active || got.InitialReason != want || got.BeganAt.IsZero() || got.EndedAt.IsZero() ||
				!got.DurationKnown || got.Duration < 0 || got.Duration != got.EndedAt.Sub(got.BeganAt) {
				t.Fatalf("real callback record: %+v, %t", got, known)
			}
		})
	}
}
