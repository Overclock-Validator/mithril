package consensus

import (
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
)

func lifecycleTiming(at time.Time) block.TurbineIngressTimings {
	return block.TurbineIngressTimings{AssemblyFirstAcceptedAt: at.Add(time.Second), AssemblyFullAt: at.Add(2 * time.Second), ReadyAt: at.Add(3 * time.Second)}
}

func lifecycleFind(t *testing.T, snapshot LifecycleSnapshot, id alpenglow.BlockID) LifecycleRecord {
	t.Helper()
	for _, row := range snapshot.Rows[:snapshot.RowCount] {
		if row.Block == id {
			return row
		}
	}
	t.Fatalf("missing lifecycle row %s in %+v", id, snapshot)
	return LifecycleRecord{}
}

func TestLifecycleJoinsExactIdentityInEitherEventOrder(t *testing.T) {
	for _, finalityFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "replay-first", true: "finality-first"}[finalityFirst], func(t *testing.T) {
			at := time.Now()
			r := newLifecycleRecorder(at)
			id, other := lifecycleID(20, 1), lifecycleID(20, 2)
			finalAt := at.Add(5 * time.Second)
			if finalityFirst {
				finalAt = at.Add(2 * time.Second)
				r.observeFinality(id, alpenglow.CertificateFinalizeFast, finalAt)
			}
			if !r.observeIngress(id, lifecycleTiming(at), at.Add(3*time.Second)) || !r.observeReplay(id, at.Add(4*time.Second)) {
				t.Fatal("valid ingress/replay was rejected")
			}
			if !finalityFirst {
				r.observeFinality(id, alpenglow.CertificateFinalizeFast, finalAt)
			}
			r.observeFinality(other, alpenglow.CertificateFinalize, at.Add(6*time.Second))
			s := r.snapshot()
			row := lifecycleFind(t, s, id)
			if row.Block != id || !row.HasIngress || !row.DirectFinalizedAt.Equal(finalAt) || row.WallOrderConflict {
				t.Fatalf("incorrect joined row: %+v", row)
			}
			for _, interval := range []LifecycleInterval{row.AssemblyFirstToFull, row.FullToReady, row.ReadyToReplay} {
				if !interval.Known || interval.Duration != time.Second {
					t.Fatalf("interval = %+v, want known 1s", interval)
				}
			}
			if finalityFirst {
				if !row.FinalityBeforeReplay || row.ReplayToFinality.Known || !row.FinalityToReplay.Known || row.FinalityToReplay.Duration != 2*time.Second {
					t.Fatalf("finality-first ordering lost: %+v", row)
				}
			} else if row.FinalityBeforeReplay || !row.ReplayToFinality.Known || row.ReplayToFinality.Duration != time.Second || row.FinalityToReplay.Known {
				t.Fatalf("replay-first ordering lost: %+v", row)
			}
			otherRow := lifecycleFind(t, s, other)
			if otherRow.HasIngress || !otherRow.ReplayAcceptedAt.IsZero() || otherRow.ReplayToFinality.Known {
				t.Fatalf("same-slot identity was cross-joined: %+v", otherRow)
			}
		})
	}
}

func TestLifecycleKeepsFirstGenerationAndDuplicateTimes(t *testing.T) {
	at := time.Now()
	r := newLifecycleRecorder(at)
	id := lifecycleID(0, 1)
	r.observeIngress(id, lifecycleTiming(at), at.Add(3*time.Second))
	r.observeReplay(id, at.Add(4*time.Second))
	r.observeFinality(id, alpenglow.CertificateGenesis, at.Add(4*time.Second))
	before := r.snapshot()
	if r.observeIngress(id, lifecycleTiming(at.Add(time.Hour)), at.Add(2*time.Hour)) ||
		r.observeReplay(id, at.Add(2*time.Hour)) || r.observeFinality(id, alpenglow.CertificateFinalizeFast, at.Add(2*time.Hour)) {
		t.Fatal("duplicate changed a retained event")
	}
	if !reflect.DeepEqual(before, r.snapshot()) {
		t.Fatal("duplicate redated/reordered the snapshot")
	}
	if retained := r.entries[0].ReadyAt; retained == retained.Round(0) {
		t.Fatal("recording or snapshot stripped the retained monotonic clock")
	}
	if copied := before.Rows[0].ReadyAt; copied != copied.Round(0) {
		t.Fatal("returned UTC snapshot retained a process-local monotonic clock")
	}
	row := before.Rows[0]
	if !row.ReplayToFinality.Known || row.ReplayToFinality.Duration != 0 || row.FinalityToReplay.Known {
		t.Fatalf("equal known timestamps must retain known zero: %+v", row)
	}
	other := lifecycleID(1, 2)
	r.observeReplay(other, at.Add(5*time.Second))
	if r.observeIngress(other, lifecycleTiming(at.Add(time.Hour)), at.Add(2*time.Hour)) || lifecycleFind(t, r.snapshot(), other).HasIngress {
		t.Fatal("later reassembly supplied ingress for an earlier replay")
	}
	row.Block.Hash[0] = 255
	before.Rows[0] = LifecycleRecord{}
	if lifecycleFind(t, r.snapshot(), id).Block != id {
		t.Fatal("snapshot mutation reached recorder storage")
	}
}

func TestLifecycleBoundsInsertionWithoutSlotArithmetic(t *testing.T) {
	at := time.Now()
	r := newLifecycleRecorder(at)
	for i := uint64(1); i <= LifecycleCapacity; i++ {
		r.observeReplay(lifecycleID(math.MaxUint64, i), at.Add(time.Duration(i)*time.Second))
	}
	old := lifecycleID(math.MaxUint64, 1)
	// Updating an old retained row does not refresh its insertion position.
	r.observeFinality(old, alpenglow.CertificateFinalize, at.Add(time.Hour))
	r.observeReplay(lifecycleID(0, 999), at.Add(2*time.Hour))
	s := r.snapshot()
	if s.Retained != LifecycleCapacity || s.RowCount != LifecycleVisibleRows || s.Evicted != 1 || s.Rows[0].Block.Slot != 0 {
		t.Fatalf("incorrect bounded snapshot: %+v", s)
	}
	if _, found := r.index[old]; found {
		t.Fatal("updated oldest entry escaped insertion-order eviction")
	}
	for i, row := range s.Rows[1:] {
		if row.Block != lifecycleID(math.MaxUint64, uint64(LifecycleCapacity-i)) {
			t.Fatalf("row %d has wrong insertion order: %+v", i+1, row)
		}
	}
	r.evicted = math.MaxUint64
	r.observeReplay(lifecycleID(1, 1000), at.Add(3*time.Hour))
	if r.snapshot().Evicted != math.MaxUint64 || len(r.index) != LifecycleCapacity {
		t.Fatal("eviction counter wrapped or storage grew")
	}
}

func TestLifecycleRejectsMissingOrUnsupportedEvents(t *testing.T) {
	at := time.Now()
	r := newLifecycleRecorder(at)
	id := lifecycleID(1, 1)
	for _, kind := range []alpenglow.CertificateType{"", "future-kind", alpenglow.CertificateNotarize, alpenglow.CertificateSkip, alpenglow.CertificateNotarizeFallback} {
		if r.observeFinality(id, kind, at) {
			t.Fatalf("accepted non-finality kind %q", kind)
		}
	}
	if r.observeReplay(alpenglow.BlockID{Slot: 1}, at) || r.observeReplay(id, time.Time{}) || r.observeReplay(id, at.Add(-time.Second)) ||
		r.observeIngress(id, block.TurbineIngressTimings{}, at) || r.observeIngress(id, lifecycleTiming(at), at.Add(time.Second)) {
		t.Fatal("accepted missing, pre-session, or future evidence")
	}
	for _, field := range []string{"first", "full", "ready"} {
		for _, invalidAt := range []time.Time{at.Add(-time.Second), at.Add(5 * time.Second)} {
			timing := lifecycleTiming(at)
			switch field {
			case "first":
				timing.AssemblyFirstAcceptedAt = invalidAt
			case "full":
				timing.AssemblyFullAt = invalidAt
			case "ready":
				timing.ReadyAt = invalidAt
			}
			if r.observeIngress(id, timing, at.Add(4*time.Second)) {
				t.Fatalf("accepted %s timestamp outside this observation/session", field)
			}
		}
	}
	if s := r.snapshot(); s.Retained != 0 || !s.UpdatedAt.IsZero() || s.Evicted != 0 {
		t.Fatalf("invalid events mutated recorder: %+v", s)
	}
}

func TestLifecycleWallOrderingUnknownAndSessionIsolation(t *testing.T) {
	at := time.Date(2026, 10, 31, 10, 0, 0, 0, time.FixedZone("test", 3600))
	r := newLifecycleRecorder(at)
	id := lifecycleID(3, 1)
	timing := lifecycleTiming(at)
	timing.AssemblyFullAt = timing.AssemblyFirstAcceptedAt.Add(-time.Millisecond)
	r.observeIngress(id, timing, at.Add(3*time.Second))
	r.observeReplay(id, at.Add(2*time.Second))
	row := r.snapshot().Rows[0]
	if !row.WallOrderConflict || row.AssemblyFirstToFull.Known || row.ReadyToReplay.Known || !row.DirectFinalizedAt.IsZero() {
		t.Fatalf("invalid wall order manufactured intervals: %+v", row)
	}
	if row.ReadyAt.Location() != time.UTC || row.FirstObservedAt.Location() != time.UTC || r.entries[0].ReadyAt.Location() == time.UTC {
		t.Fatal("UTC snapshot conversion changed retained source time")
	}
	if lifecycleInterval(at, at.Add(time.Duration(math.MaxInt64))).Known {
		t.Fatal("saturated interval presented as exact")
	}
	newSession := newLifecycleRecorder(at.Add(time.Hour))
	newSession.observeFinality(id, alpenglow.CertificateFinalize, at.Add(2*time.Hour))
	fresh := newSession.snapshot().Rows[0]
	if fresh.HasIngress || !fresh.ReplayAcceptedAt.IsZero() || fresh.ReadyToReplay.Known {
		t.Fatal("new session inherited old timing evidence")
	}
}

func TestLifecycleConcurrentSnapshotsAndDisabledCost(t *testing.T) {
	at := time.Now()
	id := lifecycleID(1, 1)
	var disabled *lifecycleRecorder
	if newLifecycleRecorder(time.Time{}) != nil {
		t.Fatal("zero session should stay disabled")
	}
	if allocations := testing.AllocsPerRun(100, func() {
		disabled.observeIngress(id, lifecycleTiming(at), at.Add(3*time.Second))
		disabled.observeReplay(id, at)
		disabled.observeFinality(id, alpenglow.CertificateFinalize, at)
		disabled.snapshot()
	}); allocations != 0 {
		t.Fatalf("disabled recorder allocated %v times", allocations)
	}
	r := newLifecycleRecorder(at)
	var workers sync.WaitGroup
	for worker := uint64(0); worker < 4; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := uint64(1); i <= 300; i++ {
				id := lifecycleID(i, worker+1)
				r.observeIngress(id, lifecycleTiming(at), at.Add(3*time.Second))
				r.observeFinality(id, alpenglow.CertificateFinalizeFast, at.Add(4*time.Second))
				r.observeReplay(id, at.Add(5*time.Second))
				s := r.snapshot()
				if s.Retained > LifecycleCapacity || s.RowCount > LifecycleVisibleRows {
					t.Errorf("concurrent snapshot exceeded bounds: %+v", s)
					return
				}
			}
		}()
	}
	workers.Wait()
}
