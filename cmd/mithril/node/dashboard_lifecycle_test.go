package node

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/gagliardetto/solana-go"
)

func TestBlockLifecycleMeasurementCopiesSourceFacts(t *testing.T) {
	at := time.Now()
	s := consensus.LifecycleSnapshot{StartedAt: at.Add(-time.Minute), UpdatedAt: at, Retained: 128, Evicted: math.MaxUint64, RowCount: 4}
	if dashboard.BlockLifecycleCapacity != consensus.LifecycleCapacity || dashboard.BlockLifecycleVisibleRows != consensus.LifecycleVisibleRows {
		t.Fatal("dashboard lifecycle bounds differ from source")
	}
	for i := range s.Rows {
		s.Rows[i] = consensus.LifecycleRecord{Block: alpenglow.BlockID{Slot: math.MaxUint64 - uint64(i), Hash: solana.Hash{byte(i + 1)}},
			FirstObservedAt: at.Add(-5 * time.Second), LastObservedAt: at, HasIngress: true,
			AssemblyFirstAcceptedAt: at.Add(-5 * time.Second), AssemblyFullAt: at.Add(-4 * time.Second), ReadyAt: at.Add(-3 * time.Second),
			ReplayAcceptedAt: at.Add(-2 * time.Second), DirectFinalizedAt: at, FinalityKind: alpenglow.CertificateFinalizeFast,
			AssemblyFirstToFull: consensus.LifecycleInterval{Known: true},
			FullToReady:         consensus.LifecycleInterval{Known: true, Duration: math.MaxInt64 - 1},
			ReplayToFinality:    consensus.LifecycleInterval{Known: true, Duration: 2 * time.Second}}
	}
	s.Rows[1].FinalityKind = alpenglow.CertificateFinalize
	s.Rows[1].DirectFinalizedAt = at.Add(-4 * time.Second)
	s.Rows[1].FinalityBeforeReplay = true
	s.Rows[1].ReplayToFinality = consensus.LifecycleInterval{}
	s.Rows[1].FinalityToReplay = consensus.LifecycleInterval{Known: true, Duration: 2 * time.Second}
	s.Rows[2] = consensus.LifecycleRecord{Block: alpenglow.BlockID{Hash: solana.Hash{3}}, FirstObservedAt: at, LastObservedAt: at,
		DirectFinalizedAt: at, FinalityKind: alpenglow.CertificateGenesis}
	s.Rows[3] = consensus.LifecycleRecord{Block: alpenglow.BlockID{Slot: 1, Hash: solana.Hash{4}}, FirstObservedAt: at, LastObservedAt: at, ReplayAcceptedAt: at}
	m := consensusMeasurement(consensus.MonitoringSnapshot{Lifecycle: &s, SafetyFault: true}, at)
	l := m.BlockLifecycle
	if m.SafetyFault == nil || !*m.SafetyFault || l == nil || l.RowCount != 4 || l.Evicted != math.MaxUint64 || l.SampledAt.Location() != time.UTC || l.Rows[0].ReadyAt.Location() != time.UTC {
		t.Fatalf("missing lifecycle projection: %+v", l)
	}
	if *l.Rows[0].AssemblyFirstToFullNS != 0 || *l.Rows[0].FullToReadyNS != math.MaxInt64-1 || l.Rows[0].ReadyToReplayNS != nil ||
		*l.Rows[0].FinalityBeforeReplay || !*l.Rows[1].FinalityBeforeReplay || l.Rows[1].ReplayToFinalityNS != nil ||
		*l.Rows[0].FinalityKind != "fast" || *l.Rows[1].FinalityKind != "slow" || *l.Rows[2].FinalityKind != "genesis" ||
		l.Rows[2].Slot != 0 || l.Rows[2].FinalityBeforeReplay != nil || l.Rows[3].FinalityKind != nil {
		t.Fatal("source zero/unknown/finality-order facts changed")
	}
	b, err := json.Marshal(l)
	if err != nil || !strings.Contains(string(b), `"slot":"18446744073709551615"`) || !strings.Contains(string(b), `"full_to_ready_ns":"9223372036854775806"`) {
		t.Fatalf("exact source integers lost: %s, %v", b, err)
	}
	s.Rows[0].FullToReady.Duration, s.Rows[0].Block.Slot, s.Rows[0].ReadyAt = 0, 0, time.Time{}
	s.UpdatedAt = time.Time{}
	if *l.Rows[0].FullToReadyNS != math.MaxInt64-1 || l.Rows[0].Slot != math.MaxUint64 || l.Rows[0].ReadyAt.IsZero() || l.UpdatedAt.IsZero() {
		t.Fatal("private lifecycle aliases source storage")
	}
}

func TestBlockLifecycleMeasurementDisabledEmptyAndInvalid(t *testing.T) {
	at := time.Now()
	measurement := consensusMeasurement(consensus.MonitoringSnapshot{}, at)
	if measurement.SafetyFault == nil || *measurement.SafetyFault {
		t.Fatal("current producer did not report a known non-fault")
	}
	if measurement.BlockLifecycle != nil {
		t.Fatal("disabled/old source manufactured lifecycle")
	}
	empty := &consensus.LifecycleSnapshot{StartedAt: at}
	if l := blockLifecycleMeasurement(empty, at); l == nil || l.UpdatedAt != nil || l.RowCount != 0 || l.Rows[0] != nil {
		t.Fatal("empty source did not remain known empty")
	}
	for _, mutate := range []func(*consensus.LifecycleSnapshot){
		func(s *consensus.LifecycleSnapshot) { s.Retained = -1 },
		func(s *consensus.LifecycleSnapshot) { s.Retained = 129 },
		func(s *consensus.LifecycleSnapshot) { s.RowCount = -1 },
		func(s *consensus.LifecycleSnapshot) { s.RowCount = 5 },
		func(s *consensus.LifecycleSnapshot) { s.Rows[0].HasIngress = true },
		func(s *consensus.LifecycleSnapshot) { s.Rows[0].ReadyAt = at },
		func(s *consensus.LifecycleSnapshot) { s.Rows[0].FinalityKind = alpenglow.CertificateFinalize },
		func(s *consensus.LifecycleSnapshot) {
			s.Rows[0].DirectFinalizedAt = at
			s.Rows[0].FinalityKind = alpenglow.CertificateSkip
		},
		func(s *consensus.LifecycleSnapshot) {
			s.Rows[0].ReplayToFinality = consensus.LifecycleInterval{Known: true, Duration: -1}
		},
		func(s *consensus.LifecycleSnapshot) {
			s.Rows[0].ReplayToFinality = consensus.LifecycleInterval{Known: true, Duration: math.MaxInt64}
		},
	} {
		s := consensus.LifecycleSnapshot{StartedAt: at.Add(-time.Minute), UpdatedAt: at, Retained: 1, RowCount: 1}
		s.Rows[0] = consensus.LifecycleRecord{Block: alpenglow.BlockID{Slot: 1, Hash: solana.Hash{1}}, FirstObservedAt: at, LastObservedAt: at, ReplayAcceptedAt: at}
		mutate(&s)
		if got := blockLifecycleMeasurement(&s, at); got != nil {
			t.Fatalf("invalid source accepted: %+v", s)
		}
	}
}
