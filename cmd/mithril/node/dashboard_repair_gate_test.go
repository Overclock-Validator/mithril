package node

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func TestRepairGateMeasurement(t *testing.T) {
	now := time.Now()
	start := now.Add(-time.Minute)
	binding := dashboard.NetworkDetailBinding{SourceStartedAt: start, ReceiverStartedAt: start, ReceiverSession: 7}
	s := blockstream.NetworkSnapshot{ConfiguredSource: "turbine", SourceStartedAt: start, SampledAt: now,
		LiveConnected: true, Turbine: &blockstream.TurbineObservation{StartedAt: start, Session: 7}, RepairCatchupFrom: 100}
	got, ok := repairGateMeasurement(s, binding, now)
	if !ok || !got.Armed || !got.SampledAt.Equal(now) {
		t.Fatal("active gate was not observed")
	}
	s.RepairCatchupFrom = 0
	if got, ok = repairGateMeasurement(s, binding, now); !ok || got.Armed {
		t.Fatal("observed inactive gate became unknown or armed")
	}
	for _, mutate := range []func(*blockstream.NetworkSnapshot){
		func(s *blockstream.NetworkSnapshot) { s.ConfiguredSource = "rpc" },
		func(s *blockstream.NetworkSnapshot) { s.LiveConnected = false },
		func(s *blockstream.NetworkSnapshot) { s.Turbine = nil },
		func(s *blockstream.NetworkSnapshot) { s.SampledAt = now.Add(time.Second) },
		func(s *blockstream.NetworkSnapshot) { s.SampledAt = now.Add(-dashboard.NetworkDetailFreshFor) },
		func(s *blockstream.NetworkSnapshot) { s.SourceStartedAt = start.Add(time.Second) },
		func(s *blockstream.NetworkSnapshot) {
			s.Turbine = &blockstream.TurbineObservation{StartedAt: start, Session: 8}
		},
		func(s *blockstream.NetworkSnapshot) { s.Turbine = &blockstream.TurbineObservation{Session: 7} },
	} {
		bad := s
		mutate(&bad)
		if _, ok := repairGateMeasurement(bad, binding, now); ok {
			t.Fatal("unavailable, stale or replaced source was accepted")
		}
	}
}
