package harmonic

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
)

func TestServiceObservationValuesAndInvalidReports(t *testing.T) {
	var got ServiceObservation
	s := &Service{
		cfg:                  ServiceConfig{Observe: func(o ServiceObservation) { got = o }},
		blockEngine:          NewBlockEngineSession(BlockEngineConfig{}),
		observationStartedAt: time.Now(),
	}
	stats := statsSnapshot{
		blockEngine: BlockEngineStats{Sessions: 1, Resets: 2, Packets: 3, DroppedSize: 4, DroppedFull: 5, Blocks: 6, NoticesSent: 7, NoticesExpired: 8, NoticeErrors: 9, StreamsLost: 10, StrategyErrors: 11, InvalidFeeInfo: 12},
		relayer:     RelayerStats{Sessions: 13, Resets: 14, Heartbeats: 15, Batches: 16, Packets: 17, DroppedSize: 18, DroppedFull: 19, InvalidTPUConfigs: 20},
		pipeline:    BlockPipelineStats{Messages: 21, WrongSlot: 22, LostSlot: 23, Bundles: 24, InvalidBundles: 25, Transactions: math.MaxUint64},
	}
	report := SlotReport{Slot: math.MaxUint64, Mode: ModeHarmonic, Flags: FlagTimeout | FlagFailed | FlagReverted | FlagUnavailable,
		Executed: 4, Reverted: 3, Rejected: 2, MaxParallel: 8, Crank: CrankLanded,
		Arrived: true, FirstArrival: time.Duration(math.MaxInt64), Aborted: true, Reason: "secret-abort-canary"}
	s.observeSlotReport(report)
	s.observeStats(stats, true, false, true, true)
	if got.StartedAt != s.observationStartedAt || got.CapturedAt.Before(got.StartedAt) || got.RequestedStrategy != "fba" ||
		!got.BlockEngineHealthy || got.RelayerConfigured || got.RelayerUsable || !got.RelayerTPUAdvertised || !got.HarmonicClientIDAdvertised ||
		got.BlockEngine != stats.blockEngine || got.Relayer != stats.relayer || got.Pipeline != stats.pipeline {
		t.Fatalf("source measurement changed: %+v", got)
	}
	wantSlot := SlotObservation{ObservedAt: got.LastSlot.ObservedAt, Slot: report.Slot, Mode: "harmonic", Timeout: true, Failed: true,
		Reverted: true, Unavailable: true, Aborted: true, Executed: 4, RevertedBundles: 3, Rejected: 2,
		MaxParallel: 8, Crank: "landed", HasFirstArrival: true, FirstArrival: report.FirstArrival}
	if !got.HasLastSlot || got.LastSlot != wantSlot || got.LastSlot.ObservedAt.Before(got.StartedAt) || got.LastSlot.ObservedAt.After(got.CapturedAt) {
		t.Fatalf("slot measurement changed: %+v", got.LastSlot)
	}
	encoded, err := json.Marshal(got)
	if err != nil || strings.Contains(string(encoded), report.Reason) {
		t.Fatal("free-form report reason entered observations")
	}
	saved := got
	s.lastSlot.Load().Executed = 999 // Test the value copy, not a production mutation.
	stats.pipeline.Transactions = 0
	if saved.LastSlot.Executed != 4 || saved.Pipeline.Transactions != math.MaxUint64 {
		t.Fatal("observation retained mutable source state")
	}
	before := got
	s.blockEngine.SetStrategy(blockenginepb.SchedulingStrategy(999))
	s.observeStats(stats, false, false, false, false)
	if got != before {
		t.Fatal("unknown strategy published a known observation")
	}
	for _, mutate := range []func(*SlotReport){
		func(r *SlotReport) { r.Mode = ModeDone },
		func(r *SlotReport) { r.Crank = CrankResult(999) },
		func(r *SlotReport) { r.MaxParallel = -1 },
		func(r *SlotReport) { r.FirstArrival = -1 },
	} {
		bad := report
		mutate(&bad)
		s.observeSlotReport(bad)
		if s.lastSlot.Load() != nil {
			t.Fatal("invalid report retained an older known value")
		}
	}
}

func TestServiceObservationUsesExistingOwnerAndStops(t *testing.T) {
	var latest atomic.Pointer[ServiceObservation]
	var calls atomic.Uint64
	h := startServiceWithObserver(t, func(o ServiceObservation) {
		latest.Store(&o)
		calls.Add(1)
	})
	eventually(t, func() bool {
		o := latest.Load()
		return o != nil && o.BlockEngineHealthy && o.RelayerConfigured && o.RelayerUsable &&
			o.RelayerTPUAdvertised && o.HarmonicClientIDAdvertised && o.BlockEngine.Sessions == 1 && o.Relayer.Sessions == 1
	}, "existing metrics owner published actual session state")
	h.service.executor.cfg.OnReport(SlotReport{Slot: 123, Mode: ModeFallback, Flags: FlagUnavailable})
	eventually(t, func() bool { o := latest.Load(); return o != nil && o.HasLastSlot && o.LastSlot.Slot == 123 }, "completed report published")
	h.stop()
	count := calls.Load()
	time.Sleep(3 * h.service.metricsInterval)
	if calls.Load() != count {
		t.Fatal("observation owner outlived service join")
	}
	var replacement atomic.Pointer[ServiceObservation]
	h2 := startServiceWithObserver(t, func(o ServiceObservation) { replacement.Store(&o) })
	eventually(t, func() bool { return replacement.Load() != nil }, "replacement service observed")
	o := replacement.Load()
	if o.HasLastSlot || o.StartedAt.Equal(latest.Load().StartedAt) || o.Pipeline.Transactions != 0 {
		t.Fatal("new service inherited prior measurements")
	}
	h2.stop()
	nilObserver := startService(t)
	if !nilObserver.service.observationStartedAt.IsZero() || nilObserver.service.lastSlot.Load() != nil ||
		reflect.ValueOf(nilObserver.service.executor.cfg.OnReport).Pointer() != reflect.ValueOf(reportSlot).Pointer() {
		t.Fatal("nil observer changed direct report behavior")
	}
	nilObserver.stop()
}
