package node

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/spf13/viper"
)

func networkSourceFixture(now time.Time) (dashboard.NetworkDetailBinding, blockstream.NetworkSnapshot) {
	start := now.Add(-time.Hour).UTC()
	b := dashboard.NetworkDetailBinding{RunID: "network-run", NodeStartedAt: start}
	o := metrics.SocketObservation{StartedAt: now.Add(-time.Millisecond), SampledAt: now, Known: true, Reason: metrics.SocketAvailable,
		Memory: metrics.SocketMemory{ReceiveAllocated: math.MaxUint32, ReceiveLimit: 1, SendAllocated: 2, SendLimit: 3,
			ForwardAllocated: 4, SendQueued: 5, OptionsAllocated: 6, Backlog: 7, Drops: 8}}
	s := blockstream.NetworkSnapshot{SourceStartedAt: start, ConfiguredSource: "turbine", LiveConnected: true,
		Turbine: &blockstream.TurbineObservation{SampledAt: now.UTC(), StartedAt: start, Session: math.MaxUint64,
			SocketMemoryObserved: true, SocketMemoryObservedAt: now,
			SocketMemory: turbine.ReceiverSocketMemory{Generation: math.MaxUint64,
				Live: turbine.SocketMemorySample{Observation: o, DropDelta: turbine.SocketDropDelta{Known: true, Drops: 8, StartedAt: now.Add(-time.Second), SampledAt: now}}}}}
	return b, s
}

func TestNetworkDetailArrivalTiming(t *testing.T) {
	now := time.Now()
	b, s := networkSourceFixture(now)
	a := turbine.ArrivalTimingSnapshot{SampledAt: now, LatestReceiptAt: now.Add(-time.Hour), RetainedSlots: 750,
		AdjacentPairs: 749, ReorderedPairs: 7, TotalInterval: time.Duration(math.MaxInt64)}
	s.Reception = &blockstream.ReceptionObservation{StartedAt: s.Turbine.StartedAt, Session: s.Turbine.Session,
		Progress: turbine.ReceptionSnapshot{ArrivalTiming: a}}
	c, err := networkDetailMeasurement(b, s, now)
	if err != nil || c.Detail.ArrivalTiming == nil {
		t.Fatalf("arrival projection: %+v %v", c, err)
	}
	want := dashboard.ArrivalTimingDetail{Scope: "retained_adjacent_tvu_arrivals", SampledAt: a.SampledAt.UTC(),
		LatestReceiptAt: a.LatestReceiptAt.UTC(), RetainedSlots: 750, AdjacentPairs: 749, ReorderedPairs: 7, TotalIntervalNanoseconds: math.MaxInt64}
	if *c.Detail.ArrivalTiming != want || c.FreshUntil != now.Add(30*time.Second) {
		t.Fatal("projection changed original clocks/counts or socket freshness")
	}
	for _, mutate := range []func(*blockstream.ReceptionObservation){
		func(r *blockstream.ReceptionObservation) { r.Session-- },
		func(r *blockstream.ReceptionObservation) { r.StartedAt = now },
		func(r *blockstream.ReceptionObservation) { r.Progress.ArrivalTiming.AdjacentPairs = 0 },
		func(r *blockstream.ReceptionObservation) {
			r.Progress.ArrivalTiming.SampledAt = now.Add(-3 * time.Second)
		},
		func(r *blockstream.ReceptionObservation) {
			r.Progress.ArrivalTiming.SampledAt = now.Add(time.Nanosecond)
		},
		func(r *blockstream.ReceptionObservation) { r.Progress.ArrivalTiming.SampledAt = now.UTC() },
	} {
		copy := *s.Reception
		mutate(&copy)
		copySource := s
		copySource.Reception = &copy
		next, err := networkDetailMeasurement(b, copySource, now)
		if err != nil || next.Detail.ArrivalTiming != nil || next.Detail.Live.Memory == nil {
			t.Fatalf("unknown timing affected independent socket data: %+v %v", next, err)
		}
	}
	s.Reception.Progress.ArrivalTiming.LatestReceiptAt = now
	if c.Detail.ArrivalTiming.LatestReceiptAt != want.LatestReceiptAt {
		t.Fatal("source mutation changed detached timing")
	}
}

func TestNetworkDetailReadByteMapping(t *testing.T) {
	now := time.Now()
	b, s := networkSourceFixture(now)
	a := turbine.SocketReadActivity{Enabled: true, StartedAt: now.Add(-time.Minute), SampledAt: now,
		LiveKnown: true, LiveBytes: math.MaxUint64 - 1}
	a.LiveWindow = turbine.SocketReadWindow{Known: true, StartedAt: now.Add(-time.Second), SampledAt: now, Duration: time.Second, Bytes: 10}
	s.Turbine.SocketMemory.ReadActivity = a
	c, err := networkDetailMeasurement(b, s, now)
	if err != nil || c.Detail.Live.ReadBytes == nil || c.Detail.Live.ReadBytes.Bytes != math.MaxUint64-1 || c.Detail.Repair.ReadBytes != nil {
		t.Fatalf("read-byte mapping: %+v, %v", c, err)
	}
	s.Turbine.SocketMemory.Live = turbine.SocketMemorySample{Observation: metrics.SocketObservation{Reason: metrics.SocketUnsupported}}
	s.Turbine.SocketMemory.ReadActivity.LiveBytes = 0
	s.Turbine.SocketMemory.ReadActivity.LiveWindow = turbine.SocketReadWindow{}
	c, err = networkDetailMeasurement(b, s, now)
	if err != nil || c.Detail.Live.Memory != nil || c.Detail.Live.ReadBytes == nil || c.Detail.Live.ReadBytes.Bytes != 0 || c.Detail.Live.State != "unsupported" {
		t.Fatalf("SO_MEMINFO availability hid a known byte zero: %+v, %v", c, err)
	}
	s.Turbine.SocketMemory.ReadActivity.LiveKnown = false
	c, err = networkDetailMeasurement(b, s, now)
	if err != nil || c.Detail.Live.ReadBytes != nil {
		t.Fatal("unknown/saturated count became zero")
	}
}

func TestNetworkDetailRepairRequestWindow(t *testing.T) {
	now := time.Now()
	b, s := networkSourceFixture(now)
	s.Turbine.SocketMemory.RepairEnabled = true
	s.Turbine.SocketMemory.Repair = s.Turbine.SocketMemory.Live
	s.Turbine.SocketMemory.RepairRequests = turbine.RepairRequestActivity{Enabled: true, Known: true,
		StartedAt: now.Add(-time.Minute), SampledAt: now, Bytes: 20,
		Window: turbine.SocketReadWindow{Known: true, StartedAt: now.Add(-time.Second), SampledAt: now, Duration: time.Second, Bytes: 7}}
	c, err := networkDetailMeasurement(b, s, now)
	if err != nil || c.Detail.RepairRequests == nil || c.Detail.RepairRequests.Window == nil ||
		c.Detail.RepairRequests.Window.Bytes != 7 || c.Detail.RepairRequests.Window.DurationNanoseconds != uint64(time.Second) {
		t.Fatalf("repair request projection: %+v %v", c.Detail.RepairRequests, err)
	}
	for name, change := range map[string]func(*turbine.RepairRequestActivity){
		"unknown total":     func(a *turbine.RepairRequestActivity) { a.Known, a.Bytes = false, 0 },
		"bad amount":        func(a *turbine.RepairRequestActivity) { a.Window.Bytes = 21 },
		"zero duration":     func(a *turbine.RepairRequestActivity) { a.Window.Duration = 0 },
		"mismatched sample": func(a *turbine.RepairRequestActivity) { a.Window.SampledAt = now.Add(-time.Second) },
		"incomplete window": func(a *turbine.RepairRequestActivity) { a.Window.Known = false },
	} {
		t.Run(name, func(t *testing.T) {
			bad := s
			copied := *s.Turbine
			bad.Turbine = &copied
			change(&bad.Turbine.SocketMemory.RepairRequests)
			if _, err := networkDetailMeasurement(b, bad, now); err == nil {
				t.Fatal("invalid repair interval was accepted")
			}
		})
	}
}

func TestNetworkDetailReconstructionDate(t *testing.T) {
	now := time.Now()
	b, s := networkSourceFixture(now)
	if c, err := networkDetailMeasurement(b, s, now); err != nil || c.Detail.Reconstruction != nil {
		t.Fatal("unknown event fabricated")
	}
	for _, slot := range []uint64{0, math.MaxUint64} {
		at := now.Add(-time.Hour)
		s.Turbine.Frontiers = turbine.ShredFrontiers{HasReconstructed: true, ReconstructedSlot: slot, ReconstructedAt: at}
		c, err := networkDetailMeasurement(b, s, now)
		if err != nil || c.Detail.Reconstruction == nil || c.Detail.Reconstruction.Slot != slot || !c.Detail.Reconstruction.ObservedAt.Equal(at) {
			t.Fatalf("historical event changed: %+v %v", c.Detail.Reconstruction, err)
		}
		s.Turbine.SampledAt = now.Add(time.Second).UTC()
		next, err := networkDetailMeasurement(b, s, now)
		if err != nil || !next.Detail.Reconstruction.ObservedAt.Equal(at) {
			t.Fatal("fresh owner capture redated an old completion")
		}
	}
	s.Turbine.Frontiers.ReconstructedAt = time.Time{}
	if c, err := networkDetailMeasurement(b, s, now); err != nil || c.Detail.Reconstruction != nil {
		t.Fatal("legacy source without date fabricated event time")
	}
}

func TestNetworkDetailOwnerMapping(t *testing.T) {
	now := time.Now()
	b, source := networkSourceFixture(now)
	c, err := networkDetailMeasurement(b, source, now)
	if err != nil {
		t.Fatal(err)
	}
	want := &dashboard.SocketMemory{ReceiveAllocatedBytes: math.MaxUint32, ReceiveLimitBytes: 1, SendAllocatedBytes: 2,
		SendLimitBytes: 3, ForwardAllocatedBytes: 4, SendQueuedBytes: 5, OptionsAllocatedBytes: 6, BacklogBytes: 7, Drops: 8}
	if !reflect.DeepEqual(c.Detail.Live.Memory, want) || c.Detail.Live.DropWindow.DurationMilliseconds != 1000 || c.Detail.Live.DropWindow.Drops != 8 ||
		c.Detail.Binding.SocketGeneration != math.MaxUint64 || c.Detail.Binding.ReceiverSession != math.MaxUint64 ||
		c.Detail.Repair.Enabled || c.Detail.Repair.State != "not_provided" || c.Detail.Repair.Memory != nil || c.FreshUntil != now.Add(30*time.Second) {
		t.Fatalf("raw values or lifetime changed: %+v", c)
	}
	if c.FreshUntil == c.FreshUntil.UTC() {
		t.Fatal("monotonic freshness stripped")
	}
	source.Turbine.SocketMemory.Live.Observation.Memory = metrics.SocketMemory{}
	if !reflect.DeepEqual(c.Detail.Live.Memory, want) {
		t.Fatal("source mutation changed detached values")
	}
	body, err := json.Marshal(c.Detail)
	if err != nil || len(body) > dashboard.MaxNetworkDetailBytes {
		t.Fatalf("fixed body exceeded bound: %d %v", len(body), err)
	}
	t.Logf("TVU plus disabled repair private body: %d bytes", len(body))

	_, source = networkSourceFixture(now)
	source.Turbine.SocketMemory.RepairEnabled = true
	source.Turbine.SocketMemory.Repair = source.Turbine.SocketMemory.Live
	source.Turbine.SocketMemory.Repair.Observation.StartedAt = now.Add(-2 * time.Second)
	source.Turbine.SocketMemory.Repair.Observation.SampledAt = now.Add(-time.Second)
	source.Turbine.SocketMemory.Repair.DropDelta = turbine.SocketDropDelta{}
	c, err = networkDetailMeasurement(b, source, now)
	if err != nil || c.FreshUntil != now.Add(29*time.Second) {
		t.Fatal("document did not use oldest enabled socket deadline")
	}
}

func TestNetworkDetailOwnerUnavailableStatesAndClocks(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		reason metrics.SocketReason
		state  string
	}{
		{metrics.SocketUnsupported, "unsupported"}, {metrics.SocketClosed, "closed"}, {metrics.SocketReadFailed, "read_failed"},
		{metrics.SocketInvalidResponse, "invalid_response"}, {metrics.SocketCaptureExpired, "capture_expired"}, {metrics.SocketClockInvalid, "clock_invalid"},
	} {
		sample := turbine.SocketMemorySample{Observation: metrics.SocketObservation{Reason: tc.reason, StartedAt: now.Add(-time.Millisecond), SampledAt: now}}
		d, until, err := socketDetailMeasurement(sample, true, now, now)
		if err != nil || d.State != tc.state || d.Memory != nil || d.DropWindow != nil || until != now.Add(30*time.Second) {
			t.Fatalf("reason %v: %+v %v", tc.reason, d, err)
		}
	}
	unsupported := turbine.SocketMemorySample{Observation: metrics.SocketObservation{Reason: metrics.SocketUnsupported}}
	d, until, err := socketDetailMeasurement(unsupported, true, now, now)
	if err != nil || d.StartedAt != nil || d.SampledAt != nil || until != now.Add(30*time.Second) || until == until.UTC() {
		t.Fatal("unsupported fallback lost raw owner clock")
	}
	if _, _, err := socketDetailMeasurement(unsupported, true, time.Time{}, now); err == nil {
		t.Fatal("unsupported fabricated a clock")
	}
	for _, mutate := range []func(*turbine.SocketMemorySample){
		func(s *turbine.SocketMemorySample) { s.Observation.SampledAt = time.Time{} },
		func(s *turbine.SocketMemorySample) { s.Observation.StartedAt = now.Add(time.Second) },
		func(s *turbine.SocketMemorySample) { s.Observation.SampledAt = now.Add(time.Second) },
		func(s *turbine.SocketMemorySample) {
			s.Observation.StartedAt = now.Add(-31 * time.Second)
			s.Observation.SampledAt = now.Add(-30 * time.Second)
		},
		func(s *turbine.SocketMemorySample) { s.Observation.Known = false },
		func(s *turbine.SocketMemorySample) { s.DropDelta.SampledAt = now.Add(-time.Second) },
		func(s *turbine.SocketMemorySample) { s.DropDelta.Drops = 9 },
	} {
		_, source := networkSourceFixture(now)
		s := source.Turbine.SocketMemory.Live
		mutate(&s)
		if _, _, err := socketDetailMeasurement(s, true, now, now); err == nil {
			t.Fatal("invalid source clock/value accepted")
		}
	}
	b, before := networkSourceFixture(now)
	for _, mutate := range []func(*blockstream.NetworkSnapshot){
		func(s *blockstream.NetworkSnapshot) { s.LiveConnected = false },
		func(s *blockstream.NetworkSnapshot) { s.SourceStartedAt = now },
		func(s *blockstream.NetworkSnapshot) { s.Turbine.Session-- },
		func(s *blockstream.NetworkSnapshot) { s.Turbine.SocketMemory.Generation-- },
		func(s *blockstream.NetworkSnapshot) { s.Turbine.SocketMemoryObserved = false },
	} {
		_, after := networkSourceFixture(now)
		mutate(&after)
		if networkDetailSourceStable(before, after) {
			t.Fatal("changed source binding accepted")
		}
	}
	before.ConfiguredSource = "rpc"
	if _, err := networkDetailMeasurement(b, before, now); err == nil || err.Error() != "unsupported_source" {
		t.Fatal("non-turbine source claimed socket facts")
	}
}

func TestDashboardNetworkDetailsOptIn(t *testing.T) {
	if validateDashboardNetworkDetails(false, "") != nil || validateDashboardNetworkDetails(true, "/private/node.sock") != nil {
		t.Fatal("valid config rejected")
	}
	for _, socket := range []string{"", " \t"} {
		if validateDashboardNetworkDetails(true, socket) == nil {
			t.Fatal("network details accepted without feed")
		}
	}
	flag := Run.Flags().Lookup("dashboard-network-details")
	if flag == nil || flag.DefValue != "false" {
		t.Fatal("network details not default off")
	}
	oldValue, oldChanged := flag.Value.String(), flag.Changed
	t.Cleanup(func() {
		viper.Reset()
		if err := flag.Value.Set(oldValue); err != nil {
			t.Error(err)
		}
		flag.Changed = oldChanged
	})
	viper.Reset()
	if err := flag.Value.Set("false"); err != nil {
		t.Fatal(err)
	}
	flag.Changed = false
	resolve := func() bool {
		return resolveBoolOption(flag, viper.IsSet("dashboard.network_details"), viper.GetBool("dashboard.network_details"))
	}
	viper.Set("dashboard.host_resources", true)
	if resolve() {
		t.Fatal("unrelated option enabled socket reads")
	}
	viper.Set("dashboard.network_details", true)
	if !resolve() {
		t.Fatal("TOML opt-in ignored")
	}
	if err := Run.Flags().Set("dashboard-network-details", "false"); err != nil {
		t.Fatal(err)
	}
	if resolve() {
		t.Fatal("explicit CLI false ignored")
	}
	for _, enabled := range []bool{false, true} {
		dir, err := os.MkdirTemp("/tmp", "dashboard-network-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.RemoveAll(dir); err != nil {
				t.Error(err)
			}
		})
		d := startNodeDashboard(context.Background(), time.Now(), filepath.Join(dir, "node.sock"), "", "network-run", "isolated", "verifying", false, false, false, enabled, false, false, false, false)
		if d == nil {
			t.Fatal("dashboard failed")
		}
		if _, _, err := d.sampleNetworkDetail(time.Now()); err == nil || err.Error() != "waiting_source" {
			t.Fatal("unstarted source became known")
		}
		d.close()
	}
	var d *nodeDashboard
	if _, _, err := d.sampleNetworkDetail(time.Now()); err == nil {
		t.Fatal("nil dashboard became known")
	}
}
