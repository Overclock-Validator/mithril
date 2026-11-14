package node

import (
	"math"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

func TestDashboardInterfaceTrafficMappingAndLifetime(t *testing.T) {
	at := time.Now()
	o := metrics.InterfaceTrafficObservation{StartedAt: at, SampledAt: at.Add(time.Millisecond), State: "available", Count: 1, TotalInterfaces: 1}
	o.Interfaces[0] = metrics.InterfaceTrafficCounters{Index: math.MaxUint32, Loopback: true, MTU: 1500, ReceiveBytes: math.MaxUint64, TransmitBytes: math.MaxUint64,
		ReceivePackets: math.MaxUint64, TransmitPackets: math.MaxUint64, FailuresAvailable: true,
		ReceiveErrors: 3, ReceiveDropped: 4, TransmitErrors: 11, TransmitDropped: math.MaxUint64}
	d, err := interfaceTrafficMeasurement(o)
	if err != nil || len(d.Interfaces) != 1 || d.Interfaces[0].MTU != 1500 || d.Interfaces[0].ReceiveBytes != math.MaxUint64 || !d.Interfaces[0].Loopback ||
		d.Interfaces[0].Failures == nil || d.Interfaces[0].Failures.ReceiveDropped != 4 || d.Interfaces[0].Failures.TransmitDropped != math.MaxUint64 {
		t.Fatal("mapping lost exact counters or role")
	}
	unknown := o
	unknown.Interfaces[0].FailuresAvailable = false
	if mapped, err := interfaceTrafficMeasurement(unknown); err != nil || mapped.Interfaces[0].Failures != nil {
		t.Fatal("an older observation invented failure counters")
	}
	zero := o
	zero.Interfaces[0].ReceiveErrors, zero.Interfaces[0].ReceiveDropped = 0, 0
	zero.Interfaces[0].TransmitErrors, zero.Interfaces[0].TransmitDropped = 0, 0
	if mapped, err := interfaceTrafficMeasurement(zero); err != nil || mapped.Interfaces[0].Failures == nil || *mapped.Interfaces[0].Failures != (dashboard.InterfaceTrafficFailures{}) {
		t.Fatal("measured zero failure counters became unknown")
	}
	binding := dashboard.NodeNetworkBinding{RunID: "node-run", NodeStartedAt: at.Add(-time.Minute).UTC()}
	deadline := at.Add(dashboard.NetworkDetailFreshFor)
	m := &interfaceTrafficMonitor{}
	m.latest.Store(&interfaceTrafficObservation{binding: binding, detail: d, freshUntil: deadline})
	first, end, reason := m.capture(binding, at.Add(time.Second))
	if reason != "" || !end.Equal(deadline) || !reflect.DeepEqual(first, &d) {
		t.Fatal("cached observation was unavailable or redated")
	}
	first.Interfaces[0].ReceiveBytes, *first.TotalInterfaces = 0, 0
	first.Interfaces[0].Failures.ReceiveDropped = 0
	again, end, reason := m.capture(binding, at.Add(2*time.Second))
	if reason != "" || again.Interfaces[0].ReceiveBytes != math.MaxUint64 || again.Interfaces[0].Failures.ReceiveDropped != 4 || *again.TotalInterfaces != 1 || !end.Equal(deadline) {
		t.Fatal("caller mutated immutable cache or renewed the source lease")
	}
	other := binding
	other.RunID = "replacement"
	if _, _, reason := m.capture(other, at.Add(time.Second)); reason != "binding_changed" {
		t.Fatal("new run accepted old counters")
	}
	if _, _, reason := m.capture(binding, deadline); reason != "source_time_invalid" {
		t.Fatal("expired counters stayed current")
	}
	m.stop()
	if _, _, reason := m.capture(binding, at.Add(time.Second)); reason != "source_unavailable" {
		t.Fatal("stopped owner remained readable")
	}
	m.observe(binding)
	if m.latest.Load() != nil {
		t.Fatal("retired owner sampled or republished")
	}
	for _, state := range []string{"read_failed", "unsupported", "invalid_record", "binding_changed", "capture_expired", "clock_invalid"} {
		failed := metrics.InterfaceTrafficObservation{StartedAt: at, SampledAt: at, State: state}
		mapped, err := interfaceTrafficMeasurement(failed)
		if err != nil || mapped.TotalInterfaces != nil || mapped.Interfaces == nil || len(mapped.Interfaces) != 0 {
			t.Fatal("failure became measured zero or null array")
		}
		failed.Interfaces[0].ReceiveBytes = 1
		if _, err := interfaceTrafficMeasurement(failed); err == nil {
			t.Fatal("failure carried partial counters")
		}
	}
}

func TestDashboardInterfaceTrafficRuntimeOptInAndIndependentRole(t *testing.T) {
	for _, role := range []string{"validator", "verifying"} {
		d := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: role, StartedAt: time.Now().Add(-time.Minute).UTC(), Mode: role}}
		d.configureHostResources(true)
		if d.interfaces == nil {
			t.Fatal("joint network/host opt-in did not configure an owner")
		}
		d.observeRuntime(time.Now(), runtime.MemStats{}, nil)
		capture, err := d.sampleNodeNetwork(time.Now())
		if err != nil || capture.Detail.Receiver != nil || capture.Detail.Interfaces == nil || capture.Detail.Interfaces.SampledAt.IsZero() ||
			capture.FreshUntil.IsZero() || capture.Detail.InterfacesReason != "" {
			t.Fatalf("%s interface-only runtime: %v", role, err)
		}
		if runtime.GOOS != "linux" && (capture.Detail.Interfaces.State != "unsupported" || capture.Detail.Interfaces.TotalInterfaces != nil) {
			t.Fatal("unsupported platform became measured zero")
		}
		d.stopping()
		if _, err := d.sampleNodeNetwork(time.Now()); err == nil {
			t.Fatal("stopped node returned current host traffic")
		}
	}
	for _, enabled := range []bool{false, true} {
		d := &nodeDashboard{}
		d.configureHostResources(enabled)
		if d.interfaces != nil {
			t.Fatal("host flag alone enabled interface collection")
		}
	}
}
