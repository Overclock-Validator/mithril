package node

import (
	"encoding/json"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/spf13/viper"
)

func TestDashboardHostResourcesOptIn(t *testing.T) {
	f := Run.Flags().Lookup("dashboard-host-resources")
	oldValue, oldChanged := f.Value.String(), f.Changed
	t.Cleanup(func() {
		viper.Reset()
		if err := f.Value.Set(oldValue); err != nil {
			t.Error(err)
		}
		f.Changed = oldChanged
	})
	viper.Reset()
	if f.DefValue != "false" {
		t.Fatal("host collection enabled by default")
	}
	if err := f.Value.Set("false"); err != nil {
		t.Fatal(err)
	}
	f.Changed = false
	resolve := func() bool {
		return resolveBoolOption(f, viper.IsSet("dashboard.host_resources"), viper.GetBool("dashboard.host_resources"))
	}
	if resolve() {
		t.Fatal("default resolution enabled host collection")
	}
	viper.Set("dashboard.host_resources", true)
	if !resolve() {
		t.Fatal("TOML opt-in ignored")
	}
	if err := Run.Flags().Set("dashboard-host-resources", "false"); err != nil {
		t.Fatal(err)
	}
	if resolve() {
		t.Fatal("explicit CLI false did not override TOML")
	}
	for _, socket := range []string{"", " \t\n"} {
		if validateDashboardHostResources(true, socket) == nil {
			t.Fatal("host option accepted without private feed")
		}
		if validateDashboardHostResources(false, socket) != nil {
			t.Fatal("disabled option required a feed")
		}
	}
	if validateDashboardHostResources(true, "/private/feed.sock") != nil {
		t.Fatal("valid private feed rejected")
	}
}

func TestDashboardHostResourcesDisabledAndOwner(t *testing.T) {
	var absent *nodeDashboard
	absent.configureHostResources(true)
	d := &nodeDashboard{}
	d.configureHostResources(false)
	if d.hostSampler != nil {
		t.Fatal("disabled node allocated a sampler")
	}
	if allocations := testing.AllocsPerRun(100, func() { _, _ = d.hostSampler.Sample() }); allocations != 0 {
		t.Fatalf("disabled sample allocations: %v", allocations)
	}
	if observation, ok := d.hostSampler.Sample(); ok || observation != (metrics.HostObservation{}) {
		t.Fatal("disabled path observed the OS")
	}
	at := time.Now().UTC().Add(-time.Second)
	d.observeRuntime(at, runtime.MemStats{}, nil)
	if d.state.Resources.Host != nil || !d.state.Resources.SampledAt.Equal(at) {
		t.Fatal("default process shape or timestamp changed")
	}
	if runtime.GOOS != "linux" {
		d.configureHostResources(true)
		d.observeRuntime(at, runtime.MemStats{}, nil)
		if d.state.Resources.Host != nil {
			t.Fatal("unsupported host fabricated a capture")
		}
	}
}

func TestHostResourcesMeasurementCopiesSource(t *testing.T) {
	start := time.Now() // retain the source's monotonic component until duration is measured
	end := start.Add(200 * time.Millisecond)
	observed := start.Add(100 * time.Millisecond)
	o := metrics.HostObservation{StartedAt: start, SampledAt: end, HasCPU: true, HasLoad: true, HasMemory: true,
		CPU: metrics.HostCPU{ObservedAt: observed, OnlineCPUs: math.MaxUint32, Ticks: [8]uint64{12345}, HasRate: true,
			Rate: metrics.HostCPURate{StartedAt: observed.Add(time.Second), EndedAt: observed, Seconds: 10,
				Percent: [8]float64{12.5, 12.5, 12.5, 12.5, 12.5, 12.5, 12.5, 12.5}, BusyPercent: 62.5}},
		Load: metrics.HostLoad{One: 0, Five: 1.25, Fifteen: 2.5, Runnable: math.MaxUint64, Tasks: math.MaxUint64},
		Memory: metrics.HostMemory{TotalBytes: math.MaxUint64, FreeBytes: 0, AvailableBytes: 0, SwapTotalBytes: math.MaxUint64,
			HasAvailable: true, HasSwapTotal: true, HasSwapFree: true}}
	h := hostResourcesMeasurement(o)
	if h == nil || h.CaptureDurationSeconds != .2 || h.SampledAt.Location() != time.UTC || h.CPU == nil || h.CPU.Window == nil || h.Load == nil || h.Memory == nil {
		t.Fatalf("incomplete host source mapping: %+v", h)
	}
	if h.CPU.Window.DurationSeconds != 10 || !h.CPU.Window.WindowStart.Equal(o.CPU.Rate.StartedAt) || h.CPU.OnlineCPUs != math.MaxUint32 {
		t.Fatal("adapter reconstructed duration or altered source count")
	}
	if h.Memory.AvailableBytes == nil || *h.Memory.AvailableBytes != 0 || h.Memory.BuffersBytes != nil || h.Memory.SwapFreeBytes == nil || *h.Memory.SwapFreeBytes != 0 {
		t.Fatal("source availability flags lost")
	}
	b, err := json.Marshal(h)
	if err != nil || strings.Contains(string(b), "ticks") || strings.Contains(string(b), "12345") || !strings.Contains(string(b), `"tasks":"18446744073709551615"`) {
		t.Fatalf("private allowlist: %s, %v", b, err)
	}
	o.CPU.Rate.Percent[0], o.Memory.AvailableBytes, o.Load.Tasks = 0, 10, 1
	if h.CPU.Window.Percent[0] != 12.5 || *h.Memory.AvailableBytes != 0 || h.Load.Tasks != math.MaxUint64 {
		t.Fatal("host adapter aliases source")
	}
	for _, elapsed := range []time.Duration{0, time.Second, time.Second + time.Nanosecond, -time.Nanosecond} {
		o.SampledAt = start.Add(elapsed)
		got := hostResourcesMeasurement(o)
		if elapsed < 0 {
			if got != nil {
				t.Fatal("reversed source capture accepted")
			}
			continue
		}
		if got == nil {
			t.Fatal("dated capture lost")
		}
		if elapsed > time.Second {
			if got.CPU != nil || got.Load != nil || got.Memory != nil {
				t.Fatal("over-budget source values retained")
			}
		} else if got.CPU == nil || got.Load == nil || got.Memory == nil {
			t.Fatal("inclusive budget rejected")
		}
	}
	o.SampledAt = end
	o.HasCPU, o.HasLoad, o.HasMemory = false, false, false
	if h := hostResourcesMeasurement(o); h == nil || h.CPU != nil || h.Load != nil || h.Memory != nil {
		t.Fatal("unknown capture fabricated scopes")
	}
	o.StartedAt = time.Time{}
	if hostResourcesMeasurement(o) != nil {
		t.Fatal("undated capture accepted")
	}
}
