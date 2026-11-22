package node

import (
	"encoding/json"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/spf13/viper"
)

func TestDashboardCgroupResourcesOptIn(t *testing.T) {
	f := Run.Flags().Lookup("dashboard-cgroup-resources")
	if f == nil {
		t.Fatal("missing explicit cgroup option")
	}
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
		t.Fatal("cgroup collection enabled by default")
	}
	if err := f.Value.Set("false"); err != nil {
		t.Fatal(err)
	}
	f.Changed = false
	resolve := func() bool {
		return resolveBoolOption(f, viper.IsSet("dashboard.cgroup_resources"), viper.GetBool("dashboard.cgroup_resources"))
	}
	if resolve() {
		t.Fatal("default resolution enabled cgroup collection")
	}
	viper.Set("dashboard.host_resources", true)
	if resolve() {
		t.Fatal("host option enabled cgroup collection")
	}
	viper.Set("dashboard.cgroup_resources", true)
	if !resolve() {
		t.Fatal("TOML opt-in ignored")
	}
	if err := Run.Flags().Set("dashboard-cgroup-resources", "false"); err != nil {
		t.Fatal(err)
	}
	if resolve() {
		t.Fatal("explicit CLI false did not override TOML")
	}
	for _, socket := range []string{"", " \t\n"} {
		if validateDashboardCgroupResources(true, socket) == nil || validateDashboardCgroupResources(false, socket) != nil {
			t.Fatal("cgroup private feed prerequisite incorrect")
		}
	}
	if validateDashboardCgroupResources(true, "/private/feed.sock") != nil {
		t.Fatal("valid private feed rejected")
	}
}

func TestDashboardCgroupResourcesDisabledAndOwner(t *testing.T) {
	var absent *nodeDashboard
	absent.configureCgroupResources(true)
	d := &nodeDashboard{}
	d.configureHostResources(true)
	d.configureCgroupResources(false)
	if d.cgroupSampler != nil {
		t.Fatal("disabled cgroup option allocated a sampler")
	}
	at := time.Now().UTC().Add(-time.Second)
	d.observeRuntime(at, runtime.MemStats{}, nil)
	if d.state.Resources.Cgroup != nil || !d.state.Resources.SampledAt.Equal(at) {
		t.Fatal("default process shape or timestamp changed")
	}
	d.configureCgroupResources(true)
	if d.cgroupSampler == nil {
		t.Fatal("explicit opt-in did not configure sampler")
	}
	before := time.Now()
	d.observeRuntime(at, runtime.MemStats{}, nil)
	after := time.Now()
	c := d.state.Resources.Cgroup
	if c == nil || c.SampledAt.Before(before) || c.SampledAt.After(after) || !d.state.Resources.SampledAt.Equal(at) {
		t.Fatal("owner reused scheduled/process time for cgroup capture")
	}
	if runtime.GOOS != "linux" && c.BindingState != "unsupported" {
		t.Fatal("unsupported platform fabricated a binding")
	}
}

func cgroupSourceFixture() metrics.CgroupObservation {
	start := time.Now()
	end := start.Add(200 * time.Millisecond)
	known := metrics.CgroupValue{Value: math.MaxUint64, Known: true}
	zero := metrics.CgroupValue{Known: true}
	line := metrics.CgroupPressureLine{Average10: 0, Average60: 50, Average300: 100, TotalMicroseconds: math.MaxUint64}
	counters := metrics.CgroupCounters{CPUUsage: known, CPUUser: known, CPUSystem: known, Periods: known,
		ThrottledPeriods: known, ThrottledMicroseconds: known, MemoryHigh: known, MemoryMax: known, OOM: zero}
	return metrics.CgroupObservation{StartedAt: start, SampledAt: end, HasBinding: true,
		CPUQuota:      metrics.CgroupCPUQuota{Quota: metrics.CgroupLimit{State: metrics.CgroupLimitUnlimited}, PeriodMicroseconds: 100000},
		EffectiveCPUs: metrics.CgroupValue{Value: math.MaxUint32, Known: true}, MemoryCurrent: known, SwapCurrent: zero,
		MemoryHigh: metrics.CgroupLimit{State: metrics.CgroupLimitFinite}, MemoryMax: metrics.CgroupLimit{State: metrics.CgroupLimitUnlimited},
		Counters: counters, HasWindow: true,
		Window:   metrics.CgroupWindow{StartedAt: end.Add(-10 * time.Second), EndedAt: end, Seconds: 10, Counters: counters},
		Pressure: [3]metrics.CgroupPressure{{Some: line, Full: line, HasSome: true, HasFull: true}, {Some: line, HasSome: true}, {Full: line, HasFull: true}}}
}

func TestCgroupResourcesMeasurement(t *testing.T) {
	o := cgroupSourceFixture()
	c := cgroupResourcesMeasurement(o)
	if c == nil || c.CaptureDurationSeconds != .2 || c.SampledAt.Location() != time.UTC || c.CaptureStartedAt.Location() != time.UTC || c.BindingState != "known" || c.Window == nil || c.Window.DurationSeconds != 10 {
		t.Fatalf("incorrect capture mapping: %+v", c)
	}
	if c.CPUQuota.State != "max" || c.CPUQuota.Value != nil || *c.CPUPeriodMicroseconds != 100000 || *c.EffectiveCPUs != math.MaxUint32 || *c.MemoryCurrentBytes != math.MaxUint64 || *c.SwapCurrentBytes != 0 || c.MemoryHighBytes.State != "finite" || *c.MemoryHighBytes.Value != 0 || c.SwapMaxBytes.State != "unknown" {
		t.Fatal("raw settings or known zero lost")
	}
	if *c.Counters.OOM != 0 || c.Counters.OOMKill != nil || c.Pressure.Memory.Full != nil || c.Pressure.IO.Some != nil || c.Pressure.CPU.Full.TotalMicroseconds != math.MaxUint64 {
		t.Fatal("independent source availability flags lost")
	}
	b, err := json.Marshal(c)
	if err != nil || strings.Contains(string(b), "/proc") || strings.Contains(string(b), "mount") || !strings.Contains(string(b), `"memory_current_bytes":"18446744073709551615"`) {
		t.Fatalf("private allowlist or exact integer failed: %s, %v", b, err)
	}
	o.MemoryCurrent.Value, o.Pressure[0].Full.TotalMicroseconds, o.Window.Counters.CPUUsage.Value = 0, 0, 0
	if *c.MemoryCurrentBytes != math.MaxUint64 || c.Pressure.CPU.Full.TotalMicroseconds != math.MaxUint64 || *c.Window.Counters.CPUUsageMicroseconds != math.MaxUint64 {
		t.Fatal("adapter retained mutable source")
	}
	o = cgroupSourceFixture()
	o.Window.StartedAt = o.SampledAt.Add(time.Second)
	if c := cgroupResourcesMeasurement(o); !c.Window.WindowStart.Equal(o.Window.StartedAt) || c.Window.DurationSeconds != 10 {
		t.Fatal("wall dates replaced monotonic window duration")
	}
	for _, elapsed := range []time.Duration{0, time.Second, time.Second + time.Nanosecond, -time.Nanosecond} {
		o := cgroupSourceFixture()
		o.SampledAt = o.StartedAt.Add(elapsed)
		c := cgroupResourcesMeasurement(o)
		if elapsed < 0 {
			if c != nil {
				t.Fatal("negative capture duration accepted")
			}
			continue
		}
		if c == nil {
			t.Fatal("dated capture lost")
		}
		if elapsed > time.Second {
			if c.BindingState != "unavailable" || c.MemoryCurrentBytes != nil || c.Window != nil || c.Counters != (dashboard.CgroupCounters{}) || c.Pressure != (dashboard.CgroupPressureScopes{}) {
				t.Fatal("over-budget values retained")
			}
		} else if c.BindingState != "known" || c.MemoryCurrentBytes == nil {
			t.Fatal("inclusive capture budget rejected")
		}
	}
}

func TestCgroupResourcesUnknownAndUnsupported(t *testing.T) {
	for _, unsupported := range []bool{false, true} {
		o := cgroupSourceFixture()
		o.HasBinding, o.UnsupportedLayout = false, unsupported
		c := cgroupResourcesMeasurement(o)
		want := "unavailable"
		if unsupported {
			want = "unsupported"
		}
		if c == nil || c.BindingState != want || c.Counters != (dashboard.CgroupCounters{}) || c.Window != nil || c.Pressure != (dashboard.CgroupPressureScopes{}) || c.CPUQuota.State != "unknown" || c.MemoryCurrentBytes != nil {
			t.Fatal("unbound observation produced measurements")
		}
	}
	o := cgroupSourceFixture()
	o.StartedAt = time.Time{}
	if cgroupResourcesMeasurement(o) != nil {
		t.Fatal("undated source accepted")
	}
	if c := sampleCgroupResources(nil); c != nil {
		t.Fatal("disabled owner captured")
	}
	if allocations := testing.AllocsPerRun(100, func() { sampleCgroupResources(nil) }); allocations != 0 {
		t.Fatalf("disabled owner allocated: %v", allocations)
	}
	if runtime.GOOS != "linux" {
		before := time.Now()
		c := sampleCgroupResources(metrics.NewCgroupSampler(true))
		after := time.Now()
		if c == nil || c.BindingState != "unsupported" || c.SampledAt.Before(before) || c.SampledAt.After(after) || !c.CaptureStartedAt.Equal(c.SampledAt) || c.CaptureDurationSeconds != 0 {
			t.Fatal("enabled unsupported OS did not date its attempt")
		}
	}
	if got := cgroupLimitMeasurement(metrics.CgroupLimit{State: 255, Value: 42}); !reflect.DeepEqual(got, dashboard.CgroupLimit{State: "unknown"}) {
		t.Fatal("unknown source limit exposed a value")
	}
}

func TestCgroupCounterMeasurementPreservesFields(t *testing.T) {
	var values [10]metrics.CgroupValue
	for i := range values {
		values[i] = metrics.CgroupValue{Value: uint64(i + 1), Known: true}
	}
	source := metrics.CgroupCounters{CPUUsage: values[0], CPUUser: values[1], CPUSystem: values[2], Periods: values[3],
		ThrottledPeriods: values[4], ThrottledMicroseconds: values[5], MemoryHigh: values[6], MemoryMax: values[7], OOM: values[8], OOMKill: values[9]}
	b, err := json.Marshal(cgroupCountersMeasurement(source))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"cpu_usage_microseconds": "1", "cpu_user_microseconds": "2", "cpu_system_microseconds": "3", "periods": "4", "throttled_periods": "5", "throttled_microseconds": "6", "memory_high": "7", "memory_max": "8", "oom": "9", "oom_kill": "10"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("counter mapping: got %v, want %v", got, want)
	}
}

func BenchmarkCgroupResourcesDisabled(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if sampleCgroupResources(nil) != nil {
			b.Fatal("disabled capture returned data")
		}
	}
}
