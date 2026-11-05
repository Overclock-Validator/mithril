package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func hostFixture(at time.Time) *HostResources {
	maximum := uint64(math.MaxUint64)
	observed := at.Add(-100 * time.Millisecond)
	return &HostResources{SampledAt: at, CaptureStartedAt: at.Add(-200 * time.Millisecond), CaptureDurationSeconds: .2,
		CPU: &HostCPU{ObservedAt: observed, OnlineCPUs: math.MaxUint32, Window: &HostCPUWindow{
			WindowStart: observed.Add(-10 * time.Second), WindowEnd: observed, DurationSeconds: 10,
			Percent: [8]float64{12.5, 12.5, 12.5, 12.5, 12.5, 12.5, 12.5, 12.5}, BusyPercent: 62.5}},
		Load: &HostLoad{One: 0, Five: 1.25, Fifteen: 2.5, Runnable: maximum, Tasks: maximum},
		Memory: &HostMemory{TotalBytes: maximum, FreeBytes: 0, AvailableBytes: &maximum, BuffersBytes: &maximum,
			CachedBytes: &maximum, ReclaimableBytes: &maximum, SwapTotalBytes: &maximum, SwapFreeBytes: &maximum,
			DirtyBytes: &maximum, WritebackBytes: &maximum}}
}

func TestHostProjectionAllowlistAndDetachment(t *testing.T) {
	at := time.Now().UTC()
	s := resourceFixture(at)
	s.Resources.Host = hostFixture(at)
	p := publicResources(&s, "fresh", at).Host
	if p == nil || p.State != "fresh" || p.Scope != "visible_host" || p.CPU == nil || p.CPU.Window == nil || p.Load == nil || p.Memory == nil {
		t.Fatalf("missing host scopes: %+v", p)
	}
	b, err := json.Marshal(p)
	if err != nil || !strings.Contains(string(b), `"tasks":"18446744073709551615"`) || !strings.Contains(string(b), `"online_cpus":4294967295`) || !strings.Contains(string(b), `"free_bytes":"0"`) {
		t.Fatalf("host precision: %s (%v)", b, err)
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(b, &keys); err != nil || len(keys) != 7 {
		t.Fatalf("host allowlist: %s (%v)", b, err)
	}
	for _, name := range []string{"state", "sampled_at", "capture_started_at", "scope", "cpu", "load", "memory"} {
		if _, ok := keys[name]; !ok {
			t.Fatalf("missing %s", name)
		}
	}
	for _, secret := range []string{"capture_duration_seconds", "ticks", "/proc", "/sys", "error"} {
		if strings.Contains(string(b), secret) {
			t.Fatalf("private field %q: %s", secret, b)
		}
	}
	s.Resources.Host.CPU.Window.Percent[0] = 0
	*s.Resources.Host.Memory.AvailableBytes = 0
	s.Resources.Host.Load.Tasks = 1
	if p.CPU.Window.Percent[0] != 12.5 || *p.Memory.AvailableBytes != math.MaxUint64 || p.Load.Tasks != math.MaxUint64 {
		t.Fatal("public host aliases private values")
	}
}

func TestHostCaptureBudgetAndUnknownScopes(t *testing.T) {
	at := time.Now().UTC()
	s := resourceFixture(at)
	for _, seconds := range []float64{0, 1, 1.000000001, -1, math.NaN(), math.Inf(1)} {
		h := hostFixture(at)
		h.CaptureDurationSeconds = seconds
		p := publicHostResources(&s, h, "fresh", at)
		if seconds < 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
			if p != nil {
				t.Fatalf("invalid capture %v accepted", seconds)
			}
			continue
		}
		if p == nil || p.State != "fresh" {
			t.Fatalf("capture %v lost dated attempt", seconds)
		}
		if seconds > 1 {
			if p.CPU != nil || p.Load != nil || p.Memory != nil {
				t.Fatal("over-budget capture refreshed values")
			}
		} else if p.CPU == nil || p.Load == nil || p.Memory == nil {
			t.Fatal("inclusive budget lost values")
		}
	}
	h := hostFixture(at)
	h.CPU, h.Load, h.Memory = nil, nil, nil
	p := publicHostResources(&s, h, "fresh", at)
	if p == nil || p.State != "fresh" || p.CPU != nil || p.Load != nil || p.Memory != nil {
		t.Fatal("all-unknown attempt fabricated values")
	}
	h = hostFixture(at)
	zero := uint64(0)
	h.Memory.AvailableBytes, h.Memory.BuffersBytes = &zero, nil
	h.Load.One, h.Load.Five, h.Load.Fifteen, h.Load.Runnable = 0, 0, 0, 0
	p = publicHostResources(&s, h, "fresh", at)
	if p.Memory.AvailableBytes == nil || *p.Memory.AvailableBytes != 0 || p.Memory.BuffersBytes != nil || p.Load == nil || p.Load.One != 0 {
		t.Fatal("zero confused with unknown")
	}
	for _, mutate := range []func(*HostResources){
		func(h *HostResources) { h.Load.One = math.NaN() },
		func(h *HostResources) { h.Load.Tasks = 0 },
		func(h *HostResources) { h.Load.Tasks = 1 },
	} {
		h = hostFixture(at)
		mutate(h)
		p = publicHostResources(&s, h, "fresh", at)
		if p.Load != nil || p.CPU == nil || p.Memory == nil {
			t.Fatal("bad load contaminated other scopes")
		}
	}
	for _, mutate := range []func(*HostResources){
		func(h *HostResources) { h.Memory.TotalBytes = 0 },
		func(h *HostResources) { h.Memory.TotalBytes = 1; h.Memory.FreeBytes = 2 },
		func(h *HostResources) { h.Memory.TotalBytes = 1 },
		func(h *HostResources) { h.Memory.SwapTotalBytes = &zero },
	} {
		h = hostFixture(at)
		mutate(h)
		p = publicHostResources(&s, h, "fresh", at)
		if p.Memory != nil || p.CPU == nil || p.Load == nil {
			t.Fatal("bad memory contaminated other scopes")
		}
	}
}

func TestHostCPUWindowValidation(t *testing.T) {
	at := time.Now().UTC()
	s := resourceFixture(at)
	for _, mutate := range []func(*HostCPUWindow){
		func(w *HostCPUWindow) { w.DurationSeconds = 0 },
		func(w *HostCPUWindow) { w.DurationSeconds = 30.000000001 },
		func(w *HostCPUWindow) { w.DurationSeconds = math.NaN() },
		func(w *HostCPUWindow) { w.BusyPercent = math.Inf(1) },
		func(w *HostCPUWindow) { w.BusyPercent++ },
		func(w *HostCPUWindow) { w.Percent[0] = -1 },
		func(w *HostCPUWindow) { w.Percent[0] = 101 },
		func(w *HostCPUWindow) { w.Percent[0] = math.NaN() },
		func(w *HostCPUWindow) { w.Percent[0]++ },
		func(w *HostCPUWindow) { w.WindowStart = time.Time{} },
		func(w *HostCPUWindow) { w.WindowEnd = w.WindowEnd.Add(time.Nanosecond) },
	} {
		h := hostFixture(at)
		mutate(h.CPU.Window)
		p := publicHostResources(&s, h, "fresh", at)
		if p.CPU == nil || p.CPU.Window != nil || p.Load == nil {
			t.Fatal("bad CPU rate retained or lost independent scope")
		}
	}
	h := hostFixture(at)
	h.CPU.Window.WindowStart = at // wall reversal does not invalidate a monotonic interval
	h.CPU.Window.DurationSeconds = 30
	p := publicHostResources(&s, h, "fresh", at)
	if p.CPU.Window == nil || p.CPU.Window.DurationSeconds != 30 {
		t.Fatal("UTC subtraction overrode source interval")
	}
	h.CPU.Window = nil
	if p := publicHostResources(&s, h, "fresh", at); p.CPU == nil || p.CPU.Window != nil {
		t.Fatal("warmup count lost")
	}
	h.CPU.OnlineCPUs = 0
	if p := publicHostResources(&s, h, "fresh", at); p.CPU != nil {
		t.Fatal("zero online count accepted")
	}
}

func TestHostIndependentClocksAndFreshness(t *testing.T) {
	at := time.Now().UTC()
	s := resourceFixture(at)
	s.Resources.Host = hostFixture(at)
	for _, processAt := range []time.Time{at.Add(-31 * time.Second), {}, at.Add(2 * time.Second)} {
		s.Resources.SampledAt = processAt
		p := publicResources(&s, "fresh", at)
		if p.Host == nil || p.Host.State != "fresh" || p.Host.CPU.Window == nil {
			t.Fatal("process timestamp redated/erased host sample")
		}
	}
	s.Resources.SampledAt = at
	s.Resources.Host = hostFixture(at.Add(-31 * time.Second))
	p := publicResources(&s, "fresh", at)
	if p.State != "fresh" || p.Host.State != "stale" || p.Host.CPU.Window != nil {
		t.Fatal("process freshness refreshed host rate")
	}
	for _, age := range []time.Duration{30 * time.Second, 30*time.Second + time.Nanosecond} {
		h := hostFixture(at)
		h.CPU.ObservedAt = at.Add(-age)
		h.CPU.Window.WindowEnd, h.CPU.Window.WindowStart = h.CPU.ObservedAt, h.CPU.ObservedAt.Add(-10*time.Second)
		p := publicHostResources(&s, h, "fresh", at)
		if (p.CPU.Window != nil) != (age <= 30*time.Second) {
			t.Fatal("CPU own-age boundary changed")
		}
	}
	s.Resources.Host = hostFixture(at)
	p = publicResources(&s, "stale", at)
	if p.Host.State != "stale" || p.Host.CPU.Window != nil {
		t.Fatal("stale source retained rate")
	}
	for _, mutate := range []func(*Snapshot, *HostResources){
		func(s *Snapshot, _ *HostResources) { s.RunID = "" },
		func(s *Snapshot, h *HostResources) { h.SampledAt = s.StartedAt.Add(-time.Nanosecond) },
		func(s *Snapshot, h *HostResources) { h.CaptureStartedAt = s.StartedAt.Add(-time.Nanosecond) },
		func(_ *Snapshot, h *HostResources) { h.SampledAt = at.Add(time.Second + time.Nanosecond) },
		func(s *Snapshot, h *HostResources) {
			s.SampledAt = at.Add(-time.Second)
			h.SampledAt = at.Add(time.Nanosecond)
		},
		func(s *Snapshot, h *HostResources) {
			s.SampledAt = at.Add(time.Second)
			h.SampledAt = at.Add(2 * time.Second)
		},
	} {
		s := resourceFixture(at)
		h := hostFixture(at)
		mutate(&s, h)
		if publicHostResources(&s, h, "fresh", at) != nil {
			t.Fatal("run or absolute future cap bypassed")
		}
	}
	h := hostFixture(at)
	h.CPU.ObservedAt = at.Add(time.Second + time.Nanosecond)
	if p := publicHostResources(&s, h, "fresh", at); p.CPU != nil || p.Load == nil {
		t.Fatal("CPU future cap bypassed")
	}
}

func TestHostCollectorOptInAndSourceLoss(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	defer c.client.CloseIdleConnections()
	at := time.Now().UTC()
	s := resourceFixture(at)
	s.Resources.Host = hostFixture(at)
	setFixture(t, body, s)
	read := func() PublicSnapshot {
		t.Helper()
		var p PublicSnapshot
		if err := json.Unmarshal(c.latest.Load().publicJSON, &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	c.poll(context.Background(), at)
	if read().Resources != nil {
		t.Fatal("private opt-in bypassed public resource gate")
	}
	c.publicOptions.Resources = true
	c.poll(context.Background(), at)
	if p := read(); p.Resources.Host == nil || p.Resources.Host.CPU.Window == nil {
		t.Fatal("enabled host absent")
	}
	body.Store(nil)
	c.poll(context.Background(), at.Add(time.Second))
	if p := read(); p.Resources.Host.State != "stale" || p.Resources.Host.CPU.Window != nil || p.Resources.Host.Load == nil {
		t.Fatal("source loss concealed history or retained rate")
	}
	c.last = nil
	c.poll(context.Background(), at.Add(2*time.Second))
	if read().Resources.Host != nil {
		t.Fatal("unavailable source retained host")
	}
	s.Resources.Host = nil
	setFixture(t, body, s)
	c.poll(context.Background(), at)
	b, _ := json.Marshal(read().Resources)
	if strings.Contains(string(b), `"host"`) {
		t.Fatal("old producer shape changed")
	}
}
