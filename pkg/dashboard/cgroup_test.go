package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func cgroupFixture(at time.Time) *CgroupResources {
	n, online := uint64(math.MaxUint64), uint64(math.MaxUint32)
	c := CgroupCounters{}
	for _, field := range cgroupCounterFields(&c) {
		*field = &n
	}
	line := &CgroupPressureLine{Average10: 0, Average60: 50, Average300: 100, TotalMicroseconds: n}
	pressure := CgroupPressure{Some: line, Full: line}
	return &CgroupResources{SampledAt: at, CaptureStartedAt: at.Add(-200 * time.Millisecond), CaptureDurationSeconds: .2,
		BindingState: "known", CPUQuota: CgroupLimit{State: "finite", Value: &n}, CPUPeriodMicroseconds: &n, EffectiveCPUs: &online,
		MemoryCurrentBytes: &n, SwapCurrentBytes: &n, MemoryHighBytes: CgroupLimit{State: "finite", Value: &n},
		MemoryMaxBytes: CgroupLimit{State: "finite", Value: &n}, SwapMaxBytes: CgroupLimit{State: "max"}, Counters: c,
		Window:   &CgroupWindow{WindowStart: at.Add(-10 * time.Second), WindowEnd: at, DurationSeconds: 10, Counters: c},
		Pressure: CgroupPressureScopes{CPU: pressure, Memory: pressure, IO: pressure}}
}

func maximumCgroupResources(at time.Time) *CgroupResources {
	c := cgroupFixture(at)
	c.SwapMaxBytes = c.MemoryMaxBytes
	wide := 0.0000012345678901234567
	c.CaptureDurationSeconds, c.Window.DurationSeconds = wide, wide
	c.Pressure.CPU.Some.Average10, c.Pressure.CPU.Some.Average60, c.Pressure.CPU.Some.Average300 = wide, wide, wide
	return c
}

func TestCgroupProjectionAllowlistAndDetachment(t *testing.T) {
	at := time.Now().UTC()
	s, c := resourceFixture(at), cgroupFixture(at)
	p := publicCgroupResources(&s, c, "fresh", at)
	if p == nil || p.State != "fresh" || p.Scope != "visible_domain_cgroup" || p.BindingState != "known" || p.Window == nil || p.Pressure.CPU.Full == nil {
		t.Fatalf("missing cgroup capture: %+v", p)
	}
	b, err := json.Marshal(p)
	if err != nil || !strings.Contains(string(b), `"memory_current_bytes":"18446744073709551615"`) || !strings.Contains(string(b), `"effective_cpus":"4294967295"`) {
		t.Fatalf("cgroup precision: %s (%v)", b, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	keys := []string{"state", "sampled_at", "capture_started_at", "capture_duration_seconds", "scope", "binding_state", "cpu_quota", "cpu_period_microseconds", "effective_cpus", "memory_current_bytes", "swap_current_bytes", "memory_high_bytes", "memory_max_bytes", "swap_max_bytes", "counters", "window", "pressure"}
	if len(fields) != len(keys) {
		t.Fatalf("unexpected cgroup fields: %s", b)
	}
	for _, key := range keys {
		if _, ok := fields[key]; !ok {
			t.Fatalf("missing %s", key)
		}
	}
	for _, forbidden := range []string{"/proc", "/sys", "mount", "inode", "device", "membership", "directory", "pid", "error"} {
		if strings.Contains(string(b), forbidden) {
			t.Fatalf("private field %q in %s", forbidden, b)
		}
	}
	*c.MemoryCurrentBytes = 0
	c.Pressure.CPU.Some.Average10 = 99
	if *p.MemoryCurrentBytes != math.MaxUint64 || *p.CPUQuota.Value != math.MaxUint64 || *p.Counters.OOM != math.MaxUint64 || *p.Window.Counters.OOMKill != math.MaxUint64 || p.Pressure.CPU.Some.Average10 != 0 {
		t.Fatal("public cgroup aliases private values")
	}
	*p.EffectiveCPUs = 1
	if *c.EffectiveCPUs != math.MaxUint32 {
		t.Fatal("public mutation reached source")
	}
}

func TestCgroupProjectionZeroUnknownAndRawRelations(t *testing.T) {
	at := time.Now().UTC()
	s, c := resourceFixture(at), cgroupFixture(at)
	zero, usage, user, system := uint64(0), uint64(30314), uint64(9094), uint64(21219)
	c.Counters.CPUUsageMicroseconds, c.Counters.CPUUserMicroseconds, c.Counters.CPUSystemMicroseconds = &usage, &user, &system
	c.Window = nil
	c.Counters.OOM, c.Counters.OOMKill = &zero, nil
	c.MemoryHighBytes, c.MemoryMaxBytes = CgroupLimit{State: "finite", Value: &zero}, CgroupLimit{State: "max"}
	c.SwapMaxBytes, c.SwapCurrentBytes = CgroupLimit{State: "unknown"}, &zero
	p := publicCgroupResources(&s, c, "fresh", at)
	if *p.Counters.CPUUsageMicroseconds != usage || *p.Counters.CPUUserMicroseconds+*p.Counters.CPUSystemMicroseconds == usage || p.Counters.OOM == nil || *p.Counters.OOM != 0 || p.Counters.OOMKill != nil || p.Window != nil || *p.MemoryCurrentBytes != math.MaxUint64 || *p.MemoryHighBytes.Value != 0 || p.MemoryMaxBytes.State != "max" || p.SwapMaxBytes.State != "unknown" || *p.SwapCurrentBytes != 0 {
		t.Fatal("raw independent values were constrained or zero became unknown")
	}
	for _, field := range cgroupCounterFields(&c.Counters) {
		*field = nil
	}
	c.Pressure = CgroupPressureScopes{}
	p = publicCgroupResources(&s, c, "fresh", at)
	if p.BindingState != "known" || p.Counters != (PublicCgroupCounters{}) || p.Pressure != (PublicCgroupPressureScopes{}) {
		t.Fatal("missing controllers fabricated values or lost successful binding")
	}
}

func TestCgroupProjectionCaptureAndBinding(t *testing.T) {
	at := time.Now().UTC()
	s := resourceFixture(at)
	for _, elapsed := range []float64{0, 1, 1.000000001, -1, math.NaN(), math.Inf(1)} {
		c := cgroupFixture(at)
		c.CaptureDurationSeconds = elapsed
		p := publicCgroupResources(&s, c, "fresh", at)
		if elapsed < 0 || math.IsNaN(elapsed) || math.IsInf(elapsed, 0) {
			if p != nil {
				t.Fatalf("invalid duration %v accepted", elapsed)
			}
			continue
		}
		if p == nil || p.State != "fresh" {
			t.Fatal("dated capture lost")
		}
		if elapsed > 1 {
			if p.BindingState != "unavailable" || p.MemoryCurrentBytes != nil || p.Window != nil || p.Counters != (PublicCgroupCounters{}) || p.Pressure != (PublicCgroupPressureScopes{}) || p.CPUQuota.State != "unknown" {
				t.Fatal("over-budget capture retained values")
			}
		} else if p.BindingState != "known" || p.MemoryCurrentBytes == nil || p.Window == nil {
			t.Fatal("inclusive budget rejected")
		}
	}
	for _, binding := range []string{"unavailable", "unsupported"} {
		c := cgroupFixture(at)
		c.BindingState = binding
		p := publicCgroupResources(&s, c, "fresh", at)
		if p == nil || p.BindingState != binding || p.Counters != (PublicCgroupCounters{}) || p.Pressure != (PublicCgroupPressureScopes{}) || p.Window != nil || p.MemoryCurrentBytes != nil || p.CPUQuota.State != "unknown" || p.MemoryMaxBytes.State != "unknown" {
			t.Fatal("unbound capture retained measurements")
		}
	}
	c := cgroupFixture(at)
	c.BindingState = "other"
	if publicCgroupResources(&s, c, "fresh", at) != nil || publicCgroupResources(&s, nil, "fresh", at) != nil {
		t.Fatal("invalid or absent optional child accepted")
	}
}

func TestCgroupProjectionLocalSettings(t *testing.T) {
	at := time.Now().UTC()
	s := resourceFixture(at)
	zero, one := uint64(0), uint64(1)
	for _, limit := range []CgroupLimit{{State: "finite"}, {State: "max", Value: &one}, {State: "unknown", Value: &one}, {State: "bad", Value: &one}} {
		c := cgroupFixture(at)
		c.MemoryMaxBytes = limit
		p := publicCgroupResources(&s, c, "fresh", at)
		if p.MemoryMaxBytes.State != "unknown" || p.MemoryMaxBytes.Value != nil || p.Counters.OOM == nil || p.MemoryHighBytes.State != "finite" {
			t.Fatal("bad limit contaminated another field")
		}
	}
	for _, mutate := range []func(*CgroupResources){
		func(c *CgroupResources) { c.CPUQuota.Value = &zero },
		func(c *CgroupResources) { c.CPUPeriodMicroseconds = &zero },
		func(c *CgroupResources) { c.CPUPeriodMicroseconds = nil },
		func(c *CgroupResources) { c.CPUQuota = CgroupLimit{State: "unknown"} },
	} {
		c := cgroupFixture(at)
		mutate(c)
		p := publicCgroupResources(&s, c, "fresh", at)
		if p.CPUQuota.State != "unknown" || p.CPUPeriodMicroseconds != nil || p.MemoryCurrentBytes == nil {
			t.Fatal("invalid quota pair retained or contaminated memory")
		}
	}
	c := cgroupFixture(at)
	c.CPUQuota = CgroupLimit{State: "max"}
	if p := publicCgroupResources(&s, c, "fresh", at); p.CPUQuota.State != "max" || p.CPUPeriodMicroseconds == nil {
		t.Fatal("unlimited local quota lost period")
	}
	for _, cpus := range []uint64{0, math.MaxUint32 + 1} {
		c.EffectiveCPUs = &cpus
		if p := publicCgroupResources(&s, c, "fresh", at); p.EffectiveCPUs != nil || p.MemoryCurrentBytes == nil {
			t.Fatal("CPU-set source bound not respected")
		}
	}
}

func TestCgroupProjectionWindowAndPressure(t *testing.T) {
	at := time.Now().UTC()
	s := resourceFixture(at)
	for _, mutate := range []func(*CgroupResources){
		func(c *CgroupResources) { c.Window.DurationSeconds = 0 },
		func(c *CgroupResources) { c.Window.DurationSeconds = 30.000000001 },
		func(c *CgroupResources) { c.Window.DurationSeconds = math.NaN() },
		func(c *CgroupResources) { c.Window.WindowStart = time.Time{} },
		func(c *CgroupResources) { c.Window.WindowStart = s.StartedAt.Add(-time.Nanosecond) },
		func(c *CgroupResources) { c.Window.WindowStart = at.Add(time.Second + time.Nanosecond) },
		func(c *CgroupResources) { c.Window.WindowEnd = at.Add(time.Nanosecond) },
		func(c *CgroupResources) { c.Window.Counters = CgroupCounters{} },
		func(c *CgroupResources) { c.Counters.OOM = nil },
		func(c *CgroupResources) { zero := uint64(0); c.Counters.OOM = &zero },
	} {
		c := cgroupFixture(at)
		mutate(c)
		p := publicCgroupResources(&s, c, "fresh", at)
		if p.Window != nil || p.MemoryCurrentBytes == nil || p.Pressure.CPU.Full == nil {
			t.Fatal("bad delta retained or erased independent facts")
		}
	}
	c := cgroupFixture(at)
	c.Window.WindowStart = at.Add(time.Second)
	c.Window.DurationSeconds = 30
	if p := publicCgroupResources(&s, c, "fresh", at); p.Window == nil {
		t.Fatal("wall reversal overwrote source monotonic interval")
	}
	for _, invalid := range []float64{-1, 100.0000001, math.NaN(), math.Inf(1)} {
		c = cgroupFixture(at)
		bad := *c.Pressure.CPU.Some
		bad.Average10 = invalid
		c.Pressure.CPU.Some = &bad
		p := publicCgroupResources(&s, c, "fresh", at)
		if p.Pressure.CPU.Some != nil || p.Pressure.CPU.Full == nil || p.Pressure.Memory.Some == nil || p.Window == nil {
			t.Fatal("invalid PSI line contaminated another scope")
		}
	}
}

func TestCgroupProjectionClocksAndFreshness(t *testing.T) {
	at := time.Now().UTC()
	s := resourceFixture(at)
	for _, age := range []time.Duration{30 * time.Second, 30*time.Second + time.Nanosecond} {
		c := cgroupFixture(at.Add(-age))
		p := publicCgroupResources(&s, c, "fresh", at)
		if p == nil || (p.Window != nil) != (age <= 30*time.Second) || p.MemoryCurrentBytes == nil {
			t.Fatal("cgroup own-age boundary changed")
		}
	}
	c := cgroupFixture(at)
	if p := publicCgroupResources(&s, c, "stale", at); p.State != "stale" || p.Window != nil || p.Counters.OOM == nil {
		t.Fatal("source loss retained a window or erased historical counters")
	}
	for _, mutate := range []func(*Snapshot, *CgroupResources){
		func(s *Snapshot, _ *CgroupResources) { s.RunID = "" },
		func(s *Snapshot, c *CgroupResources) { c.SampledAt = s.StartedAt.Add(-time.Nanosecond) },
		func(s *Snapshot, c *CgroupResources) { c.CaptureStartedAt = s.StartedAt.Add(-time.Nanosecond) },
		func(_ *Snapshot, c *CgroupResources) { c.SampledAt = at.Add(time.Second + time.Nanosecond) },
		func(s *Snapshot, c *CgroupResources) {
			s.SampledAt = at.Add(-time.Second)
			c.SampledAt = at.Add(time.Nanosecond)
		},
		func(s *Snapshot, c *CgroupResources) {
			s.SampledAt = at.Add(time.Second)
			c.SampledAt = at.Add(2 * time.Second)
		},
	} {
		s, c := resourceFixture(at), cgroupFixture(at)
		mutate(&s, c)
		if publicCgroupResources(&s, c, "fresh", at) != nil {
			t.Fatal("run or absolute future cap bypassed")
		}
	}
	before, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	publicCgroupResources(&s, c, "fresh", at)
	after, err := json.Marshal(c)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("projection mutated the private source")
	}
}

func TestCgroupIndependentProcessRoleAndClock(t *testing.T) {
	at := time.Now().UTC()
	for _, mode := range []string{"validator", "verifying"} {
		for _, rpc := range []bool{false, true} {
			s := resourceFixture(at)
			s.Mode, s.RPCConfigured = mode, rpc
			s.Resources.Cgroup = cgroupFixture(at)
			for _, processAt := range []time.Time{{}, at.Add(-31 * time.Second), at.Add(2 * time.Second)} {
				s.Resources.SampledAt = processAt
				p := publicResources(&s, "fresh", at)
				if p.Cgroup == nil || p.Cgroup.State != "fresh" || p.Cgroup.Window == nil {
					t.Fatal("role or process timestamp erased independent cgroup capture")
				}
			}
		}
	}
}

func TestCgroupRealFeedDisclosureAndRecovery(t *testing.T) {
	ctx := context.Background()
	path := socketPath(t)
	at := time.Now().UTC()
	s := resourceFixture(at)
	s.Resources.Cgroup = cgroupFixture(at)
	var calls atomic.Uint64
	feed, err := ListenFeed(ctx, path, func() Snapshot { calls.Add(1); return s })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	c := newCollector(path)
	c.public = true
	t.Cleanup(c.client.CloseIdleConnections)
	read := func() PublicSnapshot {
		t.Helper()
		set := c.latest.Load()
		var p PublicSnapshot
		if set == nil || json.Unmarshal(set.advancedPublicJSON, &p) != nil {
			t.Fatal("missing public encoding")
		}
		var frame struct{ Value json.RawMessage }
		if err := json.Unmarshal(set.advancedMessages[0], &frame); err != nil || string(frame.Value) != string(set.advancedPublicJSON) {
			t.Fatal("HTTP and WebSocket cgroup projections diverged")
		}
		return p
	}
	c.poll(ctx, time.Now().UTC())
	if read().Resources != nil {
		t.Fatal("private capture bypassed public disclosure gate")
	}
	c.publicOptions.Resources = true
	c.poll(ctx, time.Now().UTC())
	if p := read(); p.Resources.Cgroup == nil || p.Resources.Cgroup.BindingState != "known" || p.Resources.Cgroup.Window == nil {
		t.Fatal("opted-in cgroup did not traverse real Unix feed")
	}
	started, initialCalls := time.Now(), calls.Load()
	for range 20 {
		response, err := c.client.Get("http://node/snapshot")
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, MaxSnapshotBytes+1))
		closeErr := response.Body.Close()
		if response.StatusCode != http.StatusOK || readErr != nil || closeErr != nil {
			t.Fatalf("cached feed read failed: status=%d read=%v close=%v", response.StatusCode, readErr, closeErr)
		}
	}
	if calls.Load()-initialCalls > uint64(time.Since(started)/SampleInterval)+1 {
		t.Fatal("reader requests triggered extra source samples")
	}
	for _, target := range []string{"http://node/snapshot?path=/proc/self/cgroup", "http://node/cgroup", "http://node/proc/self/cgroup"} {
		response, err := c.client.Get(target)
		if err != nil {
			t.Fatal(err)
		}
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusBadRequest && response.StatusCode != http.StatusNotFound {
			t.Fatal("cgroup feature added a source-reading path")
		}
	}
	feed.Close()
	c.poll(ctx, time.Now().UTC())
	if p := read(); p.Resources.Cgroup.State != "stale" || p.Resources.Cgroup.Window != nil || p.Resources.Cgroup.Counters.OOM == nil {
		t.Fatal("source loss retained current deltas or erased historical counters")
	}
	// A replacement process has no prior binding/window, even if its identity
	// is unchanged. Feed replacement must not fill its unknowns from old data.
	nextAt := time.Now().UTC()
	next := resourceFixture(nextAt)
	next.RunID, next.StartedAt = "replacement", nextAt.Add(-time.Second)
	next.Resources.Cgroup = &CgroupResources{SampledAt: nextAt, CaptureStartedAt: nextAt, BindingState: "unavailable"}
	replacement, err := ListenFeed(ctx, path, func() Snapshot { return next })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(replacement.Close)
	c.poll(ctx, time.Now().UTC())
	if p := read(); p.Node.RunID != "replacement" || p.Resources.Cgroup == nil || p.Resources.Cgroup.BindingState != "unavailable" || p.Resources.Cgroup.Counters != (PublicCgroupCounters{}) || p.Resources.Cgroup.Window != nil {
		t.Fatal("new run reused old cgroup measurements")
	}
	replacement.Close()
	old := resourceFixture(time.Now().UTC())
	old.RunID = "older-producer"
	legacy, err := ListenFeed(ctx, path, func() Snapshot { return old })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(legacy.Close)
	c.poll(ctx, time.Now().UTC())
	if read().Resources.Cgroup != nil {
		t.Fatal("older producer inherited cgroup fields")
	}
}
