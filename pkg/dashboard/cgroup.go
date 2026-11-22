package dashboard

import (
	"math"
	"time"
)

// CgroupLimit is a local setting, not effective capacity across ancestors.
// State is finite with a value, max with no value, or unknown with no value.
type CgroupLimit struct {
	State string  `json:"state"`
	Value *uint64 `json:"value,string"`
}

// CgroupCounters contains cumulative CPU accounting and local memory events.
// CPU times are microseconds; periods and memory events are counts.
type CgroupCounters struct {
	CPUUsageMicroseconds  *uint64 `json:"cpu_usage_microseconds,string"`
	CPUUserMicroseconds   *uint64 `json:"cpu_user_microseconds,string"`
	CPUSystemMicroseconds *uint64 `json:"cpu_system_microseconds,string"`
	Periods               *uint64 `json:"periods,string"`
	ThrottledPeriods      *uint64 `json:"throttled_periods,string"`
	ThrottledMicroseconds *uint64 `json:"throttled_microseconds,string"`
	MemoryHigh            *uint64 `json:"memory_high,string"`
	MemoryMax             *uint64 `json:"memory_max,string"`
	OOM                   *uint64 `json:"oom,string"`
	OOMKill               *uint64 `json:"oom_kill,string"`
}

// CgroupWindow contains source-measured differences from one unchanged binding.
type CgroupWindow struct {
	WindowStart     time.Time      `json:"window_start"`
	WindowEnd       time.Time      `json:"window_end"`
	DurationSeconds float64        `json:"duration_seconds"`
	Counters        CgroupCounters `json:"counters"`
}

// CgroupPressureLine preserves kernel stall averages and cumulative microseconds.
type CgroupPressureLine struct {
	Average10         float64 `json:"average_10"`
	Average60         float64 `json:"average_60"`
	Average300        float64 `json:"average_300"`
	TotalMicroseconds uint64  `json:"total_microseconds,string"`
}

// CgroupPressure keeps some and full stall observations independently nullable.
type CgroupPressure struct {
	Some *CgroupPressureLine `json:"some"`
	Full *CgroupPressureLine `json:"full"`
}

// CgroupPressureScopes contains only CPU, memory and I/O stall records.
type CgroupPressureScopes struct {
	CPU    CgroupPressure `json:"cpu"`
	Memory CgroupPressure `json:"memory"`
	IO     CgroupPressure `json:"io"`
}

// CgroupResources dates one visible-domain-cgroup capture. BindingState is
// known, unavailable or unsupported; it exposes no kernel binding identifier.
// CaptureDurationSeconds is measured before the source times are made UTC.
type CgroupResources struct {
	SampledAt              time.Time            `json:"sampled_at"`
	CaptureStartedAt       time.Time            `json:"capture_started_at"`
	CaptureDurationSeconds float64              `json:"capture_duration_seconds"`
	BindingState           string               `json:"binding_state"`
	CPUQuota               CgroupLimit          `json:"cpu_quota"`
	CPUPeriodMicroseconds  *uint64              `json:"cpu_period_microseconds,string"`
	EffectiveCPUs          *uint64              `json:"effective_cpus,string"`
	MemoryCurrentBytes     *uint64              `json:"memory_current_bytes,string"`
	SwapCurrentBytes       *uint64              `json:"swap_current_bytes,string"`
	MemoryHighBytes        CgroupLimit          `json:"memory_high_bytes"`
	MemoryMaxBytes         CgroupLimit          `json:"memory_max_bytes"`
	SwapMaxBytes           CgroupLimit          `json:"swap_max_bytes"`
	Counters               CgroupCounters       `json:"counters"`
	Window                 *CgroupWindow        `json:"window"`
	Pressure               CgroupPressureScopes `json:"pressure"`
}

// PublicCgroupResources reports capture freshness, not resource health. Usage
// may include descendants, and local limits do not establish ancestor capacity.
type PublicCgroupResources struct {
	PublicObservation
	CaptureStartedAt       time.Time                  `json:"capture_started_at"`
	CaptureDurationSeconds float64                    `json:"capture_duration_seconds"`
	Scope                  string                     `json:"scope"`
	BindingState           string                     `json:"binding_state"`
	CPUQuota               PublicCgroupLimit          `json:"cpu_quota"`
	CPUPeriodMicroseconds  *uint64                    `json:"cpu_period_microseconds,string"`
	EffectiveCPUs          *uint64                    `json:"effective_cpus,string"`
	MemoryCurrentBytes     *uint64                    `json:"memory_current_bytes,string"`
	SwapCurrentBytes       *uint64                    `json:"swap_current_bytes,string"`
	MemoryHighBytes        PublicCgroupLimit          `json:"memory_high_bytes"`
	MemoryMaxBytes         PublicCgroupLimit          `json:"memory_max_bytes"`
	SwapMaxBytes           PublicCgroupLimit          `json:"swap_max_bytes"`
	Counters               PublicCgroupCounters       `json:"counters"`
	Window                 *PublicCgroupWindow        `json:"window"`
	Pressure               PublicCgroupPressureScopes `json:"pressure"`
}

type PublicCgroupLimit struct {
	State string  `json:"state"`
	Value *uint64 `json:"value,string"`
}

type PublicCgroupCounters struct {
	CPUUsageMicroseconds  *uint64 `json:"cpu_usage_microseconds,string"`
	CPUUserMicroseconds   *uint64 `json:"cpu_user_microseconds,string"`
	CPUSystemMicroseconds *uint64 `json:"cpu_system_microseconds,string"`
	Periods               *uint64 `json:"periods,string"`
	ThrottledPeriods      *uint64 `json:"throttled_periods,string"`
	ThrottledMicroseconds *uint64 `json:"throttled_microseconds,string"`
	MemoryHigh            *uint64 `json:"memory_high,string"`
	MemoryMax             *uint64 `json:"memory_max,string"`
	OOM                   *uint64 `json:"oom,string"`
	OOMKill               *uint64 `json:"oom_kill,string"`
}

type PublicCgroupWindow struct {
	WindowStart     time.Time            `json:"window_start"`
	WindowEnd       time.Time            `json:"window_end"`
	DurationSeconds float64              `json:"duration_seconds"`
	Counters        PublicCgroupCounters `json:"counters"`
}

type PublicCgroupPressureLine struct {
	Average10         float64 `json:"average_10"`
	Average60         float64 `json:"average_60"`
	Average300        float64 `json:"average_300"`
	TotalMicroseconds uint64  `json:"total_microseconds,string"`
}

type PublicCgroupPressure struct {
	Some *PublicCgroupPressureLine `json:"some"`
	Full *PublicCgroupPressureLine `json:"full"`
}

type PublicCgroupPressureScopes struct {
	CPU    PublicCgroupPressure `json:"cpu"`
	Memory PublicCgroupPressure `json:"memory"`
	IO     PublicCgroupPressure `json:"io"`
}

func publicCgroupResources(s *Snapshot, c *CgroupResources, state string, now time.Time) *PublicCgroupResources {
	if s == nil || c == nil || s.RunID == "" || state != "fresh" && state != "stale" ||
		!validPublicTime(s.StartedAt, now) || !validHostTime(s, c.SampledAt, now) ||
		!validHostTime(s, c.CaptureStartedAt, now) || !finiteHostValue(c.CaptureDurationSeconds) {
		return nil
	}
	if c.BindingState != "known" && c.BindingState != "unavailable" && c.BindingState != "unsupported" {
		return nil
	}
	p := &PublicCgroupResources{PublicObservation: publicObservation(c.SampledAt.UTC(), state, now),
		CaptureStartedAt: c.CaptureStartedAt.UTC(), CaptureDurationSeconds: c.CaptureDurationSeconds,
		Scope: "visible_domain_cgroup", BindingState: c.BindingState,
		CPUQuota: PublicCgroupLimit{State: "unknown"}, MemoryHighBytes: PublicCgroupLimit{State: "unknown"},
		MemoryMaxBytes: PublicCgroupLimit{State: "unknown"}, SwapMaxBytes: PublicCgroupLimit{State: "unknown"}}
	if c.CaptureDurationSeconds > 1 {
		p.BindingState = "unavailable"
	}
	if p.BindingState != "known" {
		return p
	}
	quota := publicCgroupLimit(c.CPUQuota)
	if quota.State != "unknown" && c.CPUPeriodMicroseconds != nil && *c.CPUPeriodMicroseconds > 0 &&
		(quota.State != "finite" || *quota.Value > 0) {
		p.CPUQuota, p.CPUPeriodMicroseconds = quota, cgroupUint64(c.CPUPeriodMicroseconds)
	}
	if c.EffectiveCPUs != nil && *c.EffectiveCPUs > 0 && *c.EffectiveCPUs <= math.MaxUint32 {
		p.EffectiveCPUs = cgroupUint64(c.EffectiveCPUs)
	}
	p.MemoryCurrentBytes, p.SwapCurrentBytes = cgroupUint64(c.MemoryCurrentBytes), cgroupUint64(c.SwapCurrentBytes)
	p.MemoryHighBytes, p.MemoryMaxBytes, p.SwapMaxBytes = publicCgroupLimit(c.MemoryHighBytes), publicCgroupLimit(c.MemoryMaxBytes), publicCgroupLimit(c.SwapMaxBytes)
	p.Counters = publicCgroupCounters(c.Counters)
	p.Pressure = PublicCgroupPressureScopes{CPU: publicCgroupPressure(c.Pressure.CPU), Memory: publicCgroupPressure(c.Pressure.Memory), IO: publicCgroupPressure(c.Pressure.IO)}
	if p.State == "fresh" {
		p.Window = publicCgroupWindow(s, c, now)
	}
	return p
}

func publicCgroupLimit(l CgroupLimit) PublicCgroupLimit {
	if l.State == "finite" && l.Value != nil {
		return PublicCgroupLimit{State: "finite", Value: cgroupUint64(l.Value)}
	}
	if l.State == "max" && l.Value == nil {
		return PublicCgroupLimit{State: "max"}
	}
	return PublicCgroupLimit{State: "unknown"}
}

func publicCgroupCounters(c CgroupCounters) PublicCgroupCounters {
	return PublicCgroupCounters{
		CPUUsageMicroseconds: cgroupUint64(c.CPUUsageMicroseconds), CPUUserMicroseconds: cgroupUint64(c.CPUUserMicroseconds),
		CPUSystemMicroseconds: cgroupUint64(c.CPUSystemMicroseconds), Periods: cgroupUint64(c.Periods),
		ThrottledPeriods: cgroupUint64(c.ThrottledPeriods), ThrottledMicroseconds: cgroupUint64(c.ThrottledMicroseconds),
		MemoryHigh: cgroupUint64(c.MemoryHigh), MemoryMax: cgroupUint64(c.MemoryMax),
		OOM: cgroupUint64(c.OOM), OOMKill: cgroupUint64(c.OOMKill),
	}
}

func cgroupUint64(v *uint64) *uint64 {
	if v == nil {
		return nil
	}
	value := *v
	return &value
}

func cgroupCounterFields(c *CgroupCounters) [10]**uint64 {
	return [10]**uint64{&c.CPUUsageMicroseconds, &c.CPUUserMicroseconds, &c.CPUSystemMicroseconds,
		&c.Periods, &c.ThrottledPeriods, &c.ThrottledMicroseconds, &c.MemoryHigh, &c.MemoryMax, &c.OOM, &c.OOMKill}
}

func copyCgroupCounters(c CgroupCounters) CgroupCounters {
	for _, field := range cgroupCounterFields(&c) {
		*field = cgroupUint64(*field)
	}
	return c
}

func publicCgroupWindow(s *Snapshot, c *CgroupResources, now time.Time) *PublicCgroupWindow {
	w := c.Window
	if w == nil || !validHostTime(s, w.WindowStart, now) || !w.WindowEnd.Equal(c.SampledAt) ||
		!finiteHostValue(w.DurationSeconds) || w.DurationSeconds == 0 || w.DurationSeconds > 30 {
		return nil
	}
	current, delta := cgroupCounterFields(&c.Counters), cgroupCounterFields(&w.Counters)
	known := false
	for i, field := range delta {
		if *field != nil {
			if *current[i] == nil || **field > **current[i] {
				return nil
			}
			known = true
		}
	}
	if !known {
		return nil
	}
	return &PublicCgroupWindow{WindowStart: w.WindowStart.UTC(), WindowEnd: w.WindowEnd.UTC(),
		DurationSeconds: w.DurationSeconds, Counters: publicCgroupCounters(w.Counters)}
}

func publicCgroupPressure(p CgroupPressure) PublicCgroupPressure {
	line := func(l *CgroupPressureLine) *PublicCgroupPressureLine {
		if l == nil || !finiteHostValue(l.Average10) || l.Average10 > 100 ||
			!finiteHostValue(l.Average60) || l.Average60 > 100 || !finiteHostValue(l.Average300) || l.Average300 > 100 {
			return nil
		}
		return &PublicCgroupPressureLine{Average10: l.Average10, Average60: l.Average60, Average300: l.Average300, TotalMicroseconds: l.TotalMicroseconds}
	}
	return PublicCgroupPressure{Some: line(p.Some), Full: line(p.Full)}
}
