package dashboard

import (
	"math"
	"time"
)

// HostResources dates one visible-host capture attempt, independently of the
// process resource sample. CaptureDurationSeconds is measured before UTC conversion.
type HostResources struct {
	SampledAt              time.Time   `json:"sampled_at"`
	CaptureStartedAt       time.Time   `json:"capture_started_at"`
	CaptureDurationSeconds float64     `json:"capture_duration_seconds"`
	CPU                    *HostCPU    `json:"cpu"`
	Load                   *HostLoad   `json:"load"`
	Memory                 *HostMemory `json:"memory"`
}

// HostCPU reports the aggregate online CPU set, not process or cgroup usage.
type HostCPU struct {
	ObservedAt time.Time      `json:"observed_at"`
	OnlineCPUs uint32         `json:"online_cpus"`
	Window     *HostCPUWindow `json:"window"`
}

// HostCPUWindow contains source-measured deltas in user, nice, system, idle,
// iowait, irq, softirq, steal order. Busy excludes idle, iowait and steal.
type HostCPUWindow struct {
	WindowStart     time.Time  `json:"window_start"`
	WindowEnd       time.Time  `json:"window_end"`
	DurationSeconds float64    `json:"duration_seconds"`
	Percent         [8]float64 `json:"percent"`
	BusyPercent     float64    `json:"busy_percent"`
}

// HostLoad contains load averages, not percentages, and task gauges.
type HostLoad struct {
	One      float64 `json:"one"`
	Five     float64 `json:"five"`
	Fifteen  float64 `json:"fifteen"`
	Runnable uint64  `json:"runnable,string"`
	Tasks    uint64  `json:"tasks,string"`
}

// HostMemory contains independently available kernel byte observations. These
// overlapping categories must not be summed into an invented available value.
type HostMemory struct {
	TotalBytes       uint64  `json:"total_bytes,string"`
	FreeBytes        uint64  `json:"free_bytes,string"`
	AvailableBytes   *uint64 `json:"available_bytes,string"`
	BuffersBytes     *uint64 `json:"buffers_bytes,string"`
	CachedBytes      *uint64 `json:"cached_bytes,string"`
	ReclaimableBytes *uint64 `json:"reclaimable_bytes,string"`
	SwapTotalBytes   *uint64 `json:"swap_total_bytes,string"`
	SwapFreeBytes    *uint64 `json:"swap_free_bytes,string"`
	DirtyBytes       *uint64 `json:"dirty_bytes,string"`
	WritebackBytes   *uint64 `json:"writeback_bytes,string"`
}

// PublicHostResources reports freshness of a capture attempt, not host health.
// Every scope may be unknown even when the dated attempt is fresh.
type PublicHostResources struct {
	PublicObservation
	CaptureStartedAt time.Time         `json:"capture_started_at"`
	Scope            string            `json:"scope"`
	CPU              *PublicHostCPU    `json:"cpu"`
	Load             *PublicHostLoad   `json:"load"`
	Memory           *PublicHostMemory `json:"memory"`
}

// PublicHostCPU dates the online count separately from capture completion.
type PublicHostCPU struct {
	ObservedAt time.Time            `json:"observed_at"`
	OnlineCPUs uint32               `json:"online_cpus"`
	Window     *PublicHostCPUWindow `json:"window"`
}

// PublicHostCPUWindow is a source-measured interval, never UTC subtraction.
type PublicHostCPUWindow struct {
	WindowStart     time.Time  `json:"window_start"`
	WindowEnd       time.Time  `json:"window_end"`
	DurationSeconds float64    `json:"duration_seconds"`
	Percent         [8]float64 `json:"percent"`
	BusyPercent     float64    `json:"busy_percent"`
}

// PublicHostLoad exposes only aggregate averages and exact task counts.
type PublicHostLoad struct {
	One      float64 `json:"one"`
	Five     float64 `json:"five"`
	Fifteen  float64 `json:"fifteen"`
	Runnable uint64  `json:"runnable,string"`
	Tasks    uint64  `json:"tasks,string"`
}

// PublicHostMemory is an explicit allowlist without OS paths or identifiers.
type PublicHostMemory struct {
	TotalBytes       uint64  `json:"total_bytes,string"`
	FreeBytes        uint64  `json:"free_bytes,string"`
	AvailableBytes   *uint64 `json:"available_bytes,string"`
	BuffersBytes     *uint64 `json:"buffers_bytes,string"`
	CachedBytes      *uint64 `json:"cached_bytes,string"`
	ReclaimableBytes *uint64 `json:"reclaimable_bytes,string"`
	SwapTotalBytes   *uint64 `json:"swap_total_bytes,string"`
	SwapFreeBytes    *uint64 `json:"swap_free_bytes,string"`
	DirtyBytes       *uint64 `json:"dirty_bytes,string"`
	WritebackBytes   *uint64 `json:"writeback_bytes,string"`
}

func publicHostResources(s *Snapshot, h *HostResources, state string, now time.Time) *PublicHostResources {
	if s == nil || h == nil || s.RunID == "" || !validPublicTime(s.StartedAt, now) || !validHostTime(s, h.SampledAt, now) ||
		!validHostTime(s, h.CaptureStartedAt, now) || !finiteHostValue(h.CaptureDurationSeconds) {
		return nil
	}
	p := &PublicHostResources{PublicObservation: publicObservation(h.SampledAt.UTC(), state, now),
		CaptureStartedAt: h.CaptureStartedAt.UTC(), Scope: "visible_host"}
	if h.CaptureDurationSeconds > 1 {
		return p // Over-budget attempts must not refresh any claimed values.
	}
	p.CPU = publicHostCPU(s, h.CPU, p.State, now)
	if l := h.Load; l != nil && finiteHostValue(l.One) && finiteHostValue(l.Five) && finiteHostValue(l.Fifteen) && l.Tasks > 0 && l.Runnable <= l.Tasks {
		p.Load = &PublicHostLoad{One: l.One, Five: l.Five, Fifteen: l.Fifteen, Runnable: l.Runnable, Tasks: l.Tasks}
	}
	if m := h.Memory; m != nil && m.TotalBytes > 0 && m.FreeBytes <= m.TotalBytes &&
		(m.AvailableBytes == nil || *m.AvailableBytes <= m.TotalBytes) &&
		(m.SwapTotalBytes == nil || m.SwapFreeBytes == nil || *m.SwapFreeBytes <= *m.SwapTotalBytes) {
		p.Memory = &PublicHostMemory{TotalBytes: m.TotalBytes, FreeBytes: m.FreeBytes}
		for _, field := range []struct {
			source *uint64
			target **uint64
		}{
			{m.AvailableBytes, &p.Memory.AvailableBytes}, {m.BuffersBytes, &p.Memory.BuffersBytes},
			{m.CachedBytes, &p.Memory.CachedBytes}, {m.ReclaimableBytes, &p.Memory.ReclaimableBytes},
			{m.SwapTotalBytes, &p.Memory.SwapTotalBytes}, {m.SwapFreeBytes, &p.Memory.SwapFreeBytes},
			{m.DirtyBytes, &p.Memory.DirtyBytes}, {m.WritebackBytes, &p.Memory.WritebackBytes},
		} {
			if field.source != nil {
				value := *field.source
				*field.target = &value
			}
		}
	}
	return p
}

func publicHostCPU(s *Snapshot, c *HostCPU, state string, now time.Time) *PublicHostCPU {
	if c == nil || c.OnlineCPUs == 0 || !validHostTime(s, c.ObservedAt, now) {
		return nil
	}
	p := &PublicHostCPU{ObservedAt: c.ObservedAt.UTC(), OnlineCPUs: c.OnlineCPUs}
	w := c.Window
	if w == nil || state != "fresh" || now.Sub(c.ObservedAt) > 30*time.Second ||
		!validHostTime(s, w.WindowStart, now) || !w.WindowEnd.Equal(c.ObservedAt) ||
		!finiteHostValue(w.DurationSeconds) || w.DurationSeconds == 0 || w.DurationSeconds > 30 ||
		!finiteHostValue(w.BusyPercent) || w.BusyPercent > 100 {
		return p
	}
	var total float64
	for _, percent := range w.Percent {
		if !finiteHostValue(percent) || percent > 100 {
			return p
		}
		total += percent
	}
	busy := w.Percent[0] + w.Percent[1] + w.Percent[2] + w.Percent[5] + w.Percent[6]
	if math.Abs(total-100) > 1e-6 || math.Abs(busy-w.BusyPercent) > 1e-6 {
		return p
	}
	p.Window = &PublicHostCPUWindow{WindowStart: w.WindowStart.UTC(), WindowEnd: w.WindowEnd.UTC(),
		DurationSeconds: w.DurationSeconds, Percent: w.Percent, BusyPercent: w.BusyPercent}
	return p
}

// validHostTime binds independently dated scopes to the current run and both
// outer clocks. It does not infer elapsed durations from serialized wall dates.
func validHostTime(s *Snapshot, at, now time.Time) bool {
	return validPublicTime(at, now) && !at.Before(s.StartedAt) && !at.After(s.SampledAt.Add(time.Second))
}

func finiteHostValue(v float64) bool { return v >= 0 && !math.IsNaN(v) && !math.IsInf(v, 0) }
