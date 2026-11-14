package dashboard

import (
	"math"
	"time"
)

// PublicResources contains opt-in process and fixed-role filesystem aggregates.
// It never includes paths, device names, process arguments or host identifiers.
type PublicResources struct {
	State      string               `json:"state"`
	SampledAt  *time.Time           `json:"sampled_at"`
	RSSBytes   *uint64              `json:"rss_bytes,string"`
	CPUSeconds *float64             `json:"cpu_seconds"`
	CPU        *PublicCPURate       `json:"cpu"`
	Root       *PublicFilesystem    `json:"root"`
	Accounts   *PublicFilesystem    `json:"accounts"`
	Ledger     *PublicFilesystem    `json:"ledger"`
	Host       *PublicHostResources `json:"host,omitempty"`
	GoRuntime  *PublicGoRuntime     `json:"go_runtime,omitempty"`
}

// PublicGoRuntime contains Go-managed gauges, not RSS, OS threads or pressure
// estimates. MemoryLimitBytes is a soft runtime limit, not a process memory cap.
type PublicGoRuntime struct {
	PublicObservation
	HeapBytes        uint64 `json:"heap_bytes,string"`
	GoBytes          uint64 `json:"go_bytes,string"`
	NextGCBytes      uint64 `json:"next_gc_bytes,string"`
	GCCount          uint64 `json:"gc_count,string"`
	MemoryLimitBytes int64  `json:"memory_limit_bytes,string"`
	Goroutines       uint64 `json:"goroutines,string"`
}

// PublicCPURate measures process CPU over two dated producer samples.
// PercentOneCore may exceed 100; it is not a share of host or cgroup capacity.
type PublicCPURate struct {
	WindowStart     time.Time `json:"window_start"`
	WindowEnd       time.Time `json:"window_end"`
	DurationSeconds float64   `json:"duration_seconds"`
	CPUSeconds      float64   `json:"cpu_seconds"`
	PercentOneCore  float64   `json:"percent_one_core"`
}

type resourceCPURate struct {
	runID     string
	startedAt time.Time
	at        time.Time
	seconds   float64
	last      *PublicCPURate
}

func (r *resourceCPURate) observe(s *Snapshot, p *PublicResources) *PublicCPURate {
	if s == nil || p.State != "fresh" || p.SampledAt == nil || p.CPUSeconds == nil {
		*r = resourceCPURate{}
		return nil
	}
	if r.runID != s.RunID || !r.startedAt.Equal(s.StartedAt) {
		*r = resourceCPURate{}
	}
	at, seconds := *p.SampledAt, *p.CPUSeconds
	if at.Equal(r.at) {
		if seconds != r.seconds {
			*r = resourceCPURate{}
			return nil
		}
		return r.last
	}
	duration := at.Sub(r.at).Seconds()
	var measured *PublicCPURate
	if !r.at.IsZero() && duration > 0 && duration <= 30 && seconds >= r.seconds {
		delta := seconds - r.seconds
		percent := delta / duration * 100
		if !math.IsInf(percent, 0) && !math.IsNaN(percent) {
			measured = &PublicCPURate{WindowStart: r.at, WindowEnd: at, DurationSeconds: duration,
				CPUSeconds: delta, PercentOneCore: percent}
		}
	}
	*r = resourceCPURate{runID: s.RunID, startedAt: s.StartedAt, at: at, seconds: seconds, last: measured}
	return measured
}

// PublicFilesystem reports capacity, usage and availability for one filesystem.
// Several roles may identify the same filesystem. Zero inodes means no count.
type PublicFilesystem struct {
	TotalBytes     uint64  `json:"total_bytes,string"`
	AvailableBytes uint64  `json:"available_bytes,string"`
	UsedBytes      *uint64 `json:"used_bytes,string,omitempty"`
	Inodes         uint64  `json:"inodes,string"`
	FreeInodes     uint64  `json:"free_inodes,string"`
}

func publicResources(s *Snapshot, state string, now time.Time) *PublicResources {
	p := &PublicResources{State: "unavailable"}
	if s == nil || state != "fresh" && state != "stale" || s.Resources == nil {
		return p
	}
	r := s.Resources
	p.Host = publicHostResources(s, r.Host, state, now)
	if !validPublicTime(r.SampledAt, now) || r.SampledAt.Before(s.StartedAt) || r.SampledAt.After(s.SampledAt.Add(time.Second)) {
		return p
	}
	p.State = publicObservation(r.SampledAt, state, now).State
	at := r.SampledAt
	p.SampledAt = &at
	if r.RSSBytes != nil {
		value := *r.RSSBytes
		p.RSSBytes = &value
	}
	if r.CPUSeconds != nil && *r.CPUSeconds >= 0 && !math.IsNaN(*r.CPUSeconds) && !math.IsInf(*r.CPUSeconds, 0) {
		value := *r.CPUSeconds
		p.CPUSeconds = &value
	}
	p.Root, p.Accounts, p.Ledger = publicFilesystem(r.Root), publicFilesystem(r.Accounts), publicFilesystem(r.Ledger)
	if g := s.Runtime; g != nil && validPublicTime(g.SampledAt, now) && !g.SampledAt.Before(s.StartedAt) &&
		!g.SampledAt.After(s.SampledAt.Add(time.Second)) && g.MemoryLimitBytes >= 0 && g.Goroutines >= 0 {
		p.GoRuntime = &PublicGoRuntime{PublicObservation: publicObservation(g.SampledAt, state, now),
			HeapBytes: g.HeapBytes, GoBytes: g.GoBytes, NextGCBytes: g.NextGCBytes, GCCount: g.GCCount,
			MemoryLimitBytes: g.MemoryLimitBytes, Goroutines: uint64(g.Goroutines)}
	}
	return p
}

func publicFilesystem(f *Filesystem) *PublicFilesystem {
	if f == nil || f.TotalBytes == 0 || f.AvailableBytes > f.TotalBytes || f.FreeInodes > f.Inodes ||
		f.UsedBytes != nil && (*f.UsedBytes > f.TotalBytes || f.AvailableBytes > f.TotalBytes-*f.UsedBytes) {
		return nil
	}
	p := &PublicFilesystem{TotalBytes: f.TotalBytes, AvailableBytes: f.AvailableBytes, Inodes: f.Inodes, FreeInodes: f.FreeInodes}
	if f.UsedBytes != nil {
		used := *f.UsedBytes
		p.UsedBytes = &used
	}
	return p
}
