package node

import (
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

type dashboardIOObservation struct {
	value      dashboard.StorageIODetail
	freshUntil time.Time
}

// Diagnostics owns these samplers. Viewers and the Feed only copy the retained
// result; no worker, recursive device scan or viewer-controlled path is added.
type dashboardIOMonitor struct {
	process *metrics.ProcessIOSampler
	block   *metrics.BlockIOSampler
	latest  atomic.Pointer[dashboardIOObservation]
}

func (d *nodeDashboard) configureIO(enabled bool) {
	if d != nil && enabled {
		d.io = &dashboardIOMonitor{process: metrics.NewProcessIOSampler(true), block: metrics.NewBlockIOSampler(true)}
	}
}

func (m *dashboardIOMonitor) observe(accounts, ledger string) {
	if m == nil {
		return
	}
	started := time.Now()
	v := dashboard.StorageIODetail{Scope: "linux_process_and_mounted_block_layer", StartedAt: started.UTC()}
	p, supported := m.process.Sample()
	v.Process.Supported = supported
	if supported {
		v.Process.StartedAt, v.Process.CapturedAt = p.StartedAt.UTC(), p.SampledAt.UTC()
		if p.Known {
			c := processIOCounts(p.Counters)
			v.Process.Counters = &c
			if p.HasWindow {
				v.Process.Window = &dashboard.ProcessIOInterval{StartedAt: p.Window.StartedAt.UTC(), EndedAt: p.Window.EndedAt.UTC(),
					DurationNanoseconds: uint64(p.Window.Duration), Counters: processIOCounts(p.Window.Counters)}
			}
		}
	}
	for _, role := range []struct {
		path   string
		target *dashboard.BlockIODetail
	}{{"/", &v.Root}, {accounts, &v.Accounts}, {ledger, &v.Ledger}} {
		if time.Since(started) > 5*time.Second {
			m.latest.Store(nil)
			return
		}
		o, supported := m.block.Sample(role.path)
		*role.target = blockIOCounts(o, supported)
	}
	completed := time.Now()
	if completed.Before(started) || completed.Sub(started) > 5*time.Second {
		m.latest.Store(nil)
		return
	}
	v.CapturedAt = completed.UTC()
	m.latest.Store(&dashboardIOObservation{value: v, freshUntil: started.Add(dashboard.StorageDetailFreshFor)})
}

func processIOCounts(c metrics.ProcessIOCounters) dashboard.ProcessIOCounts {
	return dashboard.ProcessIOCounts{ReadCharacters: c.ReadCharacters, WriteCharacters: c.WriteCharacters,
		ReadSyscalls: c.ReadSyscalls, WriteSyscalls: c.WriteSyscalls, ReadBytes: c.ReadBytes,
		WriteBytes: c.WriteBytes, CancelledWriteBytes: c.CancelledWriteBytes}
}

func cloneDashboardIO(v dashboard.StorageIODetail) dashboard.StorageIODetail {
	if v.Process.Counters != nil {
		c := *v.Process.Counters
		v.Process.Counters = &c
	}
	if v.Process.Window != nil {
		w := *v.Process.Window
		v.Process.Window = &w
	}
	for _, b := range []*dashboard.BlockIODetail{&v.Root, &v.Accounts, &v.Ledger} {
		if b.Device != nil {
			d := *b.Device
			b.Device = &d
		}
		if b.Counters != nil {
			c := *b.Counters
			b.Counters = &c
		}
	}
	return v
}

func blockIOCounts(o metrics.BlockIOObservation, supported bool) dashboard.BlockIODetail {
	d := dashboard.BlockIODetail{Supported: supported}
	if !supported {
		return d
	}
	d.StartedAt, d.CapturedAt = o.StartedAt.UTC(), o.SampledAt.UTC()
	if !o.Known {
		return d
	}
	d.Device = &dashboard.BlockIODevice{Major: o.Major, Minor: o.Minor, HasDiscard: o.HasDiscard, HasFlush: o.HasFlush}
	c := o.Counters
	d.Counters = &dashboard.BlockIOCounts{
		ReadsCompleted: c.ReadsCompleted, ReadsMerged: c.ReadsMerged, ReadSectors: c.ReadSectors, ReadMilliseconds: c.ReadMilliseconds,
		WritesCompleted: c.WritesCompleted, WritesMerged: c.WritesMerged, WriteSectors: c.WriteSectors, WriteMilliseconds: c.WriteMilliseconds,
		InFlight: c.InFlight, BusyMilliseconds: c.BusyMilliseconds, WeightedMilliseconds: c.WeightedMilliseconds,
	}
	if o.HasDiscard {
		d.Counters.DiscardsCompleted, d.Counters.DiscardsMerged = c.DiscardsCompleted, c.DiscardsMerged
		d.Counters.DiscardSectors, d.Counters.DiscardMilliseconds = c.DiscardSectors, c.DiscardMilliseconds
	}
	if o.HasFlush {
		d.Counters.FlushesCompleted, d.Counters.FlushMilliseconds = c.FlushesCompleted, c.FlushMilliseconds
	}
	return d
}
