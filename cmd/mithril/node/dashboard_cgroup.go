package node

import (
	"fmt"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

// configureCgroupResources is startup-only; diagnostics owns subsequent samples.
func (d *nodeDashboard) configureCgroupResources(enabled bool) {
	if d != nil {
		d.cgroupSampler = metrics.NewCgroupSampler(enabled)
	}
}

func validateDashboardCgroupResources(enabled bool, socket string) error {
	if enabled && strings.TrimSpace(socket) == "" {
		return fmt.Errorf("dashboard.cgroup_resources requires dashboard.socket")
	}
	return nil
}

// sampleCgroupResources is called only by the existing diagnostics owner.
func sampleCgroupResources(s *metrics.CgroupSampler) *dashboard.CgroupResources {
	if s == nil {
		return nil
	}
	o, supported := s.Sample()
	if !supported {
		at := time.Now()
		o = metrics.CgroupObservation{StartedAt: at, SampledAt: at, UnsupportedLayout: true}
	}
	return cgroupResourcesMeasurement(o)
}

func cgroupResourcesMeasurement(o metrics.CgroupObservation) *dashboard.CgroupResources {
	if o.StartedAt.IsZero() || o.SampledAt.IsZero() {
		return nil
	}
	seconds := o.SampledAt.Sub(o.StartedAt).Seconds()
	if seconds < 0 {
		return nil
	}
	c := &dashboard.CgroupResources{SampledAt: o.SampledAt.UTC(), CaptureStartedAt: o.StartedAt.UTC(),
		CaptureDurationSeconds: seconds, BindingState: "unavailable", CPUQuota: dashboard.CgroupLimit{State: "unknown"},
		MemoryHighBytes: dashboard.CgroupLimit{State: "unknown"}, MemoryMaxBytes: dashboard.CgroupLimit{State: "unknown"},
		SwapMaxBytes: dashboard.CgroupLimit{State: "unknown"}}
	if seconds > 1 {
		return c
	}
	if o.UnsupportedLayout {
		c.BindingState = "unsupported"
		return c
	}
	if !o.HasBinding {
		return c
	}
	c.BindingState = "known"
	c.CPUQuota = cgroupLimitMeasurement(o.CPUQuota.Quota)
	if c.CPUQuota.State != "unknown" {
		period := o.CPUQuota.PeriodMicroseconds
		c.CPUPeriodMicroseconds = &period
	}
	c.EffectiveCPUs = cgroupValueMeasurement(o.EffectiveCPUs)
	c.MemoryCurrentBytes, c.SwapCurrentBytes = cgroupValueMeasurement(o.MemoryCurrent), cgroupValueMeasurement(o.SwapCurrent)
	c.MemoryHighBytes, c.MemoryMaxBytes, c.SwapMaxBytes = cgroupLimitMeasurement(o.MemoryHigh), cgroupLimitMeasurement(o.MemoryMax), cgroupLimitMeasurement(o.SwapMax)
	c.Counters = cgroupCountersMeasurement(o.Counters)
	c.Pressure = dashboard.CgroupPressureScopes{CPU: cgroupPressureMeasurement(o.Pressure[0]),
		Memory: cgroupPressureMeasurement(o.Pressure[1]), IO: cgroupPressureMeasurement(o.Pressure[2])}
	if o.HasWindow {
		w := o.Window
		c.Window = &dashboard.CgroupWindow{WindowStart: w.StartedAt.UTC(), WindowEnd: w.EndedAt.UTC(),
			DurationSeconds: w.Seconds, Counters: cgroupCountersMeasurement(w.Counters)}
	}
	return c
}

func cgroupLimitMeasurement(l metrics.CgroupLimit) dashboard.CgroupLimit {
	switch l.State {
	case metrics.CgroupLimitFinite:
		value := l.Value
		return dashboard.CgroupLimit{State: "finite", Value: &value}
	case metrics.CgroupLimitUnlimited:
		return dashboard.CgroupLimit{State: "max"}
	default:
		return dashboard.CgroupLimit{State: "unknown"}
	}
}

func cgroupValueMeasurement(v metrics.CgroupValue) *uint64 {
	if !v.Known {
		return nil
	}
	return &v.Value
}

func cgroupCountersMeasurement(c metrics.CgroupCounters) dashboard.CgroupCounters {
	return dashboard.CgroupCounters{CPUUsageMicroseconds: cgroupValueMeasurement(c.CPUUsage),
		CPUUserMicroseconds: cgroupValueMeasurement(c.CPUUser), CPUSystemMicroseconds: cgroupValueMeasurement(c.CPUSystem),
		Periods: cgroupValueMeasurement(c.Periods), ThrottledPeriods: cgroupValueMeasurement(c.ThrottledPeriods),
		ThrottledMicroseconds: cgroupValueMeasurement(c.ThrottledMicroseconds), MemoryHigh: cgroupValueMeasurement(c.MemoryHigh),
		MemoryMax: cgroupValueMeasurement(c.MemoryMax), OOM: cgroupValueMeasurement(c.OOM), OOMKill: cgroupValueMeasurement(c.OOMKill)}
}

func cgroupPressureMeasurement(p metrics.CgroupPressure) dashboard.CgroupPressure {
	line := func(l metrics.CgroupPressureLine, known bool) *dashboard.CgroupPressureLine {
		if !known {
			return nil
		}
		return &dashboard.CgroupPressureLine{Average10: l.Average10, Average60: l.Average60,
			Average300: l.Average300, TotalMicroseconds: l.TotalMicroseconds}
	}
	return dashboard.CgroupPressure{Some: line(p.Some, p.HasSome), Full: line(p.Full, p.HasFull)}
}
