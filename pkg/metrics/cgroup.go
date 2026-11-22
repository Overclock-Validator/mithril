package metrics

import (
	"errors"
	"math"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	cgroupFileLimit     = 8 << 10
	cgroupMountLimit    = 256 << 10
	cgroupCaptureBudget = time.Second
)

var errCgroupRecord = errors.New("invalid cgroup observation record")

// CgroupValue distinguishes a known zero from an unavailable scalar.
type CgroupValue struct {
	Value uint64
	Known bool
}

// CgroupLimitState describes a local setting, not an effective ancestor limit.
type CgroupLimitState uint8

const (
	CgroupLimitUnknown CgroupLimitState = iota
	CgroupLimitFinite
	CgroupLimitUnlimited
)

// CgroupLimit contains a numeric local limit or the kernel's explicit max state.
type CgroupLimit struct {
	Value uint64
	State CgroupLimitState
}

// CgroupCPUQuota is the local quota and period in microseconds.
type CgroupCPUQuota struct {
	Quota              CgroupLimit
	PeriodMicroseconds uint64
}

// CgroupCounters contains cumulative CPU accounting and local memory events.
// CPU times are microseconds. Periods and memory events are counts.
type CgroupCounters struct {
	CPUUsage, CPUUser, CPUSystem                     CgroupValue
	Periods, ThrottledPeriods, ThrottledMicroseconds CgroupValue
	MemoryHigh, MemoryMax, OOM, OOMKill              CgroupValue
}

// CgroupPressureLine contains kernel stall averages in percent and total usec.
type CgroupPressureLine struct {
	Average10, Average60, Average300 float64
	TotalMicroseconds                uint64
}

// CgroupPressure keeps independently available some and full stall records.
type CgroupPressure struct {
	Some, Full       CgroupPressureLine
	HasSome, HasFull bool
}

// CgroupWindow contains counter differences for one unchanged group binding.
type CgroupWindow struct {
	StartedAt, EndedAt time.Time
	Seconds            float64
	Counters           CgroupCounters
}

// CgroupObservation describes only this process's visible domain cgroup v2.
// HasBinding does not imply all controllers are present. Usage can include
// descendants; local limits do not establish effective capacity or headroom.
type CgroupObservation struct {
	StartedAt, SampledAt           time.Time
	HasBinding, UnsupportedLayout  bool
	CPUQuota                       CgroupCPUQuota
	EffectiveCPUs                  CgroupValue
	MemoryCurrent, SwapCurrent     CgroupValue
	MemoryHigh, MemoryMax, SwapMax CgroupLimit
	Counters                       CgroupCounters
	// Pressure is CPU, memory and I/O, in that order.
	Pressure  [3]CgroupPressure
	Window    CgroupWindow
	HasWindow bool
}

type cgroupLocation struct {
	membership, mountRoot, mountPoint, directory string
	mountID                                      uint64
}

type cgroupBinding struct {
	location                               cgroupLocation
	mountDevice, mountInode, device, inode uint64
}

// cgroupAccess holds one directory open throughout capture. Its closures are
// the small platform I/O boundary; parsers never inspect arbitrary paths.
type cgroupAccess struct {
	binding  cgroupBinding
	read     func(string) ([]byte, error)
	validate func() bool
	close    func()
}

// CgroupSampler belongs to one diagnostics goroutine and creates no timers.
type CgroupSampler struct {
	previous CgroupObservation
	binding  cgroupBinding
}

// NewCgroupSampler does not read the OS. Disabled sampling allocates nothing.
func NewCgroupSampler(enabled bool) *CgroupSampler {
	if !enabled {
		return nil
	}
	return &CgroupSampler{}
}

// Sample reports false when disabled or unsupported by this operating system.
// A supported OS with unavailable data returns true and explicit unknowns.
func (s *CgroupSampler) Sample() (CgroupObservation, bool) {
	if s == nil {
		return CgroupObservation{}, false
	}
	o, binding, supported := readCgroup()
	if !supported {
		return CgroupObservation{}, false
	}
	return s.observe(o, binding), true
}

func (s *CgroupSampler) observe(o CgroupObservation, binding cgroupBinding) CgroupObservation {
	previous, previousBinding := s.previous, s.binding
	s.previous, s.binding = o, binding
	if !o.HasBinding {
		s.previous, s.binding = CgroupObservation{}, cgroupBinding{}
		return o
	}
	seconds := o.SampledAt.Sub(previous.SampledAt).Seconds()
	if !previous.HasBinding || binding != previousBinding || seconds <= 0 || seconds > 30 {
		return o
	}
	current, old := cgroupCounterFields(&o.Counters), cgroupCounterFields(&previous.Counters)
	var delta CgroupCounters
	fields := cgroupCounterFields(&delta)
	known := false
	for i, value := range current {
		if !value.Known || !old[i].Known {
			continue
		}
		if value.Value < old[i].Value {
			return o
		}
		*fields[i] = CgroupValue{Value: value.Value - old[i].Value, Known: true}
		known = true
	}
	if known {
		o.Window = CgroupWindow{StartedAt: previous.SampledAt, EndedAt: o.SampledAt, Seconds: seconds, Counters: delta}
		o.HasWindow = true
	}
	return o
}

func cgroupCounterFields(c *CgroupCounters) [10]*CgroupValue {
	return [10]*CgroupValue{&c.CPUUsage, &c.CPUUser, &c.CPUSystem, &c.Periods, &c.ThrottledPeriods,
		&c.ThrottledMicroseconds, &c.MemoryHigh, &c.MemoryMax, &c.OOM, &c.OOMKill}
}

func readCgroupFiles(readSelf func(string, int) ([]byte, error), open func(cgroupLocation) (cgroupAccess, error), now func() time.Time) (CgroupObservation, cgroupBinding) {
	o := CgroupObservation{StartedAt: now()}
	last, expired := o.StartedAt, false
	withinBudget := func() bool {
		at := now()
		if at.Before(last) || at.Sub(o.StartedAt) > cgroupCaptureBudget {
			expired = true
		}
		last = at
		return !expired
	}
	read := func(name string, limit int) ([]byte, error) {
		if !withinBudget() {
			return nil, errCgroupRecord
		}
		data, err := readSelf(name, limit)
		if !withinBudget() || len(data) > limit {
			return nil, errCgroupRecord
		}
		return data, err
	}
	resolve := func() (cgroupLocation, error) {
		membership, err := read("/proc/self/cgroup", cgroupFileLimit)
		if err != nil {
			return cgroupLocation{}, err
		}
		mounts, err := read("/proc/self/mountinfo", cgroupMountLimit)
		if err != nil {
			return cgroupLocation{}, err
		}
		return resolveCgroup(membership, mounts)
	}
	finishUnknown := func(unsupported bool) (CgroupObservation, cgroupBinding) {
		withinBudget()
		return CgroupObservation{StartedAt: o.StartedAt, SampledAt: last, UnsupportedLayout: unsupported && !expired}, cgroupBinding{}
	}
	location, err := resolve()
	if err != nil || !withinBudget() {
		return finishUnknown(false)
	}
	a, err := open(location)
	if err != nil {
		return finishUnknown(false)
	}
	defer a.close()
	if !withinBudget() {
		return finishUnknown(false)
	}
	readGroup := func(name string) ([]byte, error) {
		if !withinBudget() {
			return nil, errCgroupRecord
		}
		data, err := a.read(name)
		if !withinBudget() || len(data) > cgroupFileLimit {
			return nil, errCgroupRecord
		}
		return data, err
	}
	typeBefore, err := readGroup("cgroup.type")
	if err != nil {
		return finishUnknown(false)
	}
	if strings.TrimSpace(string(typeBefore)) != "domain" {
		return finishUnknown(true)
	}
	if data, err := readGroup("cpu.stat"); err == nil {
		parseCgroupCPU(data, &o.Counters)
	}
	if data, err := readGroup("cpu.max"); err == nil {
		o.CPUQuota = parseCgroupQuota(data)
	}
	if data, err := readGroup("cpuset.cpus.effective"); err == nil {
		if count, _, err := parseHostOnline(data); err == nil {
			o.EffectiveCPUs = CgroupValue{Value: uint64(count), Known: true}
		}
	}
	for _, field := range []struct {
		name   string
		target *CgroupValue
	}{
		{"memory.current", &o.MemoryCurrent}, {"memory.swap.current", &o.SwapCurrent},
	} {
		if data, err := readGroup(field.name); err == nil {
			*field.target = parseCgroupValue(data)
		}
	}
	for _, field := range []struct {
		name   string
		target *CgroupLimit
	}{
		{"memory.high", &o.MemoryHigh}, {"memory.max", &o.MemoryMax}, {"memory.swap.max", &o.SwapMax},
	} {
		if data, err := readGroup(field.name); err == nil {
			*field.target = parseCgroupLimit(data)
		}
	}
	if data, err := readGroup("memory.events.local"); err == nil {
		parseCgroupEvents(data, &o.Counters)
	}
	for i, name := range [...]string{"cpu.pressure", "memory.pressure", "io.pressure"} {
		if data, err := readGroup(name); err == nil {
			o.Pressure[i] = parseCgroupPressure(data)
		}
	}
	typeAfter, err := readGroup("cgroup.type")
	if err != nil || strings.TrimSpace(string(typeAfter)) != "domain" {
		return finishUnknown(false)
	}
	after, err := resolve()
	if err != nil || after != location || !withinBudget() {
		return finishUnknown(false)
	}
	valid := a.validate()
	if !withinBudget() || !valid {
		return finishUnknown(false)
	}
	o.SampledAt, o.HasBinding = last, true
	return o, a.binding
}

func parseCgroupValue(data []byte) CgroupValue {
	text := strings.TrimSpace(string(data))
	if text == "" {
		return CgroupValue{}
	}
	for _, c := range text {
		if c < '0' || c > '9' {
			return CgroupValue{}
		}
	}
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return CgroupValue{}
	}
	return CgroupValue{Value: value, Known: true}
}

func parseCgroupLimit(data []byte) CgroupLimit {
	if strings.TrimSpace(string(data)) == "max" {
		return CgroupLimit{State: CgroupLimitUnlimited}
	}
	if value := parseCgroupValue(data); value.Known {
		return CgroupLimit{Value: value.Value, State: CgroupLimitFinite}
	}
	return CgroupLimit{}
}

func parseCgroupQuota(data []byte) CgroupCPUQuota {
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return CgroupCPUQuota{}
	}
	quota, period := parseCgroupLimit([]byte(fields[0])), parseCgroupValue([]byte(fields[1]))
	if quota.State == CgroupLimitUnknown || quota.State == CgroupLimitFinite && quota.Value == 0 || !period.Known || period.Value == 0 {
		return CgroupCPUQuota{}
	}
	return CgroupCPUQuota{Quota: quota, PeriodMicroseconds: period.Value}
}

func parseCgroupKeyed(data []byte, keys []string) ([]CgroupValue, bool) {
	values := make([]CgroupValue, len(keys))
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return nil, false
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		for i, key := range keys {
			if fields[0] != key {
				continue
			}
			if len(fields) != 2 || values[i].Known {
				return nil, false
			}
			values[i] = parseCgroupValue([]byte(fields[1]))
			if !values[i].Known {
				return nil, false
			}
		}
	}
	return values, true
}

func parseCgroupCPU(data []byte, c *CgroupCounters) {
	values, valid := parseCgroupKeyed(data, []string{"usage_usec", "user_usec", "system_usec", "nr_periods", "nr_throttled", "throttled_usec"})
	if !valid || !values[0].Known || !values[1].Known || !values[2].Known {
		return
	}
	fields := cgroupCounterFields(c)
	for i, value := range values {
		*fields[i] = value
	}
}

func parseCgroupEvents(data []byte, c *CgroupCounters) {
	values, valid := parseCgroupKeyed(data, []string{"high", "max", "oom", "oom_kill"})
	if !valid {
		return
	}
	fields := cgroupCounterFields(c)
	for i, value := range values {
		*fields[i+6] = value
	}
}

func parseCgroupPressure(data []byte) CgroupPressure {
	var result CgroupPressure
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return result
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		var target *CgroupPressureLine
		var known *bool
		switch fields[0] {
		case "some":
			target, known = &result.Some, &result.HasSome
		case "full":
			target, known = &result.Full, &result.HasFull
		default:
			continue
		}
		if *known || len(fields) < 5 {
			return CgroupPressure{}
		}
		var seen uint8
		for _, field := range fields[1:] {
			key, text, ok := strings.Cut(field, "=")
			if !ok {
				return CgroupPressure{}
			}
			var bit uint8
			var average *float64
			switch key {
			case "avg10":
				bit, average = 1, &target.Average10
			case "avg60":
				bit, average = 2, &target.Average60
			case "avg300":
				bit, average = 4, &target.Average300
			case "total":
				bit = 8
			default:
				continue
			}
			if seen&bit != 0 {
				return CgroupPressure{}
			}
			seen |= bit
			if average != nil {
				v, err := strconv.ParseFloat(text, 64)
				if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 100 {
					return CgroupPressure{}
				}
				*average = v
			} else {
				v := parseCgroupValue([]byte(text))
				if !v.Known {
					return CgroupPressure{}
				}
				target.TotalMicroseconds = v.Value
			}
		}
		if seen != 15 {
			return CgroupPressure{}
		}
		*known = true
	}
	return result
}

func resolveCgroup(membership, mounts []byte) (cgroupLocation, error) {
	var member string
	if len(membership) == 0 || len(membership) > cgroupFileLimit || membership[len(membership)-1] != '\n' || len(mounts) == 0 || len(mounts) > cgroupMountLimit || mounts[len(mounts)-1] != '\n' {
		return cgroupLocation{}, errCgroupRecord
	}
	for line := range strings.SplitSeq(string(membership), "\n") {
		if !strings.HasPrefix(line, "0::") {
			continue
		}
		if member != "" {
			return cgroupLocation{}, errCgroupRecord
		}
		member = strings.TrimPrefix(line, "0::")
		if !validCgroupPath(member) {
			return cgroupLocation{}, errCgroupRecord
		}
	}
	if member == "" {
		return cgroupLocation{}, errCgroupRecord
	}
	var selected cgroupLocation
	for line := range strings.SplitSeq(string(mounts), "\n") {
		fields := strings.Fields(line)
		separator := -1
		for i, f := range fields {
			if f == "-" {
				separator = i
				break
			}
		}
		if separator < 6 || len(fields) < separator+4 || fields[separator+1] != "cgroup2" {
			continue
		}
		root, err := unescapeCgroupMount(fields[3])
		if err != nil {
			return cgroupLocation{}, err
		}
		point, err := unescapeCgroupMount(fields[4])
		if err != nil || !validCgroupPath(root) || !validCgroupPath(point) {
			return cgroupLocation{}, errCgroupRecord
		}
		if root != "/" && member != root && !strings.HasPrefix(member, root+"/") {
			continue
		}
		id := parseCgroupValue([]byte(fields[0]))
		if !id.Known || id.Value == 0 || selected.mountID != 0 {
			return cgroupLocation{}, errCgroupRecord
		}
		directory := path.Join(point, strings.TrimPrefix(member, root))
		if !validCgroupPath(directory) {
			return cgroupLocation{}, errCgroupRecord
		}
		selected = cgroupLocation{membership: member, mountRoot: root, mountPoint: point, directory: directory, mountID: id.Value}
	}
	if selected.mountID == 0 {
		return cgroupLocation{}, errCgroupRecord
	}
	return selected, nil
}

func validCgroupPath(value string) bool {
	return len(value) > 0 && len(value) <= 4096 && strings.HasPrefix(value, "/") && path.Clean(value) == value && strings.Count(value, "/") <= 128 && !strings.ContainsAny(value, "\x00\n\r")
}

func unescapeCgroupMount(value string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] != '\\' {
			b.WriteByte(value[i])
			continue
		}
		if i+3 >= len(value) {
			return "", errCgroupRecord
		}
		var c byte
		switch value[i+1 : i+4] {
		case "040":
			c = ' '
		case "011":
			c = '\t'
		case "012":
			c = '\n'
		case "134":
			c = '\\'
		default:
			return "", errCgroupRecord
		}
		b.WriteByte(c)
		i += 3
	}
	return b.String(), nil
}
