package metrics

import (
	"bufio"
	"errors"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// HostObservation describes the node's visible Linux host, not its process or
// cgroup. Each scope is independently available and contains detached values.
type HostObservation struct {
	StartedAt, SampledAt       time.Time
	CPU                        HostCPU
	Load                       HostLoad
	Memory                     HostMemory
	HasCPU, HasLoad, HasMemory bool
}

// HostCPU contains aggregate ticks in user, nice, system, idle, iowait, irq,
// softirq and steal order. Guest ticks are already included in user/nice.
// ObservedAt is the local completion time of the bounded aggregate read.
type HostCPU struct {
	ObservedAt time.Time
	OnlineCPUs uint32
	Ticks      [8]uint64
	Rate       HostCPURate
	HasRate    bool
}

// HostCPURate uses the same observed CPU set, not process or cgroup utilization.
// Percent follows HostCPU.Ticks order; BusyPercent excludes idle/iowait/steal.
type HostCPURate struct {
	StartedAt, EndedAt time.Time
	Seconds            float64
	Percent            [8]float64
	BusyPercent        float64
}

// HostLoad contains Linux load averages and runnable/total task gauges.
type HostLoad struct {
	One, Five, Fifteen float64
	Runnable, Tasks    uint64
}

// HostMemory contains bytes, not the kernel text format's KiB. Optional fields
// remain unknown when absent; AvailableBytes is never estimated from cache.
type HostMemory struct {
	TotalBytes, FreeBytes                           uint64
	AvailableBytes, BuffersBytes, CachedBytes       uint64
	ReclaimableBytes, SwapTotalBytes, SwapFreeBytes uint64
	DirtyBytes, WritebackBytes                      uint64
	HasAvailable, HasBuffers, HasCached             bool
	HasReclaimable, HasSwapTotal, HasSwapFree       bool
	HasDirty, HasWriteback                          bool
}

// HostSampler is owned by one diagnostics goroutine. It starts with no baseline
// and never creates a timer. A nil sampler makes no clock or OS reads.
type HostSampler struct {
	previous HostCPU
	online   string
}

// NewHostSampler enables opt-in host observation without reading the OS.
func NewHostSampler(enabled bool) *HostSampler {
	if !enabled {
		return nil
	}
	return &HostSampler{}
}

// Sample returns false when disabled or unsupported. A supported but failed
// read returns true with the affected scope unknown, never a fabricated zero.
func (s *HostSampler) Sample() (HostObservation, bool) {
	if s == nil {
		return HostObservation{}, false
	}
	o, online, supported := readHost()
	if !supported {
		return HostObservation{}, false
	}
	return s.observe(o, online), true
}

func (s *HostSampler) observe(o HostObservation, online string) HostObservation {
	if !o.HasCPU {
		s.previous, s.online = HostCPU{}, ""
		return o
	}
	previous, previousOnline := s.previous, s.online
	s.previous, s.online = o.CPU, online
	seconds := o.CPU.ObservedAt.Sub(previous.ObservedAt).Seconds()
	if previousOnline == "" || previousOnline != online || seconds <= 0 || seconds > 30 {
		return o
	}
	var total uint64
	var delta [8]uint64
	for i, ticks := range o.CPU.Ticks {
		if ticks < previous.Ticks[i] {
			return o
		}
		delta[i] = ticks - previous.Ticks[i]
		if math.MaxUint64-total < delta[i] {
			return o
		}
		total += delta[i]
	}
	if total == 0 {
		return o
	}
	rate := HostCPURate{StartedAt: previous.ObservedAt, EndedAt: o.CPU.ObservedAt, Seconds: seconds}
	for i, ticks := range delta {
		rate.Percent[i] = float64(ticks) / float64(total) * 100
	}
	rate.BusyPercent = float64(delta[0]+delta[1]+delta[2]+delta[5]+delta[6]) / float64(total) * 100
	o.CPU.Rate, o.CPU.HasRate = rate, true
	return o
}

var errHostRecord = errors.New("invalid host observation record")

// hostCaptureBudget is one tenth of the diagnostics owner's ten-second cadence.
// It stops later reads; it cannot interrupt a read already blocked in the kernel.
const hostCaptureBudget = time.Second

// readHostFile bounds whole files with an extra byte; aggregate CPU reads stop
// at the first complete line, irrespective of the size of the per-CPU tail.
func readHostFile(path string, limit int, firstLine bool) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if firstLine {
		line, err := bufio.NewReaderSize(f, limit+1).ReadSlice('\n')
		if err != nil || len(line) > limit {
			return nil, errHostRecord
		}
		return line, nil
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if err != nil || len(data) > limit {
		return nil, errHostRecord
	}
	return data, nil
}

func readHostFiles(read func(string, int, bool) ([]byte, error), now func() time.Time) (HostObservation, string) {
	o := HostObservation{StartedAt: now()}
	last, expired := o.StartedAt, false
	withinBudget := func() bool {
		at := now()
		if at.Before(last) || at.Sub(o.StartedAt) > hostCaptureBudget {
			expired = true
		}
		last = at
		return !expired
	}
	readScope := func(path string, limit int, first bool) ([]byte, error) {
		if expired || !withinBudget() {
			return nil, errHostRecord
		}
		data, err := read(path, limit, first)
		if !withinBudget() {
			return nil, errHostRecord
		}
		return data, err
	}
	before, beforeErr := readScope("/sys/devices/system/cpu/online", 4096, false)
	cpu, cpuErr := readScope("/proc/stat", 4096, true)
	o.CPU.ObservedAt = last
	after, afterErr := readScope("/sys/devices/system/cpu/online", 4096, false)
	var online string
	if beforeErr == nil && cpuErr == nil && afterErr == nil {
		count, firstSet, firstErr := parseHostOnline(before)
		_, secondSet, secondErr := parseHostOnline(after)
		ticks, ticksErr := parseHostCPU(cpu)
		if firstErr == nil && secondErr == nil && ticksErr == nil && firstSet == secondSet {
			o.CPU.OnlineCPUs, o.CPU.Ticks, o.HasCPU, online = count, ticks, true, firstSet
		}
	}
	if data, err := readScope("/proc/loadavg", 1024, false); err == nil {
		o.Load, err = parseHostLoad(data)
		o.HasLoad = err == nil
	}
	if data, err := readScope("/proc/meminfo", 65536, false); err == nil {
		o.Memory, err = parseHostMemory(data)
		o.HasMemory = err == nil
	}
	valid := withinBudget()
	o.SampledAt = last
	if !valid {
		// Load and memory share the capture end, so an over-budget capture
		// cannot safely refresh any scope using that later timestamp.
		return HostObservation{StartedAt: o.StartedAt, SampledAt: o.SampledAt}, ""
	}
	return o, online
}

func parseHostCPU(data []byte) ([8]uint64, error) {
	var ticks [8]uint64
	text := string(data)
	fields := strings.Fields(text)
	if len(fields) < 9 || fields[0] != "cpu" || len(data) == 0 || strings.IndexByte(text, '\n') != len(data)-1 {
		return ticks, errHostRecord
	}
	for i, field := range fields[1:] {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return [8]uint64{}, errHostRecord
		}
		if i < len(ticks) {
			ticks[i] = value
		}
	}
	return ticks, nil
}

func parseHostOnline(data []byte) (uint32, string, error) {
	text := strings.TrimSpace(string(data))
	var count uint64
	var first, last uint64
	var normalized []byte
	flush := func() {
		if len(normalized) != 0 {
			normalized = append(normalized, ',')
		}
		normalized = strconv.AppendUint(normalized, first, 10)
		if last != first {
			normalized = append(normalized, '-')
			normalized = strconv.AppendUint(normalized, last, 10)
		}
	}
	for part := range strings.SplitSeq(text, ",") {
		left, right, ranged := strings.Cut(part, "-")
		start, err := strconv.ParseUint(left, 10, 32)
		end := start
		if err == nil && ranged {
			end, err = strconv.ParseUint(right, 10, 32)
		}
		if err != nil || end < start || count != 0 && start <= last || count+end-start+1 > math.MaxUint32 {
			return 0, "", errHostRecord
		}
		if count == 0 {
			first, last = start, end
		} else if start == last+1 {
			last = end
		} else {
			flush()
			first, last = start, end
		}
		count += end - start + 1
	}
	flush()
	return uint32(count), string(normalized), nil
}

func parseHostLoad(data []byte) (HostLoad, error) {
	var load HostLoad
	fields := strings.Fields(string(data))
	if len(fields) != 5 {
		return load, errHostRecord
	}
	for i, value := range []*float64{&load.One, &load.Five, &load.Fifteen} {
		n, err := strconv.ParseFloat(fields[i], 64)
		if err != nil || n < 0 || math.IsInf(n, 0) || math.IsNaN(n) {
			return HostLoad{}, errHostRecord
		}
		*value = n
	}
	runnable, tasks, ok := strings.Cut(fields[3], "/")
	var err error
	load.Runnable, err = strconv.ParseUint(runnable, 10, 64)
	if !ok || err != nil {
		return HostLoad{}, errHostRecord
	}
	load.Tasks, err = strconv.ParseUint(tasks, 10, 64)
	if err != nil || load.Tasks == 0 || load.Runnable > load.Tasks {
		return HostLoad{}, errHostRecord
	}
	if _, err := strconv.ParseUint(fields[4], 10, 64); err != nil {
		return HostLoad{}, errHostRecord
	}
	return load, nil
}

func parseHostMemory(data []byte) (HostMemory, error) {
	var memory HostMemory
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return memory, errHostRecord
	}
	var seen uint16
	for line := range strings.SplitSeq(string(data), "\n") {
		if line == "" {
			continue
		}
		key, rest, ok := strings.Cut(line, ":")
		if !ok {
			return HostMemory{}, errHostRecord
		}
		var target *uint64
		var known *bool
		var bit uint16
		switch key {
		case "MemTotal":
			target, bit = &memory.TotalBytes, 1
		case "MemFree":
			target, bit = &memory.FreeBytes, 2
		case "MemAvailable":
			target, known, bit = &memory.AvailableBytes, &memory.HasAvailable, 4
		case "Buffers":
			target, known, bit = &memory.BuffersBytes, &memory.HasBuffers, 8
		case "Cached":
			target, known, bit = &memory.CachedBytes, &memory.HasCached, 16
		case "SReclaimable":
			target, known, bit = &memory.ReclaimableBytes, &memory.HasReclaimable, 32
		case "SwapTotal":
			target, known, bit = &memory.SwapTotalBytes, &memory.HasSwapTotal, 64
		case "SwapFree":
			target, known, bit = &memory.SwapFreeBytes, &memory.HasSwapFree, 128
		case "Dirty":
			target, known, bit = &memory.DirtyBytes, &memory.HasDirty, 256
		case "Writeback":
			target, known, bit = &memory.WritebackBytes, &memory.HasWriteback, 512
		default:
			continue
		}
		fields := strings.Fields(rest)
		if seen&bit != 0 || len(fields) != 2 || fields[1] != "kB" {
			return HostMemory{}, errHostRecord
		}
		value, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil || value > math.MaxUint64/1024 {
			return HostMemory{}, errHostRecord
		}
		*target, seen = value*1024, seen|bit
		if known != nil {
			*known = true
		}
	}
	if seen&3 != 3 || memory.TotalBytes == 0 || memory.FreeBytes > memory.TotalBytes || memory.HasAvailable && memory.AvailableBytes > memory.TotalBytes || memory.HasSwapTotal && memory.HasSwapFree && memory.SwapFreeBytes > memory.SwapTotalBytes {
		return HostMemory{}, errHostRecord
	}
	return memory, nil
}
