package metrics

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const hostCPURecord = "cpu  100 20 30 400 5 6 7 8 90 10\n"
const hostMemoryRecord = "MemTotal: 1024 kB\nMemFree: 0 kB\nMemAvailable: 512 kB\nBuffers: 1 kB\nCached: 2 kB\nSReclaimable: 3 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\nDirty: 4 kB\nWriteback: 5 kB\nHugePages_Total: 0\n"

func hostFixtureRead(path string, _ int, _ bool) ([]byte, error) {
	switch path {
	case "/sys/devices/system/cpu/online":
		return []byte("0-3,8,10-11\n"), nil
	case "/proc/stat":
		return []byte(hostCPURecord), nil
	case "/proc/loadavg":
		return []byte("1.25 2.50 3.75 2/100 1234\n"), nil
	case "/proc/meminfo":
		return []byte(hostMemoryRecord), nil
	default:
		return nil, errors.New("unexpected path")
	}
}

func TestHostReadScopeAndCapture(t *testing.T) {
	at := time.Now()
	clock := make([]time.Time, 12)
	for i := range clock {
		clock[i] = at.Add(time.Duration(i) * time.Millisecond)
	}
	index, reads := 0, 0
	now := func() time.Time { value := clock[index]; index++; return value }
	read := func(path string, limit int, first bool) ([]byte, error) {
		reads++
		if first != (path == "/proc/stat") {
			t.Fatalf("first-line flag for %s", path)
		}
		wantLimit := map[string]int{"/sys/devices/system/cpu/online": 4096, "/proc/stat": 4096, "/proc/loadavg": 1024, "/proc/meminfo": 65536}[path]
		if limit != wantLimit {
			t.Fatalf("limit for %s = %d", path, limit)
		}
		return hostFixtureRead(path, limit, first)
	}
	o, online := readHostFiles(read, now)
	if reads != 5 || !o.HasCPU || !o.HasLoad || !o.HasMemory || online != "0-3,8,10-11" || o.CPU.OnlineCPUs != 7 {
		t.Fatalf("read = %+v, %q (%d reads)", o, online, reads)
	}
	if o.StartedAt != clock[0] || o.CPU.ObservedAt != clock[4] || o.SampledAt != clock[11] {
		t.Fatal("capture timestamps changed or lost monotonic data")
	}
	if o.CPU.Ticks != ([8]uint64{100, 20, 30, 400, 5, 6, 7, 8}) {
		t.Fatalf("ticks = %v", o.CPU.Ticks)
	}
	if o.Load != (HostLoad{One: 1.25, Five: 2.5, Fifteen: 3.75, Runnable: 2, Tasks: 100}) {
		t.Fatalf("load = %+v", o.Load)
	}
	m := o.Memory
	wantMemory := HostMemory{TotalBytes: 1 << 20, AvailableBytes: 512 << 10, BuffersBytes: 1 << 10, CachedBytes: 2 << 10,
		ReclaimableBytes: 3 << 10, DirtyBytes: 4 << 10, WritebackBytes: 5 << 10,
		HasAvailable: true, HasBuffers: true, HasCached: true, HasReclaimable: true,
		HasSwapTotal: true, HasSwapFree: true, HasDirty: true, HasWriteback: true}
	if m != wantMemory {
		t.Fatalf("memory = %+v", m)
	}
	for _, failed := range []string{"/proc/stat", "/sys/devices/system/cpu/online", "/proc/loadavg", "/proc/meminfo"} {
		t.Run(failed, func(t *testing.T) {
			got, _ := readHostFiles(func(path string, limit int, first bool) ([]byte, error) {
				if path == failed {
					return nil, os.ErrPermission
				}
				return hostFixtureRead(path, limit, first)
			}, time.Now)
			if got.HasCPU != (failed != "/proc/stat" && failed != "/sys/devices/system/cpu/online") || got.HasLoad != (failed != "/proc/loadavg") || got.HasMemory != (failed != "/proc/meminfo") {
				t.Fatalf("failure contaminated scopes: %+v", got)
			}
		})
	}
	reads = 0
	got, _ := readHostFiles(func(path string, limit int, first bool) ([]byte, error) {
		reads++
		if reads == 3 {
			return []byte("0-2,8-11\n"), nil
		}
		return hostFixtureRead(path, limit, first)
	}, time.Now)
	if got.HasCPU || !got.HasLoad || !got.HasMemory {
		t.Fatalf("hotplug during capture = %+v", got)
	}
	index = 0
	clock[11] = at.Add(-time.Second)
	got, _ = readHostFiles(hostFixtureRead, now)
	if got.HasCPU || got.HasLoad || got.HasMemory {
		t.Fatal("reversed capture clock accepted")
	}
}

func TestHostCaptureBudget(t *testing.T) {
	base := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	for slowRead := 1; slowRead <= 5; slowRead++ {
		t.Run(strconv.Itoa(slowRead), func(t *testing.T) {
			clock, reads := base, 0
			o, online := readHostFiles(func(path string, limit int, first bool) ([]byte, error) {
				reads++
				if reads == slowRead {
					clock = clock.Add(time.Second + time.Nanosecond)
				}
				return hostFixtureRead(path, limit, first)
			}, func() time.Time { return clock })
			if reads != slowRead || o.HasCPU || o.HasLoad || o.HasMemory || online != "" || o.StartedAt != base || o.SampledAt != clock {
				t.Fatalf("expired capture = %+v, online=%q, reads=%d want=%d", o, online, reads, slowRead)
			}
			var sampler HostSampler
			valid, cpus := readHostFiles(hostFixtureRead, func() time.Time { return base })
			sampler.observe(valid, cpus)
			sampler.observe(o, online)
			if !sampler.previous.ObservedAt.IsZero() || sampler.online != "" {
				t.Fatal("expired capture retained a CPU baseline")
			}
			valid.CPU.ObservedAt = base.Add(10 * time.Second)
			if sampler.observe(valid, cpus).CPU.HasRate {
				t.Fatal("recovered sample bridged expired capture")
			}
		})
	}
	for _, elapsed := range []time.Duration{time.Second, time.Second + time.Nanosecond} {
		clock := base
		o, _ := readHostFiles(func(path string, limit int, first bool) ([]byte, error) {
			if path == "/proc/meminfo" {
				clock = base.Add(elapsed)
			}
			return hostFixtureRead(path, limit, first)
		}, func() time.Time { return clock })
		if o.HasCPU != (elapsed == time.Second) || o.HasLoad != (elapsed == time.Second) || o.HasMemory != (elapsed == time.Second) {
			t.Fatalf("capture at %s = %+v", elapsed, o)
		}
	}
	reads, clockReads := 0, 0
	o, _ := readHostFiles(func(path string, limit int, first bool) ([]byte, error) {
		reads++
		return hostFixtureRead(path, limit, first)
	}, func() time.Time {
		clockReads++
		if clockReads >= 4 {
			return base.Add(2 * time.Second)
		}
		return base
	})
	if reads != 1 || o.HasCPU || o.HasLoad || o.HasMemory {
		t.Fatalf("between-read deadline was ignored: %+v, reads=%d", o, reads)
	}
}

func TestHostCPUIntervals(t *testing.T) {
	at := time.Now()
	initial := HostObservation{HasCPU: true, CPU: HostCPU{ObservedAt: at, OnlineCPUs: 2, Ticks: [8]uint64{10, 10, 10, 10, 10, 10, 10, 10}}}
	next := initial
	next.CPU.ObservedAt = at.Add(10 * time.Second)
	next.CPU.Ticks = [8]uint64{20, 15, 25, 60, 15, 15, 15, 15}
	var s HostSampler
	if got := s.observe(initial, "0-1"); got.CPU.HasRate {
		t.Fatal("warmup fabricated rate")
	}
	got := s.observe(next, "0-1")
	if !got.CPU.HasRate || got.CPU.Rate.Seconds != 10 || got.CPU.Rate.Percent != ([8]float64{10, 5, 15, 50, 5, 5, 5, 5}) || got.CPU.Rate.BusyPercent != 40 {
		t.Fatalf("rate = %+v", got.CPU)
	}
	if got.CPU.Rate.StartedAt != at || got.CPU.Rate.EndedAt != next.CPU.ObservedAt {
		t.Fatal("interval lost original time values")
	}
	got.CPU.Ticks[0] = math.MaxUint64
	if s.previous.Ticks != next.CPU.Ticks {
		t.Fatal("returned observation aliases baseline")
	}
	for i := range initial.CPU.Ticks {
		var sampler HostSampler
		sampler.observe(initial, "0-1")
		candidate := next
		candidate.CPU.Ticks[i] = initial.CPU.Ticks[i] - 1
		if sampler.observe(candidate, "0-1").CPU.HasRate {
			t.Fatalf("decreased counter %d accepted", i)
		}
	}
	for _, test := range []struct {
		name, set string
		change    func(*HostObservation)
	}{
		{"same count different CPUs", "2-3", func(*HostObservation) {}},
		{"same time", "0-1", func(o *HostObservation) { o.CPU.ObservedAt = at }},
		{"reversed time", "0-1", func(o *HostObservation) { o.CPU.ObservedAt = at.Add(-time.Second) }},
		{"old baseline", "0-1", func(o *HostObservation) { o.CPU.ObservedAt = at.Add(30*time.Second + time.Nanosecond) }},
		{"decreased iowait", "0-1", func(o *HostObservation) { o.CPU.Ticks[4] = 9 }},
		{"unchanged counters", "0-1", func(o *HostObservation) { o.CPU.Ticks = initial.CPU.Ticks }},
		{"overflow", "0-1", func(o *HostObservation) {
			o.CPU.Ticks = [8]uint64{math.MaxUint64, math.MaxUint64, 10, 10, 10, 10, 10, 10}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var sampler HostSampler
			sampler.observe(initial, "0-1")
			candidate := next
			test.change(&candidate)
			if observed := sampler.observe(candidate, test.set); observed.CPU.HasRate {
				t.Fatalf("invalid rate = %+v", observed.CPU.Rate)
			}
			if sampler.previous.ObservedAt != candidate.CPU.ObservedAt {
				t.Fatal("valid current baseline not retained")
			}
			candidate.CPU.ObservedAt = candidate.CPU.ObservedAt.Add(10 * time.Second)
			candidate.CPU.Ticks[7]++
			if !sampler.observe(candidate, test.set).CPU.HasRate {
				t.Fatal("next valid interval did not recover")
			}
		})
	}
	s.observe(HostObservation{HasLoad: true}, "")
	if s.online != "" || !s.previous.ObservedAt.IsZero() {
		t.Fatal("unknown CPU retained old baseline")
	}
	if s.observe(next, "0-1").CPU.HasRate {
		t.Fatal("rate bridged unavailable CPU sample")
	}
	last := next
	last.CPU.ObservedAt = last.CPU.ObservedAt.Add(10 * time.Second)
	last.CPU.Ticks[0]++
	if !s.observe(last, "0-1").CPU.HasRate {
		t.Fatal("CPU did not recover after fresh baseline")
	}
	var boundary HostSampler
	boundary.observe(initial, "0-1")
	next.CPU.ObservedAt = at.Add(30 * time.Second)
	if !boundary.observe(next, "0-1").CPU.HasRate {
		t.Fatal("exact thirty-second interval rejected")
	}
}

func TestHostRecordParsers(t *testing.T) {
	for _, input := range []string{"", "cpu 1 2 3\n", "cpu0 1 2 3 4 5 6 7 8\n", "cpu 1 2 3 4 5 6 7 -8\n", "cpu 1 2 3 4 5 6 7 18446744073709551616\n", strings.TrimSuffix(hostCPURecord, "\n"), hostCPURecord + "cpu0 1\n", hostCPURecord + "1 2 3\n"} {
		if _, err := parseHostCPU([]byte(input)); err == nil {
			t.Fatalf("accepted CPU %q", input)
		}
	}
	for _, test := range []struct {
		input, want string
		count       uint32
	}{{"0\n", "0", 1}, {"0,1,2-3,8,10-11\n", "0-3,8,10-11", 7}, {"4294967295\n", "4294967295", 1}} {
		count, got, err := parseHostOnline([]byte(test.input))
		if err != nil || got != test.want || count != test.count {
			t.Fatalf("online %q = %d,%q,%v", test.input, count, got, err)
		}
	}
	for _, input := range []string{"", "1-0", "0,0", "1,0", "0-2,2-4", "0,,2", "1-2-3", "4294967296", "0-4294967295", "0, 2", "-1"} {
		if _, _, err := parseHostOnline([]byte(input)); err == nil {
			t.Fatalf("accepted online %q", input)
		}
	}
	for _, input := range []string{"NaN 0 0 1/2 3", "+Inf 0 0 1/2 3", "-1 0 0 1/2 3", "0 0 0 3/2 1", "0 0 0 1/0 1", "0 0 0 1/2/3 1", "0 0 0 1/2 x", "0 0 0 1/2", "0 0 0 1/2 3 extra"} {
		if _, err := parseHostLoad([]byte(input)); err == nil {
			t.Fatalf("accepted load %q", input)
		}
	}
	minimal, err := parseHostMemory([]byte("MemTotal: 1 kB\nMemFree: 0 kB\n"))
	if err != nil || minimal.HasAvailable || minimal.HasCached || minimal.HasSwapTotal || minimal.TotalBytes != 1024 {
		t.Fatalf("minimal memory = %+v,%v", minimal, err)
	}
	for _, input := range []string{"", "MemTotal: 1 kB\n", "MemTotal: 0 kB\nMemFree: 0 kB\n", "MemTotal: 1 kB\nMemFree: 2 kB\n", "MemTotal: 1 kB\nMemFree: 0 kB\nMemAvailable: 2 kB\n", hostMemoryRecord + "MemFree: 0 kB\n", strings.Replace(hostMemoryRecord, "512 kB", "512 MB", 1), strings.Replace(hostMemoryRecord, "512 kB", "18014398509481984 kB", 1), strings.TrimSuffix(hostMemoryRecord, "\n"), hostMemoryRecord + "MemAvailable\n", strings.Replace(hostMemoryRecord, "SwapFree: 0", "SwapFree: 1", 1)} {
		if _, err := parseHostMemory([]byte(input)); err == nil {
			t.Fatalf("accepted memory %q", input)
		}
	}
}

func TestHostFileBounds(t *testing.T) {
	dir := t.TempDir()
	for _, test := range []struct {
		name, data    string
		limit         int
		first, wantOK bool
		want          string
	}{
		{"whole", "1234", 4, false, true, "1234"},
		{"whole excess", "12345", 4, false, false, ""},
		{"first line", "123\n" + strings.Repeat("tail\n", 20000), 4, true, true, "123\n"},
		{"first excess", "1234\ntail", 4, true, false, ""},
		{"first incomplete", "123", 4, true, false, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(dir, test.name)
			if err := os.WriteFile(path, []byte(test.data), 0600); err != nil {
				t.Fatal(err)
			}
			got, err := readHostFile(path, test.limit, test.first)
			if (err == nil) != test.wantOK || test.wantOK && string(got) != test.want {
				t.Fatalf("read = %q,%v", got, err)
			}
		})
	}
	if _, err := readHostFile(filepath.Join(dir, "absent"), 64, false); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file = %v", err)
	}
}
