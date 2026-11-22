package metrics

import (
	"math"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const cgroupTestMembership = "0::/slice/node\n"
const cgroupTestMount = "29 24 0:26 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw\n"
const cgroupTestPSI = "some avg10=1.25 avg60=2.50 avg300=3.75 total=18446744073709551615\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"

func cgroupFixture() map[string]string {
	return map[string]string{
		"cgroup.type": "domain\n",
		"cpu.stat":    "usage_usec 30\nuser_usec 20\nsystem_usec 10\nnr_periods 100\nnr_throttled 3\nthrottled_usec 500\nnew_key 5\n",
		"cpu.max":     "200000 100000\n", "cpuset.cpus.effective": "0-3,8\n",
		"memory.current": "2048\n", "memory.swap.current": "0\n",
		"memory.high": "1024\n", "memory.max": "max\n", "memory.swap.max": "0\n",
		"memory.events.local": "low 0\nhigh 4\nmax 5\noom 0\noom_kill 0\n",
		"cpu.pressure":        cgroupTestPSI, "memory.pressure": cgroupTestPSI, "io.pressure": cgroupTestPSI,
	}
}

func cgroupSelfFixture(name string, limit int) ([]byte, error) {
	switch name {
	case "/proc/self/cgroup":
		if limit != cgroupFileLimit {
			return nil, errCgroupRecord
		}
		return []byte(cgroupTestMembership), nil
	case "/proc/self/mountinfo":
		if limit != cgroupMountLimit {
			return nil, errCgroupRecord
		}
		return []byte(cgroupTestMount), nil
	}
	return nil, os.ErrNotExist
}

func cgroupFixtureOpen(files map[string]string, reads *int, closed *bool) func(cgroupLocation) (cgroupAccess, error) {
	return func(location cgroupLocation) (cgroupAccess, error) {
		return cgroupAccess{
			binding: cgroupBinding{location: location, mountDevice: 1, mountInode: 2, device: 1, inode: 3},
			read: func(name string) ([]byte, error) {
				if reads != nil {
					*reads++
				}
				data, exists := files[name]
				if !exists {
					return nil, os.ErrNotExist
				}
				return []byte(data), nil
			},
			validate: func() bool { return true },
			close: func() {
				if closed != nil {
					*closed = true
				}
			},
		}, nil
	}
}

func TestCgroupDisabledAndUnsupported(t *testing.T) {
	if NewCgroupSampler(false) != nil {
		t.Fatal("disabled sampler allocated")
	}
	var disabled *CgroupSampler
	if got, supported := disabled.Sample(); supported || got != (CgroupObservation{}) {
		t.Fatalf("disabled = %+v, %v", got, supported)
	}
	if n := testing.AllocsPerRun(100, func() { disabled.Sample() }); n != 0 {
		t.Fatalf("disabled allocations = %v", n)
	}
	if runtime.GOOS != "linux" {
		if got, supported := NewCgroupSampler(true).Sample(); supported || got != (CgroupObservation{}) {
			t.Fatalf("unsupported = %+v, %v", got, supported)
		}
	}
}

func TestCgroupMountResolution(t *testing.T) {
	for _, test := range []struct{ name, member, mount, want string }{
		{"root", "0::/\n", cgroupTestMount, "/sys/fs/cgroup"},
		{"child", cgroupTestMembership, cgroupTestMount, "/sys/fs/cgroup/slice/node"},
		{"subtree", cgroupTestMembership, strings.Replace(cgroupTestMount, " / /sys", " /slice /sys", 1), "/sys/fs/cgroup/node"},
		{"escaped mount", "0::/node\n", strings.Replace(cgroupTestMount, "/sys/fs/cgroup", "/a\\040b/cgroup", 1), "/a b/cgroup/node"},
		{"escaped root", "0::/a b/node\n", strings.Replace(cgroupTestMount, " / /sys", " /a\\040b /sys", 1), "/sys/fs/cgroup/node"},
		{"hybrid", "2:cpu:/old\n" + cgroupTestMembership, cgroupTestMount, "/sys/fs/cgroup/slice/node"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveCgroup([]byte(test.member), []byte(test.mount))
			if err != nil || got.directory != test.want || got.mountID != 29 {
				t.Fatalf("resolved = %+v, %v; want %s", got, err, test.want)
			}
		})
	}
	for _, test := range []struct{ name, member, mount string }{
		{"v1", "2:cpu:/node\n", cgroupTestMount},
		{"duplicate membership", cgroupTestMembership + cgroupTestMembership, cgroupTestMount},
		{"duplicate mount", cgroupTestMembership, cgroupTestMount + cgroupTestMount},
		{"outside root", "0::/sibling\n", strings.Replace(cgroupTestMount, " / /sys", " /slice /sys", 1)},
		{"prefix collision", "0::/slice-two\n", strings.Replace(cgroupTestMount, " / /sys", " /slice /sys", 1)},
		{"parent traversal", "0::/slice/../sibling\n", cgroupTestMount},
		{"relative", "0::slice/node\n", cgroupTestMount},
		{"double slash", "0::/slice//node\n", cgroupTestMount},
		{"NUL", "0::/slice/\x00\n", cgroupTestMount},
		{"missing newline", "0::/node", cgroupTestMount},
		{"bad escape", cgroupTestMembership, strings.Replace(cgroupTestMount, "/sys", "/a\\077b", 1)},
		{"escaped traversal", cgroupTestMembership, strings.Replace(cgroupTestMount, "/sys", "/a/../b", 1)},
		{"mount too large", cgroupTestMembership, strings.Repeat(" ", cgroupMountLimit) + cgroupTestMount},
		{"member too large", "0::/" + strings.Repeat("a", cgroupFileLimit) + "\n", cgroupTestMount},
		{"deep path", "0::/" + strings.Repeat("a/", 129) + "z\n", cgroupTestMount},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, err := resolveCgroup([]byte(test.member), []byte(test.mount)); err == nil {
				t.Fatalf("accepted %+v", got)
			}
		})
	}
}

func TestCgroupCaptureAndPartialScopes(t *testing.T) {
	at := time.Now()
	reads, closed := 0, false
	o, binding := readCgroupFiles(cgroupSelfFixture, cgroupFixtureOpen(cgroupFixture(), &reads, &closed), func() time.Time { return at })
	if !closed || reads != 14 || !o.HasBinding || o.UnsupportedLayout || o.StartedAt != at || o.SampledAt != at || binding.inode != 3 {
		t.Fatalf("capture = %+v; reads %d closed %v binding %+v", o, reads, closed, binding)
	}
	if o.CPUQuota != (CgroupCPUQuota{Quota: CgroupLimit{Value: 200000, State: CgroupLimitFinite}, PeriodMicroseconds: 100000}) || o.EffectiveCPUs != (CgroupValue{Value: 5, Known: true}) {
		t.Fatalf("CPU = %+v, %+v", o.CPUQuota, o.EffectiveCPUs)
	}
	if !o.MemoryCurrent.Known || o.MemoryCurrent.Value != 2048 || o.MemoryHigh.Value != 1024 || o.MemoryMax.State != CgroupLimitUnlimited || o.SwapMax != (CgroupLimit{State: CgroupLimitFinite}) {
		t.Fatalf("memory = %+v", o)
	}
	for i, psi := range o.Pressure {
		if !psi.HasSome || !psi.HasFull || psi.Some.TotalMicroseconds != math.MaxUint64 || psi.Full.TotalMicroseconds != 0 {
			t.Fatalf("pressure %d = %+v", i, psi)
		}
	}
	for name := range cgroupFixture() {
		if name == "cgroup.type" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			files := cgroupFixture()
			delete(files, name)
			got, _ := readCgroupFiles(cgroupSelfFixture, cgroupFixtureOpen(files, nil, nil), func() time.Time { return at })
			if !got.HasBinding {
				t.Fatalf("scope error invalidated binding: %s", name)
			}
			want := o
			switch name {
			case "cpu.stat":
				want.Counters.CPUUsage, want.Counters.CPUUser, want.Counters.CPUSystem, want.Counters.Periods, want.Counters.ThrottledPeriods, want.Counters.ThrottledMicroseconds = CgroupValue{}, CgroupValue{}, CgroupValue{}, CgroupValue{}, CgroupValue{}, CgroupValue{}
			case "cpu.max":
				want.CPUQuota = CgroupCPUQuota{}
			case "cpuset.cpus.effective":
				want.EffectiveCPUs = CgroupValue{}
			case "memory.current":
				want.MemoryCurrent = CgroupValue{}
			case "memory.swap.current":
				want.SwapCurrent = CgroupValue{}
			case "memory.high":
				want.MemoryHigh = CgroupLimit{}
			case "memory.max":
				want.MemoryMax = CgroupLimit{}
			case "memory.swap.max":
				want.SwapMax = CgroupLimit{}
			case "memory.events.local":
				want.Counters.MemoryHigh, want.Counters.MemoryMax, want.Counters.OOM, want.Counters.OOMKill = CgroupValue{}, CgroupValue{}, CgroupValue{}, CgroupValue{}
			case "cpu.pressure":
				want.Pressure[0] = CgroupPressure{}
			case "memory.pressure":
				want.Pressure[1] = CgroupPressure{}
			case "io.pressure":
				want.Pressure[2] = CgroupPressure{}
			}
			if got != want {
				t.Fatalf("unrelated scope changed: got %+v want %+v", got, want)
			}
		})
	}
	for _, kind := range []string{"threaded\n", "domain threaded\n", "domain invalid\n"} {
		files := cgroupFixture()
		files["cgroup.type"] = kind
		reads = 0
		got, _ := readCgroupFiles(cgroupSelfFixture, cgroupFixtureOpen(files, &reads, nil), func() time.Time { return at })
		if got.HasBinding || !got.UnsupportedLayout || reads != 1 {
			t.Fatalf("unsupported = %+v reads=%d", got, reads)
		}
	}
}

func TestCgroupMigrationReplacementAndBudget(t *testing.T) {
	base := time.Now()
	for _, mode := range []string{"membership", "mount", "replacement", "layout", "open failure"} {
		t.Run(mode, func(t *testing.T) {
			selfReads, groupReads, closed := 0, 0, false
			read := func(name string, limit int) ([]byte, error) {
				selfReads++
				data, err := cgroupSelfFixture(name, limit)
				if selfReads > 2 && mode == "membership" && name == "/proc/self/cgroup" {
					return []byte("0::/other\n"), nil
				}
				if selfReads > 2 && mode == "mount" && name == "/proc/self/mountinfo" {
					return []byte(strings.Replace(string(data), "29 ", "30 ", 1)), nil
				}
				return data, err
			}
			open := func(loc cgroupLocation) (cgroupAccess, error) {
				if mode == "open failure" {
					return cgroupAccess{}, os.ErrPermission
				}
				a, err := cgroupFixtureOpen(cgroupFixture(), nil, &closed)(loc)
				original := a.read
				a.read = func(name string) ([]byte, error) {
					groupReads++
					if mode == "layout" && groupReads == 14 {
						return []byte("threaded\n"), nil
					}
					return original(name)
				}
				a.validate = func() bool { return mode != "replacement" }
				return a, err
			}
			got, binding := readCgroupFiles(read, open, func() time.Time { return base })
			if got.HasBinding || binding != (cgroupBinding{}) || got.Counters != (CgroupCounters{}) || mode != "open failure" && !closed {
				t.Fatalf("invalid binding retained: %+v %+v closed=%v", got, binding, closed)
			}
		})
	}
	for slowRead := 1; slowRead <= 18; slowRead++ {
		t.Run("slow-"+strconv.Itoa(slowRead), func(t *testing.T) {
			clock, reads := base, 0
			step := func() {
				reads++
				if reads == slowRead {
					clock = clock.Add(time.Second + time.Nanosecond)
				}
			}
			read := func(name string, limit int) ([]byte, error) { step(); return cgroupSelfFixture(name, limit) }
			open := func(loc cgroupLocation) (cgroupAccess, error) {
				a, err := cgroupFixtureOpen(cgroupFixture(), nil, nil)(loc)
				original := a.read
				a.read = func(name string) ([]byte, error) { step(); return original(name) }
				return a, err
			}
			got, binding := readCgroupFiles(read, open, func() time.Time { return clock })
			if reads != slowRead || got.HasBinding || got.Counters != (CgroupCounters{}) || binding != (cgroupBinding{}) {
				t.Fatalf("budget read %d: reads=%d, %+v", slowRead, reads, got)
			}
		})
	}
	for _, elapsed := range []time.Duration{time.Second, time.Second + time.Nanosecond, -time.Nanosecond} {
		clock := base
		open := func(loc cgroupLocation) (cgroupAccess, error) {
			a, err := cgroupFixtureOpen(cgroupFixture(), nil, nil)(loc)
			a.validate = func() bool { clock = base.Add(elapsed); return true }
			return a, err
		}
		got, _ := readCgroupFiles(cgroupSelfFixture, open, func() time.Time { return clock })
		if got.HasBinding != (elapsed == time.Second) {
			t.Fatalf("boundary %s = %+v", elapsed, got)
		}
	}
}

func TestCgroupCounterWindows(t *testing.T) {
	base := time.Now()
	o, binding := readCgroupFiles(cgroupSelfFixture, cgroupFixtureOpen(cgroupFixture(), nil, nil), func() time.Time { return base })
	var sampler CgroupSampler
	if sampler.observe(o, binding).HasWindow {
		t.Fatal("first capture has delta")
	}
	next := o
	next.SampledAt = base.Add(10 * time.Second)
	for _, field := range cgroupCounterFields(&next.Counters) {
		field.Value += 2
	}
	got := sampler.observe(next, binding)
	if !got.HasWindow || got.Window.Seconds != 10 {
		t.Fatalf("window = %+v", got)
	}
	for _, field := range cgroupCounterFields(&got.Window.Counters) {
		if !field.Known || field.Value != 2 {
			t.Fatalf("delta = %+v", got.Window.Counters)
		}
	}
	for _, mode := range []string{"inode", "mount inode", "path", "unknown", "decrease", "zero interval", "gap"} {
		t.Run(mode, func(t *testing.T) {
			var s CgroupSampler
			s.observe(o, binding)
			current, b := next, binding
			switch mode {
			case "inode":
				b.inode++
			case "mount inode":
				b.mountInode++
			case "path":
				b.location.membership = "/other"
			case "unknown":
				current.HasBinding = false
			case "decrease":
				current.Counters.CPUUsage.Value = 0
			case "zero interval":
				current.SampledAt = base
			case "gap":
				current.SampledAt = base.Add(30*time.Second + time.Nanosecond)
			}
			if v := s.observe(current, b); v.HasWindow {
				t.Fatalf("bridged %s", mode)
			}
			if mode == "unknown" && (s.previous != (CgroupObservation{}) || s.binding != (cgroupBinding{})) {
				t.Fatal("unknown did not reset baseline")
			}
		})
	}
	var s CgroupSampler
	missing := o
	missing.Counters.OOM = CgroupValue{}
	s.observe(missing, binding)
	got = s.observe(next, binding)
	if !got.HasWindow || got.Window.Counters.OOM.Known || !got.Window.Counters.CPUUsage.Known {
		t.Fatalf("missing scope fabricated or contaminated delta: %+v", got.Window)
	}
	zero := next
	zero.SampledAt = next.SampledAt.Add(10 * time.Second)
	got = s.observe(zero, binding)
	if !got.HasWindow || !got.Window.Counters.OOM.Known || got.Window.Counters.OOM.Value != 0 {
		t.Fatalf("known zero lost: %+v", got.Window)
	}
}

func TestCgroupParserBoundaries(t *testing.T) {
	for _, text := range []string{"", "-1", "+1", "1 2", "18446744073709551616"} {
		if got := parseCgroupValue([]byte(text)); got != (CgroupValue{}) {
			t.Fatalf("unknown scalar %q = %+v", text, got)
		}
	}
	if got := parseCgroupValue([]byte("18446744073709551615\n")); !got.Known || got.Value != math.MaxUint64 {
		t.Fatalf("MaxUint64 = %+v", got)
	}
	if got := parseCgroupLimit([]byte("0\n")); got != (CgroupLimit{State: CgroupLimitFinite}) {
		t.Fatalf("zero limit = %+v", got)
	}
	for _, text := range []string{"max", "1 0", "0 1", "max 0", "max -1", "1 2 3"} {
		if got := parseCgroupQuota([]byte(text)); got != (CgroupCPUQuota{}) {
			t.Fatalf("invalid quota %q = %+v", text, got)
		}
	}
	for _, text := range []string{"usage_usec 1\nuser_usec 0\nsystem_usec 0\nusage_usec 2\n", "usage_usec 1\nuser_usec 0\n", "usage_usec 1\nuser_usec 0\nsystem_usec 0\nnr_periods x\n"} {
		var counters CgroupCounters
		parseCgroupCPU([]byte(text), &counters)
		if counters != (CgroupCounters{}) {
			t.Fatalf("invalid CPU accepted: %+v", counters)
		}
	}
	for _, text := range []string{strings.Replace(cgroupTestPSI, "1.25", "NaN", 1), strings.Replace(cgroupTestPSI, "1.25", "101", 1), strings.Replace(cgroupTestPSI, "avg60", "avg10", 1), cgroupTestPSI + cgroupTestPSI, strings.TrimSuffix(cgroupTestPSI, "\n")} {
		if got := parseCgroupPressure([]byte(text)); !reflect.DeepEqual(got, CgroupPressure{}) {
			t.Fatalf("invalid PSI = %+v", got)
		}
	}
	want := parseCgroupPressure([]byte(cgroupTestPSI))
	if got := parseCgroupPressure([]byte(strings.Replace(cgroupTestPSI, "some ", "some future=9 ", 1))); got != want {
		t.Fatalf("unknown PSI key changed selected values: %+v", got)
	}
	files := cgroupFixture()
	files["memory.current"] = strings.Repeat("1", cgroupFileLimit+1)
	got, _ := readCgroupFiles(cgroupSelfFixture, cgroupFixtureOpen(files, nil, nil), time.Now)
	if !got.HasBinding || got.MemoryCurrent.Known || !got.SwapCurrent.Known || !got.Counters.CPUUsage.Known {
		t.Fatalf("oversized scope was accepted or contaminated other scopes: %+v", got)
	}
}

func BenchmarkCgroupDisabled(b *testing.B) {
	var sampler *CgroupSampler
	b.ReportAllocs()
	for b.Loop() {
		sampler.Sample()
	}
}
