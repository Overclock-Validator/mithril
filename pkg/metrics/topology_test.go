package metrics

import (
	"errors"
	"math"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func topologyFixedNow() time.Time { return time.Unix(1_700_000_000, 123) }

func topologyFixture() map[string]string {
	f := map[string]string{
		topologyCPUPath + "possible":       "0,3,8191\n",
		topologyCPUPath + "present":        "0,8191\n",
		topologyCPUPath + "online":         "8191\n",
		topologyNodePath + "possible":      "0,7\n",
		topologyNodePath + "online":        "0,7\n",
		topologyNodePath + "node0/cpulist": "0,3\n",
		topologyNodePath + "node7/cpulist": "8191\n",
		topologyNodePath + "node0/meminfo": "Node 0 MemTotal: 100 kB\nNode 0 MemFree: 40 kB\nNode 0 Shmem: 0 kB\n",
		topologyNodePath + "node7/meminfo": "Node 7 MemTotal: 0 kB\nNode 7 MemFree: 0 kB\n",
	}
	for _, id := range []int{0, 3, 8191} {
		base := topologyCPUPath + "cpu" + strconv.Itoa(id) + "/topology/"
		f[base+"physical_package_id"], f[base+"die_id"], f[base+"core_id"] = "0\n", "1\n", "2\n"
		f[base+"core_cpus_list"] = "0,3,8191\n"
	}
	return f
}

func topologyFixtureRead(f map[string]string) func(string, int, bool) ([]byte, error) {
	return func(path string, limit int, first bool) ([]byte, error) {
		if first {
			panic("topology requested an incomplete first-line read")
		}
		value, ok := f[path]
		if !ok {
			return nil, os.ErrNotExist
		}
		return []byte(value), nil
	}
}

func TestTopologyDisabledAndGeneration(t *testing.T) {
	if NewTopologySampler(false) != nil {
		t.Fatal("disabled sampler allocated")
	}
	calls := 0
	read := func() TopologyObservation {
		calls++
		return TopologyObservation{CPUState: TopologyUnsupported, NUMAState: TopologyUnsupported}
	}
	var disabled *TopologySampler
	if o, ok := disabled.sample(read); ok || !reflect.DeepEqual(o, TopologyObservation{}) || calls != 0 {
		t.Fatal("disabled sampler performed work")
	}
	if allocs := testing.AllocsPerRun(100, func() { disabled.Sample() }); allocs != 0 {
		t.Fatalf("disabled allocations = %g", allocs)
	}
	s := NewTopologySampler(true)
	if calls != 0 {
		t.Fatal("constructor captured")
	}
	for want := uint64(1); want <= 2; want++ {
		o, ok := s.sample(read)
		if !ok || o.Generation != want {
			t.Fatal("attempt generation differs")
		}
	}
	s.generation = math.MaxUint64 - 2
	last, _ := s.sample(read)
	before := calls
	for range 2 {
		o, ok := s.sample(read)
		if !ok || o.Generation != last.Generation || o.CPUState != TopologyLimitExceeded || !o.StartedAt.IsZero() || calls != before {
			t.Fatal("exhausted sampler renewed or wrapped")
		}
	}
	if runtime.GOOS != "linux" {
		o, ok := NewTopologySampler(true).Sample()
		if !ok || o.Generation != 1 || o.CPUState != TopologyUnsupported || o.NUMAState != TopologyUnsupported || o.StartedAt.IsZero() || o.SampledAt.Before(o.StartedAt) || o.Reads != 0 || o.Bytes != 0 {
			t.Fatal("unsupported platform fabricated data or omitted dated attempt")
		}
	}
}

func TestTopologyBoundedSources(t *testing.T) {
	f := topologyFixture()
	at := time.Now()
	var paths []string
	var bytes uint64
	read := topologyFixtureRead(f)
	o := readTopologyFiles(func(path string, limit int, first bool) ([]byte, error) {
		paths = append(paths, path)
		data, err := read(path, limit, first)
		bytes += uint64(len(data))
		return data, err
	}, func() time.Time { return at })
	if o.CPUState != TopologyAvailable || o.NUMAState != TopologyAvailable || len(o.CPUs) != 3 || len(o.Nodes) != 2 || len(paths) != 26 || o.Reads != 26 || o.Bytes != bytes {
		t.Fatalf("bounded complete fixture differs: %+v", o)
	}
	if o.StartedAt != at || o.SampledAt != at || o.StartedAt == o.StartedAt.Round(0) {
		t.Fatal("original capture clock changed")
	}
	for i, id := range []uint16{0, 3, 8191} {
		cpu := o.CPUs[i]
		if cpu.ID != id || cpu.Present != (id != 3) || cpu.Online != (id == 8191) || cpu.Siblings[0] != 7 || cpu.SiblingsState != TopologyAvailable || !cpu.HasNUMANode {
			t.Fatal("sparse dictionary or independent CPU bits differ")
		}
		if cpu.NUMANode != 0 && id != 8191 || id == 8191 && cpu.NUMANode != 7 {
			t.Fatal("CPU node association differs")
		}
	}
	if o.Nodes[0].Memory.TotalBytes != 102400 || o.Nodes[0].Memory.FreeBytes != 40960 || !o.Nodes[0].Memory.HasShmem || o.Nodes[0].Memory.ShmemBytes != 0 || o.Nodes[1].Memory.HasShmem || o.Nodes[1].Memory.TotalBytes != 0 {
		t.Fatal("NUMA byte/zero/unknown distinction differs")
	}
	want := readTopologyFiles(topologyFixtureRead(f), func() time.Time { return at })
	o.CPUs[0] = TopologyCPU{}
	o.Nodes[0] = TopologyNode{}
	delete(f, topologyCPUPath+"possible")
	failed := readTopologyFiles(topologyFixtureRead(f), func() time.Time { return at.Add(time.Second) })
	if len(failed.CPUs) != 0 || failed.CPUState != TopologyUnsupported || want.CPUs[0].ID != 0 || want.CPUs[0].Siblings[0] != 7 || want.SampledAt != at {
		t.Fatal("failure retained/renewed old CPU data or caller mutation escaped")
	}
	if failed.Nodes[0].Memory.TotalBytes != 102400 || failed.Nodes[0].CPUsState == TopologyAvailable {
		t.Fatal("CPU failure erased independent memory or left a dictionary-free mask")
	}
}

func TestTopologyOptionalFieldsAndScopeFailures(t *testing.T) {
	base := topologyCPUPath + "cpu8191/topology/"
	for _, tc := range []struct {
		name      string
		change    func(map[string]string)
		denied    string
		cpu, numa TopologyState
		check     func(*testing.T, TopologyObservation)
	}{
		{"negative", func(f map[string]string) { f[base+"die_id"] = "-1\n" }, "", TopologyPartial, TopologyAvailable, func(t *testing.T, o TopologyObservation) {
			if o.CPUs[2].Die.State != TopologyUnsupported || !o.CPUs[2].Online {
				t.Fatal("negative became known zero or erased online")
			}
		}},
		{"missing", func(f map[string]string) { delete(f, base+"die_id") }, "", TopologyPartial, TopologyAvailable, nil},
		{"signed_overflow", func(f map[string]string) { f[base+"core_id"] = "2147483648\n" }, "", TopologyPartial, TopologyAvailable, nil},
		{"legacy", func(f map[string]string) {
			delete(f, base+"core_cpus_list")
			f[base+"thread_siblings_list"] = "0,8191\n"
		}, "", TopologyAvailable, TopologyAvailable, func(t *testing.T, o TopologyObservation) {
			if o.CPUs[2].Siblings[0] != 5 {
				t.Fatal("legacy siblings differ")
			}
		}},
		{"denied_no_fallback", func(f map[string]string) { f[base+"thread_siblings_list"] = "8191\n" }, base + "core_cpus_list", TopologyPartial, TopologyAvailable, func(t *testing.T, o TopologyObservation) {
			if o.CPUs[2].SiblingsState != TopologyDenied || !o.CPUs[2].Online {
				t.Fatal("denied list fabricated siblings")
			}
		}},
		{"malformed_no_fallback", func(f map[string]string) {
			f[base+"core_cpus_list"] = "0-\n"
			f[base+"thread_siblings_list"] = "8191\n"
		}, "", TopologyPartial, TopologyAvailable, nil},
		{"foreign_sibling", func(f map[string]string) { f[base+"core_cpus_list"] = "0,4,8191\n" }, "", TopologyReadFailed, TopologyPartial, func(t *testing.T, o TopologyObservation) {
			if len(o.CPUs) != 0 || o.Nodes[0].Memory.TotalBytes != 102400 {
				t.Fatal("foreign CPU bit dropped or independent memory erased")
			}
		}},
		{"foreign_node", func(f map[string]string) { f[topologyNodePath+"node7/cpulist"] = "4\n" }, "", TopologyAvailable, TopologyReadFailed, nil},
		{"no_numa", func(f map[string]string) { delete(f, topologyNodePath+"possible") }, "", TopologyAvailable, TopologyUnsupported, func(t *testing.T, o TopologyObservation) {
			if len(o.Nodes) != 0 || o.CPUs[0].HasNUMANode {
				t.Fatal("missing NUMA fabricated a node")
			}
		}},
		{"missing_node_list", func(f map[string]string) { delete(f, topologyNodePath+"node7/cpulist") }, "", TopologyAvailable, TopologyPartial, func(t *testing.T, o TopologyObservation) {
			for _, c := range o.CPUs {
				if c.HasNUMANode {
					t.Fatal("missing list cannot prove uniqueness")
				}
			}
		}},
		{"multiple_nodes", func(f map[string]string) { f[topologyNodePath+"node7/cpulist"] = "0,8191\n" }, "", TopologyAvailable, TopologyAvailable, func(t *testing.T, o TopologyObservation) {
			if o.CPUs[0].HasNUMANode || !o.CPUs[1].HasNUMANode {
				t.Fatal("duplicate membership was guessed or unique membership lost")
			}
		}},
		{"offline_node", func(f map[string]string) { f[topologyNodePath+"online"] = "0\n" }, "", TopologyAvailable, TopologyAvailable, func(t *testing.T, o TopologyObservation) {
			if o.Nodes[1].Online || o.Nodes[1].MemoryState != "" || o.CPUs[2].HasNUMANode {
				t.Fatal("offline node was sampled or assigned")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := topologyFixture()
			tc.change(f)
			read := topologyFixtureRead(f)
			fallback := false
			o := readTopologyFiles(func(path string, limit int, first bool) ([]byte, error) {
				if path == base+"thread_siblings_list" {
					fallback = true
				}
				if path == tc.denied {
					return nil, os.ErrPermission
				}
				if tc.name == "offline_node" && strings.HasPrefix(path, topologyNodePath+"node7/") {
					t.Fatal("read offline node")
				}
				return read(path, limit, first)
			}, topologyFixedNow)
			if o.CPUState != tc.cpu || o.NUMAState != tc.numa {
				t.Fatalf("states = %s/%s, want %s/%s", o.CPUState, o.NUMAState, tc.cpu, tc.numa)
			}
			if fallback != (tc.name == "legacy") {
				t.Fatal("legacy fallback used without missing preferred file")
			}
			if tc.check != nil {
				tc.check(t, o)
			}
		})
	}
}

func TestTopologyHotplugAndMalformedSets(t *testing.T) {
	for _, tc := range []struct {
		path, after string
		cpu, numa   TopologyState
	}{
		{topologyCPUPath + "possible", "0,3,5,8191\n", TopologyBindingChanged, TopologyPartial},
		{topologyCPUPath + "present", "0,3,8191\n", TopologyBindingChanged, TopologyPartial},
		{topologyCPUPath + "online", "0,8191\n", TopologyBindingChanged, TopologyPartial},
		{topologyNodePath + "possible", "0,1,7\n", TopologyAvailable, TopologyBindingChanged},
		{topologyNodePath + "online", "0\n", TopologyAvailable, TopologyBindingChanged},
	} {
		t.Run(strings.TrimPrefix(tc.path, "/sys/devices/system/"), func(t *testing.T) {
			f := topologyFixture()
			read := topologyFixtureRead(f)
			seen := 0
			o := readTopologyFiles(func(path string, limit int, first bool) ([]byte, error) {
				if path == tc.path {
					seen++
					if seen == 2 {
						return []byte(tc.after), nil
					}
				}
				return read(path, limit, first)
			}, topologyFixedNow)
			if seen != 2 || o.CPUState != tc.cpu || o.NUMAState != tc.numa {
				t.Fatal("hotplug did not invalidate the affected family")
			}
			for _, cpu := range o.CPUs {
				if cpu.HasNUMANode {
					t.Fatal("changed NUMA binding retained assignment")
				}
			}
		})
	}
	for _, tc := range []struct {
		path, value string
		state       TopologyState
	}{
		{topologyCPUPath + "possible", "0-256\n", TopologyLimitExceeded},
		{topologyCPUPath + "possible", "0,8192\n", TopologyLimitExceeded},
		{topologyCPUPath + "present", "0,4\n", TopologyReadFailed},
		{topologyCPUPath + "online", "3\n", TopologyReadFailed},
		{topologyNodePath + "possible", "0-64\n", TopologyLimitExceeded},
		{topologyNodePath + "online", "8\n", TopologyReadFailed},
	} {
		f := topologyFixture()
		f[tc.path] = tc.value
		o := readTopologyFiles(topologyFixtureRead(f), topologyFixedNow)
		state := o.CPUState
		if strings.HasPrefix(tc.path, topologyNodePath) {
			state = o.NUMAState
		}
		if state != tc.state {
			t.Fatalf("%s invalid set gave %s", tc.path, state)
		}
	}
}

func TestTopologyListAndMemoryBoundaries(t *testing.T) {
	for _, text := range []string{"0", "0\n\n", "-1\n", "0,0\n", "3,2\n", "3-2\n", "0-\n", "0,\n", "0 1\n", "../1\n", "0-18446744073709551615\n", "99999999999999999999999999\n"} {
		if _, err := parseTopologyList([]byte(text), topologyCPULimit); err == nil {
			t.Fatalf("invalid list accepted: %q", text)
		}
	}
	for _, tc := range []struct {
		text           string
		maximum, count int
	}{{"\n", 64, 0}, {"0-255\n", 256, 256}, {"0-62,8191\n", 64, 64}, {"8191\n", 256, 1}} {
		ids, err := parseTopologyList([]byte(tc.text), tc.maximum)
		if err != nil || len(ids) != tc.count {
			t.Fatal("bounded valid list refused")
		}
	}
	maximumKiB := strconv.FormatUint(math.MaxUint64/1024, 10)
	m, err := parseTopologyMemory([]byte("Node 7 MemTotal: "+maximumKiB+" kB\nNode 7 MemFree: 0 kB\nNode 7 Shmem: 0 kB\n"), 7)
	if err != nil || m.TotalBytes != math.MaxUint64/1024*1024 || !m.HasShmem {
		t.Fatal("maximum KiB conversion differs")
	}
	for _, text := range []string{
		"Node 7 MemTotal: 1 kB\n", "Node 7 MemTotal: 1 kB\nNode 7 MemFree: 2 kB\n",
		"Node 7 MemTotal: 18446744073709551615 kB\nNode 7 MemFree: 0 kB\n",
		"Node 0 MemTotal: 1 kB\nNode 7 MemFree: 0 kB\n",
		"Node 7 MemTotal: 1 kB\nNode 7 MemFree: 0 kB",
		"Node 7 MemTotal: 1 kB\nNode 7 MemFree: 0 B\n",
		"Node 7 MemTotal: 1 kB\nNode 7 MemFree: 0 kB\nNode 7 Shmem: 0 kB\nNode 7 Shmem: 0 kB\n",
		strings.Repeat("Node 7 Other: 0\n", 257),
	} {
		if _, err := parseTopologyMemory([]byte(text), 7); err == nil {
			t.Fatal("invalid node memory accepted")
		}
	}
}

func TestTopologyCaptureBudgetsAndReaderAmbiguity(t *testing.T) {
	f := topologyFixture()
	at := time.Now()
	calls := 0
	read := topologyFixtureRead(f)
	o := readTopologyFiles(func(path string, limit int, first bool) ([]byte, error) {
		calls++
		data, err := read(path, limit, first)
		at = at.Add(topologyCaptureLimit + time.Nanosecond)
		return data, err
	}, func() time.Time { return at })
	if calls != 1 || o.Reads != 1 || o.CPUState != TopologyCaptureExpired || o.NUMAState != TopologyCaptureExpired || len(o.CPUs)+len(o.Nodes) != 0 {
		t.Fatal("expired capture continued reading or retained values")
	}
	for _, invalid := range []time.Time{{}, time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)} {
		calls = 0
		o = readTopologyFiles(func(path string, limit int, first bool) ([]byte, error) { calls++; return read(path, limit, first) }, func() time.Time { return invalid })
		if calls != 0 || o.CPUState != TopologyCaptureExpired {
			t.Fatal("invalid initial clock permitted a read")
		}
	}
	at = time.Now()
	calls = 0
	o = readTopologyFiles(func(path string, limit int, first bool) ([]byte, error) {
		calls++
		data, err := read(path, limit, first)
		if calls == 1 {
			at = at.Add(topologyCaptureLimit)
		}
		return data, err
	}, func() time.Time { return at })
	if calls != 26 || o.CPUState != TopologyAvailable {
		t.Fatal("exact cooperative deadline boundary refused")
	}
	for _, movement := range []time.Duration{-time.Nanosecond, topologyCaptureLimit + time.Nanosecond} {
		at = time.Now()
		calls = 0
		o = readTopologyFiles(func(path string, limit int, first bool) ([]byte, error) {
			calls++
			data, err := read(path, limit, first)
			at = at.Add(movement)
			return data, err
		}, func() time.Time { return at })
		if calls != 1 || o.CPUState != TopologyCaptureExpired {
			t.Fatal("clock reversal/budget overrun accepted")
		}
	}
	at = time.Now()
	calls = 0
	c := topologyCapture{start: at, last: at, now: func() time.Time { return at }, read: func(_ string, limit int, _ bool) ([]byte, error) { calls++; return make([]byte, limit), nil }, reads: topologyOpenLimit - 1}
	if _, err := c.file(topologyCPUPath+"possible", 4096); err != nil {
		t.Fatal(err)
	}
	if _, err := c.file(topologyCPUPath+"possible", 4096); !errors.Is(err, errTopologyLimit) || calls != 1 {
		t.Fatal("open budget exceeded")
	}
	c.reads, c.bytes, c.failure, calls = 0, topologyByteLimit-4097, "", 0
	if _, err := c.file(topologyCPUPath+"possible", 4096); err != nil {
		t.Fatal(err)
	}
	if _, err := c.file(topologyCPUPath+"possible", 4096); !errors.Is(err, errTopologyLimit) || calls != 1 {
		t.Fatal("aggregate byte budget exceeded")
	}
	for _, tc := range []struct {
		data  []byte
		err   error
		state TopologyState
	}{
		{nil, errHostRecord, TopologyReadFailed}, // Existing helper cannot distinguish oversized input from read failure.
		{make([]byte, 4097), nil, TopologyLimitExceeded},
		{nil, os.ErrPermission, TopologyDenied},
		{nil, os.ErrNotExist, TopologyUnsupported},
	} {
		o = readTopologyFiles(func(path string, limit int, first bool) ([]byte, error) {
			if path == topologyCPUPath+"possible" {
				return tc.data, tc.err
			}
			return read(path, limit, first)
		}, topologyFixedNow)
		if o.CPUState != tc.state || len(o.CPUs) != 0 {
			t.Fatal("bounded reader failure mislabeled or exposed a prefix")
		}
	}
}

func TestTopologyMaximumDictionaries(t *testing.T) {
	f := map[string]string{topologyCPUPath + "possible": "0-254,8191\n", topologyCPUPath + "present": "0-254,8191\n", topologyCPUPath + "online": "0-254,8191\n", topologyNodePath + "possible": "0-62,8191\n", topologyNodePath + "online": "0-62,8191\n"}
	for i := range 256 {
		id := i
		if i == 255 {
			id = 8191
		}
		base := topologyCPUPath + "cpu" + strconv.Itoa(id) + "/topology/"
		f[base+"physical_package_id"], f[base+"die_id"], f[base+"core_id"], f[base+"core_cpus_list"] = "2147483647\n", "0\n", "0\n", "0-254,8191\n"
	}
	for i := range 64 {
		id := i
		if i == 63 {
			id = 8191
		}
		text := strconv.Itoa(id)
		f[topologyNodePath+"node"+text+"/cpulist"] = "0-254,8191\n"
		f[topologyNodePath+"node"+text+"/meminfo"] = "Node " + text + " MemTotal: 1 kB\nNode " + text + " MemFree: 0 kB\n"
	}
	o := readTopologyFiles(topologyFixtureRead(f), topologyFixedNow)
	if o.CPUState != TopologyAvailable || o.NUMAState != TopologyAvailable || len(o.CPUs) != 256 || len(o.Nodes) != 64 || o.Reads != 1162 {
		t.Fatalf("maximum fixture coverage = %s/%s, %d/%d rows, %d reads", o.CPUState, o.NUMAState, len(o.CPUs), len(o.Nodes), o.Reads)
	}
	for _, cpu := range o.CPUs {
		for _, bits := range cpu.Siblings {
			if bits != 255 {
				t.Fatal("maximum sibling dictionary lost bits")
			}
		}
		if cpu.HasNUMANode {
			t.Fatal("ambiguous node membership was fabricated")
		}
	}
}
