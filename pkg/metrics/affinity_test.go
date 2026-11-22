package metrics

import (
	"encoding/json"
	"math"
	"os"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

type affinityFixture struct {
	topology                               TopologyObservation
	at                                     time.Time
	self                                   string
	before, after                          []affinityThread
	status                                 map[uint32]string
	readErrors                             map[uint32]error
	selfError, openError                   error
	selfReads, opens, lists, reads, closes int
}

func newAffinityFixture() *affinityFixture {
	at := time.Now()
	threads := []affinityThread{{id: 11, device: 1, inode: 101}, {id: 22, device: 1, inode: 102}, {id: 33, device: 1, inode: 103}}
	return &affinityFixture{
		topology: TopologyObservation{Generation: 7, StartedAt: at, SampledAt: at.Add(time.Millisecond), CPUState: TopologyAvailable, NUMAState: TopologyAvailable,
			CPUs: []TopologyCPU{{ID: 0}, {ID: 2}, {ID: 8191}}, Nodes: []TopologyNode{{ID: 0}, {ID: 7}}, Reads: 26, Bytes: 100},
		at: at.Add(2 * time.Millisecond), self: "Name:\tDO_NOT_RETAIN\nThreads:\t3\n",
		before: threads, after: slices.Clone(threads),
		status:     map[uint32]string{11: "Name:\tPRIVATE_THREAD\nCpus_allowed_list:\t0,8191\nMems_allowed_list:\t0,7\n", 22: "Cpus_allowed_list:\t0,8191\nMems_allowed_list:\t0,7\n", 33: "Cpus_allowed_list:\t2,8191\nMems_allowed_list:\t7\n"},
		readErrors: make(map[uint32]error),
	}
}

func (f *affinityFixture) readSelf(path string, limit int, first bool) ([]byte, error) {
	if path != "/proc/self/status" || limit != affinityStatusLimit || first {
		panic("unexpected process status input")
	}
	f.selfReads++
	if f.selfError != nil {
		return nil, f.selfError
	}
	return []byte(f.self), nil
}

func (f *affinityFixture) open(c *topologyCapture) (affinityTaskAccess, error) {
	if err := affinityOpen(c); err != nil {
		return affinityTaskAccess{}, err
	}
	f.opens++
	if f.openError != nil {
		return affinityTaskAccess{}, f.openError
	}
	return affinityTaskAccess{
		list: func() ([]affinityThread, error) {
			if err := affinityRoom(c, (affinityThreadLimit+1)*256); err != nil {
				return nil, err
			}
			f.lists++
			rows := f.before
			if f.lists%2 == 0 {
				rows = f.after
			}
			for _, row := range rows {
				c.bytes += uint64(len(strconv.FormatUint(uint64(row.id), 10)) + 1)
			}
			if !affinityCheck(c) {
				return nil, c.failureError()
			}
			return slices.Clone(rows), nil
		},
		read: func(thread affinityThread) ([]byte, error) {
			if err := affinityRoom(c, affinityStatusLimit+1); err != nil {
				return nil, err
			}
			if err := affinityOpen(c); err != nil {
				return nil, err
			}
			f.reads++
			if err := f.readErrors[thread.id]; err != nil {
				return nil, err
			}
			if err := affinityOpen(c); err != nil {
				return nil, err
			}
			data := []byte(f.status[thread.id])
			c.bytes += uint64(len(data))
			if !affinityCheck(c) {
				return nil, c.failureError()
			}
			return data, nil
		},
		close: func() { f.closes++ },
	}, nil
}

func (f *affinityFixture) capture() ThreadAffinityObservation {
	return readAffinityFiles(f.topology, f.readSelf, f.open, func() time.Time { return f.at })
}

func TestThreadAffinityCoverage(t *testing.T) {
	f := newAffinityFixture()
	o := f.capture()
	if o.State != TopologyAvailable || !o.Complete || !o.MembershipStable || o.ReportedThreads != 3 || o.ListedThreads != 3 || o.ObservedThreads != 3 || len(o.Groups) != 2 || f.selfReads != 1 || f.opens != 1 || f.lists != 2 || f.reads != 3 || f.closes != 1 {
		t.Fatalf("complete sampled coverage differs: %+v", o)
	}
	if !o.HasCPUIntersection || o.CPUIntersection[0] != 4 || !o.HasNodeIntersection || o.NodeIntersection[0] != 2 || o.Groups[0].CPUs[0] != 5 || o.Groups[0].Threads != 2 || o.Groups[1].Threads != 1 {
		t.Fatal("mask groups/intersections differ")
	}
	if o.CumulativeReads != f.topology.Reads+8 || o.CumulativeBytes <= f.topology.Bytes || o.StartedAt != f.at || o.SampledAt != f.at || o.StartedAt == o.StartedAt.Round(0) || o.TopologyGeneration != 7 {
		t.Fatal("capture clocks/budget/dictionary generation changed")
	}
	body, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "DO_NOT_RETAIN") || strings.Contains(string(body), "PRIVATE_THREAD") {
		t.Fatal("raw thread status leaked")
	}
	var check func(reflect.Type)
	check = func(typ reflect.Type) {
		if typ == reflect.TypeOf(time.Time{}) {
			return
		}
		if typ == reflect.TypeOf(affinityThread{}) {
			t.Fatal("private identity retained")
		}
		switch typ.Kind() {
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				field := typ.Field(i)
				if field.Name == "TID" || field.Name == "PID" || field.Name == "Name" || field.Name == "Raw" {
					t.Fatal("private thread field exported")
				}
				check(field.Type)
			}
		case reflect.Array, reflect.Slice:
			check(typ.Elem())
		case reflect.String:
			if typ != reflect.TypeOf(TopologyState("")) {
				t.Fatal("unbounded text retained")
			}
		case reflect.Bool, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8:
		default:
			t.Fatalf("unexpected returned type %s", typ)
		}
	}
	check(reflect.TypeOf(o))
	o.CPUDictionary[0], o.NodeDictionary[0], o.Groups[0].CPUs[0] = 5, 5, 0
	fresh := f.capture()
	if fresh.CPUDictionary[0] != 0 || fresh.NodeDictionary[0] != 0 || fresh.Groups[0].CPUs[0] != 5 {
		t.Fatal("returned containers alias source/caller state")
	}
	f.topology.CPUs[0].ID = 9
	if fresh.CPUDictionary[0] != 0 {
		t.Fatal("dictionary retains mutable topology rows")
	}
}

func TestThreadAffinityMembershipFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*affinityFixture)
		state  TopologyState
		stable bool
	}{
		{"new", func(f *affinityFixture) { f.after = append(f.after, affinityThread{id: 44, device: 1, inode: 104}) }, TopologyBindingChanged, false},
		{"gone", func(f *affinityFixture) { f.after = f.after[:2] }, TopologyBindingChanged, false},
		{"reused", func(f *affinityFixture) { f.after[1].inode++ }, TopologyBindingChanged, false},
		{"duplicate", func(f *affinityFixture) { f.before[1] = f.before[0] }, TopologyReadFailed, false},
		{"unsorted", func(f *affinityFixture) { f.before[0], f.before[1] = f.before[1], f.before[0] }, TopologyReadFailed, false},
		{"read_denied", func(f *affinityFixture) { f.readErrors[22] = os.ErrPermission }, TopologyPartial, true},
		{"read_replaced", func(f *affinityFixture) { f.readErrors[22] = errAffinityChanged }, TopologyPartial, true},
		{"count_mismatch", func(f *affinityFixture) { f.self = "Threads: 4\n" }, TopologyPartial, true},
		{"self_unavailable", func(f *affinityFixture) { f.selfError = os.ErrPermission }, TopologyPartial, true},
		{"open_denied", func(f *affinityFixture) { f.openError = os.ErrPermission }, TopologyDenied, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAffinityFixture()
			tc.mutate(f)
			o := f.capture()
			if o.State != tc.state || o.MembershipStable != tc.stable || o.Complete || o.HasCPUIntersection || o.HasNodeIntersection {
				t.Fatalf("incomplete membership became complete: %+v", o)
			}
			if f.opens == 1 && f.openError == nil && f.closes != 1 {
				t.Fatal("task anchor not closed")
			}
			if tc.stable {
				var total uint32
				for _, group := range o.Groups {
					total += group.Threads
				}
				if total != 3 {
					t.Fatal("failed thread silently omitted from groups")
				}
			} else if len(o.Groups) != 0 {
				t.Fatal("unstable membership retained grouped current data")
			}
		})
	}
}

func TestThreadAffinityIndependentMaskKnowledge(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutate    func(*affinityFixture)
		cpu, node bool
	}{
		{"foreign_cpu", func(f *affinityFixture) { f.status[11] = "Cpus_allowed_list: 1\nMems_allowed_list: 0,7\n" }, false, true},
		{"foreign_node", func(f *affinityFixture) { f.status[11] = "Cpus_allowed_list: 0,8191\nMems_allowed_list: 9\n" }, true, false},
		{"missing_cpu", func(f *affinityFixture) { f.status[11] = "Mems_allowed_list: 0,7\n" }, false, true},
		{"cpu_dictionary", func(f *affinityFixture) { f.topology.CPUState = TopologyReadFailed; f.topology.CPUs = nil }, false, true},
		{"node_dictionary", func(f *affinityFixture) { f.topology.Nodes = nil }, true, false},
		{"invalid_dictionary", func(f *affinityFixture) { f.topology.CPUs[1].ID = 0 }, false, true},
		{"truncated_status", func(f *affinityFixture) { f.status[11] = "Cpus_allowed_list: 0,8191\nMems_allowed_list: 0,7" }, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAffinityFixture()
			tc.mutate(f)
			o := f.capture()
			if o.State != TopologyPartial || o.Complete || o.HasCPUIntersection != tc.cpu || o.HasNodeIntersection != tc.node {
				t.Fatal("independent mask knownness differs")
			}
			if !tc.cpu && o.CPUIntersection != ([32]byte{}) || !tc.node && o.NodeIntersection != ([8]byte{}) {
				t.Fatal("unknown intersection retained partial bits")
			}
		})
	}
	f := newAffinityFixture()
	f.topology.Nodes = []TopologyNode{}
	for id := range f.status {
		f.status[id] = "Cpus_allowed_list: \nMems_allowed_list: \n"
	}
	o := f.capture()
	if !o.Complete || !o.HasNodeDictionary || !o.HasCPUIntersection || !o.HasNodeIntersection || o.CPUIntersection != ([32]byte{}) || o.NodeIntersection != ([8]byte{}) {
		t.Fatal("known empty masks conflated with unavailable dictionary")
	}
}

func TestThreadAffinityParserBounds(t *testing.T) {
	for _, name := range []string{"", "0", "01", "-1", "+1", "1/../2", "../1", "1\x00", "2147483648", "12345678901"} {
		if _, err := affinityTID(name); err == nil {
			t.Fatalf("invalid thread component accepted: %q", name)
		}
	}
	for _, name := range []string{"1", "2147483647"} {
		if _, err := affinityTID(name); err != nil {
			t.Fatal("valid bounded TID refused")
		}
	}
	for _, text := range []string{"Threads: 0\n", "Threads: 1", "Threads: 1\nThreads: 1\n", "Name: x\n", "Threads: 4294967296\n", strings.Repeat("Name: x\n", 257), strings.Repeat("x", affinityStatusLimit+1)} {
		if _, err := parseAffinityThreads([]byte(text)); err == nil {
			t.Fatal("invalid process status accepted")
		}
	}
	for _, text := range []string{"Cpus_allowed_list: 0\nCpus_allowed_list: 0\n", "Cpus_allowed_list: 0", "bad line\n", strings.Repeat("Name: x\n", 257), strings.Repeat("x", affinityStatusLimit+1)} {
		if _, err := parseAffinityStatus([]byte(text), []uint16{0}, []uint16{0}, true, true); err == nil {
			t.Fatal("invalid thread status framing accepted")
		}
	}
	for _, text := range []string{"Cpus_allowed_list: 0-8191\nMems_allowed_list: 0\n", "Cpus_allowed_list: 0,0\nMems_allowed_list: 0\n", "Cpus_allowed_list: 8192\nMems_allowed_list: 0\n"} {
		group, err := parseAffinityStatus([]byte(text), []uint16{0}, []uint16{0}, true, true)
		if err != nil || group.CPUState == TopologyAvailable || group.CPUs != ([32]byte{}) || group.NodeState != TopologyAvailable {
			t.Fatal("invalid CPU list lost dimension separation")
		}
	}
}

func TestThreadAffinitySharedBudgetAndClocks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*affinityFixture)
		state  TopologyState
	}{
		{"wall_only_start", func(f *affinityFixture) { f.topology.StartedAt = f.topology.StartedAt.Round(0) }, TopologyCaptureExpired},
		{"wall_only_end", func(f *affinityFixture) { f.topology.SampledAt = f.topology.SampledAt.Round(0) }, TopologyCaptureExpired},
		{"reversed", func(f *affinityFixture) { f.topology.SampledAt = f.topology.StartedAt.Add(-time.Nanosecond) }, TopologyCaptureExpired},
		{"expired_source", func(f *affinityFixture) { f.topology.CPUState = TopologyCaptureExpired }, TopologyCaptureExpired},
		{"expired_now", func(f *affinityFixture) { f.at = f.topology.StartedAt.Add(topologyCaptureLimit + time.Nanosecond) }, TopologyCaptureExpired},
		{"zero_generation", func(f *affinityFixture) { f.topology.Generation = 0 }, TopologyReadFailed},
		{"invalid_source_state", func(f *affinityFixture) { f.topology.NUMAState = "invalid" }, TopologyReadFailed},
		{"open_budget", func(f *affinityFixture) { f.topology.Reads = topologyOpenLimit }, TopologyLimitExceeded},
		{"byte_budget", func(f *affinityFixture) { f.topology.Bytes = topologyByteLimit }, TopologyLimitExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAffinityFixture()
			tc.mutate(f)
			o := f.capture()
			if o.State != tc.state || f.selfReads != 0 || f.opens != 0 || len(o.Groups) != 0 {
				t.Fatal("invalid inherited budget performed work or renewed values")
			}
		})
	}
	f := newAffinityFixture()
	f.topology.Reads = topologyOpenLimit - 1
	o := f.capture()
	if o.State != TopologyLimitExceeded || f.selfReads != 1 || f.opens != 0 || o.CumulativeReads != topologyOpenLimit {
		t.Fatal("affinity received a second open allowance")
	}
	f = newAffinityFixture()
	f.topology.Bytes = topologyByteLimit - affinityStatusLimit
	o = f.capture()
	if o.State != TopologyLimitExceeded || f.selfReads != 0 {
		t.Fatal("read did not reserve EOF overflow byte")
	}
	f = newAffinityFixture()
	f.at = f.topology.StartedAt.Add(topologyCaptureLimit)
	if o = f.capture(); !o.Complete {
		t.Fatal("exact shared deadline boundary refused")
	}
	f = newAffinityFixture()
	o = readAffinityFiles(f.topology, func(path string, limit int, first bool) ([]byte, error) {
		data, err := f.readSelf(path, limit, first)
		f.at = f.topology.StartedAt.Add(topologyCaptureLimit + time.Nanosecond)
		return data, err
	}, f.open, func() time.Time { return f.at })
	if o.State != TopologyCaptureExpired || f.opens != 0 || len(o.Groups) != 0 {
		t.Fatal("deadline after status read did not fence later work")
	}
}

func TestThreadAffinityMaximumAndDisabled(t *testing.T) {
	var disabled *ThreadAffinitySampler
	calls := 0
	read := func(TopologyObservation) ThreadAffinityObservation {
		calls++
		return ThreadAffinityObservation{State: TopologyUnsupported}
	}
	if NewThreadAffinitySampler(false) != nil {
		t.Fatal("disabled constructor allocated")
	}
	if _, ok := disabled.sample(TopologyObservation{}, read); ok || calls != 0 {
		t.Fatal("disabled source performed work")
	}
	if n := testing.AllocsPerRun(100, func() { disabled.Sample(TopologyObservation{}) }); n != 0 {
		t.Fatal("disabled sampling allocated")
	}
	s := NewThreadAffinitySampler(true)
	s.generation = math.MaxUint64 - 2
	o, _ := s.sample(TopologyObservation{}, read)
	if o.Generation != math.MaxUint64-1 || calls != 1 {
		t.Fatal("last source generation differs")
	}
	o, _ = s.sample(TopologyObservation{}, read)
	if o.State != TopologyLimitExceeded || calls != 1 {
		t.Fatal("exhaustion wrapped or read")
	}
	f := newAffinityFixture()
	f.self = "Threads: 256\n"
	f.before = make([]affinityThread, 256)
	f.topology.CPUs = make([]TopologyCPU, 256)
	f.topology.Nodes = []TopologyNode{{ID: 0}}
	for i := range 256 {
		id := uint32(i + 1)
		f.before[i] = affinityThread{id: id, device: 1, inode: uint64(id)}
		f.topology.CPUs[i].ID = uint16(i)
		f.status[id] = "Cpus_allowed_list: " + strconv.Itoa(i) + "\nMems_allowed_list: 0\n"
	}
	f.after = slices.Clone(f.before)
	o = f.capture()
	if !o.Complete || len(o.Groups) != 256 || o.ListedThreads != 256 || o.ObservedThreads != 256 || !o.HasCPUIntersection || o.CPUIntersection != ([32]byte{}) || o.CumulativeReads != f.topology.Reads+514 {
		t.Fatal("maximum bounded grouping/known empty intersection differs")
	}
	f = newAffinityFixture()
	f.self = "Threads: 257\n"
	if o = f.capture(); o.State != TopologyLimitExceeded || f.opens != 0 {
		t.Fatal("reported over-cap threads accepted")
	}
	f = newAffinityFixture()
	f.before = make([]affinityThread, 257)
	if o = f.capture(); o.State != TopologyLimitExceeded || f.reads != 0 || len(o.Groups) != 0 {
		t.Fatal("enumerated over-cap prefix accepted")
	}
	if runtime.GOOS != "linux" {
		f = newAffinityFixture()
		at := time.Now()
		f.topology.StartedAt, f.topology.SampledAt = at.Add(-2*time.Millisecond), at.Add(-time.Millisecond)
		o, ok := NewThreadAffinitySampler(true).Sample(f.topology)
		if !ok || o.State != TopologyUnsupported || o.StartedAt.IsZero() || o.CumulativeReads != f.topology.Reads {
			t.Fatal("unsupported platform performed I/O or lost dated attempt")
		}
	}
}
