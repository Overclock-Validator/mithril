//go:build linux

package metrics

import (
	"bytes"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// topologyWitnessIDs independently expands the same bytes actually read by the
// sampler. It is not a later sysfs read or a changing-gauge bracket oracle.
func topologyWitnessIDs(t *testing.T, data []byte) []uint16 {
	t.Helper()
	var ids []uint16
	text := strings.TrimSpace(string(data))
	if text == "" {
		return ids
	}
	for _, token := range strings.Split(text, ",") {
		bounds := strings.Split(token, "-")
		if len(bounds) > 2 {
			t.Fatal("available source has invalid witness list")
		}
		first, err := strconv.ParseUint(bounds[0], 10, 16)
		if err != nil {
			t.Fatal(err)
		}
		last := first
		if len(bounds) == 2 {
			last, err = strconv.ParseUint(bounds[1], 10, 16)
		}
		if err != nil || last < first || last > 8191 || last-first+1 > 256 || len(ids)+int(last-first+1) > 256 {
			t.Fatal("available witness list exceeds bounds")
		}
		for id := first; id <= last; id++ {
			ids = append(ids, uint16(id))
		}
	}
	return ids
}

func TestTopologyLinuxReadOwnership(t *testing.T) {
	var reads uint32
	var inputBytes uint64
	raw := make(map[string][][]byte)
	before := time.Now()
	o := readTopologyFiles(func(path string, limit int, first bool) ([]byte, error) {
		reads++
		if first {
			t.Fatal("topology made a first-line rather than EOF-bounded read")
		}
		want := 0
		switch path {
		case topologyCPUPath + "possible", topologyCPUPath + "present", topologyCPUPath + "online", topologyNodePath + "possible", topologyNodePath + "online":
			want = 4096
		default:
			if strings.HasPrefix(path, topologyCPUPath+"cpu") {
				parts := strings.Split(strings.TrimPrefix(path, topologyCPUPath+"cpu"), "/")
				if len(parts) != 3 || parts[1] != "topology" {
					t.Fatal("unexpected CPU source path")
				}
				id, err := strconv.ParseUint(parts[0], 10, 16)
				if err != nil || id > 8191 || parts[0] != strconv.FormatUint(id, 10) || len(raw[topologyCPUPath+"possible"]) == 0 || !slices.Contains(topologyWitnessIDs(t, raw[topologyCPUPath+"possible"][0]), uint16(id)) {
					t.Fatal("CPU path not derived from validated possible IDs")
				}
				switch parts[2] {
				case "physical_package_id", "die_id", "core_id":
					want = 32
				case "core_cpus_list", "thread_siblings_list":
					want = 4096
				}
			} else if strings.HasPrefix(path, topologyNodePath+"node") {
				parts := strings.Split(strings.TrimPrefix(path, topologyNodePath+"node"), "/")
				if len(parts) != 2 {
					t.Fatal("unexpected node source path")
				}
				id, err := strconv.ParseUint(parts[0], 10, 16)
				if err != nil || id > 8191 || parts[0] != strconv.FormatUint(id, 10) || len(raw[topologyNodePath+"online"]) == 0 || !slices.Contains(topologyWitnessIDs(t, raw[topologyNodePath+"online"][0]), uint16(id)) {
					t.Fatal("node path not derived from validated online IDs")
				}
				switch parts[1] {
				case "cpulist":
					want = 4096
				case "meminfo":
					want = 16384
				}
			}
		}
		if want == 0 || limit != want {
			t.Fatal("unexpected sysfs source or read bound")
		}
		data, err := readHostFile(path, limit, false)
		inputBytes += uint64(len(data))
		if err == nil {
			raw[path] = append(raw[path], bytes.Clone(data))
		}
		return data, err
	}, time.Now)
	after := time.Now()
	if o.Reads != reads || o.Bytes != inputBytes || reads == 0 || reads > 2048 || inputBytes > 12<<20 || o.StartedAt.Before(before) || o.SampledAt.After(after) || o.SampledAt.Before(o.StartedAt) {
		t.Fatal("capture ownership, clocks or I/O counters differ")
	}
	if o.CPUState == TopologyAvailable || o.CPUState == TopologyPartial {
		possible := topologyWitnessIDs(t, raw[topologyCPUPath+"possible"][0])
		present := topologyWitnessIDs(t, raw[topologyCPUPath+"present"][0])
		online := topologyWitnessIDs(t, raw[topologyCPUPath+"online"][0])
		if len(o.CPUs) != len(possible) {
			t.Fatal("same-byte possible dictionary differs")
		}
		for i, cpu := range o.CPUs {
			if cpu.ID != possible[i] || cpu.Present != slices.Contains(present, cpu.ID) || cpu.Online != slices.Contains(online, cpu.ID) {
				t.Fatal("same-byte CPU presence differs")
			}
			base := topologyCPUPath + "cpu" + strconv.Itoa(int(cpu.ID)) + "/topology/"
			for name, value := range map[string]TopologyID{"physical_package_id": cpu.Package, "die_id": cpu.Die, "core_id": cpu.Core} {
				if value.State != TopologyAvailable {
					continue
				}
				n, err := strconv.ParseInt(strings.TrimSpace(string(raw[base+name][0])), 10, 32)
				if err != nil || n < 0 || int32(n) != value.Value {
					t.Fatal("same-byte hierarchy identity differs")
				}
			}
			if cpu.SiblingsState == TopologyAvailable {
				data := raw[base+"core_cpus_list"]
				if len(data) == 0 {
					data = raw[base+"thread_siblings_list"]
				}
				var want [32]byte
				for _, id := range topologyWitnessIDs(t, data[0]) {
					index := slices.Index(possible, id)
					if index < 0 {
						t.Fatal("foreign sibling bit")
					}
					want[index/8] |= 1 << uint(index%8)
				}
				if want != cpu.Siblings {
					t.Fatal("same-byte sibling mask differs")
				}
			}
		}
	}
	var possibleNodes, onlineNodes []uint16
	if o.NUMAState == TopologyAvailable || o.NUMAState == TopologyPartial {
		possibleNodes = topologyWitnessIDs(t, raw[topologyNodePath+"possible"][0])
		onlineNodes = topologyWitnessIDs(t, raw[topologyNodePath+"online"][0])
		if len(o.Nodes) != len(possibleNodes) {
			t.Fatal("same-byte node dictionary differs")
		}
	}
	for index, node := range o.Nodes {
		if node.ID != possibleNodes[index] || node.Online != slices.Contains(onlineNodes, node.ID) {
			t.Fatal("same-byte node presence differs")
		}
		if node.CPUsState == TopologyAvailable {
			data := raw[topologyNodePath+"node"+strconv.Itoa(int(node.ID))+"/cpulist"][0]
			var want [32]byte
			for _, id := range topologyWitnessIDs(t, data) {
				index := slices.IndexFunc(o.CPUs, func(cpu TopologyCPU) bool { return cpu.ID == id })
				if index < 0 {
					t.Fatal("foreign node CPU bit")
				}
				want[index/8] |= 1 << uint(index%8)
			}
			if want != node.CPUs {
				t.Fatal("same-byte node CPU mask differs")
			}
		}
		if node.MemoryState != TopologyAvailable {
			continue
		}
		path := topologyNodePath + "node" + strconv.Itoa(int(node.ID)) + "/meminfo"
		values := make(map[string]uint64)
		for _, line := range strings.Split(string(raw[path][0]), "\n") {
			fields := strings.Fields(line)
			if len(fields) != 5 {
				continue
			}
			if fields[2] != "MemTotal:" && fields[2] != "MemFree:" && fields[2] != "Shmem:" {
				continue
			}
			n, err := strconv.ParseUint(fields[3], 10, 64)
			if err != nil || fields[4] != "kB" {
				t.Fatal("available node memory has invalid witness")
			}
			values[fields[2]] = n * 1024
		}
		shmem, hasShmem := values["Shmem:"]
		want := TopologyNodeMemory{TotalBytes: values["MemTotal:"], FreeBytes: values["MemFree:"], ShmemBytes: shmem, HasShmem: hasShmem}
		if !reflect.DeepEqual(node.Memory, want) {
			t.Fatal("same-byte node memory differs")
		}
	}
	t.Logf("CPU=%s NUMA=%s reads=%d bytes=%d; optional unavailable hardware is not a positive field witness", o.CPUState, o.NUMAState, reads, inputBytes)
}
