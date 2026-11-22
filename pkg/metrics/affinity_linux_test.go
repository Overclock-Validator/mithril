//go:build linux

package metrics

import (
	"bytes"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// This test-only syscall reads the locked calling thread. Its fixed buffer
// covers all 8192 IDs supported by the source; it never changes affinity.
func affinityKernelMask() ([1024]byte, error) {
	var mask [1024]byte
	n, _, errno := unix.RawSyscall(unix.SYS_SCHED_GETAFFINITY, 0, uintptr(len(mask)), uintptr(unsafe.Pointer(&mask[0])))
	if errno != 0 {
		return mask, errno
	}
	if n == 0 || n > uintptr(len(mask)) {
		return mask, errTopologyRecord
	}
	return mask, nil
}

// Build a same-byte oracle separately from parseAffinityStatus. This is used
// only after the source reports both masks known for every sampled thread.
func affinityWitnessGroup(t *testing.T, data []byte, o ThreadAffinityObservation) ThreadAffinityGroup {
	t.Helper()
	g := ThreadAffinityGroup{CPUState: TopologyAvailable, NodeState: TopologyAvailable}
	var seen uint8
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		var dictionary []uint16
		var mask []byte
		switch key {
		case "Cpus_allowed_list":
			dictionary, mask = o.CPUDictionary, g.CPUs[:]
			seen |= 1
		case "Mems_allowed_list":
			dictionary, mask = o.NodeDictionary, g.Nodes[:]
			seen |= 2
		default:
			continue
		}
		for _, id := range topologyWitnessIDs(t, []byte(strings.TrimSpace(value))) {
			index := slices.Index(dictionary, id)
			if index < 0 || index/8 >= len(mask) {
				t.Fatal("complete affinity contains an unknown dictionary ID")
			}
			mask[index/8] |= 1 << uint(index%8)
		}
	}
	if seen != 3 {
		t.Fatal("complete affinity omits an allowlisted field")
	}
	return g
}

func TestThreadAffinityLinuxReadOwnership(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	tid := uint32(unix.Gettid())
	topology, _ := NewTopologySampler(true).Sample()
	before := time.Now()
	var self []byte
	var selfCalls, opened, closed, listed, statusReads int
	raw := make(map[affinityThread][]byte)
	var ownBefore, ownAfter [1024]byte
	var ownBeforeError, ownAfterError error
	ownSeen := false
	o := readAffinityFiles(topology, func(path string, limit int, first bool) ([]byte, error) {
		selfCalls++
		if path != "/proc/self/status" || limit != 16384 || first {
			t.Fatal("unexpected process status read")
		}
		data, err := readHostFile(path, limit, first)
		if err == nil {
			self = bytes.Clone(data)
		}
		return data, err
	}, func(c *topologyCapture) (affinityTaskAccess, error) {
		access, err := openAffinityTasks(c)
		if err != nil {
			return access, err
		}
		opened++
		list, read, close := access.list, access.read, access.close
		access.list = func() ([]affinityThread, error) { listed++; return list() }
		access.read = func(thread affinityThread) ([]byte, error) {
			statusReads++
			if thread.id == tid {
				ownBefore, ownBeforeError = affinityKernelMask()
			}
			data, err := read(thread)
			if thread.id == tid {
				ownAfter, ownAfterError = affinityKernelMask()
				ownSeen = err == nil
			}
			if err == nil {
				raw[thread] = bytes.Clone(data)
			}
			return data, err
		}
		access.close = func() { closed++; close() }
		return access, nil
	}, time.Now)
	after := time.Now()
	if selfCalls > 1 || opened > 1 || closed != opened || listed > 2 || statusReads > 256 || o.CumulativeReads < topology.Reads || o.CumulativeReads > 2048 || o.CumulativeBytes < topology.Bytes || o.CumulativeBytes > 12<<20 {
		t.Fatal("own-source read, descriptor or combined budget differs")
	}
	if !o.StartedAt.IsZero() && (o.StartedAt.Before(before) || o.StartedAt == o.StartedAt.Round(0)) {
		t.Fatal("original affinity start lost")
	}
	if !o.SampledAt.IsZero() && (o.SampledAt.After(after) || o.SampledAt.Before(o.StartedAt) || o.SampledAt == o.SampledAt.Round(0)) {
		t.Fatal("original affinity end lost")
	}
	if !o.Complete {
		if o.State == TopologyAvailable || o.HasCPUIntersection && o.HasNodeIntersection {
			t.Fatal("incomplete source marked complete")
		}
		t.Logf("own-source state=%s membership_stable=%t observed=%d; unavailable/changing coverage is not a positive mask witness", o.State, o.MembershipStable, o.ObservedThreads)
		return
	}
	if selfCalls != 1 || opened != 1 || closed != 1 || listed != 2 || len(raw) != int(o.ListedThreads) || !ownSeen {
		t.Fatal("complete source lacks actual own-thread reads")
	}
	var reported uint64
	for _, line := range strings.Split(string(self), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok && key == "Threads" {
			var err error
			reported, err = strconv.ParseUint(strings.TrimSpace(value), 10, 32)
			if err != nil {
				t.Fatal("invalid same-byte thread count")
			}
		}
	}
	if reported != uint64(o.ReportedThreads) {
		t.Fatal("source-reported count differs from actual bytes")
	}
	want := make(map[ThreadAffinityGroup]uint32)
	for _, data := range raw {
		want[affinityWitnessGroup(t, data, o)]++
	}
	if len(want) != len(o.Groups) {
		t.Fatal("same-byte group count differs")
	}
	for _, group := range o.Groups {
		count := group.Threads
		group.Threads = 0
		if want[group] != count {
			t.Fatal("same-byte grouped masks/count differ")
		}
	}
	if ownBeforeError == nil && ownAfterError == nil && ownBefore == ownAfter {
		var own []byte
		for thread, data := range raw {
			if thread.id == tid {
				own = data
			}
		}
		g := affinityWitnessGroup(t, own, o)
		var wantKernel [1024]byte
		for index, id := range o.CPUDictionary {
			if g.CPUs[index/8]&(1<<uint(index%8)) != 0 {
				wantKernel[id/8] |= 1 << uint(id%8)
			}
		}
		if wantKernel != ownBefore {
			t.Fatal("locked own-thread status disagrees with unchanged read-only affinity witnesses")
		}
	} else {
		t.Log("calling-thread affinity syscall unavailable/changed; no independent syscall-equality claim")
	}
	t.Logf("same-byte sampled groups=%d threads=%d cumulative opens=%d bytes=%d; not simultaneous process affinity", len(o.Groups), o.ObservedThreads, o.CumulativeReads, o.CumulativeBytes)
}

func TestThreadAffinityLinuxAnchoredRefusal(t *testing.T) {
	at := time.Now()
	c := topologyCapture{now: time.Now, start: at, last: at}
	access, err := openAffinityTasks(&c)
	if err != nil {
		t.Fatalf("cannot open own task anchor: %v", err)
	}
	defer access.close()
	before := c.reads
	for _, thread := range []affinityThread{{id: 0, inode: 1}, {id: 1 << 31, inode: 1}, {id: uint32(unix.Gettid())}} {
		if _, err := access.read(thread); err == nil {
			t.Fatal("invalid private identity accepted")
		}
	}
	if c.reads != before {
		t.Fatal("invalid identity caused a child open")
	}
	threads, err := access.list()
	if err != nil {
		t.Fatalf("cannot enumerate own task anchor: %v", err)
	}
	// A wrong private inode is refused after opening only the own-thread
	// directory, before reading status. No foreign PID/path is accepted.
	thread := threads[0]
	thread.inode ^= 1
	if thread.inode == 0 {
		thread.inode = 2
	}
	before = c.reads
	if _, err := access.read(thread); err != errAffinityChanged || c.reads != before+1 {
		t.Fatal("replaced identity reached status read")
	}
}
