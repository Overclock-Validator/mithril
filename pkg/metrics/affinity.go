package metrics

import (
	"bytes"
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	affinityThreadLimit = 256
	affinityStatusLimit = 16 << 10
	affinityLineLimit   = 256
)

// ThreadAffinityGroup counts sampled threads with identical allowed masks and
// knownness. It is not a worker-pool binding, fixed affinity or page residence.
// Bit positions refer only to the dictionaries in the same observation.
type ThreadAffinityGroup struct {
	CPUs                [32]byte
	Nodes               [8]byte
	CPUState, NodeState TopologyState
	Threads             uint32
}

// ThreadAffinityObservation contains no OS thread IDs, names or raw status.
// Intersections are known independently and only over stable, fully read
// membership. Equal membership does not make the mask reads simultaneous.
type ThreadAffinityObservation struct {
	Generation, TopologyGeneration                  uint64
	StartedAt, SampledAt                            time.Time
	State                                           TopologyState
	CPUDictionary, NodeDictionary                   []uint16
	HasCPUDictionary, HasNodeDictionary             bool
	ReportedThreads, ListedThreads, ObservedThreads uint32
	ThreadsState                                    TopologyState
	MembershipStable, Complete                      bool
	Groups                                          []ThreadAffinityGroup
	CPUIntersection                                 [32]byte
	NodeIntersection                                [8]byte
	HasCPUIntersection, HasNodeIntersection         bool
	// These include the inherited topology work, not a new global allowance.
	CumulativeReads uint32
	CumulativeBytes uint64
}

// ThreadAffinitySampler is serially owned by the existing diagnostics owner.
// Construction performs no clock/I/O work and starts no timer or goroutine.
type ThreadAffinitySampler struct{ generation uint64 }

// NewThreadAffinitySampler enables sampling without capturing or starting work.
func NewThreadAffinitySampler(enabled bool) *ThreadAffinitySampler {
	if !enabled {
		return nil
	}
	return &ThreadAffinitySampler{}
}

// Sample consumes only the remainder of topology's original monotonic capture
// budget. The caller must not mutate that detached topology value concurrently.
// No previous affinity values are retained or renewed after failed captures.
func (s *ThreadAffinitySampler) Sample(topology TopologyObservation) (ThreadAffinityObservation, bool) {
	return s.sample(topology, readThreadAffinity)
}

func (s *ThreadAffinitySampler) sample(topology TopologyObservation, read func(TopologyObservation) ThreadAffinityObservation) (ThreadAffinityObservation, bool) {
	if s == nil {
		return ThreadAffinityObservation{}, false
	}
	if s.generation >= math.MaxUint64-1 {
		return ThreadAffinityObservation{Generation: s.generation, State: TopologyLimitExceeded}, true
	}
	o := read(topology)
	s.generation++
	o.Generation = s.generation
	return o, true
}

// affinityThread is private capture identity. Directory identities fence reuse
// between enumeration and status reads; they never reach returned observations.
type affinityThread struct {
	id            uint32
	device, inode uint64
}

// affinityTaskAccess owns one anchored /proc/self/task descriptor. Its bounded
// platform operations charge the shared capture and close every child handle.
type affinityTaskAccess struct {
	list  func() ([]affinityThread, error)
	read  func(affinityThread) ([]byte, error)
	close func()
}

var errAffinityChanged = errors.New("thread membership changed")

func affinityCheck(c *topologyCapture) bool {
	valid := c.check()
	if c.last == c.last.Round(0) {
		c.failure = TopologyCaptureExpired
		return false
	}
	return valid
}

// affinityOpen reserves an actual open attempt, including task directories.
func affinityOpen(c *topologyCapture) error {
	if !affinityCheck(c) {
		return c.failureError()
	}
	if c.reads >= topologyOpenLimit {
		c.failure = TopologyLimitExceeded
		return errTopologyLimit
	}
	c.reads++
	return nil
}

func affinityRoom(c *topologyCapture, bytes uint64) error {
	if !affinityCheck(c) {
		return c.failureError()
	}
	if c.bytes > topologyByteLimit || bytes > topologyByteLimit-c.bytes {
		c.failure = TopologyLimitExceeded
		return errTopologyLimit
	}
	return nil
}

func validAffinityClock(at time.Time) bool {
	return !at.IsZero() && at.Year() >= 1 && at.Year() <= 9999 && at != at.Round(0)
}

func affinityBudget(topology TopologyObservation, now func() time.Time) (topologyCapture, TopologyState) {
	c := topologyCapture{now: now, start: topology.StartedAt, last: topology.SampledAt, reads: topology.Reads, bytes: topology.Bytes}
	if !validAffinityClock(c.start) || !validAffinityClock(c.last) || c.last.Before(c.start) || c.last.Sub(c.start) > topologyCaptureLimit || topology.CPUState == TopologyCaptureExpired || topology.NUMAState == TopologyCaptureExpired {
		return c, TopologyCaptureExpired
	}
	if c.reads >= topologyOpenLimit || c.bytes >= topologyByteLimit {
		return c, TopologyLimitExceeded
	}
	if topology.Generation == 0 || topology.Generation == math.MaxUint64 || !affinitySourceState(topology.CPUState) || !affinitySourceState(topology.NUMAState) {
		return c, TopologyReadFailed
	}
	return c, ""
}

func affinitySourceState(state TopologyState) bool {
	switch state {
	case TopologyAvailable, TopologyPartial, TopologyUnsupported, TopologyDenied, TopologyReadFailed, TopologyLimitExceeded, TopologyBindingChanged, TopologyCaptureExpired:
		return true
	default:
		return false
	}
}

func affinityDictionary(ids []uint16, maximum int, allowEmpty bool) bool {
	if ids == nil || len(ids) > maximum || !allowEmpty && len(ids) == 0 {
		return false
	}
	for i, id := range ids {
		if id > topologyIDLimit || i != 0 && id <= ids[i-1] {
			return false
		}
	}
	return true
}

func affinityDictionaries(topology TopologyObservation, o *ThreadAffinityObservation) {
	if topology.CPUState == TopologyAvailable || topology.CPUState == TopologyPartial {
		if topology.CPUs != nil && len(topology.CPUs) <= topologyCPULimit {
			o.CPUDictionary = make([]uint16, len(topology.CPUs))
			for i, cpu := range topology.CPUs {
				o.CPUDictionary[i] = cpu.ID
			}
			o.HasCPUDictionary = affinityDictionary(o.CPUDictionary, topologyCPULimit, false)
		}
	}
	if topology.NUMAState == TopologyAvailable || topology.NUMAState == TopologyPartial {
		if topology.Nodes != nil && len(topology.Nodes) <= topologyNodeLimit {
			o.NodeDictionary = make([]uint16, len(topology.Nodes))
			for i, node := range topology.Nodes {
				o.NodeDictionary[i] = node.ID
			}
			o.HasNodeDictionary = affinityDictionary(o.NodeDictionary, topologyNodeLimit, true)
		}
	}
	if !o.HasCPUDictionary {
		o.CPUDictionary = nil
	}
	if !o.HasNodeDictionary {
		o.NodeDictionary = nil
	}
}

func readAffinityFiles(topology TopologyObservation, readSelf func(string, int, bool) ([]byte, error), open func(*topologyCapture) (affinityTaskAccess, error), now func() time.Time) ThreadAffinityObservation {
	c, failure := affinityBudget(topology, now)
	o := ThreadAffinityObservation{TopologyGeneration: topology.Generation, CumulativeReads: c.reads, CumulativeBytes: c.bytes}
	if failure != "" {
		o.State = failure
		return o
	}
	c.now = func() time.Time {
		at := now()
		if !validAffinityClock(at) {
			c.failure = TopologyCaptureExpired
		}
		return at
	}
	if !affinityCheck(&c) {
		o.State = c.failure
		return o
	}
	o.StartedAt = c.last
	affinityDictionaries(topology, &o)
	c.read = readSelf
	finish := func(state TopologyState) ThreadAffinityObservation {
		affinityCheck(&c)
		o.SampledAt, o.CumulativeReads, o.CumulativeBytes = c.last, c.reads, c.bytes
		if c.failure != "" {
			state = c.failure
		}
		o.State = state
		if state != TopologyAvailable && state != TopologyPartial {
			o.Groups = nil
			o.Complete, o.MembershipStable = false, false
			o.CPUIntersection, o.NodeIntersection = [32]byte{}, [8]byte{}
			o.HasCPUIntersection, o.HasNodeIntersection = false, false
		}
		return o
	}
	data, err := c.file("/proc/self/status", affinityStatusLimit)
	if err == nil {
		o.ReportedThreads, err = parseAffinityThreads(data)
	}
	o.ThreadsState = topologyError(err)
	if o.ReportedThreads > affinityThreadLimit {
		return finish(TopologyLimitExceeded)
	}
	access, err := open(&c)
	if err != nil {
		return finish(topologyError(err))
	}
	defer access.close()
	before, err := access.list()
	if err != nil {
		return finish(affinityError(err))
	}
	if err = validateAffinityThreads(before); err != nil {
		return finish(topologyError(err))
	}
	o.ListedThreads = uint32(len(before))
	groups := make(map[ThreadAffinityGroup]uint32)
	for _, thread := range before {
		data, err := access.read(thread)
		var group ThreadAffinityGroup
		if err == nil {
			group, err = parseAffinityStatus(data, o.CPUDictionary, o.NodeDictionary, o.HasCPUDictionary, o.HasNodeDictionary)
		}
		if err != nil {
			group = ThreadAffinityGroup{CPUState: affinityError(err), NodeState: affinityError(err)}
		} else {
			o.ObservedThreads++
		}
		groups[group]++
		if c.failure != "" {
			return finish(c.failure)
		}
	}
	after, err := access.list()
	if err != nil {
		return finish(affinityError(err))
	}
	if err = validateAffinityThreads(after); err != nil {
		return finish(topologyError(err))
	}
	if !slices.Equal(before, after) {
		return finish(TopologyBindingChanged)
	}
	o.MembershipStable = true
	for group, count := range groups {
		group.Threads = count
		o.Groups = append(o.Groups, group)
	}
	slices.SortFunc(o.Groups, func(a, b ThreadAffinityGroup) int {
		if n := bytes.Compare(a.CPUs[:], b.CPUs[:]); n != 0 {
			return n
		}
		if n := bytes.Compare(a.Nodes[:], b.Nodes[:]); n != 0 {
			return n
		}
		if n := strings.Compare(string(a.CPUState), string(b.CPUState)); n != 0 {
			return n
		}
		return strings.Compare(string(a.NodeState), string(b.NodeState))
	})
	completeReads := o.ThreadsState == TopologyAvailable && o.ReportedThreads == o.ListedThreads && o.ObservedThreads == o.ListedThreads
	o.HasCPUIntersection, o.HasNodeIntersection = completeReads, completeReads
	for i, group := range o.Groups {
		o.HasCPUIntersection = o.HasCPUIntersection && group.CPUState == TopologyAvailable
		o.HasNodeIntersection = o.HasNodeIntersection && group.NodeState == TopologyAvailable
		if i == 0 {
			o.CPUIntersection, o.NodeIntersection = group.CPUs, group.Nodes
		} else {
			for j := range o.CPUIntersection {
				o.CPUIntersection[j] &= group.CPUs[j]
			}
			for j := range o.NodeIntersection {
				o.NodeIntersection[j] &= group.Nodes[j]
			}
		}
	}
	if !o.HasCPUIntersection {
		o.CPUIntersection = [32]byte{}
	}
	if !o.HasNodeIntersection {
		o.NodeIntersection = [8]byte{}
	}
	o.Complete = o.HasCPUIntersection && o.HasNodeIntersection
	if o.Complete {
		return finish(TopologyAvailable)
	}
	return finish(TopologyPartial)
}

func affinityError(err error) TopologyState {
	if errors.Is(err, errAffinityChanged) {
		return TopologyBindingChanged
	}
	return topologyError(err)
}

func affinityTID(name string) (uint32, error) {
	if len(name) == 0 || len(name) > 10 || name[0] == '0' {
		return 0, errTopologyRecord
	}
	for _, ch := range name {
		if ch < '0' || ch > '9' {
			return 0, errTopologyRecord
		}
	}
	n, err := strconv.ParseUint(name, 10, 32)
	if err != nil || n > math.MaxInt32 {
		return 0, errTopologyRecord
	}
	return uint32(n), nil
}

func validateAffinityThreads(threads []affinityThread) error {
	if len(threads) > affinityThreadLimit {
		return errTopologyLimit
	}
	if len(threads) == 0 {
		return errTopologyRecord
	}
	for i, thread := range threads {
		if thread.id == 0 || thread.id > math.MaxInt32 || thread.inode == 0 || i != 0 && thread.id <= threads[i-1].id {
			return errTopologyRecord
		}
	}
	return nil
}

func affinityStatusLines(data []byte) ([]string, error) {
	if len(data) > affinityStatusLimit {
		return nil, errTopologyLimit
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return nil, errTopologyRecord
	}
	if bytes.Count(data, []byte{'\n'}) > affinityLineLimit {
		return nil, errTopologyLimit
	}
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n"), nil
}

func parseAffinityThreads(data []byte) (uint32, error) {
	lines, err := affinityStatusLines(data)
	if err != nil {
		return 0, err
	}
	var threads uint32
	seen := false
	for _, line := range lines {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			return 0, errTopologyRecord
		}
		if key != "Threads" {
			continue
		}
		if seen {
			return 0, errTopologyRecord
		}
		seen = true
		n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
		if err != nil || n == 0 {
			return 0, errTopologyRecord
		}
		threads = uint32(n)
	}
	if !seen {
		return 0, errTopologyRecord
	}
	return threads, nil
}

func parseAffinityStatus(data []byte, cpus, nodes []uint16, cpuKnown, nodeKnown bool) (ThreadAffinityGroup, error) {
	group := ThreadAffinityGroup{CPUState: TopologyUnsupported, NodeState: TopologyUnsupported}
	lines, err := affinityStatusLines(data)
	if err != nil {
		return group, err
	}
	var seen uint8
	for _, line := range lines {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			return ThreadAffinityGroup{}, errTopologyRecord
		}
		var bit uint8
		switch key {
		case "Cpus_allowed_list":
			bit = 1
		case "Mems_allowed_list":
			bit = 2
		default:
			continue
		}
		if seen&bit != 0 {
			return ThreadAffinityGroup{}, errTopologyRecord
		}
		seen |= bit
		var mask [32]byte
		var maskErr error
		if bit == 1 && cpuKnown {
			mask, maskErr = parseTopologyMask([]byte(strings.TrimSpace(value)+"\n"), cpus)
			group.CPUs, group.CPUState = mask, topologyError(maskErr)
		} else if bit == 2 && nodeKnown {
			mask, maskErr = parseTopologyMask([]byte(strings.TrimSpace(value)+"\n"), nodes)
			copy(group.Nodes[:], mask[:len(group.Nodes)])
			group.NodeState = topologyError(maskErr)
		}
	}
	return group, nil
}
