package metrics

import (
	"errors"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	topologyCPULimit     = 256
	topologyNodeLimit    = 64
	topologyIDLimit      = 8191
	topologyOpenLimit    = 2048
	topologyByteLimit    = 12 << 20
	topologyCaptureLimit = time.Second
	topologyCPUPath      = "/sys/devices/system/cpu/"
	topologyNodePath     = "/sys/devices/system/node/"
)

// TopologyState describes capture coverage, not processor utilization or process
// placement. The zero value is reserved for an optional field not sampled.
type TopologyState string

const (
	TopologyAvailable      TopologyState = "available"
	TopologyPartial        TopologyState = "partial"
	TopologyUnsupported    TopologyState = "unsupported"
	TopologyDenied         TopologyState = "denied"
	TopologyReadFailed     TopologyState = "read_failed"
	TopologyLimitExceeded  TopologyState = "limit_exceeded"
	TopologyBindingChanged TopologyState = "binding_changed"
	TopologyCaptureExpired TopologyState = "capture_expired"
)

// TopologyID is known only when State is available. Negative kernel sentinels
// are unsupported identities, never a fabricated package, die or core zero.
type TopologyID struct {
	Value int32
	State TopologyState
}

// TopologyCPU retains a possible CPU's independent present/online bits.
// Siblings uses indices into the same observation's sorted CPUs dictionary,
// not numeric CPU IDs. NUMANode is a raw node ID, known only when HasNUMANode.
type TopologyCPU struct {
	ID                 uint16
	Present, Online    bool
	Package, Die, Core TopologyID
	Siblings           [32]byte
	SiblingsState      TopologyState
	NUMANode           uint16
	HasNUMANode        bool
}

// TopologyNodeMemory reports visible host-node bytes, not process residence.
// Shmem is optional native shared-memory accounting, not tile attribution.
type TopologyNodeMemory struct {
	TotalBytes, FreeBytes, ShmemBytes uint64
	HasShmem                          bool
}

// TopologyNode is one possible NUMA node. Offline nodes have no sampled CPU or
// memory values. CPUs uses the observation's CPU dictionary, including offline
// possible CPUs; no foreign bit is silently discarded.
type TopologyNode struct {
	ID          uint16
	Online      bool
	CPUs        [32]byte
	CPUsState   TopologyState
	Memory      TopologyNodeMemory
	MemoryState TopologyState
}

// TopologyObservation is one bounded CPU/NUMA capture in the visible namespace.
// Clocks retain their original monotonic component. Rows are detached and sorted
// by raw ID; no prior successful values are retained or redated after a failure.
type TopologyObservation struct {
	Generation           uint64
	StartedAt, SampledAt time.Time
	CPUState, NUMAState  TopologyState
	CPUs                 []TopologyCPU
	Nodes                []TopologyNode
	Reads                uint32
	Bytes                uint64
}

// TopologySampler belongs to one diagnostics owner. It has no timer, goroutine
// or cached gauges. The caller supplies the opt-in cadence and run binding.
type TopologySampler struct{ generation uint64 }

// NewTopologySampler enables sampling without any clock or operating-system read.
func NewTopologySampler(enabled bool) *TopologySampler {
	if !enabled {
		return nil
	}
	return &TopologySampler{}
}

// Sample returns false only when disabled. Unsupported systems return a dated
// unsupported attempt. Generation exhaustion retires sampling without more I/O.
func (s *TopologySampler) Sample() (TopologyObservation, bool) { return s.sample(readTopology) }

func (s *TopologySampler) sample(read func() TopologyObservation) (TopologyObservation, bool) {
	if s == nil {
		return TopologyObservation{}, false
	}
	if s.generation >= math.MaxUint64-1 {
		return TopologyObservation{Generation: s.generation, CPUState: TopologyLimitExceeded, NUMAState: TopologyLimitExceeded}, true
	}
	o := read()
	s.generation++
	o.Generation = s.generation
	return o, true
}

var (
	errTopologyRecord  = errors.New("invalid topology record")
	errTopologyLimit   = errors.New("topology capture limit")
	errTopologyExpired = errors.New("topology capture expired")
	errTopologyForeign = errors.New("topology dictionary mismatch")
)

// topologyCapture shares one cooperative deadline and input budget across all
// reads. It cannot interrupt an already blocked kernel read.
type topologyCapture struct {
	read        func(string, int, bool) ([]byte, error)
	now         func() time.Time
	start, last time.Time
	reads       uint32
	bytes       uint64
	failure     TopologyState
}

func (c *topologyCapture) check() bool {
	at := c.now()
	if at.IsZero() || at.Year() < 1 || at.Year() > 9999 || at.Before(c.last) || at.Sub(c.start) > topologyCaptureLimit {
		c.failure = TopologyCaptureExpired
	}
	c.last = at
	return c.failure == ""
}

func (c *topologyCapture) file(path string, limit int) ([]byte, error) {
	if !c.check() {
		return nil, c.failureError()
	}
	if c.reads == topologyOpenLimit || uint64(limit)+1 > topologyByteLimit-c.bytes {
		c.failure = TopologyLimitExceeded
		return nil, errTopologyLimit
	}
	c.reads++
	data, err := c.read(path, limit, false)
	c.bytes += uint64(len(data))
	if !c.check() {
		return nil, c.failureError()
	}
	if c.bytes > topologyByteLimit {
		c.failure = TopologyLimitExceeded
		return nil, errTopologyLimit
	}
	if len(data) > limit {
		return nil, errTopologyLimit
	}
	return data, err
}

func (c *topologyCapture) failureError() error {
	if c.failure == TopologyCaptureExpired {
		return errTopologyExpired
	}
	return errTopologyLimit
}

func topologyError(err error) TopologyState {
	switch {
	case err == nil:
		return TopologyAvailable
	case errors.Is(err, errTopologyLimit):
		return TopologyLimitExceeded
	case errors.Is(err, errTopologyExpired):
		return TopologyCaptureExpired
	case os.IsNotExist(err):
		return TopologyUnsupported
	case os.IsPermission(err):
		return TopologyDenied
	default:
		// readHostFile's errHostRecord conflates oversized input and a failed
		// read. Do not guess which occurred; both refuse the affected value.
		return TopologyReadFailed
	}
}

func (c *topologyCapture) list(path string, maximum int) ([]uint16, error) {
	data, err := c.file(path, 4096)
	if err != nil {
		return nil, err
	}
	return parseTopologyList(data, maximum)
}

type topologyCPULists struct{ possible, present, online []uint16 }
type topologyNodeLists struct{ possible, online []uint16 }

func (c *topologyCapture) cpuLists() (topologyCPULists, error) {
	var lists topologyCPULists
	var err error
	if lists.possible, err = c.list(topologyCPUPath+"possible", topologyCPULimit); err != nil {
		return lists, err
	}
	if len(lists.possible) == 0 {
		return lists, errTopologyRecord
	}
	if lists.present, err = c.list(topologyCPUPath+"present", topologyCPULimit); err != nil {
		return lists, err
	}
	if lists.online, err = c.list(topologyCPUPath+"online", topologyCPULimit); err != nil {
		return lists, err
	}
	if !topologySubset(lists.present, lists.possible) || !topologySubset(lists.online, lists.present) {
		return lists, errTopologyRecord
	}
	return lists, nil
}

func (c *topologyCapture) nodeLists() (topologyNodeLists, error) {
	var lists topologyNodeLists
	var err error
	if lists.possible, err = c.list(topologyNodePath+"possible", topologyNodeLimit); err != nil {
		return lists, err
	}
	if lists.online, err = c.list(topologyNodePath+"online", topologyNodeLimit); err != nil {
		return lists, err
	}
	if !topologySubset(lists.online, lists.possible) {
		return lists, errTopologyRecord
	}
	return lists, nil
}

func readTopologyFiles(read func(string, int, bool) ([]byte, error), now func() time.Time) TopologyObservation {
	start := now()
	c := topologyCapture{read: read, now: now, start: start, last: start}
	if start.IsZero() || start.Year() < 1 || start.Year() > 9999 {
		c.failure = TopologyCaptureExpired
	}
	o := TopologyObservation{StartedAt: start}
	cpus, cpuErr := c.cpuLists()
	nodes, nodeErr := c.nodeLists()
	o.CPUState, o.NUMAState = topologyError(cpuErr), topologyError(nodeErr)
	if cpuErr == nil {
		o.CPUs = make([]TopologyCPU, len(cpus.possible))
		for i, id := range cpus.possible {
			row := &o.CPUs[i]
			row.ID = id
			_, row.Present = slices.BinarySearch(cpus.present, id)
			_, row.Online = slices.BinarySearch(cpus.online, id)
			base := topologyCPUPath + "cpu" + strconv.Itoa(int(id)) + "/topology/"
			row.Package = c.identity(base + "physical_package_id")
			row.Die = c.identity(base + "die_id")
			row.Core = c.identity(base + "core_id")
			data, err := c.file(base+"core_cpus_list", 4096)
			if os.IsNotExist(err) {
				data, err = c.file(base+"thread_siblings_list", 4096)
			}
			if err == nil {
				row.Siblings, err = parseTopologyMask(data, cpus.possible)
			}
			row.SiblingsState = topologyError(err)
			if errors.Is(err, errTopologyForeign) {
				o.CPUState = TopologyReadFailed
				break
			}
			if row.Package.State != TopologyAvailable || row.Die.State != TopologyAvailable || row.Core.State != TopologyAvailable || row.SiblingsState != TopologyAvailable {
				o.CPUState = TopologyPartial
			}
		}
	}
	if nodeErr == nil {
		o.Nodes = make([]TopologyNode, len(nodes.possible))
		for i, id := range nodes.possible {
			row := &o.Nodes[i]
			row.ID = id
			_, row.Online = slices.BinarySearch(nodes.online, id)
			if !row.Online {
				continue
			}
			base := topologyNodePath + "node" + strconv.Itoa(int(id)) + "/"
			data, err := c.file(base+"cpulist", 4096)
			if err == nil {
				if cpuErr != nil {
					err = errTopologyForeign
				} else {
					row.CPUs, err = parseTopologyMask(data, cpus.possible)
				}
			}
			row.CPUsState = topologyError(err)
			foreign := errors.Is(err, errTopologyForeign) && cpuErr == nil
			data, err = c.file(base+"meminfo", 16384)
			if err == nil {
				row.Memory, err = parseTopologyMemory(data, id)
			}
			row.MemoryState = topologyError(err)
			if foreign {
				o.NUMAState = TopologyReadFailed
				break
			}
			if row.CPUsState != TopologyAvailable || row.MemoryState != TopologyAvailable {
				o.NUMAState = TopologyPartial
			}
		}
	}
	if cpuErr == nil {
		after, err := c.cpuLists()
		if err != nil {
			o.CPUState = topologyError(err)
		} else if !slices.Equal(cpus.possible, after.possible) || !slices.Equal(cpus.present, after.present) || !slices.Equal(cpus.online, after.online) {
			o.CPUState = TopologyBindingChanged
		}
	}
	if nodeErr == nil {
		after, err := c.nodeLists()
		if err != nil {
			o.NUMAState = topologyError(err)
		} else if !slices.Equal(nodes.possible, after.possible) || !slices.Equal(nodes.online, after.online) {
			o.NUMAState = TopologyBindingChanged
		}
	}
	c.check()
	o.SampledAt, o.Reads, o.Bytes = c.last, c.reads, c.bytes
	if c.failure != "" {
		o.CPUState, o.NUMAState = c.failure, c.failure
	}
	if o.CPUState != TopologyAvailable && o.CPUState != TopologyPartial {
		o.CPUs = nil
		for i := range o.Nodes {
			if o.Nodes[i].Online {
				o.Nodes[i].CPUs, o.Nodes[i].CPUsState = [32]byte{}, TopologyReadFailed
			}
		}
		if o.NUMAState == TopologyAvailable {
			o.NUMAState = TopologyPartial
		}
	}
	if o.NUMAState != TopologyAvailable && o.NUMAState != TopologyPartial {
		o.Nodes = nil
	}
	// A missing node list could hide an additional association. Only complete
	// online-node lists support a unique CPU-to-node assignment.
	complete := o.NUMAState == TopologyAvailable || o.NUMAState == TopologyPartial
	for _, n := range o.Nodes {
		if n.Online && n.CPUsState != TopologyAvailable {
			complete = false
		}
	}
	if complete {
		for i := range o.CPUs {
			count := 0
			for _, n := range o.Nodes {
				if n.Online && n.CPUs[i/8]&(1<<uint(i%8)) != 0 {
					o.CPUs[i].NUMANode = n.ID
					count++
				}
			}
			o.CPUs[i].HasNUMANode = count == 1
			if count != 1 {
				o.CPUs[i].NUMANode = 0
			}
		}
	}
	return o
}

func (c *topologyCapture) identity(path string) TopologyID {
	data, err := c.file(path, 32)
	if err != nil {
		return TopologyID{State: topologyError(err)}
	}
	if len(data) == 0 || data[len(data)-1] != '\n' || strings.Count(string(data), "\n") != 1 {
		return TopologyID{State: TopologyReadFailed}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 32)
	if err != nil {
		return TopologyID{State: TopologyReadFailed}
	}
	if n < 0 {
		return TopologyID{State: TopologyUnsupported}
	}
	return TopologyID{Value: int32(n), State: TopologyAvailable}
}

func parseTopologyList(data []byte, maximum int) ([]uint16, error) {
	if len(data) > 4096 {
		return nil, errTopologyLimit
	}
	if len(data) == 0 || data[len(data)-1] != '\n' || strings.Count(string(data), "\n") != 1 {
		return nil, errTopologyRecord
	}
	text := strings.TrimSpace(string(data))
	ids := make([]uint16, 0)
	if text == "" {
		return ids, nil
	}
	for part := range strings.SplitSeq(text, ",") {
		left, right, ranged := strings.Cut(part, "-")
		first, err := topologyNumber(left)
		if err != nil {
			return nil, err
		}
		last := first
		if ranged {
			last, err = topologyNumber(right)
		}
		if err != nil {
			return nil, err
		}
		if last < first || len(ids) != 0 && first <= uint64(ids[len(ids)-1]) {
			return nil, errTopologyRecord
		}
		if last-first+1 > uint64(maximum-len(ids)) {
			return nil, errTopologyLimit
		}
		for id := first; id <= last; id++ {
			ids = append(ids, uint16(id))
		}
	}
	return ids, nil
}

func topologyNumber(text string) (uint64, error) {
	if text == "" {
		return 0, errTopologyRecord
	}
	for _, ch := range text {
		if ch < '0' || ch > '9' {
			return 0, errTopologyRecord
		}
	}
	n, err := strconv.ParseUint(text, 10, 64)
	if errors.Is(err, strconv.ErrRange) || n > topologyIDLimit {
		return 0, errTopologyLimit
	}
	if err != nil {
		return 0, errTopologyRecord
	}
	return n, nil
}

func topologySubset(ids, dictionary []uint16) bool {
	for _, id := range ids {
		if _, ok := slices.BinarySearch(dictionary, id); !ok {
			return false
		}
	}
	return true
}

func parseTopologyMask(data []byte, dictionary []uint16) ([32]byte, error) {
	var mask [32]byte
	ids, err := parseTopologyList(data, topologyCPULimit)
	if err != nil {
		return mask, err
	}
	for _, id := range ids {
		i, ok := slices.BinarySearch(dictionary, id)
		if !ok || i >= topologyCPULimit {
			return [32]byte{}, errTopologyForeign
		}
		mask[i/8] |= 1 << uint(i%8)
	}
	return mask, nil
}

func parseTopologyMemory(data []byte, node uint16) (TopologyNodeMemory, error) {
	var m TopologyNodeMemory
	if len(data) > 16384 {
		return m, errTopologyLimit
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return m, errTopologyRecord
	}
	if strings.Count(string(data), "\n") > 256 {
		return m, errTopologyLimit
	}
	var seen uint8
	for line := range strings.SplitSeq(strings.TrimSuffix(string(data), "\n"), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[0] != "Node" || fields[1] != strconv.Itoa(int(node)) || !strings.HasSuffix(fields[2], ":") {
			return TopologyNodeMemory{}, errTopologyRecord
		}
		var target *uint64
		var bit uint8
		switch fields[2] {
		case "MemTotal:":
			target, bit = &m.TotalBytes, 1
		case "MemFree:":
			target, bit = &m.FreeBytes, 2
		case "Shmem:":
			target, bit = &m.ShmemBytes, 4
		default:
			continue
		}
		if seen&bit != 0 || len(fields) != 5 || fields[4] != "kB" {
			return TopologyNodeMemory{}, errTopologyRecord
		}
		n, err := strconv.ParseUint(fields[3], 10, 64)
		if err != nil || n > math.MaxUint64/1024 {
			return TopologyNodeMemory{}, errTopologyRecord
		}
		*target, seen = n*1024, seen|bit
	}
	if seen&3 != 3 || m.FreeBytes > m.TotalBytes {
		return TopologyNodeMemory{}, errTopologyRecord
	}
	m.HasShmem = seen&4 != 0
	return m, nil
}
