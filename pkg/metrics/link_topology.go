package metrics

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"slices"
	"time"
)

const (
	linkTopologyLimit  = 256
	linkReceiveBytes   = 64 << 10
	linkDatagramLimit  = 16
	linkInputLimit     = 1 << 20
	linkAttributeLimit = 128
	linkDepthLimit     = 3
	linkCaptureLimit   = 250 * time.Millisecond
	linkReadLimit      = 100 * time.Millisecond
	// Linux UAPI values keep the bounded byte parser testable on other hosts.
	// The Linux test checks these against the installed x/sys constants.
	linkNew              = 16
	linkGet              = 18
	linkDone             = 3
	linkError            = 2
	linkMulti            = 2
	linkDumpInterrupted  = 0x10
	linkRequestDump      = 0x301
	linkAttrNested       = 0x8000
	linkAttrNetworkOrder = 0x4000
	linkName             = 3
	linkMTU              = 4
	linkParent           = 5
	linkMaster           = 10
	linkOperState        = 16
	linkInfo             = 18
	linkCarrier          = 33
	linkXDP              = 43
)

// LinkTopology contains sampled kernel attributes, not path reachability or
// delivery. Link/master indices are raw references, not inferred parentage.
// Kind, OperationalState and XDPMode contain only fixed allowlisted strings.
type LinkTopology struct {
	Index                  uint32
	AdminUp                bool
	MTU                    uint32
	HasMTU                 bool
	OperationalState       string
	Carrier, HasCarrier    bool
	MasterIndex, LinkIndex uint32
	HasMaster, HasLink     bool
	Kind, XDPMode          string
}

// LinkTopologyObservation has no exported names, addresses, raw flags or BPF
// identifiers. Namespace and validated names remain private join fences.
// Matching two inventories is not an atomic attribute snapshot or a lifetime
// guarantee for an interface index.
type LinkTopologyObservation struct {
	Generation, TopologyGeneration, AffinityGeneration uint64
	MetadataStartedAt, StartedAt, SampledAt            time.Time
	State                                              TopologyState
	Links                                              []LinkTopology
	CumulativeReads                                    uint32
	CumulativeBytes                                    uint64
	Datagrams                                          uint32
	NetlinkBytes                                       uint64
	namespace                                          [2]uint64
	identities                                         []linkTopologyIdentity
}

type linkTopologyIdentity struct {
	index uint32
	name  string
}
type linkTopologyRow struct {
	value    LinkTopology
	identity linkTopologyIdentity
}

// LinkTopologySampler is serially owned; construction does not read or start
// a worker. Every capture consumes only the prior metadata budget remainder.
type LinkTopologySampler struct{ generation uint64 }

// NewLinkTopologySampler enables passive sampling without any OS/clock work.
func NewLinkTopologySampler(enabled bool) *LinkTopologySampler {
	if !enabled {
		return nil
	}
	return &LinkTopologySampler{}
}

// Sample requires the detached topology and subsequent affinity attempt from
// this same metadata cycle. Failed captures never retain or renew old links.
func (s *LinkTopologySampler) Sample(topology TopologyObservation, affinity ThreadAffinityObservation) (LinkTopologyObservation, bool) {
	return s.sample(topology, affinity, readLinkTopology)
}

func (s *LinkTopologySampler) sample(topology TopologyObservation, affinity ThreadAffinityObservation, read func(TopologyObservation, ThreadAffinityObservation) LinkTopologyObservation) (LinkTopologyObservation, bool) {
	if s == nil {
		return LinkTopologyObservation{}, false
	}
	if s.generation >= math.MaxUint64-1 {
		return LinkTopologyObservation{Generation: s.generation, State: TopologyLimitExceeded}, true
	}
	o := read(topology, affinity)
	s.generation++
	o.Generation = s.generation
	return o, true
}

type linkTopologyCapture struct {
	base       topologyCapture
	started    time.Time
	datagrams  uint32
	inputBytes uint64
}

func linkTopologyBudget(topology TopologyObservation, affinity ThreadAffinityObservation, now func() time.Time) (linkTopologyCapture, TopologyState) {
	c, state := affinityBudget(topology, now)
	b := linkTopologyCapture{base: c}
	if state != "" {
		return b, state
	}
	if affinity.Generation == 0 || affinity.Generation == math.MaxUint64 || affinity.TopologyGeneration != topology.Generation || !affinitySourceState(affinity.State) || affinity.CumulativeReads < c.reads || affinity.CumulativeBytes < c.bytes {
		return b, TopologyReadFailed
	}
	if !validAffinityClock(affinity.StartedAt) || !validAffinityClock(affinity.SampledAt) || affinity.StartedAt.Before(c.last) || affinity.SampledAt.Before(affinity.StartedAt) || affinity.SampledAt.Sub(c.start) > topologyCaptureLimit || affinity.State == TopologyCaptureExpired {
		return b, TopologyCaptureExpired
	}
	b.base.last, b.base.reads, b.base.bytes = affinity.SampledAt, affinity.CumulativeReads, affinity.CumulativeBytes
	if b.base.reads >= topologyOpenLimit || b.base.bytes >= topologyByteLimit {
		return b, TopologyLimitExceeded
	}
	return b, ""
}

func (c *linkTopologyCapture) check() bool {
	if !affinityCheck(&c.base) {
		return false
	}
	if !c.started.IsZero() && c.base.last.Sub(c.started) > linkCaptureLimit {
		c.base.failure = TopologyCaptureExpired
		return false
	}
	return true
}

func (c *linkTopologyCapture) remaining() time.Duration {
	if !c.check() {
		return 0
	}
	remaining := min(linkReadLimit, topologyCaptureLimit-c.base.last.Sub(c.base.start), linkCaptureLimit-c.base.last.Sub(c.started))
	if remaining <= 0 {
		c.base.failure = TopologyCaptureExpired
		return 0
	}
	return remaining
}

// linkTopologySocket is one owned, unsubscribed route socket. Both fixed dumps
// share its bounded receive work. No callback can request a mutation message.
type linkTopologySocket struct {
	port    uint32
	send    func([]byte) error
	receive func([]byte, time.Duration) (n int, sender, groups uint32, truncated bool, err error)
	close   func()
}

var errLinkBinding = errors.New("link binding changed")
var errLinkUnsupported = errors.New("link source unsupported")

func linkTopologyError(err error) TopologyState {
	if errors.Is(err, errLinkBinding) {
		return TopologyBindingChanged
	}
	if errors.Is(err, errLinkUnsupported) {
		return TopologyUnsupported
	}
	return topologyError(err)
}

func readLinkTopologyCapture(topology TopologyObservation, affinity ThreadAffinityObservation, namespace func() ([2]uint64, error), open func(*linkTopologyCapture) (linkTopologySocket, error), now func() time.Time) (o LinkTopologyObservation) {
	c, state := linkTopologyBudget(topology, affinity, now)
	o = LinkTopologyObservation{TopologyGeneration: topology.Generation, AffinityGeneration: affinity.Generation, MetadataStartedAt: topology.StartedAt, CumulativeReads: c.base.reads, CumulativeBytes: c.base.bytes, State: state}
	if state != "" {
		return o
	}
	if !c.check() {
		o.State = c.base.failure
		return o
	}
	c.started, o.StartedAt = c.base.last, c.base.last
	defer func() {
		c.check()
		o.SampledAt, o.CumulativeReads, o.CumulativeBytes = c.base.last, c.base.reads, c.base.bytes
		o.Datagrams, o.NetlinkBytes = c.datagrams, c.inputBytes
		if c.base.failure != "" {
			o.State = c.base.failure
		}
		if o.State != TopologyAvailable {
			o.Links, o.identities, o.namespace = nil, nil, [2]uint64{}
		}
	}()
	before, err := namespace()
	if !c.check() {
		return o
	}
	if err != nil {
		o.State = linkTopologyError(err)
		return o
	}
	if before[1] == 0 {
		o.State = TopologyReadFailed
		return o
	}
	socket, err := open(&c)
	if err != nil {
		o.State = linkTopologyError(err)
		return o
	}
	defer socket.close()
	first, err := readLinkTopologyDump(&c, socket, 1)
	if err != nil {
		o.State = linkTopologyError(err)
		return o
	}
	second, err := readLinkTopologyDump(&c, socket, 2)
	if err != nil {
		o.State = linkTopologyError(err)
		return o
	}
	if !c.check() {
		return o
	}
	after, err := namespace()
	if !c.check() {
		return o
	}
	if err != nil {
		o.State = linkTopologyError(err)
		return o
	}
	if before != after || len(first) != len(second) {
		o.State = TopologyBindingChanged
		return o
	}
	for i := range first {
		if first[i].identity != second[i].identity {
			o.State = TopologyBindingChanged
			return o
		}
	}
	o.Links = make([]LinkTopology, len(first))
	o.identities = make([]linkTopologyIdentity, len(first))
	for i, row := range first {
		o.Links[i], o.identities[i] = row.value, row.identity
	}
	o.State, o.namespace = TopologyAvailable, before
	return o
}

func linkTopologyRequest(sequence uint32) [32]byte {
	var request [32]byte
	binary.NativeEndian.PutUint32(request[:4], uint32(len(request)))
	binary.NativeEndian.PutUint16(request[4:6], linkGet)
	binary.NativeEndian.PutUint16(request[6:8], linkRequestDump)
	binary.NativeEndian.PutUint32(request[8:12], sequence)
	// AF_UNSPEC, all ifinfomsg fields and the sender port are zero. There are
	// no attributes, filters, subscription groups or configurable command.
	return request
}

func readLinkTopologyDump(c *linkTopologyCapture, socket linkTopologySocket, sequence uint32) ([]linkTopologyRow, error) {
	if !c.check() {
		return nil, c.base.failureError()
	}
	request := linkTopologyRequest(sequence)
	err := socket.send(request[:])
	if !c.check() {
		return nil, c.base.failureError()
	}
	if err != nil {
		return nil, err
	}
	var buffer [linkReceiveBytes]byte
	dump := linkTopologyDump{sequence: sequence, port: socket.port}
	for !dump.done {
		// These are shared totals for both dumps, not a fresh allowance for the
		// identity check. Refusal never returns a complete truncated prefix.
		if c.datagrams >= linkDatagramLimit || c.inputBytes+linkReceiveBytes > linkInputLimit {
			return nil, errTopologyLimit
		}
		if err := affinityRoom(&c.base, linkReceiveBytes); err != nil {
			return nil, err
		}
		wait := c.remaining()
		if wait == 0 {
			return nil, c.base.failureError()
		}
		n, sender, groups, truncated, err := socket.receive(buffer[:], wait)
		if n > 0 && n <= len(buffer) {
			c.inputBytes += uint64(n)
			c.base.bytes += uint64(n)
			c.datagrams++
		}
		if !c.check() {
			return nil, c.base.failureError()
		}
		if err != nil {
			return nil, err
		}
		if n < 1 || n > len(buffer) || truncated || sender != 0 || groups != 0 {
			return nil, errTopologyRecord
		}
		if err := dump.consume(buffer[:n]); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(dump.rows, func(a, b linkTopologyRow) int {
		if a.value.Index < b.value.Index {
			return -1
		}
		if a.value.Index > b.value.Index {
			return 1
		}
		return 0
	})
	return dump.rows, nil
}

type linkTopologyDump struct {
	sequence, port uint32
	done           bool
	rows           []linkTopologyRow
	indices        map[uint32]bool
	names          map[string]bool
}

func (d *linkTopologyDump) consume(data []byte) error {
	if d.done || len(data) == 0 || len(data) > linkReceiveBytes {
		return errTopologyRecord
	}
	for len(data) > 0 {
		if d.done || len(data) < 16 {
			return errTopologyRecord
		}
		n := int(binary.NativeEndian.Uint32(data[:4]))
		if n < 16 || n > len(data) || (n+3)&^3 > len(data) {
			return errTopologyRecord
		}
		typ, flags := binary.NativeEndian.Uint16(data[4:6]), binary.NativeEndian.Uint16(data[6:8])
		if binary.NativeEndian.Uint32(data[8:12]) != d.sequence || binary.NativeEndian.Uint32(data[12:16]) != d.port {
			return errTopologyRecord
		}
		if flags&linkDumpInterrupted != 0 {
			return errLinkBinding
		}
		payload := data[16:n]
		if typ == linkError {
			if len(payload) < 4 {
				return errTopologyRecord
			}
			switch int32(binary.NativeEndian.Uint32(payload[:4])) {
			case -1, -13:
				return os.ErrPermission
			case -95, -97, -93:
				return errLinkUnsupported
			default:
				return errTopologyRecord
			}
		}
		if flags != linkMulti {
			return errTopologyRecord
		}
		switch typ {
		case linkDone:
			if len(payload) != 0 && (len(payload) != 4 || binary.NativeEndian.Uint32(payload) != 0) {
				return errTopologyRecord
			}
			d.done = true
		case linkNew:
			if len(d.rows) >= linkTopologyLimit {
				return errTopologyLimit
			}
			row, err := parseLinkTopology(payload)
			if err != nil {
				return err
			}
			if d.indices == nil {
				d.indices, d.names = make(map[uint32]bool), make(map[string]bool)
			}
			if d.indices[row.value.Index] || d.names[row.identity.name] {
				return errTopologyRecord
			}
			d.indices[row.value.Index], d.names[row.identity.name] = true, true
			d.rows = append(d.rows, row)
		default:
			return errTopologyRecord
		}
		data = data[(n+3)&^3:]
	}
	return nil
}

func parseLinkTopology(data []byte) (linkTopologyRow, error) {
	var row linkTopologyRow
	if len(data) < 16 {
		return row, errTopologyRecord
	}
	index := int32(binary.NativeEndian.Uint32(data[4:8]))
	if index <= 0 {
		return row, errTopologyRecord
	}
	row.value = LinkTopology{Index: uint32(index), AdminUp: binary.NativeEndian.Uint32(data[8:12])&1 != 0, OperationalState: "not_reported", Kind: "not_reported", XDPMode: "unknown"}
	row.identity.index = uint32(index)
	count := 0
	if err := linkTopologyAttributes(data[16:], 1, 0, &count, &row); err != nil {
		return linkTopologyRow{}, err
	}
	if row.identity.name == "" {
		return linkTopologyRow{}, errTopologyRecord
	}
	return row, nil
}

// Context 0 is link, 1 LINKINFO, 2 XDP, and 3 discarded nested attributes.
// Legacy LINKINFO/XDP omit NLA_F_NESTED, so their known containers are still
// parsed. All decoded nesting shares the per-message attribute/depth bounds.
func linkTopologyAttributes(data []byte, depth, context int, count *int, row *linkTopologyRow) error {
	if depth > linkDepthLimit {
		return errTopologyLimit
	}
	var seen uint64
	for len(data) > 0 {
		if len(data) < 4 {
			return errTopologyRecord
		}
		n, raw := int(binary.NativeEndian.Uint16(data[:2])), binary.NativeEndian.Uint16(data[2:4])
		if n < 4 || n > len(data) || (n+3)&^3 > len(data) {
			return errTopologyRecord
		}
		(*count)++
		if *count > linkAttributeLimit {
			return errTopologyLimit
		}
		kind, flags, value := raw&0x3fff, raw&0xc000, data[4:n]
		child, known := -1, false
		if context == 0 {
			switch kind {
			case linkName, linkMTU, linkParent, linkMaster, linkOperState, linkCarrier:
				known = true
			case linkInfo:
				known, child = true, 1
			case linkXDP:
				known, child = true, 2
			}
		} else if context == 1 {
			if kind == 1 {
				known = true
			}
			if kind == 2 || kind == 5 {
				child = 3
			}
		} else if context == 2 && kind == 2 {
			known = true
		}
		if known {
			bit := uint64(1) << kind
			if seen&bit != 0 {
				return errTopologyRecord
			}
			seen |= bit
			if flags&linkAttrNetworkOrder != 0 || child < 0 && flags != 0 {
				return errTopologyRecord
			}
		}
		if child >= 0 || flags&linkAttrNested != 0 {
			if flags&linkAttrNetworkOrder != 0 {
				return errTopologyRecord
			}
			if child < 0 {
				child = 3
			}
			if err := linkTopologyAttributes(value, depth+1, child, count, row); err != nil {
				return err
			}
		} else if known {
			if err := applyLinkTopologyAttribute(context, kind, value, row); err != nil {
				return err
			}
		}
		data = data[(n+3)&^3:]
	}
	return nil
}

func applyLinkTopologyAttribute(context int, kind uint16, value []byte, row *linkTopologyRow) error {
	if context == 1 {
		if len(value) < 2 || len(value) > 64 || value[len(value)-1] != 0 || bytes.IndexByte(value[:len(value)-1], 0) >= 0 {
			return errTopologyRecord
		}
		row.value.Kind = linkTopologyKind(string(value[:len(value)-1]))
		return nil
	}
	if context == 2 {
		if len(value) != 1 {
			return errTopologyRecord
		}
		modes := [...]string{"none", "driver", "generic", "offload", "multi"}
		if int(value[0]) < len(modes) {
			row.value.XDPMode = modes[value[0]]
		}
		return nil
	}
	switch kind {
	case linkName:
		if len(value) < 2 || len(value) > 16 || value[len(value)-1] != 0 || bytes.IndexByte(value[:len(value)-1], 0) >= 0 {
			return errTopologyRecord
		}
		name := string(value[:len(value)-1])
		if !validInterfaceName(name) {
			return errTopologyRecord
		}
		row.identity.name = name
	case linkMTU, linkMaster, linkParent:
		if len(value) != 4 {
			return errTopologyRecord
		}
		n := binary.NativeEndian.Uint32(value)
		switch kind {
		case linkMTU:
			row.value.MTU, row.value.HasMTU = n, true
		case linkMaster:
			row.value.MasterIndex, row.value.HasMaster = n, true
		case linkParent:
			row.value.LinkIndex, row.value.HasLink = n, true
		}
	case linkCarrier:
		if len(value) != 1 || value[0] > 1 {
			return errTopologyRecord
		}
		row.value.Carrier, row.value.HasCarrier = value[0] == 1, true
	case linkOperState:
		if len(value) != 1 {
			return errTopologyRecord
		}
		states := [...]string{"unknown", "not_present", "down", "lower_layer_down", "testing", "dormant", "up"}
		row.value.OperationalState = "unknown"
		if int(value[0]) < len(states) {
			row.value.OperationalState = states[value[0]]
		}
	}
	return nil
}

func linkTopologyKind(kind string) string {
	switch kind {
	case "bond", "bridge", "vlan", "veth", "dummy", "gre", "gretap", "ip6gre", "ip6gretap", "ipip", "sit", "ip6tnl", "vti", "vti6", "vxlan", "geneve", "tun", "wireguard", "macvlan", "macvtap", "ipvlan", "ipvtap", "vrf":
		return kind
	default:
		return "other"
	}
}
