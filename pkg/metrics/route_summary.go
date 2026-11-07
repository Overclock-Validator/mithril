package metrics

import (
	"encoding/binary"
	"math"
	"os"
	"slices"
	"time"
)

const (
	routeDatagramLimit = 32
	routeInputLimit    = 2 << 20
	routeEntryLimit    = 2048
	routeNexthopLimit  = 32
	routeNew           = 24
	routeGet           = 26
	routeIPv4          = 2
	routeIPv6          = 10
	routeMain          = 254
	routeUnicast       = 1
	routeOutput        = 4
	routeMultipath     = 9
	routeTable         = 15
	routeNexthopID     = 30
)

// RouteCounts counts main-table unicast dump records, including any native
// exception records returned by the unfiltered request. It does not count unique
// destinations or forwarding decisions. Defaults have prefix length 0.
type RouteCounts struct{ MainUnicast, Default uint32 }

// RouteFamilySummary counts a route once globally. UnknownOutput counts routes
// with at least one unresolved output; such a route can also have known outputs.
// An opaque nexthop ID alone is unresolved, but explicit compatibility outputs
// remain usable without looking up that ID.
type RouteFamilySummary struct {
	RouteCounts
	UnknownOutput, UnknownDefaultOutput uint32
}

// RouteInterfaceSummary counts each route at most once per represented index.
// An index is bound only to this observation's fenced link generation.
type RouteInterfaceSummary struct {
	Index      uint32
	IPv4, IPv6 RouteCounts
}

// RouteSummaryObservation contains no names, addresses, gateways or raw routes.
// Failed captures clear every route summary count. Available means both dumps completed and
// their link/namespace fences matched, not an atomic routing-table snapshot.
type RouteSummaryObservation struct {
	Generation, TopologyGeneration, AffinityGeneration, LinkGeneration uint64
	MetadataStartedAt, NetworkStartedAt, StartedAt, SampledAt          time.Time
	State                                                              TopologyState
	IPv4, IPv6                                                         RouteFamilySummary
	Interfaces                                                         []RouteInterfaceSummary
	CumulativeReads                                                    uint32
	CumulativeBytes                                                    uint64
	RouteDatagrams, RouteEntries, LinkDatagrams                        uint32
	RouteBytes, LinkBytes                                              uint64
}

// RouteSummarySampler is serially owned by the existing diagnostics caller.
// It retains no previous values and starts no worker or timer.
type RouteSummarySampler struct{ generation uint64 }

// NewRouteSummarySampler performs no clock, file or socket work.
func NewRouteSummarySampler(enabled bool) *RouteSummarySampler {
	if !enabled {
		return nil
	}
	return &RouteSummarySampler{}
}

// Sample continues the supplied link capture's original network/shared budget.
// The detached inputs must not be mutated concurrently by the caller.
func (s *RouteSummarySampler) Sample(topology TopologyObservation, affinity ThreadAffinityObservation, links LinkTopologyObservation) (RouteSummaryObservation, bool) {
	return s.sample(topology, affinity, links, readRouteSummary)
}

func (s *RouteSummarySampler) sample(topology TopologyObservation, affinity ThreadAffinityObservation, links LinkTopologyObservation, read func(TopologyObservation, ThreadAffinityObservation, LinkTopologyObservation) RouteSummaryObservation) (RouteSummaryObservation, bool) {
	if s == nil {
		return RouteSummaryObservation{}, false
	}
	if s.generation >= math.MaxUint64-1 {
		return RouteSummaryObservation{Generation: s.generation, State: TopologyLimitExceeded}, true
	}
	o := read(topology, affinity, links)
	s.generation++
	o.Generation = s.generation
	return o, true
}

func routeSummaryBudget(topology TopologyObservation, affinity ThreadAffinityObservation, links LinkTopologyObservation, now func() time.Time) (linkTopologyCapture, TopologyState) {
	c, state := linkTopologyBudget(topology, affinity, now)
	if state != "" {
		return c, state
	}
	if links.Generation == 0 || links.Generation == math.MaxUint64 || links.TopologyGeneration != topology.Generation || links.AffinityGeneration != affinity.Generation || !links.MetadataStartedAt.Equal(topology.StartedAt) {
		return c, TopologyBindingChanged
	}
	if links.CumulativeReads < c.base.reads || links.CumulativeBytes < c.base.bytes || links.NetlinkBytes != links.CumulativeBytes-c.base.bytes {
		return c, TopologyReadFailed
	}
	// A failed prerequisite does not erase the work it already consumed.
	c.base.reads, c.base.bytes = links.CumulativeReads, links.CumulativeBytes
	c.datagrams, c.inputBytes = links.Datagrams, links.NetlinkBytes
	if links.State != TopologyAvailable {
		if affinitySourceState(links.State) && links.State != TopologyPartial {
			return c, links.State
		}
		return c, TopologyReadFailed
	}
	if links.namespace[1] == 0 || len(links.Links) > linkTopologyLimit || links.Links == nil || len(links.Links) != len(links.identities) {
		return c, TopologyBindingChanged
	}
	if !validAffinityClock(links.StartedAt) || !validAffinityClock(links.SampledAt) || links.StartedAt.Before(c.base.last) || links.SampledAt.Before(links.StartedAt) || links.SampledAt.Sub(links.StartedAt) > linkCaptureLimit || links.SampledAt.Sub(c.base.start) > topologyCaptureLimit {
		return c, TopologyCaptureExpired
	}
	for i, row := range links.Links {
		identity := links.identities[i]
		if row.Index == 0 || row.Index > math.MaxInt32 || identity.index != row.Index || !validInterfaceName(identity.name) || i != 0 && links.Links[i-1].Index >= row.Index {
			return c, TopologyBindingChanged
		}
	}
	c.started, c.base.last = links.StartedAt, links.SampledAt
	if c.base.reads >= topologyOpenLimit || c.base.bytes >= topologyByteLimit || c.datagrams >= linkDatagramLimit || c.inputBytes >= linkInputLimit {
		return c, TopologyLimitExceeded
	}
	return c, ""
}

func readRouteSummaryCapture(topology TopologyObservation, affinity ThreadAffinityObservation, links LinkTopologyObservation, namespace func() ([2]uint64, error), open func(*linkTopologyCapture) (linkTopologySocket, error), now func() time.Time) (o RouteSummaryObservation) {
	c, state := routeSummaryBudget(topology, affinity, links, now)
	o = RouteSummaryObservation{TopologyGeneration: topology.Generation, AffinityGeneration: affinity.Generation, LinkGeneration: links.Generation, MetadataStartedAt: topology.StartedAt, NetworkStartedAt: links.StartedAt, CumulativeReads: c.base.reads, CumulativeBytes: c.base.bytes, LinkDatagrams: c.datagrams, LinkBytes: c.inputBytes, State: state}
	if state != "" {
		return o
	}
	if !c.check() {
		o.State = c.base.failure
		return o
	}
	o.StartedAt = c.base.last
	defer func() {
		c.check()
		o.SampledAt, o.CumulativeReads, o.CumulativeBytes = c.base.last, c.base.reads, c.base.bytes
		o.LinkDatagrams, o.LinkBytes = c.datagrams, c.inputBytes
		if c.base.failure != "" {
			o.State = c.base.failure
		}
		if o.State != TopologyAvailable {
			o.Interfaces = nil
			o.IPv4 = RouteFamilySummary{}
			o.IPv6 = RouteFamilySummary{}
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
	if before != links.namespace {
		o.State = TopologyBindingChanged
		return o
	}
	socket, err := open(&c)
	if err != nil {
		o.State = linkTopologyError(err)
		return o
	}
	defer socket.close()
	o.Interfaces = make([]RouteInterfaceSummary, len(links.Links))
	for i, row := range links.Links {
		o.Interfaces[i].Index = row.Index
	}
	for _, family := range []byte{routeIPv4, routeIPv6} {
		if err := readRouteSummaryDump(&c, socket, family, &o); err != nil {
			o.State = linkTopologyError(err)
			return o
		}
	}
	// Reuse the original link allowance, including BOTH earlier link dumps.
	final, err := readLinkTopologyDump(&c, socket, 2)
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
	if after != before || len(final) != len(links.identities) {
		o.State = TopologyBindingChanged
		return o
	}
	for i := range final {
		if final[i].identity != links.identities[i] {
			o.State = TopologyBindingChanged
			return o
		}
	}
	o.State = TopologyAvailable
	return o
}

func routeSummaryRequests() (v4, v6 [28]byte) {
	binary.NativeEndian.PutUint32(v4[:4], uint32(len(v4)))
	binary.NativeEndian.PutUint16(v4[4:6], routeGet)
	binary.NativeEndian.PutUint16(v4[6:8], linkRequestDump)
	binary.NativeEndian.PutUint32(v4[8:12], 3)
	v4[16] = routeIPv4
	v6 = v4
	binary.NativeEndian.PutUint32(v6[8:12], 4)
	v6[16] = routeIPv6
	// No filter, destination, attributes, subscription or configurable operation.
	return v4, v6
}

func linkTopologyReadRequestAllowed(request []byte) bool {
	one, two := linkTopologyRequest(1), linkTopologyRequest(2)
	v4, v6 := routeSummaryRequests()
	return string(request) == string(one[:]) || string(request) == string(two[:]) || string(request) == string(v4[:]) || string(request) == string(v6[:])
}

func readRouteSummaryDump(c *linkTopologyCapture, socket linkTopologySocket, family byte, o *RouteSummaryObservation) error {
	v4, v6 := routeSummaryRequests()
	request := v4
	if family == routeIPv6 {
		request = v6
	}
	if !c.check() {
		return c.base.failureError()
	}
	err := socket.send(request[:])
	if !c.check() {
		return c.base.failureError()
	}
	if err != nil {
		return err
	}
	dump := routeSummaryDump{family: family, sequence: binary.NativeEndian.Uint32(request[8:12]), port: socket.port, observation: o}
	var buffer [linkReceiveBytes]byte
	for !dump.done {
		if o.RouteDatagrams >= routeDatagramLimit || o.RouteBytes+linkReceiveBytes > routeInputLimit {
			return errTopologyLimit
		}
		if err := affinityRoom(&c.base, linkReceiveBytes); err != nil {
			return err
		}
		wait := c.remaining()
		if wait == 0 {
			return c.base.failureError()
		}
		n, sender, groups, truncated, err := socket.receive(buffer[:], wait)
		if n > 0 && n <= len(buffer) {
			o.RouteBytes += uint64(n)
			o.RouteDatagrams++
			c.base.bytes += uint64(n)
		}
		if !c.check() {
			return c.base.failureError()
		}
		if err != nil {
			return err
		}
		if n < 1 || n > len(buffer) || truncated || sender != 0 || groups != 0 {
			return errTopologyRecord
		}
		if err := dump.consume(buffer[:n]); err != nil {
			return err
		}
	}
	return nil
}

type routeSummaryDump struct {
	family         byte
	sequence, port uint32
	done           bool
	observation    *RouteSummaryObservation
}

func (d *routeSummaryDump) consume(data []byte) error {
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
		case routeNew:
			if d.observation.RouteEntries >= routeEntryLimit {
				return errTopologyLimit
			}
			d.observation.RouteEntries++
			row, err := parseRouteSummary(payload, d.family)
			if err != nil {
				return err
			}
			if row.main {
				d.add(row)
			}
		default:
			return errTopologyRecord
		}
		data = data[(n+3)&^3:]
	}
	return nil
}

type routeSummaryRow struct {
	main, isDefault, unknown bool
	outputs                  [routeNexthopLimit + 1]uint32
	count                    int
}

func (r *routeSummaryRow) output(index uint32) {
	if index == 0 || index > math.MaxInt32 {
		r.unknown = true
		return
	}
	if !slices.Contains(r.outputs[:r.count], index) {
		r.outputs[r.count] = index
		r.count++
	}
}

func (d *routeSummaryDump) add(row routeSummaryRow) {
	o := d.observation
	total := &o.IPv4
	if d.family == routeIPv6 {
		total = &o.IPv6
	}
	total.MainUnicast++
	if row.isDefault {
		total.Default++
	}
	unknown := row.unknown || row.count == 0
	for _, index := range row.outputs[:row.count] {
		i, found := slices.BinarySearchFunc(o.Interfaces, index, func(row RouteInterfaceSummary, index uint32) int {
			if row.Index < index {
				return -1
			}
			if row.Index > index {
				return 1
			}
			return 0
		})
		if !found {
			unknown = true
			continue
		}
		count := &o.Interfaces[i].IPv4
		if d.family == routeIPv6 {
			count = &o.Interfaces[i].IPv6
		}
		count.MainUnicast++
		if row.isDefault {
			count.Default++
		}
	}
	if unknown {
		total.UnknownOutput++
		if row.isDefault {
			total.UnknownDefaultOutput++
		}
	}
}

func parseRouteSummary(data []byte, family byte) (routeSummaryRow, error) {
	var row routeSummaryRow
	if len(data) < 12 || family != routeIPv4 && family != routeIPv6 || data[0] != family {
		return row, errTopologyRecord
	}
	bits := byte(32)
	if family == routeIPv6 {
		bits = 128
	}
	if data[1] > bits || data[2] > bits {
		return row, errTopologyRecord
	}
	table := uint32(data[4])
	row.isDefault = data[1] == 0
	attributeCount := 0
	attrs, err := routeSummaryAttributes(data[12:], 1, &attributeCount)
	if err != nil {
		return row, err
	}
	var seen uint32
	for _, attr := range attrs {
		kind := attr.kind & 0x3fff
		if kind != routeTable && kind != routeOutput && kind != routeMultipath && kind != routeNexthopID {
			continue
		}
		if seen&(1<<kind) != 0 || attr.kind&linkAttrNetworkOrder != 0 || kind != routeMultipath && attr.kind&linkAttrNested != 0 {
			return row, errTopologyRecord
		}
		seen |= 1 << kind
		if kind == routeMultipath {
			if err := row.multipath(attr.value, &attributeCount); err != nil {
				return row, err
			}
			continue
		}
		if len(attr.value) != 4 {
			return row, errTopologyRecord
		}
		value := binary.NativeEndian.Uint32(attr.value)
		switch kind {
		case routeTable:
			table = value
		case routeOutput:
			row.output(value)
		}
	}
	// RTA_NH_ID can coexist with explicit OIF/multipath compatibility data.
	// Keep those indices; add() handles absent/invalid/unrepresented outputs.
	// No nexthop-object lookup or guessed join is performed.
	row.main = table == routeMain && data[7] == routeUnicast
	return row, nil
}

type routeSummaryAttribute struct {
	kind  uint16
	value []byte
}

// Attribute views live only during parsing; no raw input is retained. Nested
// unknown containers are structurally checked within the same count/depth cap.
func routeSummaryAttributes(data []byte, depth int, count *int) ([]routeSummaryAttribute, error) {
	if depth > linkDepthLimit {
		return nil, errTopologyLimit
	}
	var result []routeSummaryAttribute
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, errTopologyRecord
		}
		n, kind := int(binary.NativeEndian.Uint16(data[:2])), binary.NativeEndian.Uint16(data[2:4])
		if n < 4 || n > len(data) || (n+3)&^3 > len(data) {
			return nil, errTopologyRecord
		}
		*count++
		if *count > linkAttributeLimit {
			return nil, errTopologyLimit
		}
		value := data[4:n]
		if (kind&linkAttrNested != 0 || depth == 1 && kind&0x3fff == 8) && !(depth == 1 && kind&0x3fff == routeMultipath) {
			if _, err := routeSummaryAttributes(value, depth+1, count); err != nil {
				return nil, err
			}
		}
		result = append(result, routeSummaryAttribute{kind: kind, value: value})
		data = data[(n+3)&^3:]
	}
	return result, nil
}

func (r *routeSummaryRow) multipath(data []byte, attrs *int) error {
	if len(data) == 0 {
		return errTopologyRecord
	}
	count := 0
	for len(data) > 0 {
		if len(data) < 8 {
			return errTopologyRecord
		}
		n := int(binary.NativeEndian.Uint16(data[:2]))
		if n < 8 || n > len(data) || (n+3)&^3 > len(data) {
			return errTopologyRecord
		}
		count++
		if count > routeNexthopLimit {
			return errTopologyLimit
		}
		if _, err := routeSummaryAttributes(data[8:n], 2, attrs); err != nil {
			return err
		}
		r.output(binary.NativeEndian.Uint32(data[4:8]))
		data = data[(n+3)&^3:]
	}
	return nil
}
