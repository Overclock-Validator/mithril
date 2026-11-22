package metrics

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func routeTestPayload(family, prefix, table, typ byte, attrs ...[]byte) []byte {
	b := make([]byte, 12)
	b[0], b[1], b[4], b[7] = family, prefix, table, typ
	for _, a := range attrs {
		b = append(b, a...)
	}
	return b
}

func routeTestHop(index uint32, attrs ...[]byte) []byte {
	b := make([]byte, 8)
	binary.NativeEndian.PutUint32(b[4:8], index)
	for _, a := range attrs {
		b = append(b, a...)
	}
	binary.NativeEndian.PutUint16(b[:2], uint16(len(b)))
	return b
}

type routeSummaryFixture struct {
	base  *linkTopologyFixture
	links LinkTopologyObservation
}

func newRouteSummaryFixture() *routeSummaryFixture {
	f := newLinkTopologyFixture()
	links := f.capture()
	links.Generation = 11
	f.opens, f.sends, f.receives, f.closes, f.namespaces = 0, 0, 0, 0, 0
	f.at = f.at.Add(time.Millisecond)
	f.packets = [][]byte{
		linkTestMessage(routeNew, linkMulti, 3, 42, routeTestPayload(routeIPv4, 0, routeMain, routeUnicast, linkTestAttribute(routeOutput, linkTestUint(7)))), linkTestDone(3),
		linkTestMessage(routeNew, linkMulti, 4, 42, routeTestPayload(routeIPv6, 64, routeMain, routeUnicast, linkTestAttribute(routeOutput, linkTestUint(7)))), linkTestDone(4),
		linkTestMessage(linkNew, linkMulti, 2, 42, linkTestPayload(7, "PRIVATE_LINK")), linkTestDone(2),
	}
	return &routeSummaryFixture{base: f, links: links}
}

func (f *routeSummaryFixture) capture() RouteSummaryObservation {
	return readRouteSummaryCapture(f.base.topology, f.base.affinity, f.links, f.base.namespace, func(c *linkTopologyCapture) (linkTopologySocket, error) {
		s, err := f.base.open(c)
		if err != nil {
			return s, err
		}
		s.send = func(b []byte) error {
			v4, v6 := routeSummaryRequests()
			link := linkTopologyRequest(2)
			requests := [][]byte{v4[:], v6[:], link[:]}
			if f.base.sends >= len(requests) || !bytes.Equal(b, requests[f.base.sends]) {
				return errTopologyRecord
			}
			f.base.sends++
			return nil
		}
		return s, nil
	}, func() time.Time { return f.base.at })
}

func TestRouteSummaryRedaction(t *testing.T) {
	f := newRouteSummaryFixture()
	hops := append(routeTestHop(7), routeTestHop(7)...)
	hops = append(hops, routeTestHop(99, linkTestAttribute(5, []byte{192, 0, 2, 1}))...)
	f.base.packets[0] = bytes.Join([][]byte{
		linkTestMessage(routeNew, linkMulti, 3, 42, routeTestPayload(routeIPv4, 0, 0, routeUnicast, linkTestAttribute(routeTable, linkTestUint(routeMain)), linkTestAttribute(routeMultipath, hops), linkTestAttribute(1, []byte{0, 0, 0, 0}))),
		linkTestMessage(routeNew, linkMulti, 3, 42, routeTestPayload(routeIPv4, 24, routeMain, routeUnicast)),
		linkTestMessage(routeNew, linkMulti, 3, 42, routeTestPayload(routeIPv4, 24, routeMain, routeUnicast, linkTestAttribute(routeNexthopID, linkTestUint(123)), linkTestAttribute(routeOutput, linkTestUint(7)))),
		linkTestMessage(routeNew, linkMulti, 3, 42, routeTestPayload(routeIPv4, 0, routeMain, 6, linkTestAttribute(routeOutput, linkTestUint(7)))),
		linkTestMessage(routeNew, linkMulti, 3, 42, routeTestPayload(routeIPv4, 0, routeMain, routeUnicast, linkTestAttribute(routeTable, linkTestUint(123)), linkTestAttribute(routeOutput, linkTestUint(7)))),
	}, nil)
	o := f.capture()
	if o.State != TopologyAvailable || o.IPv4 != (RouteFamilySummary{RouteCounts: RouteCounts{3, 1}, UnknownOutput: 2, UnknownDefaultOutput: 1}) || o.IPv6 != (RouteFamilySummary{RouteCounts: RouteCounts{1, 0}}) || len(o.Interfaces) != 1 || o.Interfaces[0] != (RouteInterfaceSummary{Index: 7, IPv4: RouteCounts{2, 1}, IPv6: RouteCounts{1, 0}}) {
		t.Fatalf("scoped counts differ: %+v", o)
	}
	if o.RouteEntries != 6 || o.RouteDatagrams != 4 || o.LinkDatagrams != f.links.Datagrams+2 || o.CumulativeReads != f.links.CumulativeReads+1 || o.CumulativeBytes != f.links.CumulativeBytes+o.RouteBytes+o.LinkBytes-f.links.NetlinkBytes || f.base.closes != 1 || f.base.sends != 3 || f.base.namespaces != 2 {
		t.Fatalf("shared accounting/ownership differs: %+v", o)
	}
	if o.MetadataStartedAt != f.base.topology.StartedAt || o.NetworkStartedAt != f.links.StartedAt || o.StartedAt != f.base.at || o.SampledAt != f.base.at || o.LinkGeneration != 11 || o.TopologyGeneration != 7 || o.AffinityGeneration != 9 || !validAffinityClock(o.SampledAt) {
		t.Fatal("original clocks/binding lost")
	}
	b, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{[]byte("PRIVATE_LINK"), []byte("192.0.2.1"), []byte("gateway"), []byte("namespace"), []byte("identities")} {
		if bytes.Contains(b, forbidden) {
			t.Fatal("private route data escaped")
		}
	}
	for _, b := range f.base.packets {
		clear(b)
	}
	if o.Interfaces[0].IPv4.MainUnicast != 2 {
		t.Fatal("output aliases input")
	}
	f = newRouteSummaryFixture()
	f.base.packets = [][]byte{linkTestDone(3), linkTestDone(4), f.base.packets[4], linkTestDone(2)}
	o = f.capture()
	if o.State != TopologyAvailable || o.Interfaces == nil || o.IPv4.MainUnicast != 0 || o.IPv6.MainUnicast != 0 {
		t.Fatal("completed empty became unknown")
	}
}

func TestRouteSummaryNexthopCompatibility(t *testing.T) {
	for _, family := range []byte{routeIPv4, routeIPv6} {
		for _, tc := range []struct {
			name           string
			attrs          [][]byte
			known, unknown uint32
		}{
			{"id-only", nil, 0, 1},
			{"compat-output", [][]byte{linkTestAttribute(routeOutput, linkTestUint(7))}, 1, 0},
			{"compat-multipath", [][]byte{linkTestAttribute(routeMultipath, append(routeTestHop(7), routeTestHop(7)...))}, 1, 0},
			{"compat-both-deduplicated", [][]byte{linkTestAttribute(routeOutput, linkTestUint(7)), linkTestAttribute(routeMultipath, append(routeTestHop(7), routeTestHop(7)...))}, 1, 0},
			{"compat-unrepresented", [][]byte{linkTestAttribute(routeMultipath, append(routeTestHop(7), routeTestHop(99)...))}, 1, 1},
			{"compat-invalid-index", [][]byte{linkTestAttribute(routeMultipath, append(routeTestHop(7), routeTestHop(0)...))}, 1, 1},
		} {
			t.Run(map[byte]string{routeIPv4: "ipv4", routeIPv6: "ipv6"}[family]+"/"+tc.name, func(t *testing.T) {
				attrs := append([][]byte{linkTestAttribute(routeNexthopID, linkTestUint(123))}, tc.attrs...)
				// A private gateway is parsed only as opaque input; it is never joined or retained.
				attrs = append(attrs, linkTestAttribute(5, bytes.Repeat([]byte{0xab}, map[byte]int{routeIPv4: 4, routeIPv6: 16}[family])))
				o := RouteSummaryObservation{Interfaces: []RouteInterfaceSummary{{Index: 7}}}
				d := routeSummaryDump{family: family, sequence: 3, port: 42, observation: &o}
				if err := d.consume(append(linkTestMessage(routeNew, linkMulti, 3, 42, routeTestPayload(family, 0, routeMain, routeUnicast, attrs...)), linkTestDone(3)...)); err != nil {
					t.Fatal(err)
				}
				got, output := o.IPv4, o.Interfaces[0].IPv4
				if family == routeIPv6 {
					got, output = o.IPv6, o.Interfaces[0].IPv6
				}
				if got != (RouteFamilySummary{RouteCounts: RouteCounts{1, 1}, UnknownOutput: tc.unknown, UnknownDefaultOutput: tc.unknown}) || output != (RouteCounts{tc.known, tc.known}) {
					t.Fatalf("explicit compat outputs lost or double-counted: %+v %+v", got, output)
				}
			})
		}
	}
}

func TestRouteSummaryRequestAllowlist(t *testing.T) {
	v4, v6 := routeSummaryRequests()
	l1, l2 := linkTopologyRequest(1), linkTopologyRequest(2)
	for _, b := range [][]byte{l1[:], l2[:], v4[:], v6[:]} {
		if !linkTopologyReadRequestAllowed(b) {
			t.Fatal("fixed read refused")
		}
		for i := range b {
			mutated := bytes.Clone(b)
			mutated[i] ^= 1
			if linkTopologyReadRequestAllowed(mutated) {
				t.Fatal("mutated request admitted")
			}
		}
		if linkTopologyReadRequestAllowed(append(bytes.Clone(b), 0)) || linkTopologyReadRequestAllowed(b[:len(b)-1]) {
			t.Fatal("nonexact request admitted")
		}
	}
	for i, b := range [][28]byte{v4, v6} {
		if binary.NativeEndian.Uint32(b[:4]) != 28 || binary.NativeEndian.Uint16(b[4:6]) != 26 || binary.NativeEndian.Uint16(b[6:8]) != 0x301 || binary.NativeEndian.Uint32(b[8:12]) != uint32(3+i) || binary.NativeEndian.Uint32(b[12:16]) != 0 || b[16] != []byte{2, 10}[i] || !bytes.Equal(b[17:], make([]byte, 11)) {
			t.Fatal("route dump request differs from fixed UAPI layout")
		}
	}
}

func TestRouteSummaryBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*routeSummaryFixture)
		state  TopologyState
	}{
		{"sender", func(f *routeSummaryFixture) { f.base.sender = 1 }, TopologyReadFailed},
		{"multicast", func(f *routeSummaryFixture) { f.base.groups = 1 }, TopologyReadFailed},
		{"truncated", func(f *routeSummaryFixture) { f.base.truncated = true }, TopologyReadFailed},
		{"receive", func(f *routeSummaryFixture) { f.base.receiveError = errors.New("read") }, TopologyReadFailed},
		{"denied", func(f *routeSummaryFixture) { f.base.openError = os.ErrPermission }, TopologyDenied},
		{"unsupported", func(f *routeSummaryFixture) { f.base.openError = errLinkUnsupported }, TopologyUnsupported},
		{"missing-done", func(f *routeSummaryFixture) { f.base.packets = f.base.packets[:1] }, TopologyCaptureExpired},
		{"sequence", func(f *routeSummaryFixture) { binary.NativeEndian.PutUint32(f.base.packets[0][8:12], 4) }, TopologyReadFailed},
		{"port", func(f *routeSummaryFixture) { binary.NativeEndian.PutUint32(f.base.packets[0][12:16], 0) }, TopologyReadFailed},
		{"interrupted", func(f *routeSummaryFixture) {
			binary.NativeEndian.PutUint16(f.base.packets[0][6:8], linkMulti|linkDumpInterrupted)
		}, TopologyBindingChanged},
		{"done-error", func(f *routeSummaryFixture) { f.base.packets[1][16] = 1 }, TopologyReadFailed},
		{"after-done", func(f *routeSummaryFixture) { f.base.packets[0] = append(linkTestDone(3), f.base.packets[0]...) }, TopologyReadFailed},
		{"namespace", func(f *routeSummaryFixture) { f.base.after[1]++ }, TopologyBindingChanged},
		{"prior-namespace", func(f *routeSummaryFixture) { f.links.namespace[1]++ }, TopologyBindingChanged},
		{"name-replaced", func(f *routeSummaryFixture) {
			f.base.packets[4] = linkTestMessage(linkNew, linkMulti, 2, 42, linkTestPayload(7, "new"))
		}, TopologyBindingChanged},
		{"link-missing", func(f *routeSummaryFixture) {
			f.base.packets = f.base.packets[:4]
			f.base.packets = append(f.base.packets, linkTestDone(2))
		}, TopologyBindingChanged},
		{"prior-generation", func(f *routeSummaryFixture) { f.links.AffinityGeneration++ }, TopologyBindingChanged},
		{"prior-unsupported", func(f *routeSummaryFixture) {
			f.links.State = TopologyUnsupported
			f.links.Links = nil
			f.links.identities = nil
			f.links.namespace = [2]uint64{}
		}, TopologyUnsupported},
		{"prior-denied", func(f *routeSummaryFixture) { f.links.State = TopologyDenied }, TopologyDenied},
		{"foreign-index", func(f *routeSummaryFixture) { f.links.Links[0].Index++ }, TopologyBindingChanged},
		{"clock-stripped", func(f *routeSummaryFixture) { f.base.at = f.base.at.Round(0) }, TopologyCaptureExpired},
		{"clock-reversal", func(f *routeSummaryFixture) { f.base.at = f.links.SampledAt.Add(-time.Nanosecond) }, TopologyCaptureExpired},
		{"network-deadline", func(f *routeSummaryFixture) { f.base.at = f.links.StartedAt.Add(linkCaptureLimit + 1) }, TopologyCaptureExpired},
		{"shared-deadline", func(f *routeSummaryFixture) { f.base.at = f.base.topology.StartedAt.Add(topologyCaptureLimit + 1) }, TopologyCaptureExpired},
		{"shared-bytes", func(f *routeSummaryFixture) {
			delta := uint64(topologyByteLimit-1) - f.links.CumulativeBytes
			f.base.affinity.CumulativeBytes += delta
			f.links.CumulativeBytes += delta
		}, TopologyLimitExceeded},
		{"shared-opens", func(f *routeSummaryFixture) { f.links.CumulativeReads = topologyOpenLimit }, TopologyLimitExceeded},
		{"link-limit", func(f *routeSummaryFixture) { f.links.Datagrams = linkDatagramLimit - 1 }, TopologyLimitExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRouteSummaryFixture()
			tc.mutate(f)
			o := f.capture()
			if o.State != tc.state || o.Interfaces != nil || o.IPv4 != (RouteFamilySummary{}) || o.IPv6 != (RouteFamilySummary{}) {
				t.Fatalf("failure retained counts or wrong state: %+v", o)
			}
			if f.base.opens > 0 && f.base.openError == nil && f.base.closes != 1 {
				t.Fatal("owned socket leaked")
			}
		})
	}
}

func TestRouteSummaryParserLimits(t *testing.T) {
	base := func(attrs ...[]byte) []byte { return routeTestPayload(routeIPv4, 0, routeMain, routeUnicast, attrs...) }
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"short", make([]byte, 11)},
		{"family", routeTestPayload(routeIPv6, 0, routeMain, routeUnicast)},
		{"prefix", routeTestPayload(routeIPv4, 33, routeMain, routeUnicast)},
		{"short-attribute", append(base(), 1)},
		{"bad-output", base(linkTestAttribute(routeOutput, []byte{1}))},
		{"duplicate-output", base(linkTestAttribute(routeOutput, linkTestUint(7)), linkTestAttribute(routeOutput, linkTestUint(7)))},
		{"scalar-network-order", base(linkTestAttribute(routeTable|linkAttrNetworkOrder, linkTestUint(routeMain)))},
		{"scalar-nested", base(linkTestAttribute(routeOutput|linkAttrNested, linkTestUint(7)))},
		{"empty-multipath", base(linkTestAttribute(routeMultipath, nil))},
		{"short-hop", base(linkTestAttribute(routeMultipath, make([]byte, 7)))},
		{"deep", base(linkTestAttribute(100|linkAttrNested, linkTestAttribute(100|linkAttrNested, linkTestAttribute(100|linkAttrNested, linkTestAttribute(100, nil)))))},
		{"metrics-malformed", base(linkTestAttribute(8, []byte{1}))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseRouteSummary(tc.data, routeIPv4); err == nil {
				t.Fatal("malformed route accepted")
			}
		})
	}
	for _, index := range []uint32{0, math.MaxUint32} {
		r, err := parseRouteSummary(base(linkTestAttribute(routeOutput, linkTestUint(index))), routeIPv4)
		if err != nil || !r.unknown || r.count != 0 {
			t.Fatal("invalid output became an index or silently disappeared")
		}
	}
	hops := bytes.Repeat(routeTestHop(7), 32)
	r, err := parseRouteSummary(base(linkTestAttribute(routeMultipath, hops)), routeIPv4)
	if err != nil || r.count != 1 {
		t.Fatal("32 duplicate outputs not deduplicated")
	}
	if _, err := parseRouteSummary(base(linkTestAttribute(routeMultipath, append(hops, routeTestHop(7)...))), routeIPv4); !errors.Is(err, errTopologyLimit) {
		t.Fatal("33 hops accepted")
	}
	attrs := bytes.Repeat(linkTestAttribute(100, nil), 127)
	attrs = append(attrs, linkTestAttribute(routeMultipath, routeTestHop(7, linkTestAttribute(5, []byte{1, 2, 3, 4})))...)
	if _, err := parseRouteSummary(append(base(), attrs...), routeIPv4); !errors.Is(err, errTopologyLimit) {
		t.Fatal("multipath reset shared attribute ceiling")
	}
	for _, n := range []int{128, 129} {
		_, err := parseRouteSummary(append(base(), bytes.Repeat(linkTestAttribute(100, nil), n)...), routeIPv4)
		if (err == nil) != (n == 128) {
			t.Fatal("attribute boundary differs")
		}
	}
}

func TestRouteSummaryCombinedDumpLimits(t *testing.T) {
	f := newRouteSummaryFixture()
	message := linkTestMessage(routeNew, linkMulti, 3, 42, routeTestPayload(routeIPv4, 0, routeMain, routeUnicast, linkTestAttribute(routeOutput, linkTestUint(7))))
	// Every family shares the 2,048 entry ceiling, including ignored tables.
	f.base.packets = [][]byte{bytes.Repeat(message, 1024), bytes.Repeat(message, 1024), linkTestDone(3), linkTestDone(4), f.base.packets[4], linkTestDone(2)}
	o := f.capture()
	if o.State != TopologyAvailable || o.RouteEntries != 2048 || o.IPv4.MainUnicast != 2048 {
		t.Fatalf("exact entry bound failed: %+v", o)
	}
	f = newRouteSummaryFixture()
	f.base.packets = [][]byte{bytes.Repeat(message, 1024), bytes.Repeat(message, 1024), message}
	if o = f.capture(); o.State != TopologyLimitExceeded || o.Interfaces != nil {
		t.Fatal("entry cap+1 accepted")
	}
	f = newRouteSummaryFixture()
	last := f.base.packets[4]
	f.base.packets = nil
	for range 30 {
		f.base.packets = append(f.base.packets, message)
	}
	f.base.packets = append(f.base.packets, linkTestDone(3), linkTestDone(4), last, linkTestDone(2))
	if o = f.capture(); o.State != TopologyAvailable || o.RouteDatagrams != 32 {
		t.Fatal("exact shared datagram bound refused")
	}
	f = newRouteSummaryFixture()
	f.base.packets = nil
	for range 32 {
		f.base.packets = append(f.base.packets, message)
	}
	f.base.packets = append(f.base.packets, linkTestDone(3))
	if o = f.capture(); o.State != TopologyLimitExceeded || f.base.receives != 32 {
		t.Fatal("datagram cap exceeded or missing DONE accepted")
	}
}

func TestRouteSummarySamplerLifetime(t *testing.T) {
	var s *RouteSummarySampler
	called := 0
	read := func(TopologyObservation, ThreadAffinityObservation, LinkTopologyObservation) RouteSummaryObservation {
		called++
		return RouteSummaryObservation{State: TopologyAvailable}
	}
	if _, ok := s.sample(TopologyObservation{}, ThreadAffinityObservation{}, LinkTopologyObservation{}, read); ok || called != 0 || NewRouteSummarySampler(false) != nil {
		t.Fatal("disabled sampler did work")
	}
	if n := testing.AllocsPerRun(100, func() { s.Sample(TopologyObservation{}, ThreadAffinityObservation{}, LinkTopologyObservation{}) }); n != 0 {
		t.Fatal("disabled path allocated")
	}
	s = NewRouteSummarySampler(true)
	for want := uint64(1); want <= 2; want++ {
		o, ok := s.sample(TopologyObservation{}, ThreadAffinityObservation{}, LinkTopologyObservation{}, read)
		if !ok || o.Generation != want {
			t.Fatal("generation differs")
		}
	}
	s.generation = math.MaxUint64 - 1
	o, _ := s.sample(TopologyObservation{}, ThreadAffinityObservation{}, LinkTopologyObservation{}, read)
	if called != 2 || o.Generation != math.MaxUint64-1 || o.State != TopologyLimitExceeded {
		t.Fatal("exhaustion performed work or wrapped")
	}
	if runtime.GOOS != "linux" {
		f := newRouteSummaryFixture()
		at := time.Now()
		f.base.topology.StartedAt, f.base.topology.SampledAt = at.Add(-6*time.Millisecond), at.Add(-5*time.Millisecond)
		f.base.affinity.StartedAt, f.base.affinity.SampledAt = at.Add(-4*time.Millisecond), at.Add(-3*time.Millisecond)
		f.links.MetadataStartedAt = f.base.topology.StartedAt
		f.links.StartedAt, f.links.SampledAt = at.Add(-2*time.Millisecond), at.Add(-time.Millisecond)
		o, ok := NewRouteSummarySampler(true).Sample(f.base.topology, f.base.affinity, f.links)
		if !ok || o.State != TopologyUnsupported || !reflect.DeepEqual(o.Interfaces, []RouteInterfaceSummary(nil)) {
			t.Fatalf("unsupported differs: %+v", o)
		}
	}
}

func TestRouteSummaryControlMessages(t *testing.T) {
	for _, tc := range []struct {
		name   string
		packet []byte
		state  TopologyState
	}{
		{"permission", linkTestMessage(linkError, 0, 3, 42, linkTestUint(math.MaxUint32)), TopologyDenied},
		{"unsupported", linkTestMessage(linkError, 0, 3, 42, linkTestUint(math.MaxUint32-94)), TopologyUnsupported},
		{"ack-not-done", linkTestMessage(linkError, 0, 3, 42, linkTestUint(0)), TopologyReadFailed},
		{"unknown-message", linkTestMessage(99, linkMulti, 3, 42, nil), TopologyReadFailed},
		{"short-error", linkTestMessage(linkError, 0, 3, 42, nil), TopologyReadFailed},
		{"nonmulti", linkTestMessage(routeNew, 0, 3, 42, routeTestPayload(routeIPv4, 0, routeMain, routeUnicast)), TopologyReadFailed},
		{"unexpected-filtered", linkTestMessage(routeNew, linkMulti|0x20, 3, 42, routeTestPayload(routeIPv4, 0, routeMain, routeUnicast)), TopologyReadFailed},
		{"short-header", []byte{1, 2, 3}, TopologyReadFailed},
		{"short-length", append(linkTestUint(15), make([]byte, 12)...), TopologyReadFailed},
		{"outside-length", append(linkTestUint(20), make([]byte, 12)...), TopologyReadFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRouteSummaryFixture()
			f.base.packets[0] = tc.packet
			if o := f.capture(); o.State != tc.state || o.Interfaces != nil {
				t.Fatalf("bad control state: %+v", o)
			}
		})
	}
	f := newRouteSummaryFixture()
	c, state := routeSummaryBudget(f.base.topology, f.base.affinity, f.links, func() time.Time { return f.base.at })
	if state != "" {
		t.Fatal(state)
	}
	socket, err := f.base.open(&c)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.close()
	socket.send = func([]byte) error { return nil }
	o := RouteSummaryObservation{RouteBytes: routeInputLimit - linkReceiveBytes + 1}
	if err = readRouteSummaryDump(&c, socket, routeIPv4, &o); !errors.Is(err, errTopologyLimit) || f.base.receives != 0 {
		t.Fatal("route input ceiling not reserved before receive")
	}
}

func TestRouteSummaryPrerequisiteAccounting(t *testing.T) {
	for _, state := range []TopologyState{TopologyUnsupported, TopologyDenied, TopologyReadFailed, TopologyLimitExceeded, TopologyBindingChanged, TopologyCaptureExpired} {
		f := newRouteSummaryFixture()
		f.links.State = state
		f.links.Links = nil
		f.links.identities = nil
		f.links.namespace = [2]uint64{}
		o := f.capture()
		if o.State != state || o.CumulativeBytes != f.links.CumulativeBytes || o.CumulativeReads != f.links.CumulativeReads || o.LinkDatagrams != f.links.Datagrams || o.LinkBytes != f.links.NetlinkBytes || f.base.opens != 0 || f.base.namespaces != 0 || !o.StartedAt.IsZero() {
			t.Fatalf("failed prerequisite was renewed or work erased: %+v", o)
		}
	}
}
