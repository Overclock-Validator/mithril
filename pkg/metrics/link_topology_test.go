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
	"strconv"
	"strings"
	"testing"
	"time"
)

func linkTestAttribute(kind uint16, value []byte) []byte {
	n := 4 + len(value)
	data := make([]byte, (n+3)&^3)
	binary.NativeEndian.PutUint16(data[:2], uint16(n))
	binary.NativeEndian.PutUint16(data[2:4], kind)
	copy(data[4:], value)
	return data
}

func linkTestUint(n uint32) []byte {
	b := make([]byte, 4)
	binary.NativeEndian.PutUint32(b, n)
	return b
}

func linkTestPayload(index uint32, name string, attributes ...[]byte) []byte {
	data := make([]byte, 16)
	binary.NativeEndian.PutUint32(data[4:8], index)
	binary.NativeEndian.PutUint32(data[8:12], 1)
	data = append(data, linkTestAttribute(linkName, append([]byte(name), 0))...)
	for _, attribute := range attributes {
		data = append(data, attribute...)
	}
	return data
}

func linkTestMessage(kind, flags uint16, sequence, port uint32, payload []byte) []byte {
	n := 16 + len(payload)
	data := make([]byte, (n+3)&^3)
	binary.NativeEndian.PutUint32(data[:4], uint32(n))
	binary.NativeEndian.PutUint16(data[4:6], kind)
	binary.NativeEndian.PutUint16(data[6:8], flags)
	binary.NativeEndian.PutUint32(data[8:12], sequence)
	binary.NativeEndian.PutUint32(data[12:16], port)
	copy(data[16:], payload)
	return data
}

func linkTestDone(sequence uint32) []byte {
	return linkTestMessage(linkDone, linkMulti, sequence, 42, make([]byte, 4))
}

type linkTopologyFixture struct {
	topology                                   TopologyObservation
	affinity                                   ThreadAffinityObservation
	at                                         time.Time
	before, after                              [2]uint64
	packets                                    [][]byte
	openError, receiveError                    error
	sender, groups                             uint32
	truncated                                  bool
	opens, sends, receives, closes, namespaces int
}

func newLinkTopologyFixture() *linkTopologyFixture {
	a := newAffinityFixture()
	affinity := a.capture()
	affinity.Generation = 9
	first := linkTestPayload(7, "PRIVATE_LINK", linkTestAttribute(linkMTU, linkTestUint(1500)), linkTestAttribute(linkOperState, []byte{6}), linkTestAttribute(linkCarrier, []byte{1}))
	return &linkTopologyFixture{topology: a.topology, affinity: affinity, at: a.at.Add(time.Millisecond), before: [2]uint64{1, 7}, after: [2]uint64{1, 7}, packets: [][]byte{linkTestMessage(linkNew, linkMulti, 1, 42, first), linkTestDone(1), linkTestMessage(linkNew, linkMulti, 2, 42, first), linkTestDone(2)}}
}

func (f *linkTopologyFixture) namespace() ([2]uint64, error) {
	f.namespaces++
	if f.namespaces%2 == 1 {
		return f.before, nil
	}
	return f.after, nil
}

func (f *linkTopologyFixture) open(c *linkTopologyCapture) (linkTopologySocket, error) {
	if err := affinityOpen(&c.base); err != nil {
		return linkTopologySocket{}, err
	}
	f.opens++
	if f.openError != nil {
		return linkTopologySocket{}, f.openError
	}
	return linkTopologySocket{port: 42,
		send: func(data []byte) error {
			f.sends++
			want := linkTopologyRequest(uint32(f.sends))
			if !bytes.Equal(data, want[:]) {
				return errors.New("unexpected request")
			}
			return nil
		},
		receive: func(buffer []byte, wait time.Duration) (int, uint32, uint32, bool, error) {
			if len(buffer) != 65536 || wait <= 0 || wait > 100*time.Millisecond {
				panic("invalid receive bounds")
			}
			f.receives++
			if f.receiveError != nil {
				return 0, 0, 0, false, f.receiveError
			}
			if f.receives > len(f.packets) {
				return 0, 0, 0, false, errTopologyExpired
			}
			packet := f.packets[f.receives-1]
			copy(buffer, packet)
			return len(packet), f.sender, f.groups, f.truncated, nil
		}, close: func() { f.closes++ }}, nil
}

func (f *linkTopologyFixture) capture() LinkTopologyObservation {
	return readLinkTopologyCapture(f.topology, f.affinity, f.namespace, f.open, func() time.Time { return f.at })
}

func TestLinkTopologyCapture(t *testing.T) {
	f := newLinkTopologyFixture()
	o := f.capture()
	if o.State != TopologyAvailable || len(o.Links) != 1 || o.Links[0].Index != 7 || !o.Links[0].HasMTU || o.Links[0].MTU != 1500 || f.sends != 2 || f.namespaces != 2 || f.opens != 1 || f.closes != 1 || o.Datagrams != 4 {
		t.Fatalf("actual-shaped capture differs: %+v", o)
	}
	if o.MetadataStartedAt != f.topology.StartedAt || o.StartedAt != f.at || o.SampledAt != f.at || o.StartedAt == o.StartedAt.Round(0) || o.TopologyGeneration != 7 || o.AffinityGeneration != 9 || o.CumulativeReads != f.affinity.CumulativeReads+1 || o.CumulativeBytes != f.affinity.CumulativeBytes+o.NetlinkBytes {
		t.Fatal("capture origin or shared accounting changed")
	}
	body, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(body, []byte("PRIVATE_LINK")) || bytes.Contains(body, []byte("namespace")) || bytes.Contains(body, []byte("identities")) {
		t.Fatal("private join identity exported")
	}
	for i := range f.packets {
		clear(f.packets[i])
	}
	if o.identities[0].name != "PRIVATE_LINK" || o.Links[0].MTU != 1500 {
		t.Fatal("retained rows alias receive buffers")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*linkTopologyFixture)
	}{
		{"namespace", func(f *linkTopologyFixture) { f.after[1]++ }},
		{"name", func(f *linkTopologyFixture) {
			f.packets[2] = linkTestMessage(linkNew, linkMulti, 2, 42, linkTestPayload(7, "replacement"))
		}},
		{"index", func(f *linkTopologyFixture) {
			f.packets[2] = linkTestMessage(linkNew, linkMulti, 2, 42, linkTestPayload(8, "PRIVATE_LINK"))
		}},
		{"membership", func(f *linkTopologyFixture) {
			f.packets = f.packets[:2]
			f.packets = append(f.packets, linkTestDone(2))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLinkTopologyFixture()
			tc.mutate(f)
			o := f.capture()
			if o.State != TopologyBindingChanged || o.Links != nil || o.identities != nil || o.namespace != ([2]uint64{}) || f.closes != 1 {
				t.Fatal("incompatible link binding retained")
			}
		})
	}
	// The second dump fences identity only, not changing nonmonotone gauges.
	f = newLinkTopologyFixture()
	f.packets[2] = linkTestMessage(linkNew, linkMulti, 2, 42, linkTestPayload(7, "PRIVATE_LINK", linkTestAttribute(linkMTU, linkTestUint(9000))))
	if o = f.capture(); o.State != TopologyAvailable || o.Links[0].MTU != 1500 {
		t.Fatal("later attributes replaced original capture")
	}
	f = newLinkTopologyFixture()
	f.packets = [][]byte{linkTestDone(1), linkTestDone(2)}
	if o = f.capture(); o.State != TopologyAvailable || o.Links == nil || len(o.Links) != 0 {
		t.Fatal("known empty inventory became unknown")
	}
	f = newLinkTopologyFixture()
	f.packets = nil
	for sequence := uint32(1); sequence <= 2; sequence++ {
		var batch []byte
		for index := 256; index > 0; index-- {
			batch = append(batch, linkTestMessage(linkNew, linkMulti, sequence, 42, linkTestPayload(uint32(index), "n"+strconv.Itoa(index)))...)
		}
		f.packets = append(f.packets, batch, linkTestDone(sequence))
	}
	o = f.capture()
	if o.State != TopologyAvailable || len(o.Links) != 256 || len(o.identities) != 256 || o.Links[0].Index != 1 || o.Links[255].Index != 256 {
		t.Fatal("complete 256-link capture or deterministic order differs")
	}
}

func TestLinkTopologyAttributes(t *testing.T) {
	for state, want := range []string{"unknown", "not_present", "down", "lower_layer_down", "testing", "dormant", "up"} {
		row, err := parseLinkTopology(linkTestPayload(1, "link", linkTestAttribute(linkOperState, []byte{byte(state)})))
		if err != nil || row.value.OperationalState != want {
			t.Fatal("operational state differs")
		}
	}
	for mode, want := range []string{"none", "driver", "generic", "offload", "multi", "unknown"} {
		data := linkTestPayload(1, "link", linkTestAttribute(linkXDP, append(linkTestAttribute(2, []byte{byte(mode)}), linkTestAttribute(4, linkTestUint(0xdeadbeef))...)))
		row, err := parseLinkTopology(data)
		if err != nil || row.value.XDPMode != want {
			t.Fatal("XDP attached mode differs")
		}
	}
	for _, kind := range []string{"bond", "bridge", "vlan", "gre", "tun", "wireguard", "vxlan", "PRIVATE_UNKNOWN_KIND"} {
		data := linkTestPayload(2, "link", linkTestAttribute(linkInfo|linkAttrNested, linkTestAttribute(1, append([]byte(kind), 0))))
		row, err := parseLinkTopology(data)
		want := kind
		if kind == "PRIVATE_UNKNOWN_KIND" {
			want = "other"
		}
		if err != nil || row.value.Kind != want {
			t.Fatal("kind classification differs")
		}
		body, err := json.Marshal(row.value)
		if err != nil || bytes.Contains(body, []byte("PRIVATE_UNKNOWN_KIND")) {
			t.Fatal("unknown kind text retained")
		}
	}
	row, err := parseLinkTopology(linkTestPayload(1, "link"))
	if err != nil || row.value.Kind != "not_reported" || row.value.XDPMode != "unknown" || row.value.OperationalState != "not_reported" || row.value.HasMTU || row.value.HasCarrier || row.value.HasMaster || row.value.HasLink {
		t.Fatal("optional absence fabricated values")
	}
	row, err = parseLinkTopology(linkTestPayload(1, "link", linkTestAttribute(linkMTU, linkTestUint(0)), linkTestAttribute(linkMaster, linkTestUint(math.MaxUint32)), linkTestAttribute(linkParent, linkTestUint(0)), linkTestAttribute(linkCarrier, []byte{0}), linkTestAttribute(linkOperState, []byte{255})))
	if err != nil || !row.value.HasMTU || row.value.MTU != 0 || !row.value.HasMaster || row.value.MasterIndex != math.MaxUint32 || !row.value.HasLink || row.value.LinkIndex != 0 || !row.value.HasCarrier || row.value.Carrier || row.value.OperationalState != "unknown" {
		t.Fatal("known zero/raw index/unknown enum conflated")
	}
	for _, attr := range [][]byte{linkTestAttribute(linkCarrier, []byte{2}), linkTestAttribute(linkMTU, []byte{1}), linkTestAttribute(linkMTU|linkAttrNetworkOrder, linkTestUint(1)), linkTestAttribute(linkName, []byte("duplicate\x00")), linkTestAttribute(linkXDP, linkTestAttribute(2, []byte{0, 1})), linkTestAttribute(linkInfo, linkTestAttribute(1, []byte("bad")))} {
		if _, err := parseLinkTopology(linkTestPayload(1, "link", attr)); err == nil {
			t.Fatal("invalid scalar or duplicate accepted")
		}
	}
	for _, name := range []string{"", ".", "..", "bad/name", "bad\nname", "embedded\x00name", strings.Repeat("x", 16)} {
		if _, err := parseLinkTopology(linkTestPayload(1, name)); err == nil {
			t.Fatal("invalid private name accepted")
		}
	}
	row, err = parseLinkTopology(linkTestPayload(1, "PRIVATE_NAME", linkTestAttribute(1, []byte("PRIVATE_ADDRESS")), linkTestAttribute(20, []byte("PRIVATE_ALIAS")), linkTestAttribute(linkXDP, append(linkTestAttribute(2, []byte{1}), linkTestAttribute(4, linkTestUint(3735928559))...))))
	body, marshalErr := json.Marshal(row.value)
	if err != nil || marshalErr != nil || bytes.Contains(body, []byte("PRIVATE_")) || bytes.Contains(body, []byte("3735928559")) {
		t.Fatal("discarded kernel identity/address/program fields were exposed")
	}
}

func TestLinkDumpBoundaries(t *testing.T) {
	valid := linkTestMessage(linkNew, linkMulti, 1, 42, linkTestPayload(1, "link"))
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"short_header", valid[:15]}, {"short_body", linkTestMessage(linkNew, linkMulti, 1, 42, make([]byte, 15))},
		{"wrong_sequence", linkTestMessage(linkNew, linkMulti, 2, 42, linkTestPayload(1, "link"))},
		{"wrong_port", linkTestMessage(linkNew, linkMulti, 1, 43, linkTestPayload(1, "link"))},
		{"filtered", linkTestMessage(linkNew, linkMulti|0x20, 1, 42, linkTestPayload(1, "link"))},
		{"interrupted", linkTestMessage(linkDone, linkMulti|linkDumpInterrupted, 1, 42, make([]byte, 4))},
		{"done_error", linkTestMessage(linkDone, linkMulti, 1, 42, linkTestUint(1))},
		{"unknown_message", linkTestMessage(99, linkMulti, 1, 42, nil)},
		{"after_done", append(linkTestDone(1), valid...)},
		{"zero_index", linkTestMessage(linkNew, linkMulti, 1, 42, linkTestPayload(0, "link"))},
		{"negative_index", linkTestMessage(linkNew, linkMulti, 1, 42, linkTestPayload(math.MaxUint32, "link"))},
		{"trailing_byte", append(bytes.Clone(valid), 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := linkTopologyDump{sequence: 1, port: 42}
			if err := d.consume(tc.data); err == nil {
				t.Fatal("malformed or incomplete record accepted")
			}
		})
	}
	for _, indexDuplicate := range []bool{false, true} {
		d := linkTopologyDump{sequence: 1, port: 42}
		if err := d.consume(valid); err != nil {
			t.Fatal(err)
		}
		index, name := uint32(2), "link"
		if indexDuplicate {
			index, name = 1, "other"
		}
		if err := d.consume(linkTestMessage(linkNew, linkMulti, 1, 42, linkTestPayload(index, name))); err == nil {
			t.Fatal("duplicate identity accepted")
		}
	}
	d := linkTopologyDump{sequence: 1, port: 42}
	for i := range 256 {
		if err := d.consume(linkTestMessage(linkNew, linkMulti, 1, 42, linkTestPayload(uint32(i+1), "n"+strconv.Itoa(i)))); err != nil {
			t.Fatal("full 256 links refused")
		}
	}
	if err := d.consume(linkTestMessage(linkNew, linkMulti, 1, 42, linkTestPayload(257, "overflow"))); err != errTopologyLimit {
		t.Fatal("257th link did not refuse whole dump")
	}
	attrs := make([][]byte, 127)
	for i := range attrs {
		attrs[i] = linkTestAttribute(100, nil)
	}
	if _, err := parseLinkTopology(linkTestPayload(1, "link", attrs...)); err != nil {
		t.Fatal("exact attribute cap refused")
	}
	attrs = append(attrs, linkTestAttribute(100, nil))
	if _, err := parseLinkTopology(linkTestPayload(1, "link", attrs...)); err != errTopologyLimit {
		t.Fatal("attribute cap+1 accepted")
	}
	nested := linkTestAttribute(100, nil)
	for range 2 {
		nested = linkTestAttribute(100|linkAttrNested, nested)
	}
	if _, err := parseLinkTopology(linkTestPayload(1, "link", nested)); err != nil {
		t.Fatal("depth three refused")
	}
	nested = linkTestAttribute(100|linkAttrNested, nested)
	if _, err := parseLinkTopology(linkTestPayload(1, "link", nested)); err != errTopologyLimit {
		t.Fatal("depth four accepted")
	}
	bad := linkTestAttribute(100, []byte{1})
	binary.NativeEndian.PutUint16(bad[:2], 3)
	if _, err := parseLinkTopology(linkTestPayload(1, "link", bad)); err == nil {
		t.Fatal("invalid attribute length accepted")
	}
}

func TestLinkTopologyTransportRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*linkTopologyFixture)
		state  TopologyState
	}{
		{"denied", func(f *linkTopologyFixture) { f.openError = os.ErrPermission }, TopologyDenied},
		{"unsupported", func(f *linkTopologyFixture) { f.openError = errLinkUnsupported }, TopologyUnsupported},
		{"overrun", func(f *linkTopologyFixture) { f.receiveError = errors.New("overrun") }, TopologyReadFailed},
		{"sender", func(f *linkTopologyFixture) { f.sender = 1 }, TopologyReadFailed},
		{"multicast", func(f *linkTopologyFixture) { f.groups = 1 }, TopologyReadFailed},
		{"truncated", func(f *linkTopologyFixture) { f.truncated = true }, TopologyReadFailed},
		{"missing_done", func(f *linkTopologyFixture) { f.packets = f.packets[:1] }, TopologyCaptureExpired},
		{"kernel_denied", func(f *linkTopologyFixture) {
			f.packets[0] = linkTestMessage(linkError, 0, 1, 42, linkTestUint(math.MaxUint32))
		}, TopologyDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLinkTopologyFixture()
			tc.mutate(f)
			o := f.capture()
			if o.State != tc.state || o.Links != nil || o.identities != nil || f.sends > 2 || f.openError == nil && f.closes != 1 {
				t.Fatal("failed transport retained complete data or leaked socket")
			}
		})
	}
}

func TestLinkTopologySharedBudget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*linkTopologyFixture)
		state  TopologyState
	}{
		{"wrong_topology", func(f *linkTopologyFixture) { f.affinity.TopologyGeneration++ }, TopologyReadFailed},
		{"missing_affinity", func(f *linkTopologyFixture) { f.affinity.Generation = 0 }, TopologyReadFailed},
		{"counter_regression", func(f *linkTopologyFixture) { f.affinity.CumulativeReads = 0 }, TopologyReadFailed},
		{"wall_start", func(f *linkTopologyFixture) { f.topology.StartedAt = f.topology.StartedAt.Round(0) }, TopologyCaptureExpired},
		{"wall_end", func(f *linkTopologyFixture) { f.affinity.SampledAt = f.affinity.SampledAt.Round(0) }, TopologyCaptureExpired},
		{"expired_prior", func(f *linkTopologyFixture) { f.affinity.State = TopologyCaptureExpired }, TopologyCaptureExpired},
		{"expired_now", func(f *linkTopologyFixture) { f.at = f.topology.StartedAt.Add(time.Second + time.Nanosecond) }, TopologyCaptureExpired},
		{"open_cap", func(f *linkTopologyFixture) { f.affinity.CumulativeReads = topologyOpenLimit }, TopologyLimitExceeded},
		{"byte_cap", func(f *linkTopologyFixture) { f.affinity.CumulativeBytes = topologyByteLimit }, TopologyLimitExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLinkTopologyFixture()
			tc.mutate(f)
			o := f.capture()
			if o.State != tc.state || f.opens != 0 || f.namespaces != 0 {
				t.Fatal("invalid inherited work renewed budget")
			}
		})
	}
	f := newLinkTopologyFixture()
	open := func(c *linkTopologyCapture) (linkTopologySocket, error) {
		s, err := f.open(c)
		receive := s.receive
		s.receive = func(b []byte, wait time.Duration) (int, uint32, uint32, bool, error) {
			n, sender, groups, truncated, err := receive(b, wait)
			f.at = c.started.Add(linkCaptureLimit + time.Nanosecond)
			return n, sender, groups, truncated, err
		}
		return s, err
	}
	o := readLinkTopologyCapture(f.topology, f.affinity, f.namespace, open, func() time.Time { return f.at })
	if o.State != TopologyCaptureExpired || f.receives != 1 || o.Links != nil || f.closes != 1 {
		t.Fatal("link subdeadline renewed or late data installed")
	}
	for _, extra := range []bool{false, true} {
		f = newLinkTopologyFixture()
		f.packets = nil
		for sequence := uint32(1); sequence <= 2; sequence++ {
			for index := 1; index <= 7; index++ {
				f.packets = append(f.packets, linkTestMessage(linkNew, linkMulti, sequence, 42, linkTestPayload(uint32(index), "n"+strconv.Itoa(index))))
			}
			if extra && sequence == 2 {
				f.packets = append(f.packets, linkTestMessage(linkNew, linkMulti, sequence, 42, linkTestPayload(8, "n8")))
			}
			f.packets = append(f.packets, linkTestDone(sequence))
		}
		o = f.capture()
		if !extra && (o.State != TopologyAvailable || o.Datagrams != 16) {
			t.Fatal("combined exact datagram cap refused")
		}
		if extra && (o.State != TopologyLimitExceeded || f.receives != 16 || o.Links != nil) {
			t.Fatal("second dump got a new datagram allowance")
		}
	}
	f = newLinkTopologyFixture()
	c, state := linkTopologyBudget(f.topology, f.affinity, func() time.Time { return f.at })
	if state != "" {
		t.Fatal(state)
	}
	c.started = f.at
	c.inputBytes = linkInputLimit - linkReceiveBytes + 1
	s, err := f.open(&c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if _, err := readLinkTopologyDump(&c, s, 1); err != errTopologyLimit || f.receives != 0 {
		t.Fatal("aggregate byte cap not checked before receive")
	}
}

func TestLinkTopologyDisabled(t *testing.T) {
	var s *LinkTopologySampler
	calls := 0
	read := func(TopologyObservation, ThreadAffinityObservation) LinkTopologyObservation {
		calls++
		return LinkTopologyObservation{State: TopologyUnsupported}
	}
	if NewLinkTopologySampler(false) != nil {
		t.Fatal("disabled constructor allocated")
	}
	if _, ok := s.sample(TopologyObservation{}, ThreadAffinityObservation{}, read); ok || calls != 0 {
		t.Fatal("disabled sampling performed work")
	}
	if n := testing.AllocsPerRun(100, func() { s.Sample(TopologyObservation{}, ThreadAffinityObservation{}) }); n != 0 {
		t.Fatal("disabled path allocated")
	}
	s = NewLinkTopologySampler(true)
	s.generation = math.MaxUint64 - 2
	o, _ := s.sample(TopologyObservation{}, ThreadAffinityObservation{}, read)
	if o.Generation != math.MaxUint64-1 {
		t.Fatal("last generation differs")
	}
	o, _ = s.sample(TopologyObservation{}, ThreadAffinityObservation{}, read)
	if calls != 1 || o.State != TopologyLimitExceeded {
		t.Fatal("generation exhaustion wrapped or read")
	}
	for _, sequence := range []uint32{1, 2} {
		request := linkTopologyRequest(sequence)
		if binary.NativeEndian.Uint16(request[4:6]) != 18 || binary.NativeEndian.Uint16(request[6:8]) != 0x301 || !bytes.Equal(request[12:], make([]byte, 20)) {
			t.Fatal("request builder serialized non-dump work")
		}
	}
	if runtime.GOOS != "linux" {
		f := newLinkTopologyFixture()
		at := time.Now()
		f.topology.StartedAt, f.topology.SampledAt = at.Add(-4*time.Millisecond), at.Add(-3*time.Millisecond)
		f.affinity.StartedAt, f.affinity.SampledAt = at.Add(-2*time.Millisecond), at.Add(-time.Millisecond)
		o, ok := NewLinkTopologySampler(true).Sample(f.topology, f.affinity)
		if !ok || o.State != TopologyUnsupported || o.StartedAt.IsZero() || o.CumulativeReads != f.affinity.CumulativeReads {
			t.Fatal("unsupported capture lost date or performed I/O")
		}
	}
	if reflect.TypeOf(LinkTopology{}).NumField() != 13 {
		t.Fatal("review new row fields for redaction and bounds")
	}
}
