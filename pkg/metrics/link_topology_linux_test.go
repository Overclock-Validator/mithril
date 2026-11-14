//go:build linux

package metrics

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestLinkTopologyLinuxUAPI(t *testing.T) {
	for _, pair := range [][2]int{{linkNew, unix.RTM_NEWLINK}, {linkGet, unix.RTM_GETLINK}, {linkDone, unix.NLMSG_DONE}, {linkError, unix.NLMSG_ERROR}, {linkMulti, unix.NLM_F_MULTI}, {linkDumpInterrupted, unix.NLM_F_DUMP_INTR}, {linkRequestDump, unix.NLM_F_REQUEST | unix.NLM_F_DUMP}, {linkAttrNested, unix.NLA_F_NESTED}, {linkAttrNetworkOrder, unix.NLA_F_NET_BYTEORDER}, {linkName, unix.IFLA_IFNAME}, {linkMTU, unix.IFLA_MTU}, {linkParent, unix.IFLA_LINK}, {linkMaster, unix.IFLA_MASTER}, {linkOperState, unix.IFLA_OPERSTATE}, {linkInfo, unix.IFLA_LINKINFO}, {linkCarrier, unix.IFLA_CARRIER}, {linkXDP, unix.IFLA_XDP}} {
		if pair[0] != pair[1] {
			t.Fatal("portable parser differs from installed Linux UAPI")
		}
	}
	if unix.SizeofIfInfomsg != 16 || unix.NLMSG_HDRLEN != 16 || unix.IFLA_INFO_KIND != 1 || unix.IFLA_XDP_ATTACHED != 2 || unix.IFF_UP != 1 {
		t.Fatal("fixed link layout differs from Linux UAPI")
	}
	if linkTopologyError(linkTopologySyscallError(unix.EAFNOSUPPORT)) != TopologyUnsupported || linkTopologyError(unix.EACCES) != TopologyDenied || linkTopologyError(unix.ENOBUFS) != TopologyReadFailed {
		t.Fatal("native failure classification differs")
	}
}

func linkLinuxWitnessAttrs(t *testing.T, value []byte) []syscall.NetlinkRouteAttr {
	t.Helper()
	// Use the standard library's independent route-attribute decoder, supplying
	// only its required ifinfomsg prefix for nested LINKINFO/XDP attribute sets.
	message := syscall.NetlinkMessage{Header: syscall.NlMsghdr{Type: syscall.RTM_NEWLINK}, Data: append(make([]byte, 16), value...)}
	attrs, err := syscall.ParseNetlinkRouteAttr(&message)
	if err != nil {
		t.Fatal("same-byte native attribute witness is malformed")
	}
	return attrs
}

func linkLinuxWitness(t *testing.T, message syscall.NetlinkMessage) LinkTopology {
	t.Helper()
	if len(message.Data) < 16 {
		t.Fatal("native witness missing ifinfomsg")
	}
	row := LinkTopology{Index: binary.NativeEndian.Uint32(message.Data[4:8]), AdminUp: binary.NativeEndian.Uint32(message.Data[8:12])&unix.IFF_UP != 0, OperationalState: "not_reported", Kind: "not_reported", XDPMode: "unknown"}
	attrs, err := syscall.ParseNetlinkRouteAttr(&message)
	if err != nil {
		t.Fatal("native same-byte route attributes invalid")
	}
	for _, attr := range attrs {
		switch attr.Attr.Type & 0x3fff {
		case unix.IFLA_MTU:
			row.MTU, row.HasMTU = binary.NativeEndian.Uint32(attr.Value), true
		case unix.IFLA_MASTER:
			row.MasterIndex, row.HasMaster = binary.NativeEndian.Uint32(attr.Value), true
		case unix.IFLA_LINK:
			row.LinkIndex, row.HasLink = binary.NativeEndian.Uint32(attr.Value), true
		case unix.IFLA_CARRIER:
			row.Carrier, row.HasCarrier = attr.Value[0] == 1, true
		case unix.IFLA_OPERSTATE:
			values := map[byte]string{0: "unknown", 1: "not_present", 2: "down", 3: "lower_layer_down", 4: "testing", 5: "dormant", 6: "up"}
			row.OperationalState = values[attr.Value[0]]
			if row.OperationalState == "" {
				row.OperationalState = "unknown"
			}
		case unix.IFLA_LINKINFO:
			for _, nested := range linkLinuxWitnessAttrs(t, attr.Value) {
				if nested.Attr.Type&0x3fff != unix.IFLA_INFO_KIND {
					continue
				}
				kind := strings.TrimSuffix(string(nested.Value), "\x00")
				allowed := ",bond,bridge,vlan,veth,dummy,gre,gretap,ip6gre,ip6gretap,ipip,sit,ip6tnl,vti,vti6,vxlan,geneve,tun,wireguard,macvlan,macvtap,ipvlan,ipvtap,vrf,"
				row.Kind = "other"
				if strings.Contains(allowed, ","+kind+",") {
					row.Kind = kind
				}
			}
		case unix.IFLA_XDP:
			for _, nested := range linkLinuxWitnessAttrs(t, attr.Value) {
				if nested.Attr.Type&0x3fff != unix.IFLA_XDP_ATTACHED {
					continue
				}
				values := map[byte]string{0: "none", 1: "driver", 2: "generic", 3: "offload", 4: "multi"}
				row.XDPMode = values[nested.Value[0]]
				if row.XDPMode == "" {
					row.XDPMode = "unknown"
				}
			}
		}
	}
	return row
}

func TestLinkTopologyLinuxReadOwnership(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	topology, _ := NewTopologySampler(true).Sample()
	affinity, _ := NewThreadAffinitySampler(true).Sample(topology)
	before := time.Now()
	var datagrams [][]byte
	var starts, receives, opens, closes, namespaces int
	var sequence uint32
	var tokens [][2]uint64
	o := readLinkTopologyCapture(topology, affinity, func() ([2]uint64, error) {
		namespaces++
		token, err := interfaceTrafficNamespace()
		if err == nil {
			tokens = append(tokens, token)
		}
		return token, err
	}, func(c *linkTopologyCapture) (linkTopologySocket, error) {
		socket, err := openLinkTopologySocket(c)
		if err != nil {
			return socket, err
		}
		opens++
		// Refuse a mutation-shaped message before any actual send. The socket
		// stays owned by this test, which makes only the normal two GETLINKs.
		bad := linkTopologyRequest(1)
		binary.NativeEndian.PutUint16(bad[4:6], unix.RTM_SETLINK)
		if err := socket.send(bad[:]); !errors.Is(err, errTopologyRecord) {
			t.Fatal("mutation-shaped request was not refused")
		}
		send, receive, close := socket.send, socket.receive, socket.close
		socket.send = func(data []byte) error {
			starts++
			sequence = binary.NativeEndian.Uint32(data[8:12])
			want := linkTopologyRequest(uint32(starts))
			if !bytes.Equal(data, want[:]) || starts > 2 {
				t.Fatal("unexpected native request")
			}
			return send(data)
		}
		socket.receive = func(buffer []byte, wait time.Duration) (int, uint32, uint32, bool, error) {
			receives++
			n, sender, groups, truncated, err := receive(buffer, wait)
			if err == nil && n > 0 && n <= len(buffer) && sequence == 1 {
				datagrams = append(datagrams, bytes.Clone(buffer[:n]))
			}
			return n, sender, groups, truncated, err
		}
		socket.close = func() { closes++; close() }
		return socket, nil
	}, time.Now)
	after := time.Now()
	if closes != opens || opens > 1 || starts > 2 || receives > 16 || namespaces > 2 || o.CumulativeReads > 2048 || o.CumulativeBytes > 12<<20 || o.NetlinkBytes > 1<<20 {
		t.Fatal("native link capture exceeded ownership/work bounds")
	}
	if o.State == TopologyDenied || o.State == TopologyUnsupported {
		if o.Links != nil || o.identities != nil {
			t.Fatal("unavailable source retained links")
		}
		t.Logf("source=%s; no positive native netlink availability claim", o.State)
		return
	}
	if o.State != TopologyAvailable {
		t.Fatalf("native link capture incomplete: %s (not a positive witness)", o.State)
	}
	if opens != 1 || closes != 1 || starts != 2 || namespaces != 2 || len(tokens) != 2 || tokens[0] != tokens[1] || tokens[0] != o.namespace || o.StartedAt.Before(before) || o.SampledAt.After(after) || o.SampledAt.Before(o.StartedAt) || o.StartedAt == o.StartedAt.Round(0) || o.MetadataStartedAt != topology.StartedAt {
		t.Fatal("native namespace/clock/socket ownership differs")
	}
	want := make(map[uint32]LinkTopology)
	for _, data := range datagrams {
		messages, err := syscall.ParseNetlinkMessage(data)
		if err != nil {
			t.Fatal("same-byte native message witness malformed")
		}
		for _, message := range messages {
			if message.Header.Type == unix.RTM_NEWLINK {
				row := linkLinuxWitness(t, message)
				want[row.Index] = row
			}
		}
	}
	if len(want) != len(o.Links) {
		t.Fatal("same-byte native link count differs")
	}
	for _, row := range o.Links {
		if !reflect.DeepEqual(want[row.Index], row) {
			t.Fatal("same-byte native allowlisted link attributes differ")
		}
	}
	t.Logf("native read-only links=%d datagrams=%d bytes=%d; optional modes absent here are covered by fixtures, not native positive claims", len(o.Links), o.Datagrams, o.NetlinkBytes)
}
