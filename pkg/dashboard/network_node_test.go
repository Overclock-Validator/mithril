package dashboard

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func maximumPrewarmHandover(at time.Time) *PrewarmHandoverDetail {
	first, last := uint64(10000000000000000000), uint64(math.MaxUint64)
	return &PrewarmHandoverDetail{Generation: math.MaxUint64 - 1, HandedOverAt: at,
		FloorSlot: first, Capacity: math.MaxUint64, ReturnedBlocks: last - first + 1, OverflowEvictions: math.MaxUint64,
		FirstSlot: &first, LastSlot: &last,
		Repair: &PrewarmRepairDetail{SampledAt: at, Requests: math.MaxUint64, Responses: math.MaxUint64,
			LateResponses: math.MaxUint64, Timeouts: math.MaxUint64}}
}

func maximumInterfaceTraffic(now time.Time) *InterfaceTrafficDetail {
	total := uint32(256)
	d := &InterfaceTrafficDetail{Scope: "visible_interface_counters", State: "available", CaptureStartedAt: now.UTC(), SampledAt: now.UTC(),
		TotalInterfaces: &total, Interfaces: make([]InterfaceTrafficCounter, 8)}
	for i := range d.Interfaces {
		d.Interfaces[i] = InterfaceTrafficCounter{Index: math.MaxUint32 - 8 + uint32(i), ReceiveBytes: math.MaxUint64, TransmitBytes: math.MaxUint64,
			ReceivePackets: math.MaxUint64, TransmitPackets: math.MaxUint64,
			Failures: &InterfaceTrafficFailures{ReceiveErrors: math.MaxUint64, ReceiveDropped: math.MaxUint64, TransmitErrors: math.MaxUint64, TransmitDropped: math.MaxUint64}}
	}
	return d
}

func TestPublicNetworkLegacyDoesNotReceiveMTU(t *testing.T) {
	now := time.Now()
	_, capture := nodeNetworkSourceFixture(now)
	capture.Detail.Interfaces.Interfaces[0].MTU = 1500
	family, err := preparePublicNodeNetwork(capture.Detail, now, capture.FreshUntil)
	if err != nil || strings.Contains(family.tail, `"mtu"`) || strings.Contains(family.independentOnly, `"mtu"`) {
		t.Fatal("legacy public projection included MTU or failed")
	}
	if capture.Detail.Interfaces.Interfaces[0].MTU != 1500 {
		t.Fatal("public projection mutated the source observation")
	}
	if !strings.Contains(family.latestTail, `"mtu":"1500"`) || !strings.Contains(family.latestIndependentOnly, `"mtu":"1500"`) {
		t.Fatal("negotiated public projections lost MTU")
	}
}

func TestRepairGateRequiresBoundReceiverAndFreshLease(t *testing.T) {
	now := time.Now()
	_, capture := nodeNetworkSourceFixture(now)
	capture.Detail.RepairGate = &RepairGateDetail{SampledAt: now.UTC(), Armed: false}
	if err := validateNodeNetworkDetail(capture.Detail); err != nil {
		t.Fatal(err)
	}
	if _, err := preparePublicNodeNetwork(capture.Detail, now, capture.FreshUntil); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*NodeNetworkDetail){
		func(d *NodeNetworkDetail) { d.Receiver = nil; d.ReceiverReason = "source_unavailable" },
		func(d *NodeNetworkDetail) {
			d.RepairGate.SampledAt = d.Receiver.Binding.ReceiverStartedAt.Add(-time.Second)
		},
		func(d *NodeNetworkDetail) { d.RepairGate.SampledAt = d.CapturedAt.Add(2 * time.Second) },
	} {
		d := capture.Detail
		gate := *d.RepairGate
		d.RepairGate = &gate
		change(&d)
		if validateNodeNetworkDetail(d) == nil {
			t.Fatal("unbound or invalid gate accepted")
		}
	}
	if _, err := preparePublicNodeNetwork(capture.Detail, now.Add(NetworkDetailFreshFor), now.Add(2*NetworkDetailFreshFor)); err == nil {
		t.Fatal("expired repair gate extended the public lease")
	}
}

func TestRepairGateProjectionFitsBoundedFamily(t *testing.T) {
	now := time.Now().UTC()
	d := maximumNodeNetwork(now)
	for i := range d.Interfaces.Interfaces {
		d.Interfaces.Interfaces[i].MTU = math.MaxUint32
	}
	d.RepairGate = &RepairGateDetail{SampledAt: now, Armed: true}
	family, err := preparePublicNodeNetwork(d, now, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	total := len(family.tail) + len(family.independentOnly) + len(family.extendedTail) +
		len(family.extendedIndependentOnly) + len(family.latestTail) + len(family.latestIndependentOnly) + len(family.gateTail)
	for _, body := range family.bodies {
		total += len(body)
		if family.gateTail != "" && len(body)+len(family.gateTail) > MaxNetworkDetailBytes {
			t.Fatal("gate projection exceeds document bound")
		}
	}
	if total > networkDetailFamilyBytes {
		t.Fatalf("network family exceeds cache bound: %d", total)
	}
	if strings.Contains(family.tail, `"repair_gate"`) || strings.Contains(family.latestTail, `"repair_gate"`) ||
		strings.Contains(family.latestIndependentOnly, `"repair_gate"`) {
		t.Fatal("unnegotiated or receiver-cleared response exposed repair gate")
	}
	if family.gateTail != "" && !strings.Contains(family.gateTail, `"repair_gate"`) {
		t.Fatal("negotiated projection lost repair gate")
	}
}

func maximumNodeNetwork(now time.Time) NodeNetworkDetail {
	receiver := maximumReceiverNetworkDetail(now.UTC())
	return NodeNetworkDetail{SchemaVersion: 2, Family: "network", Scope: "node_network", CapturedAt: now.UTC(),
		Binding:  NodeNetworkBinding{RunID: receiver.Binding.RunID, NodeStartedAt: receiver.Binding.NodeStartedAt, GenesisHash: receiver.Binding.GenesisHash},
		Receiver: &receiver, Interfaces: maximumInterfaceTraffic(now)}
}

func TestNodeNetworkContractAndBounds(t *testing.T) {
	now := time.Date(9999, 12, 31, 23, 59, 59, 123456789, time.UTC)
	d := maximumNodeNetwork(now)
	body, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeNodeNetworkDetail(body); err != nil {
		t.Fatal(err)
	}
	if len(body) > MaxNetworkDetailBytes {
		t.Fatalf("combined network body exceeds unchanged cap: %d", len(body))
	}
	t.Logf("combined private network document: %d/%d bytes", len(body), MaxNetworkDetailBytes)
	d.Receiver, d.ReceiverReason = nil, "source_unavailable"
	body, _ = json.Marshal(d)
	if _, err := decodeNodeNetworkDetail(body); err != nil {
		t.Fatal("interface-only capture depended on a receiver")
	}
	for _, state := range []string{"unsupported", "read_failed", "invalid_record", "binding_changed", "capture_expired", "clock_invalid"} {
		d.Interfaces = &InterfaceTrafficDetail{Scope: "visible_interface_counters", State: state, CaptureStartedAt: now.UTC(), SampledAt: now.UTC(), Interfaces: []InterfaceTrafficCounter{}}
		if validateNodeNetworkDetail(d) != nil {
			t.Fatalf("explicit interface failure refused: %s", state)
		}
	}
	for _, mutate := range []func(*NodeNetworkDetail){
		func(d *NodeNetworkDetail) { d.SchemaVersion = 1 },
		func(d *NodeNetworkDetail) { d.Binding.RunID = "" },
		func(d *NodeNetworkDetail) { d.Receiver.Binding.RunID = "another-run" },
		func(d *NodeNetworkDetail) { d.ReceiverReason = "private error" },
		func(d *NodeNetworkDetail) {
			d.Interfaces.Interfaces = append(d.Interfaces.Interfaces, d.Interfaces.Interfaces[0])
		},
		func(d *NodeNetworkDetail) { d.Interfaces.Interfaces[1].Index = d.Interfaces.Interfaces[0].Index },
		func(d *NodeNetworkDetail) { d.Interfaces.Interfaces = nil },
		func(d *NodeNetworkDetail) { d.Interfaces.TotalInterfaces = nil },
		func(d *NodeNetworkDetail) { d.Interfaces.State = "read_failed" },
		func(d *NodeNetworkDetail) { d.Interfaces.SampledAt = now.Add(2 * time.Second).UTC() },
	} {
		d := maximumNodeNetwork(now)
		mutate(&d)
		if validateNodeNetworkDetail(d) == nil {
			t.Fatal("invalid context or interface data accepted")
		}
	}
	for _, body := range []string{strings.Replace(string(body), `"schema_version":2`, `"schema_version":2,"schema_version":2`, 1), string(body) + " ", strings.Repeat(" ", MaxNetworkDetailBytes+1)} {
		if _, err := decodeNodeNetworkDetail([]byte(body)); err == nil {
			t.Fatal("noncanonical or oversized body accepted")
		}
	}
}

func TestNodeNetworkPublicFamilyBoundsAndReceiverLoss(t *testing.T) {
	at := time.Date(9999, 12, 31, 23, 59, 59, 123456789, time.UTC)
	d := maximumNodeNetwork(at)
	family, err := preparePublicNodeNetwork(d, at, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	total := len(family.independentOnly) + len(family.tail)
	for _, body := range family.bodies {
		total += len(body)
	}
	if total > 64<<10 || networkDetailFamilyBytes != 64<<10 || MaxNetworkDetailBytes != 16<<10 {
		t.Fatal("composite network family exceeded or raised an existing limit")
	}
	t.Logf("combined public network document: %d/%d; complete cached family: %d/%d", len(family.bodies[0])+len(family.tail), MaxNetworkDetailBytes, total, networkDetailFamilyBytes)
	var full, without PublicNodeNetworkDetail
	if json.Unmarshal([]byte(family.bodies[0]+family.tail), &full) != nil || json.Unmarshal([]byte(family.independentOnly), &without) != nil ||
		full.Receiver == nil || without.Receiver != nil || without.ReceiverReason != "binding_changed" || without.Interfaces == nil {
		t.Fatal("receiver-loss variant retained old sockets or removed interface facts")
	}
	for i, reason := range networkDetailPublicReasons {
		expected := full
		expected.State, expected.Reason = "stale", reason
		if reason == "" {
			expected.State = "fresh"
		}
		body, err := json.Marshal(expected)
		if err != nil || string(body) != family.bodies[i]+family.tail || len(body) > MaxNetworkDetailBytes {
			t.Fatal("shared bytes changed a finite canonical response variant")
		}
	}
	original, _ := json.Marshal(full.Interfaces)
	retained, _ := json.Marshal(without.Interfaces)
	if string(original) != string(retained) || !full.CapturedAt.Equal(without.CapturedAt) || !full.CollectorAt.Equal(without.CollectorAt) {
		t.Fatal("receiver loss redated or changed independent interface counters")
	}
	d.Interfaces.Interfaces[0].ReceiveBytes = 0
	*d.Binding.GenesisHash = "different"
	if strings.Contains(family.bodies[0]+family.tail, "different") || family.binding.GenesisHash == nil || *family.binding.GenesisHash == "different" {
		t.Fatal("caller mutated the retained family binding or bytes")
	}
}

func TestApplicationPayloadIntervalValidation(t *testing.T) {
	at := time.Now().UTC()
	start := at.Add(-time.Minute)
	total := uint64(20)
	base := ApplicationPayloadCounter{StartedAt: start, SampledAt: at, Bytes: &total,
		Window: &ApplicationPayloadInterval{StartedAt: at.Add(-time.Second), SampledAt: at, DurationNanoseconds: uint64(time.Second), Bytes: 7}}
	if !validApplicationPayloadCounter(&base, start, at) {
		t.Fatal("valid source interval rejected")
	}
	legacy := base
	legacy.Window = nil
	if !validApplicationPayloadCounter(&legacy, start, at) {
		t.Fatal("older counter without a window rejected")
	}
	for name, mutate := range map[string]func(*ApplicationPayloadCounter){
		"unknown total": func(c *ApplicationPayloadCounter) { c.Bytes = nil },
		"future end":    func(c *ApplicationPayloadCounter) { c.Window.SampledAt = at.Add(time.Second) },
		"zero duration": func(c *ApplicationPayloadCounter) { c.Window.DurationNanoseconds = 0 },
		"long duration": func(c *ApplicationPayloadCounter) { c.Window.DurationNanoseconds = uint64(NetworkDetailFreshFor) + 1 },
		"excess bytes":  func(c *ApplicationPayloadCounter) { c.Window.Bytes = 21 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate, window := base, *base.Window
			candidate.Window = &window
			mutate(&candidate)
			if validApplicationPayloadCounter(&candidate, start, at) {
				t.Fatal("invalid payload interval accepted")
			}
		})
	}
}

func TestNodeNetworkGossipOperationBoundsAndScope(t *testing.T) {
	at := time.Date(9999, 12, 31, 23, 59, 59, 123456789, time.UTC)
	d := maximumNodeNetwork(at)
	d.Gossip = &GossipOperationDetail{Scope: "gossip_client_totals", ClientGeneration: math.MaxUint64, StartedAt: at, SampledAt: at,
		RxPings: math.MaxUint64, RxPongs: math.MaxUint64, RxContacts: math.MaxUint64, RxPullRequests: math.MaxUint64,
		TxPushMessages: math.MaxUint64, TxPingMessages: math.MaxUint64, TxPongMessages: math.MaxUint64, TxPullResponses: math.MaxUint64}
	socket := d.Receiver.Live
	socket.ReadBytes = nil
	socket.DropWindow = &SocketDropWindow{StartedAt: at.Add(-30 * time.Second), SampledAt: at,
		DurationMilliseconds: 30000, Drops: math.MaxUint32}
	d.Gossip.Socket = &GossipSocketDetail{Scope: "gossip_udp_socket", Generation: math.MaxUint64, Observation: socket}
	version := uint16(math.MaxUint16)
	d.Gossip.LocalShredVersion = &version
	bytes := uint64(math.MaxUint64 - 1)
	d.Gossip.TxPullRequests = &bytes
	d.Gossip.Payload = &GossipPayloadDetail{Scope: "gossip_run_udp_payload", SocketGeneration: math.MaxUint64,
		StartedAt: at, SampledAt: at, ReceivedBytes: &bytes, SentBytes: &bytes}
	d.Gossip.Payload.Window = &GossipPayloadInterval{StartedAt: at.Add(-NetworkDetailFreshFor), SampledAt: at,
		DurationNanoseconds: uint64(NetworkDetailFreshFor), ReceivedBytes: &bytes, SentBytes: &bytes}
	count := "18446744073709551614"
	d.Gossip.Decode = &GossipDecodeDetail{Scope: "gossip_run_decode", SocketGeneration: math.MaxUint64, StartedAt: at,
		WireKinds:          [8]*string{&count, &count, &count, &count, &count, &count, &count, &count},
		AttemptedContactV2: &bytes, SignatureValid: &bytes, SignatureInvalid: &bytes, UnsupportedType: &bytes, MalformedValue: &bytes, UnparsedSuffix: &bytes}
	d.TPU = &TPUSocketDetail{Scope: "tpu_udp_socket", Generation: math.MaxUint64, SampledAt: at, Observation: socket}
	d.Votor = &VotorSocketDetail{Scope: "votor_inbound_udp_socket", Generation: math.MaxUint64, SampledAt: at, Observation: socket}
	d.VotorPayload = &VotorPayloadDetail{Scope: "votor_instance_application_payload", Generation: math.MaxUint64,
		Received:     &ApplicationPayloadCounter{StartedAt: at, SampledAt: at, Bytes: &bytes},
		SendAccepted: &ApplicationPayloadCounter{StartedAt: at, SampledAt: at, Bytes: &bytes}}
	d.LiveClock = maximumConsensusClock(at)
	d.PrewarmHandover = maximumPrewarmHandover(at)
	d.Receiver.RepairRequests = &ApplicationPayloadCounter{StartedAt: at, SampledAt: at, Bytes: &bytes}
	d.TPUPayload = &TPUPayloadDetail{Scope: "tpu_server_stream_read_prefix", Generation: math.MaxUint64,
		Received: &ApplicationPayloadCounter{StartedAt: at, SampledAt: at, Bytes: &bytes}}
	for _, counter := range []*ApplicationPayloadCounter{d.VotorPayload.Received, d.VotorPayload.SendAccepted, d.TPUPayload.Received, d.Receiver.RepairRequests} {
		counter.Window = &ApplicationPayloadInterval{StartedAt: at.Add(-NetworkDetailFreshFor), SampledAt: at,
			DurationNanoseconds: uint64(NetworkDetailFreshFor), Bytes: bytes}
	}
	d.TPUQueues = &TPUQueuesDetail{Scope: "tpu_pipeline_queue_samples", Generation: math.MaxUint64, StartedAt: at, SampledAt: at,
		Ingress:     TPUQueueGauge{Length: math.MaxUint64, Capacity: math.MaxUint64},
		DedupOutput: TPUQueueGauge{Length: math.MaxUint64, Capacity: math.MaxUint64},
		Verified:    TPUQueueGauge{Length: math.MaxUint64, Capacity: math.MaxUint64}}
	body, _ := json.Marshal(d)
	if _, err := decodeNodeNetworkDetail(body); err != nil || len(body) > MaxNetworkDetailBytes {
		var decoded NodeNetworkDetail
		decodeErr := json.Unmarshal(body, &decoded)
		t.Fatalf("maximum Gossip/receiver/interface document failed: bytes=%d error=%v json=%v original=%v decoded=%v receiver=%v", len(body), err, decodeErr, validateNodeNetworkDetail(d), validateNodeNetworkDetail(decoded), validateNetworkDetail(*d.Receiver))
	}
	saturated := uint64(math.MaxUint64)
	d.Gossip.TxPullRequests = &saturated
	if validateNodeNetworkDetail(d) == nil {
		t.Fatal("saturated pull-request count presented as known")
	}
	d.Gossip.TxPullRequests = &bytes
	for _, value := range []string{"", "01", "+1", "18446744073709551615", "18446744073709551616"} {
		d.Gossip.Decode.WireKinds[0] = &value
		if validateNodeNetworkDetail(d) == nil {
			t.Fatalf("noncanonical or saturated wire count accepted: %q", value)
		}
	}
	d.Gossip.Decode.WireKinds[0] = &count
	for _, malformed := range []string{
		strings.Replace(string(body), `"wire_kinds":[`, `"wire_kinds":[null,`, 1),
		strings.Replace(string(body), `"wire_kinds":["18446744073709551614",`, `"wire_kinds":[`, 1),
	} {
		if _, err := decodeNodeNetworkDetail([]byte(malformed)); err == nil {
			t.Fatal("wire counter array did not require exactly eight canonical entries")
		}
	}
	family, err := preparePublicNodeNetwork(d, at, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	total, largest := len(family.independentOnly)+len(family.tail), 0
	for _, body := range family.bodies {
		total += len(body)
		if len(body)+len(family.tail) > MaxNetworkDetailBytes {
			t.Fatal("complete public socket interval variant exceeded the document cap")
		}
		largest = max(largest, len(body)+len(family.tail))
		var public PublicNodeNetworkDetail
		if json.Unmarshal([]byte(body+family.tail), &public) != nil || public.Gossip == nil || public.Gossip.Socket == nil || public.TPU == nil || public.Votor == nil ||
			!reflect.DeepEqual(public.Gossip.Socket.Observation.DropWindow, socket.DropWindow) || !reflect.DeepEqual(public.TPU.Observation.DropWindow, socket.DropWindow) ||
			!reflect.DeepEqual(public.Votor.Observation.DropWindow, socket.DropWindow) || !reflect.DeepEqual(public.Gossip.Payload, d.Gossip.Payload) ||
			!reflect.DeepEqual(public.VotorPayload, d.VotorPayload) || !reflect.DeepEqual(public.LiveClock, d.LiveClock) || !reflect.DeepEqual(public.Gossip.Decode, d.Gossip.Decode) ||
			!reflect.DeepEqual(public.PrewarmHandover, d.PrewarmHandover) || !reflect.DeepEqual(public.TPUPayload, d.TPUPayload) ||
			!reflect.DeepEqual(public.TPUQueues, d.TPUQueues) ||
			public.Receiver == nil || !reflect.DeepEqual(public.Receiver.RepairRequests, d.Receiver.RepairRequests) {
			t.Fatal("public variant changed the full-width socket-drop intervals")
		}
	}
	total += len(family.extendedTail) + len(family.extendedIndependentOnly)
	if family.extendedTail == "" || family.extendedIndependentOnly == "" {
		t.Fatal("bounded maximum fixture lost the negotiated counter")
	}
	if total > networkDetailFamilyBytes {
		t.Fatal("Gossip exceeded the unchanged cached-family cap")
	}
	t.Logf("network plus Gossip private/public/family: %d/%d/%d bytes", len(body), len(family.bodies[0])+len(family.tail), total)
	t.Logf("largest complete public variant: %d/%d bytes", largest, MaxNetworkDetailBytes)
	for _, change := range []func(*VotorPayloadDetail){
		func(p *VotorPayloadDetail) { p.Scope = "wire_traffic" },
		func(p *VotorPayloadDetail) { p.Generation = 0 },
		func(p *VotorPayloadDetail) { p.Generation = 1 },
		func(p *VotorPayloadDetail) { p.Received, p.SendAccepted = nil, nil },
		func(p *VotorPayloadDetail) { p.Received = &ApplicationPayloadCounter{} },
		func(p *VotorPayloadDetail) {
			p.Received = &ApplicationPayloadCounter{StartedAt: at.Add(-time.Hour), SampledAt: at}
		},
		func(p *VotorPayloadDetail) {
			n := uint64(math.MaxUint64)
			p.Received = &ApplicationPayloadCounter{StartedAt: at, SampledAt: at, Bytes: &n}
		},
		func(p *VotorPayloadDetail) {
			p.Received = &ApplicationPayloadCounter{StartedAt: at, SampledAt: at.Add(2 * time.Second)}
		},
	} {
		candidate, payload := d, *d.VotorPayload
		candidate.VotorPayload = &payload
		change(&payload)
		if validateNodeNetworkDetail(candidate) == nil {
			t.Fatal("invalid Votor application payload accepted")
		}
	}
	for _, change := range []func(*GossipPayloadDetail){
		func(p *GossipPayloadDetail) { p.Scope = "wire_bandwidth" },
		func(p *GossipPayloadDetail) { p.SocketGeneration = 0 },
		func(p *GossipPayloadDetail) { p.SocketGeneration = 1 },
		func(p *GossipPayloadDetail) { p.StartedAt = time.Time{} },
		func(p *GossipPayloadDetail) { p.StartedAt = at.Add(-time.Second) },
		func(p *GossipPayloadDetail) { p.SampledAt = at.Add(2 * time.Second) },
		func(p *GossipPayloadDetail) { value := uint64(math.MaxUint64); p.ReceivedBytes = &value },
	} {
		candidate := d
		gossip, payload := *d.Gossip, *d.Gossip.Payload
		candidate.Gossip, gossip.Payload = &gossip, &payload
		change(&payload)
		if validateNodeNetworkDetail(candidate) == nil {
			t.Fatal("invalid payload scope, binding, date or saturated known total was accepted")
		}
	}
	d.Interfaces, d.InterfacesReason = nil, "not_provided"
	family, err = preparePublicNodeNetwork(d, at, at.Add(time.Second))
	if err != nil || family.independentOnly == "" {
		t.Fatal("independent Gossip depended on interface monitoring")
	}
	var public PublicNodeNetworkDetail
	if json.Unmarshal([]byte(family.independentOnly), &public) != nil || public.Gossip == nil || public.Receiver != nil || public.Interfaces != nil || public.Gossip.RxPings != math.MaxUint64 {
		t.Fatal("receiver clearing removed independent Gossip or changed its raw width")
	}
	d.Gossip.ClientGeneration = 0
	if validateNodeNetworkDetail(d) == nil {
		t.Fatal("unbound Gossip client generation accepted")
	}
}
