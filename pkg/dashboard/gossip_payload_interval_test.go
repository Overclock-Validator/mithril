package dashboard

import (
	"encoding/json"
	"testing"
	"time"
)

func TestGossipPayloadIntervalValidation(t *testing.T) {
	at := time.Now().UTC()
	d := maximumNodeNetwork(at)
	rx, tx := uint64(100), uint64(0)
	d.Gossip = &GossipOperationDetail{Scope: "gossip_client_totals", ClientGeneration: 1, StartedAt: at.Add(-time.Minute), SampledAt: at,
		Payload: &GossipPayloadDetail{Scope: "gossip_run_udp_payload", SocketGeneration: 1, StartedAt: at.Add(-time.Minute), SampledAt: at,
			ReceivedBytes: &rx, SentBytes: &tx}}
	p := d.Gossip.Payload
	w := GossipPayloadInterval{StartedAt: at.Add(-time.Second), SampledAt: at, DurationNanoseconds: uint64(time.Second), ReceivedBytes: &rx, SentBytes: &tx}
	for _, variant := range []GossipPayloadInterval{w, {StartedAt: w.StartedAt, SampledAt: at, DurationNanoseconds: 1, SentBytes: &tx}} {
		p.Window = &variant
		body, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeNodeNetworkDetail(body); err != nil {
			t.Fatalf("valid interval rejected: %v", err)
		}
	}
	for _, change := range []func(*GossipPayloadInterval){
		func(v *GossipPayloadInterval) { v.DurationNanoseconds = 0 },
		func(v *GossipPayloadInterval) { v.DurationNanoseconds = uint64(NetworkDetailFreshFor) + 1 },
		func(v *GossipPayloadInterval) { v.StartedAt = time.Time{} },
		func(v *GossipPayloadInterval) { v.SampledAt = at.Add(time.Nanosecond) },
		func(v *GossipPayloadInterval) { v.ReceivedBytes, v.SentBytes = nil, nil },
		func(v *GossipPayloadInterval) { n := rx + 1; v.ReceivedBytes = &n },
		func(v *GossipPayloadInterval) { n := uint64(1); v.SentBytes = &n },
	} {
		candidate := w
		change(&candidate)
		p.Window = &candidate
		if validateNodeNetworkDetail(d) == nil {
			t.Fatal("invalid interval accepted")
		}
	}
	p.Window = &w
	p.ReceivedBytes = nil
	if validateNodeNetworkDetail(d) == nil {
		t.Fatal("unknown total accepted a known interval delta")
	}
	p.Window = nil
	if validateNodeNetworkDetail(d) != nil {
		t.Fatal("older payload without interval became invalid")
	}
}
