package node

import (
	"strconv"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

func (d *nodeDashboard) gossipObserver() func(*gossip.Client) {
	if d == nil || !d.networkDetails {
		return nil
	}
	return d.observeGossipClient
}

// observeGossipClient attaches the actual bootstrap/shared/stream client. It
// enables only the existing diagnostic cache, without sampling maps or counters.
func (d *nodeDashboard) observeGossipClient(client *gossip.Client) {
	if d == nil || client == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.networkDetails || d.networkRetired || d.state.Phase == "stopping" || d.gossipClient == client {
		return
	}
	if d.gossipGeneration == ^uint64(0) {
		d.gossipClient = nil
		return
	}
	client.EnableMonitoring()
	client.EnableSocketMemoryMonitoring()
	client.EnableUDPPayloadMonitoring()
	client.EnableDecodeMonitoring()
	if d.localPeers {
		client.EnablePeerDataset()
	}
	d.gossipGeneration++
	d.gossipClient = client
}

func gossipOperationMeasurement(s gossip.MonitoringSnapshot, generation uint64) *dashboard.GossipOperationDetail {
	if generation == 0 || s.StartedAt.IsZero() || s.SampledAt.IsZero() {
		return nil
	}
	c := s.Stats
	v := &dashboard.GossipOperationDetail{Scope: "gossip_client_totals", ClientGeneration: generation, StartedAt: s.StartedAt.UTC(), SampledAt: s.SampledAt.UTC(),
		RxPings: c.RxPings, RxPongs: c.RxPongs, RxContacts: c.RxContacts, RxPullRequests: c.RxPullRequests,
		TxPushMessages: c.TxPushMessages, TxPingMessages: c.TxPingMessages, TxPongMessages: c.TxPongMessages, TxPullResponses: c.TxPullResponses}
	if c.TxPullRequests != ^uint64(0) {
		value := c.TxPullRequests
		v.TxPullRequests = &value
	}
	if s.HasLocalShredVersion {
		version := s.LocalShredVersion
		v.LocalShredVersion = &version
	}
	if s.PayloadObserved {
		p := s.Payload
		v.Payload = &dashboard.GossipPayloadDetail{Scope: "gossip_run_udp_payload", SocketGeneration: p.Generation,
			StartedAt: p.StartedAt.UTC(), SampledAt: p.SampledAt.UTC()}
		if p.ReceivedKnown {
			n := p.ReceivedBytes
			v.Payload.ReceivedBytes = &n
		}
		if p.SentKnown {
			n := p.SentBytes
			v.Payload.SentBytes = &n
		}
		if w := p.Window; w.ReceivedKnown || w.SentKnown {
			v.Payload.Window = &dashboard.GossipPayloadInterval{StartedAt: w.StartedAt.UTC(), SampledAt: w.SampledAt.UTC(),
				DurationNanoseconds: uint64(w.Duration)}
			if w.ReceivedKnown {
				n := w.ReceivedBytes
				v.Payload.Window.ReceivedBytes = &n
			}
			if w.SentKnown {
				n := w.SentBytes
				v.Payload.Window.SentBytes = &n
			}
		}
	}
	v.Decode = gossipDecodeMeasurement(s)
	return v
}

func gossipDecodeMeasurement(s gossip.MonitoringSnapshot) *dashboard.GossipDecodeDetail {
	if !s.DecodeObserved {
		return nil
	}
	o := s.Decode
	if o.Generation == 0 || o.StartedAt.IsZero() || o.SampledAt.IsZero() || o.StartedAt.Before(s.StartedAt) ||
		o.SampledAt.Before(o.StartedAt) || !o.SampledAt.Equal(s.SampledAt) ||
		s.SocketObserved && o.Generation != s.SocketGeneration || s.PayloadObserved && o.Generation != s.Payload.Generation {
		return nil
	}
	v := &dashboard.GossipDecodeDetail{Scope: "gossip_run_decode", SocketGeneration: o.Generation, StartedAt: o.StartedAt.UTC()}
	for i, count := range o.WireKinds {
		if !count.Known && count.Value != 0 || count.Known && count.Value == ^uint64(0) {
			return nil
		}
		if count.Known {
			value := strconv.FormatUint(count.Value, 10)
			v.WireKinds[i] = &value
		}
	}
	targets := [...]**uint64{&v.AttemptedContactV2, &v.SignatureValid, &v.SignatureInvalid, &v.UnsupportedType, &v.MalformedValue, &v.UnparsedSuffix}
	counts := [...]gossip.DecodeCounter{o.AttemptedContactV2, o.SignatureValid, o.SignatureInvalid, o.UnsupportedType, o.MalformedValue, o.UnparsedSuffix}
	for i, count := range counts {
		if !count.Known && count.Value != 0 || count.Known && count.Value == ^uint64(0) {
			return nil
		}
		if count.Known {
			value := count.Value
			*targets[i] = &value
		}
	}
	return v
}

// captureGossipOperations reads only the selected client's immutable cache.
// Replay/source absence does not hide a still-running shared/bootstrap client.
func (d *nodeDashboard) captureGossipOperations(binding dashboard.NodeNetworkBinding, now time.Time) (*dashboard.GossipOperationDetail, time.Time) {
	d.mu.Lock()
	client, generation, retired := d.gossipClient, d.gossipGeneration, d.networkRetired
	d.mu.Unlock()
	if retired || client == nil {
		return nil, time.Time{}
	}
	s, ok := client.MonitoringSnapshot()
	if !ok || s.SampledAt.IsZero() || now.Before(s.SampledAt) || !s.SampledAt.Add(dashboard.NetworkDetailFreshFor).After(now) {
		return nil, time.Time{}
	}
	v := gossipOperationMeasurement(s, generation)
	if v == nil {
		return nil, time.Time{}
	}
	deadline := s.SampledAt.Add(dashboard.NetworkDetailFreshFor)
	if s.PayloadObserved {
		at := s.Payload.SampledAt
		if at.IsZero() || now.Before(at) || !at.Add(dashboard.NetworkDetailFreshFor).After(now) {
			return nil, time.Time{}
		}
		if until := at.Add(dashboard.NetworkDetailFreshFor); until.Before(deadline) {
			deadline = until
		}
	}
	if s.SocketObserved {
		if s.SocketGeneration == 0 {
			return nil, time.Time{}
		}
		drops := s.SocketDrops
		observation, until, err := socketDetailMeasurement(turbine.SocketMemorySample{Observation: s.SocketMemory,
			DropDelta: turbine.SocketDropDelta{Known: drops.Known, Drops: drops.Drops, StartedAt: drops.StartedAt, SampledAt: drops.SampledAt}}, true, s.SampledAt, now)
		if err != nil {
			return nil, time.Time{}
		}
		v.Socket = &dashboard.GossipSocketDetail{Scope: "gossip_udp_socket", Generation: s.SocketGeneration, Observation: observation}
		if until.Before(deadline) {
			deadline = until
		}
	}
	if v.Decode != nil {
		// A stable Client pointer can run again with a different socket. Recheck
		// the cached Run identity without refreshing this capture's dates.
		after, known := client.MonitoringSnapshot()
		if !known || !after.DecodeObserved || after.Decode.Generation != s.Decode.Generation ||
			!after.Decode.StartedAt.Equal(s.Decode.StartedAt) {
			return nil, time.Time{}
		}
	}
	d.mu.Lock()
	unchanged := !d.networkRetired && d.state.Phase != "stopping" && d.gossipClient == client && d.gossipGeneration == generation &&
		d.state.RunID == binding.RunID && d.state.StartedAt.Equal(binding.NodeStartedAt) && sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash)
	d.mu.Unlock()
	if !unchanged {
		return nil, time.Time{}
	}
	return v, deadline
}
