package blockstream

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

func TestNetworkMonitoringUsesBoundedImmutableObservations(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 100, DisableRPCBlockFetch: true})
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.FixedZone("offset", 19800))
	bs.startedAt = now
	stats := turbine.ReceiverStats{Packets: 1<<53 + 1, DataShreds: 3, CodingShreds: 4,
		ParseErrors: 5, SignatureErrors: 6, UsefulRepairShreds: 7, SpooledRepairShreds: 8,
		LastPacketUnix: now.Unix(), Repair: turbine.RepairStats{Requests: 9, Responses: 10, LateResponses: 11, Timeouts: 12, Errors: 13}}
	frontiers := turbine.ShredFrontiers{HasObserved: true, HasReconstructed: true, ObservedSlot: 123, ReconstructedSlot: 120}
	bs.publishTurbineObservation(now, now, 1, true, stats, frontiers, turbine.ReceiverSocketMemory{}, false)
	if bs.turbineObservation.Load() != nil {
		t.Fatal("disabled monitoring published a sample")
	}
	if _, ok := bs.NetworkSnapshot(); ok {
		t.Fatal("disabled source reported monitoring")
	}
	bs.EnableMonitoring()
	bs.liveStreamConnected.Store(true)
	bs.repairCatchupFrom.Store(100)
	bs.repairCatchupUntil.Store(200)
	s, ok := bs.NetworkSnapshot()
	if !ok || s.Turbine != nil || s.ConfiguredSource != "turbine" || s.NextSlot != 100 || s.RPCBlockFetchAllowed || s.LiveActive || !s.RepairCatchupPending || s.RepairCatchupFrom != 100 || s.RepairCatchupUntil != 200 {
		t.Fatalf("incorrect source configuration or fabricated initial sample: %+v", s)
	}
	bs.repairCatchupPending.Store(false)
	active, ok := bs.NetworkSnapshot()
	if !ok || active.RepairCatchupPending || active.RepairCatchupFrom != 100 || active.RepairCatchupUntil != 200 {
		t.Fatal("active repair state was conflated with the pending hold")
	}
	if !s.SourceStartedAt.Equal(now) || s.SourceStartedAt.Location() != time.UTC || s.SampledAt.Location() != time.UTC {
		t.Fatal("source timestamps were not normalized to UTC")
	}
	stats.BlocksEmitted, stats.RecoveredData, stats.AssemblyErrors, stats.ActiveSlots = 10, 11, 12, 13
	stats.MissingLeaders, stats.ShredVersionMismatch = 21, 1<<53+3
	stats.EvictedSlots, stats.IgnoredOldShreds, stats.SigVerifies, stats.SigVerifyCached = 14, 15, 16, 17
	stats.Repair.Outstanding, stats.Repair.Peers, stats.Repair.RespondingPeers = 18, 19, 20
	stats.RetransmitEnabled, stats.Retransmit.SentPayloadBytes = true, 1<<53+2
	bs.publishTurbineObservation(now, now.Add(-time.Minute), 1, true, stats, frontiers, turbine.ReceiverSocketMemory{}, false)
	s, _ = bs.NetworkSnapshot()
	want := TurbineObservation{SampledAt: now.UTC(), StartedAt: now.Add(-time.Minute).UTC(), Session: 1,
		Packets: stats.Packets, DataShreds: 3, CodingShreds: 4, ParseErrors: 5, SignatureErrors: 6, MissingLeaders: 21, ShredVersionMismatch: 1<<53 + 3,
		UsefulRepairShreds: 7, SpooledRepairShreds: 8, LastPacketAt: time.Unix(now.Unix(), 0).UTC(), RepairEnabled: true,
		RepairRequests: 9, RepairResponses: 10, RepairLateResponses: 11, RepairTimeouts: 12, RepairErrors: 13,
		Frontiers: frontiers, BlocksEmitted: 10, RecoveredData: 11, AssemblyErrors: 12, ActiveSlots: 13,
		EvictedSlots: 14, IgnoredOldShreds: 15, SigVerifies: 16, SigVerifyCached: 17,
		RepairOutstanding: 18, RepairPeers: 19, RepairResponding: 20,
		RetransmitEnabled: true, PeerPayloadBytes: 1<<53 + 2}
	if s.Turbine == nil || *s.Turbine != want {
		t.Fatalf("receiver projection: got %+v, want %+v", s.Turbine, want)
	}
	s.Turbine.Packets = 0
	s.Turbine.Frontiers.ObservedSlot = 0
	s.Turbine.PeerPayloadBytes = 0
	if next, _ := bs.NetworkSnapshot(); next.Turbine.PeerPayloadBytes != stats.Retransmit.SentPayloadBytes || !next.Turbine.SampledAt.Equal(now) {
		t.Fatal("viewer changed or redated cached peer payload bytes")
	}
	if next, _ := bs.NetworkSnapshot(); next.Turbine.Packets != stats.Packets {
		t.Fatal("caller mutated the cached sample")
	}
	if next, _ := bs.NetworkSnapshot(); next.Turbine.Frontiers != frontiers {
		t.Fatal("caller mutated the cached frontiers")
	}
	bs.handleLiveShredStreamClosed("")
	if next, _ := bs.NetworkSnapshot(); next.LiveConnected || next.Turbine != nil {
		t.Fatal("disconnected receiver retained an available sample")
	}
	bs.liveStreamConnected.Store(true)
	if next, _ := bs.NetworkSnapshot(); next.Turbine != nil {
		t.Fatal("reconnected receiver inherited the previous session")
	}
	bs.publishTurbineObservation(now.Add(time.Minute), now.Add(time.Minute), 2, false, turbine.ReceiverStats{}, turbine.ShredFrontiers{}, turbine.ReceiverSocketMemory{}, false)
	if next, _ := bs.NetworkSnapshot(); next.Turbine.RetransmitEnabled || next.Turbine.PeerPayloadBytes != 0 {
		t.Fatal("replacement receiver inherited relay presence or bytes")
	}
	if next, _ := bs.NetworkSnapshot(); next.Turbine.Session != 2 || next.Turbine.Packets != 0 || next.Turbine.RepairEnabled || !next.Turbine.LastPacketAt.IsZero() || next.Turbine.Frontiers.HasObserved || next.Turbine.Frontiers.HasReconstructed || next.Turbine.BlocksEmitted != 0 || next.Turbine.RepairOutstanding != 0 {
		t.Fatalf("session reset retained data: %+v", next.Turbine)
	}
	bs.Stop()
	if _, ok := bs.NetworkSnapshot(); ok {
		t.Fatal("stopped source remained available")
	}
}

func TestNetworkMonitoringConcurrentPublication(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 1})
	bs.EnableMonitoring()
	bs.liveStreamConnected.Store(true)
	now := time.Now()
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 1000 {
				bs.NetworkSnapshot()
			}
		})
	}
	for i := range 1000 {
		bs.setMonitoredGossip(&gossip.Client{})
		bs.publishTurbineObservation(now, now, 1, true, turbine.ReceiverStats{Packets: uint64(i)}, turbine.ShredFrontiers{HasObserved: true, ObservedSlot: uint64(i)}, turbine.ReceiverSocketMemory{}, false)
	}
	readers.Wait()
}

func TestNetworkMonitoringCopiesReceiverSocketMemory(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 1})
	if got, ok := bs.observeTurbineSocketMemory(nil); ok || got != (turbine.ReceiverSocketMemory{}) {
		t.Fatalf("disabled monitoring observed a socket: %+v, %t", got, ok)
	}
	bs.EnableMonitoring()
	bs.liveStreamConnected.Store(true)
	at := time.Now()
	socketMemory := turbine.ReceiverSocketMemory{Generation: 7, RepairEnabled: true,
		Live: turbine.SocketMemorySample{Observation: metrics.SocketObservation{StartedAt: at, SampledAt: at, Known: true,
			Reason: metrics.SocketAvailable, Memory: metrics.SocketMemory{ReceiveAllocated: 11, Drops: 12}},
			DropDelta: turbine.SocketDropDelta{Known: true, Drops: 2, StartedAt: at.Add(-time.Second), SampledAt: at}},
		Repair: turbine.SocketMemorySample{Observation: metrics.SocketObservation{StartedAt: at, SampledAt: at, Known: true,
			Reason: metrics.SocketAvailable, Memory: metrics.SocketMemory{ReceiveAllocated: 21, Drops: 22}}}}
	bs.publishTurbineObservation(at, at, 1, true, turbine.ReceiverStats{}, turbine.ShredFrontiers{}, socketMemory, true)
	snapshot, ok := bs.NetworkSnapshot()
	if !ok || snapshot.Turbine == nil || !snapshot.Turbine.SocketMemoryObserved || snapshot.Turbine.SocketMemory != socketMemory || snapshot.Turbine.SocketMemoryObservedAt != at {
		t.Fatalf("socket observation = %+v, %t", snapshot.Turbine, ok)
	}
	snapshot.Turbine.SocketMemory.Live.Observation.Memory.Drops = 0
	snapshot.Turbine.SocketMemory.Live.DropDelta.Drops = 0
	if next, _ := bs.NetworkSnapshot(); next.Turbine.SocketMemory.Live.Observation.Memory.Drops != 12 || next.Turbine.SocketMemory.Live.DropDelta.Drops != 2 {
		t.Fatal("caller mutated the cached socket observation")
	}

	failed := turbine.ReceiverSocketMemory{Generation: 7,
		Live: turbine.SocketMemorySample{Observation: metrics.SocketObservation{StartedAt: at, SampledAt: at, Reason: metrics.SocketReadFailed}}}
	bs.publishTurbineObservation(at.Add(time.Second), at, 1, true, turbine.ReceiverStats{}, turbine.ShredFrontiers{}, failed, true)
	if next, _ := bs.NetworkSnapshot(); !next.Turbine.SocketMemoryObserved || next.Turbine.SocketMemory.Live.Observation.Known || next.Turbine.SocketMemory.Live.DropDelta.Known {
		t.Fatalf("failed observation retained known state: %+v", next.Turbine.SocketMemory)
	}
	bs.publishTurbineObservation(at.Add(2*time.Second), at, 1, false, turbine.ReceiverStats{}, turbine.ShredFrontiers{}, turbine.ReceiverSocketMemory{}, false)
	if next, _ := bs.NetworkSnapshot(); next.Turbine.SocketMemoryObserved || !next.Turbine.SocketMemoryObservedAt.IsZero() {
		t.Fatal("unavailable capture retained an owner clock")
	}
}

func TestReceptionMonitoringPublishesDetachedSessionSample(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 1})
	at := time.Now().UTC()
	progress := turbine.ReceptionSnapshot{SampledAt: at, Slot: 1<<53 + 1, ObservedAt: at, HasSlot: true, RetainedSlots: 3}
	bs.publishReceptionObservation(at, 1, progress, nil)
	if bs.receptionObservation.Load() != nil {
		t.Fatal("disabled monitoring captured reception")
	}
	if _, _, ok := bs.CachedReceptionObservation(); ok {
		t.Fatal("disabled source returned a reception observation")
	}
	bs.EnableMonitoring()
	bs.liveStreamConnected.Store(true)
	bs.publishReceptionObservation(at, 1, progress, nil)
	sourceStart, cached, known := bs.CachedReceptionObservation()
	if !known || sourceStart.IsZero() || cached.Progress != progress || cached.Session != 1 {
		t.Fatal("cached reception lost its exact source/session")
	}
	cached.Progress.Slot = 0
	if _, next, ok := bs.CachedReceptionObservation(); !ok || next.Progress.Slot != progress.Slot {
		t.Fatal("cached reception aliases its owner")
	}
	if n := testing.AllocsPerRun(1000, func() { bs.CachedReceptionObservation() }); n != 0 {
		t.Fatalf("cached reception read allocated: %v", n)
	}
	s, ok := bs.NetworkSnapshot()
	if !ok || s.Reception == nil || s.Reception.Progress != progress || s.Reception.Session != 1 || !s.Reception.StartedAt.Equal(at) || s.Turbine != nil {
		t.Fatalf("independent reception sample: %+v", s)
	}
	s.Reception.Progress.Slot = 0
	if next, _ := bs.NetworkSnapshot(); next.Reception.Progress.Slot != progress.Slot {
		t.Fatal("caller changed the cached reception")
	}
	bs.handleLiveShredStreamClosed("")
	if _, _, ok := bs.CachedReceptionObservation(); ok {
		t.Fatal("disconnected source returned old reception")
	}
	if bs.receptionObservation.Load() != nil {
		t.Fatal("disconnection retained the old session cache")
	}
	bs.liveStreamConnected.Store(true)
	if next, _ := bs.NetworkSnapshot(); next.Reception != nil {
		t.Fatal("replacement receiver inherited reception")
	}
	progress.Slot, progress.HasSlot, progress.RetainedSlots = 0, false, 0
	bs.publishReceptionObservation(at.Add(time.Second), 2, progress, nil)
	if next, _ := bs.NetworkSnapshot(); next.Reception.Session != 2 || next.Reception.Progress.HasSlot {
		t.Fatal("new session fabricated a received slot")
	}
	bs.Stop()
	if _, _, ok := bs.CachedReceptionObservation(); ok {
		t.Fatal("stopped source returned reception")
	}
	if _, ok := bs.NetworkSnapshot(); ok {
		t.Fatal("stopped source remained available")
	}
}

func TestReceptionMonitoringUsesActualStreamOwner(t *testing.T) {
	history, err := turbine.NewShredHistory(turbine.ShredHistoryBinding{RunID: "actual-stream-history",
		NodeStartedAt: time.Now(), GenesisHash: solana.Hash{1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { history.CloseAfterProducers() })
	for _, kind := range []turbine.ShredHistorySourceKind{turbine.ShredHistoryPrewarm, turbine.ShredHistoryLive} {
		handle, ok := history.RegisterSource(kind, time.Now())
		if !ok {
			t.Fatal("prior receiver registration refused")
		}
		handle.Retire()
	}
	bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 1,
		TurbineBindAddr: "127.0.0.1:0", ShredHistory: history})
	bs.EnableMonitoring()
	bs.liveStreamWg.Add(1)
	done := make(chan struct{})
	go func() { bs.runTurbineStream(); close(done) }()
	t.Cleanup(func() {
		bs.Stop()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("monitored stream did not stop")
		}
	})
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal("no reception publication from the one-second owner cycle")
		case <-poll.C:
			s, ok := bs.NetworkSnapshot()
			if !ok || s.Reception == nil {
				continue
			}
			r := s.Reception
			if r.Session != 1 || r.Progress.SampledAt.Before(r.StartedAt) || r.StartedAt.Before(s.SourceStartedAt) || r.Progress.HasSlot || r.Progress.RetainedSlots != 0 {
				t.Fatalf("invalid owner publication: %+v", r)
			}
			if r.ShredHistoryTarget().Generation() != 3 {
				t.Fatal("actual constructor equated history generation with successful session")
			}
			// Recent reception is independently sampled before the first normal
			// ten-second aggregate diagnostic observation.
			if s.Turbine != nil {
				t.Fatal("reception unexpectedly waits for or fabricates receiver counters")
			}
			return
		}
	}
}

func TestReceptionHistoryTargetRetainsOriginalGeneration(t *testing.T) {
	history, err := turbine.NewShredHistory(turbine.ShredHistoryBinding{RunID: "cached-history-target",
		NodeStartedAt: time.Now(), GenesisHash: solana.Hash{1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { history.CloseAfterProducers() })
	old, ok := history.RegisterSource(turbine.ShredHistoryLive, time.Now())
	if !ok {
		t.Fatal("initial receiver registration refused")
	}
	bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 1})
	bs.EnableMonitoring()
	bs.liveStreamConnected.Store(true)
	bs.publishReceptionObservation(time.Now(), 9, turbine.ReceptionSnapshot{}, old)
	_, cached, known := bs.CachedReceptionObservation()
	if !known || cached.ShredHistoryTarget() != old || cached.Session == old.Generation() {
		t.Fatal("cached observation lost original receiver identity")
	}
	old.Retire()
	replacement, ok := history.RegisterSource(turbine.ShredHistoryLive, time.Now())
	if !ok {
		t.Fatal("replacement receiver registration refused")
	}
	bs.publishReceptionObservation(time.Now(), 10, turbine.ReceptionSnapshot{}, replacement)
	_, current, known := bs.CachedReceptionObservation()
	if !known || current.ShredHistoryTarget() != replacement || cached.ShredHistoryTarget() != old {
		t.Fatal("replacement rewrote a detached old marker target")
	}
	body, err := json.Marshal(cached)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || len(fields) != 4 {
		t.Fatalf("private history target changed the public observation: %s, %v", body, err)
	}
	bs.handleLiveShredStreamClosed("")
	if _, _, ok := bs.CachedReceptionObservation(); ok {
		t.Fatal("disconnected source retained its marker target")
	}
	bs.Stop()
}

func TestNetworkGossipMonitoringTracksActualStreamClient(t *testing.T) {
	for _, shared := range []bool{false, true} {
		name := "owned"
		if shared {
			name = "shared"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = peer.Close() })
			// Reserve an ephemeral loopback address for the real stream receiver.
			tvu, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			tvuAddr := tvu.LocalAddr().String()
			if err := tvu.Close(); err != nil {
				t.Fatal(err)
			}
			opts := &BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 1,
				TurbineBindAddr: tvuAddr, TurbineGossipEntrypoint: peer.LocalAddr().String(),
				TurbineGossipBindAddr: "127.0.0.1:0", TurbineAdvertisedIP: "127.0.0.1", TurbineShredVersion: 42}
			if shared {
				client, err := gossip.NewClient(gossip.Config{Entrypoint: peer.LocalAddr().String(), BindAddr: "127.0.0.1:0",
					TVUAddr: tvuAddr, AdvertisedIP: "127.0.0.1", ShredVersion: 42})
				if err != nil {
					t.Fatal(err)
				}
				opts.GossipClient = client
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- client.Run(ctx) }()
				t.Cleanup(func() {
					cancel()
					select {
					case err := <-done:
						if err != nil {
							t.Errorf("shared gossip stopped: %v", err)
						}
					case <-time.After(3 * time.Second):
						t.Error("shared gossip did not stop")
					}
				})
			}
			bs := NewBlockSource(opts)
			if s, ok := bs.NetworkSnapshot(); ok || s.Gossip != nil {
				t.Fatal("disabled monitoring exposed gossip")
			}
			bs.setMonitoredGossip(&gossip.Client{})
			if bs.monitoredGossip.Load() != nil {
				t.Fatal("disabled source retained a gossip monitor")
			}
			bs.EnableMonitoring()
			var attached atomic.Pointer[gossip.Client]
			bs.SetGossipObserver(func(client *gossip.Client) { attached.Store(client) })
			if s, _ := bs.NetworkSnapshot(); s.Gossip != nil {
				t.Fatal("source fabricated gossip before stream startup")
			}
			bs.liveStreamWg.Add(1)
			streamDone := make(chan struct{})
			go func() { bs.runTurbineStream(); close(streamDone) }()
			t.Cleanup(func() {
				bs.Stop()
				select {
				case <-streamDone:
				case <-time.After(3 * time.Second):
					t.Error("turbine stream did not stop")
				}
			})
			deadline := time.NewTimer(15 * time.Second)
			defer deadline.Stop()
			poll := time.NewTicker(10 * time.Millisecond)
			defer poll.Stop()
			var sample NetworkSnapshot
			for sample.Gossip == nil {
				select {
				case <-deadline.C:
					t.Fatal("no cached gossip observation from the real diagnostic cycle")
				case <-poll.C:
					sample, _ = bs.NetworkSnapshot()
				}
			}
			client := bs.monitoredGossip.Load()
			if client == nil || attached.Load() != client || (shared && client != opts.GossipClient) || (!shared && bs.gossipClient != nil) {
				t.Fatal("monitor did not use the stream's selected gossip owner")
			}
			cached, ok := client.MonitoringSnapshot()
			if !ok || *sample.Gossip != cached || cached.StartedAt.IsZero() || cached.SampledAt.IsZero() || cached.Stats.TxPackets == 0 {
				t.Fatalf("gossip cache not copied: got %+v, want %+v", sample.Gossip, cached)
			}
			sample.Gossip.Stats.TxPackets = 0
			if next, _ := bs.NetworkSnapshot(); next.Gossip == nil || next.Gossip.Stats.TxPackets == 0 {
				t.Fatal("reader mutated the source's cached observation")
			}
			bs.handleLiveShredStreamClosed("")
			if next, _ := bs.NetworkSnapshot(); next.Gossip != nil || bs.monitoredGossip.Load() != nil {
				t.Fatal("closed stream retained the gossip reference")
			}
			bs.liveStreamConnected.Store(true)
			if next, _ := bs.NetworkSnapshot(); next.Gossip != nil {
				t.Fatal("replacement receiver inherited the previous gossip reference")
			}
			bs.setMonitoredGossip(&gossip.Client{})
			if next, _ := bs.NetworkSnapshot(); next.Gossip != nil {
				t.Fatal("unsampled replacement client inherited prior instance counters")
			}
			bs.Stop()
			if _, ok := bs.NetworkSnapshot(); ok || bs.monitoredGossip.Load() != nil {
				t.Fatal("stopped source retained available gossip")
			}
		})
	}
}

func TestNetworkGossipObserverDisabledNilAndStopped(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceTurbine, DisableRPCBlockFetch: true})
	t.Cleanup(bs.Stop)
	var attached []*gossip.Client
	bs.SetGossipObserver(func(client *gossip.Client) { attached = append(attached, client) })
	client := &gossip.Client{}
	bs.setMonitoredGossip(client)
	if len(attached) != 0 {
		t.Fatal("disabled monitoring invoked an observer")
	}
	bs.EnableMonitoring()
	bs.setMonitoredGossip(client)
	bs.setMonitoredGossip(nil)
	if len(attached) != 1 || attached[0] != client {
		t.Fatal("selection lost its actual owner or sent a nil detach")
	}
	bs.Stop()
	bs.setMonitoredGossip(&gossip.Client{})
	if len(attached) != 1 {
		t.Fatal("stopped source attached another owner")
	}
}
