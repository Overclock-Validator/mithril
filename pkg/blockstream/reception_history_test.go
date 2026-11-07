package blockstream

import (
	"crypto/ed25519"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

func newReceptionHistory(t *testing.T) *turbine.ShredHistory {
	t.Helper()
	h, err := turbine.NewShredHistory(turbine.ShredHistoryBinding{
		RunID: t.Name(), NodeStartedAt: time.Now(), GenesisHash: solana.Hash{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Interrupt)
	return h
}

func receptionHistorySocket(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func receptionHistoryPacket(t *testing.T, slot uint64) ([]byte, solana.PublicKey, solana.Hash) {
	t.Helper()
	var seed [ed25519.SeedSize]byte
	seed[0] = 1
	leader := solana.PrivateKey(ed25519.NewKeyFromSeed(seed[:]))
	g := turbine.ShredGenerator{Slot: slot, ParentSlot: slot - 1, Version: 7}
	packets, root, _, _, err := g.MakeShredsFromData(leader, nil, false, solana.Hash{1}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return packets[0], leader.PublicKey(), root
}

func sendReceptionHistoryPacket(t *testing.T, addr string, packet []byte) {
	t.Helper()
	sender, err := net.DialUDP("udp4", nil, mustResolveUDPAddr(t, addr))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if err := sender.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := sender.Write(packet); err != nil || n != len(packet) {
		t.Fatalf("send admitted fixture: bytes=%d, err=%v", n, err)
	}
}

func waitReceptionHistory(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for reception history lifecycle")
}

func receptionHistoryRows(h *turbine.ShredHistory) []turbine.ShredHistoryRow {
	s, _ := h.Snapshot()
	var rows []turbine.ShredHistoryRow
	for _, chunk := range s.Chunks {
		rows = append(rows, chunk.Rows()...)
	}
	return rows
}

func TestReceptionHistoryPrewarmLifecycle(t *testing.T) {
	for _, handover := range []bool{false, true} {
		name := "stop"
		if handover {
			name = "handover"
		}
		t.Run(name, func(t *testing.T) {
			h := newReceptionHistory(t)
			peer, tvu := receptionHistorySocket(t), receptionHistorySocket(t)
			bind := tvu.LocalAddr().String()
			if err := tvu.Close(); err != nil {
				t.Fatal(err)
			}
			packet, leader, root := receptionHistoryPacket(t, 42)
			before := time.Now()
			pw, err := StartTurbinePrewarm(TurbinePrewarmConfig{
				BindAddr: bind, GossipEntrypoint: peer.LocalAddr().String(), GossipBindAddr: "127.0.0.1:0",
				AdvertisedIP: "127.0.0.1", ShredVersion: 7, ShredHistory: h,
				LeaderForSlot: func(uint64) (solana.PublicKey, bool) { return leader, true },
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pw.Stop)
			sendReceptionHistoryPacket(t, bind, packet)
			waitReceptionHistory(t, func() bool { return len(receptionHistoryRows(h)) == 1 })
			if handover {
				blocks, dropped := pw.Handover()
				if len(blocks) != 0 || dropped != 0 {
					t.Fatal("one partial shred unexpectedly completed or evicted a block")
				}
			} else {
				pw.Stop()
			}
			pw.Stop()
			select {
			case <-pw.runDone:
			default:
				t.Fatal("prewarm returned before receiver Run joined")
			}
			if !h.CloseAfterProducers() {
				t.Fatal("prewarm interrupted the shared history owner")
			}
			s, _ := h.Snapshot()
			rows := receptionHistoryRows(h)
			if len(s.Sources) != 1 || !s.Sources[0].Retired || s.Sources[0].Generation != 1 || s.Sources[0].Kind != turbine.ShredHistoryPrewarm {
				t.Fatalf("prewarm source not retired: %+v", s.Sources)
			}
			if s.Sources[0].StartedAt.Before(before) || s.Sources[0].StartedAt == s.Sources[0].StartedAt.Round(0) {
				t.Fatal("prewarm source lost its original constructor clock")
			}
			if len(rows) != 1 || rows[0].Root != root || rows[0].SourceGeneration != 1 || rows[0].Slot != 42 || rows[0].Origin != turbine.ShredHistoryTVU || rows[0].At.Before(s.Sources[0].StartedAt) || rows[0].At == rows[0].At.Round(0) {
				t.Fatalf("wrong original prewarm receipt: %+v", rows)
			}
		})
	}
}

func TestReceptionHistoryPrewarmFailure(t *testing.T) {
	for _, beforeRun := range []bool{true, false} {
		name := "bind_failure"
		if beforeRun {
			name = "setup_failure"
		}
		t.Run(name, func(t *testing.T) {
			h := newReceptionHistory(t)
			peer, occupied := receptionHistorySocket(t), receptionHistorySocket(t)
			cfg := TurbinePrewarmConfig{BindAddr: occupied.LocalAddr().String(), GossipEntrypoint: peer.LocalAddr().String(),
				GossipBindAddr: "127.0.0.1:0", AdvertisedIP: "127.0.0.1", ShredVersion: 7, ShredHistory: h}
			if beforeRun {
				cfg.GossipEntrypoint = "["
			}
			pw, err := StartTurbinePrewarm(cfg)
			if err == nil || pw != nil {
				if pw != nil {
					pw.Stop()
				}
				t.Fatal("failed setup unexpectedly started a collector")
			}
			// Failure must retire only its receiver, not close the shared owner.
			next, ok := h.RegisterSource(turbine.ShredHistoryLive, time.Now())
			if !ok {
				t.Fatal("failed prewarm closed the shared owner")
			}
			want := uint64(2)
			if beforeRun {
				want = 1
			}
			if next.Generation() != want {
				t.Fatalf("next generation=%d, want %d", next.Generation(), want)
			}
			next.Retire()
			if !h.CloseAfterProducers() {
				t.Fatal("history drain was interrupted")
			}
			s, _ := h.Snapshot()
			if len(s.Sources) != int(want) || len(s.Chunks) != 0 {
				t.Fatalf("unexpected failed-start history: %+v", s)
			}
			for _, source := range s.Sources {
				if !source.Retired {
					t.Fatal("failed prewarm returned before source retirement")
				}
			}
		})
	}
}

func TestReceptionHistoryPrewarmStopWaitsForRun(t *testing.T) {
	// Force the narrow gap after drainer closure but before Run retirement.
	// The real constructor/UDP path is covered separately above.
	pw := observationPrewarm(0, 1, nil)
	pw.runDone = make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(pw.runDone) }) })
	done := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func() { pw.Stop(); done <- struct{}{} }()
	}
	select {
	case <-done:
		t.Fatal("Stop used drainer closure as a receiver retirement witness")
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(pw.runDone) })
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Stop did not finish after receiver retirement")
		}
	}
}

func TestReceptionHistoryLiveRetries(t *testing.T) {
	h := newReceptionHistory(t)
	peer, reserved := receptionHistorySocket(t), receptionHistorySocket(t)
	bind := reserved.LocalAddr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	pw, err := StartTurbinePrewarm(TurbinePrewarmConfig{BindAddr: bind, GossipEntrypoint: peer.LocalAddr().String(),
		GossipBindAddr: "127.0.0.1:0", AdvertisedIP: "127.0.0.1", ShredVersion: 7, ShredHistory: h})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pw.Stop)
	if blocks, dropped := pw.Handover(); len(blocks) != 0 || dropped != 0 {
		t.Fatal("empty prewarm fabricated blocks")
	}
	// Reuse the same owner and address, first forcing a real readiness failure.
	occupied, err := net.ListenUDP("udp4", mustResolveUDPAddr(t, bind))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })
	firstPacket, leader, firstRoot := receptionHistoryPacket(t, 42)
	opts := &BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 1, EndSlot: 100,
		TurbineBindAddr: bind, TurbineShredVersion: 7, ShredHistory: h,
		LeaderForSlot: func(uint64) (solana.PublicKey, bool) { return leader, true }}
	bs := NewBlockSource(opts)
	opts.ShredHistory = nil // The source retains its startup owner, not the options.
	bs.liveStreamWg.Add(1)
	done := make(chan struct{})
	go func() { bs.runTurbineStream(); close(done) }()
	t.Cleanup(func() {
		bs.Stop()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("live receiver did not join")
		}
	})
	waitReceptionHistory(t, func() bool {
		s, _ := h.Snapshot()
		return len(s.Sources) > 1 && s.Sources[len(s.Sources)-1].Retired
	})
	if err := occupied.Close(); err != nil {
		t.Fatal(err)
	}
	waitReceptionHistory(t, bs.liveStreamConnected.Load)
	sendReceptionHistoryPacket(t, bind, firstPacket)
	waitReceptionHistory(t, func() bool { return len(receptionHistoryRows(h)) == 1 })
	first := receptionHistoryRows(h)[0]
	if first.SourceGeneration < 3 || first.Root != firstRoot {
		t.Fatalf("readiness retry lost its own generation: %+v", first)
	}
	nextGeneration := first.SourceGeneration + 1
	if !bs.requestLiveStreamReconnect("reception history test") {
		t.Fatal("actual stream reconnect was refused")
	}
	waitReceptionHistory(t, func() bool {
		s, _ := h.Snapshot()
		return uint64(len(s.Sources)) == nextGeneration && s.Sources[len(s.Sources)-2].Retired && bs.liveStreamConnected.Load()
	})
	secondPacket, _, secondRoot := receptionHistoryPacket(t, 43)
	sendReceptionHistoryPacket(t, bind, secondPacket)
	waitReceptionHistory(t, func() bool { return len(receptionHistoryRows(h)) == 2 })
	bs.Stop()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("live stream did not join before history drain")
	}
	if !h.CloseAfterProducers() {
		t.Fatal("retry interrupted the shared history owner")
	}
	s, _ := h.Snapshot()
	rows := receptionHistoryRows(h)
	if len(rows) != 2 || rows[0] != first || rows[1].SourceGeneration != nextGeneration || rows[1].Root != secondRoot || rows[1].Slot != 43 {
		t.Fatalf("retry changed prior history or relabeled new receipt: %+v", rows)
	}
	if s.Binding.RunID != t.Name() || uint64(len(s.Sources)) != nextGeneration || s.Interrupted || s.QueueRefusals.Value != 0 {
		t.Fatalf("retry lost shared run binding or coverage: %+v", s)
	}
	for i, source := range s.Sources {
		kind := turbine.ShredHistoryLive
		if i == 0 {
			kind = turbine.ShredHistoryPrewarm
		}
		if source.Kind != kind || source.Generation != uint64(i+1) || !source.Retired || source.StartedAt == source.StartedAt.Round(0) {
			t.Fatalf("wrong live receiver lifetime: %+v", source)
		}
	}
}

func TestReceptionHistoryDisabledAndClosed(t *testing.T) {
	for _, closed := range []bool{false, true} {
		name := "nil"
		if closed {
			name = "closed"
		}
		t.Run(name, func(t *testing.T) {
			var h *turbine.ShredHistory
			var before turbine.ShredHistorySnapshot
			if closed {
				h = newReceptionHistory(t)
				h.CloseAfterProducers()
				before, _ = h.Snapshot()
			}
			peer, tvu := receptionHistorySocket(t), receptionHistorySocket(t)
			bind := tvu.LocalAddr().String()
			if err := tvu.Close(); err != nil {
				t.Fatal(err)
			}
			packet, leader, _ := receptionHistoryPacket(t, 42)
			pw, err := StartTurbinePrewarm(TurbinePrewarmConfig{BindAddr: bind, GossipEntrypoint: peer.LocalAddr().String(),
				GossipBindAddr: "127.0.0.1:0", AdvertisedIP: "127.0.0.1", ShredVersion: 7, ShredHistory: h,
				LeaderForSlot: func(uint64) (solana.PublicKey, bool) { return leader, true }})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pw.Stop)
			sendReceptionHistoryPacket(t, bind, packet)
			waitForBlockSourceCondition(t, func() bool { return pw.receiver.Stats().DataShreds == 1 })
			blocks, dropped := pw.Handover()
			if len(blocks) != 0 || dropped != 0 || (pw.runDone == nil) != !closed {
				t.Fatal("nil/refused history changed prewarm output or allocated a disabled join signal")
			}
			bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceTurbine, StartSlot: 1,
				TurbineBindAddr: bind, TurbineShredVersion: 7, ShredHistory: h,
				LeaderForSlot: func(uint64) (solana.PublicKey, bool) { return leader, true }})
			bs.liveStreamWg.Add(1)
			done := make(chan struct{})
			go func() { bs.runTurbineStream(); close(done) }()
			t.Cleanup(func() {
				bs.Stop()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("uninstrumented receiver did not join")
				}
			})
			waitReceptionHistory(t, bs.liveStreamConnected.Load)
			sendReceptionHistoryPacket(t, bind, packet)
			waitForBlockSourceCondition(t, func() bool {
				bs.alpenglowMu.Lock()
				r := bs.activeTurbineReceiver
				bs.alpenglowMu.Unlock()
				return r != nil && r.Stats().DataShreds == 1
			})
			bs.Stop()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("uninstrumented receiver remained active")
			}
			if after, ok := h.Snapshot(); ok != closed || !reflect.DeepEqual(after, before) {
				t.Fatal("nil/refused history acquired or changed a source")
			}
		})
	}
}

func TestReceptionHistoryNonTurbineDoesNotRegister(t *testing.T) {
	h := newReceptionHistory(t)
	for _, sourceType := range []BlockSourceType{BlockSourceRpc, BlockSourceFile, BlockSourceLightbringer} {
		bs := NewBlockSource(&BlockSourceOpts{SourceType: sourceType, StartSlot: 1, ShredHistory: h})
		bs.maybeStartLightbringerStream() // No live endpoint; no network or worker.
		if bs.liveStreamStarted.Load() {
			t.Fatal("non-turbine probe started a live stream")
		}
		bs.Stop()
	}
	h.CloseAfterProducers()
	s, _ := h.Snapshot()
	if len(s.Sources) != 0 || len(s.Chunks) != 0 {
		t.Fatal("non-turbine configuration registered a receiver")
	}
}
