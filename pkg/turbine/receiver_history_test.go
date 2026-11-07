package turbine

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Direct processPacket tests own retirement and drive the existing consumer
// synchronously. Run tests below use the real consumer and join before drain.
func attachReceiverHistory(t *testing.T, r *UDPReceiver, queue int) *ShredHistory {
	t.Helper()
	limits := historyLimits()
	limits.queue, limits.rows, limits.chunks = queue, 128, 8
	limits.retainedBytes = limits.chunks * limits.chunkBytes
	h := historyOwner(t, limits)
	r.SetShredHistoryBeforeRun(historySource(t, h, ShredHistoryLive))
	return h
}

func receiverHistoryRows(t *testing.T, h *ShredHistory) []ShredHistoryRow {
	t.Helper()
	snapshot, ok := h.Snapshot()
	require.True(t, ok)
	var rows []ShredHistoryRow
	for _, chunk := range snapshot.Chunks {
		rows = append(rows, chunk.Rows()...)
	}
	return rows
}

func TestReceiverHistoryPacketAdmission(t *testing.T) {
	leader, key := testIdentity(t)
	gen := ShredGenerator{Slot: 0, ParentSlot: 0, Version: 7}
	packets, root, _, _, err := gen.MakeShredsFromData(solana.PrivateKey(key), nil, false, solana.Hash{1}, 0, 0)
	require.NoError(t, err)
	for _, tc := range []struct {
		name      string
		change    func(*UDPReceiver, []byte)
		index     int
		repair    bool
		reception bool
		repeat    int
		want      int
	}{
		{name: "data without reception monitoring", repeat: 1, want: 1},
		{name: "same reception clock", reception: true, repeat: 1, want: 1},
		{name: "coding", index: dataShredsPerFECBlock, repeat: 1, want: 1},
		{name: "duplicate occurrences", repeat: 2, want: 2},
		{name: "unmatched repair socket", repair: true, repeat: 1, want: 1},
		{name: "wrong version", repeat: 1, change: func(r *UDPReceiver, _ []byte) { r.SetShredVersion(8) }},
		{name: "invalid leader signature", repeat: 1, change: func(_ *UDPReceiver, p []byte) { p[0] ^= 1 }},
		{name: "no leader source", repeat: 1, change: func(r *UDPReceiver, _ []byte) { r.SetLeaderForSlot(nil) }},
		{name: "missing leader", repeat: 1, change: func(r *UDPReceiver, _ []byte) {
			r.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) { return leader, false })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewUDPReceiver("")
			h := attachReceiverHistory(t, r, 8)
			r.SetShredVersion(7)
			if tc.reception {
				r.EnableReceptionMonitoring()
			}
			var lookupAt time.Time
			r.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) {
				lookupAt = time.Now()
				return leader, true
			})
			packet := bytes.Clone(packets[tc.index])
			if tc.change != nil {
				tc.change(r, packet)
			}
			before := time.Now()
			for i := 0; i < tc.repeat; i++ {
				require.True(t, r.processPacket(context.Background(), nil, packet, nil, tc.repair))
			}
			r.shredHistory.Retire()
			historyDrain(h)
			rows := receiverHistoryRows(t, h)
			require.Len(t, rows, tc.want)
			shred, err := ParseShred(packet)
			require.NoError(t, err)
			origin := ShredHistoryTVU
			if tc.repair {
				origin = ShredHistoryRepairUnmatched
			}
			for i, row := range rows {
				require.Equal(t, ShredHistoryRow{ShredHistoryReceipt: ShredHistoryReceipt{
					At: row.At, Root: root, SourceGeneration: r.shredHistory.Generation(),
					Slot: shred.Slot, Index: shred.Index, FECSetIndex: shred.FECSetIndex,
					Version: shred.Version, Variant: shred.Variant, Origin: origin,
				}, Sequence: uint64(i + 1)}, row)
				require.False(t, row.At.Before(before))
				require.False(t, row.At.After(lookupAt), "receipt must precede authentication")
				require.False(t, row.At == row.At.Round(0), "original monotonic component was lost")
			}
			if tc.reception {
				reception, ok := r.ReceptionSnapshot()
				require.True(t, ok)
				require.Equal(t, reception.ObservedAt, rows[0].At.UTC())
				require.Equal(t, r.reception.arrivals[0].at, rows[0].At, "both observers must keep the same original clock")
			}
		})
	}
	r := NewUDPReceiver("")
	h := attachReceiverHistory(t, r, 8)
	require.True(t, r.processPacket(context.Background(), nil, []byte{1}, nil, false))
	r.shredHistory.Retire()
	historyDrain(h)
	require.Empty(t, receiverHistoryRows(t, h))
	require.Equal(t, uint64(1), r.Stats().ParseErrors)
}

func TestReceiverHistoryRejectsInvalidHop(t *testing.T) {
	leader, key := testIdentity(t)
	a, keyA := testIdentity(t)
	b, keyB := testIdentity(t)
	keys := map[solana.PublicKey]ed25519.PrivateKey{a: keyA, b: keyB}
	peers := &mutableTVUPeers{peers: []gossip.TVUPeer{
		{Pubkey: gossip.Pubkey(a), TVUAddr: &net.UDPAddr{IP: net.IPv4(127, 3, 0, 1), Port: 8001}},
		{Pubkey: gossip.Pubkey(b), TVUAddr: &net.UDPAddr{IP: net.IPv4(127, 3, 0, 2), Port: 8002}},
	}}
	stakes := map[solana.PublicKey]uint64{leader: 100, a: 10, b: 20}
	gen := ShredGenerator{Slot: 91, ParentSlot: 90, Version: 7}
	packets, _, _, _, err := gen.MakeShredsFromData(solana.PrivateKey(key), nil, true, solana.Hash{7}, 0, 0)
	require.NoError(t, err)
	shred, err := ParseShred(packets[0])
	require.NoError(t, err)
	nodes := NewRetransmitClusterNodes(ClusterNodesConfig{Self: a, TVUPeers: peers.peers, Stakes: stakes, UseChaCha8: true})
	shuffle := nodes.retransmitShuffle(leader, ShredID{Slot: shred.Slot, Index: shred.Index, Type: shred.Type})
	self := nodes.nodes[shuffle[1]].pubkey
	r := NewUDPReceiver("")
	h := attachReceiverHistory(t, r, 8)
	r.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) { return leader, true })
	r.retransmitter, err = newRetransmitterWithSenders(RetransmitConfig{
		Identity: identityHolder(t, keys[self]), Peers: peers,
		Stakes: func(uint64) map[solana.PublicKey]uint64 { return stakes }, UseChaCha8: true,
	}, []packetBatchSender{newCaptureBatchSender()})
	require.NoError(t, err)
	require.True(t, r.processPacket(context.Background(), nil, packets[0], nil, false))
	require.Equal(t, uint64(1), r.Stats().Retransmit.InvalidParentSignatures)
	r.shredHistory.Retire()
	historyDrain(h)
	require.Empty(t, receiverHistoryRows(t, h))
}

func TestReceiverHistoryAssemblyParityAndQueueRefusal(t *testing.T) {
	shreds := generatedAlpenglowDataShreds(t)
	leader := testShredLeader(t).PublicKey()
	var baseline []byte
	var baselineStats ReceiverStats
	for _, queue := range []int{0, 512, 1} {
		r := NewUDPReceiver("")
		r.SetShredVersion(7)
		r.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) { return leader, true })
		r.SetShredHistoryBeforeRun(nil)
		var h *ShredHistory
		if queue != 0 {
			h = attachReceiverHistory(t, r, queue)
		}
		for _, shred := range shreds {
			require.True(t, r.processPacket(context.Background(), nil, shred.Payload, nil, false))
		}
		var completed *block.Block
		select {
		case completed = <-r.Blocks():
		default:
			t.Fatal("signed fixture did not produce a completed block")
		}
		require.True(t, completed.TransactionSignaturesVerified())
		detached := *completed
		detached.ShredFirstNanos, detached.ShredFullNanos = 0, 0
		encoded, err := json.Marshal(&detached)
		require.NoError(t, err)
		stats := r.Stats()
		stats.LastPacketUnix = 0
		if queue == 0 {
			baseline, baselineStats = encoded, stats
			continue
		}
		require.Equal(t, baseline, encoded, "history must not change assembled block data")
		require.Equal(t, baselineStats, stats, "history must not change receiver outcomes")
		r.shredHistory.Retire()
		historyDrain(h)
		rows := receiverHistoryRows(t, h)
		if queue == 1 {
			require.Len(t, rows, 1)
			require.Equal(t, uint64(len(shreds)-1), h.queueRefusals.Load())
		} else {
			require.Len(t, rows, len(shreds))
			require.Zero(t, h.queueRefusals.Load())
		}
	}
}

func TestReceiverHistorySpoolAndHydration(t *testing.T) {
	leader := testShredLeader(t)
	gen := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7}
	packets, _, _, _, err := gen.MakeShredsFromData(leader, nil, false, solana.Hash{1}, 0, 0)
	require.NoError(t, err)
	spool, err := OpenShredSpool(t.TempDir(), 1<<20)
	require.NoError(t, err)
	t.Cleanup(spool.Close)
	r := NewUDPReceiver("")
	h := attachReceiverHistory(t, r, 8)
	r.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) { return leader.PublicKey(), true })
	r.SetShredSpool(spool)
	r.SetHydrationWindow(1, 10)
	r.assembler.maxObservedSlot = 200
	require.True(t, r.skipAssemblyForSpool(100))
	require.True(t, r.processPacket(context.Background(), nil, packets[0], nil, false))
	require.Zero(t, r.assembler.ActiveSlots())
	require.True(t, spool.HasSlot(100))
	r.shredHistory.Retire()

	hydrator := NewUDPReceiver("")
	hydrator.SetShredHistoryBeforeRun(historySource(t, h, ShredHistoryLive))
	hydrator.SetShredSpool(spool)
	hydrator.SetHydrationWindow(100, 100)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); hydrator.hydrateLoop(ctx) }()
	t.Cleanup(func() { cancel(); waitSignal(t, done, "hydrator shutdown") })
	require.Eventually(t, func() bool { return hydrator.hydratedSlots.Load() > 0 }, time.Second, time.Millisecond)
	cancel()
	waitSignal(t, done, "hydrator shutdown")
	hydrator.shredHistory.Retire()
	require.Equal(t, 1, hydrator.assembler.ActiveSlots())
	historyDrain(h)
	rows := receiverHistoryRows(t, h)
	require.Len(t, rows, 1, "spool hydration must not create a new receipt")
	require.Equal(t, r.shredHistory.Generation(), rows[0].SourceGeneration)
}

func TestReceiverHistoryFailedRunRetires(t *testing.T) {
	r := NewUDPReceiver("[")
	h := attachReceiverHistory(t, r, 8)
	alias := *r.shredHistory
	require.Error(t, r.Run(context.Background()))
	require.Error(t, <-r.Ready())
	require.False(t, alias.TryReceipt(historyReceipt(0, ShredHistoryTVU)))
	require.False(t, alias.TryMarker(historyMarker()))
	// Retiring one failed receiver must not close the shared run owner.
	_, ok := h.RegisterSource(ShredHistoryLive, time.Now())
	require.True(t, ok)
	historyDrain(h)
	require.Empty(t, receiverHistoryRows(t, h))
}

func TestReceiverHistoryRunLoopback(t *testing.T) {
	leader := testShredLeader(t)
	gen := ShredGenerator{Slot: 42, ParentSlot: 41, Version: 7}
	packets, root, _, _, err := gen.MakeShredsFromData(leader, nil, false, solana.Hash{1}, 0, 0)
	require.NoError(t, err)
	h, err := NewShredHistory(historyBinding())
	require.NoError(t, err)
	t.Cleanup(h.Interrupt)
	r := NewUDPReceiver("127.0.0.1:0")
	r.SetShredHistoryBeforeRun(historySource(t, h, ShredHistoryLive))
	r.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) { return leader.PublicKey(), true })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { defer close(done); runErr = r.Run(ctx) }()
	t.Cleanup(func() { cancel(); waitSignal(t, done, "receiver shutdown") })
	select {
	case err := <-r.Ready():
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("receiver did not become ready")
	}
	r.socketMemory.mu.Lock()
	addr := *r.socketMemory.live.LocalAddr().(*net.UDPAddr)
	r.socketMemory.mu.Unlock()
	sender, err := net.DialUDP("udp4", nil, &addr)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sender.Close()) })
	require.NoError(t, sender.SetWriteDeadline(time.Now().Add(time.Second)))
	for _, index := range []int{0, dataShredsPerFECBlock} {
		n, err := sender.Write(packets[index])
		require.NoError(t, err)
		require.Equal(t, len(packets[index]), n)
	}
	require.Eventually(t, func() bool { return r.packets.Load() == 2 }, time.Second, time.Millisecond)
	cancel()
	waitSignal(t, done, "receiver shutdown")
	require.NoError(t, runErr)
	require.False(t, r.shredHistory.TryReceipt(historyReceipt(0, ShredHistoryTVU)))
	require.True(t, h.CloseAfterProducers())
	rows := receiverHistoryRows(t, h)
	require.Len(t, rows, 2)
	for _, row := range rows {
		require.Equal(t, root, row.Root)
		require.Equal(t, ShredHistoryTVU, row.Origin)
	}
	snapshot, _ := h.Snapshot()
	require.True(t, snapshot.Sources[0].Retired)
}

func TestReceiverHistoryRetiresAfterCompletionJoin(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 1<<20)
	require.NoError(t, err)
	t.Cleanup(spool.Close)
	for _, shred := range generatedAlpenglowDataShreds(t) {
		spool.Append(shred.Slot, shred.Payload)
	}
	h, err := NewShredHistory(historyBinding())
	require.NoError(t, err)
	t.Cleanup(h.Interrupt)
	r := NewUDPReceiver("127.0.0.1:0")
	r.SetShredHistoryBeforeRun(historySource(t, h, ShredHistoryLive))
	r.SetShredSpool(spool)
	r.SetHydrationWindow(100, 100)
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	r.assembler.verifyTransactions = func(context.Context, *block.Block) error {
		close(started)
		<-release
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { defer close(done); runErr = r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		waitSignal(t, done, "completion owner shutdown")
	})
	select {
	case err := <-r.Ready():
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("receiver did not become ready")
	}
	waitSignal(t, started, "hydrated completion verifier")
	cancel()
	select {
	case <-done:
		t.Fatal("Run returned before its completion worker joined")
	case <-time.After(50 * time.Millisecond):
	}
	h.gate.RLock()
	retired := r.shredHistory.retirement.retired
	h.gate.RUnlock()
	require.False(t, retired, "handle retired before Run joined its owned work")
	releaseOnce.Do(func() { close(release) })
	waitSignal(t, done, "completion owner shutdown")
	require.NoError(t, runErr)
	alias := *r.shredHistory
	require.False(t, alias.TryReceipt(historyReceipt(0, ShredHistoryTVU)))
	require.False(t, alias.TryMarker(historyMarker()))
	require.True(t, h.CloseAfterProducers())
	require.Empty(t, receiverHistoryRows(t, h), "hydrated completion is not a network receipt")
}
