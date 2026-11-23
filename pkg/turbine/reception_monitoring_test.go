package turbine

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func receptionAt(m *receptionMonitoring, now time.Time) ReceptionSnapshot {
	m.mu.Lock()
	s := m.snapshotLocked(now)
	arrivals := m.arrivals
	clockValid := !now.Before(m.watermark)
	m.mu.Unlock()
	if clockValid {
		s.ArrivalTiming = arrivalTiming(&arrivals, now)
	}
	return s
}

func TestReceptionMonotonicEligibility(t *testing.T) {
	start := time.Now()
	var m receptionMonitoring
	m.observe(10, start)
	s := receptionAt(&m, start.Add(time.Second))
	require.Equal(t, start.Add(4*time.Second), s.ValidUntil)
	require.NotEqual(t, s.ValidUntil.Round(0), s.ValidUntil, "original monotonic reading must survive")
	s = receptionAt(&m, start.Add(3*time.Second))
	require.Equal(t, start.Add(ReceptionWindow), s.ValidUntil, "receipt expiry must cap sample freshness")
	before, err := json.Marshal(s)
	require.NoError(t, err)
	s.ValidUntil = time.Time{}
	after, err := json.Marshal(s)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after), "eligibility must not change serialized source dates")
	require.False(t, receptionAt(&m, start.Add(3*time.Second)).ValidUntil.IsZero(), "returned value must be detached")
	require.True(t, receptionAt(&m, start.Add(2*time.Second)).ValidUntil.IsZero(), "reversed sample must be ineligible")
	require.True(t, receptionAt(&m, start.Add(ReceptionWindow)).ValidUntil.IsZero(), "exact receipt expiry must be ineligible")
	var wallOnly receptionMonitoring
	wallOnly.observe(10, start.UTC())
	s = receptionAt(&wallOnly, start.Add(time.Second).UTC())
	require.True(t, s.HasSlot, "existing display fact must remain available")
	require.True(t, s.ValidUntil.IsZero(), "wall-only dates cannot mint a monotonic lease")
	var limited receptionMonitoring
	for slot := range uint64(65) {
		limited.observe(slot, start)
	}
	require.True(t, receptionAt(&limited, start.Add(time.Second)).ValidUntil.IsZero(), "incomplete coverage must be ineligible")
	var empty receptionMonitoring
	require.True(t, receptionAt(&empty, start).ValidUntil.IsZero())
}

func TestReceptionMonitoringOptInAndIsolation(t *testing.T) {
	now := time.Now()
	r := NewUDPReceiver("")
	_, ok := r.ReceptionSnapshot()
	require.False(t, ok)
	require.Nil(t, r.reception)
	var absent *UDPReceiver
	_, ok = absent.ReceptionSnapshot()
	require.False(t, ok)
	r.EnableReceptionMonitoring()
	s, ok := r.ReceptionSnapshot()
	require.True(t, ok)
	require.False(t, s.HasSlot)
	require.Zero(t, s.RetainedSlots)
	r.reception.observe(0, now)
	require.True(t, r.reception.slots[0].at == now, "retain the internal monotonic clock reading")
	r.EnableReceptionMonitoring()
	s, ok = r.ReceptionSnapshot()
	require.True(t, ok)
	require.True(t, s.HasSlot)
	require.Zero(t, s.Slot)
	require.Equal(t, now.UTC(), s.ObservedAt)
	require.False(t, s.SampledAt.Before(s.ObservedAt))
	s.Slot = 99
	r.ResetSlot(0)
	s, _ = r.ReceptionSnapshot()
	require.Zero(t, s.Slot, "slot assembly reset must not erase reception")
	other := NewUDPReceiver("")
	other.EnableReceptionMonitoring()
	s, _ = other.ReceptionSnapshot()
	require.False(t, s.HasSlot, "a new receiver must not inherit reception")
}

func TestReceptionMonitoringOrderingAndExpiry(t *testing.T) {
	start := time.Now()
	var m receptionMonitoring
	m.observe(0, start)
	m.observe(^uint64(0), start.Add(time.Second))
	m.observe(7, start.Add(2*time.Second))
	m.observe(^uint64(0), start.Add(500*time.Millisecond))
	s := receptionAt(&m, start.Add(2*time.Second))
	require.True(t, s.HasSlot)
	require.Equal(t, ^uint64(0), s.Slot)
	require.Equal(t, start.Add(time.Second).UTC(), s.ObservedAt)
	// Expiry selects a lower, still-recent slot; this is not a session maximum.
	s = receptionAt(&m, start.Add(time.Second+ReceptionWindow))
	require.True(t, s.HasSlot)
	require.Equal(t, uint64(7), s.Slot)
	require.Equal(t, 1, s.RetainedSlots)
	m.observe(7, start.Add(6*time.Second))
	m.observe(7, start.Add(3*time.Second))
	s = receptionAt(&m, start.Add(6*time.Second))
	require.Equal(t, start.Add(6*time.Second).UTC(), s.ObservedAt)
	require.False(t, receptionAt(&m, start).HasSlot, "reversed sample must be unknown")
	require.True(t, receptionAt(&m, start.Add(6*time.Second)).HasSlot, "reversal must not reset state")
	s = receptionAt(&m, start.Add(6*time.Second+ReceptionWindow))
	require.False(t, s.HasSlot, "exact expiry during silence")
	require.Zero(t, s.RetainedSlots)
	m.observe(999, start.Add(6*time.Second))
	require.False(t, receptionAt(&m, start.Add(11*time.Second)).HasSlot, "late processing must not revive expired evidence")
}

func TestReceptionMonitoringCapacityLoss(t *testing.T) {
	start := time.Now()
	var m receptionMonitoring
	for slot := range uint64(len(m.slots)) {
		m.observe(slot, start)
	}
	s := receptionAt(&m, start)
	require.True(t, s.HasSlot)
	require.Equal(t, uint64(63), s.Slot)
	require.Equal(t, 64, s.RetainedSlots)
	m.observe(999, start.Add(time.Second))
	m.observe(998, start.Add(2*time.Second))
	m.observe(997, start.Add(500*time.Millisecond))
	require.Equal(t, start.Add(2*time.Second+ReceptionWindow), m.limitedUntil)
	s = receptionAt(&m, start.Add(3*time.Second))
	require.True(t, s.CoverageLimited)
	require.False(t, s.HasSlot)
	require.Zero(t, s.Slot)
	require.True(t, s.ObservedAt.IsZero())
	require.Equal(t, 64, s.RetainedSlots)
	m.observe(63, start.Add(4*time.Second))
	s = receptionAt(&m, start.Add(2*time.Second+ReceptionWindow-time.Nanosecond))
	require.True(t, s.CoverageLimited)
	s = receptionAt(&m, start.Add(2*time.Second+ReceptionWindow))
	require.False(t, s.CoverageLimited)
	require.True(t, s.HasSlot)
	require.Equal(t, uint64(63), s.Slot)
	require.Equal(t, 1, s.RetainedSlots)
	// A discarded slot can be retained later once space is available.
	m.observe(999, start.Add(7*time.Second))
	s = receptionAt(&m, start.Add(7*time.Second))
	require.True(t, s.HasSlot)
	require.Equal(t, uint64(999), s.Slot)
}

func TestReceptionMonitoringConcurrent(t *testing.T) {
	r := NewUDPReceiver("")
	r.EnableReceptionMonitoring()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			for i := 0; i < 250; i++ {
				r.reception.observe(uint64(worker), time.Now())
				s, ok := r.ReceptionSnapshot()
				if !ok || !s.HasSlot || s.CoverageLimited || s.ObservedAt.After(s.SampledAt) || s.RetainedSlots > 8 {
					t.Errorf("invalid concurrent reception sample: %+v, enabled=%t", s, ok)
					return
				}
			}
		})
	}
	wg.Wait()
	s, _ := r.ReceptionSnapshot()
	require.True(t, s.HasSlot)
	require.Equal(t, uint64(7), s.Slot)
	require.Equal(t, 8, s.RetainedSlots)
}

func TestReceptionMonitoringPacketAdmission(t *testing.T) {
	leader, key := testIdentity(t)
	gen := ShredGenerator{Slot: 0, ParentSlot: 0, Version: 7}
	packets, _, _, _, err := gen.MakeShredsFromData(solana.PrivateKey(key), nil, false, solana.Hash{1}, 0, 0)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		change func(*UDPReceiver, []byte)
		repair bool
		index  int
		want   bool
	}{
		{name: "data", want: true},
		{name: "coding", index: dataShredsPerFECBlock, want: true},
		{name: "wrong version", change: func(r *UDPReceiver, _ []byte) { r.SetShredVersion(8) }},
		{name: "bad leader signature", change: func(_ *UDPReceiver, p []byte) { p[0] ^= 1 }},
		{name: "no leader source", change: func(r *UDPReceiver, _ []byte) { r.SetLeaderForSlot(nil) }},
		{name: "missing leader", change: func(r *UDPReceiver, _ []byte) {
			r.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) { return leader, false })
		}},
		{name: "repair socket", repair: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewUDPReceiver("")
			r.EnableReceptionMonitoring()
			r.SetShredVersion(7)
			var lookupAt time.Time
			r.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) {
				lookupAt = time.Now()
				return leader, true
			})
			packet := append([]byte(nil), packets[tc.index]...)
			if tc.change != nil {
				tc.change(r, packet)
			}
			before := time.Now()
			require.True(t, r.processPacket(context.Background(), nil, packet, nil, tc.repair))
			s, _ := r.ReceptionSnapshot()
			require.Equal(t, tc.want, s.HasSlot)
			require.Equal(t, !tc.want, r.reception.arrivals[0].at.IsZero(), "timing must use the same admitted packet boundary")
			if tc.want {
				require.Zero(t, s.Slot)
				require.Equal(t, 1, s.RetainedSlots)
				require.False(t, s.ObservedAt.Before(before))
				require.False(t, s.ObservedAt.After(lookupAt), "receipt precedes leader lookup and authentication")
				require.False(t, s.ObservedAt.After(s.SampledAt))
			} else {
				require.Zero(t, s.RetainedSlots)
			}
		})
	}
	r := NewUDPReceiver("")
	r.EnableReceptionMonitoring()
	require.True(t, r.processPacket(context.Background(), nil, []byte{1}, nil, false))
	s, _ := r.ReceptionSnapshot()
	require.False(t, s.HasSlot)
}

func TestReceptionMonitoringRejectsInvalidHop(t *testing.T) {
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
	r.EnableReceptionMonitoring()
	r.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) { return leader, true })
	r.retransmitter, err = newRetransmitterWithSenders(RetransmitConfig{
		Identity: identityHolder(t, keys[self]), Peers: peers,
		Stakes: func(uint64) map[solana.PublicKey]uint64 { return stakes }, UseChaCha8: true,
	}, []packetBatchSender{newCaptureBatchSender()})
	require.NoError(t, err)
	// The generator's leader signature is valid, but its hop signature is not
	// from the selected parent. The existing retransmitter must reject it.
	require.True(t, r.processPacket(context.Background(), nil, packets[0], nil, false))
	require.Equal(t, uint64(1), r.Stats().Retransmit.InvalidParentSignatures)
	s, _ := r.ReceptionSnapshot()
	require.False(t, s.HasSlot)
	require.Zero(t, s.RetainedSlots)
}

func TestReceptionMonitoringSpoolAndHydration(t *testing.T) {
	leader, key := testIdentity(t)
	gen := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7}
	packets, _, _, _, err := gen.MakeShredsFromData(solana.PrivateKey(key), nil, false, solana.Hash{1}, 0, 0)
	require.NoError(t, err)
	spool, err := OpenShredSpool(t.TempDir(), 1<<20)
	require.NoError(t, err)
	defer spool.Close()
	r := NewUDPReceiver("")
	r.EnableReceptionMonitoring()
	r.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) { return leader, true })
	r.SetShredSpool(spool)
	r.SetHydrationWindow(1, 10)
	r.assembler.maxObservedSlot = 200
	require.True(t, r.skipAssemblyForSpool(100))
	require.True(t, r.processPacket(context.Background(), nil, packets[0], nil, false))
	s, _ := r.ReceptionSnapshot()
	require.True(t, s.HasSlot, "spool-only live reception must still be observed")
	require.Equal(t, uint64(100), s.Slot)
	require.Zero(t, r.assembler.ActiveSlots())
	require.True(t, spool.HasSlot(100))

	hydrator := NewUDPReceiver("")
	hydrator.EnableReceptionMonitoring()
	hydrator.SetShredSpool(spool)
	hydrator.SetHydrationWindow(100, 100)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); hydrator.hydrateLoop(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	require.Eventually(t, func() bool { return hydrator.hydratedSlots.Load() > 0 }, time.Second, time.Millisecond)
	require.Equal(t, 1, hydrator.assembler.ActiveSlots())
	s, _ = hydrator.ReceptionSnapshot()
	require.False(t, s.HasSlot, "spool hydration is not socket reception")
	require.Zero(t, s.RetainedSlots)
}

func BenchmarkReceptionMonitoring(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		b.Run(name, func(b *testing.B) {
			r := NewUDPReceiver("")
			if enabled {
				r.EnableReceptionMonitoring()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if r.reception != nil {
					r.reception.observe(uint64(i%64), time.Now())
				}
			}
		})
	}
}
