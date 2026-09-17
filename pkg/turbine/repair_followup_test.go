package turbine

import (
	"context"
	"fmt"
	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/stretchr/testify/require"
	"net"
	"sync"
	"testing"
	"time"
)

// Existing protocol/pacing fixtures model cold admission without packet parsing.
// Production invokes followups only after the full receiver admission path.
func observeRepairForTest(c *repairClient, conn *net.UDPConn, packet []byte, from *net.UDPAddr, sh *Shred) bool {
	matched, highest := c.matchShredResponse(packet, from, sh)
	if highest {
		a := NewSlotAssembler()
		s := newRepairSelectionSlot(sh.Slot)
		s.shreds[sh.Index] = sh
		s.haveLast, s.lastIndex = sh.LastInSlot(), sh.Index
		a.slots[sh.Slot] = s
		c.followupHighestResponse(conn, a, sh.Slot)
	}
	return matched
}

func TestHighestRepairFollowupSelection(t *testing.T) {
	a := NewSlotAssembler()
	s := newRepairSelectionSlot(50)
	a.slots[50] = s
	// A recovered first span must not be requested again; the next span
	// lacks twelve data but has eight coding, so only four repairs are needed.
	addCodedSet(s, 0, 32, 32, seq(0, 31), 0)
	addCodedSet(s, 32, 32, 32, seq(32, 51), 8)
	s.haveLast, s.lastIndex = true, 63
	r, ok := a.highestRepairFollowup(50)
	require.True(t, ok)
	require.Equal(t, []uint32{52, 53, 54, 55}, r.MissingDataShreds)
	require.False(t, r.NeedHighestDataShred)
	for i := uint32(52); i <= 63; i++ {
		s.shreds[i] = &Shred{Index: i}
	}
	_, ok = a.highestRepairFollowup(50)
	require.False(t, ok)
	delete(s.shreds, 55)
	s.completing = true
	_, ok = a.highestRepairFollowup(50)
	require.False(t, ok)
	s.completing = false
	a.completedSlots[50] = struct{}{}
	_, ok = a.highestRepairFollowup(50)
	require.False(t, ok)
	delete(a.completedSlots, 50)
	a.ResetSlot(50)
	_, ok = a.highestRepairFollowup(50)
	require.False(t, ok)
}

func TestHighestRepairFollowupColdAndDiscovery(t *testing.T) {
	a := NewSlotAssembler()
	s := newRepairSelectionSlot(50)
	a.slots[50] = s
	s.shreds[600] = &Shred{Index: 600}
	r, ok := a.highestRepairFollowup(50)
	require.True(t, ok)
	require.Len(t, r.MissingDataShreds, 256)
	require.Equal(t, uint32(0), r.MissingDataShreds[0])
	require.True(t, r.NeedHighestDataShred)
	require.Equal(t, uint32(601), r.HighestDataShredIndex)
	// Possession holes only, even without a coding layout.
	for i := uint32(0); i < 600; i++ {
		s.shreds[i] = &Shred{Index: i}
	}
	delete(s.shreds, 299)
	r, ok = a.highestRepairFollowup(50)
	require.True(t, ok)
	require.Equal(t, []uint32{299}, r.MissingDataShreds)
}

func TestHighestRepairFollowupSendsOnlyDeficit(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer conn.Close()
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer sink.Close()
	c := newPacingTestClient(t)
	c.peerCache = []gossip.RepairPeer{{Addr: sink.LocalAddr().(*net.UDPAddr)}}
	c.peerCacheAt = time.Now()
	a := NewSlotAssembler()
	s := newRepairSelectionSlot(50)
	a.slots[50] = s
	addCodedSet(s, 0, 32, 32, seq(0, 19), 8)
	s.haveLast = true
	s.lastIndex = 31
	c.followupHighestResponse(conn, a, 50)
	require.Equal(t, uint64(4), c.requests.Load())
	c.followupHighestResponse(conn, a, 50)
	require.Equal(t, uint64(4), c.requests.Load()) // same inflight dedupe
	// Race a reset with read-only selection; no old slot is recreated.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			a.highestRepairFollowup(50)
		}
	}()
	a.ResetSlot(50)
	wg.Wait()
	c.followupHighestResponse(conn, a, 50)
	require.Equal(t, uint64(4), c.requests.Load())
}

func TestReceiverHighestFollowupUsesAdmittedState(t *testing.T) {
	packets := fixtures.DataShreds(t, "mainnet", 102815960)
	require.Greater(t, len(packets), 12)
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer conn.Close()
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer sink.Close()
	r := NewUDPReceiver("127.0.0.1:0")
	c := newPacingTestClient(t)
	r.repairClient = c
	c.peerCache = []gossip.RepairPeer{{Addr: sink.LocalAddr().(*net.UDPAddr)}}
	c.peerCacheAt = time.Now()
	for _, p := range packets[:11] {
		require.True(t, r.processPacket(context.Background(), nil, p, nil, false))
	}
	from := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 9), Port: 8009}
	addr, _ := repairAddressKeyFromUDP(from)
	key := repairRequestKey{kind: repairRequestHighestWindowIndex, slot: 102815960, index: 0}
	c.outstanding[key] = outstandingRepairRequest{key: key, nonce: 42, addr: addr, sentAt: time.Now()}
	c.byResponse[repairResponseKey{addr: addr, nonce: 42}] = key
	packet := append(append([]byte(nil), packets[11]...), nonceTrailer(42)...)
	require.True(t, r.processPacket(context.Background(), conn, packet, from, true))
	require.Equal(t, uint64(1), c.requests.Load(), "only continued discovery, no already held indices")
	for key := range c.outstanding {
		require.Equal(t, repairRequestHighestWindowIndex, key.kind)
		require.Equal(t, uint32(12), key.index)
	}
}

// Disk-only discovery must remain repairable after hydration enters the slot.
func TestHighestRepairFollowupAfterDiskOnlyHydration(t *testing.T) {
	const slot = uint64(102815960)
	packets := fixtures.DataShreds(t, "mainnet", slot)
	r := NewUDPReceiver("127.0.0.1:0")
	spool, err := OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	defer spool.Close()
	r.SetShredSpool(spool)
	r.assembler.maxObservedSlot = slot + 1000
	r.SetHydrationWindow(slot-8, slot-1)
	require.True(t, r.skipAssemblyForSpool(slot))
	// Seed only a partial range on disk; an authentic highest response extends it.
	for _, p := range packets[:10] {
		require.True(t, r.processPacket(context.Background(), nil, p, nil, false))
	}
	c := newPacingTestClient(t)
	r.repairClient = c
	from := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 9), Port: 8009}
	addr, _ := repairAddressKeyFromUDP(from)
	key := repairRequestKey{kind: repairRequestHighestWindowIndex, slot: slot, index: 0}
	c.outstanding[key] = outstandingRepairRequest{key: key, nonce: 43, addr: addr, sentAt: time.Now()}
	c.byResponse[repairResponseKey{addr: addr, nonce: 43}] = key
	packet := append(append([]byte(nil), packets[11]...), nonceTrailer(43)...)
	require.True(t, r.processPacket(context.Background(), nil, packet, from, true))
	require.Equal(t, uint64(0), c.requests.Load())
	_, live := r.assembler.HeadShredDetail(slot)
	require.False(t, live)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.hydrateLoop(ctx) }()
	defer func() { cancel(); <-done }()
	r.SetRetentionFloor(slot) // replay protects the hydration window during catchup
	r.assembler.PrioritizeRepairSlot(slot)
	r.SetHydrationWindow(slot, slot)
	require.Eventually(t, func() bool { return r.hydratedSlots.Load() > 0 }, time.Second, time.Millisecond)
	req, ok := r.assembler.highestRepairFollowup(slot)
	require.True(t, ok)
	require.Equal(t, []uint32{10}, req.MissingDataShreds)
	require.True(t, req.NeedHighestDataShred)
	require.Equal(t, uint32(12), req.HighestDataShredIndex)
	// Replay priority makes the hole eligible for the ordinary scheduler.
	r.assembler.PrioritizeRepairSlot(slot)
	priority, _ := r.assembler.RepairRequestsTiered(64, 2048)
	require.NotEmpty(t, priority)
	require.Equal(t, slot, priority[0].Slot)
	require.Equal(t, []uint32{10}, priority[0].MissingDataShreds)
}

func BenchmarkHighestRepairFollowup(b *testing.B) {
	for _, n := range []int{16384, 65536} {
		for _, fragmented := range []bool{false, true} {
			b.Run(fmt.Sprintf("shreds%d/fragmented%t", n, fragmented), func(b *testing.B) {
				a := NewSlotAssembler()
				s := newRepairSelectionSlot(50)
				a.slots[50] = s
				for start := 0; start < n; start += 32 {
					last := start + 31
					coding := 0
					if fragmented {
						last = start + 19
						coding = 8
					}
					addCodedSet(s, uint32(start), 32, 32, seq(uint32(start), uint32(last)), coding)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					a.highestRepairFollowup(50)
				}
			})
		}
	}
}
