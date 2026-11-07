package turbine

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

func TestSocketReadActivityActualReads(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		r := NewUDPReceiver("")
		if enabled {
			r.EnableSocketReadActivity()
		}
		r.socketMemory.observe = func(*net.UDPConn) metrics.SocketObservation {
			return metrics.SocketObservation{Reason: metrics.SocketUnsupported}
		}
		live, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		repair, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			live.Close()
			t.Fatal(err)
		}
		generation := r.attachSocketMemoryOwners(live, repair)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 2)
		go func() { done <- r.readPackets(ctx, live, false) }()
		go func() { done <- r.readPackets(ctx, repair, true) }()
		t.Cleanup(func() {
			cancel()
			live.Close()
			repair.Close()
			for i := 0; i < 2; i++ {
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(2 * time.Second):
					t.Error("socket reader did not stop")
				}
			}
			r.detachSocketMemoryOwners(generation)
		})
		send := func(conn *net.UDPConn, size int) {
			t.Helper()
			sender, err := net.DialUDP("udp4", nil, conn.LocalAddr().(*net.UDPAddr))
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Close()
			if n, err := sender.Write(make([]byte, size)); err != nil || n != size {
				t.Fatalf("datagram send: %d, %v", n, err)
			}
		}
		send(live, 3)
		send(live, 0)
		send(live, packetDataSize+100)
		send(repair, 7)
		deadline := time.Now().Add(2 * time.Second)
		for r.packets.Load() < 4 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if r.packets.Load() != 4 {
			t.Fatal("not all datagrams reached the real read loop")
		}
		// Direct injection is deliberately not a socket read.
		r.processPacket(ctx, live, []byte{1, 2, 3, 4, 5}, nil, false)
		got, ok := r.ObserveSocketMemory()
		if !ok {
			t.Fatal("owned socket observation missing")
		}
		if !enabled {
			if got.ReadActivity != (SocketReadActivity{}) {
				t.Fatal("default-off byte accounting was enabled")
			}
			continue
		}
		a := got.ReadActivity
		if !a.Enabled || !a.LiveKnown || !a.RepairKnown || a.LiveBytes != 3+packetDataSize || a.RepairBytes != 7 || a.StartedAt.IsZero() || a.SampledAt.Before(a.StartedAt) {
			t.Fatalf("copied-byte roles/truncation/direct-injection scope: %+v", a)
		}
		// New socket ownership must not inherit totals or a prior reader's writes.
		newGeneration := r.attachSocketMemoryOwners(live, repair)
		send(live, 9)
		for r.packets.Load() < 6 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if r.packets.Load() != 6 {
			t.Fatal("old reader did not receive final fixture")
		}
		fresh, ok := r.ObserveSocketMemory()
		if !ok || fresh.Generation != newGeneration || fresh.ReadActivity.LiveBytes != 0 || fresh.ReadActivity.RepairBytes != 0 {
			t.Fatal("a replacement recorder inherited an old reader's counts")
		}
	}
}

func TestSocketReadActivitySaturation(t *testing.T) {
	var c socketReadCounter
	a := &c.bytes
	a.Store(math.MaxUint64 - 2)
	c.add(1)
	if a.Load() != math.MaxUint64-1 {
		t.Fatal("valid high counter changed")
	}
	c.add(1)
	c.add(100)
	if a.Load() != math.MaxUint64 {
		t.Fatal("saturated byte count wrapped")
	}
}

func TestSocketReadActivityWindow(t *testing.T) {
	at := time.Now()
	if w := socketReadWindow(at, at.Add(time.Second), 2, 9, true, true); !w.Known || w.Bytes != 7 || w.Duration != time.Second {
		t.Fatalf("copied-byte interval: %+v", w)
	}
	if w := socketReadWindow(at, at.Add(time.Second), 2, 2, true, true); !w.Known || w.Bytes != 0 {
		t.Fatal("measured zero interval disappeared")
	}
	for _, w := range []SocketReadWindow{
		socketReadWindow(at, at.Add(time.Second), 2, 9, false, true),
		socketReadWindow(at, at.Add(time.Second), 2, 9, true, false),
		socketReadWindow(at, at.Add(time.Second), 9, 2, true, true),
		socketReadWindow(at, at, 2, 9, true, true),
		socketReadWindow(at, at.Add(31*time.Second), 2, 9, true, true),
	} {
		if w != (SocketReadWindow{}) {
			t.Fatal("invalid interval became a rate window")
		}
	}
}
