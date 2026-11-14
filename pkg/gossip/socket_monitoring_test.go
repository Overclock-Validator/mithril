package gossip

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

func TestGossipSocketMonitoringSequentialRuns(t *testing.T) {
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	c, err := NewClient(Config{Entrypoint: peer.LocalAddr().String(), BindAddr: "127.0.0.1:0", TVUAddr: peer.LocalAddr().String(), AdvertisedIP: "127.0.0.1", ShredVersion: 42})
	if err != nil {
		t.Fatal(err)
	}
	c.EnableMonitoring()
	c.EnableSocketMemoryMonitoring()
	c.EnableUDPPayloadMonitoring()
	started := c.startedAt
	for generation := uint64(1); generation <= 2; generation++ {
		t.Run(strconv.FormatUint(generation, 10), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			joined := false
			go func() { done <- c.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				if !joined {
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Error("loopback Run did not stop")
					}
				}
			})
			deadline := time.NewTimer(15 * time.Second)
			defer deadline.Stop()
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				if s, known := c.MonitoringSnapshot(); known {
					if !s.SocketObserved || s.SocketGeneration != generation || s.StartedAt != started.UTC() {
						t.Fatal("sequential Run reused socket evidence or reset client lifetime")
					}
					if !s.PayloadObserved || s.Payload.Generation != generation || !s.Payload.SentKnown || s.Payload.SentBytes == 0 ||
						s.Payload.StartedAt.Before(started) || s.Payload.SampledAt.Before(s.Payload.StartedAt) {
						t.Fatal("actual Run did not publish its original per-socket payload counters")
					}
					break
				}
				select {
				case err := <-done:
					joined = true
					t.Fatalf("Run stopped before its diagnostic cycle: %v", err)
				case <-deadline.C:
					t.Fatal("actual Run did not publish")
				case <-tick.C:
				}
			}
			if generation == 1 {
				payload := c.udpPayload.Load()
				_, key, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.identity.Swap(key); err != nil {
					t.Fatal(err)
				}
				if c.socketGeneration.Load() != generation {
					t.Fatal("live identity swap replaced the Run socket generation")
				}
				if c.udpPayload.Load() != payload {
					t.Fatal("identity swap reset the Run's payload recorder")
				}
			}
			cancel()
			select {
			case err := <-done:
				joined = true
				// A pending identity/contact push may race the cancellation's Close.
				// Keep that existing shutdown result distinct from other failures.
				if err != nil && !(ctx.Err() != nil && errors.Is(err, net.ErrClosed)) {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Run did not join after cancellation")
			}
			if _, known := c.MonitoringSnapshot(); known {
				t.Fatal("finished Run retained its socket capture")
			}
			if c.udpPayload.Load() != nil {
				t.Fatal("returned Run retained a payload recorder")
			}
		})
	}
}

func TestGossipSocketMonitoringOptInAndGeneration(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := &Client{startedAt: time.Now()}
	c.publishMonitoringWithSocket(time.Now(), Stats{}, conn, 1)
	if _, known := c.MonitoringSnapshot(); known {
		t.Fatal("default-off monitoring published")
	}
	c.EnableMonitoring()
	c.publishMonitoringWithSocket(time.Now(), Stats{}, conn, 1)
	s, known := c.MonitoringSnapshot()
	if !known || s.SocketObserved || s.SocketGeneration != 0 || s.SocketMemory != (metrics.SocketObservation{}) {
		t.Fatal("ordinary monitoring enabled a kernel observation")
	}
	c.EnableSocketMemoryMonitoring()
	c.publishMonitoringWithSocket(time.Now(), Stats{}, conn, 0)
	if s, _ := c.MonitoringSnapshot(); s.SocketObserved {
		t.Fatal("unbound socket was observed")
	}
	first, second := c.nextSocketGeneration(), c.nextSocketGeneration()
	if first != 1 || second != 2 {
		t.Fatal("successive Run sockets reused a generation")
	}
	c.publishMonitoringWithSocket(time.Now(), Stats{}, conn, second)
	s, _ = c.MonitoringSnapshot()
	if !s.SocketObserved || s.SocketGeneration != second || s.SocketMemory.Reason == metrics.SocketNotProvided {
		t.Fatal("actual opted-in owner socket was not sampled")
	}
	if !s.SocketMemory.Known && s.SocketMemory.Memory != (metrics.SocketMemory{}) {
		t.Fatal("unavailable kernel read became counters")
	}
	if s.SocketDrops.Known {
		t.Fatal("first sample fabricated interval coverage")
	}
	c.publishMonitoringWithSocket(time.Now(), Stats{}, conn, second)
	current, _ := c.MonitoringSnapshot()
	if current.SocketMemory.Known && (!current.SocketDrops.Known || current.SocketDrops.StartedAt != s.SocketMemory.SampledAt ||
		current.SocketDrops.SampledAt != current.SocketMemory.SampledAt) {
		t.Fatal("actual same-socket reads lost their original interval endpoints")
	}
	c.publishMonitoringWithSocket(time.Now(), Stats{}, conn, second+1)
	current, _ = c.MonitoringSnapshot()
	if current.SocketDrops.Known {
		t.Fatal("new Run generation inherited a previous socket baseline")
	}
	_ = conn.Close()
	c.publishMonitoringWithSocket(time.Now(), Stats{}, conn, second)
	s, _ = c.MonitoringSnapshot()
	if !s.SocketObserved || s.SocketMemory.Known || s.SocketMemory.Memory != (metrics.SocketMemory{}) {
		t.Fatal("closed socket retained a successful memory sample")
	}
	c.socketGeneration.Store(^uint64(0))
	if c.nextSocketGeneration() != 0 || c.nextSocketGeneration() != 0 || c.socketGeneration.Load() != ^uint64(0) {
		t.Fatal("socket generation wrapped or resumed after exhaustion")
	}
}
