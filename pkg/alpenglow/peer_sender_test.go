package alpenglow

import (
	"crypto/ed25519"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Real QUIC traffic: one path is blackholed after authentication while the
// other stays healthy. This reproduces the original shared-worker failure.
func TestVotorBroadcasterIsolatesBlockedPeer(t *testing.T) {
	for _, action := range []string{"reconnect", "close", "depart", "move"} {
		t.Run(action, func(t *testing.T) { testVotorBlockedPeer(t, action) })
	}
}

func testVotorBlockedPeer(t *testing.T, action string) {
	const markerSlot = 999999
	marker := make(chan time.Time, 1)
	badMarker := make(chan time.Time, 1)
	makeReceiver := func(seed byte, record bool) (*Receiver, solana.PublicKey) {
		identity := ed25519.NewKeyFromSeed(bytesOf(seed, ed25519.SeedSize))
		r, err := NewReceiver(ReceiverConfig{
			BindAddr: "127.0.0.1:0", Identity: identity, LogInterval: -1,
			MaxDatagramsPerSecond: 100000,
			AdmitPeer:             func(solana.PublicKey) bool { return true },
			AdmitMessage: func(_ solana.PublicKey, m Message) (Message, bool) {
				if m.Slot() == markerSlot {
					target := badMarker
					if record {
						target = marker
					}
					select {
					case target <- time.Now():
					default:
					}
				}
				return Message{}, false
			},
		}, NewObserver())
		require.NoError(t, err)
		runVotorReceiver(t, r)
		return r, testVotorPubkey(identity)
	}
	badReceiver, badID := makeReceiver(191, false)
	goodReceiver, goodID := makeReceiver(192, true)
	badAddr := badReceiver.Addr().(*net.UDPAddr)
	proxy, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	var blackhole atomic.Bool
	proxyDone := make(chan struct{})
	go func() {
		defer close(proxyDone)
		buf := make([]byte, 65536)
		var client *net.UDPAddr
		for {
			n, from, err := proxy.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if blackhole.Load() {
				continue
			}
			if from.String() == badAddr.String() {
				if client != nil {
					_, _ = proxy.WriteToUDP(buf[:n], client)
				}
			} else {
				client = from
				_, _ = proxy.WriteToUDP(buf[:n], badAddr)
			}
		}
	}()
	t.Cleanup(func() { _ = proxy.Close(); <-proxyDone })
	badPeer := VotorPeer{Identity: badID, Addr: proxy.LocalAddr().(*net.UDPAddr)}
	goodPeer := VotorPeer{Identity: goodID, Addr: goodReceiver.Addr().(*net.UDPAddr)}
	peers := newMutableVotorPeers([]VotorPeer{badPeer, goodPeer})
	b, err := NewVotorBroadcaster(VotorBroadcasterConfig{
		Identity: ed25519.NewKeyFromSeed(bytesOf(193, ed25519.SeedSize)),
		Peers:    peers.Snapshot,
		Workers:  defaultVotorConnectWorkers,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	require.Eventually(t, func() bool { return b.Stats().Connections == 2 }, 3*time.Second, 5*time.Millisecond)
	badConn, ok := b.establishedConnection(badPeer)
	require.True(t, ok)
	// Establish an actual healthy delivery before introducing the fault.
	require.NoError(t, b.Enqueue(NewVoteMessage(NewSkipVote(markerSlot), testSignatureSeq(0x41), 3)))
	select {
	case <-marker:
	case <-time.After(time.Second):
		t.Fatal("healthy baseline failed")
	}
	select {
	case <-badMarker:
	case <-time.After(time.Second):
		t.Fatal("proxied baseline failed")
	}
	blackholedAt := time.Now()
	blackhole.Store(true)
	for slot := uint64(1); slot <= 256; slot++ {
		require.NoError(t, b.Enqueue(NewVoteMessage(NewSkipVote(slot), testSignatureSeq(0x41), 3)))
	}
	blockedWorkers := func() int {
		buf := make([]byte, 2<<20)
		stack := string(buf[:runtime.Stack(buf, true)])
		count := 0
		for _, goroutine := range strings.Split(stack, "\n\n") {
			if strings.Contains(goroutine, "(*datagramQueue).Add") && strings.Contains(goroutine, "(*votorPeerSender).run") {
				count++
			}
		}
		return count
	}
	require.Eventually(t, func() bool { return blockedWorkers() == 1 }, 3*time.Second, 5*time.Millisecond)
	before := b.Stats()
	require.Zero(t, before.MessagesDropped)
	goodConn, ok := b.establishedConnection(goodPeer)
	require.True(t, ok)
	b.connMu.Lock()
	badSender := b.conns[badID].sender
	b.connMu.Unlock()
	start := time.Now()
	require.NoError(t, b.Enqueue(NewVoteMessage(NewSkipVote(markerSlot), testSignatureSeq(0x42), 3)))
	select {
	case received := <-marker:
		t.Logf("Healthy marker received in %s while other peer's SendDatagram is blocked", received.Sub(start))
	case <-time.After(250 * time.Millisecond):
		t.Fatal("stalled peer delayed healthy delivery")
	}
	require.NoError(t, badConn.Context().Err(), "marker must arrive before watchdog releases stalled peer")
	if action != "reconnect" {
		switch action {
		case "close":
			closed := make(chan struct{})
			go func() { _ = b.Close(); close(closed) }()
			select {
			case <-closed:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("Close waited for stalled SendDatagram")
			}
		case "depart":
			peers.Set([]VotorPeer{goodPeer})
			b.reconcilePeers()
		case "move":
			replacement, _ := makeReceiver(191, false)
			badPeer.Addr = replacement.Addr().(*net.UDPAddr)
			peers.Set([]VotorPeer{badPeer, goodPeer})
			b.reconcilePeers()
		}
		select {
		case <-badSender.done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("old sender did not stop")
		}
		require.Error(t, badConn.Context().Err())
		require.Empty(t, badSender.queue)
		require.Positive(t, b.Stats().PeerQueueDiscarded)
		if action == "close" {
			return
		}
		if action == "move" {
			require.Eventually(t, func() bool {
				conn, ok := b.establishedConnection(badPeer)
				return ok && conn != badConn
			}, 3*time.Second, 5*time.Millisecond)
		}
		currentGood, ok := b.establishedConnection(goodPeer)
		require.True(t, ok)
		require.Same(t, goodConn, currentGood)
		require.NoError(t, b.Enqueue(NewVoteMessage(NewSkipVote(markerSlot), testSignatureSeq(0x44), 3)))
		select {
		case <-marker:
		case <-time.After(time.Second):
			t.Fatal("healthy delivery stopped after peer change")
		}
		if action == "move" {
			select {
			case <-badMarker:
			case <-time.After(time.Second):
				t.Fatal("replacement address did not receive new vote")
			}
		}
		return
	}
	// Deliberately fill just the stalled peer's bounded queue. Healthy peers
	// remain independent even when the failed peer's copies are rejected.
	payload, err := EncodeMessage(NewVoteMessage(NewSkipVote(123), testSignatureSeq(0x42), 3))
	require.NoError(t, err)
	for range 2 * defaultVotorPeerSendQueue {
		badSender.enqueue(votorDatagram{payload: payload, queuedAt: time.Now()})
	}
	require.Equal(t, defaultVotorPeerSendQueue, len(badSender.queue))
	require.Positive(t, b.Stats().PeerQueueDrops)
	require.Equal(t, b.Stats().PeerQueueDrops, b.Stats().MessagesDropped)
	require.NoError(t, b.Enqueue(NewVoteMessage(NewSkipVote(markerSlot), testSignatureSeq(0x43), 3)))
	select {
	case <-marker:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("full peer queue delayed healthy delivery")
	}
	// Queue age is measured from fanout, so PTO progress no longer restarts
	// the one-second budget. Allow two seconds of test scheduling slack beyond
	// one watchdog tick, measured from the fault rather than this assertion.
	remaining := time.Until(blackholedAt.Add(votorSendTimeout + votorSendWatchInterval + 2*time.Second))
	require.Positive(t, remaining)
	require.Eventually(t, func() bool { return badConn.Context().Err() != nil }, remaining, 5*time.Millisecond)
	t.Logf("Blackholed peer retired after %s", time.Since(blackholedAt))
	select {
	case <-badSender.done:
	case <-time.After(time.Second):
		t.Fatal("stalled sender leaked after watchdog closed its connection")
	}
	require.EqualValues(t, 1, b.Stats().PeerSendTimeouts)
	require.Positive(t, b.Stats().PeerQueueDiscarded)
	require.Empty(t, badSender.queue)
	// The old queue must never be resurrected on a replacement connection.
	blackhole.Store(false)
	require.Eventually(t, func() bool {
		conn, ok := b.establishedConnection(badPeer)
		return ok && conn != badConn
	}, 5*time.Second, 10*time.Millisecond)
	currentGood, ok := b.establishedConnection(goodPeer)
	require.True(t, ok)
	require.Same(t, goodConn, currentGood)
	require.NoError(t, b.Enqueue(NewVoteMessage(NewSkipVote(markerSlot), testSignatureSeq(0x44), 3)))
	select {
	case <-marker:
	case <-time.After(time.Second):
		t.Fatal("healthy peer did not continue after reconnect")
	}
	select {
	case <-badMarker:
	case <-time.After(time.Second):
		t.Fatal("reconnected peer did not receive new vote")
	}
}
