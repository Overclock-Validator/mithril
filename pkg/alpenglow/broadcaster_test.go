package alpenglow

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

func TestVotorBroadcasterPreconnectsBeforeFirstMessageAndDeliversDatagram(t *testing.T) {
	serverIdentity := ed25519.NewKeyFromSeed(bytesOf(31, ed25519.SeedSize))
	serverPubkey := testVotorPubkey(serverIdentity)
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(32, ed25519.SeedSize))
	clientPubkey := testVotorPubkey(clientIdentity)
	type receivedMessage struct {
		peer    solana.PublicKey
		message Message
	}
	received := make(chan receivedMessage, 1)
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:     "127.0.0.1:0",
		Identity:     serverIdentity,
		ShredVersion: 0x1234,
		LogInterval:  -1,
		AdmitPeer:    func(peer solana.PublicKey) bool { return peer == clientPubkey },
		AdmitMessage: func(peer solana.PublicKey, message Message) (Message, bool) {
			received <- receivedMessage{peer: peer, message: message}
			return Message{}, false
		},
	}, NewObserver())
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	peerAddr, ok := receiver.Addr().(*net.UDPAddr)
	require.True(t, ok)
	broadcaster, err := NewVotorBroadcaster(VotorBroadcasterConfig{
		Identity:     clientIdentity,
		ShredVersion: 0x1234,
		Peers: func() []VotorPeer {
			return []VotorPeer{{Identity: serverPubkey, Addr: cloneUDPAddr(peerAddr)}}
		},
		Workers: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, broadcaster.Close()) })
	require.Equal(t, []string{"alpenglow-v1"}, broadcaster.tlsConfig.NextProtos)
	require.True(t, broadcaster.quicConfig.EnableDatagrams)
	require.EqualValues(t, -1, broadcaster.quicConfig.MaxIncomingStreams)
	require.EqualValues(t, -1, broadcaster.quicConfig.MaxIncomingUniStreams)
	require.Equal(t, VotorQUICInitialPacketSize, broadcaster.quicConfig.InitialPacketSize)
	require.True(t, broadcaster.quicConfig.DisablePathMTUDiscovery)
	require.Eventually(t, func() bool {
		return broadcaster.Stats().Connections == 1
	}, 3*time.Second, 10*time.Millisecond)
	preconnectStats := broadcaster.Stats()
	require.Zero(t, preconnectStats.MessagesQueued)
	require.Zero(t, preconnectStats.PeerSends)
	require.EqualValues(t, 1, preconnectStats.ConnectionAttempts)

	vote := NewVoteMessage(NewSkipVote(77), testSignatureSeq(0x41), 3)
	require.NoError(t, broadcaster.Enqueue(vote))
	select {
	case got := <-received:
		require.Equal(t, clientPubkey, got.peer)
		require.NotNil(t, got.message.Vote)
		require.EqualValues(t, 77, got.message.Vote.Vote.Slot)
		require.EqualValues(t, 0x1234, got.message.ShredVersion)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for outbound Votor datagram")
	}
	require.Eventually(t, func() bool {
		stats := broadcaster.Stats()
		return stats.PeerSends == 1 && stats.Connections == 1
	}, time.Second, 10*time.Millisecond)
	require.EqualValues(t, 1, receiver.Stats().DatagramsReceived)
}

func TestVotorBroadcasterRejectsUnexpectedServerIdentity(t *testing.T) {
	serverIdentity := ed25519.NewKeyFromSeed(bytesOf(41, ed25519.SeedSize))
	serverPubkey := testVotorPubkey(serverIdentity)
	expectedIdentity := ed25519.NewKeyFromSeed(bytesOf(42, ed25519.SeedSize))
	expectedPubkey := testVotorPubkey(expectedIdentity)
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(43, ed25519.SeedSize))
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:    "127.0.0.1:0",
		Identity:    serverIdentity,
		LogInterval: -1,
		AdmitPeer:   func(solana.PublicKey) bool { return true },
	}, NewObserver())
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	peerAddr := receiver.Addr().(*net.UDPAddr)
	broadcaster, err := NewVotorBroadcaster(VotorBroadcasterConfig{
		Identity: clientIdentity,
		Peers: func() []VotorPeer {
			return []VotorPeer{{Identity: expectedPubkey, Addr: cloneUDPAddr(peerAddr)}}
		},
		Workers: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, broadcaster.Close()) })

	require.Eventually(t, func() bool {
		return broadcaster.Stats().ConnectionErrors >= 1
	}, 3*time.Second, 10*time.Millisecond)
	stats := broadcaster.Stats()
	require.Zero(t, stats.PeerSends)
	require.Zero(t, stats.PeerSendErrors)
	require.Zero(t, stats.Connections)
	require.False(t, stats.LastConnectionErrorAt.IsZero())
	require.True(t, strings.Contains(stats.LastConnectionError, expectedPubkey.String()))
	require.True(t, strings.Contains(stats.LastConnectionError, serverPubkey.String()))
	require.Zero(t, receiver.Stats().DatagramsReceived)
}

func TestVotorBroadcasterAcceptsAgaveStyleUntrustedServerCertificate(t *testing.T) {
	serverIdentity := ed25519.NewKeyFromSeed(bytesOf(44, ed25519.SeedSize))
	serverPubkey := testVotorPubkey(serverIdentity)
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(45, ed25519.SeedSize))
	clientPubkey := testVotorPubkey(clientIdentity)
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{testUntrustedVotorCertificate(t, serverIdentity)},
		ClientAuth:   tls.RequireAnyClientCert,
		NextProtos:   []string{VotorQUICALPN},
		MinVersion:   tls.VersionTLS13,
	}, testVotorQUICConfig())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
	})
	type ingress struct {
		peer    solana.PublicKey
		payload []byte
		err     error
	}
	received := make(chan ingress, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		if err != nil {
			received <- ingress{err: err}
			return
		}
		peer, err := votorPeerIdentity(conn.ConnectionState().TLS)
		if err != nil {
			received <- ingress{err: err}
			return
		}
		payload, err := conn.ReceiveDatagram(ctx)
		received <- ingress{peer: peer, payload: payload, err: err}
	}()

	peerAddr := listener.Addr().(*net.UDPAddr)
	broadcaster, err := NewVotorBroadcaster(VotorBroadcasterConfig{
		Identity: clientIdentity,
		Peers: func() []VotorPeer {
			return []VotorPeer{{Identity: serverPubkey, Addr: cloneUDPAddr(peerAddr)}}
		},
		Workers: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, broadcaster.Close()) })
	require.Eventually(t, func() bool {
		return broadcaster.Stats().Connections == 1
	}, 3*time.Second, 10*time.Millisecond)
	require.NoError(t, broadcaster.Enqueue(NewVoteMessage(NewSkipVote(89), testSignatureSeq(0x24), 1)))

	select {
	case got := <-received:
		require.NoError(t, got.err)
		require.Equal(t, clientPubkey, got.peer)
		message, err := DecodeMessage(got.payload)
		require.NoError(t, err)
		require.EqualValues(t, 89, message.Slot())
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for broadcaster datagram")
	}
}

func TestVotorBroadcasterDeduplicatesPeerIdentity(t *testing.T) {
	serverIdentity := ed25519.NewKeyFromSeed(bytesOf(51, ed25519.SeedSize))
	serverPubkey := testVotorPubkey(serverIdentity)
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(52, ed25519.SeedSize))
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:    "127.0.0.1:0",
		Identity:    serverIdentity,
		LogInterval: -1,
		AdmitPeer:   func(solana.PublicKey) bool { return true },
	}, NewObserver())
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	peerAddr := receiver.Addr().(*net.UDPAddr)
	broadcaster, err := NewVotorBroadcaster(VotorBroadcasterConfig{
		Identity: clientIdentity,
		Peers: func() []VotorPeer {
			peer := VotorPeer{Identity: serverPubkey, Addr: cloneUDPAddr(peerAddr)}
			return []VotorPeer{peer, peer}
		},
		Workers: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, broadcaster.Close()) })
	require.Eventually(t, func() bool {
		return broadcaster.Stats().Connections == 1
	}, 3*time.Second, 10*time.Millisecond)

	require.NoError(t, broadcaster.Enqueue(NewVoteMessage(NewSkipVote(99), testSignatureSeq(0x23), 1)))
	require.Eventually(t, func() bool {
		return receiver.Stats().DatagramsReceived == 1
	}, 3*time.Second, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 1, broadcaster.Stats().PeerSends)
	require.EqualValues(t, 1, receiver.Stats().DatagramsReceived)
}

func TestVotorBroadcasterReusesProactiveConnectionForConcurrentMessages(t *testing.T) {
	serverIdentity := ed25519.NewKeyFromSeed(bytesOf(71, ed25519.SeedSize))
	serverPubkey := testVotorPubkey(serverIdentity)
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(72, ed25519.SeedSize))
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:              "127.0.0.1:0",
		Identity:              serverIdentity,
		LogInterval:           -1,
		MaxDatagramsPerSecond: 50,
		AdmitPeer:             func(solana.PublicKey) bool { return true },
	}, NewObserver())
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	peerAddr := receiver.Addr().(*net.UDPAddr)
	broadcaster, err := NewVotorBroadcaster(VotorBroadcasterConfig{
		Identity: clientIdentity,
		Peers: func() []VotorPeer {
			return []VotorPeer{{Identity: serverPubkey, Addr: cloneUDPAddr(peerAddr)}}
		},
		Workers: 16,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, broadcaster.Close()) })
	require.Eventually(t, func() bool {
		return broadcaster.Stats().Connections == 1
	}, 3*time.Second, 10*time.Millisecond)

	const messages = 16
	for i := range messages {
		require.NoError(t, broadcaster.Enqueue(NewVoteMessage(NewSkipVote(uint64(100+i)), testSignatureSeq(byte(i+1)), 1)))
	}
	require.Eventually(t, func() bool {
		return broadcaster.Stats().PeerSends == messages && receiver.Stats().DatagramsReceived == messages
	}, 3*time.Second, 10*time.Millisecond)
	require.EqualValues(t, 1, receiver.Stats().ConnectionsAccepted)
}

func TestVotorBroadcasterClosesDepartedPeer(t *testing.T) {
	serverIdentity := ed25519.NewKeyFromSeed(bytesOf(81, ed25519.SeedSize))
	serverPubkey := testVotorPubkey(serverIdentity)
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(82, ed25519.SeedSize))
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:    "127.0.0.1:0",
		Identity:    serverIdentity,
		LogInterval: -1,
		AdmitPeer:   func(solana.PublicKey) bool { return true },
	}, NewObserver())
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	peers := newMutableVotorPeers([]VotorPeer{{Identity: serverPubkey, Addr: receiver.Addr().(*net.UDPAddr)}})
	broadcaster, err := NewVotorBroadcaster(VotorBroadcasterConfig{
		Identity: clientIdentity,
		Peers:    peers.Snapshot,
		Workers:  1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, broadcaster.Close()) })
	require.Eventually(t, func() bool { return broadcaster.Stats().Connections == 1 }, 3*time.Second, 10*time.Millisecond)

	broadcaster.connMu.Lock()
	departed := broadcaster.conns[serverPubkey].conn
	broadcaster.connMu.Unlock()
	require.NotNil(t, departed)
	peers.Set(nil)
	broadcaster.reconcilePeers()
	require.Eventually(t, func() bool {
		stats := broadcaster.Stats()
		return stats.DesiredPeers == 0 && stats.Connections == 0 && departed.Context().Err() != nil
	}, time.Second, 10*time.Millisecond)
}

func TestVotorBroadcasterReplacesChangedPeerAddress(t *testing.T) {
	serverIdentity := ed25519.NewKeyFromSeed(bytesOf(83, ed25519.SeedSize))
	serverPubkey := testVotorPubkey(serverIdentity)
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(84, ed25519.SeedSize))
	newReceiver := func() *Receiver {
		receiver, err := NewReceiver(ReceiverConfig{
			BindAddr:    "127.0.0.1:0",
			Identity:    serverIdentity,
			LogInterval: -1,
			AdmitPeer:   func(solana.PublicKey) bool { return true },
		}, NewObserver())
		require.NoError(t, err)
		runVotorReceiver(t, receiver)
		return receiver
	}
	firstReceiver := newReceiver()
	secondReceiver := newReceiver()
	firstAddr := firstReceiver.Addr().(*net.UDPAddr)
	secondAddr := secondReceiver.Addr().(*net.UDPAddr)
	peers := newMutableVotorPeers([]VotorPeer{{Identity: serverPubkey, Addr: firstAddr}})
	broadcaster, err := NewVotorBroadcaster(VotorBroadcasterConfig{
		Identity: clientIdentity,
		Peers:    peers.Snapshot,
		Workers:  1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, broadcaster.Close()) })
	require.Eventually(t, func() bool { return broadcaster.Stats().Connections == 1 }, 3*time.Second, 10*time.Millisecond)

	broadcaster.connMu.Lock()
	old := broadcaster.conns[serverPubkey].conn
	broadcaster.connMu.Unlock()
	peers.Set([]VotorPeer{{Identity: serverPubkey, Addr: secondAddr}})
	broadcaster.reconcilePeers()
	require.Eventually(t, func() bool {
		broadcaster.connMu.Lock()
		current := broadcaster.conns[serverPubkey]
		broadcaster.connMu.Unlock()
		return current.conn != nil && current.conn != old && current.addr == secondAddr.String() && old.Context().Err() != nil
	}, 3*time.Second, 10*time.Millisecond)
}

func TestVotorBroadcasterRetriesConnectionWithoutMessageTraffic(t *testing.T) {
	expectedServerIdentity := ed25519.NewKeyFromSeed(bytesOf(86, ed25519.SeedSize))
	expectedServerPubkey := testVotorPubkey(expectedServerIdentity)
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(87, ed25519.SeedSize))
	reserved, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	bindAddr := reserved.LocalAddr().String()
	peerAddr := cloneUDPAddr(reserved.LocalAddr().(*net.UDPAddr))
	require.NoError(t, reserved.Close())

	peers := newMutableVotorPeers([]VotorPeer{{Identity: expectedServerPubkey, Addr: peerAddr}})
	broadcaster, err := NewVotorBroadcaster(VotorBroadcasterConfig{
		Identity: clientIdentity,
		Peers:    peers.Snapshot,
		Workers:  1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, broadcaster.Close()) })
	require.Eventually(t, func() bool { return broadcaster.Stats().ConnectionErrors >= 1 }, 4*time.Second, 10*time.Millisecond)

	correctReceiver, err := NewReceiver(ReceiverConfig{
		BindAddr:    bindAddr,
		Identity:    expectedServerIdentity,
		LogInterval: -1,
		AdmitPeer:   func(solana.PublicKey) bool { return true },
	}, NewObserver())
	require.NoError(t, err)
	runVotorReceiver(t, correctReceiver)
	require.Eventually(t, func() bool {
		stats := broadcaster.Stats()
		return stats.Connections == 1 && stats.ConnectionAttempts >= 2
	}, 5*time.Second, 20*time.Millisecond)
	stats := broadcaster.Stats()
	require.Zero(t, stats.MessagesQueued)
	require.Zero(t, stats.PeerSendErrors)
}

func TestVotorBroadcasterReconcileDoesNotQueueDuplicateConnects(t *testing.T) {
	sink, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	const peerCount = 64
	peers := make([]VotorPeer, 0, peerCount)
	for i := range peerCount {
		identity := ed25519.NewKeyFromSeed(bytesOf(byte(i+1), ed25519.SeedSize))
		peers = append(peers, VotorPeer{Identity: testVotorPubkey(identity), Addr: sink.LocalAddr().(*net.UDPAddr)})
	}
	source := newMutableVotorPeers(peers)
	broadcaster, err := NewVotorBroadcaster(VotorBroadcasterConfig{
		Identity: ed25519.NewKeyFromSeed(bytesOf(90, ed25519.SeedSize)),
		Peers:    source.Snapshot,
		Workers:  1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, broadcaster.Close()) })

	for range 100 {
		broadcaster.reconcilePeers()
	}
	broadcaster.connMu.Lock()
	desired := len(broadcaster.desired)
	pending := len(broadcaster.connectQueued)
	queuedJobs := len(broadcaster.jobs)
	broadcaster.connMu.Unlock()
	require.Equal(t, peerCount, desired)
	require.LessOrEqual(t, pending, peerCount)
	require.LessOrEqual(t, queuedJobs, peerCount)
	require.LessOrEqual(t, queuedJobs, cap(broadcaster.jobs))
	require.Zero(t, broadcaster.Stats().ConnectionJobsDropped)
}

func TestVotorBroadcasterConnectionJobQueueIsBounded(t *testing.T) {
	const queueCapacity = 2
	broadcaster := &VotorBroadcaster{
		jobs:          make(chan votorPeerJob, queueCapacity),
		done:          make(chan struct{}),
		desired:       make(map[solana.PublicKey]VotorPeer),
		conns:         make(map[solana.PublicKey]votorConnection),
		dialing:       make(map[solana.PublicKey]*votorDial),
		connectQueued: make(map[solana.PublicKey]struct{}),
	}
	identities := make([]solana.PublicKey, 4)
	for i := range identities {
		identities[i][0] = byte(i + 1)
		broadcaster.desired[identities[i]] = VotorPeer{
			Identity: identities[i],
			Addr:     &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8000 + i},
		}
	}

	broadcaster.connMu.Lock()
	for _, identity := range identities {
		broadcaster.queueConnectLocked(identity)
	}
	// Reconciliation may encounter already-queued identities any number of
	// times without consuming another queue slot.
	for range 100 {
		broadcaster.queueConnectLocked(identities[0])
		broadcaster.queueConnectLocked(identities[1])
	}
	pending := len(broadcaster.connectQueued)
	queuedJobs := len(broadcaster.jobs)
	broadcaster.connMu.Unlock()

	require.Equal(t, queueCapacity, queuedJobs)
	require.Equal(t, queueCapacity, pending)
	require.EqualValues(t, len(identities)-queueCapacity, broadcaster.connectionJobsDropped.Load())
}

func TestValidVotorPeerRequiresIPv4UnicastAndIdentity(t *testing.T) {
	identity := testVotorPubkey(ed25519.NewKeyFromSeed(bytesOf(73, ed25519.SeedSize)))
	require.True(t, validVotorPeer(VotorPeer{Identity: identity, Addr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8000}}))
	require.False(t, validVotorPeer(VotorPeer{Addr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8000}}))
	require.False(t, validVotorPeer(VotorPeer{Identity: identity, Addr: &net.UDPAddr{IP: net.ParseIP("::1"), Port: 8000}}))
	require.False(t, validVotorPeer(VotorPeer{Identity: identity, Addr: &net.UDPAddr{IP: net.IPv4(224, 0, 0, 1), Port: 8000}}))
}

type mutableVotorPeers struct {
	mu    sync.Mutex
	peers []VotorPeer
}

func newMutableVotorPeers(peers []VotorPeer) *mutableVotorPeers {
	source := &mutableVotorPeers{}
	source.Set(peers)
	return source
}

func (p *mutableVotorPeers) Set(peers []VotorPeer) {
	p.mu.Lock()
	p.peers = cloneVotorPeers(peers)
	p.mu.Unlock()
}

func (p *mutableVotorPeers) Snapshot() []VotorPeer {
	p.mu.Lock()
	defer p.mu.Unlock()
	return cloneVotorPeers(p.peers)
}

func cloneVotorPeers(peers []VotorPeer) []VotorPeer {
	cloned := make([]VotorPeer, len(peers))
	for i, peer := range peers {
		cloned[i] = peer
		cloned[i].Addr = cloneUDPAddr(peer.Addr)
	}
	return cloned
}
