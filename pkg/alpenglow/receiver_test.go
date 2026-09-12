package alpenglow

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

func TestReceiverDecodesVotorVoteFromQUICDatagram(t *testing.T) {
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(1, ed25519.SeedSize))
	clientPubkey := testVotorPubkey(clientIdentity)
	observer := NewObserver()
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:     "127.0.0.1:0",
		ShredVersion: 0x1234,
		LogInterval:  -1,
		AdmitPeer: func(peer solana.PublicKey) bool {
			return peer == clientPubkey
		},
	}, observer)
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	msg := NewVoteMessage(NewFinalizationVote(42), testSignatureSeq(0x55), 7)
	msg.ShredVersion = 0x1234
	payload, err := EncodeMessage(msg)
	require.NoError(t, err)

	conn := sendVotorQUICDatagram(t, receiver.Addr().String(), clientIdentity, payload)
	t.Cleanup(func() { _ = conn.CloseWithError(0, "test done") })

	require.Eventually(t, func() bool {
		return observer.Snapshot().VotesObserved == 1
	}, 2*time.Second, 10*time.Millisecond)

	stats := receiver.Stats()
	require.EqualValues(t, 1, stats.ConnectionsAccepted)
	require.EqualValues(t, 1, stats.DatagramsReceived)
	require.EqualValues(t, 1, stats.MessagesDecoded)
	require.EqualValues(t, 1, stats.VotesDecoded)
	require.EqualValues(t, 42, stats.LatestVoteSlot)
}

func TestReceiverAcceptsAgaveStyleUntrustedCertificateSignature(t *testing.T) {
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(10, ed25519.SeedSize))
	clientPubkey := testVotorPubkey(clientIdentity)
	observer := NewObserver()
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:    "127.0.0.1:0",
		LogInterval: -1,
		AdmitPeer:   func(peer solana.PublicKey) bool { return peer == clientPubkey },
	}, observer)
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	certificate := testUntrustedVotorCertificate(t, clientIdentity)
	conn := dialVotorQUICWithCertificate(t, receiver.Addr().String(), certificate)
	t.Cleanup(func() { _ = conn.CloseWithError(0, "test done") })
	msg := NewVoteMessage(NewSkipVote(43), testSignatureSeq(0x56), 1)
	payload, err := EncodeMessage(msg)
	require.NoError(t, err)
	require.NoError(t, conn.SendDatagram(payload))
	require.Eventually(t, func() bool {
		return observer.Snapshot().VotesObserved == 1
	}, 2*time.Second, 10*time.Millisecond)
}

func TestReceiverRejectsQUICStreams(t *testing.T) {
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(2, ed25519.SeedSize))
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:    "127.0.0.1:0",
		LogInterval: -1,
		AdmitPeer:   func(peer solana.PublicKey) bool { return peer == testVotorPubkey(clientIdentity) },
	}, NewObserver())
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	conn := dialVotorQUIC(t, receiver.Addr().String(), clientIdentity)
	t.Cleanup(func() { _ = conn.CloseWithError(0, "test done") })

	assertNoStream := func(open func(context.Context) error) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := open(ctx)
		require.Error(t, err)
		var streamLimit *quic.StreamLimitReachedError
		require.True(t, errors.As(err, &streamLimit) || errors.Is(err, context.DeadlineExceeded), err)
	}
	assertNoStream(func(ctx context.Context) error {
		_, err := conn.OpenUniStreamSync(ctx)
		return err
	})
	assertNoStream(func(ctx context.Context) error {
		_, err := conn.OpenStreamSync(ctx)
		return err
	})
	require.Zero(t, receiver.Stats().DatagramsReceived)
	require.Zero(t, receiver.Stats().MessagesDecoded)
}

func TestReceiverRejectsMismatchedShredVersionBeforeObservation(t *testing.T) {
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(3, ed25519.SeedSize))
	observer := NewObserver()
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:     "127.0.0.1:0",
		ShredVersion: 0x1234,
		LogInterval:  -1,
		AdmitPeer:    func(solana.PublicKey) bool { return true },
	}, observer)
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	msg := NewVoteMessage(NewFinalizationVote(42), testSignatureSeq(0x55), 7)
	msg.ShredVersion = 0x5678
	payload, err := EncodeMessage(msg)
	require.NoError(t, err)
	conn := sendVotorQUICDatagram(t, receiver.Addr().String(), clientIdentity, payload)
	t.Cleanup(func() { _ = conn.CloseWithError(0, "test done") })

	require.Eventually(t, func() bool {
		return receiver.Stats().ShredVersionMismatches == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, observer.Snapshot().VotesObserved)
	require.Zero(t, receiver.Stats().DecodeErrors)
}

func TestReceiverPassesTLSIdentityToMessageAdmission(t *testing.T) {
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(4, ed25519.SeedSize))
	clientPubkey := testVotorPubkey(clientIdentity)
	observer := NewObserver()
	type admission struct {
		peer solana.PublicKey
		msg  Message
	}
	admissions := make(chan admission, 1)
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:    "127.0.0.1:0",
		LogInterval: -1,
		AdmitPeer:   func(peer solana.PublicKey) bool { return peer == clientPubkey },
		AdmitMessage: func(peer solana.PublicKey, msg Message) (Message, bool) {
			admissions <- admission{peer: peer, msg: msg}
			return Message{}, false
		},
	}, observer)
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	msg := NewCertificateMessage(Certificate{
		Type:      CertificateSkip,
		Slot:      42,
		Signature: testSignatureSeq(0x42),
		Bitmap:    []byte{0, 0, 0},
	})
	payload, err := EncodeMessage(msg)
	require.NoError(t, err)
	conn := sendVotorQUICDatagram(t, receiver.Addr().String(), clientIdentity, payload)
	t.Cleanup(func() { _ = conn.CloseWithError(0, "test done") })

	select {
	case got := <-admissions:
		require.Equal(t, clientPubkey, got.peer)
		require.NotNil(t, got.msg.Certificate)
	case <-time.After(2 * time.Second):
		t.Fatal("admission callback was not invoked")
	}
	require.EqualValues(t, 1, receiver.Stats().MessagesDecoded)
	require.Zero(t, observer.Snapshot().CertificatesObserved)
}

func TestReceiverRejectsUnadmittedTLSIdentity(t *testing.T) {
	allowedIdentity := ed25519.NewKeyFromSeed(bytesOf(5, ed25519.SeedSize))
	wrongIdentity := ed25519.NewKeyFromSeed(bytesOf(6, ed25519.SeedSize))
	allowedPubkey := testVotorPubkey(allowedIdentity)
	seen := make(chan solana.PublicKey, 1)
	observer := NewObserver()
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:    "127.0.0.1:0",
		LogInterval: -1,
		AdmitPeer: func(peer solana.PublicKey) bool {
			seen <- peer
			return peer == allowedPubkey
		},
	}, observer)
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	msg := NewVoteMessage(NewSkipVote(51), testSignatureSeq(0x33), 1)
	payload, err := EncodeMessage(msg)
	require.NoError(t, err)
	conn := sendVotorQUICDatagram(t, receiver.Addr().String(), wrongIdentity, payload)
	t.Cleanup(func() { _ = conn.CloseWithError(0, "test done") })

	select {
	case peer := <-seen:
		require.Equal(t, testVotorPubkey(wrongIdentity), peer)
	case <-time.After(2 * time.Second):
		t.Fatal("peer admission callback was not invoked")
	}
	require.Eventually(t, func() bool {
		return receiver.Stats().ConnectionsRejected == 1 && conn.Context().Err() != nil
	}, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, receiver.Stats().ConnectionsAccepted)
	require.Zero(t, receiver.Stats().DatagramsReceived)
	require.Zero(t, observer.Snapshot().VotesObserved)
}

func TestReceiverLimitsConnectionsPerAuthenticatedPeer(t *testing.T) {
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(9, ed25519.SeedSize))
	clientPubkey := testVotorPubkey(clientIdentity)
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:        "127.0.0.1:0",
		LogInterval:     -1,
		MaxConnsPerIP:   10,
		MaxConnsPerPeer: 2,
		AdmitPeer:       func(peer solana.PublicKey) bool { return peer == clientPubkey },
	}, NewObserver())
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	first := dialVotorQUIC(t, receiver.Addr().String(), clientIdentity)
	second := dialVotorQUIC(t, receiver.Addr().String(), clientIdentity)
	third := dialVotorQUIC(t, receiver.Addr().String(), clientIdentity)
	t.Cleanup(func() {
		_ = first.CloseWithError(0, "test done")
		_ = second.CloseWithError(0, "test done")
		_ = third.CloseWithError(0, "test done")
	})

	require.Eventually(t, func() bool {
		stats := receiver.Stats()
		return stats.ConnectionsAccepted == 2 && stats.ConnectionsRejected == 1 && third.Context().Err() != nil
	}, 2*time.Second, 10*time.Millisecond)
	receiver.mu.Lock()
	connections := receiver.connsByPeer[clientPubkey]
	receiver.mu.Unlock()
	require.Equal(t, 2, connections)
}

func TestReceiverCountsMalformedVotorDatagram(t *testing.T) {
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(7, ed25519.SeedSize))
	observer := NewObserver()
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:    "127.0.0.1:0",
		LogInterval: -1,
		AdmitPeer:   func(solana.PublicKey) bool { return true },
	}, observer)
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	conn := sendVotorQUICDatagram(t, receiver.Addr().String(), clientIdentity, []byte{0xff})
	t.Cleanup(func() { _ = conn.CloseWithError(0, "test done") })

	require.Eventually(t, func() bool {
		return receiver.Stats().DecodeErrors == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, observer.Snapshot().VotesObserved)
}

func TestReceiverDatagramRateLimit(t *testing.T) {
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(8, ed25519.SeedSize))
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:              "127.0.0.1:0",
		LogInterval:           -1,
		MaxDatagramsPerSecond: 1,
		AdmitPeer:             func(solana.PublicKey) bool { return true },
	}, NewObserver())
	require.NoError(t, err)
	runVotorReceiver(t, receiver)

	msg := NewVoteMessage(NewSkipVote(61), testSignatureSeq(0x22), 1)
	payload, err := EncodeMessage(msg)
	require.NoError(t, err)
	conn := dialVotorQUIC(t, receiver.Addr().String(), clientIdentity)
	t.Cleanup(func() { _ = conn.CloseWithError(0, "test done") })
	require.NoError(t, conn.SendDatagram(payload))
	require.NoError(t, conn.SendDatagram(payload))

	require.Eventually(t, func() bool {
		stats := receiver.Stats()
		return stats.DatagramsReceived == 1 && stats.RateLimitedDatagrams == 1
	}, 2*time.Second, 10*time.Millisecond)
}

func runVotorReceiver(t *testing.T, receiver *Receiver) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- receiver.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, receiver.Close())
		select {
		case err := <-runErr:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Error("Votor receiver did not stop")
		}
	})
}

func sendVotorQUICDatagram(t *testing.T, addr string, identity ed25519.PrivateKey, payload []byte) *quic.Conn {
	t.Helper()
	conn := dialVotorQUIC(t, addr, identity)
	require.NoError(t, conn.SendDatagram(payload))
	return conn
}

func dialVotorQUIC(t *testing.T, addr string, identity ed25519.PrivateKey) *quic.Conn {
	t.Helper()
	cert, err := newVotorQUICCertificate(identity)
	require.NoError(t, err)
	return dialVotorQUICWithCertificate(t, addr, cert)
}

func dialVotorQUICWithCertificate(t *testing.T, addr string, cert tls.Certificate) *quic.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(ctx, addr, &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{VotorQUICALPN},
	}, testVotorQUICConfig())
	require.NoError(t, err)
	require.True(t, conn.ConnectionState().SupportsDatagrams.Local)
	require.True(t, conn.ConnectionState().SupportsDatagrams.Remote)
	return conn
}

func testVotorQUICConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout:    2 * time.Second,
		MaxIdleTimeout:          5 * time.Second,
		KeepAlivePeriod:         2 * time.Second,
		MaxIncomingStreams:      -1,
		MaxIncomingUniStreams:   -1,
		InitialPacketSize:       VotorQUICInitialPacketSize,
		DisablePathMTUDiscovery: true,
		EnableDatagrams:         true,
	}
}

func testVotorPubkey(identity ed25519.PrivateKey) solana.PublicKey {
	var pubkey solana.PublicKey
	copy(pubkey[:], identity.Public().(ed25519.PublicKey))
	return pubkey
}
