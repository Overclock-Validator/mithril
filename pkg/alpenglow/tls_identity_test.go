package alpenglow

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVotorCertificateMatchesAgaveAndFiredancerTemplate(t *testing.T) {
	// Independent fixture from Agave's constructor, also accepted by
	// Firedancer's fd_x509_mock_pubkey_v2 byte-pattern parser.
	fixture, err := os.ReadFile("testdata/agave_votor_certificate.der")
	require.NoError(t, err)
	require.Len(t, fixture, 249)
	for _, seed := range []byte{61, 62} {
		identity := ed25519.NewKeyFromSeed(bytesOf(seed, ed25519.SeedSize))
		certificate, err := newVotorQUICCertificate(identity)
		require.NoError(t, err)
		expected := append([]byte(nil), fixture...)
		copy(expected[100:132], identity.Public().(ed25519.PublicKey))
		require.Equal(t, expected, certificate.Certificate[0])
		require.Equal(t, identity.Public(), certificate.Leaf.PublicKey)
		// Do not replace the dummy signature with a real one: Firedancer's
		// parser matches it too. Authentication happens in CertificateVerify.
		require.Error(t, certificate.Leaf.CheckSignature(certificate.Leaf.SignatureAlgorithm,
			certificate.Leaf.RawTBSCertificate, certificate.Leaf.Signature))
	}
}

func TestVotorCertificateStillRequiresIdentityKeyPossession(t *testing.T) {
	identity := ed25519.NewKeyFromSeed(bytesOf(63, ed25519.SeedSize))
	certificate, err := newVotorQUICCertificate(identity)
	require.NoError(t, err)
	certificate.PrivateKey = ed25519.NewKeyFromSeed(bytesOf(64, ed25519.SeedSize))
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { _ = serverConn.Close(); _ = clientConn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server := tls.Server(serverConn, &tls.Config{
		Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13,
	})
	client := tls.Client(clientConn, &tls.Config{
		InsecureSkipVerify: true, MinVersion: tls.VersionTLS13,
	})
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.HandshakeContext(ctx) }()
	// Skipping the dummy X.509 signature does not bypass CertificateVerify.
	require.ErrorContains(t, client.HandshakeContext(ctx), "invalid signature")
	require.Error(t, <-serverDone)
}

func TestVotorPeerIdentityRequiresOneEd25519Certificate(t *testing.T) {
	identity := ed25519.NewKeyFromSeed(bytesOf(61, ed25519.SeedSize))
	certificate, err := newVotorQUICCertificate(identity)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	require.NoError(t, err)
	_, err = votorPeerIdentity(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}})
	require.ErrorContains(t, err, "not complete")

	got, err := votorPeerIdentity(tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{leaf}})
	require.NoError(t, err)
	require.Equal(t, testVotorPubkey(identity), got)

	_, err = votorPeerIdentity(tls.ConnectionState{HandshakeComplete: true})
	require.ErrorContains(t, err, "exactly one")
	_, err = votorPeerIdentity(tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{leaf, leaf}})
	require.ErrorContains(t, err, "exactly one")
}

func TestVotorQUICConfigMatchesAgaveDatagramTransport(t *testing.T) {
	cfg := newVotorQUICConfig()
	require.True(t, cfg.EnableDatagrams)
	require.EqualValues(t, -1, cfg.MaxIncomingStreams)
	require.EqualValues(t, -1, cfg.MaxIncomingUniStreams)
	require.Equal(t, VotorQUICInitialPacketSize, cfg.InitialPacketSize)
	require.True(t, cfg.DisablePathMTUDiscovery)
	require.Equal(t, 5*time.Second, cfg.MaxIdleTimeout)
	require.Equal(t, 2*time.Second, cfg.KeepAlivePeriod)
	require.Equal(t, 2*time.Second, cfg.HandshakeIdleTimeout)

	receiverCfg := DefaultReceiverConfig()
	require.Equal(t, 50, receiverCfg.MaxDatagramsPerSecond)
	require.Equal(t, 2, receiverCfg.MaxConnsPerPeer)
	require.EqualValues(t, VotorQUICInitialPacketSize, receiverCfg.MaxMessageBytes)
}

func testUntrustedVotorCertificate(t *testing.T, identity ed25519.PrivateKey) tls.Certificate {
	t.Helper()
	certificate, err := newVotorQUICCertificate(identity)
	require.NoError(t, err)
	certificate.Certificate[0] = append([]byte(nil), certificate.Certificate[0]...)
	// Agave's generated validator certificate deliberately has an invalid X.509
	// self-signature. TLS CertificateVerify, not WebPKI, proves key possession.
	certificate.Certificate[0][len(certificate.Certificate[0])-1] ^= 0xff
	certificate.Leaf, err = x509.ParseCertificate(certificate.Certificate[0])
	require.NoError(t, err)
	require.Error(t, certificate.Leaf.CheckSignature(
		certificate.Leaf.SignatureAlgorithm,
		certificate.Leaf.RawTBSCertificate,
		certificate.Leaf.Signature,
	))
	return certificate
}
