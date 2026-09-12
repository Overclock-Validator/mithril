package alpenglow

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
