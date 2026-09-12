package alpenglow

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/quic-go/quic-go"
)

func newVotorQUICConfig() *quic.Config {
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

// newVotorQUICCertificate creates the single-certificate Ed25519 identity
// chain used by the Agave Votor transport. Peers recover the validator identity
// directly from the leaf certificate's SubjectPublicKeyInfo; TLS 1.3's
// CertificateVerify proves possession of the corresponding private key.
func newVotorQUICCertificate(identity ed25519.PrivateKey) (tls.Certificate, error) {
	priv := identity
	var pub ed25519.PublicKey
	if len(priv) == 0 {
		var err error
		pub, priv, err = ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return tls.Certificate{}, err
		}
	} else {
		if len(priv) != ed25519.PrivateKeySize {
			return tls.Certificate{}, fmt.Errorf("invalid Votor QUIC identity size %d", len(priv))
		}
		pub = priv.Public().(ed25519.PublicKey)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "Mithril Alpenglow observer",
		},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Date(4096, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost"},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		return tls.Certificate{}, err
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  priv,
		Leaf:        cert,
	}, nil
}

// votorPeerIdentity extracts the validator identity from an authenticated TLS
// connection. Agave deliberately accepts only a single Ed25519 certificate so
// certificate chains cannot make the identity ambiguous.
func votorPeerIdentity(state tls.ConnectionState) (solana.PublicKey, error) {
	if !state.HandshakeComplete {
		return solana.PublicKey{}, fmt.Errorf("Votor TLS handshake is not complete")
	}
	if len(state.PeerCertificates) != 1 {
		return solana.PublicKey{}, fmt.Errorf("Votor peer presented %d certificates, want exactly one", len(state.PeerCertificates))
	}
	pub, ok := state.PeerCertificates[0].PublicKey.(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return solana.PublicKey{}, fmt.Errorf("Votor peer certificate does not contain an Ed25519 public key")
	}
	var identity solana.PublicKey
	copy(identity[:], pub)
	return identity, nil
}
