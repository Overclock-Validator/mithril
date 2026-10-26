package quicserver

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/identity"
)

// newServerTLSConfig presents the most recently refreshed identity
// certificate on every handshake, so a set-identity swap rotates the TPU
// certificate without rebinding the listeners.
func newServerTLSConfig(certs *identity.Derived[tls.Certificate]) *tls.Config {
	return &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert := certs.Get()
			return &cert, nil
		},
		ClientAuth: tls.RequireAnyClientCert,
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{ALPNTPUProtocolID},
	}
}

func newIdentityCertificate(key ed25519.PrivateKey) (tls.Certificate, error) {
	if len(key) != ed25519.PrivateKeySize {
		return tls.Certificate{}, fmt.Errorf("invalid identity key size %d", len(key))
	}
	pub := key.Public().(ed25519.PublicKey)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "Solana node",
		},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Date(4096, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{"localhost"},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	leaf, err := x509.ParseCertificate(certDER)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
		Leaf:        leaf,
	}, nil
}
