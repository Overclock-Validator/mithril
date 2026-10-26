package harmonic

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	authpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestParseEndpoint(t *testing.T) {
	cases := map[string]struct {
		raw, sni string
		want     Endpoint
	}{
		"https default port":   {"https://be.example.com", "", Endpoint{Target: "be.example.com:443", TLS: true, ServerName: "be.example.com"}},
		"https explicit port":  {"https://be.example.com:8443/", "", Endpoint{Target: "be.example.com:8443", TLS: true, ServerName: "be.example.com"}},
		"server name override": {"https://10.0.0.1:443", "be.example.com", Endpoint{Target: "10.0.0.1:443", TLS: true, ServerName: "be.example.com"}},
		"plaintext":            {"http://127.0.0.1:5999", "", Endpoint{Target: "127.0.0.1:5999", ServerName: "127.0.0.1"}},
		"plaintext default":    {"http://relayer.local", "", Endpoint{Target: "relayer.local:80", ServerName: "relayer.local"}},
	}
	for name, tc := range cases {
		got, err := ParseEndpoint(tc.raw, tc.sni)
		require.NoError(t, err, name)
		assert.Equal(t, tc.want, got, name)
	}
	for _, raw := range []string{"grpc://be.example.com", "be.example.com:443", "https://", "://bad"} {
		_, err := ParseEndpoint(raw, "")
		assert.Errorf(t, err, "%q", raw)
	}
}

func selfSignedTLS(t *testing.T, host string) credentials.TransportCredentials {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: host},
		DNSNames:     []string{host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	return credentials.NewServerTLSFromCert(&tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key})
}

func TestDialTLSVerifiesCertificateUnlessDisabled(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.Creds(selfSignedTLS(t, "be.test")))
	auth := newMockAuth(time.Now)
	authpb.RegisterAuthServiceServer(srv, auth)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	ep, err := ParseEndpoint("https://"+lis.Addr().String(), "be.test")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = Dial(ctx, DialConfig{Endpoint: ep})
	require.Error(t, err, "an untrusted certificate is refused by default")

	conn, err := Dial(context.Background(), DialConfig{Endpoint: ep, InsecureSkipVerify: true})
	require.NoError(t, err)
	defer conn.Close()
	_, err = authpb.NewAuthServiceClient(conn).GenerateAuthChallenge(context.Background(), &authpb.GenerateAuthChallengeRequest{
		Role: authpb.Role_VALIDATOR, Pubkey: make([]byte, 32),
	})
	require.NoError(t, err)
}

func TestDialFailsWithinTheConnectDeadline(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	ep, err := ParseEndpoint("http://"+addr, "")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = Dial(ctx, DialConfig{Endpoint: ep})
	require.Error(t, err)
	assert.Less(t, time.Since(start), 5*time.Second)
}
