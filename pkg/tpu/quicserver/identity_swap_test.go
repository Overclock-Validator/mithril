package quicserver_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/tpu/quicserver"
	"github.com/Overclock-Validator/mithril/pkg/tpu/quicserver/testutils"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func newTestIdentity() (*identity.Holder, error) {
	return identity.NewEphemeralHolder()
}

func TestServerPresentsSwappedIdentityToNewConnections(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer udpConn.Close()
	holder, err := identity.NewEphemeralHolder()
	require.NoError(t, err)
	result, err := quicserver.Spawn(ctx, "identity-swap", []quicserver.QuicSocket{quicserver.QuicSocketFromUDP(udpConn)}, holder, quicserver.DefaultServerConfig())
	require.NoError(t, err)
	defer result.Close(context.Background())

	client, err := testutils.NewClient()
	require.NoError(t, err)
	serverIdentity := func() solana.PublicKey {
		conn, err := client.Dial(ctx, udpConn.LocalAddr().String())
		require.NoError(t, err)
		defer conn.CloseWithError(0, "")
		certs := conn.ConnectionState().TLS.PeerCertificates
		require.Len(t, certs, 1)
		return solana.PublicKeyFromBytes(certs[0].PublicKey.(ed25519.PublicKey))
	}
	require.Equal(t, holder.PublicKey(), serverIdentity())

	_, next, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, err = holder.Swap(next)
	require.NoError(t, err)
	require.Equal(t, solana.PublicKeyFromBytes(next.Public().(ed25519.PublicKey)), serverIdentity())
}
