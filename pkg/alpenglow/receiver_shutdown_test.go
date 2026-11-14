package alpenglow

import (
	"context"
	"crypto/ed25519"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestReceiverCloseDuringAcceptedPeerAdmission(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	serverIdentity := ed25519.NewKeyFromSeed(bytesOf(181, ed25519.SeedSize))
	r, err := NewReceiver(ReceiverConfig{
		BindAddr: "127.0.0.1:0", Identity: identityHolder(t, serverIdentity), LogInterval: -1,
		AdmitPeer: func(solana.PublicKey) bool {
			close(entered)
			<-release
			return true
		},
	}, NewObserver())
	require.NoError(t, err)
	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(context.Background()) }()
	clientIdentity := ed25519.NewKeyFromSeed(bytesOf(182, ed25519.SeedSize))
	conn := dialVotorQUIC(t, r.Addr().String(), clientIdentity)
	defer conn.CloseWithError(0, "test complete")
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		unblock()
		t.Fatal("peer admission did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		unblock()
		t.Fatal("receiver close blocked on peer admission")
	}
	unblock()
	select {
	case <-conn.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("peer did not observe receiver close")
	}
	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("receiver did not stop")
	}
}

func TestReceiverCloseBeforeRun(t *testing.T) {
	key := ed25519.NewKeyFromSeed(bytesOf(181, ed25519.SeedSize))
	r, err := NewReceiver(ReceiverConfig{BindAddr: "127.0.0.1:0", Identity: identityHolder(t, key)}, NewObserver())
	require.NoError(t, err)
	addr := r.Addr().String()
	require.NoError(t, r.Close())
	require.NoError(t, r.Run(context.Background()))
	require.Eventually(t, func() bool {
		listener, err := net.ListenPacket("udp", addr)
		if err != nil {
			return false
		}
		return listener.Close() == nil
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, (&Receiver{}).Close())
}

func TestReceiverCloseFromAdmission(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	serverKey := ed25519.NewKeyFromSeed(bytesOf(181, ed25519.SeedSize))
	var r *Receiver
	var err error
	r, err = NewReceiver(ReceiverConfig{
		BindAddr: "127.0.0.1:0", Identity: identityHolder(t, serverKey), LogInterval: -1,
		AdmitPeer: func(solana.PublicKey) bool {
			close(entered)
			<-release
			_ = r.Close()
			return true
		},
	}, NewObserver())
	require.NoError(t, err)
	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(context.Background()) }()
	clientKey := ed25519.NewKeyFromSeed(bytesOf(182, ed25519.SeedSize))
	conn := dialVotorQUIC(t, r.Addr().String(), clientKey)
	defer conn.CloseWithError(0, "test complete")
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		unblock()
		t.Fatal("peer admission did not start")
	}
	unblock()
	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("receiver deadlocked during admission")
	}
	select {
	case <-conn.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("peer did not observe receiver close")
	}
}
