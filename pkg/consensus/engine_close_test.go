package consensus

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/stretchr/testify/require"
)

func TestEngineCloseReleasesReceiverAfterVoterCloseError(t *testing.T) {
	receiver, err := alpenglow.NewReceiver(alpenglow.ReceiverConfig{BindAddr: "127.0.0.1:0"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := receiver.Close(); err != nil {
			t.Errorf("close receiver: %v", err)
		}
	})
	addr := receiver.Addr().String()
	historyLock, err := os.Create(filepath.Join(t.TempDir(), "history.lock"))
	if err != nil {
		t.Fatal(err)
	}
	if err := historyLock.Close(); err != nil {
		t.Fatal(err)
	}
	// The real voter close returns os.ErrClosed from the already-closed lock.
	engine := &AlpenglowObserverEngine{
		receiver: receiver,
		voter:    &alpenglowVoter{done: make(chan struct{}), historyLock: historyLock},
	}
	if err := engine.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("engine close error = %v, want %v", err, os.ErrClosed)
	}
	// QUIC releases its owned UDP transport asynchronously after listener close.
	var rebound net.PacketConn
	require.Eventually(t, func() bool {
		rebound, err = net.ListenPacket("udp4", addr)
		return err == nil
	}, time.Second, time.Millisecond, "receiver port still bound after failed voter close")
	if err := rebound.Close(); err != nil {
		t.Errorf("close rebound socket: %v", err)
	}
}
