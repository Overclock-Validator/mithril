package alpenglow

import (
	"crypto/ed25519"
	"sync"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestReceiverCloseWaitsForMessageAdmission(t *testing.T) {
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:    "127.0.0.1:0",
		LogInterval: -1,
		AdmitMessage: func(_ solana.PublicKey, msg Message) (Message, bool) {
			close(entered)
			<-release
			return msg, true
		},
		OnMessage: func(Message) { close(finished) },
	}, NewObserver())
	require.NoError(t, err)
	runVotorReceiver(t, receiver)
	t.Cleanup(unblock) // Release admission before the helper's receiver cleanup.
	payload, err := EncodeMessage(NewVoteMessage(NewSkipVote(42), testSignatureSeq(0x22), 1))
	require.NoError(t, err)
	identity := ed25519.NewKeyFromSeed(bytesOf(21, ed25519.SeedSize))
	conn := sendVotorQUICDatagram(t, receiver.Addr().String(), identity, payload)
	t.Cleanup(func() { require.NoError(t, conn.CloseWithError(0, "test done")) })
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("message admission was not reached")
	}

	closed := make(chan error, 2)
	go func() { closed <- receiver.Close() }()
	require.Eventually(t, receiver.isClosed, time.Second, time.Millisecond)
	go func() { closed <- receiver.Close() }() // Already-closed callers must also join.
	select {
	case err := <-closed:
		t.Fatalf("Close returned while message admission was blocked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	for range 2 {
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("Close did not finish after releasing message admission")
		}
	}
	select {
	case <-finished:
	default:
		t.Fatal("Close returned before the admitted message callback completed")
	}
}
