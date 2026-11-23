package alpenglow

import (
	"crypto/ed25519"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVotorBroadcasterReconnectsUnderSwappedIdentity(t *testing.T) {
	serverIdentity := ed25519.NewKeyFromSeed(bytesOf(51, ed25519.SeedSize))
	oldIdentity := ed25519.NewKeyFromSeed(bytesOf(52, ed25519.SeedSize))
	newIdentity := ed25519.NewKeyFromSeed(bytesOf(53, ed25519.SeedSize))
	var mu sync.Mutex
	var admitted []solana.PublicKey
	receiver, err := NewReceiver(ReceiverConfig{
		BindAddr:    "127.0.0.1:0",
		Identity:    identityHolder(t, serverIdentity),
		LogInterval: -1,
		AdmitPeer: func(peer solana.PublicKey) bool {
			mu.Lock()
			admitted = append(admitted, peer)
			mu.Unlock()
			return true
		},
	}, NewObserver())
	require.NoError(t, err)
	runVotorReceiver(t, receiver)
	admittedPeer := func(peer solana.PublicKey) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, got := range admitted {
			if got == peer {
				return true
			}
		}
		return false
	}

	holder := identityHolder(t, oldIdentity)
	peer := VotorPeer{Identity: testVotorPubkey(serverIdentity), Addr: cloneUDPAddr(receiver.Addr().(*net.UDPAddr))}
	broadcaster, err := NewVotorBroadcaster(VotorBroadcasterConfig{
		RecordApplicationPayload: true,
		Identity:                 holder,
		Peers:                    func() []VotorPeer { return []VotorPeer{peer} },
		Workers:                  1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, broadcaster.Close()) })
	require.Eventually(t, func() bool { return admittedPeer(testVotorPubkey(oldIdentity)) }, 3*time.Second, 10*time.Millisecond)
	before, ok := broadcaster.establishedConnection(peer)
	require.True(t, ok)
	vote := NewVoteMessage(NewSkipVote(77), testSignatureSeq(0x41), 3)
	wire, err := EncodeMessage(vote)
	require.NoError(t, err)
	require.NoError(t, broadcaster.Enqueue(vote))
	require.Eventually(t, func() bool { return broadcaster.Stats().ApplicationPayload.Bytes == uint64(len(wire)) }, time.Second, 10*time.Millisecond)
	payloadStart := broadcaster.Stats().ApplicationPayload.StartedAt

	_, err = holder.Swap(newIdentity)
	require.NoError(t, err)
	// The updater drops the connection authenticated as the old identity
	// before Swap returns.
	require.Error(t, before.Context().Err())
	require.Eventually(t, func() bool { return admittedPeer(testVotorPubkey(newIdentity)) }, 3*time.Second, 10*time.Millisecond)
	require.NoError(t, broadcaster.Enqueue(vote))
	require.Eventually(t, func() bool { return broadcaster.Stats().ApplicationPayload.Bytes == uint64(2*len(wire)) }, time.Second, 10*time.Millisecond)
	require.Equal(t, payloadStart, broadcaster.Stats().ApplicationPayload.StartedAt)
}
