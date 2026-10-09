package alpenglow

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Real authenticated connection with no reconciliation loop or connection
// workers. Any reconnect job must come from the sender, never the periodic tick.
func passiveVotorSender(t *testing.T) (*VotorBroadcaster, *votorPeerSender, *Receiver) {
	t.Helper()
	serverIdentity := ed25519.NewKeyFromSeed(bytesOf(181, ed25519.SeedSize))
	r, err := NewReceiver(ReceiverConfig{
		BindAddr: "127.0.0.1:0", Identity: serverIdentity, LogInterval: -1,
		AdmitPeer: func(solana.PublicKey) bool { return true },
	}, NewObserver())
	require.NoError(t, err)
	runVotorReceiver(t, r)
	cert, err := newVotorQUICCertificate(ed25519.NewKeyFromSeed(bytesOf(182, ed25519.SeedSize)))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	peer := VotorPeer{Identity: testVotorPubkey(serverIdentity), Addr: r.Addr().(*net.UDPAddr)}
	b := &VotorBroadcaster{
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
		tlsConfig:  &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{VotorQUICALPN}, MinVersion: tls.VersionTLS13, InsecureSkipVerify: true},
		quicConfig: newVotorQUICConfig(),
		jobs:       make(chan votorPeerJob, 2), desired: map[solana.PublicKey]VotorPeer{peer.Identity: peer},
		conns: make(map[solana.PublicKey]votorConnection), dialing: make(map[solana.PublicKey]*votorDial), connectQueued: make(map[solana.PublicKey]struct{}),
	}
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	_, err = b.connection(peer)
	require.NoError(t, err)
	b.connMu.Lock()
	sender := b.conns[peer.Identity].sender
	b.connMu.Unlock()
	return b, sender, r
}

func TestVotorPeerReconnectOnRemoteCloseWithoutReconcile(t *testing.T) {
	for _, duringDequeue := range []bool{false, true} {
		name := "idle"
		if duringDequeue {
			name = "dequeue"
		}
		t.Run(name, func(t *testing.T) {
			b, s, receiver := passiveVotorSender(t)
			if duringDequeue {
				func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					s.queue <- votorDatagram{payload: []byte{1}, queuedAt: time.Now()}
					// Force the job branch to win select, then close remotely
					// while the sender is waiting to check the connection state.
					require.Eventually(t, func() bool { return len(s.queue) == 0 }, time.Second, time.Millisecond)
					require.NoError(t, receiver.Close())
					select {
					case <-s.conn.Context().Done():
					case <-time.After(time.Second):
						t.Fatal("remote close not observed")
					}
				}()
			} else {
				require.NoError(t, receiver.Close())
			}
			select {
			case <-s.done:
			case <-time.After(time.Second):
				t.Fatal("sender did not exit")
			}
			select {
			case job := <-b.jobs:
				require.Equal(t, s.peer.Identity, job.peer.Identity)
			default:
				t.Fatal("sender exited without requesting reconnect")
			}
			require.Empty(t, b.jobs, "only one reconnect request per peer")
		})
	}
}

func TestVotorPeerDeadlineIncludesQueueAge(t *testing.T) {
	for _, tc := range []struct {
		name            string
		queued, sending time.Duration
		active, expired bool
	}{
		{"progress_does_not_reset_age", 950 * time.Millisecond, 100 * time.Millisecond, true, true},
		{"below_deadline", 800 * time.Millisecond, 100 * time.Millisecond, true, false},
		{"blocked_call", 0, time.Second, true, true},
		{"completed_send", 2 * time.Second, 0, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, s, _ := passiveVotorSender(t)
			now := time.Now()
			s.mu.Lock()
			if tc.active {
				s.sendingSince = now.Add(-tc.sending)
			}
			s.lastQueueDelay = tc.queued
			s.mu.Unlock()
			// Inject a deterministic clock/state boundary. There is no actual
			// datagram in progress and no timer goroutine in this fixture.
			b.expirePeerSends(now)
			require.Equal(t, tc.expired, s.conn.Context().Err() != nil)
			if tc.expired {
				require.EqualValues(t, 1, b.peerSendTimeouts.Load())
				b.expirePeerSends(now.Add(time.Second))
				require.EqualValues(t, 1, b.peerSendTimeouts.Load())
			} else {
				require.Zero(t, b.peerSendTimeouts.Load())
			}
		})
	}
}

func TestVotorPeerRejectsAgedQueueBeforeQUICEnqueue(t *testing.T) {
	b, s, _ := passiveVotorSender(t)
	s.enqueue(votorDatagram{payload: []byte{1}, queuedAt: time.Now().Add(-2 * time.Second)})
	select {
	case <-s.done:
	case <-time.After(time.Second):
		t.Fatal("aged queue did not retire its connection")
	}
	require.Error(t, s.conn.Context().Err())
	require.Zero(t, b.sends.Load(), "stale backlog must not enter the QUIC queue")
	require.EqualValues(t, 1, b.peerSendTimeouts.Load())
	require.EqualValues(t, 1, b.peerQueueDiscarded.Load())
	require.Len(t, b.jobs, 1)
}
