package turbine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/sigverify"
	"github.com/stretchr/testify/require"
)

func TestReceiverEntryPrefetchLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name     string
		disabled bool
		custom   bool
		wantPool bool
	}{
		{name: "default", wantPool: true},
		{name: "disabled", disabled: true},
		{name: "custom verifier", custom: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := sigverify.Cfg.DisableShredOverlap
			sigverify.Cfg.DisableShredOverlap = tc.disabled
			defer func() { sigverify.Cfg.DisableShredOverlap = previous }()
			r := NewUDPReceiver("127.0.0.1:0")
			if tc.custom {
				r.assembler.verifyTransactions = func(context.Context, *block.Block) error { return nil }
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var runErr error
			go func() { runErr = r.Run(ctx); close(done) }()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("receiver did not join on cleanup")
				}
			}()
			select {
			case err := <-r.Ready():
				require.NoError(t, err)
			case <-time.After(3 * time.Second):
				t.Fatal("receiver did not become ready")
			}
			r.assembler.mu.Lock()
			attached := r.assembler.entryPrefetch != nil
			r.assembler.mu.Unlock()
			require.Equal(t, tc.wantPool, attached, "pool must be attached before readiness")
			cancel()
			waitSignal(t, done, "receiver shutdown")
			require.NoError(t, runErr)
			r.assembler.mu.Lock()
			detached := r.assembler.entryPrefetch == nil
			r.assembler.mu.Unlock()
			require.True(t, detached, "shutdown must detach early workers from the assembler")
		})
	}
}

func TestReceiverStartupFailureDoesNotAttachEntryPrefetch(t *testing.T) {
	r := NewUDPReceiver("invalid::udp::address")
	require.Error(t, r.Run(context.Background()))
	require.Error(t, <-r.Ready())
	require.Nil(t, r.assembler.entryPrefetch)
	_, blocksOpen := <-r.Blocks()
	require.False(t, blocksOpen)
}

func TestReceiverShutdownJoinsRepairScheduler(t *testing.T) {
	r := NewUDPReceiver("127.0.0.1:0")
	started := make(chan struct{})
	release := make(chan struct{})
	var first sync.Once
	var releaseOnce sync.Once
	require.NoError(t, r.SetRepairPeerSource(nil, func() []gossip.RepairPeer {
		first.Do(func() { close(started) })
		<-release
		return nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { runErr = r.Run(ctx); close(done) }()
	defer func() {
		releaseOnce.Do(func() { close(release) })
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("receiver did not join on cleanup")
		}
	}()
	select {
	case err := <-r.Ready():
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("receiver did not become ready")
	}
	waitSignal(t, started, "repair scheduler peer lookup")
	cancel()
	select {
	case <-done:
		t.Fatal("receiver returned while repair scheduler was still running")
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	waitSignal(t, done, "repair scheduler joined")
	require.NoError(t, runErr)
}
