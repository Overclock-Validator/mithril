package replay

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/stretchr/testify/require"
)

func TestHistoryAdmissionBackpressureCancellation(t *testing.T) {
	var disabled *TransactionHistorySession
	require.NoError(t, disabled.wait(context.Background()))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if err := disabled.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	session := &TransactionHistorySession{write: func(transactionhistory.Block) error {
		close(entered)
		<-release
		return nil
	}}
	defer func() {
		unblock()
		requireHistorySessionStopped(t, session, nil)
	}()
	capture := session.begin()
	capture.Record = &blockMetadata{Slot: 8}
	session.finish(capture, true)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("write did not start")
	}
	pending := session.pending
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, session.wait(ctx), context.Canceled)
	require.Same(t, pending, session.pending, "cancellation must not release worker ownership")
	require.Nil(t, session.err, "replay cancellation must not become a sticky store failure")
	require.False(t, session.stopped)
	require.Nil(t, session.begin(), "a retry cannot admit while the same job is pending")
	joined, err := session.Stop(ctx)
	require.False(t, joined)
	require.ErrorIs(t, err, context.Canceled)
	require.Same(t, pending, session.pending)
	unblock()
	requireHistorySessionStopped(t, session, nil)
	require.NoError(t, session.wait(context.Background()))
	require.Nil(t, session.begin(), "Stop still prevents later admission")
}

func TestHistoryAdmissionBackpressureErrors(t *testing.T) {
	for _, kind := range []string{"completed", "retention", "write", "conversion"} {
		t.Run(kind, func(t *testing.T) {
			failure := errors.New("held history write failed")
			writes := 0
			session := &TransactionHistorySession{write: func(transactionhistory.Block) error {
				writes++
				switch kind {
				case "retention":
					return transactionhistory.ErrRetention
				case "write":
					return failure
				default:
					return nil
				}
			}}
			capture := session.begin()
			capture.Record = &blockMetadata{Slot: 8}
			if kind == "conversion" {
				capture.Record.Transactions = []*transactionMetadata{nil}
			}
			session.finish(capture, true)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := session.wait(ctx)
			require.Nil(t, session.pending)
			if kind == "write" || kind == "conversion" {
				require.Error(t, err)
				if kind == "write" {
					require.ErrorIs(t, err, failure)
					require.Equal(t, 1, writes)
				} else {
					require.ErrorContains(t, err, "missing transaction capture")
					require.Zero(t, writes)
				}
				require.ErrorIs(t, session.wait(context.Background()), err)
				require.Nil(t, session.begin())
				requireHistorySessionStopped(t, session, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, writes)
			require.NotNil(t, session.begin(), "completion and retention pressure must permit another capture")
			requireHistorySessionStopped(t, session, nil)
		})
	}
}
