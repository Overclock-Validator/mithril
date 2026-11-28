package replay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/stretchr/testify/require"
)

func requireHistorySessionStopped(t *testing.T, writer *TransactionHistorySession, want error) {
	t.Helper()
	joined, err := writer.Stop(context.Background())
	require.True(t, joined)
	if want == nil {
		require.NoError(t, err)
	} else {
		require.ErrorIs(t, err, want)
	}
}

func TestTransactionHistoryWriterDisabled(t *testing.T) {
	require.Zero(t, testing.AllocsPerRun(100, func() {
		writer, err := NewTransactionHistorySession(nil, "verifying")
		if err != nil || writer.begin() != nil || writer.poll() != nil {
			t.Fatal("disabled writer admitted work")
		}
		joined, err := writer.Stop(context.Background())
		if !joined || err != nil {
			t.Fatal("disabled writer did not stop")
		}
		writer.finish(nil, true)
	}))
}

func TestTransactionHistoryWriterCaptureOwnership(t *testing.T) {
	writes := 0
	writer := &TransactionHistorySession{write: func(transactionhistory.Block) error {
		writes++
		return nil
	}}
	capture := writer.begin()
	require.NotNil(t, capture)
	require.Nil(t, writer.begin())
	foreign := &blockMetadataCapture{Record: &blockMetadata{Slot: 1}}
	writer.finish(foreign, true)
	require.Same(t, capture, writer.current)
	require.NotNil(t, foreign.Record)

	// A finalized record can still belong to a rejected stream or block.
	capture.Record = &blockMetadata{Slot: 2}
	writer.finish(capture, false)
	require.Nil(t, capture.Record)
	require.Nil(t, writer.pending)
	other := writer.begin()
	require.NotNil(t, other)
	writer.finish(capture, true)
	require.Same(t, other, writer.current, "stale finish cannot release a new capture")
	writer.finish(other, true)
	require.Nil(t, writer.pending, "missing seal must not be written")

	failed := writer.begin()
	failed.Err = errors.New("capture ceiling")
	failed.Record = &blockMetadata{Slot: 3}
	writer.finish(failed, true)
	require.Nil(t, failed.Record)
	require.NotNil(t, writer.begin(), "one capture refusal must not disable the writer")
	requireHistorySessionStopped(t, writer, nil)
	require.Nil(t, writer.current)
	require.Nil(t, writer.begin())
	require.Zero(t, writes)
}

func TestTransactionHistoryWriterBusyUntilAcknowledged(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	released := false
	writer := &TransactionHistorySession{write: func(transactionhistory.Block) error {
		close(entered)
		<-release
		return nil
	}}
	defer func() {
		if !released {
			close(release)
		}
		requireHistorySessionStopped(t, writer, nil)
	}()
	capture := writer.begin()
	capture.Record = &blockMetadata{Slot: 8}
	writer.finish(capture, true)
	require.Nil(t, capture.Record)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not reach the blocked write")
	}
	require.Nil(t, writer.begin())
	require.NoError(t, writer.poll())
	writer.finish(capture, true)
	require.NotNil(t, writer.pending, "duplicate finish cannot retire an in-flight write")
	job := writer.pending
	close(release)
	released = true
	select {
	case <-job.done:
	case <-time.After(time.Second):
		t.Fatal("released write did not finish")
	}
	next := writer.begin()
	require.NotNil(t, next, "acknowledged write releases admission")
	writer.finish(capture, false)
	require.Same(t, next, writer.current)
	writer.finish(next, false)
}

func TestTransactionHistoryWriterStopTimeoutAndJoin(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	released := false
	writer := &TransactionHistorySession{write: func(transactionhistory.Block) error {
		close(entered)
		<-release
		return nil
	}}
	defer func() {
		if !released {
			close(release)
		}
		requireHistorySessionStopped(t, writer, nil)
	}()
	capture := writer.begin()
	capture.Record = &blockMetadata{Slot: 8}
	writer.finish(capture, true)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not reach the blocked write")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	joined, err := writer.Stop(ctx)
	require.False(t, joined)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, writer.pending)
	require.Nil(t, writer.begin())
	close(release)
	released = true
	requireHistorySessionStopped(t, writer, nil)
	require.Nil(t, writer.pending)
	requireHistorySessionStopped(t, writer, nil)
}

func TestTransactionHistoryWriterStickyFailure(t *testing.T) {
	for _, conversion := range []bool{false, true} {
		t.Run(map[bool]string{false: "write", true: "conversion"}[conversion], func(t *testing.T) {
			failure := errors.New("write failed")
			writes := 0
			writer := &TransactionHistorySession{write: func(transactionhistory.Block) error {
				writes++
				return failure
			}}
			capture := writer.begin()
			capture.Record = &blockMetadata{Slot: 8}
			if conversion {
				capture.Record.Transactions = []*transactionMetadata{nil}
			}
			writer.finish(capture, true)
			select {
			case <-writer.pending.done:
			case <-time.After(time.Second):
				t.Fatal("failed write did not finish")
			}
			err := writer.poll()
			require.Error(t, err)
			require.False(t, writer.stopped, "failure must stop admission without requiring stop")
			if conversion {
				require.Zero(t, writes)
			} else {
				require.ErrorIs(t, err, failure)
				require.Equal(t, 1, writes)
			}
			require.ErrorIs(t, writer.poll(), err)
			require.Nil(t, writer.begin())
			requireHistorySessionStopped(t, writer, err)
		})
	}
}

func TestTransactionHistoryWriterCaptureToStore(t *testing.T) {
	store, err := transactionhistory.Open(t.TempDir(), [32]byte{1}, transactionhistory.DefaultLimits())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	writer, err := NewTransactionHistorySession(store, "verifying")
	require.NoError(t, err)
	capture := writer.begin()
	capture.Record = capturedHistoryBlock(t)
	want, err := capture.Record.historyBlock()
	require.NoError(t, err)
	writer.finish(capture, true)
	requireHistorySessionStopped(t, writer, nil)
	require.Nil(t, capture.Record)

	// The writer does not own Store.Close; a joined writer leaves it readable.
	view, err := store.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	got, err := view.Block(want.ID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
