package replay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func historyPendingWriteTimestamp(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == statsd.TransactionHistoryPendingWriteStarted.String() {
			require.Len(t, family.Metric, 1)
			require.Empty(t, family.Metric[0].Label)
			return family.Metric[0].GetGauge().GetValue()
		}
	}
	t.Fatal("pending history write metric was not published")
	return 0
}

func TestTransactionHistoryPendingWriteTimestamp(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		var want error
		if fail {
			name = "failure"
			want = errors.New("write failed")
		}
		t.Run(name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			released := false
			writer := &TransactionHistorySession{write: func(transactionhistory.Block) error {
				close(entered)
				<-release
				return want
			}}
			defer func() {
				if !released {
					close(release)
				}
				requireHistorySessionStopped(t, writer, want)
			}()
			start := float64(time.Now().UnixNano()) / 1e9
			capture := writer.begin()
			require.NotNil(t, capture)
			capture.Record = &blockMetadata{Slot: 8}
			writer.finish(capture, true)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("writer did not reach blocked write")
			}
			pending := historyPendingWriteTimestamp(t)
			require.GreaterOrEqual(t, pending, start)
			require.LessOrEqual(t, pending, float64(time.Now().UnixNano())/1e9)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			joined, err := writer.Stop(ctx)
			require.False(t, joined)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, pending, historyPendingWriteTimestamp(t), "a timed-out join must not clear a live write")
			close(release)
			released = true
			requireHistorySessionStopped(t, writer, want)
			require.Zero(t, historyPendingWriteTimestamp(t), "joined writes must clear the timestamp, including errors")
		})
	}
	// Conversion failures finish before the store callback but still clear it.
	writer := &TransactionHistorySession{write: func(transactionhistory.Block) error {
		t.Error("invalid captured record reached storage")
		return nil
	}}
	capture := writer.begin()
	capture.Record = &blockMetadata{Slot: 9, Transactions: []*transactionMetadata{nil}}
	writer.finish(capture, true)
	joined, err := writer.Stop(context.Background())
	require.True(t, joined)
	require.Error(t, err)
	require.Zero(t, historyPendingWriteTimestamp(t))
}
