package replay

import (
	"errors"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func historyCaptureCount(t *testing.T, result string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != statsd.TransactionHistoryCaptures.String() {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "result" && label.GetValue() == result {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func TestTransactionHistoryRetentionRetryAndMetrics(t *testing.T) {
	before := make(map[string]float64)
	for _, result := range []string{"started", "busy", "retention_refused", "written", "capture_refused"} {
		before[result] = historyCaptureCount(t, result)
	}
	attempts := 0
	session := &TransactionHistorySession{write: func(transactionhistory.Block) error {
		attempts++
		if attempts == 1 {
			return errors.Join(errors.New("space reserve"), transactionhistory.ErrRetention)
		}
		return nil
	}}
	defer requireHistorySessionStopped(t, session, nil)
	for slot := uint64(8); slot <= 9; slot++ {
		capture := session.begin()
		require.NotNil(t, capture, "retention refusal must not stop the session")
		require.Nil(t, session.begin(), "one capture at a time")
		capture.Record = &blockMetadata{Slot: slot}
		session.finish(capture, true)
		select {
		case <-session.pending.done:
		case <-time.After(time.Second):
			t.Fatal("writer did not finish")
		}
		require.NoError(t, session.poll())
	}
	capture := session.begin()
	require.NotNil(t, capture)
	capture.Err = errMetadataBudget
	session.acceptBlock(nil)
	require.Equal(t, 2, attempts)
	for result, delta := range map[string]float64{"started": 3, "busy": 2, "retention_refused": 1, "written": 1, "capture_refused": 1} {
		require.Equal(t, before[result]+delta, historyCaptureCount(t, result), result)
	}
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var writeObserved bool
	for _, family := range families {
		if family.GetName() == statsd.TransactionHistoryWrite.String() {
			for _, metric := range family.Metric {
				writeObserved = metric.GetHistogram().GetSampleCount() >= 2
				require.Positive(t, metric.GetHistogram().GetSampleSum())
			}
		}
	}
	require.True(t, writeObserved, "write duration must reach the metrics registry")
}

func TestTransactionHistoryStorageMetrics(t *testing.T) {
	store, err := transactionhistory.Open(t.TempDir(), [32]byte{1}, transactionhistory.DefaultLimits())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	session, err := NewTransactionHistorySession(store, "verifying")
	require.NoError(t, err)
	defer requireHistorySessionStopped(t, session, nil)
	capture := session.begin()
	require.NotNil(t, capture)
	capture.Record = capturedHistoryBlock(t)
	session.finish(capture, true)
	requireHistorySessionStopped(t, session, nil)
	state, err := store.Stats()
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.Blocks)
	require.Positive(t, state.LogicalBytes)
	want := map[string]float64{
		statsd.TransactionHistoryBlocks.String():    float64(state.Blocks),
		statsd.TransactionHistoryBytes.String():     float64(state.LogicalBytes),
		statsd.TransactionHistoryFirstSlot.String(): float64(state.FirstSlot),
		statsd.TransactionHistoryLastSlot.String():  float64(state.LastSlot),
	}
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if value, ok := want[family.GetName()]; ok {
			require.Len(t, family.Metric, 1)
			require.Equal(t, value, family.Metric[0].GetGauge().GetValue(), family.GetName())
			delete(want, family.GetName())
		}
	}
	require.Empty(t, want, "storage gauges must reach the metrics registry")
}
