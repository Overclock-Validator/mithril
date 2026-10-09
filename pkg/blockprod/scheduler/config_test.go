package scheduler

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/stretchr/testify/require"
)

func TestConfiguredQueueCapacityAndPreparation(t *testing.T) {
	defaults := NewWithConfig(nil, Config{})
	require.Equal(t, MaxBufferedTxns, defaults.buffer.Capacity())
	feats := features.NewFeaturesDefault()
	custom := NewWithConfig(nil, Config{MaxBufferedTransactions: 2, FeatureSource: func() *features.Features { return feats }})
	require.Equal(t, 2, custom.buffer.Capacity())
	require.NotNil(t, custom.preparer.Load())
	for i := byte(1); i <= 2; i++ {
		result, _ := custom.buffer.Insert(testEntry(10, uint64(i), i))
		require.Equal(t, InsertAccepted, result)
	}
	result, evicted := custom.buffer.Insert(testEntry(9, 3, 3))
	require.Equal(t, InsertRejectedCapacity, result)
	require.Nil(t, evicted)
	result, evicted = custom.buffer.Insert(testEntry(11, 4, 4))
	require.Equal(t, InsertAccepted, result)
	require.Equal(t, uint64(2), evicted.seq)
	require.Equal(t, 2, custom.Buffered())
}
