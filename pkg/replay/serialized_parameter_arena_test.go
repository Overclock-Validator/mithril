package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSerializedParameterArenaResetMetrics(t *testing.T) {
	oldArena, oldMetrics := SerializedParameterArena, metrics.GlobalBlockReplay
	t.Cleanup(func() { SerializedParameterArena, metrics.GlobalBlockReplay = oldArena, oldMetrics })
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
	SerializedParameterArena = nil
	resetSerializedParameterArena()
	require.Zero(t, metrics.GlobalBlockReplay.SerializedParameterArenaReset.Count)
	SerializedParameterArena = arena.New[byte](1024)
	data, ok := SerializedParameterArena.AllocN(17)
	require.True(t, ok)
	for i := range data {
		data[i] = 42
	}
	resetSerializedParameterArena()
	require.EqualValues(t, 1, metrics.GlobalBlockReplay.SerializedParameterArenaReset.Count)
	require.EqualValues(t, 17, metrics.GlobalBlockReplay.SerializedParameterArenaResetBytes)
	require.Zero(t, SerializedParameterArena.Used())
	require.Equal(t, make([]byte, 17), data)
	resetSerializedParameterArena()
	require.EqualValues(t, 2, metrics.GlobalBlockReplay.SerializedParameterArenaReset.Count)
	require.EqualValues(t, 17, metrics.GlobalBlockReplay.SerializedParameterArenaResetBytes)
}
