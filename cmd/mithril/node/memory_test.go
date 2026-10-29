package node

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestConfigureMemoryBudgets(t *testing.T) {
	oldFold, oldWorking, oldLimit, oldProfile := replay.FoldMaxBytes, replay.WorkingSetMaxBytes, nodeMemoryLimit, heapProfileThreshold
	t.Cleanup(func() {
		viper.Reset()
		replay.FoldMaxBytes, replay.WorkingSetMaxBytes, nodeMemoryLimit, heapProfileThreshold = oldFold, oldWorking, oldLimit, oldProfile
	})
	viper.Reset()
	require.NoError(t, configureMemoryBudgets())
	require.Equal(t, replay.DefaultFoldMaxBytes, replay.FoldMaxBytes)
	require.Equal(t, replay.DefaultWorkingSetMaxBytes, replay.WorkingSetMaxBytes)
	require.Zero(t, nodeMemoryLimit, "omitted setting must preserve GOMEMLIMIT")
	viper.Set("storage.fold_max_mb", 1024)
	viper.Set("storage.working_set_max_mb", 4096)
	viper.Set("tuning.memory_limit_mb", 81920)
	viper.Set("tuning.pprof.heap_profile_threshold_mb", 16384)
	require.NoError(t, configureMemoryBudgets())
	require.Equal(t, uint64(1<<30), replay.FoldMaxBytes)
	require.Equal(t, uint64(4<<30), replay.WorkingSetMaxBytes)
	require.Equal(t, int64(80<<30), nodeMemoryLimit)
	require.Equal(t, uint64(16<<30), heapProfileThreshold)
	for _, tc := range []struct {
		key   string
		value int64
	}{
		{"storage.fold_max_mb", 0},
		{"storage.fold_max_mb", 8192},
		{"storage.working_set_max_mb", -1},
		{"tuning.memory_limit_mb", -1},
		{"tuning.memory_limit_mb", 1 << 43},
		{"tuning.pprof.heap_profile_threshold_mb", -1},
	} {
		old := viper.Get(tc.key)
		viper.Set(tc.key, tc.value)
		require.Error(t, configureMemoryBudgets(), tc.key)
		require.Equal(t, int64(80<<30), nodeMemoryLimit, "invalid configuration must not partially apply")
		viper.Set(tc.key, old)
	}
}

func TestHeapProfileIsReadableAndAtomicallyReplaced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "heap-pressure-0.pprof")
	for range 2 {
		require.NoError(t, writeHeapProfile(path))
		f, err := os.Open(path)
		require.NoError(t, err)
		r, err := gzip.NewReader(f)
		require.NoError(t, err)
		data, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NotEmpty(t, data)
		require.NoError(t, r.Close())
		require.NoError(t, f.Close())
		_, err = os.Stat(path + ".tmp")
		require.True(t, os.IsNotExist(err))
	}
}
