package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestStorageGuardChecksEveryWritingVolume(t *testing.T) {
	paths := []string{"accounts", "shredstore", "logs"}
	for _, low := range paths {
		err := checkStorageSpace(paths, 32<<30, func(path string) (uint64, error) {
			if path == low {
				return 31 << 30, nil
			}
			return 100 << 30, nil
		})
		require.ErrorContains(t, err, low)
	}
	require.NoError(t, checkStorageSpace(paths, 32<<30, func(string) (uint64, error) { return 32 << 30, nil }))
	require.Error(t, checkStorageSpace(paths, 32<<30, func(string) (uint64, error) { return 0, errors.New("stat failed") }))
}

func TestStorageGuardCancelsRunAndJoins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := startStorageSpaceGuard(ctx, func() error { return errors.New("low disk") }, cancel)
	defer stop()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("storage guard did not stop replay")
	}
	stop()
}

func TestConfigureDiskBudget(t *testing.T) {
	old := storageMinFreeBytes
	t.Cleanup(func() { viper.Reset(); storageMinFreeBytes = old })
	viper.Reset()
	require.NoError(t, configureDiskBudget())
	require.Equal(t, uint64(32<<30), storageMinFreeBytes)
	viper.Set("storage.min_free_mb", 65536)
	require.NoError(t, configureDiskBudget())
	require.Equal(t, uint64(64<<30), storageMinFreeBytes)
	for _, invalid := range []int64{0, -1, 1 << 43} {
		viper.Set("storage.min_free_mb", invalid)
		require.Error(t, configureDiskBudget())
	}
}
