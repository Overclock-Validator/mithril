package accountsdb

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCacheSettingsValidation(t *testing.T) {
	oldIndex, oldLayout := IndexCacheMaxMB, CommonAccountCacheLayout
	t.Cleanup(func() {
		IndexCacheMaxMB, CommonAccountCacheLayout = oldIndex, oldLayout
	})
	for _, layout := range []string{"fixed", "flat"} {
		IndexCacheMaxMB = 512
		CommonAccountCacheLayout = layout
		require.NoError(t, ValidateCacheSettings())
	}
	IndexCacheMaxMB = 0
	require.Error(t, ValidateCacheSettings())
	IndexCacheMaxMB = -1
	require.Error(t, ValidateCacheSettings())
	IndexCacheMaxMB = int(^uint(0) >> 1)
	require.Error(t, ValidateCacheSettings())
	IndexCacheMaxMB = 512
	CommonAccountCacheLayout = "tiered"
	require.Error(t, ValidateCacheSettings())
}
