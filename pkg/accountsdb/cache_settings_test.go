package accountsdb

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCacheSettingsValidation(t *testing.T) {
	oldIndex, oldLayout, oldAccounting := IndexCacheMaxMB, CommonAccountCacheLayout, ProgramCacheAccounting
	t.Cleanup(func() {
		IndexCacheMaxMB, CommonAccountCacheLayout, ProgramCacheAccounting = oldIndex, oldLayout, oldAccounting
	})
	for _, layout := range []string{"flat", "tiered", "expanded"} {
		for _, accounting := range []string{"mib", "kib"} {
			IndexCacheMaxMB = 512
			CommonAccountCacheLayout = layout
			ProgramCacheAccounting = accounting
			require.NoError(t, ValidateCacheSettings())
		}
	}
	IndexCacheMaxMB = 0
	require.Error(t, ValidateCacheSettings())
	IndexCacheMaxMB = -1
	require.Error(t, ValidateCacheSettings())
	IndexCacheMaxMB = int(^uint(0) >> 1)
	require.Error(t, ValidateCacheSettings())
	IndexCacheMaxMB = 512
	CommonAccountCacheLayout = "unknown"
	require.Error(t, ValidateCacheSettings())
	CommonAccountCacheLayout = "flat"
	ProgramCacheAccounting = "unknown"
	require.Error(t, ValidateCacheSettings())
}
