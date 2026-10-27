package node

import (
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestCacheSettingsCLIAndTOML(t *testing.T) {
	oldIndex, oldLayout := accountsdb.IndexCacheMaxMB, accountsdb.CommonAccountCacheLayout
	names := []string{"index-cache-max-mb", "common-account-cache-layout", "streaming-execution"}
	type flagState struct {
		value   string
		changed bool
	}
	saved := map[string]flagState{}
	for _, n := range names {
		f := Run.Flags().Lookup(n)
		saved[n] = flagState{f.Value.String(), f.Changed}
		f.Changed = false
	}
	t.Cleanup(func() {
		viper.Reset()
		accountsdb.IndexCacheMaxMB, accountsdb.CommonAccountCacheLayout = oldIndex, oldLayout
		for n, s := range saved {
			f := Run.Flags().Lookup(n)
			require.NoError(t, f.Value.Set(s.value))
			f.Changed = s.changed
		}
	})
	viper.Reset()
	require.NoError(t, Run.Flags().Lookup("index-cache-max-mb").Value.Set(strconv.Itoa(accountsdb.DefaultIndexCacheMaxMB)))
	require.NoError(t, Run.Flags().Lookup("common-account-cache-layout").Value.Set("fixed"))
	require.NoError(t, configureCacheSettings(&Run))
	require.Equal(t, accountsdb.DefaultIndexCacheMaxMB, accountsdb.IndexCacheMaxMB)
	require.Equal(t, "fixed", accountsdb.CommonAccountCacheLayout)
	viper.Set("tuning.index_cache_max_mb", 512)
	viper.Set("tuning.common_account_cache_layout", "flat")
	require.NoError(t, configureCacheSettings(&Run))
	require.Equal(t, 512, accountsdb.IndexCacheMaxMB)
	require.Equal(t, "flat", accountsdb.CommonAccountCacheLayout)
	require.NoError(t, Run.Flags().Set("index-cache-max-mb", "256"))
	require.NoError(t, configureCacheSettings(&Run))
	require.Equal(t, 256, accountsdb.IndexCacheMaxMB)
	viper.Set("tuning.common_account_cache_layout", "invalid")
	require.Error(t, configureCacheSettings(&Run))
	require.Equal(t, 256, accountsdb.IndexCacheMaxMB)
	require.Equal(t, "flat", accountsdb.CommonAccountCacheLayout)
	require.Equal(t, "true", Run.Flags().Lookup("streaming-execution").DefValue)
	viper.Reset()
	f := Run.Flags().Lookup("streaming-execution")
	require.NoError(t, f.Value.Set("true"))
	f.Changed = false
	require.True(t, resolveBoolOption(Run.Flags().Lookup("streaming-execution"), viper.IsSet("tuning.streaming_execution"), viper.GetBool("tuning.streaming_execution")))
	viper.Set("tuning.streaming_execution", false)
	require.False(t, resolveBoolOption(Run.Flags().Lookup("streaming-execution"), viper.IsSet("tuning.streaming_execution"), viper.GetBool("tuning.streaming_execution")))
	require.NoError(t, Run.Flags().Set("streaming-execution", "true"))
	require.True(t, resolveBoolOption(Run.Flags().Lookup("streaming-execution"), viper.IsSet("tuning.streaming_execution"), viper.GetBool("tuning.streaming_execution")))
}
