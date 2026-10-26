package node

import (
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCacheSettingsCLIAndTOML(t *testing.T) {
	oldIndex, oldLayout, oldAccounting := accountsdb.IndexCacheMaxMB, accountsdb.CommonAccountCacheLayout, accountsdb.ProgramCacheAccounting
	names := []string{"index-cache-max-mb", "common-account-cache-layout", "program-cache-accounting", "streaming-execution"}
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
		accountsdb.IndexCacheMaxMB, accountsdb.CommonAccountCacheLayout, accountsdb.ProgramCacheAccounting = oldIndex, oldLayout, oldAccounting
		for n, s := range saved {
			f := Run.Flags().Lookup(n)
			require.NoError(t, f.Value.Set(s.value))
			f.Changed = s.changed
		}
	})
	viper.Reset()
	require.NoError(t, Run.Flags().Lookup("index-cache-max-mb").Value.Set("8"))
	require.NoError(t, Run.Flags().Lookup("common-account-cache-layout").Value.Set("flat"))
	require.NoError(t, Run.Flags().Lookup("program-cache-accounting").Value.Set("mib"))
	require.NoError(t, configureCacheSettings(&Run))
	require.Equal(t, 8, accountsdb.IndexCacheMaxMB)
	viper.Set("tuning.index_cache_max_mb", 512)
	viper.Set("tuning.common_account_cache_layout", "expanded")
	viper.Set("tuning.program_cache_accounting", "kib")
	require.NoError(t, configureCacheSettings(&Run))
	require.Equal(t, 512, accountsdb.IndexCacheMaxMB)
	require.Equal(t, "expanded", accountsdb.CommonAccountCacheLayout)
	require.Equal(t, "kib", accountsdb.ProgramCacheAccounting)
	require.NoError(t, Run.Flags().Set("index-cache-max-mb", "256"))
	require.NoError(t, configureCacheSettings(&Run))
	require.Equal(t, 256, accountsdb.IndexCacheMaxMB)
	viper.Set("tuning.program_cache_accounting", "invalid")
	require.Error(t, configureCacheSettings(&Run))
	require.Equal(t, 256, accountsdb.IndexCacheMaxMB)
	require.Equal(t, "kib", accountsdb.ProgramCacheAccounting)
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
