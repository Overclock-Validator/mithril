package node

import (
	"fmt"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/spf13/cobra"
	"strconv"
)

func cacheOption(cmd *cobra.Command, flagName, key string) string {
	f := cmd.Flags().Lookup(flagName)
	if f.Changed {
		return f.Value.String()
	}
	if config.IsSet(key) {
		return config.GetString(key)
	}
	return f.DefValue
}

func configureCacheSettings(cmd *cobra.Command) error {
	index, err := strconv.Atoi(cacheOption(cmd, "index-cache-max-mb", "tuning.index_cache_max_mb"))
	if err != nil {
		return fmt.Errorf("tuning.index_cache_max_mb must be an integer MiB count: %w", err)
	}
	layout := cacheOption(cmd, "common-account-cache-layout", "tuning.common_account_cache_layout")
	accounting := cacheOption(cmd, "program-cache-accounting", "tuning.program_cache_accounting")
	oldIndex, oldLayout, oldAccounting := accountsdb.IndexCacheMaxMB, accountsdb.CommonAccountCacheLayout, accountsdb.ProgramCacheAccounting
	accountsdb.IndexCacheMaxMB, accountsdb.CommonAccountCacheLayout, accountsdb.ProgramCacheAccounting = index, layout, accounting
	if err := accountsdb.ValidateCacheSettings(); err != nil {
		accountsdb.IndexCacheMaxMB, accountsdb.CommonAccountCacheLayout, accountsdb.ProgramCacheAccounting = oldIndex, oldLayout, oldAccounting
		return err
	}
	return nil
}
