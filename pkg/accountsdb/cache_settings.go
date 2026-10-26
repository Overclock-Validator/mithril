package accountsdb

import "fmt"

const DefaultIndexCacheMaxMB = 8

// Fixed before opening a database or initializing caches. Defaults preserve the
// existing memory budgets; the expanded layout is an explicit tuning choice.
var IndexCacheMaxMB = DefaultIndexCacheMaxMB
var CommonAccountCacheLayout = "flat"
var ProgramCacheAccounting = "mib"

func ValidateCacheSettings() error {
	if IndexCacheMaxMB <= 0 || uint64(IndexCacheMaxMB) > uint64((1<<63-1)>>20) {
		return fmt.Errorf("tuning.index_cache_max_mb must be a positive MiB count representable in bytes")
	}
	switch CommonAccountCacheLayout {
	case "flat", "tiered", "expanded":
	default:
		return fmt.Errorf("tuning.common_account_cache_layout must be flat, tiered or expanded")
	}
	switch ProgramCacheAccounting {
	case "mib", "kib":
	default:
		return fmt.Errorf("tuning.program_cache_accounting must be mib or kib")
	}
	return nil
}
