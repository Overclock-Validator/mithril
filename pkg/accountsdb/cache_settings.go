package accountsdb

import "fmt"

const DefaultIndexCacheMaxMB = 256

// Fixed before opening a database or initializing caches. The defaults (768 MiB
// account cache in the fixed layout, 256 MiB index cache) replayed mainnet
// catch-up ~15% faster than the earlier 256 MiB flat / 8 MiB index; a 128 or
// 512 MiB index cache measured the same, 256 MiB leaves headroom for heavier
// traffic.
var IndexCacheMaxMB = DefaultIndexCacheMaxMB
var CommonAccountCacheLayout = "fixed"

func ValidateCacheSettings() error {
	if IndexCacheMaxMB <= 0 || uint64(IndexCacheMaxMB) > uint64((1<<63-1)>>20) {
		return fmt.Errorf("tuning.index_cache_max_mb must be a positive MiB count representable in bytes")
	}
	switch CommonAccountCacheLayout {
	case "fixed", "flat":
	default:
		return fmt.Errorf("tuning.common_account_cache_layout must be fixed or flat")
	}
	return nil
}
