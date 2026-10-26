package accountsdb

import "github.com/cockroachdb/pebble"

// Bound scratch memory independently of segment size. Once full, uncached keys
// still get their normal index lookup; no record or validation is skipped.
const sparseIndexCacheLimit = 65536

type sparseIndexValue struct {
	entry AccountIndexEntry
	found bool
}

// sparseIndexCache is valid only while foldMu is held and no index mutation has
// occurred. CompactOnce owns it, discards it before relocation, and never shares
// it with replay or retains it across cycles (including folds and rewinds).
type sparseIndexCache struct{ values map[[32]byte]sparseIndexValue }

func (cache *sparseIndexCache) lookup(db *AccountsDb, key [32]byte) (AccountIndexEntry, bool, error) {
	if cache != nil {
		if v, ok := cache.values[key]; ok {
			return v.entry, v.found, nil
		}
	}
	var v sparseIndexValue
	raw, closer, err := db.Index.Get(key[:])
	if err != nil && err != pebble.ErrNotFound {
		return v.entry, false, err
	}
	if err == nil {
		v.entry, err = UnmarshalAcctIdxEntryValue(raw)
		closeErr := closer.Close()
		if err != nil {
			return v.entry, false, err
		}
		if closeErr != nil {
			return v.entry, false, closeErr
		}
		v.found = true
	}
	if cache != nil && len(cache.values) < sparseIndexCacheLimit {
		if cache.values == nil {
			cache.values = make(map[[32]byte]sparseIndexValue)
		}
		cache.values[key] = v
	}
	return v.entry, v.found, nil
}
