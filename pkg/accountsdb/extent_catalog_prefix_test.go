package accountsdb

import "testing"

func TestPersistentExtentCatalogPrecomputesDistinctPrefixesInOnePass(t *testing.T) {
	catalog, err := clonePersistentExtentCatalog(nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	const extentCount = 1024
	for ordinal := uint64(0); ordinal < extentCount; ordinal++ {
		if err := catalog.ensureExtent(AccountIndexEntry{
			Slot: ordinal + 1, FileId: ordinal + 10, Offset: 8,
		}); err != nil {
			t.Fatal(err)
		}
	}
	counts := []uint32{1024, 1, 768, 256, 512, 768, 17, 1000}
	work, err := catalog.precomputePrefixHashes(counts)
	if err != nil {
		t.Fatal(err)
	}
	if work != extentCount || work > uint32(catalog.Len()) {
		t.Fatalf("prefix precompute hashed %d records for %d extents", work, catalog.Len())
	}
	for _, count := range counts {
		if _, err := catalog.prefixHash(count); err != nil {
			t.Fatalf("cached prefix %d: %v", count, err)
		}
	}
	work, err = catalog.precomputePrefixHashes(counts)
	if err != nil {
		t.Fatal(err)
	}
	if work != 0 {
		t.Fatalf("cached prefix precompute repeated %d extent hashes", work)
	}
}

func TestPersistentExtentCatalogPrecomputesRollingRebaseBindingsInOnePass(t *testing.T) {
	catalog, err := clonePersistentExtentCatalog(nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	const (
		extentCount = 4096
		shardCount  = 1024
	)
	for ordinal := uint64(0); ordinal < extentCount; ordinal++ {
		if err := catalog.ensureExtent(AccountIndexEntry{
			Slot: ordinal + 1, FileId: ordinal + 10, Offset: 8,
		}); err != nil {
			t.Fatal(err)
		}
	}
	bindings := make([]extentCatalogBinding, shardCount)
	for shardID := range bindings {
		// Model shard generations spread evenly over the complete append-only
		// catalog. Independently hashing these prefixes would process more than
		// two million records; the batched path must stop at the largest prefix.
		bindings[shardID].RequiredCount = uint32((shardID + 1) * (extentCount / shardCount))
	}

	work, err := catalog.precomputeBindingPrefixes(bindings)
	if err != nil {
		t.Fatal(err)
	}
	if work != extentCount {
		t.Fatalf("rolling-rebase prefix work = %d records, want max prefix %d", work, extentCount)
	}
	work, err = catalog.precomputeBindingPrefixes(bindings)
	if err != nil {
		t.Fatal(err)
	}
	if work != 0 {
		t.Fatalf("cached rolling-rebase prefixes repeated %d record hashes", work)
	}
}

func TestPersistentExtentCatalogSuccessorCarriesPrefixCache(t *testing.T) {
	previous, err := clonePersistentExtentCatalog(nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	const extentCount = 1024
	for ordinal := uint64(0); ordinal < extentCount; ordinal++ {
		if err := previous.ensureExtent(AccountIndexEntry{
			Slot: ordinal + 1, FileId: ordinal + 10, Offset: 8,
		}); err != nil {
			t.Fatal(err)
		}
	}
	bindings := []extentCatalogBinding{
		{RequiredCount: 1},
		{RequiredCount: 257},
		{RequiredCount: 768},
		{RequiredCount: extentCount},
	}
	if work, err := previous.precomputeBindingPrefixes(bindings); err != nil || work != extentCount {
		t.Fatalf("prepare predecessor prefixes: work=%d err=%v", work, err)
	}

	successor, err := clonePersistentExtentCatalog(previous, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := successor.ensureExtent(AccountIndexEntry{
		Slot: extentCount + 1, FileId: extentCount + 10, Offset: 8,
	}); err != nil {
		t.Fatal(err)
	}
	work, err := successor.precomputeBindingPrefixes(bindings)
	if err != nil {
		t.Fatal(err)
	}
	if work != 0 {
		t.Fatalf("append-only successor rehashed %d predecessor records", work)
	}
}
