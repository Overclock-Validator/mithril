package snapshot

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
)

// TestFlushPipelineDedupAndOrder feeds a known set of key/slot entries through the
// shard logger's flush pipeline and asserts the ingested index keeps exactly one
// entry per key: the one with the highest slot. This pins the pipeline's transform
// (sort key-ASC / slot-DESC, dedup keep-first) to the same behavior as the old
// serial logToSST path.
func TestFlushPipelineDedupAndOrder(t *testing.T) {
	logsDir := t.TempDir()
	const numShards = 8
	sl := NewShardLogger(numShards, logsDir)

	// Build a key -> expected (highest-slot) entry map while enqueuing several
	// entries per key at differing slots, deliberately NOT in slot order.
	const numKeys = 5000
	expected := make(map[solana.PublicKey]accountsdb.AccountIndexEntry, numKeys)
	makeKey := func(i int) solana.PublicKey {
		var k solana.PublicKey
		if i%50 == 0 {
			// Force a shared 8-byte prefix but distinct later bytes, so the sort
			// comparator must fall through to the full key, not just its first word.
			binary.BigEndian.PutUint64(k[:8], 0xABCDEF0011223344)
			binary.BigEndian.PutUint64(k[8:16], uint64(i))
		} else {
			binary.BigEndian.PutUint64(k[:8], uint64(i)*0x9E3779B97F4A7C15)
			binary.BigEndian.PutUint32(k[8:12], uint32(i))
		}
		return k
	}
	for i := 0; i < numKeys; i++ {
		k := makeKey(i)
		// Three writes for this key at slots that peak in the middle, so the
		// winner is neither the first nor the last enqueued.
		slots := []uint64{uint64(i) + 10, uint64(i) + 100, uint64(i) + 50}
		var best accountsdb.AccountIndexEntry
		for j, slot := range slots {
			e := accountsdb.AccountIndexEntry{Slot: slot, FileId: uint64(i), Offset: uint64(j)}
			sl.EnqueueRequest(k, e)
			if slot > best.Slot {
				best = e
			}
		}
		expected[k] = best
	}

	if err := sl.CloseWithProgress(context.Background(), nil); err != nil {
		t.Fatalf("CloseWithProgress: %v", err)
	}

	indexDir := t.TempDir()
	db, err := ingestSSTFiles(indexDir, logsDir)
	if err != nil {
		t.Fatalf("ingestSSTFiles: %v", err)
	}
	defer db.Close()

	iter, err := db.NewIter(nil)
	if err != nil {
		t.Fatalf("NewIter: %v", err)
	}
	defer iter.Close()

	var count int
	var prevKey []byte
	for iter.First(); iter.Valid(); iter.Next() {
		key := append([]byte(nil), iter.Key()...)
		if prevKey != nil && string(key) <= string(prevKey) {
			t.Fatalf("keys not strictly ascending / deduped: %x then %x", prevKey, key)
		}
		prevKey = key

		got, err := accountsdb.UnmarshalAcctIdxEntry(iter.Value())
		if err != nil {
			t.Fatalf("UnmarshalAcctIdxEntry: %v", err)
		}
		want, ok := expected[solana.PublicKey(key)]
		if !ok {
			t.Fatalf("unexpected key in index: %x", key)
		}
		if *got != want {
			t.Fatalf("key %x: got %+v, want %+v", key, *got, want)
		}
		count++
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	if count != len(expected) {
		t.Fatalf("index has %d keys, want %d", count, len(expected))
	}
}
