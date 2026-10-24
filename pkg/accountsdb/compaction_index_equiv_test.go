package accountsdb

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// pointLookupLiveRecords is the liveness scan compactFile used before the
// iterator: one Index.Get per record, in source order.
func pointLookupLiveRecords(db *AccountsDb, c compactCandidate, keys []solana.PublicKey, entries []AccountIndexEntry) ([]int, error) {
	live := make([]int, 0, len(entries))
	for i := range entries {
		cur, closer, err := db.Index.Get(keys[i][:])
		if err == pebble.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		entry, derr := UnmarshalAcctIdxEntry(cur)
		closer.Close()
		if derr != nil {
			return nil, derr
		}
		if entry.FileId == c.fileId && entry.Offset == entries[i].Offset && entry.Slot == c.slot {
			live = append(live, i)
		}
	}
	return live, nil
}

// The iterator path must classify exactly like per-record point lookups on any
// index state: tombstones from rewind deletes, values split across memtable and
// SSTs, near-collision keys, unrelated keys of other lengths sitting between
// candidates, duplicate records and batch sizes on both sides of the threshold.
func TestCompactionLiveRecordsMatchesPointLookups(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	db.foldMu.Lock()
	defer db.foldMu.Unlock()
	rng := rand.New(rand.NewSource(7))
	c := compactCandidate{slot: 100, fileId: 42}

	var base solana.PublicKey
	rng.Read(base[:])
	pool := make([]solana.PublicKey, 0, 800)
	for len(pool) < cap(pool) {
		k := base
		switch rng.Intn(5) {
		case 0:
			rng.Read(k[:])
		case 1:
			k[31] = byte(rng.Intn(256))
		case 2:
			k[30], k[31] = byte(rng.Intn(3)), byte(rng.Intn(256))
		case 3:
			k[31] = 0xFF
			k[30] = byte(rng.Intn(256))
		default:
			if rng.Intn(2) == 0 {
				k = solana.PublicKey{}
				k[31] = byte(rng.Intn(4))
			} else {
				for j := range k {
					k[j] = 0xFF
				}
				k[31] -= byte(rng.Intn(4))
			}
		}
		pool = append(pool, k)
	}
	put := func(key []byte, e AccountIndexEntry) {
		var buf [24]byte
		e.Marshal(&buf)
		require.NoError(t, db.Index.Set(key, buf[:], pebble.NoSync))
	}
	offset := func() uint64 { return uint64(rng.Intn(6)) * 128 }
	sizes := []int{0, 1, batchIndexIteratorThreshold - 1, batchIndexIteratorThreshold, batchIndexIteratorThreshold + 1, 64, 400}
	iteratorRuns, liveTotal := 0, 0
	for round := 0; round < 400; round++ {
		for j := 0; j < 60; j++ {
			k := pool[rng.Intn(len(pool))]
			switch rng.Intn(7) {
			case 0, 1:
				require.NoError(t, db.Index.Delete(k[:], pebble.NoSync))
			case 2:
				put(k[:], AccountIndexEntry{Slot: c.slot, FileId: c.fileId + 1, Offset: offset()})
			case 3:
				put(k[:], AccountIndexEntry{Slot: c.slot + 1, FileId: c.fileId, Offset: offset()})
			default:
				put(k[:], AccountIndexEntry{Slot: c.slot, FileId: c.fileId, Offset: offset()})
			}
		}
		if round%7 == 0 {
			// Keys that are not 32 bytes but share a candidate's prefix, holding a
			// value that would count as live if it were ever matched.
			k := pool[rng.Intn(len(pool))]
			put(append(append([]byte{}, k[:]...), 0), AccountIndexEntry{Slot: c.slot, FileId: c.fileId, Offset: 0})
			put(append([]byte{}, k[:31]...), AccountIndexEntry{Slot: c.slot, FileId: c.fileId, Offset: 0})
		}
		if rng.Intn(5) == 0 {
			require.NoError(t, db.Index.Flush())
		}
		n := sizes[rng.Intn(len(sizes))]
		keys := make([]solana.PublicKey, n)
		entries := make([]AccountIndexEntry, n)
		for i := range keys {
			keys[i] = pool[rng.Intn(len(pool))]
			entries[i] = AccountIndexEntry{Slot: c.slot, FileId: c.fileId, Offset: offset()}
		}
		want, err := pointLookupLiveRecords(db, c, keys, entries)
		require.NoError(t, err)
		got, err := db.compactionLiveRecords(c, keys, entries)
		require.NoError(t, err)
		require.Equal(t, want, got, "round %d, %d records", round, n)
		if n >= batchIndexIteratorThreshold {
			iteratorRuns++
			liveTotal += len(got)
		}
	}
	// Not vacuous: the iterator path ran and classified records as live.
	require.Greater(t, iteratorRuns, 100)
	require.Greater(t, liveTotal, 1000)
}
