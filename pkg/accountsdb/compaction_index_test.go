package accountsdb

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestCompactionLiveRecords(t *testing.T) {
	for _, n := range []int{0, 1, 15, 16, 17, 256} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			db, _ := newFoldTestDb(t)
			defer db.CloseDb()
			db.foldMu.Lock()
			defer db.foldMu.Unlock()
			keys := make([]solana.PublicKey, n)
			entries := make([]AccountIndexEntry, n)
			want := []int{}
			c := compactCandidate{slot: 100, fileId: 42}
			for i := range keys {
				// Reverse key order, spanning zero and maximal keys.
				for j := range keys[i] {
					keys[i][j] = byte(n - 1 - i)
				}
				entries[i] = AccountIndexEntry{Slot: 100, FileId: 42, Offset: uint64(i * 128)}
				current := entries[i]
				switch i % 5 {
				case 1:
					continue
				case 2:
					current.Slot++
				case 3:
					current.FileId++
				case 4:
					current.Offset++
				default:
					want = append(want, i)
				}
				var buf [24]byte
				current.Marshal(&buf)
				require.NoError(t, db.Index.Set(keys[i][:], buf[:], pebble.NoSync))
			}
			// Check the exact expected live records before and after SST flush.
			for pass := 0; pass < 2; pass++ {
				got, err := db.compactionLiveRecords(c, keys, entries)
				require.NoError(t, err)
				require.Equal(t, want, got)
				require.NoError(t, db.Index.Flush())
			}
			if n > 0 {
				require.NoError(t, db.Index.Set(keys[0][:], []byte{1}, pebble.NoSync))
				_, err := db.compactionLiveRecords(c, keys, entries)
				require.Error(t, err)
			}
		})
	}
}

func TestCompactionLiveRecordsDuplicateKey(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	db.foldMu.Lock()
	defer db.foldMu.Unlock()
	keys := make([]solana.PublicKey, 20)
	entries := make([]AccountIndexEntry, 20)
	for i := range entries {
		entries[i] = AccountIndexEntry{Slot: 100, FileId: 42, Offset: uint64(i)}
	}
	var value [24]byte
	entries[7].Marshal(&value)
	require.NoError(t, db.Index.Set(keys[0][:], value[:], pebble.NoSync))
	got, err := db.compactionLiveRecords(compactCandidate{slot: 100, fileId: 42}, keys, entries)
	require.NoError(t, err)
	require.Equal(t, []int{7}, got)
}
