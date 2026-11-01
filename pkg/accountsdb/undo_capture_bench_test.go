package accountsdb

import (
	"bytes"
	"math/rand"
	"runtime"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"golang.org/x/sync/errgroup"
)

// pointLookupUndo is the previous capture: one goroutine and Get per key.
func pointLookupUndo(db *AccountsDb, keys [][32]byte) ([]undoEntry, error) {
	out := make([]undoEntry, len(keys))
	var g errgroup.Group
	g.SetLimit(max(4, runtime.NumCPU()/2))
	for i := range keys {
		g.Go(func() error {
			val, closer, err := db.Index.Get(keys[i][:])
			if err == pebble.ErrNotFound {
				return nil
			}
			if err != nil {
				return err
			}
			e, derr := UnmarshalAcctIdxEntryValue(val)
			closer.Close()
			if derr != nil {
				return derr
			}
			out[i] = undoEntry{entry: e, valid: true}
			return nil
		})
	}
	return out, g.Wait()
}

func benchUndoDb(b *testing.B, indexed, folded int) (*AccountsDb, [][32]byte) {
	db, _ := newFoldTestDb(b)
	b.Cleanup(func() {
		if err := db.CloseDb(); err != nil {
			b.Fatal(err)
		}
	})
	rng := rand.New(rand.NewSource(7))
	all := make([][32]byte, indexed)
	var buf [24]byte
	batch := db.Index.NewBatch()
	for i := range all {
		rng.Read(all[i][:])
		(&AccountIndexEntry{Slot: uint64(i), FileId: 1, Offset: uint64(i)}).Marshal(&buf)
		if err := batch.Set(all[i][:], buf[:], nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		b.Fatal(err)
	}
	if err := db.Index.Flush(); err != nil {
		b.Fatal(err)
	}
	keys := make([][32]byte, 0, folded)
	for i := 0; i < folded; i++ {
		if i%10 == 0 {
			var k [32]byte
			rng.Read(k[:]) // ~10% new accounts
			keys = append(keys, k)
		} else {
			keys = append(keys, all[rng.Intn(len(all))])
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	return db, keys
}

func BenchmarkUndoCapture(b *testing.B) {
	for _, size := range []struct {
		name            string
		indexed, folded int
	}{{"index500k_keys70k", 500_000, 70_000}, {"index5M_keys117k", 5_000_000, 117_000}} {
		b.Run(size.name, func(b *testing.B) {
			db, keys := benchUndoDb(b, size.indexed, size.folded)
			b.Run("point_lookups", func(b *testing.B) {
				for range b.N {
					if _, err := pointLookupUndo(db, keys); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("chunked_point_lookups", func(b *testing.B) {
				for range b.N {
					if _, err := db.captureUndoEntries(keys); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
