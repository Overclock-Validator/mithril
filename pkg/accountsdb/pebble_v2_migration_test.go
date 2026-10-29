package accountsdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	pebblev1 "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// Exercise a real v1-created store. Never use a production path in this test.
func TestPebbleV2Migration(t *testing.T) {
	for _, columnar := range []bool{false, true} {
		t.Run(fmt.Sprint(columnar), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "index")
			old, err := pebblev1.Open(dir, &pebblev1.Options{})
			require.NoError(t, err)
			key := func(i uint64) []byte { b := make([]byte, 32); binary.BigEndian.PutUint64(b[24:], i); return b }
			value := func(i uint64) []byte { b := make([]byte, 24); binary.LittleEndian.PutUint64(b, i+100); return b }
			for i := uint64(0); i < 1024; i++ {
				require.NoError(t, old.Set(key(i), value(i), pebblev1.NoSync))
			}
			require.NoError(t, old.Flush())
			require.NoError(t, old.Close())
			// Unsupported legacy stores must fail closed, without destroying their contents.
			rejected, err := pebble.Open(dir, &pebble.Options{})
			require.Error(t, err)
			require.Nil(t, rejected)
			old, err = pebblev1.Open(dir, &pebblev1.Options{})
			require.NoError(t, err)
			require.NoError(t, old.RatchetFormatMajorVersion(pebblev1.FormatFlushableIngest))
			require.NoError(t, old.Close())
			db, err := pebble.Open(dir, &pebble.Options{FormatMajorVersion: pebble.FormatFlushableIngest})
			require.NoError(t, err)
			if columnar {
				require.NoError(t, db.RatchetFormatMajorVersion(pebble.FormatColumnarBlocks))
			}
			for i := uint64(0); i < 1024; i += 3 {
				require.NoError(t, db.Delete(key(i), pebble.NoSync))
			}
			for i := uint64(1024); i < 2048; i++ {
				require.NoError(t, db.Set(key(i), value(i), pebble.NoSync))
			}
			require.NoError(t, db.Flush())
			require.NoError(t, db.Compact(context.Background(), key(0), key(2048), false))
			require.NoError(t, db.Close())
			db, err = pebble.Open(dir, &pebble.Options{})
			require.NoError(t, err)
			defer db.Close()
			for i := uint64(0); i < 2048; i++ {
				got, closer, err := db.Get(key(i))
				if i < 1024 && i%3 == 0 {
					require.ErrorIs(t, err, pebble.ErrNotFound)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, value(i), got)
				require.NoError(t, closer.Close())
			}
			iter, err := db.NewIter(nil)
			require.NoError(t, err)
			defer iter.Close()
			count := 0
			for ok := iter.First(); ok; ok = iter.Next() {
				count++
			}
			require.NoError(t, iter.Error())
			require.Equal(t, 1706, count)
		})
	}
}
