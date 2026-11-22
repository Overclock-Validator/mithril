//go:build linux

package accountsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Mainnet retained tens of MiB per retired fold despite only a few live/undo
// records: none of these small payloads contains an entire punchable page.
func TestSparseCompactionReclaimsSmallDeadRecords(t *testing.T) {
	for _, walOff := range []bool{false, true} {
		for _, crash := range []string{"none", "marker", "punch"} {
			t.Run(fmt.Sprintf("wal_off=%t/crash=%s", walOff, crash), func(t *testing.T) {
				previous := DisableIndexWAL
				DisableIndexWAL = walOff
				t.Cleanup(func() { DisableIndexWAL = previous })
				db, dir := newFoldTestDb(t)
				t.Cleanup(func() { db.CloseDb() })
				key := func(n uint64) solana.PublicKey {
					var k solana.PublicKey
					binary.BigEndian.PutUint64(k[:8], n+1)
					return k
				}
				makeAcct := func(n uint64, data []byte) *accounts.Account {
					a := foldAcct(0, n+10, data)
					a.Key = key(n)
					return a
				}
				payload := bytes.Repeat([]byte{0xa7}, 1024)
				var initial, updated []*accounts.Account
				for n := uint64(0); n < 8000; n++ {
					initial = append(initial, makeAcct(n, payload))
					if n != 17 && n != 4097 {
						updated = append(updated, makeAcct(n, nil))
					}
				}
				r1 := commitTestBatch(t, db, 110, initial...)
				commitTestBatch(t, db, 120, updated...)
				commitTestBatch(t, db, 130, makeAcct(9000, nil))
				commitTestBatch(t, db, 140, makeAcct(4097, []byte("updated")))
				source := filepath.Join(db.AcctsDir, SegmentDataName(110, r1.FileId))
				before, err := os.Stat(source)
				require.NoError(t, err)
				indexBefore := snapshotIndex(t, db)
				// Exercise upgrading an existing payload-only sparse marker.
				m, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, 110, r1.FileId))
				require.NoError(t, err)
				m.Kind, m.BatchSeq, m.DataCRC = ManifestKindCompact, 0, 0
				require.NoError(t, WriteSegmentManifest(db.AcctsDir, m))
				if crash == "marker" {
					db.compactHooks.afterSparseMarker = func() { panic("crash after marker") }
				} else if crash == "punch" {
					db.compactHooks.afterSparsePunch = func() { panic("crash during holes") }
				}
				// Keep this test focused on in-place reclamation, including retry.
				cfg := CompactionConfig{RewindHorizonBatches: 1, MaxMoveBytesPerCycle: 1}
				if crash == "none" {
					_, err = db.CompactOnce(cfg)
					require.NoError(t, err)
				} else {
					require.Panics(t, func() { _, _ = db.CompactOnce(cfg) })
				}
				db = reopenFoldTestDb(t, db, dir)
				recovery, err := db.RecoverFoldState()
				require.NoError(t, err)
				require.Equal(t, uint64(140), recovery.DurableThrough)
				_, err = db.CompactOnce(cfg)
				require.NoError(t, err)
				after, err := os.Stat(source)
				require.NoError(t, err)
				require.Equal(t, before.Size(), after.Size())
				require.Less(t, allocatedBytesFromInfo(after), allocatedBytesFromInfo(before)-(8<<20))
				require.Equal(t, indexBefore, snapshotIndex(t, db))
				require.Equal(t, payload, mustColdRead(t, db, 140, key(17)).Data)
				_, err = db.RewindToBatchBoundary(130)
				require.NoError(t, err)
				require.Equal(t, payload, mustColdRead(t, db, 130, key(4097)).Data)
			})
		}
	}
}
