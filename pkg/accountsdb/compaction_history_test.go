package accountsdb

import (
	"fmt"
	"github.com/cockroachdb/pebble/v2"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCompactionHistorySurvivesRewind(t *testing.T) {
	for _, walOff := range []bool{false, true} {
		t.Run(fmt.Sprint(walOff), func(t *testing.T) {
			old := DisableIndexWAL
			DisableIndexWAL = walOff
			defer func() { DisableIndexWAL = old }()
			db, dir := newFoldTestDb(t)
			for i := uint64(1); i <= 6; i++ {
				commitTestBatch(t, db, 100+i, foldAcct(1, i, []byte{byte(i)}))
			}
			_, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 2})
			require.NoError(t, err)
			floor, err := db.readCompactionHistoryFloor()
			require.NoError(t, err)
			require.Equal(t, uint64(4), floor.BatchSeq)
			// Old manifests are really retired, not just hidden from the assertion.
			hs, err := ListFoldManifests(db.AcctsDir)
			require.NoError(t, err)
			for _, h := range hs {
				require.GreaterOrEqual(t, h.BatchSeq, uint64(4))
			}
			_, err = db.RewindToBatchBoundaryAndPersist(105, func(RewindResult) error { return nil })
			require.NoError(t, err)
			db = reopenFoldTestDb(t, db, dir)
			defer db.CloseDb()
			_, err = db.RecoverFoldState()
			require.NoError(t, err)
			_, err = db.CompactOnce(CompactionConfig{RewindHorizonBatches: 2})
			require.NoError(t, err, "rewind must not invent a requirement for expired batch3")
			_, err = db.CompactOnce(CompactionConfig{RewindHorizonBatches: 64})
			require.NoError(t, err, "larger configured horizon cannot recreate expired history")
			floor, err = db.readCompactionHistoryFloor()
			require.NoError(t, err)
			require.Equal(t, uint64(4), floor.BatchSeq)
			pts, err := db.ListRewindPoints()
			require.NoError(t, err)
			for _, p := range pts {
				require.GreaterOrEqual(t, p.BatchSeq, uint64(4))
			}
			_, err = db.RewindToBatchBoundaryAndPersist(104, func(RewindResult) error { return nil })
			require.NoError(t, err)
			require.Equal(t, uint64(4), mustColdRead(t, db, 104, solana.PublicKey{1}).Lamports)
		})
	}
}

func TestCompactionHistoryRejectsExpiredTargetBeforeIntent(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	var boundary BatchCommitResult
	for i := uint64(1); i <= 6; i++ {
		r := commitTestBatch(t, db, 100+i, foldAcct(1, i, nil))
		if i == 4 {
			boundary = r
		}
	}
	// Simulate a crash after durable frontier, before old files are retired.
	require.NoError(t, db.persistCompactionHistoryFloor(foldMeta{boundary.BatchSeq, boundary.ThroughSlot, boundary.FileId}))
	_, err := db.RewindToBatchBoundaryAndPersist(103, func(RewindResult) error { t.Fatal("must not persist"); return nil })
	require.ErrorContains(t, err, "below expired-history boundary")
	_, err = os.Stat(filepath.Join(db.AcctsDir, rewindIntentName))
	require.True(t, os.IsNotExist(err))
	meta, _, err := db.readFoldMeta()
	require.NoError(t, err)
	require.Equal(t, uint64(6), meta.BatchSeq)
}

func TestCompactionHistoryFailsClosed(t *testing.T) {
	for _, damage := range []string{"gap", "crc", "floor-record", "floor-boundary"} {
		t.Run(damage, func(t *testing.T) {
			db, _ := newFoldTestDb(t)
			defer db.CloseDb()
			results := map[uint64]BatchCommitResult{}
			for i := uint64(1); i <= 6; i++ {
				results[i] = commitTestBatch(t, db, 100+i, foldAcct(1, i, nil))
			}
			_, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 2})
			require.NoError(t, err)
			for i := uint64(7); i <= 10; i++ {
				commitTestBatch(t, db, 100+i, foldAcct(1, i, nil))
			}
			// Batch5 is now below the prospective floor8, but is still promised by4.
			r := results[5]
			p := segmentManifestPath(db.AcctsDir, r.ThroughSlot, r.FileId)
			switch damage {
			case "gap":
				require.NoError(t, os.Remove(p))
			case "crc":
				data, err := os.ReadFile(p)
				require.NoError(t, err)
				data[len(data)-1] ^= 1
				require.NoError(t, os.WriteFile(p, data, 0600))
			case "floor-record":
				require.NoError(t, db.Index.Set(metaKeyCompactionHistoryFloor, []byte("bad"), pebble.Sync))
			case "floor-boundary":
				r = results[4]
				require.NoError(t, os.Remove(segmentManifestPath(db.AcctsDir, r.ThroughSlot, r.FileId)))
			}
			_, err = db.CompactOnce(CompactionConfig{RewindHorizonBatches: 2})
			require.Error(t, err)
			if damage == "gap" || damage == "crc" {
				f, err := db.readCompactionHistoryFloor()
				require.NoError(t, err)
				require.Equal(t, uint64(4), f.BatchSeq)
			}
		})
	}
}

func TestCompactionHistoryCrashChild(t *testing.T) {
	dir := os.Getenv("MITHRIL_HISTORY_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess only")
	}
	DisableIndexWAL = os.Getenv("MITHRIL_HISTORY_WAL_OFF") == "true"
	db, err := OpenDb(dir)
	require.NoError(t, err)
	db.RootedDurable = true
	db.InitCaches()
	_, err = db.RecoverFoldState()
	require.NoError(t, err)
	crash := func() { os.Exit(92) }
	phase := os.Getenv("MITHRIL_HISTORY_CRASH_PHASE")
	if phase == "floor" {
		db.compactHooks.afterHistoryFloor = crash
	}
	_, err = db.CompactOnce(CompactionConfig{RewindHorizonBatches: 2})
	require.NoError(t, err)
	if phase == "compact" {
		crash()
	}
	db.rewindHooks.afterIndex = crash
	_, err = db.RewindToBatchBoundaryAndPersist(105, func(RewindResult) error { return nil })
	require.NoError(t, err)
	t.Fatal("crash hook did not run")
}

func TestCompactionHistoryProcessCrashes(t *testing.T) {
	for _, walOff := range []bool{false, true} {
		for _, phase := range []string{"floor", "compact", "rewind"} {
			t.Run(fmt.Sprintf("waloff=%t/%s", walOff, phase), func(t *testing.T) {
				old := DisableIndexWAL
				DisableIndexWAL = walOff
				defer func() { DisableIndexWAL = old }()
				db, dir := newFoldTestDb(t)
				for i := uint64(1); i <= 6; i++ {
					commitTestBatch(t, db, 100+i, foldAcct(1, i, nil))
				}
				db.CloseDb()
				cmd := exec.Command(os.Args[0], "-test.run=^TestCompactionHistoryCrashChild$")
				cmd.Env = append(os.Environ(), "MITHRIL_HISTORY_CRASH_DIR="+dir, "MITHRIL_HISTORY_WAL_OFF="+fmt.Sprint(walOff), "MITHRIL_HISTORY_CRASH_PHASE="+phase)
				output, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, string(output))
				require.Equal(t, 92, exit.ExitCode(), string(output))
				require.NotContains(t, string(output), "WARNING: DATA RACE")
				db, err = OpenDb(dir)
				require.NoError(t, err)
				defer db.CloseDb()
				db.RootedDurable = true
				db.InitCaches()
				_, err = db.RecoverFoldState()
				require.NoError(t, err)
				f, err := db.readCompactionHistoryFloor()
				require.NoError(t, err)
				require.Equal(t, uint64(4), f.BatchSeq)
				_, err = db.RewindToBatchBoundaryAndPersist(105, func(RewindResult) error { return nil })
				require.NoError(t, err)
				_, err = db.CompactOnce(CompactionConfig{RewindHorizonBatches: 2})
				require.NoError(t, err)
				require.Equal(t, uint64(5), mustColdRead(t, db, 105, solana.PublicKey{1}).Lamports)
			})
		}
	}
}
