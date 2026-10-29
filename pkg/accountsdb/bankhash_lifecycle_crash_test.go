package accountsdb

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestBankhashRecoveryAfterLifecycleCrash(t *testing.T) {
	if dir := os.Getenv("MITHRIL_BANKHASH_LIFECYCLE_DIR"); dir != "" {
		mode := os.Getenv("MITHRIL_BANKHASH_LIFECYCLE_MODE")
		require.Contains(t, []string{"refold", "compaction"}, mode)
		DisableIndexWAL = false
		require.NoError(t, os.Mkdir(filepath.Join(dir, "accounts"), 0o700))
		for _, name := range []string{"largest_file_id", "bootstrap_high_file_id"} {
			require.NoError(t, os.WriteFile(filepath.Join(dir, name), make([]byte, 8), 0o600))
		}
		db, err := OpenDb(dir)
		require.NoError(t, err)
		db.RootedDurable = true
		db.InitCaches()
		if mode == "refold" {
			commitTestBatch(t, db, 280)
			commitTestBatch(t, db, 296)
			require.NoError(t, db.BankHashStore.LogData(nil, pebble.Sync))
			_, err = db.RewindToBatchBoundary(280)
			require.NoError(t, err)
			_, err = db.CommitBatch(nil, 296, map[uint64][32]byte{296: {99}}, []byte("new-fork-296"))
			require.NoError(t, err)
			got, err := db.GetBankHashForSlot(296)
			require.NoError(t, err)
			require.Equal(t, append([]byte{99}, make([]byte, 31)...), got)
		} else {
			first := commitTestBatch(t, db, 289)
			boundary := commitTestBatch(t, db, 296)
			commitTestBatch(t, db, 304)
			stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1})
			require.NoError(t, err)
			require.Equal(t, 1, stats.FilesDeleted)
			require.NoFileExists(t, segmentManifestPath(db.AcctsDir, first.ThroughSlot, first.FileId))
			require.FileExists(t, segmentManifestPath(db.AcctsDir, boundary.ThroughSlot, boundary.FileId), "the rewind boundary must survive compaction")
			got, err := db.GetBankHashForSlot(289)
			require.NoError(t, err)
			want := bh(289)
			require.Equal(t, want[:], got)
		}
		fmt.Println("acknowledged lifecycle operation with readable bankhash")
		os.Exit(73)
	}
	for _, mode := range []string{"refold", "compaction"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestBankhashRecoveryAfterLifecycleCrash$")
			command.Env = append(os.Environ(), "MITHRIL_BANKHASH_LIFECYCLE_DIR="+dir, "MITHRIL_BANKHASH_LIFECYCLE_MODE="+mode)
			output, err := command.CombinedOutput()
			var exited *exec.ExitError
			require.ErrorAs(t, err, &exited, "%s", output)
			require.Equal(t, 73, exited.ExitCode(), "%s", output)
			require.True(t, bytes.Contains(output, []byte("acknowledged lifecycle operation with readable bankhash")), "%s", output)
			oldWAL := DisableIndexWAL
			DisableIndexWAL = false
			defer func() { DisableIndexWAL = oldWAL }()
			db, err := OpenDb(dir)
			require.NoError(t, err)
			db.RootedDurable = true
			db.InitCaches()
			defer db.CloseDb()
			recovered, err := db.RecoverFoldState()
			require.NoError(t, err)
			if mode == "refold" {
				require.Equal(t, uint64(296), recovered.DurableThrough)
				require.Equal(t, [32]byte{99}, recovered.RootedBankhash)
				got, err := db.GetBankHashForSlot(296)
				require.NoError(t, err)
				require.Equal(t, append([]byte{99}, make([]byte, 31)...), got, "the active committed manifest must replace the old fork's advisory row")
			} else {
				require.Equal(t, uint64(304), recovered.DurableThrough)
				got, err := db.GetBankHashForSlot(289)
				require.NoError(t, err, "compaction must retain the hash after removing its only recovery manifest")
				want := bh(289)
				require.Equal(t, want[:], got)
			}
		})
	}
}
