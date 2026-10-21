package accountsdb

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This child exits without Go cleanup after a real acknowledged fold. A small
// advisory bankhash write is visible before exit but need not have left its WAL buffer.
func TestCommittedBankhashRecoveryAfterProcessExit(t *testing.T) {
	if dir := os.Getenv("MITHRIL_BANKHASH_CRASH_DIR"); dir != "" {
		mode := os.Getenv("MITHRIL_BANKHASH_CRASH_MODE")
		require.Contains(t, []string{"wal-on", "wal-off", "wal-off-flushed-index", "graceful"}, mode)
		DisableIndexWAL = mode == "wal-off" || mode == "wal-off-flushed-index"
		require.NoError(t, os.Mkdir(filepath.Join(dir, "accounts"), 0o700))
		for _, name := range []string{"largest_file_id", "bootstrap_high_file_id"} {
			require.NoError(t, os.WriteFile(filepath.Join(dir, name), make([]byte, 8), 0o600))
		}
		db, err := OpenDb(dir)
		require.NoError(t, err)
		db.RootedDurable = true
		db.InitCaches()
		_, err = db.CommitBatch([]accounts.SlotDelta{
			{Slot: 289, Delta: []*accounts.Account{foldAcct(1, 100, []byte("receipt-account"))}},
			{Slot: 296, Delta: []*accounts.Account{foldAcct(2, 200, []byte("root-account"))}},
		}, 296, map[uint64][32]byte{289: bh(289), 296: bh(296)}, []byte("checkpoint-296"))
		require.NoError(t, err)
		for _, slot := range []uint64{289, 296} {
			got, err := db.GetBankHashForSlot(slot)
			require.NoError(t, err)
			want := bh(slot)
			require.Equal(t, want[:], got)
		}
		if mode == "wal-off-flushed-index" {
			require.NoError(t, db.Index.Flush())
		}
		if mode == "graceful" {
			db.CloseDb()
		}
		fmt.Println("acknowledged fold 296 with readable bankhashes 289 and 296")
		os.Exit(73)
	}
	for _, mode := range []string{"wal-on", "wal-off", "wal-off-flushed-index", "graceful"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestCommittedBankhashRecoveryAfterProcessExit$")
			command.Env = append(os.Environ(), "MITHRIL_BANKHASH_CRASH_DIR="+dir, "MITHRIL_BANKHASH_CRASH_MODE="+mode)
			output, err := command.CombinedOutput()
			var exited *exec.ExitError
			require.ErrorAs(t, err, &exited, "%s", output)
			require.Equal(t, 73, exited.ExitCode(), "%s", output)
			require.True(t, bytes.Contains(output, []byte("acknowledged fold 296 with readable bankhashes 289 and 296")), "%s", output)
			oldWAL := DisableIndexWAL
			DisableIndexWAL = mode == "wal-off" || mode == "wal-off-flushed-index"
			defer func() { DisableIndexWAL = oldWAL }()
			db, err := OpenDb(dir)
			require.NoError(t, err)
			db.RootedDurable = true
			db.InitCaches()
			defer db.CloseDb()
			headers, err := ListFoldManifests(db.AcctsDir)
			require.NoError(t, err)
			require.Len(t, headers, 1)
			manifest, err := ReadSegmentManifest(headers[0].Path)
			require.NoError(t, err)
			require.Equal(t, uint64(296), manifest.ThroughSlot)
			require.Equal(t, []SlotBankhash{{Slot: 289, Bankhash: bh(289)}, {Slot: 296, Bankhash: bh(296)}}, manifest.Bankhashes)
			meta, present, err := db.readFoldMeta()
			require.NoError(t, err)
			t.Logf("before recovery: index metadata present=%t, batch=%d, through=%d", present, meta.BatchSeq, meta.ThroughSlot)
			if mode != "wal-off" {
				require.True(t, present)
				require.Equal(t, uint64(296), meta.ThroughSlot)
			}
			recovered, err := db.RecoverFoldState()
			require.NoError(t, err)
			require.Equal(t, uint64(296), recovered.DurableThrough)
			require.Equal(t, bh(296), recovered.RootedBankhash)
			t.Logf("replayed batches: %v", recovered.ReplayedBatches)
			for _, slot := range []uint64{289, 296} {
				got, err := db.GetBankHashForSlot(slot)
				if !assert.NoError(t, err, "manifest-backed slot %d must be queryable after recovery", slot) {
					continue
				}
				want := bh(slot)
				require.Equal(t, want[:], got)
			}
		})
	}
}
