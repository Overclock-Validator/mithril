package accountsdb

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// This child exits without deferred closes/flushes. Both WAL modes exercise
// real lost memtables, rather than CloseDb making the interruption durable.
func TestRewindIntentCrashChild(t *testing.T) {
	dir := os.Getenv("MITHRIL_REWIND_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess only")
	}
	DisableIndexWAL = os.Getenv("MITHRIL_REWIND_WAL_OFF") == "true"
	db, err := OpenDb(dir)
	require.NoError(t, err)
	db.RootedDurable = true
	db.InitCaches()
	_, err = db.RecoverFoldState()
	require.NoError(t, err)
	// Source head is deliberately not explicitly flushed here in WAL-off mode.
	commitTestBatch(t, db, 120, foldAcct(1, 120, []byte("v2")))
	commitTestBatch(t, db, 130, foldAcct(1, 130, []byte("v3")))
	crash := func() { os.Exit(91) }
	switch os.Getenv("MITHRIL_REWIND_CRASH_AT") {
	case "intent":
		db.rewindHooks.afterIntent = crash
	case "partial-park":
		db.rewindHooks.afterPark = crash
	case "all-park":
		parks := 0
		db.rewindHooks.afterPark = func() {
			parks++
			if parks == 2 {
				crash()
			}
		}
	case "index-before-flush":
		db.rewindHooks.beforeIndexFlush = crash
	case "index-durable":
		db.rewindHooks.afterIndex = crash
	case "state-durable":
		db.rewindHooks.afterState = crash
	}
	_, err = db.RewindToBatchBoundaryAndPersist(110, func(res RewindResult) error {
		if os.Getenv("MITHRIL_REWIND_CRASH_AT") == "before-state" {
			crash()
		}
		path := filepath.Join(dir, "test-state")
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		if _, err = fmt.Fprint(f, res.NewThrough); err != nil {
			f.Close()
			return err
		}
		if err = f.Sync(); err != nil {
			f.Close()
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		return fsyncDir(dir)
	})
	require.NoError(t, err)
	// Completion followed by a crash must not resurrect a stale intent.
	os.Exit(91)
}

func TestRewindIntentProcessCrashMatrix(t *testing.T) {
	for _, walOff := range []bool{false, true} {
		for _, phase := range []string{"intent", "partial-park", "all-park", "index-before-flush", "index-durable", "before-state", "state-durable", "complete"} {
			t.Run(fmt.Sprintf("walOff=%t/%s", walOff, phase), func(t *testing.T) {
				old := DisableIndexWAL
				DisableIndexWAL = walOff
				defer func() { DisableIndexWAL = old }()
				db, dir := newFoldTestDb(t)
				commitTestBatch(t, db, 110, foldAcct(1, 110, []byte("v1")))
				db.CloseDb()
				cmd := exec.Command(os.Args[0], "-test.run=^TestRewindIntentCrashChild$")
				cmd.Env = append(os.Environ(), "MITHRIL_REWIND_CRASH_DIR="+dir, "MITHRIL_REWIND_WAL_OFF="+fmt.Sprint(walOff), "MITHRIL_REWIND_CRASH_AT="+phase)
				output, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, string(output))
				require.Equal(t, 91, exit.ExitCode(), string(output))
				require.NotContains(t, string(output), "WARNING: DATA RACE")
				db, err = OpenDb(dir)
				require.NoError(t, err)
				db.RootedDurable = true
				db.InitCaches()
				rec, err := db.RecoverFoldState()
				require.NoError(t, err)
				require.Equal(t, phase != "complete", rec.RewindInProgress)
				require.Empty(t, rec.OrphansRemoved)
				if phase != "complete" {
					target, err := db.PendingRewindTarget()
					require.NoError(t, err)
					require.Equal(t, uint64(110), target)
					_, err = db.RewindToBatchBoundaryAndPersist(target, func(res RewindResult) error {
						require.Equal(t, uint64(110), res.NewThrough)
						require.Equal(t, []byte("ctx-110"), res.ResumeCtx)
						return nil // storage test; node tests exercise actual MithrilState.Save
					})
					require.NoError(t, err)
				}
				require.Equal(t, uint64(110), mustColdRead(t, db, 110, solana.PublicKey{1}).Lamports)
				db = reopenFoldTestDb(t, db, dir)
				defer db.CloseDb()
				rec, err = db.RecoverFoldState()
				require.NoError(t, err)
				require.False(t, rec.RewindInProgress)
				require.Equal(t, uint64(110), rec.DurableThrough)
				require.Equal(t, uint64(110), mustColdRead(t, db, 110, solana.PublicKey{1}).Lamports)
			})
		}
	}
}

func TestRewindIntentFailsClosed(t *testing.T) {
	for _, fault := range []string{"save", "checksum", "target", "frontier", "different-target", "intent-write"} {
		t.Run(fault, func(t *testing.T) {
			db, _ := newFoldTestDb(t)
			defer db.CloseDb()
			r := commitTestBatch(t, db, 110, foldAcct(1, 110, nil))
			commitTestBatch(t, db, 120, foldAcct(1, 120, nil))
			if fault == "intent-write" {
				require.NoError(t, os.Mkdir(filepath.Join(db.AcctsDir, rewindIntentName+".tmp"), 0755))
				_, err := db.RewindToBatchBoundaryAndPersist(110, func(RewindResult) error { t.Fatal("callback before intent"); return nil })
				require.Error(t, err)
				require.Equal(t, uint64(120), mustColdRead(t, db, 120, solana.PublicKey{1}).Lamports)
				return
			}
			_, err := db.RewindToBatchBoundaryAndPersist(110, func(RewindResult) error { return fmt.Errorf("injected state save/fsync failure") })
			require.ErrorContains(t, err, "injected")
			path := filepath.Join(db.AcctsDir, rewindIntentName)
			require.FileExists(t, path)
			_, foldErr := db.CommitBatch(nil, 130, nil, nil)
			require.ErrorContains(t, foldErr, "pending rewind")
			_, compactErr := db.CompactOnce(CompactionConfig{})
			require.ErrorContains(t, compactErr, "pending rewind")
			_, rewindErr := db.RewindToBatchBoundary(110)
			require.ErrorContains(t, rewindErr, "pending rewind")
			switch fault {
			case "checksum":
				require.NoError(t, os.WriteFile(path, []byte(`{"Intent":{"Version":1}}`), 0600))
			case "target":
				require.NoError(t, os.WriteFile(segmentManifestPath(db.AcctsDir, 110, r.FileId), []byte("bad manifest"), 0600))
			case "frontier":
				require.NoError(t, db.Index.Delete(metaKeyLastBatch, nil))
			case "different-target":
				_, err = db.RewindToBatchBoundaryAndPersist(120, func(RewindResult) error { t.Fatal("wrong target callback"); return nil })
				require.ErrorContains(t, err, "must finish")
				return
			}
			rec, err := db.RecoverFoldState()
			if fault == "save" {
				require.NoError(t, err)
				require.True(t, rec.RewindInProgress)
			} else {
				require.Error(t, err)
			}
			require.FileExists(t, path)
		})
	}
}
