package node

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestRewindPersistsLowerStateBeforeForwardReplay(t *testing.T) {
	for _, saveFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failed-save-restart"}[saveFails], func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0644))
			db, err := accountsdb.OpenDb(dir)
			require.NoError(t, err)
			db.RootedDurable = true
			db.InitCaches()
			var tip *state.ResumeContext
			commit := func(slot uint64) {
				tip = testDurableStatusContext(t, dir, slot)
				encoded, err := json.Marshal(tip)
				require.NoError(t, err)
				_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{{Key: solana.PublicKey{9}, Lamports: slot}}}}, slot, nil, encoded)
				require.NoError(t, err)
			}
			commit(2)
			commit(4)
			s := &state.MithrilState{StateSchemaVersion: state.CurrentStateSchemaVersion, Stage: "ready", LastRootedSlot: 4, LastRootedContext: tip, LastRootedBankhash: tip.Bankhash, LastSlot: 4}
			require.NoError(t, s.Save(dir))
			if saveFails {
				require.NoError(t, os.Mkdir(filepath.Join(dir, state.StateFileName+".tmp"), 0755))
			}
			_, err = db.RewindToBatchBoundaryAndPersist(2, func(res accountsdb.RewindResult) error { return persistRewindResult(dir, s, res) })
			if saveFails {
				require.ErrorContains(t, err, "persist rewound state")
				require.Equal(t, uint64(4), s.LastRootedSlot, "failed save must not publish in-memory state")
				require.NoError(t, os.Remove(filepath.Join(dir, state.StateFileName+".tmp")))
			} else {
				require.NoError(t, err)
			}
			db.CloseDb()
			db, err = accountsdb.OpenDb(dir)
			require.NoError(t, err)
			db.RootedDurable = true
			db.InitCaches()
			s, err = state.LoadState(dir)
			require.NoError(t, err)
			rec, err := db.RecoverFoldState()
			require.NoError(t, err)
			require.Equal(t, saveFails, rec.RewindInProgress)
			if saveFails {
				completeInterruptedRewind(dir, db, s)
			}
			persisted, err := state.LoadState(dir)
			require.NoError(t, err)
			require.Equal(t, uint64(2), persisted.LastRootedSlot)
			require.Equal(t, uint64(2), persisted.LastSlot)
			require.Equal(t, uint64(2), persisted.LastRootedContext.Slot)
			require.NoError(t, validateDurableResumeContext(dir, 2, persisted.LastRootedContext))
			// Reproduce the original incident's ordering: fold ahead without a graceful
			// state save. A restart must see the store ahead, never stale state ahead.
			commit(3)
			db.CloseDb()
			db, err = accountsdb.OpenDb(dir)
			require.NoError(t, err)
			defer db.CloseDb()
			rec, err = db.RecoverFoldState()
			require.NoError(t, err)
			require.False(t, rec.RewindInProgress)
			require.Greater(t, rec.DurableThrough, persisted.LastRootedSlot)
			require.NoError(t, adoptRecoveredManifestContextBeforeIntegrity(dir, persisted, rec))
			require.Equal(t, uint64(3), persisted.LastRootedSlot)
		})
	}
}

func TestPersistRewindRejectsInvalidStatusCheckpoint(t *testing.T) {
	dir := t.TempDir()
	ctx := testDurableStatusContext(t, dir, 2)
	s := &state.MithrilState{StateSchemaVersion: state.CurrentStateSchemaVersion, LastRootedSlot: 4, LastSlot: 4}
	require.NoError(t, s.Save(dir))
	// Mutate the serialized target identity, keeping the sidecar itself valid.
	ctx.AlpenglowBlockID = solana.Hash{99}.String()
	encoded, err := json.Marshal(ctx)
	require.NoError(t, err)
	require.Error(t, persistRewindResult(dir, s, accountsdb.RewindResult{NewThrough: 2, ResumeCtx: encoded}))
	saved, err := state.LoadState(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(4), saved.LastRootedSlot)
	require.Equal(t, uint64(4), s.LastRootedSlot)
}
