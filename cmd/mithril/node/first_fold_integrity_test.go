package node

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func firstFoldIntegrityFixture(t *testing.T) (string, *accountsdb.AccountsDb, *state.MithrilState) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "accounts"), 0o700))
	for _, name := range []string{"largest_file_id", "bootstrap_high_file_id"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), make([]byte, 8), 0o600))
	}
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.CloseDb()) })
	s := state.NewReadyState(7, 0, "", "", 0, 0)
	hash := [32]byte{8}
	require.NoError(t, s.UpdateOnShutdown(dir, 8, hash[:], nil))
	s, err = state.LoadState(dir)
	require.NoError(t, err)
	return dir, db, s
}

// The fixture saves an executed tip through the real state writer; no durable
// fold has committed that tip.
func TestValidateRecoveredStateBeforeIntegrityFirstFold(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		name := "saved_unfolded_tip"
		if fresh {
			name = "fresh_snapshot"
		}
		t.Run(name, func(t *testing.T) {
			dir, db, s := firstFoldIntegrityFixture(t)
			if fresh {
				s = state.NewReadyState(7, 0, "", "", 0, 0)
				require.NoError(t, s.Save(dir))
			}
			before, err := os.ReadFile(filepath.Join(dir, "mithril_state.json"))
			require.NoError(t, err)
			original := *s
			rec, err := db.RecoverFoldState()
			require.NoError(t, err)
			require.Zero(t, rec.DurableThrough)
			require.Zero(t, rec.BatchSeq)
			require.NoError(t, adoptRecoveredManifestContextBeforeIntegrity(dir, s, rec))
			require.NoError(t, validateRecoveredStateBeforeIntegrity(s, db, true, rec))
			require.Equal(t, original, *s, "validation must preserve executed-tip diagnostics")
			after, err := os.ReadFile(filepath.Join(dir, "mithril_state.json"))
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestValidateRecoveredStateBeforeIntegrityClassic(t *testing.T) {
	for _, outcome := range []string{"matching", "missing", "mismatch", "beyond_tip"} {
		t.Run(outcome, func(t *testing.T) {
			_, db, s := firstFoldIntegrityFixture(t)
			s.Cluster = "alpenglow" // Configured mode, not the JSON label, is authoritative.
			hash := [32]byte{8}
			if outcome != "missing" {
				if outcome == "mismatch" {
					hash[0]++
				}
				require.NoError(t, db.StoreBankHashForSlot(8, hash[:]))
			}
			if outcome == "beyond_tip" {
				require.NoError(t, db.StoreBankHashForSlot(9, hash[:]))
			}
			err := validateRecoveredStateBeforeIntegrity(s, db, false, accountsdb.RecoveryResult{})
			if outcome == "matching" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, s.ValidateAgainstBankhashDB(db).Error())
			}
		})
	}
}

func TestValidateRecoveredStateBeforeIntegrityRejectsContradictions(t *testing.T) {
	cases := []struct {
		name  string
		alter func(*state.MithrilState, *accountsdb.RecoveryResult)
	}{
		{"saved_root", func(s *state.MithrilState, _ *accountsdb.RecoveryResult) { s.LastRootedSlot = 8 }},
		{"saved_root_hash", func(s *state.MithrilState, _ *accountsdb.RecoveryResult) { s.LastRootedBankhash = "unexpected" }},
		{"saved_root_context", func(s *state.MithrilState, _ *accountsdb.RecoveryResult) {
			s.LastRootedContext = &state.ResumeContext{}
		}},
		{"batch_without_frontier", func(_ *state.MithrilState, r *accountsdb.RecoveryResult) { r.BatchSeq = 1 }},
		{"recovered_context", func(_ *state.MithrilState, r *accountsdb.RecoveryResult) { r.ResumeCtx = []byte("{}") }},
		{"recovered_root_hash", func(_ *state.MithrilState, r *accountsdb.RecoveryResult) { r.RootedBankhash[0] = 8 }},
		{"interrupted_rewind", func(_ *state.MithrilState, r *accountsdb.RecoveryResult) { r.RewindInProgress = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, db, s := firstFoldIntegrityFixture(t)
			// Fresh JSON would pass the legacy check: only the contradictory
			// recovery/root metadata should make these controls fail.
			s.LastSlot = 0
			s.LastBankhash = ""
			rec, err := db.RecoverFoldState()
			require.NoError(t, err)
			tc.alter(s, &rec)
			require.Error(t, validateRecoveredStateBeforeIntegrity(s, db, true, rec))
		})
	}
}

func TestValidateRecoveredStateBeforeIntegrityRejectsNonemptyStore(t *testing.T) {
	for _, name := range []string{"unrelated_slot", "malformed_key"} {
		key := []byte("malformed")
		if name == "unrelated_slot" {
			key = make([]byte, 8)
		}
		if len(key) == 8 {
			binary.LittleEndian.PutUint64(key, 50) // Neither the tip nor its successor.
		}
		t.Run(name, func(t *testing.T) {
			_, db, s := firstFoldIntegrityFixture(t)
			require.NoError(t, db.BankHashStore.Set(key, []byte{1}, nil))
			rec, err := db.RecoverFoldState()
			require.NoError(t, err)
			require.Error(t, validateRecoveredStateBeforeIntegrity(s, db, true, rec))
		})
	}
}

func TestValidateRecoveredStateBeforeIntegrityLegacyAlpenglow(t *testing.T) {
	_, db, s := firstFoldIntegrityFixture(t)
	hash := [32]byte{8}
	require.NoError(t, db.StoreBankHashForSlot(8, hash[:]))
	rec, err := db.RecoverFoldState()
	require.NoError(t, err)
	require.NoError(t, validateRecoveredStateBeforeIntegrity(s, db, true, rec))
}

func TestValidateRecoveredStateBeforeIntegrityIteratorError(t *testing.T) {
	var inject atomic.Bool
	fs := errorfs.Wrap(vfs.NewMem(), errorfs.InjectorFunc(func(op errorfs.Op, path string) error {
		if inject.Load() && strings.HasSuffix(path, ".sst") && op.OpKind() == errorfs.OpKindRead {
			return errorfs.ErrInjected
		}
		return nil
	}))
	opts := &pebble.Options{FS: fs}
	store, err := pebble.Open("bankhash", opts)
	require.NoError(t, err)
	require.NoError(t, store.Set([]byte("unrelated"), []byte{1}, nil))
	require.NoError(t, store.Flush())
	require.NoError(t, store.Close())
	store, err = pebble.Open("bankhash", opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		inject.Store(false)
		require.NoError(t, store.Close())
	})
	inject.Store(true)
	err = validateRecoveredStateBeforeIntegrity(state.NewReadyState(7, 0, "", "", 0, 0),
		&accountsdb.AccountsDb{BankHashStore: store}, true, accountsdb.RecoveryResult{})
	require.ErrorIs(t, err, errorfs.ErrInjected)
}

func TestValidateRecoveredStateBeforeIntegrityFolded(t *testing.T) {
	for _, outcome := range []string{"matching", "missing", "mismatch", "lost_frontier_with_row", "invalid_context"} {
		t.Run(outcome, func(t *testing.T) {
			dir, db, s := firstFoldIntegrityFixture(t)
			hash := [32]byte{8}
			ctx := testDurableStatusContext(t, dir, 8)
			ctx.Bankhash = base58.Encode(hash[:])
			encoded, err := json.Marshal(ctx)
			require.NoError(t, err)
			if outcome == "invalid_context" {
				encoded = []byte("{}")
			}
			_, err = db.CommitBatch(nil, 8, map[uint64][32]byte{8: hash}, encoded)
			require.NoError(t, err)
			rec, err := db.RecoverFoldState()
			require.NoError(t, err)
			require.Equal(t, uint64(8), rec.DurableThrough)
			require.Equal(t, uint64(1), rec.BatchSeq)
			switch outcome {
			case "missing":
				var key [8]byte
				binary.LittleEndian.PutUint64(key[:], 8)
				require.NoError(t, db.BankHashStore.Delete(key[:], nil))
			case "mismatch":
				hash[0]++
				require.NoError(t, db.StoreBankHashForSlot(8, hash[:]))
			case "lost_frontier_with_row":
				s.LastRootedSlot = 9
				s.LastRootedBankhash = base58.Encode(hash[:])
				require.NoError(t, db.StoreBankHashForSlot(9, hash[:]))
				require.NoError(t, s.ValidateAgainstBankhashDB(db), "advisory row alone must not establish durability")
			}
			err = adoptRecoveredManifestContextBeforeIntegrity(dir, s, rec)
			if err == nil {
				err = validateRecoveredStateBeforeIntegrity(s, db, true, rec)
			}
			if outcome == "matching" {
				require.NoError(t, err)
				require.Equal(t, uint64(8), s.LastRootedSlot)
			} else {
				require.Error(t, err)
			}
		})
	}
}
