package node

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The GC runs before replay reads anything, so its keep set must be exactly
// what the resume path will look for. Every keep-set bug found so far deleted
// a sidecar that resume then needed.
func TestClassicStartupCheckpointGCKeepSet(t *testing.T) {
	const (
		snapshotSlot = uint64(50)
		lastSlot     = uint64(200)
		staleSlot    = uint64(100)
	)

	type fixture struct {
		current *state.TransactionStatusCheckpointRef // sidecar at LastSlot
		stale   *state.TransactionStatusCheckpointRef // sidecar at an earlier slot
	}

	tests := []struct {
		name          string
		mutate        func(f fixture, s *state.MithrilState)
		wantCurrent   bool // sidecar at LastSlot survives
		wantKeptCount int
		wantRefuse    bool
	}{
		{
			name: "classic ref names LastSlot",
			mutate: func(f fixture, s *state.MithrilState) {
				s.LastTransactionStatusCheckpoint = f.current
			},
			wantCurrent:   true,
			wantKeptCount: 1,
		},
		{
			name: "rooted context at LastSlot",
			mutate: func(f fixture, s *state.MithrilState) {
				s.LastRootedContext = &state.ResumeContext{Slot: lastSlot, TransactionStatusCheckpoint: f.current}
			},
			wantCurrent:   true,
			wantKeptCount: 1,
		},
		{
			// a reference left over from an earlier slot must not
			// retain its own file, and must not suppress the recovery scan.
			name: "classic ref names an earlier slot",
			mutate: func(f fixture, s *state.MithrilState) {
				s.LastTransactionStatusCheckpoint = f.stale
			},
			wantCurrent:   true,
			wantKeptCount: 1,
		},
		{
			// Same for a rooted context that does not sit at LastSlot.
			name: "rooted context at an earlier slot",
			mutate: func(f fixture, s *state.MithrilState) {
				s.LastRootedContext = &state.ResumeContext{Slot: staleSlot, TransactionStatusCheckpoint: f.stale}
			},
			wantCurrent:   true,
			wantKeptCount: 1,
		},
		{
			// an older binary rewrites the state file without the
			// reference. The sidecar is still recoverable and must be retained.
			name: "no ref but recoverable sidecar at LastSlot",
			mutate: func(fixture, *state.MithrilState) {
			},
			wantCurrent:   true,
			wantKeptCount: 1,
		},
		{
			// LastSlot at the snapshot means resume reads Agave's seed, not a
			// sidecar, so the recovery scan is deliberately skipped.
			name: "no ref and LastSlot is the snapshot slot",
			mutate: func(f fixture, s *state.MithrilState) {
				s.SnapshotSlot = lastSlot
			},
			wantCurrent:   false,
			wantKeptCount: 0,
		},
		{
			// a failed checkpoint records the slot with no
			// reference, so nothing on disk names LastSlot. Collecting on an
			// empty keep set deleted the operator's last good sidecar.
			name: "no ref and no sidecar for LastSlot",
			mutate: func(f fixture, s *state.MithrilState) {
				s.LastSlot = lastSlot + 100
			},
			wantRefuse: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accountsPath := t.TempDir()
			current, err := replay.PrepareTransactionStatusCheckpoint(accountsPath, lastSlot, []byte("window-at-last-slot"))
			require.NoError(t, err)
			stale, err := replay.PrepareTransactionStatusCheckpoint(accountsPath, staleSlot, []byte("window-at-an-older-slot"))
			require.NoError(t, err)

			s := &state.MithrilState{SnapshotSlot: snapshotSlot, LastSlot: lastSlot}
			tt.mutate(fixture{current: current, stale: stale}, s)

			keep, keepErr := classicCheckpointKeepSet(s, accountsPath)
			if tt.wantRefuse {
				require.Error(t, keepErr, "an unresolvable keep set must refuse, not empty the directory")
				return
			}
			require.NoError(t, keepErr)
			require.Len(t, keep, tt.wantKeptCount)
			_, err = replay.CleanupTransactionStatusCheckpoints(accountsPath, keep)
			require.NoError(t, err)

			if tt.wantCurrent {
				assert.FileExists(t, checkpointFilePath(accountsPath, current),
					"the sidecar resume will ask for was deleted")
				_, err := replay.ReadTransactionStatusCheckpoint(accountsPath, current)
				assert.NoError(t, err)
			} else {
				assert.NoFileExists(t, checkpointFilePath(accountsPath, current))
			}
			assert.NoFileExists(t, checkpointFilePath(accountsPath, stale),
				"a sidecar for an earlier slot is never what resume asks for")
		})
	}
}

// A durable slot the state file cannot describe is not "nothing to keep" — it is
// "cannot tell what is current". Refusing is what stops cleanup running on an
// empty keep set and deleting a sidecar that is still the operator's best copy.
func TestClassicStartupCheckpointGCNothingOnDisk(t *testing.T) {
	accountsPath := t.TempDir()
	// A sidecar exists, but for a slot the state file does not claim.
	orphan, err := replay.PrepareTransactionStatusCheckpoint(accountsPath, 150, []byte("window-at-an-unclaimed-slot"))
	require.NoError(t, err)

	s := &state.MithrilState{SnapshotSlot: 50, LastSlot: 200}
	keep, keepErr := classicCheckpointKeepSet(s, accountsPath)
	require.Error(t, keepErr, "no sidecar for the durable slot must refuse collection")
	require.Empty(t, keep)

	// The caller skips cleanup on that error; if it did not, the orphan would be
	// the file destroyed, so assert it is still there.
	assert.FileExists(t, checkpointFilePath(accountsPath, orphan),
		"refusing to collect must leave existing sidecars untouched")
}
