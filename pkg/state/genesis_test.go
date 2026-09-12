package state

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenesisSchemaExplicitRoot(t *testing.T) {
	s := &MithrilState{StateSchemaVersion: GenesisStateSchemaVersion, Origin: "genesis", Stage: "ready", GenesisHash: "4qe9kVjEAD22FJbJ4ivcy4tC9ycXqRov8s2wdc9tyHEw", Genesis: &GenesisOrigin{Version: 1, StorageFormat: GenesisStorageFormat, BankHash: "B66Q7Rn9cdv3Gd4Pj946Y3CeJN76vYUxfqaN1FmR5sxJ", NextReplaySlot: 1, MetadataSHA256: strings.Repeat("ab", 32)}}
	require.NoError(t, s.ValidateGenesisOrigin())
	require.True(t, s.HasDurableRoot())
	require.False(t, (&MithrilState{}).HasDurableRoot())
	path := t.TempDir()
	require.NoError(t, s.Save(path))
	loaded, err := LoadState(path)
	require.NoError(t, err)
	require.Equal(t, s, loaded)
	require.Equal(t, uint64(1), loaded.GetResumeSlot())
	s.StateSchemaVersion = GenesisReplayStateSchemaVersion
	require.NoError(t, s.Save(path))
	loaded, err = LoadState(path)
	require.NoError(t, err)
	require.True(t, loaded.HasGenesisRoot(), "the slot-0 anchor is preserved")
	_, err = CheckAndLoadValidState(path)
	require.ErrorContains(t, err, "manifest-aware")
	require.ErrorContains(t, RejectGenesisLaunch(path), "preserved")
	s.StateSchemaVersion = CurrentStateSchemaVersion
	require.NoError(t, s.Save(path))
	_, err = LoadState(path)
	require.ErrorContains(t, err, "unsupported")
	s.StateSchemaVersion = GenesisStateSchemaVersion
	s.Genesis = nil
	require.NoError(t, s.Save(path))
	_, err = LoadState(path)
	require.ErrorContains(t, err, "malformed")
	// Unsupported future schemas must never be treated as absent state.
	require.NoError(t, os.WriteFile(filepath.Join(path, StateFileName), []byte(`{"state_schema_version":99,"stage":"ready"}`), 0644))
	_, err = CheckAndLoadValidState(path)
	require.Error(t, err)
}

func TestPebbleGenesisRejectsV2SchemasAndWrongBackend(t *testing.T) {
	for _, version := range []uint32{4, 5, GenesisStateSchemaVersion, GenesisReplayStateSchemaVersion} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			root := t.TempDir()
			s := &MithrilState{StateSchemaVersion: version, Origin: "genesis", Stage: "ready", Genesis: &GenesisOrigin{Version: 1, StorageFormat: "streamhash-v2"}}
			require.NoError(t, s.Save(root))
			before, err := os.ReadFile(filepath.Join(root, StateFileName))
			require.NoError(t, err)
			_, err = LoadState(root)
			require.Error(t, err)
			require.Error(t, RejectGenesisLaunch(root))
			after, err := os.ReadFile(filepath.Join(root, StateFileName))
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
