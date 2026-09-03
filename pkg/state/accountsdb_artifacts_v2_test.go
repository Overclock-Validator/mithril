package state

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stateProductionIndexSource struct{}

func (stateProductionIndexSource) Scan(
	ctx context.Context,
	visit func(solana.PublicKey, accountsdb.AccountIndexEntry) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return visit(
		solana.PublicKey{1},
		accountsdb.AccountIndexEntry{Slot: 1, FileId: 1, Offset: 8},
	)
}

func initializeReadyV2StateForTest(t *testing.T) (string, accountsdb.ProductionAccountIndexConfig) {
	t.Helper()
	root := t.TempDir()
	config := accountsdb.DefaultProductionAccountIndexConfig()
	config.ShardCount = 1
	config.CheckpointWorkers = 1
	config.MaxConcurrentSeals = 1
	require.NoError(t, accountsdb.InitializeProductionAccountIndex(
		t.Context(), root, stateProductionIndexSource{}, config,
	))
	for _, directory := range []string{"accounts", "bankhash_db"} {
		require.NoError(t, os.Mkdir(filepath.Join(root, directory), 0o755))
	}
	for file, data := range map[string][]byte{
		"bank_hash": make([]byte, 32),
		"manifest":  []byte("test"),
	} {
		require.NoError(t, os.WriteFile(filepath.Join(root, file), data, 0o644))
	}
	require.NoError(t, accountsdb.WriteLargestFileID(root, 0))
	require.NoError(t, accountsdb.WriteBootstrapHighFileID(root, 0))
	require.NoError(t, accountsdb.WriteStakePubkeyIndex(
		filepath.Join(root, "stake_pubkeys.idx"),
		[]accountsdb.StakeIndexEntry{{Pubkey: solana.PublicKey{1}, FileId: 1, Offset: 8}},
	))
	require.NoError(t, NewReadyState(100, 2, "", "", 0, 0).Save(root))
	return root, config
}

func stateTestJournalHeader(baseSequence uint64, stateFrames uint32) []byte {
	header := make([]byte, 32)
	copy(header[:8], []byte("MITHDJ01"))
	binary.LittleEndian.PutUint32(header[8:12], 1)
	binary.LittleEndian.PutUint32(header[12:16], 32)
	binary.LittleEndian.PutUint64(header[16:24], baseSequence)
	binary.LittleEndian.PutUint32(header[24:28], stateFrames)
	binary.LittleEndian.PutUint32(
		header[28:32],
		crc32.Checksum(header[:28], crc32.MakeTable(crc32.Castagnoli)),
	)
	return header
}

func TestValidateAccountsDbArtifactsPreservesTypedLegacyMigrationError(t *testing.T) {
	root := t.TempDir()
	legacyPath := filepath.Join(root, "mithril_db")
	require.NoError(t, os.Mkdir(legacyPath, 0o755))

	err := ValidateAccountsDbArtifacts(root)
	require.ErrorIs(t, err, accountsdb.ErrAccountIndexMigrationRequired)
	var migration *accountsdb.AccountIndexMigrationError
	require.True(t, errors.As(err, &migration))
	assert.Equal(t, accountsdb.LegacyAccountIndexPebble, migration.Format)
}

func TestCheckAndLoadValidStateFailsClosedForReadyStateWithInvalidIndex(t *testing.T) {
	root := t.TempDir()
	ready := NewReadyState(100, 2, "", "", 0, 0)
	require.NoError(t, ready.Save(root))

	loaded, err := CheckAndLoadValidState(root)
	assert.Nil(t, loaded)
	require.ErrorIs(t, err, accountsdb.ErrProductionAccountIndexPoisoned)
	require.ErrorContains(t, err, "state file says ready")
}

func TestValidateAccountsDbArtifactsRejectsIncompleteRequiredSidecars(t *testing.T) {
	tests := []struct {
		name     string
		artifact string
		mutate   func(t *testing.T, path string)
	}{
		{
			name:     "missing bootstrap high-water mark",
			artifact: "bootstrap_high_file_id",
			mutate: func(t *testing.T, path string) {
				require.NoError(t, os.Remove(path))
			},
		},
		{
			name:     "truncated bootstrap high-water mark",
			artifact: "bootstrap_high_file_id",
			mutate: func(t *testing.T, path string) {
				require.NoError(t, os.WriteFile(path, make([]byte, 7), 0o644))
			},
		},
		{
			name:     "corrupt bootstrap high-water checksum",
			artifact: "bootstrap_high_file_id",
			mutate: func(t *testing.T, path string) {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				data[16] ^= 0x80
				require.NoError(t, os.WriteFile(path, data, 0o644))
			},
		},
		{
			name:     "missing stake index",
			artifact: "stake_pubkeys.idx",
			mutate: func(t *testing.T, path string) {
				require.NoError(t, os.Remove(path))
			},
		},
		{
			name:     "malformed stake index",
			artifact: "stake_pubkeys.idx",
			mutate: func(t *testing.T, path string) {
				require.NoError(t, os.WriteFile(path, make([]byte, 8+accountsdb.StakeIndexRecordSize), 0o644))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, _ := initializeReadyV2StateForTest(t)
			test.mutate(t, filepath.Join(root, test.artifact))
			err := ValidateAccountsDbArtifacts(root)
			require.Error(t, err)
			require.ErrorContains(t, err, test.artifact)
		})
	}
}

func TestReadyStateAllowsOnlyRuntimeRepairableJournalTail(t *testing.T) {
	t.Run("ordinary trailing partial write", func(t *testing.T) {
		root, config := initializeReadyV2StateForTest(t)
		journalPath := filepath.Join(root, accountsdb.ShardedDeltaIndexJournalFileName)
		journal, err := os.OpenFile(journalPath, os.O_WRONLY|os.O_APPEND, 0)
		require.NoError(t, err)
		_, err = journal.Write([]byte("partial"))
		require.NoError(t, err)
		require.NoError(t, journal.Close())

		loaded, err := CheckAndLoadValidState(root)
		require.NoError(t, err)
		require.NotNil(t, loaded)
		before, err := os.Stat(journalPath)
		require.NoError(t, err)
		assert.Greater(t, before.Size(), int64(32))

		index, err := accountsdb.OpenProductionAccountIndex(root, config)
		require.NoError(t, err)
		require.NoError(t, index.Close())
		after, err := os.Stat(journalPath)
		require.NoError(t, err)
		assert.Equal(t, int64(32), after.Size(), "runtime open owns safe torn-tail repair")
	})

	t.Run("later frame marker makes a short frame interior corruption", func(t *testing.T) {
		root, _ := initializeReadyV2StateForTest(t)
		frameHeader := make([]byte, 64)
		copy(frameHeader[:8], []byte("MITHDF01"))
		binary.LittleEndian.PutUint32(frameHeader[8:12], 1)
		binary.LittleEndian.PutUint64(frameHeader[16:24], 192)
		binary.LittleEndian.PutUint64(frameHeader[24:32], 1)
		binary.LittleEndian.PutUint32(frameHeader[56:60], 2)
		journal := append(stateTestJournalHeader(0, 0), frameHeader...)
		journal = append(journal, make([]byte, 8)...)
		journal = append(journal, []byte("MITHDF01")...)
		require.NoError(t, os.WriteFile(
			filepath.Join(root, accountsdb.ShardedDeltaIndexJournalFileName), journal, 0o644,
		))

		loaded, err := CheckAndLoadValidState(root)
		assert.Nil(t, loaded)
		require.ErrorIs(t, err, accountsdb.ErrProductionAccountIndexPoisoned)
		require.ErrorContains(t, err, "interior")
	})

	t.Run("compact state tail is not repairable", func(t *testing.T) {
		root, _ := initializeReadyV2StateForTest(t)
		journal := append(stateTestJournalHeader(0, 1), []byte("partial")...)
		require.NoError(t, os.WriteFile(
			filepath.Join(root, accountsdb.ShardedDeltaIndexJournalFileName), journal, 0o644,
		))

		loaded, err := CheckAndLoadValidState(root)
		assert.Nil(t, loaded)
		require.ErrorIs(t, err, accountsdb.ErrProductionAccountIndexPoisoned)
		require.ErrorContains(t, err, "compact sharded mutable state is torn")
	})
}
