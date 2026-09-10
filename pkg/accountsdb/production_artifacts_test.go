package accountsdb

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type productionArtifactTestSource struct{}

func (productionArtifactTestSource) Scan(
	ctx context.Context,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := solana.PublicKey{1, 2, 3}
	return visit(key, AccountIndexEntry{Slot: 11, FileId: 7, Offset: 16})
}

func initializeProductionArtifactsForTest(t *testing.T) (string, *RootIndexCatalog) {
	t.Helper()
	root := t.TempDir()
	config := DefaultProductionAccountIndexConfig()
	config.ShardCount = 1
	config.CheckpointWorkers = 1
	config.MaxConcurrentSeals = 1
	require.NoError(t, InitializeProductionAccountIndex(t.Context(), root, productionArtifactTestSource{}, config))
	catalog, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	return root, catalog
}

func TestValidateProductionAccountIndexArtifactsRoundTripAndIdentity(t *testing.T) {
	root, catalog := initializeProductionArtifactsForTest(t)
	require.NoError(t, ValidateProductionAccountIndexArtifacts(root))

	scanPath, err := ResolveIndexCatalogArtifactPath(root, catalog.Shards[0].BaseRecords)
	require.NoError(t, err)
	file, err := os.OpenFile(scanPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte{0xff}, 0)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	err = ValidateProductionAccountIndexArtifacts(root)
	require.ErrorIs(t, err, ErrProductionAccountIndexPoisoned)
	require.ErrorContains(t, err, "root-selected immutable artifacts")
}

func TestValidateProductionAccountIndexArtifactsReturnsTypedMigrationErrors(t *testing.T) {
	tests := []struct {
		name       string
		artifact   string
		wantFormat LegacyAccountIndexFormat
	}{
		{"Pebble", "mithril_db", LegacyAccountIndexPebble},
		{"V1 StreamHash", StreamIndexManifestFileName, LegacyAccountIndexV1},
		{"V1 checkpoint", DeltaCheckpointFilePrefix + "00000000000000000001.desc", LegacyAccountIndexV1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, test.artifact)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
			require.NoError(t, os.WriteFile(path, []byte("legacy"), 0o644))

			err := ValidateProductionAccountIndexArtifacts(root)
			require.ErrorIs(t, err, ErrAccountIndexMigrationRequired)
			var migration *AccountIndexMigrationError
			require.True(t, errors.As(err, &migration))
			assert.Equal(t, test.wantFormat, migration.Format)
			assert.Contains(t, migration.Artifacts, path)
		})
	}
}

func TestValidateShardedMutableJournalReadOnlyChecksEveryRecordAndClassifiesRepairableTail(t *testing.T) {
	root, catalog := initializeProductionArtifactsForTest(t)
	header := encodeDeltaJournalHeader(0, 0)
	frame, err := encodeDeltaFrame(1, []deltaIndexMutation{
		liveDeltaMutation(solana.PublicKey{9}, AccountIndexEntry{Slot: 4, FileId: 5, Offset: 8}),
	}, nil, false)
	require.NoError(t, err)
	journalPath := filepath.Join(root, ShardedDeltaIndexJournalFileName)
	require.NoError(t, os.WriteFile(journalPath, append(header, frame...), 0o644))
	require.NoError(t, validateShardedMutableJournalReadOnly(root, catalog))

	// A valid CRC cannot disguise a semantically invalid mutation.
	frame[deltaFrameHeaderSize+56] = deltaMutationRetire
	binary.LittleEndian.PutUint32(frame[60:64], checksumDeltaFrame(frame[:deltaFrameHeaderSize], frame[deltaFrameHeaderSize:]))
	require.NoError(t, os.WriteFile(journalPath, append(header, frame...), 0o644))
	err = validateShardedMutableJournalReadOnly(root, catalog)
	require.ErrorContains(t, err, "non-zero pubkey")

	// A normal crash tail after complete state is accepted but left byte-for-byte
	// for OpenProductionAccountIndex to repair under its exclusive lock.
	frame[deltaFrameHeaderSize+56] = deltaMutationLive
	binary.LittleEndian.PutUint32(frame[60:64], checksumDeltaFrame(frame[:deltaFrameHeaderSize], frame[deltaFrameHeaderSize:]))
	torn := append(append([]byte(nil), header...), frame[:len(frame)-1]...)
	require.NoError(t, os.WriteFile(journalPath, torn, 0o644))
	before, err := os.Stat(journalPath)
	require.NoError(t, err)
	require.NoError(t, validateShardedMutableJournalReadOnly(root, catalog))
	after, statErr := os.Stat(journalPath)
	require.NoError(t, statErr)
	assert.Equal(t, before.Size(), after.Size(), "read-only validation must not repair/truncate")
}

func TestValidateShardedMutableJournalReadOnlyRejectsSymlinkWithoutMutatingTarget(t *testing.T) {
	root, catalog := initializeProductionArtifactsForTest(t)
	journalPath := filepath.Join(root, ShardedDeltaIndexJournalFileName)
	externalPath := filepath.Join(root, "external-journal-target")
	require.NoError(t, os.Rename(journalPath, externalPath))
	want, err := os.ReadFile(externalPath)
	require.NoError(t, err)
	require.NoError(t, os.Symlink(externalPath, journalPath))

	err = validateShardedMutableJournalReadOnly(root, catalog)
	require.Error(t, err)
	got, err := os.ReadFile(externalPath)
	require.NoError(t, err)
	assert.Equal(t, want, got, "rejected journal symlink must not mutate its target")
	info, err := os.Lstat(journalPath)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink)
}

func TestValidateShardedMutableJournalRejectsSparseHugeFrameBeforePayloadRead(t *testing.T) {
	root, catalog := initializeProductionArtifactsForTest(t)
	journalPath := filepath.Join(root, ShardedDeltaIndexJournalFileName)

	frameHeader := make([]byte, deltaFrameHeaderSize)
	copy(frameHeader[:8], deltaFrameMagic[:])
	binary.LittleEndian.PutUint32(frameHeader[8:12], deltaJournalVersion)
	count := uint32(ShardedMutableAbsoluteMaxFrameBytes/deltaMutationSize) + 1
	binary.LittleEndian.PutUint64(frameHeader[16:24], uint64(deltaFrameHeaderSize)+uint64(count)*deltaMutationSize)
	binary.LittleEndian.PutUint64(frameHeader[24:32], 1)
	binary.LittleEndian.PutUint32(frameHeader[56:60], count)
	contents := append(encodeDeltaJournalHeader(0, 0), frameHeader...)
	require.NoError(t, os.WriteFile(journalPath, contents, 0o644))
	file, err := os.OpenFile(journalPath, os.O_WRONLY, 0)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(16<<30))
	require.NoError(t, file.Close())

	err = validateShardedMutableJournalReadOnly(root, catalog)
	require.ErrorContains(t, err, "invalid sharded mutable frame length")
	info, statErr := os.Stat(journalPath)
	require.NoError(t, statErr)
	assert.Equal(t, int64(16<<30), info.Size())
}
