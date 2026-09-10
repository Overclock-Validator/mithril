package accountsdb

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistentExtentCatalogRoundTripAndStableExtension(t *testing.T) {
	root := t.TempDir()
	first, err := clonePersistentExtentCatalog(nil, 1)
	require.NoError(t, err)
	entries := []AccountIndexEntry{
		{Slot: 10, FileId: 20, Offset: 8},
		{Slot: 10, FileId: 20, Offset: streamIndexExtentSize + 24},
		{Slot: 11, FileId: 21, Offset: 40},
	}
	for _, entry := range entries {
		require.NoError(t, first.ensureExtent(entry))
	}
	firstBinding, err := first.binding()
	require.NoError(t, err)

	firstPath := filepath.Join(root, "first.cat")
	require.NoError(t, writePersistentExtentCatalog(t.Context(), firstPath, first))
	firstArtifact, err := ComputeIndexCatalogArtifact(root, "first.cat")
	require.NoError(t, err)
	opened, err := OpenPersistentExtentCatalogArtifact(root, firstArtifact)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), opened.Generation())
	assert.Equal(t, 3, opened.Len())
	assert.Equal(t, firstArtifact, opened.Artifact())

	oldLocators := make([]uint64, len(entries))
	for i, entry := range entries {
		locator, err := opened.PackAccountIndexEntry(entry)
		require.NoError(t, err)
		oldLocators[i] = locator
		decoded, err := opened.UnpackAccountIndexEntry(locator)
		require.NoError(t, err)
		assert.Equal(t, entry, decoded)
		assert.LessOrEqual(t, locator, packedAccountLocatorMask)
	}

	extended, err := clonePersistentExtentCatalog(opened, 2)
	require.NoError(t, err)
	newEntry := AccountIndexEntry{Slot: 99, FileId: 77, Offset: 3*streamIndexExtentSize + 56}
	require.NoError(t, extended.ensureExtent(newEntry))
	assert.Equal(t, opened.Len()+1, extended.Len())
	for i, entry := range entries {
		locator, err := extended.PackAccountIndexEntry(entry)
		require.NoError(t, err)
		assert.Equal(t, oldLocators[i], locator, "an existing extent ordinal must never move")
	}
	require.NoError(t, extended.validateBinding(firstBinding), "an appended catalog must serve old shards")
	newBinding, err := extended.binding()
	require.NoError(t, err)
	require.Error(t, opened.validateBinding(newBinding), "an old catalog must reject a shard requiring its extension")
}

func TestPersistentExtentCatalogExtensionReusesExactPriorWhenComplete(t *testing.T) {
	root := t.TempDir()
	prior, err := clonePersistentExtentCatalog(nil, 1)
	require.NoError(t, err)
	existing := AccountIndexEntry{Slot: 10, FileId: 20, Offset: 8}
	require.NoError(t, prior.ensureExtent(existing))
	path := filepath.Join(root, "prior.cat")
	require.NoError(t, writePersistentExtentCatalog(t.Context(), path, prior))
	artifact, err := ComputeIndexCatalogArtifact(root, "prior.cat")
	require.NoError(t, err)
	prior, err = OpenPersistentExtentCatalogArtifact(root, artifact)
	require.NoError(t, err)

	extension, err := newPersistentExtentCatalogExtension(prior, 2)
	require.NoError(t, err)
	require.NoError(t, extension.ensureExtent(existing))
	reused, ownsArtifact, err := extension.finish()
	require.NoError(t, err)
	require.False(t, ownsArtifact)
	require.Same(t, prior, reused)
	require.Equal(t, uint64(1), reused.Generation())
	require.Equal(t, artifact, reused.Artifact())

	extension, err = newPersistentExtentCatalogExtension(prior, 2)
	require.NoError(t, err)
	added := AccountIndexEntry{Slot: 11, FileId: 21, Offset: streamIndexExtentSize + 16}
	require.NoError(t, extension.ensureExtent(existing))
	require.NoError(t, extension.ensureExtent(added))
	extended, ownsArtifact, err := extension.finish()
	require.NoError(t, err)
	require.True(t, ownsArtifact)
	require.NotSame(t, prior, extended)
	require.Equal(t, uint64(2), extended.Generation())
	require.Equal(t, prior.Len()+1, extended.Len())
	got, err := extended.PackAccountIndexEntry(added)
	require.NoError(t, err)
	require.Equal(t, uint64(prior.Len())<<streamIndexRelativeBits|2, got)
}

func TestPersistentExtentCatalogAdoptsVerifiedArtifactInPlace(t *testing.T) {
	root := t.TempDir()
	catalog, err := clonePersistentExtentCatalog(nil, 7)
	require.NoError(t, err)
	entries := []AccountIndexEntry{
		{Slot: 10, FileId: 20, Offset: 8},
		{Slot: 11, FileId: 21, Offset: streamIndexExtentSize + 16},
		{Slot: 12, FileId: 22, Offset: 2*streamIndexExtentSize + 24},
	}
	for _, entry := range entries {
		require.NoError(t, catalog.ensureExtent(entry))
	}
	ordinalsPointer := reflect.ValueOf(catalog.ordinals).Pointer()
	extentsPointer := &catalog.extents[0]

	path := filepath.Join(root, "extents.cat")
	require.NoError(t, writePersistentExtentCatalog(t.Context(), path, catalog))
	verified, err := verifyWrittenPersistentExtentCatalog(t.Context(), root, "extents.cat", catalog)
	require.NoError(t, err)
	require.NoError(t, catalog.adoptVerifiedArtifact(root, verified))

	require.True(t, catalog.frozen)
	require.Equal(t, verified.artifact, catalog.Artifact())
	require.Equal(t, verified.artifact.Size, catalog.identity.Size)
	require.Equal(t, verified.artifact.SHA256, catalog.identity.SHA256)
	require.Equal(t, verified.absolutePath, catalog.verifiedPath)
	require.Same(t, verified.fileInfo, catalog.verifiedInfo)
	assert.Equal(t, ordinalsPointer, reflect.ValueOf(catalog.ordinals).Pointer(), "adoption must retain the built ordinal map")
	assert.Same(t, extentsPointer, &catalog.extents[0], "adoption must retain the built extent slice")

	reopened, err := OpenPersistentExtentCatalogArtifact(root, verified.artifact)
	require.NoError(t, err)
	assert.Equal(t, catalog.generation, reopened.generation)
	assert.Equal(t, catalog.lineage, reopened.lineage)
	assert.Equal(t, catalog.extents, reopened.extents)
	assert.Equal(t, catalog.ordinals, reopened.ordinals)
	assert.Equal(t, catalog.identity, reopened.identity)
	for _, entry := range entries {
		adoptedLocator, err := catalog.PackAccountIndexEntry(entry)
		require.NoError(t, err)
		reopenedLocator, err := reopened.PackAccountIndexEntry(entry)
		require.NoError(t, err)
		assert.Equal(t, reopenedLocator, adoptedLocator)
	}
}

func TestPersistentExtentCatalogAdoptionRejectsArtifactPathReplacement(t *testing.T) {
	root := t.TempDir()
	catalog, err := clonePersistentExtentCatalog(nil, 1)
	require.NoError(t, err)
	require.NoError(t, catalog.ensureExtent(AccountIndexEntry{Slot: 1, FileId: 2, Offset: 8}))
	path := filepath.Join(root, "extents.cat")
	require.NoError(t, writePersistentExtentCatalog(t.Context(), path, catalog))
	verified, err := verifyWrittenPersistentExtentCatalog(t.Context(), root, "extents.cat", catalog)
	require.NoError(t, err)

	encoded, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.Rename(path, path+".replaced"))
	require.NoError(t, os.WriteFile(path, encoded, 0o600))
	err = catalog.adoptVerifiedArtifact(root, verified)
	require.ErrorContains(t, err, "changed after verification")
	assert.False(t, catalog.frozen)
	assert.True(t, catalog.artifact.isZero())
	assert.Zero(t, catalog.identity.Size)
	assert.Empty(t, catalog.verifiedPath)
	assert.Nil(t, catalog.verifiedInfo)
}

func TestPersistentExtentCatalogAdoptedPathIdentityRejectsLaterMutation(t *testing.T) {
	root := t.TempDir()
	catalog, err := clonePersistentExtentCatalog(nil, 1)
	require.NoError(t, err)
	require.NoError(t, catalog.ensureExtent(AccountIndexEntry{Slot: 1, FileId: 2, Offset: 8}))
	path := filepath.Join(root, "extents.cat")
	require.NoError(t, writePersistentExtentCatalog(t.Context(), path, catalog))
	verified, err := verifyWrittenPersistentExtentCatalog(t.Context(), root, "extents.cat", catalog)
	require.NoError(t, err)
	require.NoError(t, catalog.adoptVerifiedArtifact(root, verified))

	require.NoError(t, os.Rename(path, path+".replaced"))
	require.NoError(t, os.WriteFile(path, make([]byte, verified.artifact.Size), 0o600))
	err = validateRegularFilePathIdentity(catalog.verifiedPath, catalog.verifiedInfo)
	require.ErrorContains(t, err, "changed after verification")
}

func TestVerifyWrittenPersistentExtentCatalogRejectsDiskDivergence(t *testing.T) {
	root := t.TempDir()
	catalog, err := clonePersistentExtentCatalog(nil, 1)
	require.NoError(t, err)
	require.NoError(t, catalog.ensureExtent(AccountIndexEntry{Slot: 1, FileId: 2, Offset: 8}))
	require.NoError(t, catalog.ensureExtent(AccountIndexEntry{Slot: 3, FileId: 4, Offset: streamIndexExtentSize + 16}))
	validPath := filepath.Join(root, "valid.cat")
	require.NoError(t, writePersistentExtentCatalog(t.Context(), validPath, catalog))
	valid, err := os.ReadFile(validPath)
	require.NoError(t, err)

	tests := []struct {
		name   string
		mutate func([]byte)
		want   string
	}{
		{
			name: "ordinary-bit-flip",
			mutate: func(data []byte) {
				data[0] ^= 1
			},
			want: "bad magic",
		},
		{
			name: "crc-resealed-record",
			mutate: func(data []byte) {
				binary.LittleEndian.PutUint64(data[extentCatalogHeaderSize:extentCatalogHeaderSize+8], 999)
				resealExtentCatalogForTest(data)
			},
			want: "written extent 0 disagrees",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded := append([]byte(nil), valid...)
			test.mutate(encoded)
			relative := test.name + ".cat"
			require.NoError(t, os.WriteFile(filepath.Join(root, relative), encoded, 0o600))
			_, err := verifyWrittenPersistentExtentCatalog(t.Context(), root, relative, catalog)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestPersistentExtentCatalogLocatorBounds(t *testing.T) {
	catalog, err := clonePersistentExtentCatalog(nil, 1)
	require.NoError(t, err)
	entry := AccountIndexEntry{Slot: 1, FileId: 2, Offset: 8}
	require.NoError(t, catalog.ensureExtent(entry))

	_, err = catalog.PackAccountIndexEntry(AccountIndexEntry{Slot: 1, FileId: 2, Offset: 9})
	assert.ErrorIs(t, err, ErrInvalidBaseLocator)
	_, err = catalog.PackAccountIndexEntry(AccountIndexEntry{Slot: 3, FileId: 4, Offset: 8})
	assert.ErrorIs(t, err, ErrInvalidBaseLocator)
	_, err = catalog.UnpackAccountIndexEntry(packedAccountLocatorMask + 1)
	assert.ErrorIs(t, err, ErrInvalidBaseLocator)
	_, err = catalog.UnpackAccountIndexEntry(uint64(catalog.Len()) << streamIndexRelativeBits)
	assert.ErrorIs(t, err, ErrInvalidBaseLocator)
}

func TestPersistentExtentCatalogRejectsCorruption(t *testing.T) {
	root := t.TempDir()
	catalog, err := clonePersistentExtentCatalog(nil, 1)
	require.NoError(t, err)
	require.NoError(t, catalog.ensureExtent(AccountIndexEntry{Slot: 1, FileId: 2, Offset: 8}))
	require.NoError(t, catalog.ensureExtent(AccountIndexEntry{Slot: 3, FileId: 4, Offset: streamIndexExtentSize + 16}))
	validPath := filepath.Join(root, "valid.cat")
	require.NoError(t, writePersistentExtentCatalog(t.Context(), validPath, catalog))
	valid, err := os.ReadFile(validPath)
	require.NoError(t, err)

	tests := []struct {
		name   string
		mutate func([]byte) []byte
		want   string
	}{
		{
			name: "header crc",
			mutate: func(data []byte) []byte {
				data[95] ^= 1
				return data
			},
			want: "header CRC",
		},
		{
			name: "body crc",
			mutate: func(data []byte) []byte {
				data[extentCatalogHeaderSize] ^= 1
				return data
			},
			want: "body CRC",
		},
		{
			name: "truncated",
			mutate: func(data []byte) []byte {
				return data[:len(data)-1]
			},
			want: "file size",
		},
		{
			name: "duplicate extent with valid crcs",
			mutate: func(data []byte) []byte {
				copy(data[extentCatalogHeaderSize+extentCatalogRecordSize:], data[extentCatalogHeaderSize:extentCatalogHeaderSize+extentCatalogRecordSize])
				resealExtentCatalogForTest(data)
				return data
			},
			want: "duplicates ordinal",
		},
		{
			name: "unaligned extent with valid crcs",
			mutate: func(data []byte) []byte {
				offset := extentCatalogHeaderSize + 16
				binary.LittleEndian.PutUint64(data[offset:offset+8], 1)
				resealExtentCatalogForTest(data)
				return data
			},
			want: "unaligned",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := test.mutate(append([]byte(nil), valid...))
			path := filepath.Join(root, test.name+".cat")
			require.NoError(t, os.WriteFile(path, data, 0o600))
			_, err := OpenPersistentExtentCatalog(path)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func resealExtentCatalogForTest(data []byte) {
	body := data[extentCatalogHeaderSize:]
	binary.LittleEndian.PutUint32(data[48:52], crc32.Checksum(body, extentCatalogCRC))
	binary.LittleEndian.PutUint32(data[92:96], crc32.Checksum(data[:92], extentCatalogCRC))
}
