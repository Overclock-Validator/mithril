package accountsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stakeIndexTestEntry(seed byte, fileID, offset uint64) StakeIndexEntry {
	var pubkey solana.PublicKey
	for index := range pubkey {
		pubkey[index] = seed + byte(index)
	}
	return StakeIndexEntry{Pubkey: pubkey, FileId: fileID, Offset: offset}
}

func TestStakePubkeyIndexV3RoundTripAndAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stake_pubkeys.idx")
	initial := []StakeIndexEntry{
		stakeIndexTestEntry(1, 10, 100),
		stakeIndexTestEntry(2, 20, 200),
	}
	require.NoError(t, WriteStakePubkeyIndex(path, initial))

	entries, format, err := ReadStakePubkeyIndex(path)
	require.NoError(t, err)
	assert.Equal(t, StakeIndexVersion, format.Version)
	assert.Equal(t, uint64(len(initial)), format.RecordCount)
	assert.False(t, format.IncompleteTail)
	assert.Equal(t, initial, entries)

	appended := []StakeIndexEntry{stakeIndexTestEntry(3, 30, 300)}
	require.NoError(t, AppendStakePubkeyIndex(path, appended))
	entries, format, err = ReadStakePubkeyIndex(path)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), format.RecordCount)
	assert.Equal(t, append(initial, appended...), entries)
	_, err = ValidateStakePubkeyIndex(path)
	require.NoError(t, err)
}

func TestStakePubkeyIndexWriterSplitsBoundedFrames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stake_pubkeys.idx")
	entries := make([]StakeIndexEntry, stakeIndexMaxFrameRecords+1)
	for index := range entries {
		entries[index] = stakeIndexTestEntry(byte(index), uint64(index), uint64(index*8))
	}
	require.NoError(t, WriteStakePubkeyIndex(path, entries))

	decoded, format, err := ReadStakePubkeyIndex(path)
	require.NoError(t, err)
	assert.Equal(t, entries, decoded)
	assert.Equal(t, uint64(len(entries)), format.RecordCount)
	wantSize := int64(stakeIndexFileHeaderSize + 2*(stakeIndexFrameHeaderSize+stakeIndexFrameTrailerSize) + len(entries)*StakeIndexRecordSize)
	assert.Equal(t, wantSize, format.FileBytes)
}

func TestStakePubkeyIndexTornTailIsIgnoredAndRepaired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stake_pubkeys.idx")
	initial := []StakeIndexEntry{stakeIndexTestEntry(1, 10, 100)}
	require.NoError(t, WriteStakePubkeyIndex(path, initial))
	before, err := os.Stat(path)
	require.NoError(t, err)

	var frame bytes.Buffer
	require.NoError(t, writeStakeIndexFrame(&frame, []StakeIndexEntry{stakeIndexTestEntry(2, 20, 200)}))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = file.Write(frame.Bytes()[:stakeIndexFrameHeaderSize+10])
	require.NoError(t, err)
	require.NoError(t, file.Close())

	decoded, format, err := ReadStakePubkeyIndex(path)
	require.NoError(t, err)
	assert.Equal(t, initial, decoded)
	assert.True(t, format.IncompleteTail)
	assert.Equal(t, before.Size(), format.ValidBytes)

	appended := stakeIndexTestEntry(3, 30, 300)
	require.NoError(t, AppendStakePubkeyIndex(path, []StakeIndexEntry{appended}))
	decoded, format, err = ReadStakePubkeyIndex(path)
	require.NoError(t, err)
	assert.False(t, format.IncompleteTail)
	assert.Equal(t, append(initial, appended), decoded)
}

func TestStakePubkeyIndexAcceptsEveryTornFramePrefix(t *testing.T) {
	directory := t.TempDir()
	basePath := filepath.Join(directory, "base.idx")
	initial := []StakeIndexEntry{stakeIndexTestEntry(1, 10, 100)}
	require.NoError(t, WriteStakePubkeyIndex(basePath, initial))
	base, err := os.ReadFile(basePath)
	require.NoError(t, err)

	var frame bytes.Buffer
	require.NoError(t, writeStakeIndexFrame(&frame, []StakeIndexEntry{stakeIndexTestEntry(2, 20, 200)}))
	for prefix := 1; prefix < frame.Len(); prefix++ {
		path := filepath.Join(directory, "torn.idx")
		contents := append(append([]byte(nil), base...), frame.Bytes()[:prefix]...)
		require.NoError(t, os.WriteFile(path, contents, 0o644), "prefix=%d", prefix)
		decoded, format, readErr := ReadStakePubkeyIndex(path)
		require.NoError(t, readErr, "prefix=%d", prefix)
		assert.True(t, format.IncompleteTail, "prefix=%d", prefix)
		assert.Equal(t, initial, decoded, "prefix=%d", prefix)
	}
}

func TestStakePubkeyIndexRejectsChecksumAndCountCorruption(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func([]byte)
		match  string
	}{
		{
			name: "record checksum",
			mutate: func(data []byte) {
				data[stakeIndexFileHeaderSize+stakeIndexFrameHeaderSize] ^= 0xff
			},
			match: "checksum mismatch",
		},
		{
			name: "frame count",
			mutate: func(data []byte) {
				data[stakeIndexFileHeaderSize+8] ^= 0x01
			},
			match: "header checksum mismatch",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "stake_pubkeys.idx")
			require.NoError(t, WriteStakePubkeyIndex(path, []StakeIndexEntry{stakeIndexTestEntry(1, 1, 1)}))
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			test.mutate(data)
			require.NoError(t, os.WriteFile(path, data, 0o644))

			_, _, err = ReadStakePubkeyIndex(path)
			require.ErrorContains(t, err, test.match)
			_, err = ValidateStakePubkeyIndex(path)
			require.ErrorContains(t, err, test.match)
		})
	}
}

func TestStakePubkeyIndexReadsAndUpgradesLegacyFormats(t *testing.T) {
	entry := stakeIndexTestEntry(7, 70, 700)
	for _, test := range []struct {
		name    string
		version uint32
		encode  func(StakeIndexEntry) []byte
	}{
		{
			name:    "v1",
			version: 1,
			encode: func(entry StakeIndexEntry) []byte {
				return append([]byte(nil), entry.Pubkey[:]...)
			},
		},
		{
			name:    "v2",
			version: StakeIndexLegacyVersion,
			encode: func(entry StakeIndexEntry) []byte {
				data := make([]byte, 8+StakeIndexRecordSize)
				copy(data[:4], StakeIndexMagic[:])
				binary.LittleEndian.PutUint32(data[4:8], StakeIndexLegacyVersion)
				copy(data[8:40], entry.Pubkey[:])
				binary.LittleEndian.PutUint64(data[40:48], entry.FileId)
				binary.LittleEndian.PutUint64(data[48:56], entry.Offset)
				return data
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "stake_pubkeys.idx")
			require.NoError(t, os.WriteFile(path, test.encode(entry), 0o644))

			decoded, format, err := ReadStakePubkeyIndex(path)
			require.NoError(t, err)
			assert.Equal(t, test.version, format.Version)
			require.Len(t, decoded, 1)
			assert.Equal(t, entry.Pubkey, decoded[0].Pubkey)
			if test.version == StakeIndexLegacyVersion {
				assert.Equal(t, entry.FileId, decoded[0].FileId)
			} else {
				assert.Zero(t, decoded[0].FileId)
			}

			newEntry := stakeIndexTestEntry(8, 80, 800)
			require.NoError(t, AppendStakePubkeyIndex(path, []StakeIndexEntry{newEntry}))
			decoded, format, err = ReadStakePubkeyIndex(path)
			require.NoError(t, err)
			assert.Equal(t, StakeIndexVersion, format.Version)
			assert.Len(t, decoded, 2)
			assert.Equal(t, newEntry, decoded[1])
		})
	}
}

func TestStakePubkeyIndexAtomicPublicationFailures(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "stake_pubkeys.idx")
	old := []StakeIndexEntry{stakeIndexTestEntry(1, 1, 1)}
	newEntries := []StakeIndexEntry{stakeIndexTestEntry(2, 2, 2)}
	require.NoError(t, WriteStakePubkeyIndex(path, old))

	t.Run("pre-rename leaves old file", func(t *testing.T) {
		injected := errors.New("rename failed")
		ops := defaultStakeIndexAtomicWriteOps
		ops.rename = func(string, string) error { return injected }
		err := writeStakePubkeyIndexAtomic(path, newEntries, ops)
		require.ErrorIs(t, err, injected)
		assert.False(t, errors.Is(err, ErrStakeIndexCommitDecided))
		decoded, _, readErr := ReadStakePubkeyIndex(path)
		require.NoError(t, readErr)
		assert.Equal(t, old, decoded)
		matches, globErr := filepath.Glob(filepath.Join(directory, ".stake_pubkeys.idx.tmp-*"))
		require.NoError(t, globErr)
		assert.Empty(t, matches)
	})

	t.Run("post-rename is commit-decided and retained", func(t *testing.T) {
		injected := errors.New("directory sync failed")
		ops := defaultStakeIndexAtomicWriteOps
		ops.syncDirectory = func(string) error { return injected }
		err := writeStakePubkeyIndexAtomic(path, newEntries, ops)
		require.ErrorIs(t, err, injected)
		require.ErrorIs(t, err, ErrStakeIndexCommitDecided)
		decoded, _, readErr := ReadStakePubkeyIndex(path)
		require.NoError(t, readErr)
		assert.Equal(t, newEntries, decoded)
		matches, globErr := filepath.Glob(filepath.Join(directory, ".stake_pubkeys.idx.tmp-*"))
		require.NoError(t, globErr)
		assert.Empty(t, matches)
	})
}

func TestStakePubkeyIndexRejectsSymlinks(t *testing.T) {
	directory := t.TempDir()
	referent := filepath.Join(directory, "referent")
	require.NoError(t, os.WriteFile(referent, []byte("do not replace"), 0o644))
	path := filepath.Join(directory, "stake_pubkeys.idx")
	require.NoError(t, os.Symlink(referent, path))

	err := WriteStakePubkeyIndex(path, []StakeIndexEntry{stakeIndexTestEntry(1, 1, 1)})
	require.ErrorContains(t, err, "not a regular file")
	err = AppendStakePubkeyIndex(path, []StakeIndexEntry{stakeIndexTestEntry(2, 2, 2)})
	require.ErrorContains(t, err, "not a regular file")
	contents, readErr := os.ReadFile(referent)
	require.NoError(t, readErr)
	assert.Equal(t, []byte("do not replace"), contents)

	realDirectory := t.TempDir()
	linkedDirectory := filepath.Join(t.TempDir(), "linked-db")
	require.NoError(t, os.Symlink(realDirectory, linkedDirectory))
	err = WriteStakePubkeyIndex(
		filepath.Join(linkedDirectory, "stake_pubkeys.idx"),
		[]StakeIndexEntry{stakeIndexTestEntry(3, 3, 3)},
	)
	require.ErrorContains(t, err, "not a real directory")
	_, statErr := os.Stat(filepath.Join(realDirectory, "stake_pubkeys.idx"))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}
