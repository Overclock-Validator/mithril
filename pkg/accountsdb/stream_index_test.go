package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type streamIndexTestRecord struct {
	key   solana.PublicKey
	entry AccountIndexEntry
}

type streamIndexTestSource struct {
	records []streamIndexTestRecord
	scans   int
}

func (source *streamIndexTestSource) Scan(
	ctx context.Context,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) error {
	source.scans++
	for _, record := range source.records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(record.key, record.entry); err != nil {
			return err
		}
	}
	return nil
}

func TestStreamAccountIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	keys := []solana.PublicKey{
		streamIndexTestKey(0x10, 1),
		streamIndexTestKey(0x20, 2),
		streamIndexTestKey(0x30, 3),
	}
	entries := []AccountIndexEntry{
		{Slot: 500, FileId: 1<<40 + 7, Offset: 64},
		{Slot: 500, FileId: 1<<40 + 7, Offset: streamIndexExtentSize + 40},
		{Slot: 700, FileId: ^uint64(0) - 9, Offset: 3*streamIndexExtentSize + 120},
	}
	source := streamIndexTestSource{records: streamIndexTestRecords(keys, entries)}

	output := filepath.Join(dir, StreamIndexFileName)
	stats, err := BuildStreamAccountIndex(context.Background(), &source, output, 2)
	require.NoError(t, err)
	assert.Equal(t, StreamIndexBuildStats{InputKeys: 3, BaseKeys: 3, Extents: 3}, stats)
	assert.Equal(t, 2, source.scans, "one sizing scan plus one build scan")
	assert.NoFileExists(t, output+".partial")
	require.NoError(t, ValidateStreamIndexArtifacts(dir))

	identity, err := readStreamIndexManifest(dir)
	require.NoError(t, err)
	wantIdentity, err := computeStreamIndexFileIdentity(output)
	require.NoError(t, err)
	assert.Equal(t, wantIdentity, identity)

	index, err := OpenStreamAccountIndex(output)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	require.Equal(t, uint64(len(keys)), index.NumKeys())
	for i, key := range keys {
		got, found, err := index.LookupCandidate(key)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, entries[i], got)
	}

	// Fingerprints are probabilistic, so find a deterministic rejected
	// non-member instead of assuming one particular key cannot false-positive.
	rejected := false
	for i := uint64(1); i < 1000; i++ {
		var missing solana.PublicKey
		binary.LittleEndian.PutUint64(missing[0:8], i*0x9e3779b97f4a7c15)
		binary.LittleEndian.PutUint64(missing[8:16], i*0xd6e8feb86659fd93)
		_, found, err := index.LookupCandidate(missing)
		require.NoError(t, err)
		if !found {
			rejected = true
			break
		}
	}
	assert.True(t, rejected, "expected at least one fingerprint-rejected non-member")
}

func TestStreamAccountIndexHashesCompletePubkeysSharingPrefix(t *testing.T) {
	dir := t.TempDir()
	first := streamIndexTestKey(0x42, 1)
	second := first
	second[31] = 2
	firstEntry := AccountIndexEntry{Slot: 10, FileId: 100, Offset: 8}
	secondEntry := AccountIndexEntry{Slot: 11, FileId: 101, Offset: streamIndexExtentSize + 16}
	source := streamIndexTestSource{records: []streamIndexTestRecord{
		{key: first, entry: firstEntry},
		{key: second, entry: secondEntry},
	}}

	output := filepath.Join(dir, StreamIndexFileName)
	stats, err := BuildStreamAccountIndex(context.Background(), &source, output, 1)
	require.NoError(t, err)
	assert.Equal(t, StreamIndexBuildStats{InputKeys: 2, BaseKeys: 2, Extents: 2}, stats)

	index, err := OpenStreamAccountIndex(output)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	firstHash := streamIndexHashKey(first, index.hashSeed)
	secondHash := streamIndexHashKey(second, index.hashSeed)
	assert.NotEqual(t, firstHash, secondHash)
	for key, want := range map[solana.PublicKey]AccountIndexEntry{first: firstEntry, second: secondEntry} {
		got, found, err := index.LookupCandidate(key)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, want, got)
	}
}

func TestBuildStreamAccountIndexRetriesPrehashCollision(t *testing.T) {
	dir := t.TempDir()
	first := streamIndexTestKey(0x51, 1)
	second := streamIndexTestKey(0x52, 2)
	firstEntry := AccountIndexEntry{Slot: 50, FileId: 5, Offset: 8}
	secondEntry := AccountIndexEntry{Slot: 50, FileId: 5, Offset: 16}
	source := streamIndexTestSource{records: []streamIndexTestRecord{
		{key: first, entry: firstEntry},
		{key: second, entry: secondEntry},
	}}

	oldSeedSource, oldHash := streamIndexSeedSource, streamIndexHash
	defer func() {
		streamIndexSeedSource = oldSeedSource
		streamIndexHash = oldHash
	}()
	var seedCalls int
	streamIndexSeedSource = func() (uint64, error) {
		seedCalls++
		return uint64(seedCalls), nil
	}
	streamIndexHash = func(key solana.PublicKey, seed uint64) (hash [16]byte) {
		if seed == 1 {
			return hash
		}
		return streamIndexHashKey(key, seed)
	}

	output := filepath.Join(dir, StreamIndexFileName)
	stats, err := BuildStreamAccountIndex(context.Background(), &source, output, 2)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), stats.HashSeedRetries)
	assert.Equal(t, 2, seedCalls)
	assert.Equal(t, 3, source.scans, "collision retry must rescan the repeatable source")

	index, err := OpenStreamAccountIndex(output)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	assert.Equal(t, uint64(2), index.hashSeed)
	got, found, err := index.LookupCandidate(first)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, firstEntry, got)
	got, found, err = index.LookupCandidate(second)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, secondEntry, got)
}

func TestBuildStreamAccountIndexRejectsMalformedSourceAndCleansTemps(t *testing.T) {
	tests := []struct {
		name    string
		records []streamIndexTestRecord
		wantErr string
	}{
		{
			name: "unaligned offset",
			records: []streamIndexTestRecord{{
				key: streamIndexTestKey(1, 1), entry: AccountIndexEntry{Slot: 1, FileId: 2, Offset: 7},
			}},
			wantErr: "not 8-byte aligned",
		},
		{
			name: "duplicate key",
			records: []streamIndexTestRecord{
				{key: streamIndexTestKey(1, 1), entry: AccountIndexEntry{Offset: 8}},
				{key: streamIndexTestKey(1, 1), entry: AccountIndexEntry{Offset: 16}},
			},
			wantErr: "duplicate",
		},
		{
			name: "out of order",
			records: []streamIndexTestRecord{
				{key: streamIndexTestKey(2, 1), entry: AccountIndexEntry{Offset: 8}},
				{key: streamIndexTestKey(1, 1), entry: AccountIndexEntry{Offset: 16}},
			},
			wantErr: "out-of-order",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			source := streamIndexTestSource{records: test.records}
			output := filepath.Join(dir, StreamIndexFileName)
			_, err := BuildStreamAccountIndex(context.Background(), &source, output, 1)
			require.ErrorContains(t, err, test.wantErr)
			assert.NoFileExists(t, output)
			assert.NoFileExists(t, output+".partial")
			assert.NoFileExists(t, filepath.Join(dir, StreamIndexManifestFileName))
		})
	}
}

func TestStreamIndexRunSourceRepeatedScanAndValidation(t *testing.T) {
	t.Run("repeated disjoint runs", func(t *testing.T) {
		dir := t.TempDir()
		first := streamIndexTestRecord{streamIndexTestKey(0x10, 1), AccountIndexEntry{Slot: 1, Offset: 8}}
		second := streamIndexTestRecord{streamIndexTestKey(0x80, 2), AccountIndexEntry{Slot: 2, Offset: 16}}
		writeStreamIndexTestRun(t, filepath.Join(dir, "000.run"), []streamIndexTestRecord{first})
		writeStreamIndexTestRun(t, filepath.Join(dir, "001.run"), nil)
		writeStreamIndexTestRun(t, filepath.Join(dir, "002.run"), []streamIndexTestRecord{second})
		source, err := OpenStreamIndexRunSource(dir)
		require.NoError(t, err)
		for range 2 {
			var got []streamIndexTestRecord
			require.NoError(t, source.Scan(context.Background(), func(key solana.PublicKey, entry AccountIndexEntry) error {
				got = append(got, streamIndexTestRecord{key, entry})
				return nil
			}))
			assert.Equal(t, []streamIndexTestRecord{first, second}, got)
		}
	})

	t.Run("torn record", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "000.run"), make([]byte, StreamIndexRunRecordSize-1), 0o644))
		_, err := OpenStreamIndexRunSource(dir)
		require.ErrorContains(t, err, "not a multiple")
	})

	for _, test := range []struct {
		name    string
		records [][]streamIndexTestRecord
		wantErr string
	}{
		{
			name: "duplicate within run",
			records: [][]streamIndexTestRecord{{
				{key: streamIndexTestKey(1, 1)}, {key: streamIndexTestKey(1, 1)},
			}},
			wantErr: "duplicate",
		},
		{
			name: "duplicate across runs",
			records: [][]streamIndexTestRecord{
				{{key: streamIndexTestKey(1, 1)}}, {{key: streamIndexTestKey(1, 1)}},
			},
			wantErr: "duplicate",
		},
		{
			name: "overlapping shard ranges",
			records: [][]streamIndexTestRecord{
				{{key: streamIndexTestKey(2, 1)}}, {{key: streamIndexTestKey(1, 1)}},
			},
			wantErr: "out-of-order",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			for i, records := range test.records {
				writeStreamIndexTestRun(t, filepath.Join(dir, string(rune('0'+i))+".run"), records)
			}
			source, err := OpenStreamIndexRunSource(dir)
			require.NoError(t, err)
			err = source.Scan(context.Background(), func(solana.PublicKey, AccountIndexEntry) error { return nil })
			require.ErrorContains(t, err, test.wantErr)
		})
	}

	t.Run("cancelled", func(t *testing.T) {
		dir := t.TempDir()
		writeStreamIndexTestRun(t, filepath.Join(dir, "000.run"), []streamIndexTestRecord{{key: streamIndexTestKey(1, 1)}})
		source, err := OpenStreamIndexRunSource(dir)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = source.Scan(ctx, func(solana.PublicKey, AccountIndexEntry) error { return nil })
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestStreamIndexRunSourceDetectsMutationBetweenScans(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000.run")
	writeStreamIndexTestRun(t, path, []streamIndexTestRecord{{key: streamIndexTestKey(1, 1)}})
	source, err := OpenStreamIndexRunSource(dir)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte("changed"), 0o644))
	err = source.Scan(context.Background(), func(solana.PublicKey, AccountIndexEntry) error { return nil })
	require.ErrorContains(t, err, "changed or is malformed")
}

func TestStreamIndexRunSourceRejectsSymlinksAndSameSizeReplacement(t *testing.T) {
	t.Run("symlinked run directory", func(t *testing.T) {
		target := t.TempDir()
		writeStreamIndexTestRun(t, filepath.Join(target, "000.run"), nil)
		linked := filepath.Join(t.TempDir(), "runs")
		require.NoError(t, os.Symlink(target, linked))
		_, err := OpenStreamIndexRunSource(linked)
		require.ErrorContains(t, err, "not a real directory")
	})

	t.Run("symlinked run", func(t *testing.T) {
		dir := t.TempDir()
		external := filepath.Join(t.TempDir(), "outside.run")
		writeStreamIndexTestRun(t, external, []streamIndexTestRecord{{key: streamIndexTestKey(1, 1)}})
		require.NoError(t, os.Symlink(external, filepath.Join(dir, "000.run")))
		_, err := OpenStreamIndexRunSource(dir)
		require.ErrorContains(t, err, "not a regular file")
	})

	t.Run("same-size replacement", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "000.run")
		writeStreamIndexTestRun(t, path, []streamIndexTestRecord{{key: streamIndexTestKey(1, 1)}})
		source, err := OpenStreamIndexRunSource(dir)
		require.NoError(t, err)
		replacement := filepath.Join(dir, "replacement")
		writeStreamIndexTestRun(t, replacement, []streamIndexTestRecord{{key: streamIndexTestKey(2, 2)}})
		require.NoError(t, os.Rename(replacement, path))
		err = source.Scan(context.Background(), func(solana.PublicKey, AccountIndexEntry) error { return nil })
		require.ErrorContains(t, err, "changed or is malformed")
	})
}

func TestStreamIndexMetadataValidation(t *testing.T) {
	want := []streamIndexExtent{{Slot: 9, FileID: 10, BaseOffset: 2 * streamIndexExtentSize}}
	const wantSeed = uint64(0x123456789abcdef0)
	encoded, err := encodeStreamIndexMetadata(want, wantSeed)
	require.NoError(t, err)
	got, gotSeed, err := decodeStreamIndexMetadata(encoded)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, wantSeed, gotSeed)

	badMagic := bytes.Clone(encoded)
	badMagic[0] ^= 0xff
	_, _, err = decodeStreamIndexMetadata(badMagic)
	require.ErrorContains(t, err, "magic")

	_, _, err = decodeStreamIndexMetadata(encoded[:len(encoded)-1])
	require.ErrorContains(t, err, "want")

	unaligned := bytes.Clone(encoded)
	binary.LittleEndian.PutUint64(unaligned[40:48], 3)
	_, _, err = decodeStreamIndexMetadata(unaligned)
	require.ErrorContains(t, err, "unaligned")
}

func TestBuildStreamAccountIndexRejectsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source := streamIndexTestSource{}
	_, err := BuildStreamAccountIndex(ctx, &source, filepath.Join(t.TempDir(), StreamIndexFileName), 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestStreamAccountIndexNilAndClosedBehavior(t *testing.T) {
	var index *StreamAccountIndex
	entry, found, err := index.LookupCandidate(solana.PublicKey{})
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, AccountIndexEntry{}, entry)
	require.NoError(t, index.Close())
	assert.Zero(t, index.NumKeys())
}

func streamIndexTestRecords(keys []solana.PublicKey, entries []AccountIndexEntry) []streamIndexTestRecord {
	records := make([]streamIndexTestRecord, len(keys))
	for i := range keys {
		records[i] = streamIndexTestRecord{key: keys[i], entry: entries[i]}
	}
	return records
}

func streamIndexTestKey(prefix, tail byte) solana.PublicKey {
	var key solana.PublicKey
	key[0] = prefix
	key[7] = prefix ^ 0x5a
	key[15] = prefix ^ 0xa5
	key[31] = tail
	return key
}

func writeStreamIndexTestRun(t *testing.T, path string, records []streamIndexTestRecord) {
	t.Helper()
	var data bytes.Buffer
	for _, record := range records {
		_, err := data.Write(record.key[:])
		require.NoError(t, err)
		var encoded [24]byte
		record.entry.Marshal(&encoded)
		_, err = data.Write(encoded[:])
		require.NoError(t, err)
	}
	require.NoError(t, os.WriteFile(path, data.Bytes(), 0o644))
}

var _ StreamIndexSource = (*streamIndexTestSource)(nil)
