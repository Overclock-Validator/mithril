package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sparseCountingReaderAt struct {
	size      int64
	headers   map[int64][hdrLen]byte
	readCalls int
	readBytes int
}

func (r *sparseCountingReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	r.readCalls++
	r.readBytes += len(dst)
	if offset < 0 || offset >= r.size {
		return 0, io.EOF
	}
	clear(dst)
	if header, ok := r.headers[offset]; ok {
		copy(dst, header[:])
	}
	available := r.size - offset
	if int64(len(dst)) > available {
		return int(available), io.EOF
	}
	return len(dst), nil
}

func appendVecTestHeader(key solana.PublicKey, lamports, dataLen uint64) [hdrLen]byte {
	var header [hdrLen]byte
	binary.LittleEndian.PutUint64(header[dataLenOffset:dataLenOffset+8], dataLen)
	copy(header[pubkeyOffset:pubkeyOffset+32], key[:])
	binary.LittleEndian.PutUint64(header[lamportsOffset:lamportsOffset+8], lamports)
	return header
}

func TestScanAppendVecRecordsSkipsBodiesInHugeSparseFile(t *testing.T) {
	dataLen := uint64(maxAppendVecAccountDataLen)
	recordEnd := int64(hdrLen) + int64((dataLen+7)&^uint64(7))
	reader := &sparseCountingReaderAt{
		size: 1 << 40,
		headers: map[int64][hdrLen]byte{
			0: appendVecTestHeader(solana.PublicKey{1}, 1, dataLen),
			// recordEnd is deliberately absent: the synthetic hole reads as the
			// canonical zero terminator.
		},
	}

	var records []appendVecScanRecord
	err := scanAppendVecRecords(context.Background(), reader, reader.size, func(rec appendVecScanRecord) error {
		records = append(records, rec)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, uint64(0), records[0].Offset)
	assert.Equal(t, uint64(recordEnd), records[0].Span)
	assert.Equal(t, 2, reader.readCalls)
	assert.Equal(t, 2*hdrLen, reader.readBytes, "a 1TiB logical file must not cause account bodies to be read")
}

func TestScanAppendVecRecordsRejectsMalformedInput(t *testing.T) {
	t.Run("oversized account data", func(t *testing.T) {
		header := appendVecTestHeader(solana.PublicKey{1}, 1, maxAppendVecAccountDataLen+1)
		_, err := scanAppendVecBytes(header[:])
		require.ErrorContains(t, err, "exceeds maximum")
	})

	t.Run("truncated account data", func(t *testing.T) {
		header := appendVecTestHeader(solana.PublicKey{1}, 1, 8)
		data := append(header[:], make([]byte, 7)...)
		_, err := scanAppendVecBytes(data)
		require.ErrorContains(t, err, "truncated appendvec account data")
	})

	t.Run("nonzero partial header", func(t *testing.T) {
		header := appendVecTestHeader(solana.PublicKey{1}, 1, 0)
		data := append(header[:], 1)
		_, err := scanAppendVecBytes(data)
		require.ErrorContains(t, err, "truncated appendvec header")
	})

	t.Run("missing final alignment padding is valid", func(t *testing.T) {
		header := appendVecTestHeader(solana.PublicKey{1}, 1, 1)
		data := append(header[:], 0xab)
		records, err := scanAppendVecBytes(data)
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Equal(t, uint64(len(data)), records[0].Span)
	})

	t.Run("cancellation precedes IO", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		reader := &sparseCountingReaderAt{size: 1 << 40}
		err := scanAppendVecRecords(ctx, reader, reader.size, func(appendVecScanRecord) error { return nil })
		require.ErrorIs(t, err, context.Canceled)
		assert.Zero(t, reader.readCalls)
	})
}

func scanAppendVecBytes(data []byte) ([]appendVecScanRecord, error) {
	reader := &sliceReaderAt{data: data}
	var records []appendVecScanRecord
	err := scanAppendVecRecords(context.Background(), reader, int64(len(data)), func(rec appendVecScanRecord) error {
		records = append(records, rec)
		return nil
	})
	return records, err
}

type sliceReaderAt struct{ data []byte }

func (r *sliceReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	if offset < 0 || offset >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(dst, r.data[offset:])
	if n != len(dst) {
		return n, io.EOF
	}
	return n, nil
}

func TestCompactCrashOrderingKeepsAReadableCopy(t *testing.T) {
	for _, tc := range []struct {
		name  string
		arm   func(*foldTestHooks, func())
		moved bool
	}{
		{
			name: "manifest durable before index movement",
			arm:  func(h *foldTestHooks, crash func()) { h.afterManifestRename = crash },
		},
		{
			name:  "index durable before source unlink",
			arm:   func(h *foldTestHooks, crash func()) { h.afterIndexCommit = crash },
			moved: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dir := newFoldTestDb(t)
			t.Cleanup(func() { db.CloseDb() })

			big := make([]byte, 2048)
			source := commitTestBatch(t, db, 110,
				foldAcct(1, 100, big),
				foldAcct(2, 200, []byte("survivor")),
			)
			commitTestBatch(t, db, 120, foldAcct(1, 101, big))
			commitTestBatch(t, db, 130, foldAcct(1, 102, big))
			sourcePath := filepath.Join(db.AcctsDir, SegmentDataName(110, source.FileId))

			crashed := false
			tc.arm(&db.foldHooks, func() { panic("simulated process loss") })
			func() {
				defer func() { crashed = recover() != nil }()
				_, _ = db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5})
			}()
			db.foldHooks = foldTestHooks{}
			require.True(t, crashed)
			assert.FileExists(t, sourcePath, "the source must exist at every pre-unlink crash point")

			value, ok := db.Index.Lookup(solana.PublicKey{2})
			require.True(t, ok)
			if tc.moved {
				assert.NotEqual(t, source.FileId, value.Entry.FileId)
				assert.True(t, db.Index.IsRetired(110, source.FileId))
				assert.FileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(110, value.Entry.FileId)))
			} else {
				assert.Equal(t, source.FileId, value.Entry.FileId)
			}

			// Reopening replays the same durable side of the transition. Either
			// location remains readable because source unlink is strictly last.
			db = reopenFoldTestDb(t, db, dir)
			got := mustColdRead(t, db, 130, solana.PublicKey{2})
			assert.Equal(t, []byte("survivor"), got.Data)
		})
	}
}

func TestCompactRejectsSymlinkedOrReplacedSource(t *testing.T) {
	newCandidate := func(t *testing.T) (*AccountsDb, string, string) {
		t.Helper()
		db, _ := newFoldTestDb(t)
		big := make([]byte, 2048)
		source := commitTestBatch(t, db, 110, foldAcct(1, 100, big))
		commitTestBatch(t, db, 120, foldAcct(1, 120, big))
		commitTestBatch(t, db, 130, foldAcct(2, 130, []byte("unrelated head")))
		sourcePath := filepath.Join(db.AcctsDir, SegmentDataName(110, source.FileId))
		return db, sourcePath, segmentManifestPath(db.AcctsDir, 110, source.FileId)
	}

	t.Run("final-component symlink", func(t *testing.T) {
		db, sourcePath, manifestPath := newCandidate(t)
		defer db.CloseDb()
		original, err := os.ReadFile(sourcePath)
		require.NoError(t, err)
		external := filepath.Join(t.TempDir(), "external-appendvec")
		require.NoError(t, os.WriteFile(external, original, 0o644))
		saved := sourcePath + ".saved"
		require.NoError(t, os.Rename(sourcePath, saved))
		require.NoError(t, os.Symlink(external, sourcePath))

		_, err = db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5})
		require.ErrorContains(t, err, "not a regular file")
		assert.FileExists(t, saved)
		assert.FileExists(t, manifestPath)
		after, readErr := os.ReadFile(external)
		require.NoError(t, readErr)
		assert.Equal(t, original, after, "compaction must not read through or mutate a symlink target")
		linkInfo, statErr := os.Lstat(sourcePath)
		require.NoError(t, statErr)
		assert.NotZero(t, linkInfo.Mode()&os.ModeSymlink)
	})

	t.Run("replacement after source scan", func(t *testing.T) {
		db, sourcePath, manifestPath := newCandidate(t)
		defer db.CloseDb()
		externalBytes := []byte("external target must remain untouched")
		external := filepath.Join(t.TempDir(), "external-appendvec")
		require.NoError(t, os.WriteFile(external, externalBytes, 0o644))
		saved := sourcePath + ".saved"
		db.foldHooks.afterCompactionSourceScan = func() {
			require.NoError(t, os.Rename(sourcePath, saved))
			require.NoError(t, os.Symlink(external, sourcePath))
		}

		_, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5})
		db.foldHooks.afterCompactionSourceScan = nil
		require.ErrorContains(t, err, "changed while reading")
		assert.FileExists(t, saved)
		assert.FileExists(t, manifestPath)
		after, readErr := os.ReadFile(external)
		require.NoError(t, readErr)
		assert.Equal(t, externalBytes, after)
		linkInfo, statErr := os.Lstat(sourcePath)
		require.NoError(t, statErr)
		assert.NotZero(t, linkInfo.Mode()&os.ModeSymlink)
	})
}

func TestCompactRetiresPostBootstrapSourceWithoutFileIDHeuristic(t *testing.T) {
	db, _ := newFoldTestDb(t) // bootstrap high-water is zero
	defer db.CloseDb()

	big := make([]byte, 2048)
	survivor := foldAcct(2, 200, []byte("live"))
	source := commitTestBatch(t, db, 110,
		foldAcct(1, 100, big),
		survivor,
	)
	commitTestBatch(t, db, 120, foldAcct(1, 101, big))
	commitTestBatch(t, db, 130, foldAcct(1, 102, big))
	bootstrapHigh, err := db.bootstrapHighFileId()
	require.NoError(t, err)
	require.Greater(t, source.FileId, bootstrapHigh)

	stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesCompacted)
	assert.True(t, db.Index.IsRetired(110, source.FileId), "retirement must not assume immutable bases contain only bootstrap file IDs")
	assert.NoFileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(110, source.FileId)))

	entry, ok := db.Index.Lookup(solana.PublicKey{2})
	require.True(t, ok)
	manifest, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, 110, entry.Entry.FileId))
	require.NoError(t, err)
	assert.Empty(t, manifest.Records, "compaction relocation metadata belongs in the WAL, not an O(records) in-memory manifest")
	data, err := os.ReadFile(filepath.Join(db.AcctsDir, SegmentDataName(110, entry.Entry.FileId)))
	require.NoError(t, err)
	assert.Equal(t, manifest.DataCRC, crc32Checksum(data))
	var expected bytes.Buffer
	_, err = (&AppendVecAccount{
		DataLen:    uint64(len(survivor.Data)),
		Pubkey:     survivor.Key,
		Lamports:   survivor.Lamports,
		RentEpoch:  survivor.RentEpoch,
		Owner:      survivor.Owner,
		Executable: survivor.Executable,
		Data:       survivor.Data,
	}).MarshalReturningLength(&expected)
	require.NoError(t, err)
	assert.Equal(t, expected.Bytes(), data, "compaction must raw-copy the complete live record exactly")
}

func crc32Checksum(data []byte) uint32 {
	// Keep the integration assertion readable without exposing implementation
	// internals from compact.go.
	return crc32.ChecksumIEEE(data)
}
