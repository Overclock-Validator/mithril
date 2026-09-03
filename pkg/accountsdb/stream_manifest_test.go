package accountsdb

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamIndexManifestRoundTripAndArtifactValidation(t *testing.T) {
	dir := t.TempDir()
	baseData := []byte("complete immutable StreamHash generation")
	identity := writeStreamManifestTestBase(t, dir, baseData)

	require.NoError(t, publishStreamIndexManifest(dir, identity))
	got, err := readStreamIndexManifest(dir)
	require.NoError(t, err)
	assert.Equal(t, identity, got)
	require.NoError(t, validateStreamIndexManifestArtifacts(dir))

	manifestPath := filepath.Join(dir, StreamIndexManifestFileName)
	info, err := os.Stat(manifestPath)
	require.NoError(t, err)
	assert.Equal(t, int64(streamIndexManifestSize), info.Size())
	assert.NoFileExists(t, manifestPath+".tmp")
}

func TestStreamIndexManifestMissingAndOneSidedArtifacts(t *testing.T) {
	t.Run("both absent", func(t *testing.T) {
		dir := t.TempDir()
		_, err := readStreamIndexManifest(dir)
		assert.ErrorIs(t, err, os.ErrNotExist)
		require.NoError(t, validateStreamIndexManifestArtifacts(dir))
	})

	t.Run("base only", func(t *testing.T) {
		dir := t.TempDir()
		writeStreamManifestTestBase(t, dir, []byte("base without manifest"))
		err := validateStreamIndexManifestArtifacts(dir)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exists without")
	})

	t.Run("manifest only", func(t *testing.T) {
		dir := t.TempDir()
		identity := streamManifestTestIdentity([]byte("missing base"))
		require.NoError(t, publishStreamIndexManifest(dir, identity))
		err := validateStreamIndexManifestArtifacts(dir)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exists without")
	})

	t.Run("non-regular base", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(dir, StreamIndexFileName), 0o755))
		err := validateStreamIndexManifestArtifacts(dir)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a regular file")
	})
}

func TestStreamIndexManifestRejectsMismatchedBase(t *testing.T) {
	t.Run("size", func(t *testing.T) {
		dir := t.TempDir()
		identity := writeStreamManifestTestBase(t, dir, []byte("first base"))
		require.NoError(t, publishStreamIndexManifest(dir, identity))
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, StreamIndexFileName), []byte("different-sized base"), 0o644,
		))

		err := validateStreamIndexManifestArtifacts(dir)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "size mismatch")
	})

	t.Run("sha", func(t *testing.T) {
		dir := t.TempDir()
		identity := writeStreamManifestTestBase(t, dir, []byte("base bytes A"))
		require.NoError(t, publishStreamIndexManifest(dir, identity))
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, StreamIndexFileName), []byte("base bytes B"), 0o644,
		))

		err := validateStreamIndexManifestArtifacts(dir)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SHA-256 mismatch")
	})
}

func TestStreamIndexManifestRejectsCorruptAndTornRecords(t *testing.T) {
	identity := streamManifestTestIdentity([]byte("manifest identity"))
	encoded, err := encodeStreamIndexManifest(identity)
	require.NoError(t, err)

	t.Run("crc", func(t *testing.T) {
		corrupt := encoded
		corrupt[31] ^= 0x80
		_, err := decodeStreamIndexManifest(corrupt[:])
		assert.ErrorIs(t, err, ErrInvalidStreamIndexManifest)
		assert.Contains(t, err.Error(), "CRC mismatch")
	})

	for _, length := range []int{0, 1, streamIndexManifestSize - 1, streamIndexManifestSize + 1} {
		t.Run(fmt.Sprintf("length_%d", length), func(t *testing.T) {
			data := make([]byte, length)
			copy(data, encoded[:])
			_, err := decodeStreamIndexManifest(data)
			assert.ErrorIs(t, err, ErrInvalidStreamIndexManifest)
		})
	}

	t.Run("version", func(t *testing.T) {
		bad := encoded
		binary.LittleEndian.PutUint32(bad[8:12], streamIndexManifestVersion+1)
		rewriteStreamManifestTestCRC(&bad)
		_, err := decodeStreamIndexManifest(bad[:])
		assert.ErrorIs(t, err, ErrInvalidStreamIndexManifest)
		assert.Contains(t, err.Error(), "unsupported version")
	})

	t.Run("encoded size", func(t *testing.T) {
		bad := encoded
		binary.LittleEndian.PutUint32(bad[12:16], streamIndexManifestSize-1)
		rewriteStreamManifestTestCRC(&bad)
		_, err := decodeStreamIndexManifest(bad[:])
		assert.ErrorIs(t, err, ErrInvalidStreamIndexManifest)
		assert.Contains(t, err.Error(), "encoded size")
	})

	t.Run("reserved", func(t *testing.T) {
		bad := encoded
		bad[56] = 1
		rewriteStreamManifestTestCRC(&bad)
		_, err := decodeStreamIndexManifest(bad[:])
		assert.ErrorIs(t, err, ErrInvalidStreamIndexManifest)
		assert.Contains(t, err.Error(), "reserved")
	})

	t.Run("zero identity size", func(t *testing.T) {
		bad := encoded
		binary.LittleEndian.PutUint64(bad[16:24], 0)
		rewriteStreamManifestTestCRC(&bad)
		_, err := decodeStreamIndexManifest(bad[:])
		assert.ErrorIs(t, err, ErrInvalidStreamIndexManifest)
		assert.Contains(t, err.Error(), "zero StreamHash file size")
	})
}

func TestStreamIndexManifestAtomicReplacement(t *testing.T) {
	dir := t.TempDir()
	first := streamManifestTestIdentity([]byte("first immutable generation"))
	second := streamManifestTestIdentity([]byte("second immutable generation"))
	require.NoError(t, publishStreamIndexManifest(dir, first))

	// A stale interrupted-publication file must never obstruct replacement.
	tmpPath := filepath.Join(dir, StreamIndexManifestFileName) + ".tmp"
	require.NoError(t, os.WriteFile(tmpPath, []byte("torn"), 0o644))

	const readers = 4
	stop := make(chan struct{})
	started := make(chan struct{}, readers)
	errCh := make(chan error, readers)
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := readStreamIndexManifest(dir)
				if err != nil {
					errCh <- err
					return
				}
				if got != first && got != second {
					errCh <- fmt.Errorf("reader observed unpublished identity: %+v", got)
					return
				}
			}
		}()
	}
	for range readers {
		<-started
	}

	for i := 0; i < 20; i++ {
		identity := first
		if i%2 == 0 {
			identity = second
		}
		require.NoError(t, publishStreamIndexManifest(dir, identity))
	}
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	got, err := readStreamIndexManifest(dir)
	require.NoError(t, err)
	assert.Equal(t, first, got)
	assert.NoFileExists(t, tmpPath)
}

func TestPublishStreamIndexManifestRejectsInvalidInput(t *testing.T) {
	identity := streamManifestTestIdentity([]byte("valid"))
	assert.Error(t, publishStreamIndexManifest("", identity))
	assert.ErrorIs(
		t,
		publishStreamIndexManifest(t.TempDir(), streamIndexFileIdentity{}),
		ErrInvalidStreamIndexManifest,
	)
}

func streamManifestTestIdentity(data []byte) streamIndexFileIdentity {
	return streamIndexFileIdentity{Size: uint64(len(data)), SHA256: sha256.Sum256(data)}
}

func writeStreamManifestTestBase(t *testing.T, dir string, data []byte) streamIndexFileIdentity {
	t.Helper()
	path := filepath.Join(dir, StreamIndexFileName)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	identity, err := computeStreamIndexFileIdentity(path)
	require.NoError(t, err)
	return identity
}

func rewriteStreamManifestTestCRC(encoded *[streamIndexManifestSize]byte) {
	binary.LittleEndian.PutUint32(
		encoded[60:64],
		crc32.Checksum(encoded[:60], streamIndexManifestCRC),
	)
}

func TestStreamIndexManifestErrorIdentity(t *testing.T) {
	// Keep the sentinel usable through the context added by file-level readers.
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, StreamIndexManifestFileName), []byte("torn"), 0o644,
	))
	_, err := readStreamIndexManifest(dir)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidStreamIndexManifest))
}
