package snapshot

import (
	"archive/tar"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func writeTestSnapshotArchive(t *testing.T, memberName string, manifest []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snapshot.tar.zst")
	file, err := os.Create(path)
	require.NoError(t, err)
	compressor, err := zstd.NewWriter(file)
	require.NoError(t, err)
	archive := tar.NewWriter(compressor)
	require.NoError(t, archive.WriteHeader(&tar.Header{
		Name:     memberName,
		Mode:     0o644,
		Size:     int64(len(manifest)),
		Typeflag: tar.TypeReg,
	}))
	_, err = archive.Write(manifest)
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	require.NoError(t, compressor.Close())
	require.NoError(t, file.Close())
	return path
}

func TestUnmarshalManifestValidatesBeforePublishing(t *testing.T) {
	accountsDirectory := t.TempDir()
	canonicalPath := filepath.Join(accountsDirectory, "manifest")
	require.NoError(t, os.WriteFile(canonicalPath, []byte("known-good"), 0o644))
	snapshotPath := writeTestSnapshotArchive(t, "snapshots/42/42", []byte("truncated"))

	_, err := UnmarshalManifestFromSnapshot(context.Background(), snapshotPath, accountsDirectory)
	require.ErrorContains(t, err, "decode snapshot manifest")
	contents, readErr := os.ReadFile(canonicalPath)
	require.NoError(t, readErr)
	require.Equal(t, []byte("known-good"), contents)
}

func TestSnapshotManifestTarPathIsCanonical(t *testing.T) {
	require.True(t, isSnapshotManifestTarPath("snapshots/42/42"))
	for _, name := range []string{
		"/snapshots/42/42",
		"./snapshots/42/42",
		"prefix/snapshots/42",
		"snapshots/0042/0042",
		"snapshots/42/43",
		"snapshots/42/42/extra",
	} {
		require.Falsef(t, isSnapshotManifestTarPath(name), "accepted %q", name)
	}
}

func TestParseSnapshotTypeRejectsUnknownAndRecognizesLZ4(t *testing.T) {
	kind, err := parseSnapshotType("snapshot.tar.lz4")
	require.NoError(t, err)
	require.Equal(t, snapshotTypeLz4, kind)
	kind, err = parseSnapshotType("snapshot.tar.zst")
	require.NoError(t, err)
	require.Equal(t, snapshotTypeZst, kind)
	kind, err = parseSnapshotType("https://snapshot.example/snapshot.tar.zst?token=secret")
	require.NoError(t, err)
	require.Equal(t, snapshotTypeZst, kind)
	_, err = parseSnapshotType("snapshot.tar.gz")
	require.ErrorContains(t, err, "unknown snapshot compression type")
}

func TestLoadManifestRejectsSymlinkAndOversizedSparseFile(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	require.NoError(t, os.WriteFile(target, []byte("manifest"), 0o644))
	symlink := filepath.Join(directory, "manifest-link")
	require.NoError(t, os.Symlink(target, symlink))
	_, err := LoadManifestFromFile(symlink)
	require.ErrorContains(t, err, "not a regular file")

	oversized := filepath.Join(directory, "oversized")
	file, err := os.Create(oversized)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(maxSnapshotManifestSize+1))
	require.NoError(t, file.Close())
	_, err = LoadManifestFromFile(oversized)
	require.ErrorContains(t, err, "exceeds maximum")
}
