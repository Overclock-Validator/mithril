package snapshot

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestReadTarRetainsStatusCacheForEveryBuilderOptionPath(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "snapshot.tar.zst")
	payload := []byte("status-cache-seed")
	writeStatusCacheArchive(t, archive, payload, 1)

	tests := []struct {
		name          string
		savePath      bool
		isIncremental bool
	}{
		{name: "paths full"},
		{name: "paths incremental", isIncremental: true},
		{name: "auto full with save path", savePath: true},
		{name: "auto incremental with save path", savePath: true, isIncremental: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			destination := filepath.Join(dir, tt.name, "status-cache")
			options := readTarOptions{
				isIncremental:   tt.isIncremental,
				statusCachePath: destination,
			}
			if tt.savePath {
				options.savePath = filepath.Join(dir, tt.name, "saved.tar.zst")
			}
			if err := readTar(context.Background(), &sync.WaitGroup{}, archive, nil, options); err != nil {
				t.Fatalf("readTar: %v", err)
			}
			assertStatusCacheFileBytes(t, destination, payload)
		})
	}
}

func TestReadTarRequiresExactlyOneStatusCacheAndPreservesInstalledSeed(t *testing.T) {
	dir := t.TempDir()
	destination := filepath.Join(dir, "status-cache")
	installed := []byte("full-status-cache")
	if err := os.WriteFile(destination, installed, 0o644); err != nil {
		t.Fatalf("write installed seed: %v", err)
	}

	missing := filepath.Join(dir, "missing.tar.zst")
	writeStatusCacheArchive(t, missing, nil, 0)
	if err := readTar(context.Background(), &sync.WaitGroup{}, missing, nil, readTarOptions{
		isIncremental: true, statusCachePath: destination,
	}); err == nil {
		t.Fatal("expected missing status-cache error")
	}
	assertStatusCacheFileBytes(t, destination, installed)

	duplicate := filepath.Join(dir, "duplicate.tar.zst")
	writeStatusCacheArchive(t, duplicate, []byte("new-status-cache"), 2)
	if err := readTar(context.Background(), &sync.WaitGroup{}, duplicate, nil, readTarOptions{
		isIncremental: true, statusCachePath: destination,
	}); err == nil {
		t.Fatal("expected duplicate status-cache error")
	}
	assertStatusCacheFileBytes(t, destination, installed)
}

func TestReadTarCorruptTailAfterStatusCachePreservesInstalledSeed(t *testing.T) {
	dir := t.TempDir()
	destination := filepath.Join(dir, "status-cache")
	installed := []byte("full-status-cache")
	if err := os.WriteFile(destination, installed, 0o644); err != nil {
		t.Fatalf("write installed seed: %v", err)
	}

	archive := filepath.Join(dir, "corrupt-tail.tar.zst")
	writeStatusCacheArchiveWithCorruptTail(t, archive, []byte("new-status-cache"))
	if err := readTar(context.Background(), &sync.WaitGroup{}, archive, nil, readTarOptions{
		isIncremental: true, statusCachePath: destination,
	}); err == nil {
		t.Fatal("expected corrupt tar tail error")
	}
	assertStatusCacheFileBytes(t, destination, installed)

	temporaries, err := filepath.Glob(filepath.Join(dir, ".snapshot-status-cache-*.partial"))
	if err != nil {
		t.Fatalf("glob status-cache temporaries: %v", err)
	}
	if len(temporaries) != 0 {
		t.Fatalf("temporary status-cache files remain after corrupt archive: %v", temporaries)
	}
}

func TestReadTarRejectsArchiveMissingManifestAppendVec(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "missing-appendvec.tar.zst")
	writeStatusCacheArchive(t, archive, []byte("status-cache"), 1)
	pools := &snapshotWorkerPools{
		errors: &snapshotWorkerErrors{},
		manifest: &SnapshotManifest{AccountsDb: &AccountsDbFields{Storages: map[uint64]SlotAcctVecs{
			7: {Slot: 7, AcctVecs: []AcctVec{{Id: 9, FileSize: 123}}},
		}}},
	}
	destination := filepath.Join(dir, "status-cache")

	err := readTar(context.Background(), &sync.WaitGroup{}, archive, pools, readTarOptions{
		statusCachePath: destination,
	})
	if err == nil || !strings.Contains(err.Error(), "omitted 1 manifest appendvec") {
		t.Fatalf("expected missing manifest appendvec error, got %v", err)
	}
	if _, statErr := os.Stat(destination); !os.IsNotExist(statErr) {
		t.Fatalf("status cache was published for incomplete archive: %v", statErr)
	}
}

func TestReadTarBindsManifestPassAndRequiresSupportedVersion(t *testing.T) {
	dir := t.TempDir()
	expectedManifest := []byte("manifest-selected-on-first-pass")
	manifest := &SnapshotManifest{
		AccountsDb:   &AccountsDbFields{Storages: map[uint64]SlotAcctVecs{}},
		rawDigest:    sha256.Sum256(expectedManifest),
		hasRawDigest: true,
	}
	pools := &snapshotWorkerPools{errors: &snapshotWorkerErrors{}, manifest: manifest}

	valid := filepath.Join(dir, "valid.tar.zst")
	writeBootstrapMetadataArchive(t, valid, expectedManifest, "1.2.0", 1, 1)
	require.NoError(t, readTar(context.Background(), &sync.WaitGroup{}, valid, pools, readTarOptions{
		statusCachePath: filepath.Join(dir, "valid-status"),
	}))

	changed := filepath.Join(dir, "changed.tar.zst")
	writeBootstrapMetadataArchive(t, changed, []byte("different-manifest"), "1.2.0", 1, 1)
	err := readTar(context.Background(), &sync.WaitGroup{}, changed, pools, readTarOptions{
		statusCachePath: filepath.Join(dir, "changed-status"),
	})
	require.ErrorContains(t, err, "manifest differs")

	unsupported := filepath.Join(dir, "unsupported.tar.zst")
	writeBootstrapMetadataArchive(t, unsupported, expectedManifest, "2.0.0", 1, 1)
	err = readTar(context.Background(), &sync.WaitGroup{}, unsupported, pools, readTarOptions{
		statusCachePath: filepath.Join(dir, "unsupported-status"),
	})
	require.ErrorContains(t, err, "unsupported snapshot version")

	duplicate := filepath.Join(dir, "duplicate-manifest.tar.zst")
	writeBootstrapMetadataArchive(t, duplicate, expectedManifest, "1.2.0", 2, 1)
	err = readTar(context.Background(), &sync.WaitGroup{}, duplicate, pools, readTarOptions{
		statusCachePath: filepath.Join(dir, "duplicate-status"),
	})
	require.ErrorContains(t, err, "multiple canonical manifest")
}

func writeBootstrapMetadataArchive(
	t *testing.T,
	filename string,
	manifest []byte,
	version string,
	manifestCopies int,
	versionCopies int,
) {
	t.Helper()
	f, err := os.Create(filename)
	require.NoError(t, err)
	zw, err := zstd.NewWriter(f)
	require.NoError(t, err)
	tw := tar.NewWriter(zw)
	for range versionCopies {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: "version", Mode: 0o644, Size: int64(len(version))}))
		_, err = tw.Write([]byte(version))
		require.NoError(t, err)
	}
	for range manifestCopies {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: "snapshots/7/7", Mode: 0o644, Size: int64(len(manifest))}))
		_, err = tw.Write(manifest)
		require.NoError(t, err)
	}
	status := bytes.Repeat([]byte{0x7a}, 32)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: agaveStatusCacheArchiveMember, Mode: 0o644, Size: int64(len(status))}))
	_, err = tw.Write(status)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, zw.Close())
	require.NoError(t, f.Close())
}

func writeStatusCacheArchive(t *testing.T, filename string, payload []byte, copies int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		t.Fatalf("mkdir archive dir: %v", err)
	}
	f, err := os.Create(filename)
	if err != nil {
		t.Fatalf("create archive: %v", err)
	}
	zw, err := zstd.NewWriter(f)
	if err != nil {
		f.Close()
		t.Fatalf("create zstd writer: %v", err)
	}
	tw := tar.NewWriter(zw)
	for range copies {
		header := &tar.Header{Name: agaveStatusCacheArchiveMember, Mode: 0o644, Size: int64(len(payload))}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatalf("write status-cache header: %v", err)
		}
		if _, err := tw.Write(payload); err != nil {
			t.Fatalf("write status-cache payload: %v", err)
		}
	}
	if copies == 0 {
		dummy := []byte("ignored")
		if err := tw.WriteHeader(&tar.Header{Name: "snapshots/other", Mode: 0o644, Size: int64(len(dummy))}); err != nil {
			t.Fatalf("write dummy header: %v", err)
		}
		if _, err := tw.Write(dummy); err != nil {
			t.Fatalf("write dummy payload: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zstd writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close archive: %v", err)
	}
}

func writeStatusCacheArchiveWithCorruptTail(t *testing.T, filename string, payload []byte) {
	t.Helper()
	var raw bytes.Buffer
	tw := tar.NewWriter(&raw)
	header := &tar.Header{Name: agaveStatusCacheArchiveMember, Mode: 0o644, Size: int64(len(payload))}
	if err := tw.WriteHeader(header); err != nil {
		t.Fatalf("write status-cache header: %v", err)
	}
	if _, err := tw.Write(payload); err != nil {
		t.Fatalf("write status-cache payload: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	if raw.Len() < 1024 {
		t.Fatalf("tar archive unexpectedly short: %d", raw.Len())
	}
	// Strip the two clean end-of-archive records, then leave a partial next
	// header. readTar must not install the already-captured candidate until it
	// has consumed and validated this tail.
	corrupt := append([]byte(nil), raw.Bytes()[:raw.Len()-1024]...)
	corrupt = append(corrupt, bytes.Repeat([]byte{0xff}, 100)...)

	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		t.Fatalf("mkdir archive dir: %v", err)
	}
	f, err := os.Create(filename)
	if err != nil {
		t.Fatalf("create archive: %v", err)
	}
	zw, err := zstd.NewWriter(f)
	if err != nil {
		f.Close()
		t.Fatalf("create zstd writer: %v", err)
	}
	if _, err := zw.Write(corrupt); err != nil {
		t.Fatalf("write corrupt archive: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zstd writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close archive: %v", err)
	}
}
