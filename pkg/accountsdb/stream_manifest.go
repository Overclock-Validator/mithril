package accountsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	// StreamIndexManifestFileName is the durable publication record for the
	// immutable StreamHash account index. The base and manifest form one
	// fail-closed pair: neither artifact is usable without the other.
	StreamIndexManifestFileName = "accounts_index.manifest"

	streamIndexManifestVersion = uint32(1)
	streamIndexManifestSize    = 64
)

var (
	streamIndexManifestMagic = [8]byte{'M', 'I', 'T', 'H', 'S', 'M', '0', '1'}
	streamIndexManifestCRC   = crc32.MakeTable(crc32.Castagnoli)

	// ErrInvalidStreamIndexManifest identifies a present publication record
	// that is torn, corrupt, or structurally incompatible.
	ErrInvalidStreamIndexManifest = errors.New("accountsdb: invalid StreamHash index manifest")
)

// Stream-index manifest layout (all integers little-endian):
//
//	[0:8]   magic
//	[8:12]  format version
//	[12:16] encoded manifest size
//	[16:24] complete StreamHash file size
//	[24:56] complete StreamHash file SHA-256
//	[56:60] reserved (zero)
//	[60:64] CRC-32C of bytes [0:60]
//
// The fixed layout makes a short write unambiguously torn and leaves no
// variable-length fields for a corrupt decoder to trust.
func encodeStreamIndexManifest(identity streamIndexFileIdentity) ([streamIndexManifestSize]byte, error) {
	var encoded [streamIndexManifestSize]byte
	if err := validateStreamIndexManifestIdentity(identity); err != nil {
		return encoded, err
	}

	copy(encoded[0:8], streamIndexManifestMagic[:])
	binary.LittleEndian.PutUint32(encoded[8:12], streamIndexManifestVersion)
	binary.LittleEndian.PutUint32(encoded[12:16], streamIndexManifestSize)
	binary.LittleEndian.PutUint64(encoded[16:24], identity.Size)
	copy(encoded[24:56], identity.SHA256[:])
	binary.LittleEndian.PutUint32(
		encoded[60:64],
		crc32.Checksum(encoded[:60], streamIndexManifestCRC),
	)
	return encoded, nil
}

func decodeStreamIndexManifest(encoded []byte) (streamIndexFileIdentity, error) {
	var identity streamIndexFileIdentity
	if len(encoded) != streamIndexManifestSize {
		return identity, fmt.Errorf(
			"%w: length %d, want %d",
			ErrInvalidStreamIndexManifest, len(encoded), streamIndexManifestSize,
		)
	}
	if !bytes.Equal(encoded[0:8], streamIndexManifestMagic[:]) {
		return identity, fmt.Errorf(
			"%w: bad magic %x", ErrInvalidStreamIndexManifest, encoded[0:8],
		)
	}
	if version := binary.LittleEndian.Uint32(encoded[8:12]); version != streamIndexManifestVersion {
		return identity, fmt.Errorf(
			"%w: unsupported version %d", ErrInvalidStreamIndexManifest, version,
		)
	}
	if size := binary.LittleEndian.Uint32(encoded[12:16]); size != streamIndexManifestSize {
		return identity, fmt.Errorf(
			"%w: encoded size %d, want %d", ErrInvalidStreamIndexManifest, size, streamIndexManifestSize,
		)
	}
	if !streamIndexManifestAllZero(encoded[56:60]) {
		return identity, fmt.Errorf("%w: non-zero reserved bytes", ErrInvalidStreamIndexManifest)
	}
	wantCRC := binary.LittleEndian.Uint32(encoded[60:64])
	gotCRC := crc32.Checksum(encoded[:60], streamIndexManifestCRC)
	if gotCRC != wantCRC {
		return identity, fmt.Errorf(
			"%w: CRC mismatch: got %08x, want %08x",
			ErrInvalidStreamIndexManifest, gotCRC, wantCRC,
		)
	}

	identity.Size = binary.LittleEndian.Uint64(encoded[16:24])
	copy(identity.SHA256[:], encoded[24:56])
	if err := validateStreamIndexManifestIdentity(identity); err != nil {
		return streamIndexFileIdentity{}, err
	}
	return identity, nil
}

func validateStreamIndexManifestIdentity(identity streamIndexFileIdentity) error {
	if identity.Size == 0 {
		return fmt.Errorf("%w: zero StreamHash file size", ErrInvalidStreamIndexManifest)
	}
	return nil
}

func streamIndexManifestAllZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

// publishStreamIndexManifest atomically and durably replaces the publication
// record. The StreamHash base itself must already be durable before this is
// called; publishing this file is the final commit point for the pair.
func publishStreamIndexManifest(accountsDbDir string, identity streamIndexFileIdentity) error {
	encoded, err := encodeStreamIndexManifest(identity)
	if err != nil {
		return err
	}
	if accountsDbDir == "" {
		return errors.New("accountsdb: empty StreamHash manifest directory")
	}

	finalPath := filepath.Join(accountsDbDir, StreamIndexManifestFileName)
	tmpPath := finalPath + ".tmp"
	if err := os.Remove(tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("accountsdb: remove stale StreamHash manifest temporary file: %w", err)
	}
	defer os.Remove(tmpPath)

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("accountsdb: create StreamHash manifest temporary file: %w", err)
	}
	n, writeErr := f.Write(encoded[:])
	if writeErr == nil && n != len(encoded) {
		writeErr = io.ErrShortWrite
	}
	if writeErr != nil {
		_ = f.Close()
		return fmt.Errorf("accountsdb: write StreamHash manifest: %w", writeErr)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("accountsdb: sync StreamHash manifest: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("accountsdb: close StreamHash manifest: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("accountsdb: publish StreamHash manifest: %w", err)
	}
	if err := fsyncDir(accountsDbDir); err != nil {
		return fmt.Errorf("accountsdb: sync published StreamHash manifest: %w", err)
	}
	return nil
}

// readStreamIndexManifest strictly reads the publication record in
// accountsDbDir. A missing record remains distinguishable with errors.Is(err,
// os.ErrNotExist).
func readStreamIndexManifest(accountsDbDir string) (streamIndexFileIdentity, error) {
	path := filepath.Join(accountsDbDir, StreamIndexManifestFileName)
	file, info, err := openAtomicSelectorRegularFile(path)
	if err != nil {
		return streamIndexFileIdentity{}, fmt.Errorf(
			"%w: open %s: %w", ErrInvalidStreamIndexManifest, path, err,
		)
	}
	defer file.Close()
	if info.Size() != streamIndexManifestSize {
		return streamIndexFileIdentity{}, fmt.Errorf(
			"%w: length %d, want %d",
			ErrInvalidStreamIndexManifest, info.Size(), streamIndexManifestSize,
		)
	}
	encoded := make([]byte, streamIndexManifestSize)
	if _, err := io.ReadFull(file, encoded); err != nil {
		return streamIndexFileIdentity{}, fmt.Errorf("accountsdb: read StreamHash index manifest: %w", err)
	}
	if err := validateOpenedRegularFile(file, info); err != nil {
		return streamIndexFileIdentity{}, fmt.Errorf("%w: %v", ErrInvalidStreamIndexManifest, err)
	}
	identity, err := decodeStreamIndexManifest(encoded)
	if err != nil {
		return streamIndexFileIdentity{}, fmt.Errorf("accountsdb: decode StreamHash index manifest: %w", err)
	}
	return identity, nil
}

// validateStreamIndexManifestArtifacts validates the fail-closed pairing of
// the immutable StreamHash base and its publication manifest. Both absent is a
// valid legacy/no-base state. If both exist, the complete base file is hashed
// and must match the size and SHA-256 bound into the manifest.
func validateStreamIndexManifestArtifacts(accountsDbDir string) error {
	basePath := filepath.Join(accountsDbDir, StreamIndexFileName)
	manifestPath := filepath.Join(accountsDbDir, StreamIndexManifestFileName)

	baseExists, err := streamIndexRegularFileExists(basePath)
	if err != nil {
		return fmt.Errorf("accountsdb: inspect StreamHash index base: %w", err)
	}
	manifestExists, err := streamIndexRegularFileExists(manifestPath)
	if err != nil {
		return fmt.Errorf("accountsdb: inspect StreamHash index manifest: %w", err)
	}

	switch {
	case !baseExists && !manifestExists:
		return nil
	case baseExists && !manifestExists:
		return fmt.Errorf("accountsdb: StreamHash base %s exists without %s", basePath, manifestPath)
	case !baseExists && manifestExists:
		return fmt.Errorf("accountsdb: StreamHash manifest %s exists without %s", manifestPath, basePath)
	}

	identity, err := readStreamIndexManifest(accountsDbDir)
	if err != nil {
		return err
	}
	return verifyStreamIndexFileIdentity(basePath, identity)
}

func streamIndexRegularFileExists(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("%s is not a regular file", path)
	}
	return true, nil
}
