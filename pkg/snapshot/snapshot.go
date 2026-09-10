package snapshot

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"golang.org/x/sys/unix"
)

const maxSnapshotManifestSize = 256 << 20

func UnmarshalManifestFromSnapshot(ctx context.Context, filename string, accountsDbDir string) (*SnapshotManifest, error) {
	if err := os.MkdirAll(accountsDbDir, 0775); err != nil {
		return nil, err
	}

	tarReader, closer, err := newSnapshotReader(ctx, filename)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var manifestBytes []byte
	var archiveManifestSlot uint64

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("snapshot archive contains no canonical manifest member")
		}
		if err != nil {
			return nil, err
		}

		manifestSlot, isManifest := parseSnapshotManifestTarPath(header.Name)
		if !isManifest {
			continue
		}
		archiveManifestSlot = manifestSlot
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return nil, fmt.Errorf("snapshot manifest member %q is not a regular file", header.Name)
		}
		if header.Size < 0 || header.Size > maxSnapshotManifestSize {
			return nil, fmt.Errorf(
				"snapshot manifest member %q has invalid size %d (maximum %d)",
				header.Name, header.Size, maxSnapshotManifestSize,
			)
		}
		writer := bytes.NewBuffer(make([]byte, 0, int(header.Size)))
		copied, err := io.CopyN(writer, tarReader, header.Size)
		if err != nil {
			return nil, fmt.Errorf("read snapshot manifest member %q: %w", header.Name, err)
		}
		if copied != header.Size {
			return nil, fmt.Errorf("short snapshot manifest member %q: read %d of %d bytes", header.Name, copied, header.Size)
		}
		manifestBytes = writer.Bytes()
		break
	}

	manifest := new(SnapshotManifest)
	decoder := bin.NewBinDecoder(manifestBytes)
	if err := manifest.UnmarshalWithDecoder(decoder); err != nil {
		return nil, fmt.Errorf("decode snapshot manifest: %w", err)
	}
	manifest.rawDigest = sha256.Sum256(manifestBytes)
	manifest.hasRawDigest = true
	if err := validateSnapshotManifestIdentity(manifest, archiveManifestSlot); err != nil {
		return nil, fmt.Errorf("validate snapshot manifest identity: %w", err)
	}
	if err := writeFileAtomically(filepath.Join(accountsDbDir, "manifest"), manifestBytes, 0o644); err != nil {
		return nil, fmt.Errorf("publish decoded snapshot manifest: %w", err)
	}
	return manifest, nil
}

func isSnapshotManifestTarPath(name string) bool {
	_, ok := parseSnapshotManifestTarPath(name)
	return ok
}

func parseSnapshotManifestTarPath(name string) (uint64, bool) {
	if name == "" || pathpkg.IsAbs(name) || pathpkg.Clean(name) != name {
		return 0, false
	}
	parts := strings.Split(name, "/")
	if len(parts) != 3 || parts[0] != "snapshots" || parts[1] != parts[2] {
		return 0, false
	}
	slot, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || strconv.FormatUint(slot, 10) != parts[1] {
		return 0, false
	}
	return slot, true
}

type appendVecCopyingTask struct {
	Path string
	// Data is retained only for direct worker tests. Production archive
	// ingestion streams to Path and leaves Data nil.
	Data     []byte
	Slot     uint64
	FileID   uint64
	FileSize uint64
}

type indexEntryBuilderTask struct {
	Path     string
	FileSize uint64
	Slot     uint64
	FileId   uint64
}

type indexEntryCommitterTask struct {
	IndexEntries []accountsdb.AccountIndexEntry
	Pubkeys      []solana.PublicKey
}

const (
	snapshotTypeZst = iota
	snapshotTypeLz4
)

var (
	ZstdDecoderConcurrency = runtime.NumCPU()
)

func readerForCompressionType(snapshotType int, bmr *bufmonreader) (r io.Reader, closeCompression func(), err error) {
	if snapshotType == snapshotTypeZst {
		zstdReader, err := zstd.NewReader(bmr, zstd.WithDecoderConcurrency(ZstdDecoderConcurrency))
		if err != nil {
			return nil, nil, err
		}
		r = zstdReader
		closeCompression = zstdReader.Close
	} else if snapshotType == snapshotTypeLz4 {
		r = lz4.NewReader(bmr)
	} else {
		return nil, nil, fmt.Errorf("unknown snapshot compression type %d", snapshotType)
	}

	return r, closeCompression, nil
}

type snapshotReadCloser struct {
	once             sync.Once
	decompressed     io.Reader
	closeCompression func()
	source           io.Closer
	err              error
}

func (closer *snapshotReadCloser) close(drain bool) error {
	if closer == nil {
		return nil
	}
	closer.once.Do(func() {
		if drain {
			_, closer.err = io.Copy(io.Discard, closer.decompressed)
		}
		if closer.closeCompression != nil {
			closer.closeCompression()
		}
		closer.err = errors.Join(closer.err, closer.source.Close())
	})
	return closer.err
}

// Finish consumes the compression trailer/checksum before synchronizing and
// closing the raw source. Close alone is an abort path and does not drain a
// potentially remote stream.
func (closer *snapshotReadCloser) Finish() error { return closer.close(true) }
func (closer *snapshotReadCloser) Close() error  { return closer.close(false) }

func parseSnapshotType(snapshotFileName string) (int, error) {
	name := snapshotFileName
	if strings.HasPrefix(name, "https://") || strings.HasPrefix(name, "http://") {
		parsed, err := url.Parse(name)
		if err != nil {
			return 0, fmt.Errorf("parse snapshot URL: %w", err)
		}
		name = parsed.Path
	}
	switch fileExt := filepath.Ext(name); fileExt {
	case ".zst":
		return snapshotTypeZst, nil
	case ".lz4":
		return snapshotTypeLz4, nil
	default:
		return 0, fmt.Errorf("unknown snapshot compression type: extension %q", fileExt)
	}
}

func newSnapshotReader(ctx context.Context, filename string) (*tar.Reader, io.Closer, error) {
	return newSnapshotReaderWithSave(ctx, filename, "")
}

// newSnapshotReaderWithSave creates a tar reader for a snapshot file or HTTP URL.
// If filename is an HTTP URL and savePath is non-empty, the data will be saved
// to disk while streaming (using io.TeeReader for parallel download+processing+saving).
func newSnapshotReaderWithSave(ctx context.Context, filename string, savePath string) (*tar.Reader, io.Closer, error) {
	tarReader, bmr, closer, err := newSnapshotReaderWithProgress(ctx, filename, savePath)
	if err != nil {
		return nil, nil, err
	}
	// Return closer, bmr is not exposed in this version
	_ = bmr
	return tarReader, closer, nil
}

// newSnapshotReaderWithProgress creates a tar reader and also returns the bufmonreader
// for progress tracking. Use bufmonreader.SetProgressCallback() to receive progress updates.
func newSnapshotReaderWithProgress(ctx context.Context, filename string, savePath string) (*tar.Reader, *bufmonreader, io.Closer, error) {
	snapshotType, err := parseSnapshotType(filename)
	if err != nil {
		return nil, nil, nil, err
	}
	var bmr *bufmonreader
	if strings.HasPrefix(filename, "https://") || strings.HasPrefix(filename, "http://") {
		bmr, err = NewBufMonReaderHTTPWithSave(ctx, filename, savePath)
	} else {
		snapshotFile, err := os.Open(filename)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("open %s: %w", filename, err)
		}
		bmr, err = NewBufMonReaderFromFile(snapshotFile)
		if err != nil {
			err = errors.Join(err, snapshotFile.Close())
		}
	}
	if err != nil {
		return nil, nil, nil, err
	}
	reader, closeCompression, err := readerForCompressionType(snapshotType, bmr)
	if err != nil {
		return nil, nil, nil, errors.Join(fmt.Errorf("opening compression reader: %w", err), bmr.Close())
	}
	closer := &snapshotReadCloser{
		decompressed:     reader,
		closeCompression: closeCompression,
		source:           bmr,
	}
	return tar.NewReader(reader), bmr, closer, nil
}

func LoadManifestFromFile(filename string) (*SnapshotManifest, error) {
	manifestBytes, err := readManifestFile(filename)
	if err != nil {
		return nil, fmt.Errorf("reading manifest from file=%s: %w", filename, err)
	}

	manifest := new(SnapshotManifest)
	decoder := bin.NewBinDecoder(manifestBytes)
	err = manifest.UnmarshalWithDecoder(decoder)
	if err != nil {
		return nil, err
	}
	manifest.rawDigest = sha256.Sum256(manifestBytes)
	manifest.hasRawDigest = true

	return manifest, nil
}

func readManifestFile(filename string) ([]byte, error) {
	pathInfo, err := os.Lstat(filename)
	if err != nil {
		return nil, err
	}
	if !pathInfo.Mode().IsRegular() || pathInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("manifest is not a regular file")
	}
	if pathInfo.Size() < 0 || pathInfo.Size() > maxSnapshotManifestSize {
		return nil, fmt.Errorf("manifest size %d exceeds maximum %d", pathInfo.Size(), maxSnapshotManifestSize)
	}

	fd, err := unix.Open(filename, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), filename)
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(pathInfo, openedInfo) {
		return nil, fmt.Errorf("manifest changed while opening")
	}
	if openedInfo.Size() < 0 || openedInfo.Size() > maxSnapshotManifestSize {
		return nil, fmt.Errorf("manifest size %d exceeds maximum %d", openedInfo.Size(), maxSnapshotManifestSize)
	}
	contents := make([]byte, int(openedInfo.Size()))
	if _, err := io.ReadFull(file, contents); err != nil {
		return nil, err
	}
	var growthProbe [1]byte
	if n, err := file.Read(growthProbe[:]); err != io.EOF || n != 0 {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("manifest grew while reading")
	}
	afterInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if afterInfo.Size() != openedInfo.Size() || !afterInfo.ModTime().Equal(openedInfo.ModTime()) {
		return nil, fmt.Errorf("manifest changed while reading")
	}
	finalPathInfo, err := os.Lstat(filename)
	if err != nil {
		return nil, err
	}
	if !finalPathInfo.Mode().IsRegular() || finalPathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(openedInfo, finalPathInfo) {
		return nil, fmt.Errorf("manifest path changed while reading")
	}
	return contents, nil
}
