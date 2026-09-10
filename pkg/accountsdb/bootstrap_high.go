package accountsdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	// BootstrapHighFileIDFileName is the write-once snapshot file-ID boundary.
	// Manifest-less data above it can be an interrupted post-snapshot commit;
	// data at or below it belongs to snapshot bootstrap.
	BootstrapHighFileIDFileName = "bootstrap_high_file_id"
	// BootstrapHighFileIDSize is the exact encoded marker size.
	BootstrapHighFileIDSize = 32
	bootstrapHighVersion    = uint32(1)
)

var (
	bootstrapHighMagic = [8]byte{'M', 'I', 'T', 'H', 'B', 'H', '0', '1'}
	bootstrapHighCRC   = crc32.MakeTable(crc32.Castagnoli)

	ErrInvalidBootstrapHighFileID = errors.New("accountsdb: invalid bootstrap high file ID")
)

// WriteBootstrapHighFileID durably publishes the write-once snapshot
// high-water mark. It refuses to replace an existing marker: changing this
// boundary in a live lineage could turn snapshot data into orphan candidates.
func WriteBootstrapHighFileID(root string, fileID uint64) (retErr error) {
	if root == "" {
		return fmt.Errorf("%w: empty AccountsDB root", ErrInvalidBootstrapHighFileID)
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("%w: inspect AccountsDB root: %v", ErrInvalidBootstrapHighFileID, err)
	}
	if !rootInfo.IsDir() {
		return fmt.Errorf("%w: AccountsDB root is not a real directory", ErrInvalidBootstrapHighFileID)
	}

	path := filepath.Join(root, BootstrapHighFileIDFileName)
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("%w: marker already exists", ErrInvalidBootstrapHighFileID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: inspect existing marker: %v", ErrInvalidBootstrapHighFileID, err)
	}

	var encoded [BootstrapHighFileIDSize]byte
	copy(encoded[:8], bootstrapHighMagic[:])
	binary.LittleEndian.PutUint32(encoded[8:12], bootstrapHighVersion)
	binary.LittleEndian.PutUint32(encoded[12:16], BootstrapHighFileIDSize)
	binary.LittleEndian.PutUint64(encoded[16:24], fileID)
	binary.LittleEndian.PutUint32(encoded[28:32], crc32.Checksum(encoded[:28], bootstrapHighCRC))

	tmp, err := os.CreateTemp(root, ".bootstrap-high-*.tmp")
	if err != nil {
		return fmt.Errorf("%w: create temporary marker: %v", ErrInvalidBootstrapHighFileID, err)
	}
	tmpPath := tmp.Name()
	tmpOpen := true
	cleanupTmp := true
	defer func() {
		if tmpOpen {
			retErr = errors.Join(retErr, tmp.Close())
		}
		if cleanupTmp {
			if err := os.Remove(tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				retErr = errors.Join(retErr, fmt.Errorf("remove temporary bootstrap marker: %w", err))
			}
		}
	}()
	if err := writeAll(tmp, encoded[:]); err != nil {
		return fmt.Errorf("%w: write temporary marker: %v", ErrInvalidBootstrapHighFileID, err)
	}
	if err := tmp.Chmod(0o644); err != nil {
		return fmt.Errorf("%w: set temporary marker permissions: %v", ErrInvalidBootstrapHighFileID, err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("%w: sync temporary marker: %v", ErrInvalidBootstrapHighFileID, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("%w: close temporary marker: %v", ErrInvalidBootstrapHighFileID, err)
	}
	tmpOpen = false

	// Linking publishes without replacement. Unlike Rename, this cannot race
	// another writer and overwrite the write-once boundary after the Lstat
	// check above. The temporary inode is already complete and durable.
	if err := os.Link(tmpPath, path); err != nil {
		return fmt.Errorf("%w: publish marker: %v", ErrInvalidBootstrapHighFileID, err)
	}
	// Keep the temporary link until the final link itself is durable. If this
	// sync fails, retaining both names is safer than unlinking the only directory
	// entry whose persistence the filesystem might have ordered first.
	cleanupTmp = false
	if err := fsyncDir(root); err != nil {
		return fmt.Errorf("%w: marker is visible but publication durability is uncertain: %v", ErrInvalidBootstrapHighFileID, err)
	}
	if err := os.Remove(tmpPath); err != nil {
		return fmt.Errorf("%w: marker is durable but temporary-link cleanup failed: %v", ErrInvalidBootstrapHighFileID, err)
	}
	if err := fsyncDir(root); err != nil {
		return fmt.Errorf("%w: marker is durable but temporary-link cleanup durability is uncertain: %v", ErrInvalidBootstrapHighFileID, err)
	}
	return nil
}

// ReadBootstrapHighFileID validates the complete marker before returning its
// value. Recovery must fail closed on any error; guessing a smaller boundary
// could delete valid snapshot appendvecs.
func ReadBootstrapHighFileID(root string) (uint64, error) {
	path := filepath.Join(root, BootstrapHighFileIDFileName)
	file, info, err := openStableRegularFile(path)
	if err != nil {
		return 0, fmt.Errorf("%w: open marker: %v", ErrInvalidBootstrapHighFileID, err)
	}
	defer file.Close()
	if info.Size() != BootstrapHighFileIDSize {
		return 0, fmt.Errorf("%w: size %d, expected %d", ErrInvalidBootstrapHighFileID, info.Size(), BootstrapHighFileIDSize)
	}
	var encoded [BootstrapHighFileIDSize]byte
	if _, err := io.ReadFull(file, encoded[:]); err != nil {
		return 0, fmt.Errorf("%w: read marker: %v", ErrInvalidBootstrapHighFileID, err)
	}
	if string(encoded[:8]) != string(bootstrapHighMagic[:]) ||
		binary.LittleEndian.Uint32(encoded[8:12]) != bootstrapHighVersion ||
		binary.LittleEndian.Uint32(encoded[12:16]) != BootstrapHighFileIDSize {
		return 0, fmt.Errorf("%w: unsupported header", ErrInvalidBootstrapHighFileID)
	}
	if binary.LittleEndian.Uint32(encoded[24:28]) != 0 {
		return 0, fmt.Errorf("%w: non-zero reserved bytes", ErrInvalidBootstrapHighFileID)
	}
	wantCRC := binary.LittleEndian.Uint32(encoded[28:32])
	if got := crc32.Checksum(encoded[:28], bootstrapHighCRC); got != wantCRC {
		return 0, fmt.Errorf("%w: checksum mismatch", ErrInvalidBootstrapHighFileID)
	}
	if err := validateStableRegularFile(file, path, info); err != nil {
		return 0, fmt.Errorf("%w: marker changed while reading: %v", ErrInvalidBootstrapHighFileID, err)
	}
	return binary.LittleEndian.Uint64(encoded[16:24]), nil
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := writer.Write(data)
		if n < 0 || n > len(data) {
			return fmt.Errorf("invalid write count %d for %d bytes", n, len(data))
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
