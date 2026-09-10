package accountsdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	// LargestFileIDFileName stores the global appendvec/segment file-ID
	// high-water mark. File IDs are globally unique across slots.
	LargestFileIDFileName = "largest_file_id"
	// LargestFileIDSize is the exact size of the checksummed V1 encoding.
	LargestFileIDSize    = 32
	largestFileIDVersion = uint32(1)
)

var (
	largestFileIDMagic = [8]byte{'M', 'I', 'T', 'H', 'L', 'F', '0', '1'}
	largestFileIDCRC   = crc32.MakeTable(crc32.Castagnoli)

	ErrInvalidLargestFileID = errors.New("accountsdb: invalid largest file ID")
	// ErrLargestFileIDCommitDecided means the selector rename completed but a
	// later durability operation failed. The running process must not allocate
	// another ID because it cannot safely infer which selector survives a crash.
	ErrLargestFileIDCommitDecided = errors.New("accountsdb: largest file ID publication commit decided")
	ErrLargestFileIDExhausted     = errors.New("accountsdb: largest file ID exhausted")
)

type largestFileIDPublisher func(string, uint64) (bool, error)

// fsyncDir makes a directory entry change (create/rename/unlink) durable.
func fsyncDir(dir string) (retErr error) {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("accountsdb: open dir for fsync: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, d.Close()) }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("accountsdb: sync dir: %w", err)
	}
	return nil
}

func encodeLargestFileID(fileID uint64) [LargestFileIDSize]byte {
	var encoded [LargestFileIDSize]byte
	copy(encoded[:8], largestFileIDMagic[:])
	binary.LittleEndian.PutUint32(encoded[8:12], largestFileIDVersion)
	binary.LittleEndian.PutUint32(encoded[12:16], LargestFileIDSize)
	binary.LittleEndian.PutUint64(encoded[16:24], fileID)
	binary.LittleEndian.PutUint32(encoded[28:32], crc32.Checksum(encoded[:28], largestFileIDCRC))
	return encoded
}

// WriteLargestFileID atomically and durably publishes a complete high-water
// selector. A post-rename error is explicitly commit-decided: callers must
// fence further allocation in the current process.
func WriteLargestFileID(root string, fileID uint64) error {
	renamed, err := writeLargestFileIDAtomic(root, fileID, os.Rename, fsyncDir)
	if err != nil && renamed {
		return errors.Join(ErrLargestFileIDCommitDecided, err)
	}
	return err
}

func writeLargestFileIDAtomic(
	root string,
	fileID uint64,
	rename func(string, string) error,
	syncDirectory func(string) error,
) (renamed bool, retErr error) {
	if root == "" {
		return false, fmt.Errorf("%w: empty AccountsDB root", ErrInvalidLargestFileID)
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return false, fmt.Errorf("%w: inspect AccountsDB root: %w", ErrInvalidLargestFileID, err)
	}
	if !rootInfo.IsDir() {
		return false, fmt.Errorf("%w: AccountsDB root is not a real directory", ErrInvalidLargestFileID)
	}
	if rename == nil || syncDirectory == nil {
		return false, fmt.Errorf("%w: incomplete persistence operations", ErrInvalidLargestFileID)
	}

	path := filepath.Join(root, LargestFileIDFileName)
	if info, statErr := os.Lstat(path); statErr == nil {
		if !info.Mode().IsRegular() {
			return false, fmt.Errorf("%w: existing selector is not a regular file", ErrInvalidLargestFileID)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return false, fmt.Errorf("%w: inspect existing selector: %w", ErrInvalidLargestFileID, statErr)
	}

	encoded := encodeLargestFileID(fileID)
	tmp, err := os.CreateTemp(root, ".largest-file-id-*.tmp")
	if err != nil {
		return false, fmt.Errorf("%w: create temporary selector: %w", ErrInvalidLargestFileID, err)
	}
	tmpPath := tmp.Name()
	tmpOpen := true
	cleanupTmp := true
	defer func() {
		if tmpOpen {
			retErr = errors.Join(retErr, tmp.Close())
		}
		if cleanupTmp {
			if removeErr := os.Remove(tmpPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				retErr = errors.Join(retErr, fmt.Errorf("remove temporary largest-file-ID selector: %w", removeErr))
			}
		}
	}()
	if err := writeAll(tmp, encoded[:]); err != nil {
		return false, fmt.Errorf("%w: write temporary selector: %w", ErrInvalidLargestFileID, err)
	}
	if err := tmp.Chmod(0o644); err != nil {
		return false, fmt.Errorf("%w: set temporary selector permissions: %w", ErrInvalidLargestFileID, err)
	}
	if err := tmp.Sync(); err != nil {
		return false, fmt.Errorf("%w: sync temporary selector: %w", ErrInvalidLargestFileID, err)
	}
	if err := tmp.Close(); err != nil {
		return false, fmt.Errorf("%w: close temporary selector: %w", ErrInvalidLargestFileID, err)
	}
	tmpOpen = false
	if err := rename(tmpPath, path); err != nil {
		return false, fmt.Errorf("%w: publish selector: %w", ErrInvalidLargestFileID, err)
	}
	renamed = true
	cleanupTmp = false
	if err := syncDirectory(root); err != nil {
		return true, fmt.Errorf("%w: selector is visible but publication durability is uncertain: %w", ErrInvalidLargestFileID, err)
	}
	return true, nil
}

// ReadLargestFileID validates a complete selector. Atomic replacement is
// allowed while opening; mutation of the inode actually consumed is not.
func ReadLargestFileID(root string) (uint64, error) {
	path := filepath.Join(root, LargestFileIDFileName)
	file, info, err := openAtomicSelectorRegularFile(path)
	if err != nil {
		return 0, fmt.Errorf("%w: open selector: %w", ErrInvalidLargestFileID, err)
	}
	defer file.Close()
	if info.Size() != LargestFileIDSize {
		return 0, fmt.Errorf("%w: size %d, expected %d", ErrInvalidLargestFileID, info.Size(), LargestFileIDSize)
	}
	var encoded [LargestFileIDSize]byte
	if _, err := io.ReadFull(file, encoded[:]); err != nil {
		return 0, fmt.Errorf("%w: read selector: %w", ErrInvalidLargestFileID, err)
	}
	if string(encoded[:8]) != string(largestFileIDMagic[:]) ||
		binary.LittleEndian.Uint32(encoded[8:12]) != largestFileIDVersion ||
		binary.LittleEndian.Uint32(encoded[12:16]) != LargestFileIDSize {
		return 0, fmt.Errorf("%w: unsupported header", ErrInvalidLargestFileID)
	}
	if binary.LittleEndian.Uint32(encoded[24:28]) != 0 {
		return 0, fmt.Errorf("%w: non-zero reserved bytes", ErrInvalidLargestFileID)
	}
	wantCRC := binary.LittleEndian.Uint32(encoded[28:32])
	if got := crc32.Checksum(encoded[:28], largestFileIDCRC); got != wantCRC {
		return 0, fmt.Errorf("%w: checksum mismatch", ErrInvalidLargestFileID)
	}
	if err := validateOpenedRegularFile(file, info); err != nil {
		return 0, fmt.Errorf("%w: selector changed while reading: %w", ErrInvalidLargestFileID, err)
	}
	return binary.LittleEndian.Uint64(encoded[16:24]), nil
}

// ValidateLargestFileID also reconciles the selector against every canonical
// appendvec/segment and final manifest. A counter behind disk, or one ID used
// by different slot/file identities, is corruption and fails startup closed.
func ValidateLargestFileID(root string) (uint64, error) {
	fileID, err := ReadLargestFileID(root)
	if err != nil {
		return 0, err
	}
	maxOnDisk, err := scanLargestCanonicalFileID(filepath.Join(root, "accounts"))
	if err != nil {
		return 0, err
	}
	if fileID < maxOnDisk {
		return 0, fmt.Errorf(
			"%w: persisted high-water %d is below canonical on-disk file ID %d",
			ErrInvalidLargestFileID, fileID, maxOnDisk,
		)
	}
	return fileID, nil
}

func scanLargestCanonicalFileID(accountsDir string) (maximum uint64, retErr error) {
	dirInfo, err := os.Lstat(accountsDir)
	if err != nil {
		return 0, fmt.Errorf("%w: inspect accounts directory: %w", ErrInvalidLargestFileID, err)
	}
	if !dirInfo.IsDir() {
		return 0, fmt.Errorf("%w: accounts path is not a real directory", ErrInvalidLargestFileID)
	}
	directory, err := os.Open(accountsDir)
	if err != nil {
		return 0, fmt.Errorf("%w: open accounts directory: %w", ErrInvalidLargestFileID, err)
	}
	defer func() { retErr = errors.Join(retErr, directory.Close()) }()
	// Keep only the slot for each global ID: data, final manifest, and parked
	// manifest of the same identity are allowed, while a different slot proves
	// ID reuse. Streaming directory batches avoids os.ReadDir's O(files) name
	// slice and sort on large mainnet stores.
	identities := make(map[uint64]uint64)
	for {
		entries, readErr := directory.ReadDir(4096)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return 0, fmt.Errorf("%w: read accounts directory: %w", ErrInvalidLargestFileID, readErr)
		}
		for _, entry := range entries {
			stem, fileID, recognized, err := canonicalPersistentFileIdentity(entry.Name())
			if err != nil {
				return 0, err
			}
			if !recognized {
				continue
			}
			info, err := os.Lstat(filepath.Join(accountsDir, entry.Name()))
			if err != nil {
				return 0, fmt.Errorf("%w: inspect %s: %w", ErrInvalidLargestFileID, entry.Name(), err)
			}
			if !info.Mode().IsRegular() {
				return 0, fmt.Errorf("%w: canonical artifact %s is not a regular file", ErrInvalidLargestFileID, entry.Name())
			}
			slot, _, _ := parseDataFileName(stem)
			if priorSlot, exists := identities[fileID]; exists && priorSlot != slot {
				return 0, fmt.Errorf(
					"%w: file ID %d is reused by slots %d and %d",
					ErrInvalidLargestFileID, fileID, priorSlot, slot,
				)
			}
			identities[fileID] = slot
			maximum = max(maximum, fileID)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
	}
	return maximum, nil
}

func canonicalPersistentFileIdentity(name string) (string, uint64, bool, error) {
	stem := name
	switch {
	case strings.HasSuffix(name, segManifestRewoundSuffix):
		stem = strings.TrimSuffix(name, segManifestRewoundSuffix)
	case strings.HasSuffix(name, segManifestSuffix):
		stem = strings.TrimSuffix(name, segManifestSuffix)
	case strings.HasSuffix(name, segManifestTmpSuffix):
		// A temporary manifest is not a durable decision and does not reserve an
		// ID independently of its already-published data file.
		return "", 0, false, nil
	}
	slot, fileID, ok := parseDataFileName(stem)
	if !ok {
		return "", 0, false, nil
	}
	canonical := strconv.FormatUint(slot, 10) + "." + strconv.FormatUint(fileID, 10)
	if stem != canonical {
		return "", 0, false, fmt.Errorf("%w: non-canonical numeric artifact name %s", ErrInvalidLargestFileID, name)
	}
	return canonical, fileID, true, nil
}

// allocateFileID durably advances the selector before returning an ID that may
// be used for a data file. It serializes every writer and permanently fences
// this process after a post-rename ambiguity.
func (accountsDb *AccountsDb) allocateFileID() (uint64, error) {
	accountsDb.fileIDMu.Lock()
	defer accountsDb.fileIDMu.Unlock()
	if accountsDb.fileIDFatal != nil {
		return 0, errors.Join(ErrLargestFileIDCommitDecided, accountsDb.fileIDFatal)
	}
	current := accountsDb.LargestFileId.Load()
	if current == math.MaxUint64 {
		return 0, ErrLargestFileIDExhausted
	}
	next := current + 1
	publish := accountsDb.publishLargestFileID
	if publish == nil {
		publish = func(root string, fileID uint64) (bool, error) {
			return writeLargestFileIDAtomic(root, fileID, os.Rename, fsyncDir)
		}
	}
	renamed, err := publish(filepath.Dir(accountsDb.AcctsDir), next)
	if err != nil {
		if renamed {
			accountsDb.fileIDFatal = err
			return 0, errors.Join(ErrLargestFileIDCommitDecided, err)
		}
		return 0, err
	}
	if !renamed {
		accountsDb.fileIDFatal = errors.New("largest-file-ID publisher succeeded without rename")
		return 0, errors.Join(ErrLargestFileIDCommitDecided, accountsDb.fileIDFatal)
	}
	accountsDb.LargestFileId.Store(next)
	return next, nil
}
