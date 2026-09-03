package accountsdb

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
	"golang.org/x/sys/unix"
)

type AccountIndexEntry struct {
	Slot   uint64
	FileId uint64
	Offset uint64
}

func (entry AccountIndexEntry) Marshal(out *[24]byte) {
	binary.LittleEndian.PutUint64(out[0:8], entry.Slot)
	binary.LittleEndian.PutUint64(out[8:16], entry.FileId)
	binary.LittleEndian.PutUint64(out[16:24], entry.Offset)
}

func (entry *AccountIndexEntry) Unmarshal(in *[24]byte) {
	entry.Slot = binary.LittleEndian.Uint64(in[0:8])
	entry.FileId = binary.LittleEndian.Uint64(in[8:16])
	entry.Offset = binary.LittleEndian.Uint64(in[16:24])
}

func UnmarshalAcctIdxEntry(data []byte) (*AccountIndexEntry, error) {
	out, err := UnmarshalAcctIdxEntryValue(data)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func UnmarshalAcctIdxEntryValue(data []byte) (AccountIndexEntry, error) {
	if len(data) < 24 {
		return AccountIndexEntry{}, fmt.Errorf("UnmarshalAcctIdxEntry: input had %d < 24 minimum bytes", len(data))
	}
	out := AccountIndexEntry{}
	out.Unmarshal((*[24]byte)(data[:24]))
	return out, nil
}

// StakeIndexEntry stores a stake account pubkey with its appendvec location hint.
// The location (FileId, Offset) is used for sorting to achieve sequential I/O.
// It may be stale; actual reads still go through the account index for the
// canonical location.
type StakeIndexEntry struct {
	Pubkey solana.PublicKey
	FileId uint64
	Offset uint64
}

// StakeIndexMagic is the magic header for stake pubkey index files.
var StakeIndexMagic = [4]byte{'S', 'T', 'K', 'I'}

const (
	// StakeIndexVersion is an append-safe, checksummed sequence of frames. V1
	// (bare 32-byte pubkeys) and V2 (an 8-byte header followed by unframed
	// records) remain readable and are atomically upgraded on the next append.
	StakeIndexVersion       = uint32(3)
	StakeIndexLegacyVersion = uint32(2)
	StakeIndexRecordSize    = 48 // 32-byte pubkey + 8-byte fileId + 8-byte offset

	stakeIndexFileHeaderSize   = 16
	stakeIndexFrameHeaderSize  = 32
	stakeIndexFrameTrailerSize = 48
	stakeIndexFormatFlags      = uint32(0)
	stakeIndexFrameFormatFlags = uint32(0)
	stakeIndexWriteBufferSize  = 1 << 20
	stakeIndexMaxFrameRecords  = 1 << 16
)

var (
	stakeIndexFrameMagic   = [4]byte{'S', 'T', 'K', 'F'}
	stakeIndexTrailerMagic = [4]byte{'S', 'T', 'K', 'E'}
	stakeIndexCRC32CTable  = crc32.MakeTable(crc32.Castagnoli)

	// ErrStakeIndexCommitDecided means the new complete file was renamed over
	// the canonical path, but syncing the containing directory failed. The old
	// or new file may survive a crash, so callers must not delete or roll back
	// either version based on this error.
	ErrStakeIndexCommitDecided = errors.New("accountsdb: stake index replacement commit decided")
)

// StakeIndexFileInfo describes the on-disk format consumed by
// ReadStakePubkeyIndex. IncompleteTail is possible only for V3: a final frame
// without its validated commit trailer is an uncommitted append and is ignored
// by readers and truncated by the next writer.
type StakeIndexFileInfo struct {
	Version        uint32
	RecordCount    uint64
	FileBytes      int64
	ValidBytes     int64
	IncompleteTail bool
}

type stakeIndexAtomicWriteOps struct {
	createTemp    func(string, string) (*os.File, error)
	rename        func(string, string) error
	syncDirectory func(string) error
}

var defaultStakeIndexAtomicWriteOps = stakeIndexAtomicWriteOps{
	createTemp:    os.CreateTemp,
	rename:        os.Rename,
	syncDirectory: syncStakeIndexDirectory,
}

// WriteStakePubkeyIndex atomically and durably replaces path with a V3 stake
// index. The new inode is completely written, flushed, fsynced and closed
// before rename; the parent directory is fsynced after rename.
func WriteStakePubkeyIndex(path string, entries []StakeIndexEntry) error {
	return writeStakePubkeyIndexAtomic(path, entries, defaultStakeIndexAtomicWriteOps)
}

// AppendStakePubkeyIndex durably appends one committed V3 frame. A legacy V1
// or V2 file is read and atomically upgraded once. A torn V3 tail left by an
// interrupted append is discarded before retrying.
func AppendStakePubkeyIndex(path string, entries []StakeIndexEntry) error {
	if len(entries) == 0 {
		return nil
	}

	pathInfo, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return WriteStakePubkeyIndex(path, entries)
		}
		return fmt.Errorf("inspect stake index before append: %w", err)
	}
	if !pathInfo.Mode().IsRegular() {
		return fmt.Errorf("stake index append target %q is not a regular file", path)
	}

	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open stake index for append: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("open stake index for append returned an invalid descriptor")
	}
	closed := false
	closeFile := func() error {
		if closed {
			return nil
		}
		closed = true
		return file.Close()
	}
	defer closeFile()

	openedInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat opened stake index: %w", err)
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(pathInfo, openedInfo) {
		return fmt.Errorf("stake index changed while opening for append")
	}

	version, err := readStakeIndexVersion(file, openedInfo.Size())
	if err != nil {
		return fmt.Errorf("inspect stake index before append: %w", err)
	}
	if version != StakeIndexVersion {
		current, _, err := readStakePubkeyIndexFromFile(file, openedInfo.Size(), true)
		if err != nil {
			return fmt.Errorf("validate legacy stake index before upgrade: %w", err)
		}
		if err := closeFile(); err != nil {
			return fmt.Errorf("close legacy stake index before upgrade: %w", err)
		}
		upgraded := make([]StakeIndexEntry, 0, len(current)+len(entries))
		upgraded = append(upgraded, current...)
		upgraded = append(upgraded, entries...)
		return WriteStakePubkeyIndex(path, upgraded)
	}

	appendOffset, incompleteTail, err := stakeIndexV3AppendOffset(file, openedInfo.Size())
	if err != nil {
		return fmt.Errorf("validate stake index before append: %w", err)
	}
	if incompleteTail {
		if err := file.Truncate(appendOffset); err != nil {
			return fmt.Errorf("truncate incomplete stake index frame: %w", err)
		}
	}
	if _, err := file.Seek(appendOffset, io.SeekStart); err != nil {
		return fmt.Errorf("seek stake index append position: %w", err)
	}
	buffer := bufio.NewWriterSize(file, stakeIndexBufferSize(len(entries)))
	if err := writeStakeIndexFrames(buffer, entries); err != nil {
		return fmt.Errorf("append stake index frame: %w", err)
	}
	if err := buffer.Flush(); err != nil {
		return fmt.Errorf("flush stake index append: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync stake index append: %w", err)
	}

	// Detect an out-of-process atomic replacement while we were appending. The
	// store-wide production lock normally excludes this, but failing closed is
	// safer than reporting an append to an unlinked inode as durable.
	finalPathInfo, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect stake index after append: %w", err)
	}
	if !finalPathInfo.Mode().IsRegular() || !os.SameFile(openedInfo, finalPathInfo) {
		return fmt.Errorf("stake index was replaced while appending")
	}
	if err := closeFile(); err != nil {
		return fmt.Errorf("close stake index append: %w", err)
	}
	return nil
}

func writeStakePubkeyIndexAtomic(path string, entries []StakeIndexEntry, ops stakeIndexAtomicWriteOps) (retErr error) {
	if path == "" {
		return errors.New("empty stake index path")
	}
	if ops.createTemp == nil || ops.rename == nil || ops.syncDirectory == nil {
		return errors.New("incomplete stake index atomic-write operations")
	}
	directory := filepath.Dir(path)
	base := filepath.Base(path)
	if base == "." || base == string(filepath.Separator) || base == "" {
		return fmt.Errorf("invalid stake index path %q", path)
	}
	directoryInfo, err := os.Lstat(directory)
	if err != nil {
		return fmt.Errorf("inspect stake index directory: %w", err)
	}
	if !directoryInfo.IsDir() {
		return fmt.Errorf("stake index parent %q is not a real directory", directory)
	}
	if targetInfo, statErr := os.Lstat(path); statErr == nil {
		if !targetInfo.Mode().IsRegular() {
			return fmt.Errorf("stake index target %q is not a regular file", path)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("inspect stake index target: %w", statErr)
	}

	// os.CreateTemp uses O_CREATE|O_EXCL and a unique final component, so it
	// cannot follow a pre-existing symlink at the temporary name.
	file, err := ops.createTemp(directory, "."+base+".tmp-")
	if err != nil {
		return fmt.Errorf("create stake index temporary file: %w", err)
	}
	temporaryPath := file.Name()
	published := false
	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, file.Close())
		}
		if !published {
			if cleanupErr := os.Remove(temporaryPath); cleanupErr != nil && !errors.Is(cleanupErr, os.ErrNotExist) {
				retErr = errors.Join(retErr, fmt.Errorf("remove failed stake index temporary file: %w", cleanupErr))
			}
		}
	}()

	if err := file.Chmod(0o644); err != nil {
		return fmt.Errorf("set stake index temporary permissions: %w", err)
	}
	buffer := bufio.NewWriterSize(file, stakeIndexBufferSize(len(entries)))
	if err := writeStakeIndexFileHeader(buffer); err != nil {
		return fmt.Errorf("write stake index header: %w", err)
	}
	if err := writeStakeIndexFrames(buffer, entries); err != nil {
		return fmt.Errorf("write stake index records: %w", err)
	}
	if err := buffer.Flush(); err != nil {
		return fmt.Errorf("flush stake index: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync stake index: %w", err)
	}
	if err := file.Close(); err != nil {
		closed = true
		return fmt.Errorf("close stake index before publication: %w", err)
	}
	closed = true

	if err := ops.rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish stake index: %w", err)
	}
	published = true
	if err := ops.syncDirectory(directory); err != nil {
		return errors.Join(
			ErrStakeIndexCommitDecided,
			fmt.Errorf("sync stake index directory after rename: %w", err),
		)
	}
	return nil
}

func writeStakeIndexFileHeader(writer io.Writer) error {
	var header [stakeIndexFileHeaderSize]byte
	copy(header[0:4], StakeIndexMagic[:])
	binary.LittleEndian.PutUint32(header[4:8], StakeIndexVersion)
	binary.LittleEndian.PutUint32(header[8:12], StakeIndexRecordSize)
	binary.LittleEndian.PutUint32(header[12:16], stakeIndexFormatFlags)
	return writeStakeIndexBytes(writer, header[:])
}

func stakeIndexBufferSize(recordCount int) int {
	const minimumBufferSize = 4 << 10
	if recordCount >= (stakeIndexWriteBufferSize-stakeIndexFrameHeaderSize-stakeIndexFrameTrailerSize)/StakeIndexRecordSize {
		return stakeIndexWriteBufferSize
	}
	size := stakeIndexFrameHeaderSize + stakeIndexFrameTrailerSize + recordCount*StakeIndexRecordSize
	if size < minimumBufferSize {
		return minimumBufferSize
	}
	return size
}

func writeStakeIndexFrames(writer io.Writer, entries []StakeIndexEntry) error {
	if len(entries) == 0 {
		return writeStakeIndexFrame(writer, nil)
	}
	for len(entries) > 0 {
		frameCount := len(entries)
		if frameCount > stakeIndexMaxFrameRecords {
			frameCount = stakeIndexMaxFrameRecords
		}
		if err := writeStakeIndexFrame(writer, entries[:frameCount]); err != nil {
			return err
		}
		entries = entries[frameCount:]
	}
	return nil
}

func writeStakeIndexFrame(writer io.Writer, entries []StakeIndexEntry) error {
	var header [stakeIndexFrameHeaderSize]byte
	copy(header[0:4], stakeIndexFrameMagic[:])
	binary.LittleEndian.PutUint32(header[4:8], stakeIndexFrameFormatFlags)
	count := uint64(len(entries))
	binary.LittleEndian.PutUint64(header[8:16], count)
	binary.LittleEndian.PutUint64(header[16:24], ^count)
	binary.LittleEndian.PutUint32(header[24:28], crc32.Checksum(header[:24], stakeIndexCRC32CTable))
	// Bytes 28..32 are reserved and remain zero.

	hasher := sha256.New()
	if _, err := hasher.Write(header[:]); err != nil {
		return err
	}
	if err := writeStakeIndexBytes(writer, header[:]); err != nil {
		return err
	}

	var record [StakeIndexRecordSize]byte
	for _, entry := range entries {
		copy(record[0:32], entry.Pubkey[:])
		binary.LittleEndian.PutUint64(record[32:40], entry.FileId)
		binary.LittleEndian.PutUint64(record[40:48], entry.Offset)
		if _, err := hasher.Write(record[:]); err != nil {
			return err
		}
		if err := writeStakeIndexBytes(writer, record[:]); err != nil {
			return err
		}
	}

	var trailer [stakeIndexFrameTrailerSize]byte
	copy(trailer[0:4], stakeIndexTrailerMagic[:])
	// Bytes 4..8 are reserved and remain zero.
	binary.LittleEndian.PutUint64(trailer[8:16], count)
	copy(trailer[16:48], hasher.Sum(nil))
	return writeStakeIndexBytes(writer, trailer[:])
}

func writeStakeIndexBytes(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(data) {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func syncStakeIndexDirectory(path string) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	directory := os.NewFile(uintptr(fd), path)
	if directory == nil {
		_ = unix.Close(fd)
		return errors.New("open stake index directory returned an invalid descriptor")
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}

func readStakeIndexVersion(file *os.File, fileSize int64) (uint32, error) {
	if fileSize == 0 {
		return 0, errors.New("stake pubkey index is empty")
	}
	if fileSize < 8 {
		return 1, nil
	}
	var discriminator [8]byte
	if _, err := file.ReadAt(discriminator[:], 0); err != nil {
		return 0, err
	}
	if !bytes.Equal(discriminator[:4], StakeIndexMagic[:]) {
		return 1, nil
	}
	return binary.LittleEndian.Uint32(discriminator[4:8]), nil
}

// stakeIndexV3AppendOffset keeps the steady-state append cost proportional to
// the most recent frame, not the complete historical file. A normal EOF is
// authenticated backwards from its commit trailer. Only an abnormal/torn EOF
// requires a forward scan to locate the last committed frame.
func stakeIndexV3AppendOffset(file *os.File, fileSize int64) (int64, bool, error) {
	if fileSize < stakeIndexFileHeaderSize {
		return 0, false, fmt.Errorf("stake pubkey index V3 header is truncated at %d bytes", fileSize)
	}
	var fileHeader [stakeIndexFileHeaderSize]byte
	if _, err := file.ReadAt(fileHeader[:], 0); err != nil {
		return 0, false, err
	}
	if !bytes.Equal(fileHeader[:4], StakeIndexMagic[:]) ||
		binary.LittleEndian.Uint32(fileHeader[4:8]) != StakeIndexVersion ||
		binary.LittleEndian.Uint32(fileHeader[8:12]) != StakeIndexRecordSize ||
		binary.LittleEndian.Uint32(fileHeader[12:16]) != stakeIndexFormatFlags {
		return 0, false, errors.New("stake pubkey index V3 has invalid header fields")
	}
	if fileSize == stakeIndexFileHeaderSize {
		return fileSize, false, nil
	}

	if fileSize >= stakeIndexFileHeaderSize+stakeIndexFrameHeaderSize+stakeIndexFrameTrailerSize {
		var trailer [stakeIndexFrameTrailerSize]byte
		if _, err := file.ReadAt(trailer[:], fileSize-stakeIndexFrameTrailerSize); err != nil {
			return 0, false, err
		}
		if bytes.Equal(trailer[:4], stakeIndexTrailerMagic[:]) &&
			binary.LittleEndian.Uint32(trailer[4:8]) == 0 {
			count := binary.LittleEndian.Uint64(trailer[8:16])
			maxPayload := uint64(fileSize - stakeIndexFileHeaderSize - stakeIndexFrameHeaderSize - stakeIndexFrameTrailerSize)
			if count <= maxPayload/StakeIndexRecordSize {
				frameBytes := int64(stakeIndexFrameHeaderSize+stakeIndexFrameTrailerSize) + int64(count)*StakeIndexRecordSize
				frameOffset := fileSize - frameBytes
				var frameHeader [stakeIndexFrameHeaderSize]byte
				if _, err := file.ReadAt(frameHeader[:], frameOffset); err != nil {
					return 0, false, err
				}
				if bytes.Equal(frameHeader[:4], stakeIndexFrameMagic[:]) &&
					binary.LittleEndian.Uint32(frameHeader[4:8]) == stakeIndexFrameFormatFlags &&
					binary.LittleEndian.Uint64(frameHeader[8:16]) == count &&
					binary.LittleEndian.Uint64(frameHeader[16:24]) == ^count &&
					binary.LittleEndian.Uint32(frameHeader[24:28]) == crc32.Checksum(frameHeader[:24], stakeIndexCRC32CTable) &&
					binary.LittleEndian.Uint32(frameHeader[28:32]) == 0 {
					hasher := sha256.New()
					sectionBytes := frameBytes - stakeIndexFrameTrailerSize
					copyBufferSize := sectionBytes
					if copyBufferSize > stakeIndexWriteBufferSize {
						copyBufferSize = stakeIndexWriteBufferSize
					}
					copied, err := io.CopyBuffer(
						hasher,
						io.NewSectionReader(file, frameOffset, sectionBytes),
						make([]byte, int(copyBufferSize)),
					)
					if err != nil {
						return 0, false, err
					}
					if copied != sectionBytes {
						return 0, false, fmt.Errorf("short read validating final stake index frame: %d of %d", copied, sectionBytes)
					}
					if !bytes.Equal(trailer[16:48], hasher.Sum(nil)) {
						return 0, false, errors.New("stake pubkey index V3 final frame checksum mismatch")
					}
					return fileSize, false, nil
				}
			}
		}
	}

	// The EOF is not a committed trailer. Forward validation distinguishes a
	// normal torn append (recoverable) from corruption in a committed frame.
	_, format, err := readStakePubkeyIndexFromFile(file, fileSize, false)
	if err != nil {
		return 0, false, err
	}
	if !format.IncompleteTail {
		return 0, false, errors.New("stake pubkey index V3 has no valid final commit trailer")
	}
	return format.ValidBytes, true, nil
}

// ReadStakePubkeyIndex reads and validates all records without following a
// final-component symlink. Legacy V1/V2 files are accepted read-only. V3 frame
// counts, header CRCs, commit trailers and SHA-256 checksums are validated.
func ReadStakePubkeyIndex(path string) ([]StakeIndexEntry, StakeIndexFileInfo, error) {
	file, openedInfo, err := openStableRegularFile(path)
	if err != nil {
		return nil, StakeIndexFileInfo{}, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()

	entries, format, err := readStakePubkeyIndexFromFile(file, openedInfo.Size(), true)
	if err != nil {
		return nil, StakeIndexFileInfo{}, err
	}
	if err := validateStableRegularFile(file, path, openedInfo); err != nil {
		return nil, StakeIndexFileInfo{}, err
	}
	if err := file.Close(); err != nil {
		closed = true
		return nil, StakeIndexFileInfo{}, err
	}
	closed = true
	return entries, format, nil
}

// ValidateStakePubkeyIndex validates the complete committed prefix of a stake
// index without retaining its records in memory. A torn final V3 frame is an
// uncommitted WAL tail and is permitted; every committed frame is checksummed.
func ValidateStakePubkeyIndex(path string) (StakeIndexFileInfo, error) {
	file, openedInfo, err := openStableRegularFile(path)
	if err != nil {
		return StakeIndexFileInfo{}, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()

	_, format, err := readStakePubkeyIndexFromFile(file, openedInfo.Size(), false)
	if err != nil {
		return StakeIndexFileInfo{}, err
	}
	if format.RecordCount == 0 {
		return StakeIndexFileInfo{}, errors.New("stake pubkey index contains no committed records")
	}
	if err := validateStableRegularFile(file, path, openedInfo); err != nil {
		return StakeIndexFileInfo{}, err
	}
	if err := file.Close(); err != nil {
		closed = true
		return StakeIndexFileInfo{}, err
	}
	closed = true
	return format, nil
}

func readStakePubkeyIndexFromFile(file *os.File, fileSize int64, collect bool) ([]StakeIndexEntry, StakeIndexFileInfo, error) {
	format := StakeIndexFileInfo{FileBytes: fileSize}
	if fileSize < 0 {
		return nil, format, errors.New("stake pubkey index has a negative size")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, format, err
	}
	if fileSize == 0 {
		return nil, format, errors.New("stake pubkey index is empty")
	}

	var discriminator [8]byte
	if fileSize < int64(len(discriminator)) {
		return readStakeIndexV1(file, fileSize, collect)
	}
	if _, err := io.ReadFull(file, discriminator[:]); err != nil {
		return nil, format, err
	}
	if !bytes.Equal(discriminator[:4], StakeIndexMagic[:]) {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return nil, format, err
		}
		return readStakeIndexV1(file, fileSize, collect)
	}

	version := binary.LittleEndian.Uint32(discriminator[4:8])
	switch version {
	case StakeIndexLegacyVersion:
		return readStakeIndexV2(file, fileSize, collect)
	case StakeIndexVersion:
		return readStakeIndexV3(file, fileSize, discriminator, collect)
	default:
		return nil, format, fmt.Errorf("stake pubkey index: unsupported version %d", version)
	}
}

func readStakeIndexV1(file *os.File, fileSize int64, collect bool) ([]StakeIndexEntry, StakeIndexFileInfo, error) {
	format := StakeIndexFileInfo{Version: 1, FileBytes: fileSize, ValidBytes: fileSize}
	if fileSize%32 != 0 {
		return nil, format, fmt.Errorf("stake pubkey index V1 corrupt: length %d not multiple of 32", fileSize)
	}
	count := uint64(fileSize / 32)
	format.RecordCount = count
	if !stakeIndexCountFitsInt(count) {
		return nil, format, fmt.Errorf("stake pubkey index V1 has too many records: %d", count)
	}
	var entries []StakeIndexEntry
	if collect {
		entries = make([]StakeIndexEntry, int(count))
	}
	var pubkey [32]byte
	for i := uint64(0); i < count; i++ {
		if _, err := io.ReadFull(file, pubkey[:]); err != nil {
			return nil, format, err
		}
		if collect {
			copy(entries[int(i)].Pubkey[:], pubkey[:])
		}
	}
	return entries, format, nil
}

func readStakeIndexV2(file *os.File, fileSize int64, collect bool) ([]StakeIndexEntry, StakeIndexFileInfo, error) {
	format := StakeIndexFileInfo{Version: StakeIndexLegacyVersion, FileBytes: fileSize, ValidBytes: fileSize}
	const headerSize = int64(8)
	if fileSize < headerSize || (fileSize-headerSize)%StakeIndexRecordSize != 0 {
		return nil, format, fmt.Errorf("stake pubkey index V2 corrupt: size %d is not header plus complete records", fileSize)
	}
	count := uint64((fileSize - headerSize) / StakeIndexRecordSize)
	format.RecordCount = count
	if !stakeIndexCountFitsInt(count) {
		return nil, format, fmt.Errorf("stake pubkey index V2 has too many records: %d", count)
	}
	return readStakeIndexRecords(file, count, collect, format)
}

func readStakeIndexV3(file *os.File, fileSize int64, discriminator [8]byte, collect bool) ([]StakeIndexEntry, StakeIndexFileInfo, error) {
	format := StakeIndexFileInfo{Version: StakeIndexVersion, FileBytes: fileSize}
	if fileSize < stakeIndexFileHeaderSize {
		return nil, format, fmt.Errorf("stake pubkey index V3 header is truncated at %d bytes", fileSize)
	}
	var header [stakeIndexFileHeaderSize]byte
	copy(header[:8], discriminator[:])
	if _, err := io.ReadFull(file, header[8:]); err != nil {
		return nil, format, err
	}
	if binary.LittleEndian.Uint32(header[8:12]) != StakeIndexRecordSize ||
		binary.LittleEndian.Uint32(header[12:16]) != stakeIndexFormatFlags {
		return nil, format, errors.New("stake pubkey index V3 has invalid header fields")
	}

	format.ValidBytes = stakeIndexFileHeaderSize
	remaining := fileSize - stakeIndexFileHeaderSize
	var entries []StakeIndexEntry
	if collect {
		// Frame overhead makes this a small upper bound. Preallocating once avoids
		// transient 2x backing arrays while loading a large snapshot index.
		estimatedCount := uint64(remaining / StakeIndexRecordSize)
		if !stakeIndexCountFitsInt(estimatedCount) {
			return nil, format, fmt.Errorf("stake pubkey index V3 is too large to load: %d bytes", fileSize)
		}
		entries = make([]StakeIndexEntry, 0, int(estimatedCount))
	}
	for remaining > 0 {
		if remaining < stakeIndexFrameHeaderSize {
			format.IncompleteTail = true
			break
		}

		var frameHeader [stakeIndexFrameHeaderSize]byte
		if _, err := io.ReadFull(file, frameHeader[:]); err != nil {
			return nil, format, err
		}
		if !bytes.Equal(frameHeader[:4], stakeIndexFrameMagic[:]) ||
			binary.LittleEndian.Uint32(frameHeader[4:8]) != stakeIndexFrameFormatFlags ||
			binary.LittleEndian.Uint32(frameHeader[28:32]) != 0 {
			return nil, format, errors.New("stake pubkey index V3 has an invalid frame header")
		}
		count := binary.LittleEndian.Uint64(frameHeader[8:16])
		if binary.LittleEndian.Uint64(frameHeader[16:24]) != ^count ||
			binary.LittleEndian.Uint32(frameHeader[24:28]) != crc32.Checksum(frameHeader[:24], stakeIndexCRC32CTable) {
			return nil, format, errors.New("stake pubkey index V3 frame header checksum mismatch")
		}

		remainingAfterHeader := remaining - stakeIndexFrameHeaderSize
		if remainingAfterHeader < stakeIndexFrameTrailerSize ||
			count > uint64((remainingAfterHeader-stakeIndexFrameTrailerSize)/StakeIndexRecordSize) {
			format.IncompleteTail = true
			break
		}
		if count > uint64((^uint(0)>>1))-format.RecordCount {
			return nil, format, fmt.Errorf("stake pubkey index V3 record count overflows memory: %d", count)
		}

		hasher := sha256.New()
		_, _ = hasher.Write(frameHeader[:])
		frameStart := len(entries)
		if collect {
			entries = append(entries, make([]StakeIndexEntry, int(count))...)
		}
		var record [StakeIndexRecordSize]byte
		for i := uint64(0); i < count; i++ {
			if _, err := io.ReadFull(file, record[:]); err != nil {
				return nil, format, err
			}
			_, _ = hasher.Write(record[:])
			if collect {
				unmarshalStakeIndexRecord(record[:], &entries[frameStart+int(i)])
			}
		}

		var trailer [stakeIndexFrameTrailerSize]byte
		if _, err := io.ReadFull(file, trailer[:]); err != nil {
			return nil, format, err
		}
		if !bytes.Equal(trailer[:4], stakeIndexTrailerMagic[:]) ||
			binary.LittleEndian.Uint32(trailer[4:8]) != 0 ||
			binary.LittleEndian.Uint64(trailer[8:16]) != count {
			return nil, format, errors.New("stake pubkey index V3 has an invalid frame commit trailer")
		}
		if !bytes.Equal(trailer[16:48], hasher.Sum(nil)) {
			return nil, format, errors.New("stake pubkey index V3 frame checksum mismatch")
		}

		frameBytes := int64(stakeIndexFrameHeaderSize+stakeIndexFrameTrailerSize) + int64(count)*StakeIndexRecordSize
		format.ValidBytes += frameBytes
		format.RecordCount += count
		remaining -= frameBytes
	}
	return entries, format, nil
}

func readStakeIndexRecords(file *os.File, count uint64, collect bool, format StakeIndexFileInfo) ([]StakeIndexEntry, StakeIndexFileInfo, error) {
	var entries []StakeIndexEntry
	if collect {
		entries = make([]StakeIndexEntry, int(count))
	}
	var record [StakeIndexRecordSize]byte
	for i := uint64(0); i < count; i++ {
		if _, err := io.ReadFull(file, record[:]); err != nil {
			return nil, format, err
		}
		if collect {
			unmarshalStakeIndexRecord(record[:], &entries[int(i)])
		}
	}
	return entries, format, nil
}

func unmarshalStakeIndexRecord(record []byte, entry *StakeIndexEntry) {
	copy(entry.Pubkey[:], record[0:32])
	entry.FileId = binary.LittleEndian.Uint64(record[32:40])
	entry.Offset = binary.LittleEndian.Uint64(record[40:48])
}

func stakeIndexCountFitsInt(count uint64) bool {
	return count <= uint64(^uint(0)>>1)
}

// BuildIndexEntriesFromAppendVecs parses an appendvec and returns:
// - pubkeys: all account pubkeys
// - acctIdxEntries: index entries for each account
// - stakeEntries: stake account pubkeys with their appendvec location hints
func BuildIndexEntriesFromAppendVecs(data []byte, fileSize uint64, slot uint64, fileId uint64) ([]solana.PublicKey, []AccountIndexEntry, []StakeIndexEntry, error) {
	pubkeys := make([]solana.PublicKey, 0, 20000)
	acctIdxEntries := make([]AccountIndexEntry, 0, 20000)
	stakeEntries := make([]StakeIndexEntry, 0, 1000)
	parser := &appendVecParser{Buf: data, FileSize: fileSize, FileId: fileId, Slot: slot}

	var owner solana.PublicKey
	for {
		pubkeys = append(pubkeys, solana.PublicKey{})
		acctIdxEntries = append(acctIdxEntries, AccountIndexEntry{})
		err := parser.ParseNextAcctWithOwner(&pubkeys[len(pubkeys)-1], &acctIdxEntries[len(acctIdxEntries)-1], &owner)
		if err != nil {
			pubkeys = pubkeys[:len(pubkeys)-1]
			acctIdxEntries = acctIdxEntries[:len(acctIdxEntries)-1]
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, nil, nil, fmt.Errorf("parse appendvec slot=%d file_id=%d: %w", slot, fileId, err)
		}
		// Collect stake account entries with appendvec location hints
		if bytes.Equal(owner[:], addresses.StakeProgramAddr[:]) {
			idx := len(acctIdxEntries) - 1
			stakeEntries = append(stakeEntries, StakeIndexEntry{
				Pubkey: pubkeys[len(pubkeys)-1],
				FileId: acctIdxEntries[idx].FileId,
				Offset: acctIdxEntries[idx].Offset,
			})
		}
	}

	return pubkeys, acctIdxEntries, stakeEntries, nil
}
