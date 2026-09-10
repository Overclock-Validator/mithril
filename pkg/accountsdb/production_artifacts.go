package accountsdb

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const productionJournalValidationBufferSize = 256 << 10

// IsProductionAccountIndexGenerationDirectory reports whether name has the
// complete generation-and-nonce shape used by an immutable V2 base build. It
// is exported for snapshot rebuild cleanup; prefix matching alone would make
// recursive deletion unnecessarily broad.
func IsProductionAccountIndexGenerationDirectory(name string) bool {
	return isShardedBaseGenerationDirectoryName(name)
}

// LegacyAccountIndexFormat names an on-disk index format that cannot be
// opened as AccountsDB V2. Conversion or a fresh snapshot is an explicit
// operator action; silently ignoring one of these artifacts is unsafe.
type LegacyAccountIndexFormat string

const (
	LegacyAccountIndexPebble LegacyAccountIndexFormat = "pebble"
	LegacyAccountIndexV1     LegacyAccountIndexFormat = "streamhash-v1"
)

// AccountIndexMigrationError reports the precise incompatible format and
// paths found by startup validation. It unwraps to
// ErrAccountIndexMigrationRequired so callers can select policy with
// errors.Is while retaining actionable diagnostics with errors.As.
type AccountIndexMigrationError struct {
	Format    LegacyAccountIndexFormat
	Artifacts []string
}

func (err *AccountIndexMigrationError) Error() string {
	if err == nil {
		return ErrAccountIndexMigrationRequired.Error()
	}
	return fmt.Sprintf(
		"%s: found legacy %s artifact(s) %v; rebuild from a fresh snapshot or run an explicit offline converter",
		ErrAccountIndexMigrationRequired,
		err.Format,
		err.Artifacts,
	)
}

func (err *AccountIndexMigrationError) Unwrap() error {
	return ErrAccountIndexMigrationRequired
}

// ValidateProductionAccountIndexArtifacts validates an AccountsDB V2 root
// without mutating it. It verifies the CRC-protected root selector, complete
// identities and internal metadata of every selected immutable artifact, and
// every complete frame of the mutable journal. It recognizes the same narrow
// repairable trailing partial write as runtime recovery, but never mutates it;
// compact-state tears, interior corruption, and complete bad frames fail
// closed. OpenProductionAccountIndex later repairs an accepted tail under the
// exclusive WAL lock.
func ValidateProductionAccountIndexArtifacts(root string) (retErr error) {
	if root == "" {
		return productionArtifactsPoisoned("empty AccountsDB root", nil)
	}
	storeLock, err := acquireProductionAccountIndexStoreLock(root, false)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, storeLock.Close()) }()
	if migrationErr, err := detectLegacyAccountIndexArtifacts(root); err != nil {
		return productionArtifactsPoisoned("inspect legacy account-index artifacts", err)
	} else if migrationErr != nil {
		return migrationErr
	}

	catalog, err := ReadRootIndexCatalog(root)
	if err != nil {
		return productionArtifactsPoisoned("validate root catalog", err)
	}
	immutable, err := OpenShardedImmutableIndex(root, catalog)
	if err != nil {
		return productionArtifactsPoisoned("validate root-selected immutable artifacts", err)
	}
	if err := immutable.closeUnmanaged(); err != nil {
		return productionArtifactsPoisoned("close validated immutable artifacts", err)
	}
	if err := validateShardedMutableJournalReadOnly(root, catalog); err != nil {
		return productionArtifactsPoisoned("validate mutable journal", err)
	}
	return nil
}

func productionArtifactsPoisoned(operation string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrProductionAccountIndexPoisoned, operation)
	}
	return fmt.Errorf("%w: %s: %v", ErrProductionAccountIndexPoisoned, operation, cause)
}

func detectLegacyAccountIndexArtifacts(root string) (*AccountIndexMigrationError, error) {
	formats := []struct {
		format LegacyAccountIndexFormat
		names  []string
	}{
		{
			format: LegacyAccountIndexPebble,
			names:  []string{"mithril_db", "mithril_db_log_shards"},
		},
		{
			format: LegacyAccountIndexV1,
			names: []string{
				DeltaIndexJournalFileName,
				DeltaIndexJournalRewriteFileName,
				StreamIndexFileName,
				StreamIndexFileName + ".partial",
				StreamIndexManifestFileName,
				StreamIndexManifestFileName + ".tmp",
			},
		},
	}
	for _, candidate := range formats {
		found := make([]string, 0, len(candidate.names))
		for _, name := range candidate.names {
			path := filepath.Join(root, name)
			if _, err := os.Lstat(path); err == nil {
				found = append(found, path)
			} else if !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("stat %s: %w", path, err)
			}
		}
		if len(found) != 0 {
			return &AccountIndexMigrationError{Format: candidate.format, Artifacts: found}, nil
		}
	}
	legacyCheckpoints, err := filepath.Glob(filepath.Join(root, DeltaCheckpointFilePrefix+"*"))
	if err != nil {
		return nil, fmt.Errorf("match legacy delta checkpoints: %w", err)
	}
	if len(legacyCheckpoints) != 0 {
		return &AccountIndexMigrationError{
			Format:    LegacyAccountIndexV1,
			Artifacts: legacyCheckpoints,
		}, nil
	}
	return nil, nil
}

// validateShardedMutableJournalReadOnly intentionally mirrors the journal
// decoder's format checks without materializing mutations and without taking
// a write lock or truncating a repairable torn tail. Its memory use is constant
// even for a multi-gigabyte or sparse corrupt file.
func validateShardedMutableJournalReadOnly(root string, catalog *RootIndexCatalog) (retErr error) {
	if catalog == nil {
		return errors.New("nil root catalog")
	}
	path := filepath.Join(root, ShardedDeltaIndexJournalFileName)
	file, openedInfo, err := openStableRegularFile(path)
	if err != nil {
		return fmt.Errorf("open sharded mutable journal: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, file.Close())
	}()
	if err := unix.Flock(int(file.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		return fmt.Errorf("lock sharded mutable journal for validation: %w", err)
	}
	// Recheck after flock so a path replacement cannot slip between the stable
	// no-follow open and lock acquisition.
	if err := validateStableRegularFile(file, path, openedInfo); err != nil {
		return fmt.Errorf("sharded mutable journal changed while locking: %w", err)
	}
	fileSize := openedInfo.Size()
	if fileSize < deltaJournalHeaderSize {
		return fmt.Errorf("sharded mutable journal length %d is shorter than its header", fileSize)
	}

	var journalHeader [deltaJournalHeaderSize]byte
	if err := readFullAt(file, journalHeader[:], 0); err != nil {
		return fmt.Errorf("read sharded mutable journal header: %w", err)
	}
	if !bytes.Equal(journalHeader[:8], deltaJournalMagic[:]) {
		return fmt.Errorf("invalid sharded mutable journal magic %x", journalHeader[:8])
	}
	if version := uint32FromLE(journalHeader[8:12]); version != deltaJournalVersion {
		return fmt.Errorf("unsupported sharded mutable journal version %d", version)
	}
	if size := uint32FromLE(journalHeader[12:16]); size != deltaJournalHeaderSize {
		return fmt.Errorf("invalid sharded mutable journal header size %d", size)
	}
	if got, want := checksumDelta(journalHeader[:28]), uint32FromLE(journalHeader[28:32]); got != want {
		return fmt.Errorf("sharded mutable journal header CRC mismatch: got %08x want %08x", got, want)
	}

	lastSequence := uint64FromLE(journalHeader[16:24])
	stateFrameCount := uint32FromLE(journalHeader[24:28])
	stateFramesSeen := uint32(0)
	offset := int64(deltaJournalHeaderSize)
	var frameHeader [deltaFrameHeaderSize]byte
	payloadBuffer := make([]byte, productionJournalValidationBufferSize)
	for offset < fileSize {
		remainingFile := fileSize - offset
		if remainingFile < deltaFrameHeaderSize {
			if stateFramesSeen < stateFrameCount {
				return fmt.Errorf(
					"compact sharded mutable state is torn after %d of %d frames",
					stateFramesSeen,
					stateFrameCount,
				)
			}
			break
		}
		if err := readFullAt(file, frameHeader[:], offset); err != nil {
			return fmt.Errorf("read sharded mutable frame header at %d: %w", offset, err)
		}
		if !bytes.Equal(frameHeader[:8], deltaFrameMagic[:]) {
			return fmt.Errorf("invalid sharded mutable frame magic at %d", offset)
		}
		if version := uint32FromLE(frameHeader[8:12]); version != deltaJournalVersion {
			return fmt.Errorf("unsupported sharded mutable frame version %d at %d", version, offset)
		}
		flags := uint32FromLE(frameHeader[12:16])
		if flags&^(deltaFrameHasFoldMeta|deltaFrameIsState) != 0 {
			return fmt.Errorf("sharded mutable frame at %d has unknown flags %#x", offset, flags)
		}
		count := uint32FromLE(frameHeader[56:60])
		frameLength := uint64FromLE(frameHeader[16:24])
		wantLength := uint64(deltaFrameHeaderSize) + uint64(count)*deltaMutationSize
		if frameLength != wantLength || frameLength > ShardedMutableAbsoluteMaxFrameBytes {
			return fmt.Errorf(
				"invalid sharded mutable frame length %d for %d mutations at %d",
				frameLength, count, offset,
			)
		}
		if frameLength > uint64(remainingFile) {
			if stateFramesSeen < stateFrameCount {
				return fmt.Errorf(
					"compact sharded mutable state frame %d of %d is torn",
					stateFramesSeen+1,
					stateFrameCount,
				)
			}
			laterMagic, scanErr := journalHasLaterFrameMagic(file, offset+deltaFrameHeaderSize, fileSize)
			if scanErr != nil {
				return scanErr
			}
			if laterMagic {
				return fmt.Errorf("corrupt interior sharded mutable frame length at %d", offset)
			}
			break
		}
		sequence := uint64FromLE(frameHeader[24:32])
		if lastSequence == ^uint64(0) || sequence != lastSequence+1 {
			return fmt.Errorf("sharded mutable frame sequence %d does not follow %d", sequence, lastSequence)
		}
		expectState := stateFramesSeen < stateFrameCount
		isState := flags&deltaFrameIsState != 0
		if isState != expectState {
			return fmt.Errorf("sharded mutable frame %d state flag=%t, want %t", sequence, isState, expectState)
		}
		if isState {
			stateFramesSeen++
		}

		crc := crc32.Update(0, deltaCRC, frameHeader[:60])
		payloadOffset := offset + deltaFrameHeaderSize
		payloadRemaining := frameLength - deltaFrameHeaderSize
		mutationOrdinal := uint64(0)
		for payloadRemaining != 0 {
			chunkLength := min(uint64(len(payloadBuffer)), payloadRemaining)
			chunk := payloadBuffer[:int(chunkLength)]
			if err := readFullAt(file, chunk, payloadOffset); err != nil {
				return fmt.Errorf("read sharded mutable frame %d payload: %w", sequence, err)
			}
			crc = crc32.Update(crc, deltaCRC, chunk)
			for recordOffset := 0; recordOffset < len(chunk); recordOffset += deltaMutationSize {
				record := chunk[recordOffset : recordOffset+deltaMutationSize]
				if err := validateDeltaMutationRecord(record); err != nil {
					return fmt.Errorf("sharded mutable frame %d mutation %d: %w", sequence, mutationOrdinal, err)
				}
				mutationOrdinal++
			}
			payloadOffset += int64(chunkLength)
			payloadRemaining -= chunkLength
		}
		if got, want := crc, uint32FromLE(frameHeader[60:64]); got != want {
			return fmt.Errorf("sharded mutable frame CRC mismatch at %d: got %08x want %08x", offset, got, want)
		}
		lastSequence = sequence
		offset += int64(frameLength)
	}
	if stateFramesSeen != stateFrameCount {
		return fmt.Errorf("compact sharded mutable state has %d of %d frames", stateFramesSeen, stateFrameCount)
	}
	maximumCoverage := uint64(0)
	for i := range catalog.Shards {
		maximumCoverage = max(maximumCoverage, catalog.Shards[i].effectiveCoveredSequence())
	}
	if lastSequence < maximumCoverage {
		return fmt.Errorf(
			"sharded mutable journal tail %d precedes maximum shard coverage %d",
			lastSequence, maximumCoverage,
		)
	}
	if err := validateStableRegularFile(file, path, openedInfo); err != nil {
		return fmt.Errorf("sharded mutable journal changed while validating: %w", err)
	}
	return nil
}

func validateDeltaMutationRecord(record []byte) error {
	if len(record) != deltaMutationSize {
		return fmt.Errorf("record has %d bytes, want %d", len(record), deltaMutationSize)
	}
	for _, reserved := range record[57:64] {
		if reserved != 0 {
			return errors.New("non-zero reserved bytes")
		}
	}
	switch kind := record[56]; kind {
	case deltaMutationLive, deltaMutationTombstone:
		return nil
	case deltaMutationRetire:
		if !allZero(record[:32]) {
			return errors.New("retired appendvec mutation has a non-zero pubkey")
		}
		return nil
	default:
		return fmt.Errorf("invalid mutation kind %d", kind)
	}
}

func readFullAt(file *os.File, destination []byte, offset int64) error {
	n, err := file.ReadAt(destination, offset)
	if err != nil && !(errors.Is(err, io.EOF) && n == len(destination)) {
		return err
	}
	if n != len(destination) {
		return io.ErrUnexpectedEOF
	}
	return nil
}
