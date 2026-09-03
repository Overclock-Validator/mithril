package snapshot

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"golang.org/x/sys/unix"
)

const maximumSolanaAppendVecFileSize = uint64(16 << 30)

type snapshotAppendVecKey struct {
	slot   uint64
	fileID uint64
}

// expectedSnapshotAppendVecs turns the manifest's AccountsDB storage table
// into the exact set of appendvec members that the corresponding archive must
// contain.  Building this once also prevents an incremental archive from
// falling back to a same-named file in the full manifest.
func expectedSnapshotAppendVecs(manifest *SnapshotManifest) (map[snapshotAppendVecKey]uint64, error) {
	if manifest == nil || manifest.AccountsDb == nil {
		return nil, errors.New("snapshot manifest has no AccountsDB storage table")
	}

	expected := make(map[snapshotAppendVecKey]uint64)
	for storageSlot, storage := range manifest.AccountsDb.Storages {
		if storage.Slot != storageSlot {
			return nil, fmt.Errorf(
				"snapshot manifest storage key %d does not match embedded slot %d",
				storageSlot, storage.Slot,
			)
		}
		// Modern Agave snapshot restore requires exactly one storage entry for
		// every slot represented in this table.  Accepting several appendvecs
		// for a slot would require the legacy write-version winner semantics;
		// FileID order is not an equivalent substitute.  Alpenglow snapshots
		// use the modern one-storage-per-slot format, so fail closed instead of
		// silently constructing a potentially different account state.
		if len(storage.AcctVecs) != 1 {
			return nil, fmt.Errorf(
				"snapshot manifest storage slot=%d contains %d appendvecs; production bootstrap requires exactly one",
				storageSlot, len(storage.AcctVecs),
			)
		}
		for _, appendVec := range storage.AcctVecs {
			if appendVec.FileSize == 0 {
				return nil, fmt.Errorf(
					"snapshot manifest appendvec slot=%d file_id=%d has zero size",
					storageSlot, appendVec.Id,
				)
			}
			if appendVec.FileSize > maximumSolanaAppendVecFileSize {
				return nil, fmt.Errorf(
					"snapshot manifest appendvec slot=%d file_id=%d size %d exceeds Solana maximum %d",
					storageSlot, appendVec.Id, appendVec.FileSize, maximumSolanaAppendVecFileSize,
				)
			}
			key := snapshotAppendVecKey{slot: storageSlot, fileID: appendVec.Id}
			if _, exists := expected[key]; exists {
				return nil, fmt.Errorf(
					"snapshot manifest contains duplicate appendvec slot=%d file_id=%d",
					storageSlot, appendVec.Id,
				)
			}
			expected[key] = appendVec.FileSize
		}
	}
	return expected, nil
}

// snapshotVerificationAppendVecs returns the complete physical input set for
// independently verifying the final state of a full snapshot, optionally
// overlaid by its incremental snapshot.  The returned order is deterministic
// so verification and diagnostics are repeatable.
func snapshotVerificationAppendVecs(
	full *SnapshotManifest,
	incremental *SnapshotManifest,
) ([]accountsdb.SnapshotAppendVecSpec, error) {
	type manifestRole struct {
		name     string
		manifest *SnapshotManifest
	}
	roles := []manifestRole{{name: "full", manifest: full}}
	if incremental != nil {
		roles = append(roles, manifestRole{name: "incremental", manifest: incremental})
	}

	var specs []accountsdb.SnapshotAppendVecSpec
	seenSlots := make(map[uint64]string)
	seenFileIDs := make(map[uint64]snapshotAppendVecKey)
	for _, role := range roles {
		expected, err := expectedSnapshotAppendVecs(role.manifest)
		if err != nil {
			return nil, fmt.Errorf("validate %s snapshot storage table: %w", role.name, err)
		}
		for key, fileSize := range expected {
			if previousRole, exists := seenSlots[key.slot]; exists {
				return nil, fmt.Errorf(
					"snapshot pair represents storage slot %d in both %s and %s manifests",
					key.slot, previousRole, role.name,
				)
			}
			if previous, exists := seenFileIDs[key.fileID]; exists {
				return nil, fmt.Errorf(
					"snapshot pair reuses appendvec file ID %d for slots %d and %d",
					key.fileID, previous.slot, key.slot,
				)
			}
			seenSlots[key.slot] = role.name
			seenFileIDs[key.fileID] = key
			specs = append(specs, accountsdb.SnapshotAppendVecSpec{
				Slot:     key.slot,
				FileID:   key.fileID,
				FileSize: fileSize,
			})
		}
	}
	sort.Slice(specs, func(i, j int) bool {
		if specs[i].Slot != specs[j].Slot {
			return specs[i].Slot < specs[j].Slot
		}
		return specs[i].FileID < specs[j].FileID
	})
	return specs, nil
}

func firstMissingSnapshotAppendVec(expected map[snapshotAppendVecKey]uint64) snapshotAppendVecKey {
	var first snapshotAppendVecKey
	haveFirst := false
	for key := range expected {
		if !haveFirst || key.slot < first.slot || key.slot == first.slot && key.fileID < first.fileID {
			first = key
			haveFirst = true
		}
	}
	return first
}

// snapshotArchiveIdentity returns the filename portion that cryptographically
// identifies a Solana snapshot archive.  Mirrors may use different hosts or
// query credentials, but an incremental retry must never silently switch to a
// different slot/hash after files from the first archive have been published.
func snapshotArchiveIdentity(location string) (string, error) {
	archivePath := location
	parsed, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("parse snapshot location %q: %w", location, err)
	}
	if parsed.Scheme != "" {
		archivePath = parsed.Path
	}
	identity := pathpkg.Base(archivePath)
	if archivePath == "" || identity == "." || identity == "/" || identity == "" {
		return "", fmt.Errorf("snapshot location %q has no archive filename", location)
	}
	return identity, nil
}

func trimSnapshotArchiveExtension(identity string) (string, error) {
	for _, extension := range []string{".tar.zst", ".tar.lz4"} {
		if strings.HasSuffix(identity, extension) {
			return strings.TrimSuffix(identity, extension), nil
		}
	}
	return "", fmt.Errorf("snapshot archive %q has no supported .tar.zst or .tar.lz4 extension", identity)
}

func parseFullSnapshotArchiveSlot(location string) (uint64, error) {
	slot, _, err := parseFullSnapshotArchiveIdentity(location)
	return slot, err
}

func parseFullSnapshotArchiveIdentity(location string) (slot uint64, snapshotHash solana.Hash, err error) {
	identity, err := snapshotArchiveIdentity(location)
	if err != nil {
		return 0, solana.Hash{}, err
	}
	stem, err := trimSnapshotArchiveExtension(identity)
	if err != nil {
		return 0, solana.Hash{}, err
	}
	const prefix = "snapshot-"
	if !strings.HasPrefix(stem, prefix) {
		return 0, solana.Hash{}, fmt.Errorf("invalid full snapshot archive filename %q", identity)
	}
	parts := strings.SplitN(strings.TrimPrefix(stem, prefix), "-", 2)
	if len(parts) != 2 || parts[1] == "" {
		return 0, solana.Hash{}, fmt.Errorf("invalid full snapshot archive filename %q", identity)
	}
	slot, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil || strconv.FormatUint(slot, 10) != parts[0] {
		return 0, solana.Hash{}, fmt.Errorf("invalid full snapshot slot in %q", identity)
	}
	snapshotHash, err = parseCanonicalSnapshotArchiveHash(parts[1], identity)
	if err != nil {
		return 0, solana.Hash{}, err
	}
	return slot, snapshotHash, nil
}

func parseIncrementalSnapshotArchiveSlots(location string) (baseSlot uint64, endSlot uint64, err error) {
	baseSlot, endSlot, _, err = parseIncrementalSnapshotArchiveIdentity(location)
	return baseSlot, endSlot, err
}

func parseIncrementalSnapshotArchiveIdentity(
	location string,
) (baseSlot uint64, endSlot uint64, snapshotHash solana.Hash, err error) {
	identity, err := snapshotArchiveIdentity(location)
	if err != nil {
		return 0, 0, solana.Hash{}, err
	}
	stem, err := trimSnapshotArchiveExtension(identity)
	if err != nil {
		return 0, 0, solana.Hash{}, err
	}
	const prefix = "incremental-snapshot-"
	if !strings.HasPrefix(stem, prefix) {
		return 0, 0, solana.Hash{}, fmt.Errorf("invalid incremental snapshot archive filename %q", identity)
	}
	parts := strings.SplitN(strings.TrimPrefix(stem, prefix), "-", 3)
	if len(parts) != 3 || parts[2] == "" {
		return 0, 0, solana.Hash{}, fmt.Errorf("invalid incremental snapshot archive filename %q", identity)
	}
	baseSlot, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil || strconv.FormatUint(baseSlot, 10) != parts[0] {
		return 0, 0, solana.Hash{}, fmt.Errorf("invalid incremental base slot in %q", identity)
	}
	endSlot, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil || strconv.FormatUint(endSlot, 10) != parts[1] {
		return 0, 0, solana.Hash{}, fmt.Errorf("invalid incremental end slot in %q", identity)
	}
	snapshotHash, err = parseCanonicalSnapshotArchiveHash(parts[2], identity)
	if err != nil {
		return 0, 0, solana.Hash{}, err
	}
	return baseSlot, endSlot, snapshotHash, nil
}

func parseCanonicalSnapshotArchiveHash(encoded, identity string) (solana.Hash, error) {
	snapshotHash, err := solana.HashFromBase58(encoded)
	if err != nil {
		return solana.Hash{}, fmt.Errorf("invalid snapshot hash in archive filename %q: %w", identity, err)
	}
	if snapshotHash.String() != encoded {
		return solana.Hash{}, fmt.Errorf("non-canonical snapshot hash in archive filename %q", identity)
	}
	return snapshotHash, nil
}

func validateSnapshotManifestIdentity(manifest *SnapshotManifest, archiveSlot uint64) error {
	if manifest == nil || manifest.Bank == nil {
		return errors.New("snapshot manifest has no bank")
	}
	if manifest.AccountsDb == nil {
		return errors.New("snapshot manifest has no AccountsDB fields")
	}
	if manifest.Bank.Slot != archiveSlot {
		return fmt.Errorf(
			"archive manifest slot %d does not match bank slot %d",
			archiveSlot, manifest.Bank.Slot,
		)
	}
	if manifest.AccountsDb.Slot != manifest.Bank.Slot {
		return fmt.Errorf(
			"AccountsDB slot %d does not match bank slot %d",
			manifest.AccountsDb.Slot, manifest.Bank.Slot,
		)
	}
	return nil
}

func validateSnapshotManifestPair(full, incremental *SnapshotManifest) error {
	if full == nil || full.Bank == nil || full.AccountsDb == nil {
		return errors.New("full snapshot manifest is incomplete")
	}
	if incremental == nil {
		return nil
	}
	if incremental.Bank == nil || incremental.AccountsDb == nil {
		return errors.New("incremental snapshot manifest is incomplete")
	}
	if incremental.Bank.Slot <= full.Bank.Slot {
		return fmt.Errorf(
			"incremental snapshot slot %d is not newer than full snapshot slot %d",
			incremental.Bank.Slot, full.Bank.Slot,
		)
	}
	persistence := incremental.BankIncrementalSnapshotPersistence
	if persistence != nil && persistence.FullSlot != full.Bank.Slot {
		return fmt.Errorf(
			"incremental snapshot full slot %d does not match selected full snapshot slot %d",
			persistence.FullSlot, full.Bank.Slot,
		)
	}
	return nil
}

func validateSnapshotArchiveManifestSlots(
	fullLocation string,
	full *SnapshotManifest,
	incrementalLocation string,
	incremental *SnapshotManifest,
) error {
	if full == nil || full.Bank == nil {
		return errors.New("full snapshot manifest has no bank")
	}
	fullArchiveSlot, fullArchiveHash, err := parseFullSnapshotArchiveIdentity(fullLocation)
	if err != nil {
		return err
	}
	if fullArchiveSlot != full.Bank.Slot {
		return fmt.Errorf(
			"full snapshot filename slot %d does not match manifest bank slot %d",
			fullArchiveSlot, full.Bank.Slot,
		)
	}
	if err := validateSnapshotArchiveLtHash("full", fullArchiveHash, full); err != nil {
		return err
	}
	if incremental == nil {
		return nil
	}
	if incremental.Bank == nil {
		return errors.New("incremental snapshot manifest has no bank")
	}
	baseSlot, endSlot, incrementalArchiveHash, err := parseIncrementalSnapshotArchiveIdentity(incrementalLocation)
	if err != nil {
		return err
	}
	if baseSlot != full.Bank.Slot {
		return fmt.Errorf(
			"incremental snapshot filename base slot %d does not match full snapshot slot %d",
			baseSlot, full.Bank.Slot,
		)
	}
	if endSlot != incremental.Bank.Slot {
		return fmt.Errorf(
			"incremental snapshot filename end slot %d does not match manifest bank slot %d",
			endSlot, incremental.Bank.Slot,
		)
	}
	if err := validateSnapshotArchiveLtHash("incremental", incrementalArchiveHash, incremental); err != nil {
		return err
	}
	return nil
}

func validateSnapshotArchiveLtHash(
	kind string,
	archiveHash solana.Hash,
	manifest *SnapshotManifest,
) error {
	if manifest == nil || manifest.LtHash == nil {
		return fmt.Errorf("%s snapshot manifest has no AccountsLtHash", kind)
	}
	checksum := manifest.LtHash.Checksum()
	if len(checksum) != len(solana.Hash{}) {
		return fmt.Errorf("%s snapshot manifest AccountsLtHash checksum has invalid length %d", kind, len(checksum))
	}
	expected := solana.HashFromBytes(checksum)
	if archiveHash != expected {
		return fmt.Errorf(
			"%s snapshot filename hash %s does not match manifest AccountsLtHash checksum %s",
			kind, archiveHash, expected,
		)
	}
	return nil
}

func parseAppendVecTarPath(name string) (slot uint64, fileID uint64, isAppendVec bool, err error) {
	if name == "accounts" || name == "accounts/" || !strings.HasPrefix(name, "accounts/") {
		return 0, 0, false, nil
	}
	if pathpkg.IsAbs(name) || pathpkg.Clean(name) != name {
		return 0, 0, false, fmt.Errorf("non-canonical appendvec path %q", name)
	}
	parts := strings.Split(name, "/")
	if len(parts) != 2 || parts[0] != "accounts" {
		return 0, 0, false, fmt.Errorf("non-canonical appendvec path %q", name)
	}
	nameParts := strings.Split(parts[1], ".")
	if len(nameParts) != 2 || nameParts[0] == "" || nameParts[1] == "" {
		return 0, 0, false, fmt.Errorf("invalid appendvec filename %q", name)
	}
	slot, err = strconv.ParseUint(nameParts[0], 10, 64)
	if err != nil || strconv.FormatUint(slot, 10) != nameParts[0] {
		return 0, 0, false, fmt.Errorf("invalid appendvec slot in %q", name)
	}
	fileID, err = strconv.ParseUint(nameParts[1], 10, 64)
	if err != nil || strconv.FormatUint(fileID, 10) != nameParts[1] {
		return 0, 0, false, fmt.Errorf("invalid appendvec file ID in %q", name)
	}
	return slot, fileID, true, nil
}

func isBootstrapHighTempFileName(name string) bool {
	return isDecimalTemporaryName(name, ".bootstrap-high-", ".tmp")
}

func isDecimalTemporaryName(name, prefix, suffix string) bool {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	randomPart := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	if randomPart == "" || len(randomPart) > 10 {
		return false
	}
	for _, character := range randomPart {
		if character < '0' || character > '9' {
			return false
		}
	}
	value, err := strconv.ParseUint(randomPart, 10, 32)
	return err == nil && strconv.FormatUint(value, 10) == randomPart
}

func isDeltaCheckpointArtifactFileName(name string) bool {
	if !strings.HasPrefix(name, accountsdb.DeltaCheckpointFilePrefix) {
		return false
	}
	remainder := strings.TrimPrefix(name, accountsdb.DeltaCheckpointFilePrefix)
	if strings.HasSuffix(remainder, ".partial") {
		remainder = strings.TrimSuffix(remainder, ".partial")
	}
	for _, suffix := range []string{".index", ".records", ".desc"} {
		if !strings.HasSuffix(remainder, suffix) {
			continue
		}
		generationText := strings.TrimSuffix(remainder, suffix)
		if len(generationText) != 20 {
			return false
		}
		generation, err := strconv.ParseUint(generationText, 10, 64)
		return err == nil && generation != 0 && fmt.Sprintf("%020d", generation) == generationText
	}
	return false
}

func writeSnapshotAppendVec(accountsDbDir string, task appendVecCopyingTask) error {
	if uint64(len(task.Data)) != task.FileSize {
		return fmt.Errorf(
			"appendvec %d.%d length %d does not match manifest length %d",
			task.Slot, task.FileID, len(task.Data), task.FileSize,
		)
	}
	_, _, err := streamSnapshotAppendVec(
		accountsDbDir, task.Slot, task.FileID, task.FileSize, bytes.NewReader(task.Data),
	)
	return err
}

// streamSnapshotAppendVec copies one tar member directly into a synced
// same-directory temporary file and publishes it without replacement. It
// never materializes the appendvec as a Go heap slice. A retry of the exact
// archive is idempotent only when the existing file has the same digest.
func streamSnapshotAppendVec(
	accountsDbDir string,
	slot uint64,
	fileID uint64,
	fileSize uint64,
	source io.Reader,
) (_ string, bytesRead int64, returnErr error) {
	if source == nil {
		return "", 0, errors.New("snapshot appendvec source is nil")
	}
	if fileSize == 0 || fileSize > maximumSolanaAppendVecFileSize {
		return "", 0, fmt.Errorf("appendvec %d.%d has invalid size %d", slot, fileID, fileSize)
	}
	if fileSize > uint64(^uint(0)>>1) || fileSize > uint64(^uint64(0)>>1) {
		return "", 0, fmt.Errorf("appendvec %d.%d size %d exceeds this process address space", slot, fileID, fileSize)
	}

	canonicalName := fmt.Sprintf("%d.%d", slot, fileID)
	directory := filepath.Join(accountsDbDir, "accounts")
	directoryInfo, err := os.Lstat(directory)
	if err != nil {
		return "", 0, fmt.Errorf("inspect appendvec directory: %w", err)
	}
	if !directoryInfo.IsDir() || directoryInfo.Mode()&os.ModeSymlink != 0 {
		return "", 0, fmt.Errorf("appendvec parent %s is not a real directory", directory)
	}
	outputPath := filepath.Join(directory, canonicalName)
	temp, err := os.CreateTemp(directory, "."+canonicalName+".tmp-")
	if err != nil {
		return "", 0, fmt.Errorf("create temporary appendvec %s: %w", canonicalName, err)
	}
	tempPath := temp.Name()
	closed := false
	defer func() {
		if !closed {
			returnErr = errors.Join(returnErr, temp.Close())
		}
		if removeErr := os.Remove(tempPath); removeErr != nil && !os.IsNotExist(removeErr) {
			returnErr = errors.Join(returnErr, fmt.Errorf("remove temporary appendvec %s: %w", tempPath, removeErr))
		}
	}()
	if err := temp.Chmod(0o644); err != nil {
		return "", 0, fmt.Errorf("set temporary appendvec permissions: %w", err)
	}
	hasher := sha256.New()
	bytesRead, err = io.CopyN(io.MultiWriter(temp, hasher), source, int64(fileSize))
	if err != nil {
		return "", bytesRead, fmt.Errorf("copy appendvec %s: %w", canonicalName, err)
	}
	if bytesRead != int64(fileSize) {
		return "", bytesRead, fmt.Errorf(
			"short appendvec %s: copied %d of %d bytes",
			canonicalName, bytesRead, fileSize,
		)
	}
	if err := temp.Sync(); err != nil {
		return "", bytesRead, fmt.Errorf("sync temporary appendvec %s: %w", canonicalName, err)
	}
	if err := temp.Close(); err != nil {
		closed = true
		return "", bytesRead, fmt.Errorf("close temporary appendvec %s: %w", canonicalName, err)
	}
	closed = true

	err = unix.Renameat2(unix.AT_FDCWD, tempPath, unix.AT_FDCWD, outputPath, unix.RENAME_NOREPLACE)
	if err == nil {
		return outputPath, bytesRead, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return "", bytesRead, fmt.Errorf("publish appendvec %s: %w", canonicalName, err)
	}
	existingDigest, digestErr := stableRegularFileDigest(outputPath, fileSize)
	if digestErr != nil {
		return "", bytesRead, fmt.Errorf("validate existing appendvec %s after retry: %w", canonicalName, digestErr)
	}
	var streamedDigest [sha256.Size]byte
	copy(streamedDigest[:], hasher.Sum(nil))
	if existingDigest != streamedDigest {
		return "", bytesRead, fmt.Errorf("appendvec %s already exists with different contents", canonicalName)
	}
	return outputPath, bytesRead, nil
}

func stableRegularFileDigest(path string, expectedSize uint64) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return digest, err
	}
	if !pathInfo.Mode().IsRegular() || pathInfo.Mode()&os.ModeSymlink != 0 || pathInfo.Size() != int64(expectedSize) {
		return digest, fmt.Errorf("file is not a regular file of size %d", expectedSize)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return digest, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return digest, errors.New("open regular file returned an invalid descriptor")
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return digest, err
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(pathInfo, openedInfo) || openedInfo.Size() != int64(expectedSize) {
		return digest, errors.New("regular file changed while opening")
	}
	hasher := sha256.New()
	written, err := io.CopyN(hasher, file, openedInfo.Size())
	if err != nil {
		return digest, err
	}
	if written != openedInfo.Size() {
		return digest, io.ErrUnexpectedEOF
	}
	var trailing [1]byte
	if n, readErr := file.Read(trailing[:]); n != 0 || !errors.Is(readErr, io.EOF) {
		if readErr != nil {
			return digest, readErr
		}
		return digest, errors.New("regular file grew while validating")
	}
	afterInfo, err := file.Stat()
	if err != nil {
		return digest, err
	}
	if afterInfo.Size() != openedInfo.Size() || !afterInfo.ModTime().Equal(openedInfo.ModTime()) {
		return digest, errors.New("regular file changed while validating")
	}
	finalPathInfo, err := os.Lstat(path)
	if err != nil {
		return digest, err
	}
	if !finalPathInfo.Mode().IsRegular() || finalPathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(openedInfo, finalPathInfo) {
		return digest, errors.New("regular file path changed while validating")
	}
	copy(digest[:], hasher.Sum(nil))
	return digest, nil
}

func mmapSnapshotAppendVec(path string, expectedSize uint64) ([]byte, func() error, error) {
	if expectedSize == 0 || expectedSize > uint64(^uint(0)>>1) {
		return nil, nil, fmt.Errorf("appendvec mmap size %d exceeds this process address space", expectedSize)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if !pathInfo.Mode().IsRegular() || pathInfo.Mode()&os.ModeSymlink != 0 || pathInfo.Size() != int64(expectedSize) {
		return nil, nil, fmt.Errorf("appendvec %s is not a regular file of size %d", path, expectedSize)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, nil, errors.New("open appendvec for mmap returned an invalid descriptor")
	}
	openedInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !openedInfo.Mode().IsRegular() || openedInfo.Size() != int64(expectedSize) || !os.SameFile(pathInfo, openedInfo) {
		_ = file.Close()
		return nil, nil, errors.New("appendvec changed while opening for mmap")
	}
	mapping, err := unix.Mmap(int(file.Fd()), 0, int(expectedSize), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	finalPathInfo, err := os.Lstat(path)
	if err != nil || !finalPathInfo.Mode().IsRegular() || !os.SameFile(openedInfo, finalPathInfo) {
		returnErr := errors.Join(err, unix.Munmap(mapping), file.Close())
		if returnErr == nil {
			returnErr = errors.New("appendvec path changed while mapping")
		}
		return nil, nil, returnErr
	}
	cleanup := func() error {
		return errors.Join(unix.Munmap(mapping), file.Close())
	}
	return mapping, cleanup, nil
}

func finalizeSnapshotBootstrapArtifacts(
	accountsDbDir string,
	largestFileID uint64,
	bankHash [32]byte,
	stakeEntries []accountsdb.StakeIndexEntry,
) error {
	if len(stakeEntries) == 0 {
		return errors.New("snapshot produced no stake-index entries")
	}
	if err := accountsdb.WriteLargestFileID(accountsDbDir, largestFileID); err != nil {
		return fmt.Errorf("write largest file ID %d: %w", largestFileID, err)
	}
	if err := accountsdb.WriteBootstrapHighFileID(accountsDbDir, largestFileID); err != nil {
		return fmt.Errorf("write bootstrap high file ID %d: %w", largestFileID, err)
	}
	if err := writeFileAtomically(filepath.Join(accountsDbDir, "bank_hash"), bankHash[:], 0o644); err != nil {
		return fmt.Errorf("write bank hash %x: %w", bankHash, err)
	}
	if err := accountsdb.WriteStakePubkeyIndex(filepath.Join(accountsDbDir, "stake_pubkeys.idx"), stakeEntries); err != nil {
		return fmt.Errorf("write stake pubkey index: %w", err)
	}

	bankhashDir := filepath.Join(accountsDbDir, "bankhash_db")
	bankhashDb, err := pebble.Open(bankhashDir, &pebble.Options{})
	if err != nil {
		return fmt.Errorf("open bankhash DB %s: %w", bankhashDir, err)
	}
	if err := bankhashDb.Close(); err != nil {
		return fmt.Errorf("close bankhash DB %s: %w", bankhashDir, err)
	}
	if err := syncDirectory(accountsDbDir); err != nil {
		return fmt.Errorf("final AccountsDB bootstrap directory barrier: %w", err)
	}
	return nil
}
