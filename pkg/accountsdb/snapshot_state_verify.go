package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/gagliardetto/solana-go"
	"golang.org/x/sys/unix"
)

const snapshotStateVerificationBatchRecords = 16 << 10

var ErrSnapshotAccountsStateVerification = errors.New(
	"accountsdb: snapshot accounts state verification failed",
)

// PreparedSnapshotAccountIndex is a one-shot, opaque handoff containing the
// immutable V2 generation which passed both complete artifact verification and
// the independent appendvec/AccountsLtHash check. It deliberately contains no
// mutable runtime or maintenance goroutines. The bootstrap store guard remains
// the sole owner of the underlying flock until guarded AccountsDB open adopts
// this generation.
type PreparedSnapshotAccountIndex struct {
	mu        sync.Mutex
	root      string
	config    ProductionAccountIndexConfig
	catalog   *RootIndexCatalog
	immutable *ShardedImmutableIndex
	storeLock *productionAccountIndexStoreLock
	closed    bool
}

// Close releases an unadopted immutable generation. It never releases the
// bootstrap store lock, which remains owned by the guard. Close is idempotent;
// after successful AccountsDB adoption it is a no-op.
func (prepared *PreparedSnapshotAccountIndex) Close() error {
	if prepared == nil {
		return nil
	}
	prepared.mu.Lock()
	immutable := prepared.immutable
	prepared.immutable = nil
	prepared.catalog = nil
	prepared.storeLock = nil
	prepared.closed = true
	prepared.mu.Unlock()
	if immutable == nil {
		return nil
	}
	return immutable.closeUnmanaged()
}

func (prepared *PreparedSnapshotAccountIndex) productionConfig() (ProductionAccountIndexConfig, error) {
	if prepared == nil {
		return ProductionAccountIndexConfig{}, errors.New("accountsdb: nil prepared snapshot account index")
	}
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.closed || prepared.immutable == nil {
		return ProductionAccountIndexConfig{}, errors.New("accountsdb: prepared snapshot account index is closed or consumed")
	}
	return prepared.config, nil
}

// adopt serializes Close against the one permitted handoff. action reports
// whether ownership of immutable was consumed; once consumed, the prepared
// handle is permanently empty even if later AccountsDB sidecar setup fails.
func (prepared *PreparedSnapshotAccountIndex) adopt(
	accountsDBRoot string,
	config ProductionAccountIndexConfig,
	storeLock *productionAccountIndexStoreLock,
	action func(*RootIndexCatalog, *ShardedImmutableIndex) (bool, error),
) error {
	if prepared == nil {
		return errors.New("accountsdb: nil prepared snapshot account index")
	}
	if action == nil {
		return errors.New("accountsdb: nil prepared snapshot adoption action")
	}
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.closed || prepared.catalog == nil || prepared.immutable == nil {
		return errors.New("accountsdb: prepared snapshot account index is closed or consumed")
	}
	if storeLock == nil || storeLock.file == nil || storeLock != prepared.storeLock {
		return errors.New("accountsdb: prepared snapshot account index does not belong to the held store lock")
	}
	canonicalRoot, err := filepath.Abs(accountsDBRoot)
	if err != nil {
		return fmt.Errorf("accountsdb: resolve prepared AccountsDB root: %w", err)
	}
	if filepath.Clean(canonicalRoot) != prepared.root {
		return fmt.Errorf(
			"accountsdb: prepared snapshot account index is for %s, not %s",
			prepared.root,
			filepath.Clean(canonicalRoot),
		)
	}
	config = config.withCheckpointBudgetDefaults()
	if config != prepared.config {
		return errors.New("accountsdb: production account-index configuration changed after snapshot verification")
	}

	// Finalization writes only AccountsDB sidecars, but re-read the root and
	// mutable header immediately before handoff. Along with inode checks below,
	// this makes accidental mutation fail closed without repeating complete
	// SHA/CRC/StreamHash passes over the immutable files.
	currentCatalog, err := ReadRootIndexCatalog(accountsDBRoot)
	if err != nil {
		return fmt.Errorf("accountsdb: re-read prepared snapshot root catalog: %w", err)
	}
	same, err := sameRootIndexCatalog(prepared.catalog, currentCatalog)
	if err != nil {
		return fmt.Errorf("accountsdb: compare prepared snapshot root catalog: %w", err)
	}
	if !same {
		return errors.New("accountsdb: root catalog changed after snapshot account-state verification")
	}
	if err := validateFreshSnapshotRootCatalog(currentCatalog); err != nil {
		return err
	}
	if err := validateFreshSnapshotMutableJournal(accountsDBRoot); err != nil {
		return err
	}
	if err := prepared.immutable.validatePreparedSnapshotArtifactPathsStable(); err != nil {
		return fmt.Errorf("accountsdb: validate prepared immutable artifact stability: %w", err)
	}

	consumed, actionErr := action(currentCatalog, prepared.immutable)
	if consumed {
		prepared.immutable = nil
		prepared.catalog = nil
		prepared.storeLock = nil
		prepared.closed = true
	}
	if actionErr == nil && !consumed {
		return errors.New("accountsdb: prepared snapshot adoption succeeded without consuming immutable ownership")
	}
	return actionErr
}

// PrepareVerifiedSnapshotAccountIndexWithStoreGuard performs the same
// independent state verification as VerifySnapshotAccountsStateWithStoreGuard
// and retains the already-opened immutable generation for one guarded
// AccountsDB open. This avoids a second complete SHA/CRC/StreamHash verification
// of every snapshot index artifact.
func PrepareVerifiedSnapshotAccountIndexWithStoreGuard(
	ctx context.Context,
	accountsDBRoot string,
	appendVecs []SnapshotAppendVecSpec,
	expectedLtHash *lthash.LtHash,
	expectedCapitalization uint64,
	config ProductionAccountIndexConfig,
	guard *ProductionAccountIndexStoreGuard,
) (*PreparedSnapshotAccountIndex, error) {
	if guard == nil {
		return nil, fmt.Errorf("%w: nil production account-index store guard", ErrSnapshotAccountsStateVerification)
	}
	config = config.withCheckpointBudgetDefaults()
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSnapshotAccountsStateVerification, err)
	}
	var prepared *PreparedSnapshotAccountIndex
	err := guard.withProductionAccountIndexStoreLock(
		accountsDBRoot,
		func(storeLock *productionAccountIndexStoreLock) error {
			var prepareErr error
			prepared, prepareErr = prepareVerifiedSnapshotAccountIndexLocked(
				ctx,
				accountsDBRoot,
				appendVecs,
				expectedLtHash,
				expectedCapitalization,
				config,
				storeLock,
			)
			return prepareErr
		},
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSnapshotAccountsStateVerification, err)
	}
	return prepared, nil
}

// SnapshotAppendVecSpec is one appendvec declared by the snapshot manifest.
// FileSize is the exact valid byte length, not the capacity of the appendvec.
// Snapshot file IDs are globally unique even when their storage slots differ.
type SnapshotAppendVecSpec struct {
	Slot     uint64
	FileID   uint64
	FileSize uint64
}

// VerifySnapshotAccountsStateWithStoreGuard verifies that a freshly built V2
// immutable base selects exactly the account state committed by the snapshot
// manifest. It must run after InitializeProductionAccountIndexWithStoreGuard
// and before the guard is transferred to an opened AccountsDB.
//
// The verifier scans declared appendvecs in stable physical order and probes
// StreamHash in bounded batches. Older duplicate versions are structurally
// checked but contribute neither capitalization nor AccountsLtHash. A selected
// record contributes only when the complete location returned for its full
// public key equals the record being scanned.
func VerifySnapshotAccountsStateWithStoreGuard(
	ctx context.Context,
	accountsDBRoot string,
	appendVecs []SnapshotAppendVecSpec,
	expectedLtHash *lthash.LtHash,
	expectedCapitalization uint64,
	guard *ProductionAccountIndexStoreGuard,
) error {
	if guard == nil {
		return fmt.Errorf("%w: nil production account-index store guard", ErrSnapshotAccountsStateVerification)
	}
	err := guard.withProductionAccountIndexStoreLock(
		accountsDBRoot,
		func(storeLock *productionAccountIndexStoreLock) error {
			return verifySnapshotAccountsStateLocked(
				ctx,
				accountsDBRoot,
				appendVecs,
				expectedLtHash,
				expectedCapitalization,
				storeLock,
			)
		},
	)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrSnapshotAccountsStateVerification, err)
	}
	return nil
}

type snapshotStateRecord struct {
	key        solana.PublicKey
	entry      AccountIndexEntry
	lamports   uint64
	owner      solana.PublicKey
	executable bool
	data       []byte
}

type snapshotStateAccumulator struct {
	hash                 lthash.LtHash
	capitalization       uint64
	physicalAccountCount uint64
	selectedAccountCount uint64
}

func verifySnapshotAccountsStateLocked(
	ctx context.Context,
	accountsDBRoot string,
	appendVecs []SnapshotAppendVecSpec,
	expectedLtHash *lthash.LtHash,
	expectedCapitalization uint64,
	storeLock *productionAccountIndexStoreLock,
) (retErr error) {
	specs, expectedHash, catalog, err := prepareSnapshotStateVerificationLocked(
		ctx, accountsDBRoot, appendVecs, expectedLtHash, storeLock,
	)
	if err != nil {
		return err
	}
	immutable, err := OpenShardedImmutableIndex(accountsDBRoot, catalog)
	if err != nil {
		return fmt.Errorf("open fresh immutable account index: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, immutable.closeUnmanaged())
	}()
	return verifySnapshotAccountsStateAgainstImmutable(
		ctx,
		accountsDBRoot,
		specs,
		expectedHash,
		expectedCapitalization,
		immutable,
	)
}

func prepareVerifiedSnapshotAccountIndexLocked(
	ctx context.Context,
	accountsDBRoot string,
	appendVecs []SnapshotAppendVecSpec,
	expectedLtHash *lthash.LtHash,
	expectedCapitalization uint64,
	config ProductionAccountIndexConfig,
	storeLock *productionAccountIndexStoreLock,
) (_ *PreparedSnapshotAccountIndex, retErr error) {
	specs, expectedHash, catalog, err := prepareSnapshotStateVerificationLocked(
		ctx, accountsDBRoot, appendVecs, expectedLtHash, storeLock,
	)
	if err != nil {
		return nil, err
	}
	if err := config.ValidateCatalogShardCount(catalog); err != nil {
		return nil, err
	}
	immutable, err := OpenShardedImmutableIndex(accountsDBRoot, catalog)
	if err != nil {
		return nil, fmt.Errorf("open fresh immutable account index: %w", err)
	}
	keepImmutable := false
	defer func() {
		if !keepImmutable {
			retErr = errors.Join(retErr, immutable.closeUnmanaged())
		}
	}()
	if err := verifySnapshotAccountsStateAgainstImmutable(
		ctx,
		accountsDBRoot,
		specs,
		expectedHash,
		expectedCapitalization,
		immutable,
	); err != nil {
		return nil, err
	}
	canonicalRoot, err := filepath.Abs(accountsDBRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve verified AccountsDB root: %w", err)
	}
	keepImmutable = true
	return &PreparedSnapshotAccountIndex{
		root:      filepath.Clean(canonicalRoot),
		config:    config,
		catalog:   catalog.Clone(),
		immutable: immutable,
		storeLock: storeLock,
	}, nil
}

func prepareSnapshotStateVerificationLocked(
	ctx context.Context,
	accountsDBRoot string,
	appendVecs []SnapshotAppendVecSpec,
	expectedLtHash *lthash.LtHash,
	storeLock *productionAccountIndexStoreLock,
) ([]SnapshotAppendVecSpec, *lthash.LtHash, *RootIndexCatalog, error) {
	if ctx == nil {
		return nil, nil, nil, errors.New("nil snapshot state verification context")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	if accountsDBRoot == "" {
		return nil, nil, nil, errors.New("empty AccountsDB root")
	}
	if storeLock == nil || storeLock.file == nil {
		return nil, nil, nil, errors.New("snapshot state verification requires a held store lock")
	}
	if expectedLtHash == nil {
		return nil, nil, nil, errors.New("snapshot manifest has no AccountsLtHash")
	}
	expectedHash := expectedLtHash.Clone()

	specs, err := normalizeSnapshotAppendVecSpecs(appendVecs)
	if err != nil {
		return nil, nil, nil, err
	}
	catalog, err := ReadRootIndexCatalog(accountsDBRoot)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read fresh production root catalog: %w", err)
	}
	if err := validateFreshSnapshotRootCatalog(catalog); err != nil {
		return nil, nil, nil, err
	}
	if err := validateFreshSnapshotMutableJournal(accountsDBRoot); err != nil {
		return nil, nil, nil, err
	}
	return specs, expectedHash, catalog, nil
}

func verifySnapshotAccountsStateAgainstImmutable(
	ctx context.Context,
	accountsDBRoot string,
	specs []SnapshotAppendVecSpec,
	expectedHash *lthash.LtHash,
	expectedCapitalization uint64,
	immutable *ShardedImmutableIndex,
) (retErr error) {
	if immutable == nil || expectedHash == nil {
		return errors.New("nil immutable index or expected AccountsLtHash")
	}
	expectedSelectedCount, err := immutableBaseKeyCount(immutable)
	if err != nil {
		return err
	}
	accountsDir := filepath.Join(accountsDBRoot, "accounts")
	directoryFD, err := openSnapshotAccountsDirectory(accountsDir)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := unix.Close(directoryFD); closeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close snapshot accounts directory: %w", closeErr))
		}
	}()

	var actual snapshotStateAccumulator
	for _, spec := range specs {
		if err := ctx.Err(); err != nil {
			return err
		}
		partial, err := verifySnapshotAppendVec(
			ctx,
			directoryFD,
			spec,
			immutable,
		)
		if err != nil {
			return fmt.Errorf("verify appendvec %d.%d: %w", spec.Slot, spec.FileID, err)
		}
		if actual.physicalAccountCount > math.MaxUint64-partial.physicalAccountCount {
			return errors.New("physical account count overflows uint64")
		}
		actual.physicalAccountCount += partial.physicalAccountCount
		if actual.selectedAccountCount > math.MaxUint64-partial.selectedAccountCount {
			return errors.New("selected account count overflows uint64")
		}
		actual.selectedAccountCount += partial.selectedAccountCount
		if actual.capitalization > math.MaxUint64-partial.capitalization {
			return errors.New("selected account capitalization overflows uint64")
		}
		actual.capitalization += partial.capitalization
		actual.hash.MixIn(&partial.hash)
	}

	if actual.selectedAccountCount != expectedSelectedCount {
		return fmt.Errorf(
			"immutable base selects %d keys but %d exact selected locations were present in the declared appendvecs (scanned %d physical accounts)",
			expectedSelectedCount,
			actual.selectedAccountCount,
			actual.physicalAccountCount,
		)
	}
	if actual.capitalization != expectedCapitalization {
		return fmt.Errorf(
			"capitalization mismatch: calculated %d, snapshot manifest declares %d",
			actual.capitalization,
			expectedCapitalization,
		)
	}
	if !actual.hash.Equals(expectedHash) {
		return errors.New("AccountsLtHash mismatch")
	}
	return ctx.Err()
}

func normalizeSnapshotAppendVecSpecs(
	appendVecs []SnapshotAppendVecSpec,
) ([]SnapshotAppendVecSpec, error) {
	if uint64(len(appendVecs)) > math.MaxUint64/uint64(snapshotStateVerificationBatchRecords) {
		return nil, errors.New("appendvec specification count overflows verifier bounds")
	}
	specs := append([]SnapshotAppendVecSpec(nil), appendVecs...)
	sort.Slice(specs, func(i, j int) bool {
		if specs[i].Slot != specs[j].Slot {
			return specs[i].Slot < specs[j].Slot
		}
		return specs[i].FileID < specs[j].FileID
	})
	fileSlots := make(map[uint64]uint64, len(specs))
	for i, spec := range specs {
		if spec.FileSize == 0 {
			return nil, fmt.Errorf("appendvec %d.%d has zero declared size", spec.Slot, spec.FileID)
		}
		if spec.FileSize > uint64(maxInt) {
			return nil, fmt.Errorf(
				"appendvec %d.%d size %d exceeds addressable mmap size",
				spec.Slot, spec.FileID, spec.FileSize,
			)
		}
		if i > 0 && spec.Slot == specs[i-1].Slot && spec.FileID == specs[i-1].FileID {
			return nil, fmt.Errorf("duplicate appendvec specification %d.%d", spec.Slot, spec.FileID)
		}
		if slot, exists := fileSlots[spec.FileID]; exists && slot != spec.Slot {
			return nil, fmt.Errorf(
				"appendvec file ID %d is reused by slots %d and %d",
				spec.FileID, slot, spec.Slot,
			)
		}
		fileSlots[spec.FileID] = spec.Slot
	}
	return specs, nil
}

func validateFreshSnapshotRootCatalog(catalog *RootIndexCatalog) error {
	if err := catalog.Validate(); err != nil {
		return err
	}
	if catalog.Generation != 1 || catalog.CoveredSequence != 0 ||
		catalog.RootedBatchSequence != 0 || catalog.RootedSlot != 0 {
		return fmt.Errorf(
			"account index is not a fresh snapshot root: generation=%d covered_sequence=%d rooted_batch_sequence=%d rooted_slot=%d",
			catalog.Generation,
			catalog.CoveredSequence,
			catalog.RootedBatchSequence,
			catalog.RootedSlot,
		)
	}
	for shardID := range catalog.Shards {
		shard := catalog.Shards[shardID]
		if shard.BaseGeneration != 1 || shard.BaseCoveredSequence != 0 ||
			shard.DeltaGeneration != 0 || shard.DeltaCoveredSequence != 0 {
			return fmt.Errorf(
				"account-index shard %d is not fresh: base_generation=%d base_coverage=%d delta_generation=%d delta_coverage=%d",
				shardID,
				shard.BaseGeneration,
				shard.BaseCoveredSequence,
				shard.DeltaGeneration,
				shard.DeltaCoveredSequence,
			)
		}
	}
	return nil
}

func validateFreshSnapshotMutableJournal(root string) (retErr error) {
	path := filepath.Join(root, ShardedDeltaIndexJournalFileName)
	file, info, err := openStableRegularFile(path)
	if err != nil {
		return fmt.Errorf("open fresh mutable account-index journal: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, file.Close())
	}()
	if info.Size() != deltaJournalHeaderSize {
		return fmt.Errorf(
			"mutable account-index journal is not fresh: size %d, want %d",
			info.Size(), deltaJournalHeaderSize,
		)
	}
	header := make([]byte, deltaJournalHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return fmt.Errorf("read fresh mutable account-index journal: %w", err)
	}
	if want := encodeDeltaJournalHeader(0, 0); !bytes.Equal(header, want) {
		return errors.New("mutable account-index journal is not a fresh sequence-zero header")
	}
	if err := validateStableRegularFile(file, path, info); err != nil {
		return fmt.Errorf("validate fresh mutable account-index journal: %w", err)
	}
	return nil
}

func immutableBaseKeyCount(index *ShardedImmutableIndex) (uint64, error) {
	if index == nil {
		return 0, errors.New("nil immutable account index")
	}
	var count uint64
	for shardID := range index.shards {
		base := index.shards[shardID].base
		if base == nil {
			return 0, fmt.Errorf("immutable account-index shard %d has no base", shardID)
		}
		shardCount := base.NumKeys()
		if count > math.MaxUint64-shardCount {
			return 0, errors.New("immutable base key count overflows uint64")
		}
		count += shardCount
	}
	return count, nil
}

func openSnapshotAccountsDirectory(path string) (int, error) {
	fd, err := unix.Open(
		path,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return -1, fmt.Errorf("open snapshot accounts directory %s: %w", path, err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("stat snapshot accounts directory %s: %w", path, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("snapshot accounts path %s is not a real directory", path)
	}
	return fd, nil
}

func verifySnapshotAppendVec(
	ctx context.Context,
	directoryFD int,
	spec SnapshotAppendVecSpec,
	immutable *ShardedImmutableIndex,
) (result snapshotStateAccumulator, retErr error) {
	name := SegmentDataName(spec.Slot, spec.FileID)
	fd, err := unix.Openat(
		directoryFD,
		name,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return result, fmt.Errorf("open declared appendvec: %w", err)
	}
	defer func() {
		if closeErr := unix.Close(fd); closeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close declared appendvec: %w", closeErr))
		}
	}()
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil {
		return result, fmt.Errorf("stat opened appendvec: %w", err)
	}
	if opened.Mode&unix.S_IFMT != unix.S_IFREG {
		return result, errors.New("declared appendvec is not a regular file")
	}
	if opened.Size < 0 || uint64(opened.Size) != spec.FileSize {
		return result, fmt.Errorf(
			"declared appendvec size mismatch: opened %d, manifest declares %d",
			opened.Size, spec.FileSize,
		)
	}
	mapping, err := unix.Mmap(fd, 0, int(spec.FileSize), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return result, fmt.Errorf("mmap declared appendvec: %w", err)
	}
	defer func() {
		if unmapErr := unix.Munmap(mapping); unmapErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("unmap declared appendvec: %w", unmapErr))
		}
	}()

	records := make([]snapshotStateRecord, 0, snapshotStateVerificationBatchRecords)
	flush := func() error {
		if len(records) == 0 {
			return nil
		}
		partial, err := verifySnapshotStateBatch(ctx, immutable, records)
		if err != nil {
			return err
		}
		if result.physicalAccountCount > math.MaxUint64-partial.physicalAccountCount {
			return errors.New("physical account count overflows uint64")
		}
		result.physicalAccountCount += partial.physicalAccountCount
		if result.selectedAccountCount > math.MaxUint64-partial.selectedAccountCount {
			return errors.New("selected account count overflows uint64")
		}
		result.selectedAccountCount += partial.selectedAccountCount
		if result.capitalization > math.MaxUint64-partial.capitalization {
			return errors.New("selected account capitalization overflows uint64")
		}
		result.capitalization += partial.capitalization
		result.hash.MixIn(&partial.hash)
		records = records[:0]
		return nil
	}

	limit := uint64(len(mapping))
	for offset := uint64(0); offset < limit; {
		if len(records)&255 == 0 {
			if err := ctx.Err(); err != nil {
				return result, err
			}
		}
		remaining := limit - offset
		if remaining < hdrLen {
			for _, value := range mapping[int(offset):] {
				if value != 0 {
					return result, fmt.Errorf(
						"truncated appendvec header at offset %d: have %d bytes, need %d",
						offset, remaining, hdrLen,
					)
				}
			}
			break
		}
		header := mapping[int(offset) : int(offset)+hdrLen : int(offset)+hdrLen]
		dataLen := binary.LittleEndian.Uint64(header[dataLenOffset : dataLenOffset+8])
		key := solana.PublicKeyFromBytes(header[pubkeyOffset : pubkeyOffset+32])
		lamports := binary.LittleEndian.Uint64(header[lamportsOffset : lamportsOffset+8])
		if key == (solana.PublicKey{}) && lamports == 0 {
			// Agave treats an all-zero StoredMeta/AccountMeta prefix as unused
			// appendvec capacity. Snapshot FileSize is the exact declared byte
			// range, however, so prove that the whole remaining range is unused.
			// Otherwise a corrupt snapshot could hide arbitrary unvalidated bytes
			// behind a forged terminator while still passing the state hash check.
			for tailOffset := offset; tailOffset < limit; tailOffset++ {
				if tailOffset&((1<<20)-1) == 0 {
					if err := ctx.Err(); err != nil {
						return result, err
					}
				}
				if mapping[int(tailOffset)] != 0 {
					return result, fmt.Errorf(
						"non-zero byte after appendvec terminator at offset %d",
						tailOffset,
					)
				}
			}
			break
		}
		if dataLen > maxAppendVecAccountDataLen {
			return result, fmt.Errorf(
				"appendvec account data length %d exceeds maximum %d at offset %d",
				dataLen, maxAppendVecAccountDataLen, offset,
			)
		}
		dataOffset := offset + hdrLen
		if dataLen > limit-dataOffset {
			return result, fmt.Errorf(
				"truncated appendvec account data at offset %d: data length %d exceeds %d available bytes",
				offset, dataLen, limit-dataOffset,
			)
		}
		if header[96] > 1 {
			return result, fmt.Errorf("invalid executable byte %d at offset %d", header[96], offset)
		}
		alignedDataLen := (dataLen + 7) &^ uint64(7)
		dataEnd := dataOffset + dataLen
		recordEnd := dataOffset + alignedDataLen
		records = append(records, snapshotStateRecord{
			key:        key,
			entry:      AccountIndexEntry{Slot: spec.Slot, FileId: spec.FileID, Offset: offset},
			lamports:   lamports,
			owner:      solana.PublicKeyFromBytes(header[ownerOffset : ownerOffset+32]),
			executable: header[96] == 1,
			data:       mapping[int(dataOffset):int(dataEnd):int(dataEnd)],
		})
		if len(records) == cap(records) {
			if err := flush(); err != nil {
				return result, err
			}
		}
		offset = recordEnd
	}
	if err := flush(); err != nil {
		return result, err
	}
	if err := validateSnapshotAppendVecIdentity(directoryFD, name, spec.FileSize, fd, opened); err != nil {
		return result, err
	}
	return result, nil
}

func validateSnapshotAppendVecIdentity(
	directoryFD int,
	name string,
	expectedSize uint64,
	fd int,
	opened unix.Stat_t,
) error {
	var finalOpened unix.Stat_t
	if err := unix.Fstat(fd, &finalOpened); err != nil {
		return fmt.Errorf("restat opened appendvec: %w", err)
	}
	var finalPath unix.Stat_t
	if err := unix.Fstatat(directoryFD, name, &finalPath, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("restat appendvec path: %w", err)
	}
	stable := func(stat unix.Stat_t) bool {
		return stat.Mode&unix.S_IFMT == unix.S_IFREG &&
			stat.Size >= 0 && uint64(stat.Size) == expectedSize &&
			stat.Dev == opened.Dev && stat.Ino == opened.Ino &&
			stat.Mtim == opened.Mtim && stat.Ctim == opened.Ctim
	}
	if !stable(finalOpened) || !stable(finalPath) {
		return errors.New("declared appendvec changed while being verified")
	}
	return nil
}

func verifySnapshotStateBatch(
	ctx context.Context,
	immutable *ShardedImmutableIndex,
	records []snapshotStateRecord,
) (snapshotStateAccumulator, error) {
	var result snapshotStateAccumulator
	if err := ctx.Err(); err != nil {
		return result, err
	}
	candidates := make([]AccountIndexEntry, len(records))
	found := make([]bool, len(records))
	if err := immutable.lookupBatchBaseCandidates(
		ctx,
		len(records),
		func(index int) solana.PublicKey { return records[index].key },
		nil,
		func(index int, entry AccountIndexEntry, ok bool) {
			candidates[index] = entry
			found[index] = ok
		},
	); err != nil {
		return result, err
	}

	type partialAccumulator struct {
		hash           lthash.LtHash
		capitalization uint64
		selected       uint64
	}
	partials := make([]partialAccumulator, 0, min(len(records), runtime.GOMAXPROCS(0)*2))
	var partialsMu sync.Mutex
	err := runBatchStaticRanges(ctx, len(records), func(workerCtx context.Context, start, end int) error {
		var partial partialAccumulator
		var hasher lthash.AccountHasher
		var contribution lthash.LtHash
		for index := start; index < end; index++ {
			if index&255 == 0 {
				if err := workerCtx.Err(); err != nil {
					return err
				}
			}
			record := &records[index]
			if !found[index] {
				return fmt.Errorf("physical account %x is absent from the immutable base", record.key)
			}
			if candidates[index] != record.entry {
				continue
			}
			if partial.selected == math.MaxUint64 {
				return errors.New("selected account count overflows uint64")
			}
			partial.selected++
			if partial.capitalization > math.MaxUint64-record.lamports {
				return errors.New("selected account capitalization overflows uint64")
			}
			partial.capitalization += record.lamports
			account := accounts.Account{
				Key:        record.key,
				Lamports:   record.lamports,
				Data:       record.data,
				Owner:      record.owner,
				Executable: record.executable,
			}
			hasher.HashInto(&contribution, &account)
			partial.hash.MixIn(&contribution)
		}
		partialsMu.Lock()
		partials = append(partials, partial)
		partialsMu.Unlock()
		return nil
	})
	if err != nil {
		return result, err
	}
	result.physicalAccountCount = uint64(len(records))
	for index := range partials {
		partial := &partials[index]
		if result.selectedAccountCount > math.MaxUint64-partial.selected {
			return result, errors.New("selected account count overflows uint64")
		}
		result.selectedAccountCount += partial.selected
		if result.capitalization > math.MaxUint64-partial.capitalization {
			return result, errors.New("selected account capitalization overflows uint64")
		}
		result.capitalization += partial.capitalization
		result.hash.MixIn(&partial.hash)
	}
	return result, ctx.Err()
}
