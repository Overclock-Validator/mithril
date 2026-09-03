package accountsdb

import (
	"bufio"
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	mmap "github.com/edsrzf/mmap-go"
	"github.com/gagliardetto/solana-go"
	"github.com/stellar/streamhash"
	"github.com/zeebo/xxh3"
)

// A delta checkpoint is an immutable, exact account-index generation. The
// StreamHash file is the fast probabilistic directory; its six-byte payload is
// an ordinal into the fixed-width record file. The record's complete public
// key is always compared before a live value or tombstone is accepted.
//
// Publication is generation-numbered. The two data artifacts are made durable
// first and a CRC-protected descriptor that binds their complete SHA-256
// identities is renamed last. Thus an interrupted build is either invisible
// (no descriptor) or fails closed (a published descriptor does not match its
// artifacts). No build overwrites an older, possibly memory-mapped generation.
const (
	// DeltaCheckpointFilePrefix identifies every generation-numbered exact
	// mutable-index checkpoint artifact.
	DeltaCheckpointFilePrefix     = "accounts_delta_checkpoint."
	deltaCheckpointFilePrefix     = DeltaCheckpointFilePrefix
	deltaCheckpointIndexSuffix    = ".stmh"
	deltaCheckpointRecordSuffix   = ".rec"
	deltaCheckpointDescSuffix     = ".desc"
	deltaCheckpointPartialSuffix  = ".partial"
	DeltaCheckpointBuildDirPrefix = ".delta-checkpoint-build-"
	deltaCheckpointBuildDirPrefix = DeltaCheckpointBuildDirPrefix

	deltaCheckpointPayloadBytes      = 6
	deltaCheckpointFingerprintBytes  = 2
	deltaCheckpointMaxPayload        = uint64(1<<(8*deltaCheckpointPayloadBytes)) - 1
	deltaCheckpointMaxBuildAttempts  = 8
	deltaCheckpointContextCheckEvery = 4096

	deltaCheckpointRecordHeaderSize = 64
	deltaCheckpointRecordSize       = 64
	deltaCheckpointDescriptorSize   = 128
	deltaCheckpointMetadataSize     = 56

	deltaCheckpointRecordLive      = uint8(1)
	deltaCheckpointRecordTombstone = uint8(2)
)

var (
	deltaCheckpointRecordMagic     = [8]byte{'M', 'I', 'T', 'H', 'D', 'R', '0', '1'}
	deltaCheckpointDescriptorMagic = [8]byte{'M', 'I', 'T', 'H', 'D', 'C', '0', '1'}
	deltaCheckpointMetadataMagic   = [8]byte{'M', 'I', 'T', 'H', 'D', 'M', '0', '1'}
	deltaCheckpointVersion         = uint32(1)
	deltaCheckpointCRCTable        = crc32.MakeTable(crc32.Castagnoli)

	// Generation allocation is serialized per checkpoint directory. Different
	// persistent shards have private directories and may therefore build in
	// parallel, while V1 callers that share one directory still cannot select
	// the same next generation.
	deltaCheckpointBuildLocks = deltaCheckpointDirectoryLocks{
		locks: make(map[string]*deltaCheckpointDirectoryLock),
	}

	// Test hooks let collision retry and exact false-positive handling be
	// exercised deterministically. Production uses a random per-build seed and
	// hashes every byte of the 32-byte Solana public key.
	deltaCheckpointSeedSource = newDeltaCheckpointSeed
	deltaCheckpointHashKey    = hashDeltaCheckpointKey

	// ErrDeltaCheckpointCapacity means the bounded exact delta has exhausted
	// its safety envelope and the immutable snapshot base must be rebuilt.
	ErrDeltaCheckpointCapacity = errors.New("accountsdb: delta checkpoint capacity exhausted")
	// ErrInvalidDeltaCheckpoint identifies a published checkpoint whose
	// descriptor, exact records, or StreamHash metadata are structurally
	// inconsistent. Runtime maintenance must fail closed on this class rather
	// than treating durable corruption as a transient build failure.
	ErrInvalidDeltaCheckpoint = errors.New("accountsdb: invalid delta checkpoint")
	// ErrDeltaCheckpointCommitDecided means the descriptor rename completed,
	// so the generation may be selected after a crash even though a later
	// durability check or reopen failed. Callers must retain all artifacts and
	// halt rather than attempting retry cleanup.
	ErrDeltaCheckpointCommitDecided = errors.New("accountsdb: delta checkpoint publication decided")
)

type deltaCheckpointDirectoryLock struct {
	mu   sync.Mutex
	refs uint64
}

type deltaCheckpointDirectoryLocks struct {
	mu    sync.Mutex
	locks map[string]*deltaCheckpointDirectoryLock
}

func lockDeltaCheckpointDirectory(dir string) (func(), error) {
	canonical, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: resolve delta checkpoint directory %q: %w", dir, err)
	}
	canonical = filepath.Clean(canonical)
	// Resolve aliases when the directory already exists. If it does not, the
	// absolute clean path remains a stable identity and the later build reports
	// the more useful create/read error.
	if resolved, resolveErr := filepath.EvalSymlinks(canonical); resolveErr == nil {
		canonical = resolved
	} else if !errors.Is(resolveErr, os.ErrNotExist) {
		return nil, fmt.Errorf("accountsdb: resolve delta checkpoint directory symlinks %q: %w", dir, resolveErr)
	}

	deltaCheckpointBuildLocks.mu.Lock()
	lock := deltaCheckpointBuildLocks.locks[canonical]
	if lock == nil {
		lock = &deltaCheckpointDirectoryLock{}
		deltaCheckpointBuildLocks.locks[canonical] = lock
	}
	lock.refs++
	deltaCheckpointBuildLocks.mu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		deltaCheckpointBuildLocks.mu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(deltaCheckpointBuildLocks.locks, canonical)
		}
		deltaCheckpointBuildLocks.mu.Unlock()
	}, nil
}

type deltaCheckpointIdentity struct {
	Size   uint64
	SHA256 [sha256.Size]byte
}

type deltaCheckpointDescriptor struct {
	Generation  uint64
	CoveredSeq  uint64
	RecordCount uint64
	Index       deltaCheckpointIdentity
	Records     deltaCheckpointIdentity
}

// DeltaCheckpoint is one opened immutable exact delta generation. Its record
// bytes and StreamHash index are memory-mapped; the record descriptor is not
// retained after a successful open. Methods may be called concurrently; Close
// waits for active lookups/enumerations.
type DeltaCheckpoint struct {
	mu          sync.RWMutex
	generation  uint64
	coveredSeq  uint64
	recordCount uint64
	hashSeed    uint64
	index       *streamhash.PayloadIndex
	recordPath  string
	records     mmap.MMap
	closed      bool
}

// Generation returns the immutable artifact generation number.
func (checkpoint *DeltaCheckpoint) Generation() uint64 {
	if checkpoint == nil {
		return 0
	}
	return checkpoint.generation
}

// CoveredSeq is the highest mutable-index journal sequence incorporated into
// this checkpoint.
func (checkpoint *DeltaCheckpoint) CoveredSeq() uint64 {
	if checkpoint == nil {
		return 0
	}
	return checkpoint.coveredSeq
}

// Len returns the number of exact newest-wins records in the generation.
func (checkpoint *DeltaCheckpoint) Len() uint64 {
	if checkpoint == nil {
		return 0
	}
	return checkpoint.recordCount
}

// OpenLatestDeltaCheckpoint opens the highest published descriptor in dir. A
// directory with no published checkpoint returns (nil, nil). Partial artifacts
// and complete-looking data files without a descriptor are ignored. Once a
// descriptor is published, corruption or a missing bound artifact is a hard
// error; opening never silently rolls back to an older generation.
func OpenLatestDeltaCheckpoint(dir string) (*DeltaCheckpoint, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: list delta checkpoints: %w", err)
	}
	var latest uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		generation, suffix, partial, ok := parseDeltaCheckpointArtifactName(entry.Name())
		if !ok || partial || suffix != deltaCheckpointDescSuffix {
			continue
		}
		if generation > latest {
			latest = generation
		}
	}
	if latest == 0 {
		return nil, nil
	}
	return openDeltaCheckpointGeneration(dir, latest)
}

func openDeltaCheckpointGeneration(dir string, generation uint64) (*DeltaCheckpoint, error) {
	return openDeltaCheckpointGenerationWithCatalogArtifacts(dir, generation, nil, nil)
}

// openDeltaCheckpointGenerationWithCatalogArtifacts binds the descriptor's
// identities to the root-selected identities before hashing each data file.
// That preserves both trust chains with one full pass per artifact instead of
// hashing once for the root and again for the descriptor.
func openDeltaCheckpointGenerationWithCatalogArtifacts(
	dir string,
	generation uint64,
	selectedIndex *IndexCatalogArtifact,
	selectedRecords *IndexCatalogArtifact,
) (*DeltaCheckpoint, error) {
	if (selectedIndex == nil) != (selectedRecords == nil) {
		return nil, fmt.Errorf("%w: incomplete root-selected delta artifact pair", ErrInvalidRootIndexCatalog)
	}
	paths := makeDeltaCheckpointPaths(dir, generation)
	descriptorBytes, err := readDeltaCheckpointDescriptor(paths.descriptor)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: read descriptor generation %d: %w",
			ErrInvalidDeltaCheckpoint, generation, err,
		)
	}
	descriptor, err := decodeDeltaCheckpointDescriptor(descriptorBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: decode descriptor generation %d: %v", ErrInvalidDeltaCheckpoint, generation, err)
	}
	if descriptor.Generation != generation {
		return nil, fmt.Errorf(
			"%w: descriptor generation %d names generation %d",
			ErrInvalidDeltaCheckpoint,
			generation,
			descriptor.Generation,
		)
	}
	if selectedIndex != nil {
		if descriptor.Index.Size != selectedIndex.Size ||
			!bytes.Equal(descriptor.Index.SHA256[:], selectedIndex.SHA256[:]) {
			return nil, fmt.Errorf(
				"%w: root and descriptor index identities differ for delta generation %d",
				ErrInvalidRootIndexCatalog, generation,
			)
		}
		if descriptor.Records.Size != selectedRecords.Size ||
			!bytes.Equal(descriptor.Records.SHA256[:], selectedRecords.SHA256[:]) {
			return nil, fmt.Errorf(
				"%w: root and descriptor record identities differ for delta generation %d",
				ErrInvalidRootIndexCatalog, generation,
			)
		}
	}
	if err := verifyDeltaCheckpointIdentity(paths.index, descriptor.Index); err != nil {
		return nil, fmt.Errorf("accountsdb: verify delta checkpoint index generation %d: %w", generation, err)
	}
	if err := verifyDeltaCheckpointIdentity(paths.records, descriptor.Records); err != nil {
		return nil, fmt.Errorf("accountsdb: verify delta checkpoint records generation %d: %w", generation, err)
	}

	recordFile, recordInfo, err := openStableRegularFile(paths.records)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: open delta checkpoint records generation %d: %w", generation, err)
	}
	if uint64(recordInfo.Size()) != descriptor.Records.Size {
		_ = recordFile.Close()
		return nil, fmt.Errorf(
			"%w: opened record size %d does not match descriptor %d",
			ErrInvalidDeltaCheckpoint, recordInfo.Size(), descriptor.Records.Size,
		)
	}
	records, err := mmap.Map(recordFile, mmap.RDONLY, 0)
	if err != nil {
		_ = recordFile.Close()
		return nil, fmt.Errorf("accountsdb: mmap delta checkpoint records generation %d: %w", generation, err)
	}
	recordFileOpen := true
	closeRecordFile := func() error {
		if !recordFileOpen {
			return nil
		}
		recordFileOpen = false
		return recordFile.Close()
	}
	closeRecords := func() {
		_ = records.Unmap()
		_ = closeRecordFile()
	}
	recordHeader, err := decodeDeltaCheckpointRecordHeader(records)
	if err != nil {
		closeRecords()
		return nil, fmt.Errorf("%w: decode record header generation %d: %v", ErrInvalidDeltaCheckpoint, generation, err)
	}
	if recordHeader.generation != generation ||
		recordHeader.coveredSeq != descriptor.CoveredSeq ||
		recordHeader.recordCount != descriptor.RecordCount {
		closeRecords()
		return nil, fmt.Errorf(
			"%w: record header does not match descriptor: generation=%d/%d covered=%d/%d count=%d/%d",
			ErrInvalidDeltaCheckpoint,
			recordHeader.generation, generation,
			recordHeader.coveredSeq, descriptor.CoveredSeq,
			recordHeader.recordCount, descriptor.RecordCount,
		)
	}
	expectedRecordBytes, ok := deltaCheckpointRecordsFileSize(descriptor.RecordCount)
	if !ok || uint64(len(records)) != expectedRecordBytes {
		closeRecords()
		return nil, fmt.Errorf(
			"%w: record length %d does not match count %d",
			ErrInvalidDeltaCheckpoint,
			len(records),
			descriptor.RecordCount,
		)
	}

	index, err := streamhash.OpenPayload(paths.index)
	if err != nil {
		closeRecords()
		return nil, fmt.Errorf("accountsdb: open delta checkpoint StreamHash generation %d: %w", generation, err)
	}
	closeAll := func() {
		_ = index.Close()
		closeRecords()
	}
	stats := index.Stats()
	if stats.Algorithm != streamhash.AlgoPTRHash ||
		stats.PayloadSize != deltaCheckpointPayloadBytes ||
		stats.FingerprintSize != deltaCheckpointFingerprintBytes {
		closeAll()
		return nil, fmt.Errorf(
			"%w: incompatible StreamHash (algorithm=%s payload=%d fingerprint=%d)",
			ErrInvalidDeltaCheckpoint,
			stats.Algorithm, stats.PayloadSize, stats.FingerprintSize,
		)
	}
	metadata, err := decodeDeltaCheckpointMetadata(index.UserMetadata())
	if err != nil {
		closeAll()
		return nil, fmt.Errorf("%w: decode StreamHash metadata: %v", ErrInvalidDeltaCheckpoint, err)
	}
	if metadata.generation != generation ||
		metadata.coveredSeq != descriptor.CoveredSeq ||
		metadata.recordCount != descriptor.RecordCount {
		closeAll()
		return nil, fmt.Errorf(
			"%w: StreamHash metadata does not match descriptor: generation=%d/%d covered=%d/%d count=%d/%d",
			ErrInvalidDeltaCheckpoint,
			metadata.generation, generation,
			metadata.coveredSeq, descriptor.CoveredSeq,
			metadata.recordCount, descriptor.RecordCount,
		)
	}
	if index.NumKeys() != descriptor.RecordCount {
		closeAll()
		return nil, fmt.Errorf(
			"%w: StreamHash key count %d does not match descriptor %d",
			ErrInvalidDeltaCheckpoint,
			index.NumKeys(),
			descriptor.RecordCount,
		)
	}
	if err := index.Verify(); err != nil {
		closeAll()
		return nil, fmt.Errorf("%w: verify StreamHash: %v", ErrInvalidDeltaCheckpoint, err)
	}
	if err := validateStableRegularFile(recordFile, paths.records, recordInfo); err != nil {
		closeAll()
		return nil, fmt.Errorf("%w: records changed while opening: %v", ErrInvalidDeltaCheckpoint, err)
	}
	// A read-only mmap owns its VM object independently from the descriptor.
	// Keeping one descriptor per exact delta shard would otherwise consume up
	// to another 1024 FDs in the steady state.
	if err := closeRecordFile(); err != nil {
		_ = index.Close()
		_ = records.Unmap()
		return nil, fmt.Errorf(
			"accountsdb: close mmapped delta checkpoint records generation %d: %w",
			generation,
			err,
		)
	}

	return &DeltaCheckpoint{
		generation:  generation,
		coveredSeq:  descriptor.CoveredSeq,
		recordCount: descriptor.RecordCount,
		hashSeed:    metadata.hashSeed,
		index:       index,
		recordPath:  paths.records,
		records:     records,
	}, nil
}

// Lookup performs an exact lookup. StreamHash false positives are rejected by
// comparing the complete key in the ordinal record. Exact tombstones return
// (value-with-Tombstone, true, nil); callers use found=true to suppress older
// generations.
func (checkpoint *DeltaCheckpoint) Lookup(key solana.PublicKey) (deltaIndexValue, bool, error) {
	if checkpoint == nil {
		return deltaIndexValue{}, false, nil
	}
	checkpoint.mu.RLock()
	defer checkpoint.mu.RUnlock()
	if checkpoint.closed || checkpoint.index == nil {
		return deltaIndexValue{}, false, errors.New("accountsdb: delta checkpoint is closed")
	}
	return checkpoint.lookupPinned(key)
}

// LookupBatch performs exact lookups while pinning the checkpoint mapping only
// once. values and found must both have len(keys); every output element is
// overwritten. Independent probes run over static worker ranges, avoiding a
// contended per-key scheduling counter. If an error is returned, output may be
// partially populated and must not be consumed.
func (checkpoint *DeltaCheckpoint) LookupBatch(
	ctx context.Context,
	keys []solana.PublicKey,
	values []deltaIndexValue,
	found []bool,
) error {
	if ctx == nil {
		return errors.New("accountsdb: nil delta checkpoint lookup context")
	}
	if len(values) != len(keys) || len(found) != len(keys) {
		return fmt.Errorf(
			"accountsdb: delta checkpoint batch buffer mismatch: keys=%d values=%d found=%d",
			len(keys), len(values), len(found),
		)
	}
	clear(values)
	clear(found)
	if len(keys) == 0 {
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if checkpoint == nil {
		return nil
	}

	checkpoint.mu.RLock()
	defer checkpoint.mu.RUnlock()
	if checkpoint.closed || checkpoint.index == nil {
		return errors.New("accountsdb: delta checkpoint is closed")
	}
	if len(keys) < batchStaticParallelMinJobs {
		for job, key := range keys {
			value, ok, err := checkpoint.lookupPinned(key)
			if err != nil {
				return fmt.Errorf("accountsdb: delta checkpoint batch lookup %d: %w", job, err)
			}
			values[job] = value
			found[job] = ok
		}
		return ctx.Err()
	}
	return checkpoint.lookupBatchPinned(
		ctx,
		len(keys),
		func(job int) solana.PublicKey { return keys[job] },
		nil,
		func(job int, value deltaIndexValue, ok bool) {
			values[job] = value
			found[job] = ok
		},
	)
}

// lookupPinned performs one exact lookup while the caller pins checkpoint's
// lifetime, either with checkpoint.mu or with MutableAccountIndex.mu through a
// MutableAccountIndexSnapshot. It deliberately performs no lock operation.
func (checkpoint *DeltaCheckpoint) lookupPinned(key solana.PublicKey) (deltaIndexValue, bool, error) {

	hash := deltaCheckpointHashKey(key, checkpoint.hashSeed)
	_, ordinal, err := checkpoint.index.QueryPayload(hash[:])
	if errors.Is(err, streamhash.ErrNotFound) {
		return deltaIndexValue{}, false, nil
	}
	if err != nil {
		return deltaIndexValue{}, false, fmt.Errorf("accountsdb: query delta checkpoint StreamHash: %w", err)
	}
	if ordinal >= checkpoint.recordCount {
		return deltaIndexValue{}, false, fmt.Errorf("accountsdb: corrupt delta checkpoint ordinal %d >= %d", ordinal, checkpoint.recordCount)
	}
	recordKey, value, err := decodeDeltaCheckpointRecord(checkpoint.records, ordinal)
	if err != nil {
		return deltaIndexValue{}, false, err
	}
	if recordKey != key {
		return deltaIndexValue{}, false, nil
	}
	return value, true, nil
}

// lookupBatchPinned probes a pinned checkpoint without allocating a key or
// result array. keyAt and accept may be invoked concurrently for distinct job
// indexes. skip, when non-nil, identifies jobs already resolved by a newer RAM
// generation. The full-key and per-record CRC checks remain in lookupPinned.
func (checkpoint *DeltaCheckpoint) lookupBatchPinned(
	ctx context.Context,
	count int,
	keyAt func(job int) solana.PublicKey,
	skip func(job int) bool,
	accept func(job int, value deltaIndexValue, found bool),
) error {
	if count == 0 {
		return nil
	}
	return runBatchStaticRanges(ctx, count, func(workerCtx context.Context, start, end int) error {
		for job := start; job < end; job++ {
			if job&255 == 0 {
				if err := workerCtx.Err(); err != nil {
					return err
				}
			}
			if skip != nil && skip(job) {
				continue
			}
			value, found, err := checkpoint.lookupPinned(keyAt(job))
			if err != nil {
				return fmt.Errorf("accountsdb: delta checkpoint batch lookup %d: %w", job, err)
			}
			accept(job, value, found)
		}
		return nil
	})
}

// ForEachSorted enumerates the checkpoint's exact records in ascending public
// key byte order. The callback must not call Close on this checkpoint.
func (checkpoint *DeltaCheckpoint) ForEachSorted(fn func(key solana.PublicKey, value deltaIndexValue) error) error {
	if checkpoint == nil {
		return nil
	}
	if fn == nil {
		return errors.New("accountsdb: nil delta checkpoint enumeration callback")
	}
	checkpoint.mu.RLock()
	defer checkpoint.mu.RUnlock()
	if checkpoint.closed {
		return errors.New("accountsdb: delta checkpoint is closed")
	}
	for ordinal := uint64(0); ordinal < checkpoint.recordCount; ordinal++ {
		key, value, err := decodeDeltaCheckpointRecord(checkpoint.records, ordinal)
		if err != nil {
			return err
		}
		if err := fn(key, value); err != nil {
			return err
		}
	}
	return nil
}

// Close releases both mappings. The record-file descriptor was closed as soon
// as its read-only mapping was validated. Close is safe and idempotent.
func (checkpoint *DeltaCheckpoint) Close() error {
	if checkpoint == nil {
		return nil
	}
	checkpoint.mu.Lock()
	defer checkpoint.mu.Unlock()
	if checkpoint.closed {
		return nil
	}
	checkpoint.closed = true
	var errs []error
	if checkpoint.index != nil {
		errs = append(errs, checkpoint.index.Close())
		checkpoint.index = nil
	}
	if checkpoint.records != nil {
		errs = append(errs, checkpoint.records.Unmap())
		checkpoint.records = nil
	}
	checkpoint.recordPath = ""
	return errors.Join(errs...)
}

// BuildDeltaCheckpoint creates and publishes a new generation containing the
// newest-wins union of old and frozen. frozen must not be mutated concurrently.
// old remains open and unchanged; after the caller atomically swaps its runtime
// pointer and drains old readers it may close old and garbage-collect it.
func BuildDeltaCheckpoint(
	ctx context.Context,
	dir string,
	old *DeltaCheckpoint,
	frozen map[solana.PublicKey]deltaIndexValue,
	coveredSeq uint64,
	workers int,
) (*DeltaCheckpoint, error) {
	return buildDeltaCheckpoint(ctx, dir, old, frozen, coveredSeq, workers, 0)
}

func buildDeltaCheckpoint(
	ctx context.Context,
	dir string,
	old *DeltaCheckpoint,
	frozen map[solana.PublicKey]deltaIndexValue,
	coveredSeq uint64,
	workers int,
	maxRecords uint64,
) (*DeltaCheckpoint, error) {
	if ctx == nil {
		return nil, errors.New("accountsdb: nil delta checkpoint build context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if workers < 0 {
		return nil, fmt.Errorf("accountsdb: negative delta checkpoint workers %d", workers)
	}
	if old != nil && coveredSeq < old.CoveredSeq() {
		return nil, fmt.Errorf("accountsdb: delta checkpoint covered sequence regressed from %d to %d", old.CoveredSeq(), coveredSeq)
	}

	unlockDirectory, err := lockDeltaCheckpointDirectory(dir)
	if err != nil {
		return nil, err
	}
	defer unlockDirectory()

	generation, err := nextDeltaCheckpointGeneration(dir)
	if err != nil {
		return nil, err
	}
	paths := makeDeltaCheckpointPaths(dir, generation)
	partials := []string{paths.indexPartial, paths.recordsPartial, paths.descriptorPartial}
	for _, path := range partials {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("accountsdb: remove stale delta checkpoint partial %s: %w", path, err)
		}
	}
	cleanupPartials := true
	defer func() {
		if cleanupPartials {
			for _, path := range partials {
				_ = os.Remove(path)
			}
		}
	}()

	frozenRecords := make([]deltaCheckpointMapRecord, 0, len(frozen))
	for key, value := range frozen {
		frozenRecords = append(frozenRecords, deltaCheckpointMapRecord{key: key, value: canonicalDeltaCheckpointValue(value)})
	}
	sort.Slice(frozenRecords, func(i, j int) bool {
		return bytes.Compare(frozenRecords[i].key[:], frozenRecords[j].key[:]) < 0
	})

	recordCount, err := writeMergedDeltaCheckpointRecords(
		ctx, paths.recordsPartial, generation, coveredSeq, old, frozenRecords, maxRecords,
	)
	if err != nil {
		return nil, err
	}
	if recordCount > deltaCheckpointMaxPayload+1 {
		return nil, fmt.Errorf("accountsdb: delta checkpoint record count %d exceeds six-byte ordinal capacity", recordCount)
	}

	if err := buildDeltaCheckpointStreamHash(
		ctx, paths.indexPartial, paths.recordsPartial, dir,
		generation, coveredSeq, recordCount, workers,
	); err != nil {
		return nil, err
	}

	recordIdentity, err := computeDeltaCheckpointIdentity(paths.recordsPartial)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: identify delta checkpoint records: %w", err)
	}
	indexIdentity, err := computeDeltaCheckpointIdentity(paths.indexPartial)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: identify delta checkpoint index: %w", err)
	}
	descriptor := deltaCheckpointDescriptor{
		Generation:  generation,
		CoveredSeq:  coveredSeq,
		RecordCount: recordCount,
		Index:       indexIdentity,
		Records:     recordIdentity,
	}

	for _, pair := range [][2]string{
		{paths.recordsPartial, paths.records},
		{paths.indexPartial, paths.index},
	} {
		if _, err := os.Stat(pair[1]); err == nil {
			return nil, fmt.Errorf("accountsdb: refusing to overwrite delta checkpoint artifact %s", pair[1])
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("accountsdb: stat delta checkpoint artifact %s: %w", pair[1], err)
		}
		if err := os.Rename(pair[0], pair[1]); err != nil {
			return nil, fmt.Errorf("accountsdb: publish delta checkpoint artifact %s: %w", pair[1], err)
		}
	}
	if err := fsyncDir(dir); err != nil {
		return nil, fmt.Errorf("accountsdb: sync delta checkpoint artifacts: %w", err)
	}
	if err := publishDeltaCheckpointDescriptor(
		dir, paths.descriptorPartial, paths.descriptor, descriptor, fsyncDir,
	); err != nil {
		return nil, err
	}
	cleanupPartials = false

	checkpoint, err := openDeltaCheckpointGeneration(dir, generation)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: reopen published delta checkpoint generation %d: %w",
			ErrDeltaCheckpointCommitDecided, generation, err,
		)
	}
	return checkpoint, nil
}

type deltaCheckpointMapRecord struct {
	key   solana.PublicKey
	value deltaIndexValue
}

type deltaCheckpointRecordHeader struct {
	generation  uint64
	coveredSeq  uint64
	recordCount uint64
}

func writeMergedDeltaCheckpointRecords(
	ctx context.Context,
	path string,
	generation uint64,
	coveredSeq uint64,
	old *DeltaCheckpoint,
	frozen []deltaCheckpointMapRecord,
	maxRecords uint64,
) (uint64, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return 0, fmt.Errorf("accountsdb: create delta checkpoint records: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	if n, err := file.Write(make([]byte, deltaCheckpointRecordHeaderSize)); err != nil {
		return 0, fmt.Errorf("accountsdb: reserve delta checkpoint record header: %w", err)
	} else if n != deltaCheckpointRecordHeaderSize {
		return 0, fmt.Errorf("accountsdb: reserve delta checkpoint record header: %w", io.ErrShortWrite)
	}
	writer := bufio.NewWriterSize(file, 1<<20)

	if old != nil {
		old.mu.RLock()
		defer old.mu.RUnlock()
		if old.closed {
			return 0, errors.New("accountsdb: cannot merge a closed delta checkpoint")
		}
	}

	var oldOrdinal uint64
	var frozenOrdinal int
	var count uint64
	for (old != nil && oldOrdinal < old.recordCount) || frozenOrdinal < len(frozen) {
		if count%deltaCheckpointContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}

		var selected deltaCheckpointMapRecord
		switch {
		case old == nil || oldOrdinal >= old.recordCount:
			selected = frozen[frozenOrdinal]
			frozenOrdinal++
		case frozenOrdinal >= len(frozen):
			key, value, err := decodeDeltaCheckpointRecord(old.records, oldOrdinal)
			if err != nil {
				return 0, err
			}
			selected = deltaCheckpointMapRecord{key: key, value: value}
			oldOrdinal++
		default:
			oldKey, oldValue, err := decodeDeltaCheckpointRecord(old.records, oldOrdinal)
			if err != nil {
				return 0, err
			}
			comparison := bytes.Compare(oldKey[:], frozen[frozenOrdinal].key[:])
			switch {
			case comparison < 0:
				selected = deltaCheckpointMapRecord{key: oldKey, value: oldValue}
				oldOrdinal++
			case comparison > 0:
				selected = frozen[frozenOrdinal]
				frozenOrdinal++
			default:
				// frozen is newer and replaces the old checkpoint's value.
				selected = frozen[frozenOrdinal]
				oldOrdinal++
				frozenOrdinal++
			}
		}

		if maxRecords != 0 && count >= maxRecords {
			return 0, fmt.Errorf(
				"%w: more than %d changed account keys; rebuild from a fresh snapshot",
				ErrDeltaCheckpointCapacity, maxRecords,
			)
		}
		var encoded [deltaCheckpointRecordSize]byte
		encodeDeltaCheckpointRecord(&encoded, selected.key, selected.value)
		if _, err := writer.Write(encoded[:]); err != nil {
			return 0, fmt.Errorf("accountsdb: write delta checkpoint record %d: %w", count, err)
		}
		count++
	}
	if err := writer.Flush(); err != nil {
		return 0, fmt.Errorf("accountsdb: flush delta checkpoint records: %w", err)
	}
	header := encodeDeltaCheckpointRecordHeader(deltaCheckpointRecordHeader{
		generation:  generation,
		coveredSeq:  coveredSeq,
		recordCount: count,
	})
	if err := writeFullAt(file, header, 0); err != nil {
		return 0, fmt.Errorf("accountsdb: write delta checkpoint record header: %w", err)
	}
	if err := file.Sync(); err != nil {
		return 0, fmt.Errorf("accountsdb: sync delta checkpoint records: %w", err)
	}
	if err := file.Close(); err != nil {
		return 0, fmt.Errorf("accountsdb: close delta checkpoint records: %w", err)
	}
	closed = true
	return count, nil
}

func buildDeltaCheckpointStreamHash(
	ctx context.Context,
	indexPath string,
	recordPath string,
	tempParent string,
	generation uint64,
	coveredSeq uint64,
	recordCount uint64,
	workers int,
) error {
	tempDir, err := os.MkdirTemp(tempParent, ".delta-checkpoint-build-")
	if err != nil {
		return fmt.Errorf("accountsdb: create delta checkpoint StreamHash temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	recordFile, err := os.Open(recordPath)
	if err != nil {
		return fmt.Errorf("accountsdb: open delta checkpoint records for StreamHash build: %w", err)
	}
	records, err := mmap.Map(recordFile, mmap.RDONLY, 0)
	if err != nil {
		_ = recordFile.Close()
		return fmt.Errorf("accountsdb: mmap delta checkpoint records for StreamHash build: %w", err)
	}
	defer records.Unmap()
	defer recordFile.Close()

	for attempt := 0; attempt < deltaCheckpointMaxBuildAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.Remove(indexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("accountsdb: remove prior delta checkpoint StreamHash attempt: %w", err)
		}
		seed, err := deltaCheckpointSeedSource()
		if err != nil {
			return err
		}
		metadata := encodeDeltaCheckpointMetadata(deltaCheckpointMetadata{
			generation:  generation,
			coveredSeq:  coveredSeq,
			recordCount: recordCount,
			hashSeed:    seed,
		})
		opts := []streamhash.BuildOption{
			streamhash.WithAlgorithm(streamhash.AlgoPTRHash),
			streamhash.WithPayload(deltaCheckpointPayloadBytes),
			streamhash.WithFingerprint(deltaCheckpointFingerprintBytes),
			streamhash.WithGlobalSeed(seed ^ 0x9e3779b97f4a7c15),
			streamhash.WithMetadata(metadata),
		}
		if workers > 0 {
			opts = append(opts, streamhash.WithWorkers(workers))
		}
		builder, err := streamhash.NewUnsortedBuilder(ctx, indexPath, recordCount, tempDir, opts...)
		if err != nil {
			return fmt.Errorf("accountsdb: create delta checkpoint StreamHash builder: %w", err)
		}
		buildErr := func() error {
			for ordinal := uint64(0); ordinal < recordCount; ordinal++ {
				if ordinal%deltaCheckpointContextCheckEvery == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				key, _, err := decodeDeltaCheckpointRecord(records, ordinal)
				if err != nil {
					return err
				}
				hash := deltaCheckpointHashKey(key, seed)
				if err := builder.AddKey(hash[:], ordinal); err != nil {
					return err
				}
			}
			return builder.Finish()
		}()
		closeErr := builder.Close()
		buildErr = errors.Join(buildErr, closeErr)
		if errors.Is(buildErr, streamhash.ErrDuplicateKey) ||
			errors.Is(buildErr, streamhash.ErrIndistinguishableHashes) {
			continue
		}
		if buildErr != nil {
			return fmt.Errorf("accountsdb: build delta checkpoint StreamHash: %w", buildErr)
		}

		verification, err := streamhash.OpenPayload(indexPath)
		if err != nil {
			return fmt.Errorf("accountsdb: reopen built delta checkpoint StreamHash: %w", err)
		}
		if verification.NumKeys() != recordCount {
			got := verification.NumKeys()
			_ = verification.Close()
			return fmt.Errorf("accountsdb: built delta checkpoint StreamHash has %d keys, want %d", got, recordCount)
		}
		if err := verification.Verify(); err != nil {
			_ = verification.Close()
			return fmt.Errorf("accountsdb: verify built delta checkpoint StreamHash: %w", err)
		}
		if err := verification.Close(); err != nil {
			return fmt.Errorf("accountsdb: close verified delta checkpoint StreamHash: %w", err)
		}
		return nil
	}
	return fmt.Errorf("accountsdb: failed to build collision-free delta checkpoint StreamHash after %d seeds", deltaCheckpointMaxBuildAttempts)
}

func canonicalDeltaCheckpointValue(value deltaIndexValue) deltaIndexValue {
	if value.Tombstone {
		return deltaIndexValue{Tombstone: true}
	}
	return value
}

func encodeDeltaCheckpointRecord(out *[deltaCheckpointRecordSize]byte, key solana.PublicKey, value deltaIndexValue) {
	clear(out[:])
	copy(out[0:32], key[:])
	value = canonicalDeltaCheckpointValue(value)
	if value.Tombstone {
		out[32] = deltaCheckpointRecordTombstone
	} else {
		out[32] = deltaCheckpointRecordLive
		binary.LittleEndian.PutUint64(out[36:44], value.Entry.Slot)
		binary.LittleEndian.PutUint64(out[44:52], value.Entry.FileId)
		binary.LittleEndian.PutUint64(out[52:60], value.Entry.Offset)
	}
	binary.LittleEndian.PutUint32(out[60:64], crc32.Checksum(out[:60], deltaCheckpointCRCTable))
}

func decodeDeltaCheckpointRecord(data []byte, ordinal uint64) (solana.PublicKey, deltaIndexValue, error) {
	var key solana.PublicKey
	offset, ok := deltaCheckpointRecordOffset(ordinal)
	if !ok || offset > uint64(len(data)) || uint64(len(data))-offset < deltaCheckpointRecordSize {
		return key, deltaIndexValue{}, fmt.Errorf("%w: record ordinal %d is out of bounds", ErrInvalidDeltaCheckpoint, ordinal)
	}
	record := data[offset : offset+deltaCheckpointRecordSize]
	copy(key[:], record[:32])
	for _, reserved := range record[33:36] {
		if reserved != 0 {
			return key, deltaIndexValue{}, fmt.Errorf("%w: record %d has non-zero reserved bytes", ErrInvalidDeltaCheckpoint, ordinal)
		}
	}
	wantCRC := binary.LittleEndian.Uint32(record[60:64])
	if got := crc32.Checksum(record[:60], deltaCheckpointCRCTable); got != wantCRC {
		return key, deltaIndexValue{}, fmt.Errorf("%w: record %d CRC mismatch: got %08x want %08x", ErrInvalidDeltaCheckpoint, ordinal, got, wantCRC)
	}
	switch record[32] {
	case deltaCheckpointRecordLive:
		return key, deltaIndexValue{Entry: AccountIndexEntry{
			Slot:   binary.LittleEndian.Uint64(record[36:44]),
			FileId: binary.LittleEndian.Uint64(record[44:52]),
			Offset: binary.LittleEndian.Uint64(record[52:60]),
		}}, nil
	case deltaCheckpointRecordTombstone:
		for _, encoded := range record[36:60] {
			if encoded != 0 {
				return key, deltaIndexValue{}, fmt.Errorf("%w: tombstone record %d has a non-zero location", ErrInvalidDeltaCheckpoint, ordinal)
			}
		}
		return key, deltaIndexValue{Tombstone: true}, nil
	default:
		return key, deltaIndexValue{}, fmt.Errorf("%w: record %d has invalid kind %d", ErrInvalidDeltaCheckpoint, ordinal, record[32])
	}
}

func deltaCheckpointRecordOffset(ordinal uint64) (uint64, bool) {
	if ordinal > (^uint64(0)-deltaCheckpointRecordHeaderSize)/deltaCheckpointRecordSize {
		return 0, false
	}
	return deltaCheckpointRecordHeaderSize + ordinal*deltaCheckpointRecordSize, true
}

func deltaCheckpointRecordsFileSize(count uint64) (uint64, bool) {
	if count > (^uint64(0)-deltaCheckpointRecordHeaderSize)/deltaCheckpointRecordSize {
		return 0, false
	}
	return deltaCheckpointRecordHeaderSize + count*deltaCheckpointRecordSize, true
}

func encodeDeltaCheckpointRecordHeader(header deltaCheckpointRecordHeader) []byte {
	encoded := make([]byte, deltaCheckpointRecordHeaderSize)
	copy(encoded[:8], deltaCheckpointRecordMagic[:])
	binary.LittleEndian.PutUint32(encoded[8:12], deltaCheckpointVersion)
	binary.LittleEndian.PutUint32(encoded[12:16], deltaCheckpointRecordHeaderSize)
	binary.LittleEndian.PutUint64(encoded[16:24], header.generation)
	binary.LittleEndian.PutUint64(encoded[24:32], header.coveredSeq)
	binary.LittleEndian.PutUint64(encoded[32:40], header.recordCount)
	binary.LittleEndian.PutUint32(encoded[60:64], crc32.Checksum(encoded[:60], deltaCheckpointCRCTable))
	return encoded
}

func decodeDeltaCheckpointRecordHeader(encoded []byte) (deltaCheckpointRecordHeader, error) {
	var header deltaCheckpointRecordHeader
	if len(encoded) < deltaCheckpointRecordHeaderSize {
		return header, fmt.Errorf("record file has %d bytes, shorter than its header", len(encoded))
	}
	encoded = encoded[:deltaCheckpointRecordHeaderSize]
	if !bytes.Equal(encoded[:8], deltaCheckpointRecordMagic[:]) {
		return header, fmt.Errorf("invalid record magic %x", encoded[:8])
	}
	if version := binary.LittleEndian.Uint32(encoded[8:12]); version != deltaCheckpointVersion {
		return header, fmt.Errorf("unsupported record version %d", version)
	}
	if size := binary.LittleEndian.Uint32(encoded[12:16]); size != deltaCheckpointRecordHeaderSize {
		return header, fmt.Errorf("invalid record header size %d", size)
	}
	for _, reserved := range encoded[40:60] {
		if reserved != 0 {
			return header, errors.New("record header has non-zero reserved bytes")
		}
	}
	wantCRC := binary.LittleEndian.Uint32(encoded[60:64])
	if got := crc32.Checksum(encoded[:60], deltaCheckpointCRCTable); got != wantCRC {
		return header, fmt.Errorf("record header CRC mismatch: got %08x want %08x", got, wantCRC)
	}
	header.generation = binary.LittleEndian.Uint64(encoded[16:24])
	header.coveredSeq = binary.LittleEndian.Uint64(encoded[24:32])
	header.recordCount = binary.LittleEndian.Uint64(encoded[32:40])
	return header, nil
}

type deltaCheckpointMetadata struct {
	generation  uint64
	coveredSeq  uint64
	recordCount uint64
	hashSeed    uint64
}

func encodeDeltaCheckpointMetadata(metadata deltaCheckpointMetadata) []byte {
	encoded := make([]byte, deltaCheckpointMetadataSize)
	copy(encoded[:8], deltaCheckpointMetadataMagic[:])
	binary.LittleEndian.PutUint32(encoded[8:12], deltaCheckpointVersion)
	binary.LittleEndian.PutUint32(encoded[12:16], deltaCheckpointMetadataSize)
	binary.LittleEndian.PutUint64(encoded[16:24], metadata.generation)
	binary.LittleEndian.PutUint64(encoded[24:32], metadata.coveredSeq)
	binary.LittleEndian.PutUint64(encoded[32:40], metadata.recordCount)
	binary.LittleEndian.PutUint64(encoded[40:48], metadata.hashSeed)
	binary.LittleEndian.PutUint32(encoded[52:56], crc32.Checksum(encoded[:52], deltaCheckpointCRCTable))
	return encoded
}

func decodeDeltaCheckpointMetadata(encoded []byte) (deltaCheckpointMetadata, error) {
	var metadata deltaCheckpointMetadata
	if len(encoded) != deltaCheckpointMetadataSize {
		return metadata, fmt.Errorf("metadata has %d bytes, want %d", len(encoded), deltaCheckpointMetadataSize)
	}
	if !bytes.Equal(encoded[:8], deltaCheckpointMetadataMagic[:]) {
		return metadata, fmt.Errorf("invalid metadata magic %x", encoded[:8])
	}
	if version := binary.LittleEndian.Uint32(encoded[8:12]); version != deltaCheckpointVersion {
		return metadata, fmt.Errorf("unsupported metadata version %d", version)
	}
	if size := binary.LittleEndian.Uint32(encoded[12:16]); size != deltaCheckpointMetadataSize {
		return metadata, fmt.Errorf("invalid metadata size %d", size)
	}
	for _, reserved := range encoded[48:52] {
		if reserved != 0 {
			return metadata, errors.New("metadata has non-zero reserved bytes")
		}
	}
	wantCRC := binary.LittleEndian.Uint32(encoded[52:56])
	if got := crc32.Checksum(encoded[:52], deltaCheckpointCRCTable); got != wantCRC {
		return metadata, fmt.Errorf("metadata CRC mismatch: got %08x want %08x", got, wantCRC)
	}
	metadata.generation = binary.LittleEndian.Uint64(encoded[16:24])
	metadata.coveredSeq = binary.LittleEndian.Uint64(encoded[24:32])
	metadata.recordCount = binary.LittleEndian.Uint64(encoded[32:40])
	metadata.hashSeed = binary.LittleEndian.Uint64(encoded[40:48])
	return metadata, nil
}

func encodeDeltaCheckpointDescriptor(descriptor deltaCheckpointDescriptor) []byte {
	encoded := make([]byte, deltaCheckpointDescriptorSize)
	copy(encoded[:8], deltaCheckpointDescriptorMagic[:])
	binary.LittleEndian.PutUint32(encoded[8:12], deltaCheckpointVersion)
	binary.LittleEndian.PutUint32(encoded[12:16], deltaCheckpointDescriptorSize)
	binary.LittleEndian.PutUint64(encoded[16:24], descriptor.Generation)
	binary.LittleEndian.PutUint64(encoded[24:32], descriptor.CoveredSeq)
	binary.LittleEndian.PutUint64(encoded[32:40], descriptor.RecordCount)
	binary.LittleEndian.PutUint64(encoded[40:48], descriptor.Index.Size)
	binary.LittleEndian.PutUint64(encoded[48:56], descriptor.Records.Size)
	copy(encoded[56:88], descriptor.Index.SHA256[:])
	copy(encoded[88:120], descriptor.Records.SHA256[:])
	binary.LittleEndian.PutUint32(encoded[124:128], crc32.Checksum(encoded[:124], deltaCheckpointCRCTable))
	return encoded
}

func decodeDeltaCheckpointDescriptor(encoded []byte) (deltaCheckpointDescriptor, error) {
	var descriptor deltaCheckpointDescriptor
	if len(encoded) != deltaCheckpointDescriptorSize {
		return descriptor, fmt.Errorf("descriptor has %d bytes, want %d", len(encoded), deltaCheckpointDescriptorSize)
	}
	if !bytes.Equal(encoded[:8], deltaCheckpointDescriptorMagic[:]) {
		return descriptor, fmt.Errorf("invalid descriptor magic %x", encoded[:8])
	}
	if version := binary.LittleEndian.Uint32(encoded[8:12]); version != deltaCheckpointVersion {
		return descriptor, fmt.Errorf("unsupported descriptor version %d", version)
	}
	if size := binary.LittleEndian.Uint32(encoded[12:16]); size != deltaCheckpointDescriptorSize {
		return descriptor, fmt.Errorf("invalid descriptor size %d", size)
	}
	for _, reserved := range encoded[120:124] {
		if reserved != 0 {
			return descriptor, errors.New("descriptor has non-zero reserved bytes")
		}
	}
	wantCRC := binary.LittleEndian.Uint32(encoded[124:128])
	if got := crc32.Checksum(encoded[:124], deltaCheckpointCRCTable); got != wantCRC {
		return descriptor, fmt.Errorf("descriptor CRC mismatch: got %08x want %08x", got, wantCRC)
	}
	descriptor.Generation = binary.LittleEndian.Uint64(encoded[16:24])
	descriptor.CoveredSeq = binary.LittleEndian.Uint64(encoded[24:32])
	descriptor.RecordCount = binary.LittleEndian.Uint64(encoded[32:40])
	descriptor.Index.Size = binary.LittleEndian.Uint64(encoded[40:48])
	descriptor.Records.Size = binary.LittleEndian.Uint64(encoded[48:56])
	copy(descriptor.Index.SHA256[:], encoded[56:88])
	copy(descriptor.Records.SHA256[:], encoded[88:120])
	if err := validateDeltaCheckpointDescriptorValue(descriptor); err != nil {
		return deltaCheckpointDescriptor{}, err
	}
	return descriptor, nil
}

func validateDeltaCheckpointDescriptorValue(descriptor deltaCheckpointDescriptor) error {
	if descriptor.Generation == 0 {
		return errors.New("descriptor has zero generation")
	}
	if descriptor.Index.Size == 0 || descriptor.Records.Size < deltaCheckpointRecordHeaderSize {
		return errors.New("descriptor has invalid zero/truncated artifact size")
	}
	if allZero(descriptor.Index.SHA256[:]) || allZero(descriptor.Records.SHA256[:]) {
		return errors.New("descriptor has a zero artifact SHA-256")
	}
	expectedRecordBytes, ok := deltaCheckpointRecordsFileSize(descriptor.RecordCount)
	if !ok || descriptor.RecordCount > deltaCheckpointMaxPayload+1 || descriptor.Records.Size != expectedRecordBytes {
		return fmt.Errorf(
			"descriptor record size %d disagrees with count %d",
			descriptor.Records.Size, descriptor.RecordCount,
		)
	}
	return nil
}

func readDeltaCheckpointDescriptor(path string) ([]byte, error) {
	file, info, err := openStableRegularFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if info.Size() != deltaCheckpointDescriptorSize {
		return nil, fmt.Errorf("descriptor has %d bytes, want %d", info.Size(), deltaCheckpointDescriptorSize)
	}
	encoded := make([]byte, deltaCheckpointDescriptorSize)
	if _, err := io.ReadFull(file, encoded); err != nil {
		return nil, err
	}
	if err := validateStableRegularFile(file, path, info); err != nil {
		return nil, err
	}
	return encoded, nil
}

func publishDeltaCheckpointDescriptor(
	dir string,
	partialPath string,
	finalPath string,
	descriptor deltaCheckpointDescriptor,
	syncDirectory func(string) error,
) error {
	if syncDirectory == nil {
		return errors.New("accountsdb: nil delta checkpoint directory sync")
	}
	if err := writeDeltaCheckpointDescriptorAtomic(partialPath, finalPath, descriptor); err != nil {
		return err
	}
	if err := syncDirectory(dir); err != nil {
		return fmt.Errorf(
			"%w: sync published delta checkpoint descriptor: %w",
			ErrDeltaCheckpointCommitDecided, err,
		)
	}
	return nil
}

func writeDeltaCheckpointDescriptorAtomic(partialPath, finalPath string, descriptor deltaCheckpointDescriptor) error {
	if err := validateDeltaCheckpointDescriptorValue(descriptor); err != nil {
		return fmt.Errorf("accountsdb: invalid delta checkpoint descriptor: %w", err)
	}
	if _, err := os.Stat(finalPath); err == nil {
		return fmt.Errorf("accountsdb: refusing to overwrite delta checkpoint descriptor %s", finalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("accountsdb: stat delta checkpoint descriptor %s: %w", finalPath, err)
	}
	file, err := os.OpenFile(partialPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("accountsdb: create delta checkpoint descriptor partial: %w", err)
	}
	encoded := encodeDeltaCheckpointDescriptor(descriptor)
	if n, err := file.Write(encoded); err != nil {
		_ = file.Close()
		return fmt.Errorf("accountsdb: write delta checkpoint descriptor: %w", err)
	} else if n != len(encoded) {
		_ = file.Close()
		return fmt.Errorf("accountsdb: write delta checkpoint descriptor: %w", io.ErrShortWrite)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("accountsdb: sync delta checkpoint descriptor: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("accountsdb: close delta checkpoint descriptor: %w", err)
	}
	if err := os.Rename(partialPath, finalPath); err != nil {
		return fmt.Errorf("accountsdb: publish delta checkpoint descriptor: %w", err)
	}
	return nil
}

func computeDeltaCheckpointIdentity(path string) (deltaCheckpointIdentity, error) {
	var identity deltaCheckpointIdentity
	size, digest, err := hashStableRegularFile(path)
	if err != nil {
		return identity, err
	}
	identity.Size = size
	identity.SHA256 = digest
	return identity, nil
}

func verifyDeltaCheckpointIdentity(path string, want deltaCheckpointIdentity) error {
	got, err := computeDeltaCheckpointIdentity(path)
	if err != nil {
		return err
	}
	if got.Size != want.Size {
		return fmt.Errorf("%w: artifact size mismatch: got %d want %d", ErrInvalidDeltaCheckpoint, got.Size, want.Size)
	}
	if !bytes.Equal(got.SHA256[:], want.SHA256[:]) {
		return fmt.Errorf("%w: artifact SHA-256 mismatch", ErrInvalidDeltaCheckpoint)
	}
	return nil
}

type deltaCheckpointPaths struct {
	index             string
	records           string
	descriptor        string
	indexPartial      string
	recordsPartial    string
	descriptorPartial string
}

func makeDeltaCheckpointPaths(dir string, generation uint64) deltaCheckpointPaths {
	stem := filepath.Join(dir, fmt.Sprintf("%s%020d", deltaCheckpointFilePrefix, generation))
	return deltaCheckpointPaths{
		index:             stem + deltaCheckpointIndexSuffix,
		records:           stem + deltaCheckpointRecordSuffix,
		descriptor:        stem + deltaCheckpointDescSuffix,
		indexPartial:      stem + deltaCheckpointIndexSuffix + deltaCheckpointPartialSuffix,
		recordsPartial:    stem + deltaCheckpointRecordSuffix + deltaCheckpointPartialSuffix,
		descriptorPartial: stem + deltaCheckpointDescSuffix + deltaCheckpointPartialSuffix,
	}
}

func parseDeltaCheckpointArtifactName(name string) (generation uint64, suffix string, partial bool, ok bool) {
	if !strings.HasPrefix(name, deltaCheckpointFilePrefix) {
		return 0, "", false, false
	}
	remainder := strings.TrimPrefix(name, deltaCheckpointFilePrefix)
	if strings.HasSuffix(remainder, deltaCheckpointPartialSuffix) {
		partial = true
		remainder = strings.TrimSuffix(remainder, deltaCheckpointPartialSuffix)
	}
	for _, candidate := range []string{deltaCheckpointIndexSuffix, deltaCheckpointRecordSuffix, deltaCheckpointDescSuffix} {
		if !strings.HasSuffix(remainder, candidate) {
			continue
		}
		number := strings.TrimSuffix(remainder, candidate)
		if len(number) != 20 {
			return 0, "", false, false
		}
		generation, err := strconv.ParseUint(number, 10, 64)
		if err != nil || generation == 0 {
			return 0, "", false, false
		}
		return generation, candidate, partial, true
	}
	return 0, "", false, false
}

func nextDeltaCheckpointGeneration(dir string) (uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("accountsdb: list delta checkpoint artifacts: %w", err)
	}
	var highest uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		generation, _, _, ok := parseDeltaCheckpointArtifactName(entry.Name())
		if ok && generation > highest {
			highest = generation
		}
	}
	if highest == ^uint64(0) {
		return 0, errors.New("accountsdb: delta checkpoint generation overflow")
	}
	return highest + 1, nil
}

// GarbageCollectDeltaCheckpointFiles removes recognized checkpoint artifacts
// whose generations are not in keepGenerations, plus private StreamHash spill
// directories left by interrupted builds. Callers must first swap away from,
// drain readers of, and Close every generation they allow this helper to
// remove. Unrecognized files and directories are never touched.
func GarbageCollectDeltaCheckpointFiles(dir string, keepGenerations ...uint64) ([]string, error) {
	keep := make(map[uint64]struct{}, len(keepGenerations))
	for _, generation := range keepGenerations {
		if generation != 0 {
			keep[generation] = struct{}{}
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: list delta checkpoint files for GC: %w", err)
	}
	var removed []string
	for _, entry := range entries {
		if entry.IsDir() {
			// StreamHash's unsorted builder can leave a substantial spill
			// directory after a process crash. There is only one account-index
			// writer/build at a time, and GC is called after publication, so an
			// exact private-prefix match is a stale build rather than live state.
			if isDeltaCheckpointBuildDirectoryName(entry.Name()) {
				path := filepath.Join(dir, entry.Name())
				if err := os.RemoveAll(path); err != nil {
					return removed, fmt.Errorf("accountsdb: remove stale delta checkpoint build dir %s: %w", path, err)
				}
				removed = append(removed, path)
			}
			continue
		}
		generation, _, _, ok := parseDeltaCheckpointArtifactName(entry.Name())
		if !ok {
			continue
		}
		if _, retained := keep[generation]; retained {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return removed, fmt.Errorf("accountsdb: remove delta checkpoint file %s: %w", path, err)
		}
		removed = append(removed, path)
	}
	if len(removed) > 0 {
		if err := fsyncDir(dir); err != nil {
			return removed, fmt.Errorf("accountsdb: sync delta checkpoint GC: %w", err)
		}
	}
	sort.Strings(removed)
	return removed, nil
}

func isDeltaCheckpointBuildDirectoryName(name string) bool {
	suffix := strings.TrimPrefix(name, deltaCheckpointBuildDirPrefix)
	if suffix == name || len(suffix) == 0 || len(suffix) > 10 {
		return false
	}
	for i := range len(suffix) {
		if suffix[i] < '0' || suffix[i] > '9' {
			return false
		}
	}
	value, err := strconv.ParseUint(suffix, 10, 32)
	return err == nil && strconv.FormatUint(value, 10) == suffix
}

func newDeltaCheckpointSeed() (uint64, error) {
	var encoded [8]byte
	if _, err := io.ReadFull(cryptorand.Reader, encoded[:]); err != nil {
		return 0, fmt.Errorf("accountsdb: generate delta checkpoint hash seed: %w", err)
	}
	return binary.LittleEndian.Uint64(encoded[:]), nil
}

func hashDeltaCheckpointKey(key solana.PublicKey, seed uint64) (dst [streamhash.MinKeySize]byte) {
	hash := xxh3.Hash128Seed(key[:], seed)
	binary.LittleEndian.PutUint64(dst[0:8], hash.Lo)
	binary.LittleEndian.PutUint64(dst[8:16], hash.Hi)
	return dst
}
