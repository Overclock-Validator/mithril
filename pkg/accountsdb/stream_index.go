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
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/gagliardetto/solana-go"
	"github.com/stellar/streamhash"
	"github.com/zeebo/xxh3"
)

const (
	// StreamIndexFileName is the immutable account-index base built from a
	// snapshot. The mutable account-index journal beside it is the exact delta.
	StreamIndexFileName = "accounts_index.stmh"

	// StreamIndexRunRecordSize is the on-disk size of one sorted snapshot run
	// record: a 32-byte public key followed by an AccountIndexEntry.
	StreamIndexRunRecordSize = 32 + 24

	streamIndexPayloadBytes      = 6
	streamIndexFingerprintBytes  = 2
	streamIndexExtentSize        = uint64(128 << 20)
	streamIndexExtentOrdinalBits = 24
	streamIndexRelativeBits      = 24
	streamIndexMaxExtents        = 1 << streamIndexExtentOrdinalBits
	streamIndexPayloadMask       = uint64(1<<streamIndexRelativeBits) - 1

	streamIndexMetadataVersion    = uint32(2)
	streamIndexMetadataHeaderSize = 24
	streamIndexMetadataRecordSize = 24
	streamIndexContextCheckEvery  = 4096
	streamIndexMaxBuildAttempts   = 8
)

var (
	streamIndexMetadataMagic = [8]byte{'M', 'I', 'T', 'H', 'S', 'H', '0', '1'}

	streamIndexSeedSource = newStreamIndexHashSeed
	streamIndexHash       = streamIndexHashKey
)

// StreamIndexBuildStats describes an immutable StreamHash base build.
type StreamIndexBuildStats struct {
	InputKeys       uint64
	BaseKeys        uint64
	Extents         uint64
	HashSeedRetries uint64
}

type streamIndexExtent struct {
	Slot       uint64
	FileID     uint64
	BaseOffset uint64
}

// StreamIndexSource is an exact, repeatable view of snapshot account-index
// entries. Every Scan must begin at the first entry and yield each public key
// exactly once in strictly increasing byte order. BuildStreamAccountIndex scans
// the source more than once and may scan it again after a hash collision.
type StreamIndexSource interface {
	Scan(context.Context, func(solana.PublicKey, AccountIndexEntry) error) error
}

type streamIndexRunFile struct {
	path string
	info os.FileInfo
}

// streamIndexRunSource scans sorted, non-overlapping shard runs in filename
// order. It is deliberately stateless: every Scan reopens every run, which
// makes the source repeatable without retaining one file descriptor per shard.
type streamIndexRunSource struct {
	runs []streamIndexRunFile
}

// OpenStreamIndexRunSource opens the fixed-width .run files produced by the
// snapshot shard logger. Empty runs are valid, but at least one run must exist.
// File sizes are checked here and again on every scan; record ordering and
// uniqueness are checked while scanning.
func OpenStreamIndexRunSource(runDir string) (StreamIndexSource, error) {
	if runDir == "" {
		return nil, errors.New("accountsdb: empty StreamHash run directory")
	}
	directoryInfo, err := os.Lstat(runDir)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: inspect StreamHash run directory: %w", err)
	}
	if !directoryInfo.IsDir() {
		return nil, fmt.Errorf("accountsdb: StreamHash run directory %s is not a real directory", runDir)
	}
	pattern := filepath.Join(runDir, "*.run")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: discover StreamHash runs %s: %w", pattern, err)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("accountsdb: no StreamHash run files match %s", pattern)
	}
	sort.Strings(paths)

	runs := make([]streamIndexRunFile, 0, len(paths))
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("accountsdb: stat StreamHash run %s: %w", path, err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("accountsdb: StreamHash run %s is not a regular file", path)
		}
		if info.Size()%StreamIndexRunRecordSize != 0 {
			return nil, fmt.Errorf(
				"accountsdb: StreamHash run %s has size %d, not a multiple of %d",
				path, info.Size(), StreamIndexRunRecordSize,
			)
		}
		runs = append(runs, streamIndexRunFile{path: path, info: info})
	}
	return &streamIndexRunSource{runs: runs}, nil
}

func (source *streamIndexRunSource) Scan(
	ctx context.Context,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) error {
	if ctx == nil {
		return errors.New("accountsdb: nil StreamHash run scan context")
	}
	if source == nil {
		return errors.New("accountsdb: nil StreamHash run source")
	}
	if visit == nil {
		return errors.New("accountsdb: nil StreamHash run visitor")
	}

	var previous solana.PublicKey
	havePrevious := false
	seen := uint64(0)
	for _, run := range source.runs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := scanStreamIndexRun(ctx, run, &previous, &havePrevious, &seen, visit); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func scanStreamIndexRun(
	ctx context.Context,
	run streamIndexRunFile,
	previous *solana.PublicKey,
	havePrevious *bool,
	seen *uint64,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) error {
	file, info, err := openStableRegularFile(run.path)
	if err != nil {
		return fmt.Errorf("accountsdb: open StreamHash run %s: %w", run.path, err)
	}
	defer file.Close()
	if run.info == nil || !os.SameFile(run.info, info) ||
		info.Size() != run.info.Size() || !info.ModTime().Equal(run.info.ModTime()) ||
		info.Size()%StreamIndexRunRecordSize != 0 {
		return fmt.Errorf(
			"accountsdb: StreamHash run %s changed or is malformed: size %d, expected %d",
			run.path, info.Size(), run.info.Size(),
		)
	}

	reader := bufio.NewReaderSize(file, 1<<20)
	var encoded [StreamIndexRunRecordSize]byte
	for offset := int64(0); offset < run.info.Size(); offset += StreamIndexRunRecordSize {
		if *seen%streamIndexContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if _, err := io.ReadFull(reader, encoded[:]); err != nil {
			return fmt.Errorf("accountsdb: read StreamHash run %s at offset %d: %w", run.path, offset, err)
		}

		key := solana.PublicKey(encoded[:32])
		if *havePrevious {
			switch cmp := bytes.Compare(previous[:], key[:]); {
			case cmp == 0:
				return fmt.Errorf("accountsdb: duplicate StreamHash run key %x in %s", key, run.path)
			case cmp > 0:
				return fmt.Errorf("accountsdb: out-of-order StreamHash run key %x in %s", key, run.path)
			}
		}

		var entry AccountIndexEntry
		entry.Unmarshal((*[24]byte)(encoded[32:]))
		if err := visit(key, entry); err != nil {
			return err
		}
		*previous = key
		*havePrevious = true
		*seen = *seen + 1
	}
	if err := validateStableRegularFile(file, run.path, info); err != nil {
		return fmt.Errorf("accountsdb: StreamHash run %s changed during scan: %w", run.path, err)
	}
	return nil
}

// streamIndexFileIdentity lives outside the immutable StreamHash file, in its
// publication manifest. StreamHash's own Verify deliberately excludes its
// header and RAM index; binding the complete file here prevents corruption or
// a mismatched generation from becoming a silent false miss.
type streamIndexFileIdentity struct {
	Size   uint64
	SHA256 [sha256.Size]byte
}

// StreamAccountIndex is an immutable StreamHash base account index. A lookup
// is only a location candidate: callers must compare the full public key in
// the appendvec before accepting it. The two-byte fingerprint rejects almost
// all non-members, but it is intentionally not an exact membership proof.
type StreamAccountIndex struct {
	idx      *streamhash.PayloadIndex
	extents  []streamIndexExtent
	hashSeed uint64
}

// OpenStreamAccountIndex memory-maps and structurally validates an immutable
// account index. AccountsDb callers additionally verify the whole-file
// identity stored in the publication manifest before calling this function.
func OpenStreamAccountIndex(path string) (*StreamAccountIndex, error) {
	idx, err := streamhash.OpenPayload(path)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: open StreamHash index: %w", err)
	}

	stats := idx.Stats()
	if stats.PayloadSize != streamIndexPayloadBytes ||
		stats.FingerprintSize != streamIndexFingerprintBytes ||
		stats.Algorithm != streamhash.AlgoPTRHash {
		_ = idx.Close()
		return nil, fmt.Errorf(
			"accountsdb: incompatible StreamHash index (algorithm=%s payload=%d fingerprint=%d)",
			stats.Algorithm, stats.PayloadSize, stats.FingerprintSize,
		)
	}

	extents, hashSeed, err := decodeStreamIndexMetadata(idx.UserMetadata())
	if err != nil {
		_ = idx.Close()
		return nil, err
	}
	if idx.NumKeys() != 0 && len(extents) == 0 {
		_ = idx.Close()
		return nil, errors.New("accountsdb: StreamHash index has keys but no extents")
	}

	return &StreamAccountIndex{idx: idx, extents: extents, hashSeed: hashSeed}, nil
}

func (db *AccountsDb) isAppendVecRetired(slot, fileID uint64) (bool, error) {
	if db == nil {
		return false, nil
	}
	if db.ProductionIndex != nil {
		return db.ProductionIndex.IsRetired(slot, fileID), nil
	}
	if db.Index == nil || db.BaseIndex == nil {
		return false, nil
	}
	return db.Index.IsRetired(slot, fileID), nil
}

func (db *AccountsDb) markAppendVecRetired(slot, fileID uint64) error {
	if err := db.applyAccountIndexMutations([]deltaIndexMutation{retireDeltaMutation(slot, fileID)}, nil); err != nil {
		return fmt.Errorf("accountsdb: retire appendvec %d.%d: %w", slot, fileID, err)
	}
	return nil
}

// ValidateStreamIndexArtifacts checks the fail-closed relationship between
// the immutable base and its standalone publication manifest.
func ValidateStreamIndexArtifacts(accountsDbDir string) error {
	return validateStreamIndexManifestArtifacts(accountsDbDir)
}

func computeStreamIndexFileIdentity(path string) (streamIndexFileIdentity, error) {
	var identity streamIndexFileIdentity
	size, digest, err := hashStableRegularFile(path)
	if err != nil {
		return identity, err
	}
	identity.Size = size
	identity.SHA256 = digest
	return identity, nil
}

func verifyStreamIndexFileIdentity(path string, want streamIndexFileIdentity) error {
	got, err := computeStreamIndexFileIdentity(path)
	if err != nil {
		return fmt.Errorf("accountsdb: hash StreamHash index %s: %w", path, err)
	}
	if got.Size != want.Size {
		return fmt.Errorf(
			"accountsdb: StreamHash index size mismatch: got %d, want %d",
			got.Size, want.Size,
		)
	}
	if !bytes.Equal(got.SHA256[:], want.SHA256[:]) {
		return errors.New("accountsdb: StreamHash index SHA-256 mismatch")
	}
	return nil
}

func newStreamIndexHashSeed() (uint64, error) {
	var encoded [8]byte
	if _, err := io.ReadFull(cryptorand.Reader, encoded[:]); err != nil {
		return 0, fmt.Errorf("accountsdb: generate StreamHash key seed: %w", err)
	}
	return binary.LittleEndian.Uint64(encoded[:]), nil
}

// streamIndexHashKey mixes all 32 public-key bytes into StreamHash's
// 16-byte key. A per-build random seed makes deliberately chosen trailing-byte
// variants no more useful than random misses; the builder rejects any actual
// 128-bit collision and retries with a fresh seed before publication.
func streamIndexHashKey(key solana.PublicKey, seed uint64) (dst [streamhash.MinKeySize]byte) {
	hash := xxh3.Hash128Seed(key[:], seed)
	binary.LittleEndian.PutUint64(dst[0:8], hash.Lo)
	binary.LittleEndian.PutUint64(dst[8:16], hash.Hi)
	return dst
}

// NumKeys returns the number of snapshot public keys in the immutable base.
func (idx *StreamAccountIndex) NumKeys() uint64 {
	if idx == nil || idx.idx == nil {
		return 0
	}
	return idx.idx.NumKeys()
}

// LookupCandidate returns an appendvec location candidate. Callers must verify
// the complete 32-byte public key at the returned location.
func (idx *StreamAccountIndex) LookupCandidate(pubkey solana.PublicKey) (AccountIndexEntry, bool, error) {
	if idx == nil || idx.idx == nil {
		return AccountIndexEntry{}, false, nil
	}

	hash := streamIndexHash(pubkey, idx.hashSeed)
	_, payload, err := idx.idx.QueryPayload(hash[:])
	if errors.Is(err, streamhash.ErrNotFound) {
		return AccountIndexEntry{}, false, nil
	}
	if err != nil {
		return AccountIndexEntry{}, false, fmt.Errorf("accountsdb: query StreamHash index: %w", err)
	}

	extentOrdinal := payload >> streamIndexRelativeBits
	if extentOrdinal >= uint64(len(idx.extents)) {
		return AccountIndexEntry{}, false, fmt.Errorf(
			"accountsdb: corrupt StreamHash payload extent %d >= %d",
			extentOrdinal, len(idx.extents),
		)
	}
	extent := idx.extents[extentOrdinal]
	relativeUnits := payload & streamIndexPayloadMask
	relativeOffset := relativeUnits * 8
	if extent.BaseOffset > ^uint64(0)-relativeOffset {
		return AccountIndexEntry{}, false, fmt.Errorf(
			"accountsdb: corrupt StreamHash payload offset %d+%d overflows uint64",
			extent.BaseOffset, relativeOffset,
		)
	}

	return AccountIndexEntry{
		Slot:   extent.Slot,
		FileId: extent.FileID,
		Offset: extent.BaseOffset + relativeOffset,
	}, true, nil
}

// Close releases the memory mapping backing the immutable index.
func (idx *StreamAccountIndex) Close() error {
	if idx == nil || idx.idx == nil {
		return nil
	}
	err := idx.idx.Close()
	idx.idx = nil
	idx.extents = nil
	idx.hashSeed = 0
	return err
}

// BuildStreamAccountIndex converts an exact sorted account-index source into an
// immutable StreamHash base. Every public key is first mixed, with a random
// per-build seed, from 32 bytes to StreamHash's 16-byte input. The unsorted
// builder detects a true 128-bit collision; in that case the build retries with
// a new seed and publishes only a collision-free generation. The immutable
// file is checksum-verified before atomic publication, and the standalone
// manifest is published last.
func BuildStreamAccountIndex(
	ctx context.Context,
	source StreamIndexSource,
	outputPath string,
	workers int,
) (StreamIndexBuildStats, error) {
	var stats StreamIndexBuildStats
	if ctx == nil {
		return stats, errors.New("accountsdb: nil StreamHash build context")
	}
	if source == nil {
		return stats, errors.New("accountsdb: nil StreamHash source")
	}
	if outputPath == "" {
		return stats, errors.New("accountsdb: empty StreamHash output path")
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}

	parentDir := filepath.Dir(outputPath)
	partialPath := outputPath + ".partial"
	if err := os.Remove(partialPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return stats, fmt.Errorf("accountsdb: remove stale StreamHash partial: %w", err)
	}
	// StreamHash removes its output on most build failures, but verification
	// happens after Finish. Cover that final failure window as well.
	defer os.Remove(partialPath)

	extents := make([]streamIndexExtent, 0, 1024)
	extentOrdinals := make(map[streamIndexExtent]uint32, 1024)
	var err error
	stats, err = scanStreamIndexSource(ctx, source, &extents, extentOrdinals)
	if err != nil {
		return stats, err
	}
	stats.Extents = uint64(len(extents))

	built := false
	for attempt := 0; attempt < streamIndexMaxBuildAttempts; attempt++ {
		hashSeed, seedErr := streamIndexSeedSource()
		if seedErr != nil {
			return stats, seedErr
		}
		buildErr := buildStreamIndexAttempt(
			ctx, source, partialPath, parentDir, stats.BaseKeys,
			extents, extentOrdinals, hashSeed, workers,
		)
		if errors.Is(buildErr, streamhash.ErrDuplicateKey) ||
			errors.Is(buildErr, streamhash.ErrIndistinguishableHashes) {
			stats.HashSeedRetries++
			_ = os.Remove(partialPath)
			continue
		}
		if buildErr != nil {
			return stats, buildErr
		}
		built = true
		break
	}
	if !built {
		return stats, fmt.Errorf(
			"accountsdb: failed to build collision-free StreamHash index after %d seeds",
			streamIndexMaxBuildAttempts,
		)
	}

	verificationIndex, err := OpenStreamAccountIndex(partialPath)
	if err != nil {
		return stats, fmt.Errorf("accountsdb: reopen built StreamHash index: %w", err)
	}
	if verificationIndex.NumKeys() != stats.BaseKeys {
		gotKeys := verificationIndex.NumKeys()
		_ = verificationIndex.Close()
		return stats, fmt.Errorf(
			"accountsdb: built StreamHash key count %d != expected %d",
			gotKeys, stats.BaseKeys,
		)
	}
	if err := verificationIndex.idx.Verify(); err != nil {
		_ = verificationIndex.Close()
		return stats, fmt.Errorf("accountsdb: verify built StreamHash index: %w", err)
	}
	if err := verificationIndex.Close(); err != nil {
		return stats, fmt.Errorf("accountsdb: close verified StreamHash index: %w", err)
	}
	identity, err := computeStreamIndexFileIdentity(partialPath)
	if err != nil {
		return stats, fmt.Errorf("accountsdb: identify built StreamHash index: %w", err)
	}

	if err := os.Rename(partialPath, outputPath); err != nil {
		return stats, fmt.Errorf("accountsdb: publish StreamHash index: %w", err)
	}
	if err := fsyncDir(parentDir); err != nil {
		return stats, fmt.Errorf("accountsdb: sync published StreamHash index: %w", err)
	}

	// Publish the externally stored whole-file identity only after the base is
	// durable. The manifest rename is the final commit point for the pair.
	if err := publishStreamIndexManifest(parentDir, identity); err != nil {
		return stats, fmt.Errorf("accountsdb: publish StreamHash manifest: %w", err)
	}

	return stats, nil
}

func scanStreamIndexSource(
	ctx context.Context,
	source StreamIndexSource,
	extents *[]streamIndexExtent,
	extentOrdinals map[streamIndexExtent]uint32,
) (StreamIndexBuildStats, error) {
	var stats StreamIndexBuildStats
	var previous solana.PublicKey
	havePrevious := false
	err := source.Scan(ctx, func(key solana.PublicKey, entry AccountIndexEntry) error {
		if stats.InputKeys%streamIndexContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if havePrevious {
			switch cmp := bytes.Compare(previous[:], key[:]); {
			case cmp == 0:
				return fmt.Errorf("accountsdb: duplicate StreamHash source key %x", key)
			case cmp > 0:
				return fmt.Errorf("accountsdb: out-of-order StreamHash source key %x", key)
			}
		}
		previous = key
		havePrevious = true
		stats.InputKeys++

		if entry.Offset%8 != 0 {
			return fmt.Errorf(
				"accountsdb: StreamHash source offset %d for %x is not 8-byte aligned",
				entry.Offset, key,
			)
		}

		extent := extentForAccountIndexEntry(entry)
		if _, ok := extentOrdinals[extent]; !ok {
			if len(*extents) >= streamIndexMaxExtents {
				return fmt.Errorf("accountsdb: StreamHash extent count exceeds %d", streamIndexMaxExtents)
			}
			extentOrdinals[extent] = uint32(len(*extents))
			*extents = append(*extents, extent)
		}
		stats.BaseKeys++
		return nil
	})
	if err != nil {
		return stats, fmt.Errorf("accountsdb: scan StreamHash source: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	return stats, nil
}

func buildStreamIndexAttempt(
	ctx context.Context,
	source StreamIndexSource,
	partialPath string,
	tempDir string,
	totalKeys uint64,
	extents []streamIndexExtent,
	extentOrdinals map[streamIndexExtent]uint32,
	hashSeed uint64,
	workers int,
) error {
	metadata, err := encodeStreamIndexMetadata(extents, hashSeed)
	if err != nil {
		return err
	}
	opts := []streamhash.BuildOption{
		streamhash.WithAlgorithm(streamhash.AlgoPTRHash),
		streamhash.WithPayload(streamIndexPayloadBytes),
		streamhash.WithFingerprint(streamIndexFingerprintBytes),
		streamhash.WithGlobalSeed(hashSeed ^ 0x9e3779b97f4a7c15),
		streamhash.WithMetadata(metadata),
	}
	if workers > 0 {
		opts = append(opts, streamhash.WithWorkers(workers))
	}
	builder, err := streamhash.NewUnsortedBuilder(ctx, partialPath, totalKeys, tempDir, opts...)
	if err != nil {
		return fmt.Errorf("accountsdb: create unsorted StreamHash builder: %w", err)
	}
	buildErr := populateStreamIndex(ctx, source, extentOrdinals, builder, hashSeed, totalKeys)
	closeErr := builder.Close()
	if buildErr != nil {
		return fmt.Errorf("accountsdb: populate prehashed StreamHash index: %w", errors.Join(buildErr, closeErr))
	}
	if closeErr != nil {
		return fmt.Errorf("accountsdb: close StreamHash builder: %w", closeErr)
	}
	return nil
}

func populateStreamIndex(
	ctx context.Context,
	source StreamIndexSource,
	extentOrdinals map[streamIndexExtent]uint32,
	builder *streamhash.UnsortedBuilder,
	hashSeed uint64,
	totalKeys uint64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if totalKeys == 0 {
		return builder.Finish()
	}
	// Use one ingestion writer for arbitrary valid key distributions. Splitting
	// writers by original pubkey ranges would violate StreamHash's per-writer
	// spill sizing when a synthetic workload deliberately skews those ranges.
	// Finish still uses the requested worker count to build blocks in parallel.
	if err := addStreamIndexSource(ctx, source, extentOrdinals, hashSeed, totalKeys, builder.AddKey); err != nil {
		return err
	}
	return builder.Finish()
}

func addStreamIndexSource(
	ctx context.Context,
	source StreamIndexSource,
	extentOrdinals map[streamIndexExtent]uint32,
	hashSeed uint64,
	totalKeys uint64,
	addKey func([]byte, uint64) error,
) error {
	var previous solana.PublicKey
	havePrevious := false
	seen := uint64(0)
	err := source.Scan(ctx, func(key solana.PublicKey, entry AccountIndexEntry) error {
		if seen%streamIndexContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if havePrevious {
			switch cmp := bytes.Compare(previous[:], key[:]); {
			case cmp == 0:
				return fmt.Errorf("accountsdb: duplicate StreamHash source key %x", key)
			case cmp > 0:
				return fmt.Errorf("accountsdb: out-of-order StreamHash source key %x", key)
			}
		}
		previous = key
		havePrevious = true
		seen++

		if entry.Offset%8 != 0 {
			return fmt.Errorf(
				"accountsdb: StreamHash source offset %d for %x is not 8-byte aligned",
				entry.Offset, key,
			)
		}
		extent := extentForAccountIndexEntry(entry)
		ordinal, ok := extentOrdinals[extent]
		if !ok {
			return fmt.Errorf("accountsdb: missing StreamHash extent for %x", key)
		}
		relativeUnits := (entry.Offset - extent.BaseOffset) / 8
		if relativeUnits > streamIndexPayloadMask {
			return fmt.Errorf("accountsdb: StreamHash relative offset %d overflows 24 bits", relativeUnits)
		}
		payload := uint64(ordinal)<<streamIndexRelativeBits | relativeUnits
		hashed := streamIndexHash(key, hashSeed)
		if err := addKey(hashed[:], payload); err != nil {
			return fmt.Errorf("accountsdb: add prehashed StreamHash key %x: %w", key, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("accountsdb: populate StreamHash index: %w", err)
	}
	if seen != totalKeys {
		return fmt.Errorf(
			"accountsdb: StreamHash source changed between scans: got %d keys, want %d",
			seen, totalKeys,
		)
	}
	return nil
}

func extentForAccountIndexEntry(entry AccountIndexEntry) streamIndexExtent {
	return streamIndexExtent{
		Slot:       entry.Slot,
		FileID:     entry.FileId,
		BaseOffset: entry.Offset &^ (streamIndexExtentSize - 1),
	}
}

func encodeStreamIndexMetadata(extents []streamIndexExtent, hashSeed uint64) ([]byte, error) {
	if len(extents) > streamIndexMaxExtents {
		return nil, fmt.Errorf("accountsdb: StreamHash extent count exceeds %d", streamIndexMaxExtents)
	}
	metadata := make([]byte, streamIndexMetadataHeaderSize+len(extents)*streamIndexMetadataRecordSize)
	copy(metadata[:8], streamIndexMetadataMagic[:])
	binary.LittleEndian.PutUint32(metadata[8:12], streamIndexMetadataVersion)
	binary.LittleEndian.PutUint32(metadata[12:16], uint32(len(extents)))
	binary.LittleEndian.PutUint64(metadata[16:24], hashSeed)
	for i, extent := range extents {
		if extent.BaseOffset%streamIndexExtentSize != 0 {
			return nil, fmt.Errorf("accountsdb: StreamHash extent %d base offset is not 128 MiB aligned", i)
		}
		offset := streamIndexMetadataHeaderSize + i*streamIndexMetadataRecordSize
		binary.LittleEndian.PutUint64(metadata[offset:offset+8], extent.Slot)
		binary.LittleEndian.PutUint64(metadata[offset+8:offset+16], extent.FileID)
		binary.LittleEndian.PutUint64(metadata[offset+16:offset+24], extent.BaseOffset)
	}
	return metadata, nil
}

func decodeStreamIndexMetadata(metadata []byte) ([]streamIndexExtent, uint64, error) {
	if len(metadata) < streamIndexMetadataHeaderSize {
		return nil, 0, fmt.Errorf("accountsdb: StreamHash metadata is %d bytes, want at least %d", len(metadata), streamIndexMetadataHeaderSize)
	}
	if !bytes.Equal(metadata[:8], streamIndexMetadataMagic[:]) {
		return nil, 0, errors.New("accountsdb: invalid StreamHash metadata magic")
	}
	version := binary.LittleEndian.Uint32(metadata[8:12])
	if version != streamIndexMetadataVersion {
		return nil, 0, fmt.Errorf("accountsdb: unsupported StreamHash metadata version %d", version)
	}
	count := uint64(binary.LittleEndian.Uint32(metadata[12:16]))
	if count > streamIndexMaxExtents {
		return nil, 0, fmt.Errorf("accountsdb: StreamHash metadata extent count %d exceeds %d", count, streamIndexMaxExtents)
	}
	wantSize := uint64(streamIndexMetadataHeaderSize) + count*streamIndexMetadataRecordSize
	if uint64(len(metadata)) != wantSize {
		return nil, 0, fmt.Errorf("accountsdb: StreamHash metadata is %d bytes, want %d", len(metadata), wantSize)
	}
	hashSeed := binary.LittleEndian.Uint64(metadata[16:24])

	extents := make([]streamIndexExtent, int(count))
	for i := range extents {
		offset := streamIndexMetadataHeaderSize + i*streamIndexMetadataRecordSize
		extent := streamIndexExtent{
			Slot:       binary.LittleEndian.Uint64(metadata[offset : offset+8]),
			FileID:     binary.LittleEndian.Uint64(metadata[offset+8 : offset+16]),
			BaseOffset: binary.LittleEndian.Uint64(metadata[offset+16 : offset+24]),
		}
		if extent.BaseOffset%streamIndexExtentSize != 0 {
			return nil, 0, fmt.Errorf("accountsdb: StreamHash metadata extent %d has unaligned base offset %d", i, extent.BaseOffset)
		}
		extents[i] = extent
	}
	return extents, hashSeed, nil
}
