package accountsdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const (
	// RootIndexCatalogFileName is intentionally distinct from the V1
	// accounts_index.manifest. A store cannot accidentally interpret one format
	// as the other.
	RootIndexCatalogFileName = "accounts_index.root"

	rootIndexCatalogVersion    = uint32(2)
	rootIndexCatalogHeaderSize = uint32(112)
	rootIndexCatalogMaxSize    = uint64(64 << 20)
	indexCatalogMaxPathBytes   = 1024
	indexCatalogArtifactSize   = 4 + 8 + sha256.Size
	indexCatalogShardFixedSize = 4 + 4 + 8 + 8 + 8 + 8
)

var (
	rootIndexCatalogMagic = [8]byte{'M', 'I', 'T', 'H', 'R', 'C', '0', '1'}
	rootIndexCatalogCRC   = crc32.MakeTable(crc32.Castagnoli)

	ErrInvalidRootIndexCatalog = errors.New("accountsdb: invalid root index catalog")
	ErrInvalidCatalogArtifact  = errors.New("accountsdb: invalid index catalog artifact")
)

// IndexCatalogArtifact identifies one immutable file selected by a root
// catalog. RelativePath is always a canonical slash-separated path below the
// AccountsDB directory. Size and SHA256 bind the complete artifact, not merely
// the data region understood by its owning library.
type IndexCatalogArtifact struct {
	RelativePath string
	Size         uint64
	SHA256       [sha256.Size]byte
}

// IndexCatalogShard selects the immutable base and optional exact delta for
// one persistent shard. BaseRecords is the cold sorted enumeration sidecar;
// DeltaRecords carries exact keys, live locators, and tombstones.
type IndexCatalogShard struct {
	ShardID uint32

	BaseGeneration      uint64
	BaseCoveredSequence uint64
	BaseIndex           IndexCatalogArtifact
	BaseRecords         IndexCatalogArtifact

	DeltaGeneration      uint64
	DeltaCoveredSequence uint64
	DeltaIndex           IndexCatalogArtifact
	DeltaRecords         IndexCatalogArtifact
}

func (shard IndexCatalogShard) effectiveCoveredSequence() uint64 {
	if shard.DeltaGeneration != 0 {
		return shard.DeltaCoveredSequence
	}
	return shard.BaseCoveredSequence
}

// RootIndexCatalog is the single durable selector for an AccountsDB V2 index
// view. Publishing individual shard files does not make them visible; only an
// atomic publication of this catalog does.
//
// CoveredSequence is the account-index WAL reclamation watermark: it must
// equal the minimum effective WAL sequence covered by every shard. It is a
// different sequence space from RootedBatchSequence: compaction and other
// index-only publications advance the WAL without advancing the fold batch.
// Lineage identifies the rooted chain incarnation against which background
// builds were started. A rebase publisher must additionally compare Generation
// before replacing the root.
type RootIndexCatalog struct {
	Version uint32

	Generation          uint64
	ShardCount          uint32
	CoveredSequence     uint64
	RootedBatchSequence uint64
	RootedSlot          uint64
	Lineage             [32]byte
	RoutingKey          PersistentIndexRoutingKey
	SharedExtentCatalog IndexCatalogArtifact
	Shards              []IndexCatalogShard
}

// NewRootIndexCatalog initializes the format fields that must agree with the
// shard array. Callers still populate lineage, artifact identities, generation
// and coverage before validation or publication.
func NewRootIndexCatalog(shardCount int) (*RootIndexCatalog, error) {
	if err := ValidatePersistentIndexShardCount(shardCount); err != nil {
		return nil, err
	}
	routingKey, err := NewPersistentIndexRoutingKey()
	if err != nil {
		return nil, err
	}
	catalog := &RootIndexCatalog{
		Version:    rootIndexCatalogVersion,
		ShardCount: uint32(shardCount),
		RoutingKey: routingKey,
		Shards:     make([]IndexCatalogShard, shardCount),
	}
	for i := range catalog.Shards {
		catalog.Shards[i].ShardID = uint32(i)
	}
	return catalog, nil
}

// Clone returns a deep copy suitable for constructing a replacement catalog
// without mutating the catalog held by active readers.
func (catalog *RootIndexCatalog) Clone() *RootIndexCatalog {
	if catalog == nil {
		return nil
	}
	cloned := *catalog
	cloned.Shards = append([]IndexCatalogShard(nil), catalog.Shards...)
	return &cloned
}

func (catalog *RootIndexCatalog) Validate() error {
	if catalog == nil {
		return fmt.Errorf("%w: nil catalog", ErrInvalidRootIndexCatalog)
	}
	if catalog.Version != rootIndexCatalogVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrInvalidRootIndexCatalog, catalog.Version)
	}
	if catalog.Generation == 0 {
		return fmt.Errorf("%w: zero generation", ErrInvalidRootIndexCatalog)
	}
	if err := ValidatePersistentIndexShardCount(int(catalog.ShardCount)); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRootIndexCatalog, err)
	}
	if len(catalog.Shards) != int(catalog.ShardCount) {
		return fmt.Errorf(
			"%w: %d shard records for shard count %d",
			ErrInvalidRootIndexCatalog,
			len(catalog.Shards),
			catalog.ShardCount,
		)
	}
	if allZero(catalog.Lineage[:]) {
		return fmt.Errorf("%w: zero lineage", ErrInvalidRootIndexCatalog)
	}
	if allZero(catalog.RoutingKey[:]) {
		return fmt.Errorf("%w: zero routing key", ErrInvalidRootIndexCatalog)
	}
	if err := validateRequiredCatalogArtifact(catalog.SharedExtentCatalog); err != nil {
		return fmt.Errorf("%w: shared extent catalog: %v", ErrInvalidRootIndexCatalog, err)
	}

	paths := make(map[string]string, 1+len(catalog.Shards)*4)
	if err := addUniqueCatalogArtifactPath(paths, "shared extent catalog", catalog.SharedExtentCatalog); err != nil {
		return err
	}
	minimumCovered := uint64(math.MaxUint64)
	for i := range catalog.Shards {
		shard := &catalog.Shards[i]
		if shard.ShardID != uint32(i) {
			return fmt.Errorf(
				"%w: shard record %d names shard %d",
				ErrInvalidRootIndexCatalog,
				i,
				shard.ShardID,
			)
		}
		if shard.BaseGeneration == 0 {
			return fmt.Errorf("%w: shard %d has zero base generation", ErrInvalidRootIndexCatalog, i)
		}
		if err := validateRequiredCatalogArtifact(shard.BaseIndex); err != nil {
			return fmt.Errorf("%w: shard %d base index: %v", ErrInvalidRootIndexCatalog, i, err)
		}
		if err := validateRequiredCatalogArtifact(shard.BaseRecords); err != nil {
			return fmt.Errorf("%w: shard %d base records: %v", ErrInvalidRootIndexCatalog, i, err)
		}
		if err := addUniqueCatalogArtifactPath(paths, fmt.Sprintf("shard %d base index", i), shard.BaseIndex); err != nil {
			return err
		}
		if err := addUniqueCatalogArtifactPath(paths, fmt.Sprintf("shard %d base records", i), shard.BaseRecords); err != nil {
			return err
		}

		switch {
		case shard.DeltaGeneration == 0:
			if shard.DeltaCoveredSequence != 0 || !shard.DeltaIndex.isZero() || !shard.DeltaRecords.isZero() {
				return fmt.Errorf(
					"%w: shard %d has delta state without a delta generation",
					ErrInvalidRootIndexCatalog,
					i,
				)
			}
		case shard.DeltaCoveredSequence < shard.BaseCoveredSequence:
			return fmt.Errorf(
				"%w: shard %d delta sequence %d precedes base sequence %d",
				ErrInvalidRootIndexCatalog,
				i,
				shard.DeltaCoveredSequence,
				shard.BaseCoveredSequence,
			)
		default:
			if err := validateRequiredCatalogArtifact(shard.DeltaIndex); err != nil {
				return fmt.Errorf("%w: shard %d delta index: %v", ErrInvalidRootIndexCatalog, i, err)
			}
			if err := validateRequiredCatalogArtifact(shard.DeltaRecords); err != nil {
				return fmt.Errorf("%w: shard %d delta records: %v", ErrInvalidRootIndexCatalog, i, err)
			}
			if err := addUniqueCatalogArtifactPath(paths, fmt.Sprintf("shard %d delta index", i), shard.DeltaIndex); err != nil {
				return err
			}
			if err := addUniqueCatalogArtifactPath(paths, fmt.Sprintf("shard %d delta records", i), shard.DeltaRecords); err != nil {
				return err
			}
		}
		minimumCovered = min(minimumCovered, shard.effectiveCoveredSequence())
	}
	if catalog.CoveredSequence != minimumCovered {
		return fmt.Errorf(
			"%w: covered sequence %d does not equal shard minimum %d",
			ErrInvalidRootIndexCatalog,
			catalog.CoveredSequence,
			minimumCovered,
		)
	}
	return nil
}

func (artifact IndexCatalogArtifact) isZero() bool {
	return artifact.RelativePath == "" && artifact.Size == 0 && allZero(artifact.SHA256[:])
}

func validateRequiredCatalogArtifact(artifact IndexCatalogArtifact) error {
	if artifact.RelativePath == "" {
		return fmt.Errorf("%w: empty path", ErrInvalidCatalogArtifact)
	}
	if err := validateIndexCatalogRelativePath(artifact.RelativePath); err != nil {
		return err
	}
	if artifact.Size == 0 || artifact.Size > math.MaxInt64 {
		return fmt.Errorf("%w: invalid size %d for %q", ErrInvalidCatalogArtifact, artifact.Size, artifact.RelativePath)
	}
	if allZero(artifact.SHA256[:]) {
		return fmt.Errorf("%w: zero SHA-256 for %q", ErrInvalidCatalogArtifact, artifact.RelativePath)
	}
	return nil
}

func addUniqueCatalogArtifactPath(paths map[string]string, owner string, artifact IndexCatalogArtifact) error {
	if previous, exists := paths[artifact.RelativePath]; exists {
		return fmt.Errorf(
			"%w: artifact path %q is shared by %s and %s",
			ErrInvalidRootIndexCatalog,
			artifact.RelativePath,
			previous,
			owner,
		)
	}
	paths[artifact.RelativePath] = owner
	return nil
}

func validateIndexCatalogRelativePath(relative string) error {
	if relative == "" || len(relative) > indexCatalogMaxPathBytes {
		return fmt.Errorf("%w: path length %d", ErrInvalidCatalogArtifact, len(relative))
	}
	if strings.IndexByte(relative, 0) >= 0 || strings.ContainsRune(relative, '\\') {
		return fmt.Errorf("%w: unsafe path %q", ErrInvalidCatalogArtifact, relative)
	}
	if path.IsAbs(relative) || filepath.IsAbs(relative) || path.Clean(relative) != relative {
		return fmt.Errorf("%w: non-canonical relative path %q", ErrInvalidCatalogArtifact, relative)
	}
	for _, component := range strings.Split(relative, "/") {
		if component == "" || component == "." || component == ".." || len(component) > 255 {
			return fmt.Errorf("%w: unsafe path component in %q", ErrInvalidCatalogArtifact, relative)
		}
		for i := 0; i < len(component); i++ {
			c := component[i]
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
				(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
				continue
			}
			return fmt.Errorf("%w: unsupported byte %#x in path %q", ErrInvalidCatalogArtifact, c, relative)
		}
	}
	return nil
}

func allZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func (catalog *RootIndexCatalog) MarshalBinary() ([]byte, error) {
	if err := catalog.Validate(); err != nil {
		return nil, err
	}

	encoded := make([]byte, rootIndexCatalogHeaderSize)
	copy(encoded[0:8], rootIndexCatalogMagic[:])
	binary.LittleEndian.PutUint32(encoded[8:12], rootIndexCatalogVersion)
	binary.LittleEndian.PutUint32(encoded[12:16], rootIndexCatalogHeaderSize)
	// [16:24] total encoded length is populated after the variable body.
	binary.LittleEndian.PutUint64(encoded[24:32], catalog.Generation)
	binary.LittleEndian.PutUint32(encoded[32:36], catalog.ShardCount)
	// [36:40] flags/reserved stays zero.
	binary.LittleEndian.PutUint64(encoded[40:48], catalog.CoveredSequence)
	binary.LittleEndian.PutUint64(encoded[48:56], catalog.RootedBatchSequence)
	binary.LittleEndian.PutUint64(encoded[56:64], catalog.RootedSlot)
	copy(encoded[64:96], catalog.Lineage[:])
	copy(encoded[96:112], catalog.RoutingKey[:])

	encoded = appendCatalogArtifact(encoded, catalog.SharedExtentCatalog)
	for i := range catalog.Shards {
		shard := &catalog.Shards[i]
		encoded = binary.LittleEndian.AppendUint32(encoded, shard.ShardID)
		encoded = binary.LittleEndian.AppendUint32(encoded, 0) // reserved
		encoded = binary.LittleEndian.AppendUint64(encoded, shard.BaseGeneration)
		encoded = binary.LittleEndian.AppendUint64(encoded, shard.BaseCoveredSequence)
		encoded = binary.LittleEndian.AppendUint64(encoded, shard.DeltaGeneration)
		encoded = binary.LittleEndian.AppendUint64(encoded, shard.DeltaCoveredSequence)
		encoded = appendCatalogArtifact(encoded, shard.BaseIndex)
		encoded = appendCatalogArtifact(encoded, shard.BaseRecords)
		encoded = appendCatalogArtifact(encoded, shard.DeltaIndex)
		encoded = appendCatalogArtifact(encoded, shard.DeltaRecords)
	}
	if uint64(len(encoded))+4 > rootIndexCatalogMaxSize {
		return nil, fmt.Errorf("%w: encoded size exceeds %d", ErrInvalidRootIndexCatalog, rootIndexCatalogMaxSize)
	}
	encoded = binary.LittleEndian.AppendUint32(encoded, crc32.Checksum(encoded, rootIndexCatalogCRC))
	binary.LittleEndian.PutUint64(encoded[16:24], uint64(len(encoded)))
	// The length field is covered by the checksum, so recompute after filling it.
	binary.LittleEndian.PutUint32(encoded[len(encoded)-4:], crc32.Checksum(encoded[:len(encoded)-4], rootIndexCatalogCRC))
	return encoded, nil
}

func appendCatalogArtifact(encoded []byte, artifact IndexCatalogArtifact) []byte {
	encoded = binary.LittleEndian.AppendUint32(encoded, uint32(len(artifact.RelativePath)))
	encoded = binary.LittleEndian.AppendUint64(encoded, artifact.Size)
	encoded = append(encoded, artifact.SHA256[:]...)
	encoded = append(encoded, artifact.RelativePath...)
	return encoded
}

type rootIndexCatalogDecoder struct {
	data []byte
	off  uint64
}

func (decoder *rootIndexCatalogDecoder) take(size uint64) ([]byte, error) {
	if size > uint64(len(decoder.data)) || decoder.off > uint64(len(decoder.data))-size {
		return nil, io.ErrUnexpectedEOF
	}
	start := decoder.off
	decoder.off += size
	return decoder.data[start:decoder.off], nil
}

func (decoder *rootIndexCatalogDecoder) u32() (uint32, error) {
	data, err := decoder.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data), nil
}

func (decoder *rootIndexCatalogDecoder) u64() (uint64, error) {
	data, err := decoder.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(data), nil
}

func (decoder *rootIndexCatalogDecoder) artifact() (IndexCatalogArtifact, error) {
	var artifact IndexCatalogArtifact
	pathLength, err := decoder.u32()
	if err != nil {
		return artifact, err
	}
	if pathLength > indexCatalogMaxPathBytes {
		return artifact, fmt.Errorf("artifact path length %d exceeds %d", pathLength, indexCatalogMaxPathBytes)
	}
	if artifact.Size, err = decoder.u64(); err != nil {
		return artifact, err
	}
	hash, err := decoder.take(sha256.Size)
	if err != nil {
		return artifact, err
	}
	copy(artifact.SHA256[:], hash)
	pathBytes, err := decoder.take(uint64(pathLength))
	if err != nil {
		return artifact, err
	}
	artifact.RelativePath = string(pathBytes)
	return artifact, nil
}

func UnmarshalRootIndexCatalog(encoded []byte) (*RootIndexCatalog, error) {
	minimumSize := uint64(rootIndexCatalogHeaderSize) + indexCatalogArtifactSize + 4
	if uint64(len(encoded)) < minimumSize || uint64(len(encoded)) > rootIndexCatalogMaxSize {
		return nil, fmt.Errorf("%w: encoded size %d", ErrInvalidRootIndexCatalog, len(encoded))
	}
	wantCRC := binary.LittleEndian.Uint32(encoded[len(encoded)-4:])
	if got := crc32.Checksum(encoded[:len(encoded)-4], rootIndexCatalogCRC); got != wantCRC {
		return nil, fmt.Errorf(
			"%w: CRC mismatch: got %08x, want %08x",
			ErrInvalidRootIndexCatalog,
			got,
			wantCRC,
		)
	}
	if !bytes.Equal(encoded[:8], rootIndexCatalogMagic[:]) {
		return nil, fmt.Errorf("%w: bad magic %x", ErrInvalidRootIndexCatalog, encoded[:8])
	}
	if version := binary.LittleEndian.Uint32(encoded[8:12]); version != rootIndexCatalogVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrInvalidRootIndexCatalog, version)
	}
	if headerSize := binary.LittleEndian.Uint32(encoded[12:16]); headerSize != rootIndexCatalogHeaderSize {
		return nil, fmt.Errorf("%w: header size %d", ErrInvalidRootIndexCatalog, headerSize)
	}
	if totalSize := binary.LittleEndian.Uint64(encoded[16:24]); totalSize != uint64(len(encoded)) {
		return nil, fmt.Errorf(
			"%w: declared size %d, actual %d",
			ErrInvalidRootIndexCatalog,
			totalSize,
			len(encoded),
		)
	}
	if !allZero(encoded[36:40]) {
		return nil, fmt.Errorf("%w: non-zero header flags", ErrInvalidRootIndexCatalog)
	}

	shardCount := binary.LittleEndian.Uint32(encoded[32:36])
	if shardCount > MaxPersistentIndexShards {
		return nil, fmt.Errorf("%w: shard count %d exceeds maximum", ErrInvalidRootIndexCatalog, shardCount)
	}
	if err := ValidatePersistentIndexShardCount(int(shardCount)); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidRootIndexCatalog, err)
	}
	minimumForShardCount := uint64(rootIndexCatalogHeaderSize) + indexCatalogArtifactSize +
		uint64(shardCount)*(indexCatalogShardFixedSize+4*indexCatalogArtifactSize) + 4
	if uint64(len(encoded)) < minimumForShardCount {
		return nil, fmt.Errorf(
			"%w: encoded size %d is below minimum %d for %d shards",
			ErrInvalidRootIndexCatalog,
			len(encoded),
			minimumForShardCount,
			shardCount,
		)
	}

	catalog := &RootIndexCatalog{
		Version:             rootIndexCatalogVersion,
		Generation:          binary.LittleEndian.Uint64(encoded[24:32]),
		ShardCount:          shardCount,
		CoveredSequence:     binary.LittleEndian.Uint64(encoded[40:48]),
		RootedBatchSequence: binary.LittleEndian.Uint64(encoded[48:56]),
		RootedSlot:          binary.LittleEndian.Uint64(encoded[56:64]),
		Shards:              make([]IndexCatalogShard, int(shardCount)),
	}
	copy(catalog.Lineage[:], encoded[64:96])
	copy(catalog.RoutingKey[:], encoded[96:112])
	decoder := rootIndexCatalogDecoder{data: encoded[:len(encoded)-4], off: uint64(rootIndexCatalogHeaderSize)}
	var err error
	if catalog.SharedExtentCatalog, err = decoder.artifact(); err != nil {
		return nil, fmt.Errorf("%w: decode shared extent catalog: %v", ErrInvalidRootIndexCatalog, err)
	}
	for i := range catalog.Shards {
		shard := &catalog.Shards[i]
		if shard.ShardID, err = decoder.u32(); err != nil {
			return nil, fmt.Errorf("%w: decode shard %d ID: %v", ErrInvalidRootIndexCatalog, i, err)
		}
		reserved, err := decoder.u32()
		if err != nil {
			return nil, fmt.Errorf("%w: decode shard %d reserved field: %v", ErrInvalidRootIndexCatalog, i, err)
		}
		if reserved != 0 {
			return nil, fmt.Errorf("%w: shard %d has non-zero flags", ErrInvalidRootIndexCatalog, i)
		}
		if shard.BaseGeneration, err = decoder.u64(); err != nil {
			return nil, fmt.Errorf("%w: decode shard %d base generation: %v", ErrInvalidRootIndexCatalog, i, err)
		}
		if shard.BaseCoveredSequence, err = decoder.u64(); err != nil {
			return nil, fmt.Errorf("%w: decode shard %d base sequence: %v", ErrInvalidRootIndexCatalog, i, err)
		}
		if shard.DeltaGeneration, err = decoder.u64(); err != nil {
			return nil, fmt.Errorf("%w: decode shard %d delta generation: %v", ErrInvalidRootIndexCatalog, i, err)
		}
		if shard.DeltaCoveredSequence, err = decoder.u64(); err != nil {
			return nil, fmt.Errorf("%w: decode shard %d delta sequence: %v", ErrInvalidRootIndexCatalog, i, err)
		}
		artifacts := []*IndexCatalogArtifact{
			&shard.BaseIndex,
			&shard.BaseRecords,
			&shard.DeltaIndex,
			&shard.DeltaRecords,
		}
		for artifactOrdinal, artifact := range artifacts {
			*artifact, err = decoder.artifact()
			if err != nil {
				return nil, fmt.Errorf(
					"%w: decode shard %d artifact %d: %v",
					ErrInvalidRootIndexCatalog,
					i,
					artifactOrdinal,
					err,
				)
			}
		}
	}
	if decoder.off != uint64(len(decoder.data)) {
		return nil, fmt.Errorf(
			"%w: %d trailing bytes",
			ErrInvalidRootIndexCatalog,
			uint64(len(decoder.data))-decoder.off,
		)
	}
	if err := catalog.Validate(); err != nil {
		return nil, err
	}
	return catalog, nil
}

// ResolveIndexCatalogArtifactPath validates an artifact path and resolves it
// below root. It does not inspect the filesystem.
func ResolveIndexCatalogArtifactPath(root string, artifact IndexCatalogArtifact) (string, error) {
	if root == "" {
		return "", fmt.Errorf("%w: empty AccountsDB root", ErrInvalidCatalogArtifact)
	}
	if err := validateRequiredCatalogArtifact(artifact); err != nil {
		return "", err
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("%w: resolve AccountsDB root: %v", ErrInvalidCatalogArtifact, err)
	}
	resolved := filepath.Join(rootAbs, filepath.FromSlash(artifact.RelativePath))
	relativeBack, err := filepath.Rel(rootAbs, resolved)
	if err != nil || relativeBack == ".." || strings.HasPrefix(relativeBack, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: path %q escapes root", ErrInvalidCatalogArtifact, artifact.RelativePath)
	}
	return resolved, nil
}

// validateIndexCatalogArtifactPathComponents rejects symlinked directory
// components below the AccountsDB root. Lexical containment alone is
// insufficient here: "generation/shard" still escapes if generation is a
// symlink. The final component is checked again by openStableRegularFile so a
// concurrent replacement cannot turn this walk into a time-of-check bypass.
func validateIndexCatalogArtifactPathComponents(root, relative string) error {
	if err := validateIndexCatalogRelativePath(relative); err != nil {
		return err
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	rootInfo, err := os.Lstat(rootAbs)
	if err != nil {
		return err
	}
	if !rootInfo.IsDir() {
		return fmt.Errorf("AccountsDB root %s is not a real directory", rootAbs)
	}
	current := rootAbs
	components := strings.Split(relative, "/")
	for i, component := range components {
		current = filepath.Join(current, filepath.FromSlash(component))
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if i+1 < len(components) {
			if !info.IsDir() {
				return fmt.Errorf("artifact path component %s is not a real directory", current)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("artifact %s is not a regular file", current)
		}
	}
	return nil
}

// ComputeIndexCatalogArtifact hashes one existing immutable artifact for a
// catalog. Symlinks and non-regular files are rejected.
func ComputeIndexCatalogArtifact(root, relative string) (IndexCatalogArtifact, error) {
	artifact := IndexCatalogArtifact{RelativePath: relative}
	// Use a temporary non-zero identity solely to apply strict path validation
	// in the shared resolver before the file has been measured.
	artifact.Size = 1
	artifact.SHA256[0] = 1
	resolved, err := ResolveIndexCatalogArtifactPath(root, artifact)
	if err != nil {
		return IndexCatalogArtifact{}, err
	}
	if err := validateIndexCatalogArtifactPathComponents(root, relative); err != nil {
		return IndexCatalogArtifact{}, fmt.Errorf("%w: inspect %q: %v", ErrInvalidCatalogArtifact, relative, err)
	}
	size, digest, err := hashStableRegularFile(resolved)
	if err != nil {
		return IndexCatalogArtifact{}, fmt.Errorf("accountsdb: identify index artifact %q: %w", relative, err)
	}
	artifact.Size = size
	artifact.SHA256 = digest
	return artifact, nil
}

func VerifyIndexCatalogArtifact(root string, artifact IndexCatalogArtifact) error {
	measured, err := ComputeIndexCatalogArtifact(root, artifact.RelativePath)
	if err != nil {
		return err
	}
	if measured.Size != artifact.Size || !bytes.Equal(measured.SHA256[:], artifact.SHA256[:]) {
		return fmt.Errorf("%w: identity mismatch for %q", ErrInvalidCatalogArtifact, artifact.RelativePath)
	}
	return nil
}

// WriteRootIndexCatalogAtomic writes and fsyncs a temporary file, renames it
// over the canonical selector, then fsyncs the containing directory. The
// immutable artifacts referenced by catalog must be durable before this call.
// Production callers must also hold the store-wide exclusive lifetime lock;
// initialization and an opened ProductionAccountIndex provide that ownership.
func WriteRootIndexCatalogAtomic(accountsDBRoot string, catalog *RootIndexCatalog) (retErr error) {
	_, retErr = writeRootIndexCatalogAtomic(accountsDBRoot, catalog)
	return retErr
}

// writeRootIndexCatalogAtomic reports whether rename installed the selector,
// even when the following directory fsync fails. A caller that owns newly
// selected artifacts must stop destructive rollback as soon as renamed is
// true: after that point durability is ambiguous and deleting either possible
// root's dependencies would make recovery unsafe.
func writeRootIndexCatalogAtomic(
	accountsDBRoot string,
	catalog *RootIndexCatalog,
) (renamed bool, retErr error) {
	return writeRootIndexCatalogAtomicWithDirSync(accountsDBRoot, catalog, fsyncDir)
}

func writeRootIndexCatalogAtomicWithDirSync(
	accountsDBRoot string,
	catalog *RootIndexCatalog,
	syncDirectory func(string) error,
) (renamed bool, retErr error) {
	if syncDirectory == nil {
		return false, errors.New("accountsdb: nil root index catalog directory sync")
	}
	encoded, err := catalog.MarshalBinary()
	if err != nil {
		return false, err
	}
	if accountsDBRoot == "" {
		return false, fmt.Errorf("%w: empty AccountsDB root", ErrInvalidRootIndexCatalog)
	}
	rootInfo, err := os.Lstat(accountsDBRoot)
	if err != nil {
		return false, fmt.Errorf("accountsdb: stat root index catalog directory: %w", err)
	}
	if !rootInfo.IsDir() {
		return false, fmt.Errorf("accountsdb: root index catalog parent is not a real directory")
	}

	finalPath := filepath.Join(accountsDBRoot, RootIndexCatalogFileName)
	if info, statErr := os.Lstat(finalPath); statErr == nil && !info.Mode().IsRegular() {
		return false, fmt.Errorf("accountsdb: root index catalog target is not a regular file")
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return false, fmt.Errorf("accountsdb: inspect root index catalog target: %w", statErr)
	}
	file, err := os.CreateTemp(accountsDBRoot, RootIndexCatalogFileName+".tmp-")
	if err != nil {
		return false, fmt.Errorf("accountsdb: create root index catalog temporary file: %w", err)
	}
	temporaryPath := file.Name()
	defer func() {
		if cleanupErr := os.Remove(temporaryPath); retErr == nil && cleanupErr != nil && !errors.Is(cleanupErr, os.ErrNotExist) {
			retErr = cleanupErr
		}
	}()

	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, file.Close())
		}
	}()
	if err := file.Chmod(0o644); err != nil {
		return false, fmt.Errorf("accountsdb: set root index catalog permissions: %w", err)
	}
	written, err := io.Copy(file, bytes.NewReader(encoded))
	if err != nil {
		return false, fmt.Errorf("accountsdb: write root index catalog: %w", err)
	}
	if written != int64(len(encoded)) {
		return false, fmt.Errorf("accountsdb: short root index catalog write: wrote %d of %d", written, len(encoded))
	}
	if err := file.Sync(); err != nil {
		return false, fmt.Errorf("accountsdb: sync root index catalog: %w", err)
	}
	if err := file.Close(); err != nil {
		return false, fmt.Errorf("accountsdb: close root index catalog: %w", err)
	}
	closed = true
	if err := os.Rename(temporaryPath, finalPath); err != nil {
		return false, fmt.Errorf("accountsdb: publish root index catalog: %w", err)
	}
	renamed = true
	if err := syncDirectory(accountsDBRoot); err != nil {
		return true, fmt.Errorf("accountsdb: sync root index catalog directory: %w", err)
	}
	return true, nil
}

func ReadRootIndexCatalog(accountsDBRoot string) (*RootIndexCatalog, error) {
	if accountsDBRoot == "" {
		return nil, fmt.Errorf("%w: empty AccountsDB root", ErrInvalidRootIndexCatalog)
	}
	rootInfo, err := os.Lstat(accountsDBRoot)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: stat root index catalog directory: %w", err)
	}
	if !rootInfo.IsDir() {
		return nil, fmt.Errorf("%w: root index catalog parent is not a real directory", ErrInvalidRootIndexCatalog)
	}
	catalogPath := filepath.Join(accountsDBRoot, RootIndexCatalogFileName)
	file, info, err := openStableRegularFile(catalogPath)
	if err != nil {
		return nil, fmt.Errorf("%w: open root index catalog: %v", ErrInvalidRootIndexCatalog, err)
	}
	defer file.Close()
	if info.Size() <= 0 || uint64(info.Size()) > rootIndexCatalogMaxSize {
		return nil, fmt.Errorf("%w: catalog is not a bounded non-empty regular file", ErrInvalidRootIndexCatalog)
	}
	encoded, err := io.ReadAll(io.LimitReader(file, int64(rootIndexCatalogMaxSize)+1))
	if err != nil {
		return nil, fmt.Errorf("accountsdb: read root index catalog: %w", err)
	}
	if int64(len(encoded)) != info.Size() || uint64(len(encoded)) > rootIndexCatalogMaxSize {
		return nil, fmt.Errorf("%w: catalog changed while reading", ErrInvalidRootIndexCatalog)
	}
	if err := validateStableRegularFile(file, catalogPath, info); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidRootIndexCatalog, err)
	}
	return UnmarshalRootIndexCatalog(encoded)
}
