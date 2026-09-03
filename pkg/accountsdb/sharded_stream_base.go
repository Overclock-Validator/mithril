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
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stellar/streamhash"
)

const (
	shardedBasePayloadBytes     = 6
	shardedBaseFingerprintBytes = 1
	shardedBaseScanRecordSize   = 32 + shardedBasePayloadBytes
	shardedBaseMetadataSize     = 256
	shardedBaseFormatVersion    = uint32(3)
	shardedBaseBuildDirAttempts = 16
	// A full keyed partition can have one active writer per shard because a
	// bytewise-sorted source does not group keyed shard IDs. Divide this fixed
	// budget across writers so increasing the persisted shard count cannot turn
	// sidecar construction into unbounded buffered heap growth.
	shardedBaseScanBufferBudget = 64 << 20
	shardedBaseScanBufferMin    = 16 << 10
	shardedBaseScanBufferMax    = 1 << 20
)

var (
	shardedBaseIndexMetadataMagic = [8]byte{'M', 'I', 'T', 'H', 'B', 'I', '0', '1'}
	shardedBaseScanMagic          = [8]byte{'M', 'I', 'T', 'H', 'B', 'S', '0', '1'}
	shardedBaseCRC                = crc32.MakeTable(crc32.Castagnoli)

	ErrInvalidShardedStreamBase = errors.New("accountsdb: invalid sharded StreamHash base")
	// ErrShardedStreamBaseBuildCleanup marks a failed attempt to remove a
	// private, unpublished base generation after its build failed. Retrying
	// that build would allocate a fresh generation directory and could leak one
	// partial directory per attempt, so mutable maintenance must fail closed.
	ErrShardedStreamBaseBuildCleanup = errors.New(
		"accountsdb: clean up unpublished sharded StreamHash base build",
	)
)

// ShardedStreamBaseShardArtifacts identifies one immutable shard pair. Index
// is the one-byte-fingerprint StreamHash lookup structure; Scan is its exact,
// sorted and enumerable 32-byte-key + six-byte-locator sidecar.
type ShardedStreamBaseShardArtifacts struct {
	ShardID    uint32
	KeyCount   uint64
	Generation uint64
	Index      IndexCatalogArtifact
	Scan       IndexCatalogArtifact
}

// ShardedStreamBaseBuildResult contains newly published but not yet selected
// immutable artifacts. The caller may place these identities into a root
// catalog only after any generation/lineage conflict checks it requires.
type ShardedStreamBaseBuildResult struct {
	Generation            uint64
	ShardCount            uint32
	RoutingKey            PersistentIndexRoutingKey
	ExtentCatalog         *PersistentExtentCatalog
	ExtentCatalogArtifact IndexCatalogArtifact
	// OwnsExtentCatalogArtifact is false only when a rebuild introduced no
	// extents and deliberately reused the caller's immutable catalog artifact.
	OwnsExtentCatalogArtifact bool
	Shards                    []ShardedStreamBaseShardArtifacts
}

// ShardedStreamBaseShardBuildResult is the rolling-rebase equivalent for one
// shard. ExtentCatalog may be an append-only extension; untouched shard files
// remain compatible because they bind their required ordinal prefix.
type ShardedStreamBaseShardBuildResult struct {
	Generation                uint64
	ShardCount                uint32
	RoutingKey                PersistentIndexRoutingKey
	ExtentCatalog             *PersistentExtentCatalog
	ExtentCatalogArtifact     IndexCatalogArtifact
	OwnsExtentCatalogArtifact bool
	Shard                     ShardedStreamBaseShardArtifacts
}

type shardedBaseMetadata struct {
	ShardCount        uint32
	ShardID           uint32
	Generation        uint64
	KeyCount          uint64
	HashSeed          uint64
	RoutingKey        PersistentIndexRoutingKey
	CatalogGeneration uint64
	CatalogBinding    extentCatalogBinding
	// ScanSeal is present only in the root-authenticated StreamHash metadata.
	// It binds the exact sidecar digest to the strong Linux identity observed
	// after the sidecar was completely hashed and semantically consumed by the
	// builder. The sidecar header leaves it zero to avoid self-reference.
	ScanSeal shardedBaseScanSeal
	BodyCRC  uint32
}

// shardedBaseScanSeal is a persistent validation receipt for an immutable
// exact sidecar. SHA256 is authenticated by the root-selected StreamHash file;
// the remaining fields prove that the current no-follow opened inode is still
// the inode whose bytes were validated. ctime is essential: unlike mtime it
// cannot be restored with ordinary filesystem APIs after an in-place write.
// If any field is unavailable or disagrees, open falls back to a complete
// semantic/CRC/SHA-256 pass rather than trusting weaker metadata.
type shardedBaseScanSeal struct {
	SHA256           [sha256.Size]byte
	Device           uint64
	Inode            uint64
	Size             uint64
	MTimeSeconds     int64
	MTimeNanoseconds int64
	CTimeSeconds     int64
	CTimeNanoseconds int64
}

type shardedBaseScanFileIdentity struct {
	Device           uint64
	Inode            uint64
	Size             uint64
	MTimeSeconds     int64
	MTimeNanoseconds int64
	CTimeSeconds     int64
	CTimeNanoseconds int64
}

func (seal shardedBaseScanSeal) isZero() bool {
	return seal == (shardedBaseScanSeal{})
}

func validateShardedBaseScanSealFields(seal shardedBaseScanSeal) error {
	if seal.isZero() {
		return nil
	}
	if allZero(seal.SHA256[:]) || seal.Inode == 0 || seal.Size == 0 {
		return fmt.Errorf("%w: incomplete exact-sidecar identity seal", ErrInvalidShardedStreamBase)
	}
	if seal.MTimeNanoseconds < 0 || seal.MTimeNanoseconds >= int64(time.Second) ||
		seal.CTimeNanoseconds < 0 || seal.CTimeNanoseconds >= int64(time.Second) {
		return fmt.Errorf("%w: invalid exact-sidecar identity timestamp", ErrInvalidShardedStreamBase)
	}
	return nil
}

func shardedBaseScanSealFromFile(
	file *os.File,
	digest [sha256.Size]byte,
) (shardedBaseScanSeal, error) {
	var seal shardedBaseScanSeal
	if file == nil {
		return seal, errors.New("accountsdb: nil exact-sidecar file for identity seal")
	}
	identity, err := readShardedBaseScanFileIdentity(file)
	if err != nil {
		return seal, err
	}
	seal = shardedBaseScanSeal{
		SHA256:           digest,
		Device:           identity.Device,
		Inode:            identity.Inode,
		Size:             identity.Size,
		MTimeSeconds:     identity.MTimeSeconds,
		MTimeNanoseconds: identity.MTimeNanoseconds,
		CTimeSeconds:     identity.CTimeSeconds,
		CTimeNanoseconds: identity.CTimeNanoseconds,
	}
	if err := validateShardedBaseScanSealFields(seal); err != nil {
		return shardedBaseScanSeal{}, err
	}
	return seal, nil
}

func captureShardedBaseScanSeal(
	path string,
	artifact IndexCatalogArtifact,
) (_ shardedBaseScanSeal, retErr error) {
	file, info, err := openStableRegularFile(path)
	if err != nil {
		return shardedBaseScanSeal{}, err
	}
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	if info.Size() <= 0 || uint64(info.Size()) != artifact.Size {
		return shardedBaseScanSeal{}, fmt.Errorf(
			"%w: exact-sidecar identity size %d does not match artifact %d",
			ErrInvalidShardedStreamBase, info.Size(), artifact.Size,
		)
	}
	before, err := shardedBaseScanSealFromFile(file, artifact.SHA256)
	if err != nil {
		// A strong ctime-bearing identity is an optimization, never a
		// correctness prerequisite. The builder and every later opener fall
		// back to complete semantic/CRC/SHA verification when unavailable.
		if stableErr := validateStableRegularFile(file, path, info); stableErr != nil {
			return shardedBaseScanSeal{}, stableErr
		}
		return shardedBaseScanSeal{}, nil
	}
	if err := validateStableRegularFile(file, path, info); err != nil {
		return shardedBaseScanSeal{}, err
	}
	after, err := shardedBaseScanSealFromFile(file, artifact.SHA256)
	if err != nil {
		return shardedBaseScanSeal{}, err
	}
	if before != after {
		return shardedBaseScanSeal{}, fmt.Errorf("%w: exact sidecar changed while sealing", ErrInvalidShardedStreamBase)
	}
	return after, nil
}

// ShardedStreamBaseShard is one opened immutable shard. Lookups are
// probabilistic membership candidates because the index deliberately uses one
// fingerprint byte. Callers must compare the full key in the appendvec. The
// exact scan sidecar is reopened by verified inode identity only while a scan
// is active, avoiding one steady descriptor per shard.
type ShardedStreamBaseShard struct {
	mu        sync.RWMutex
	idx       *streamhash.PayloadIndex
	indexPath string
	indexInfo os.FileInfo
	scanPath  string
	scanInfo  os.FileInfo
	scanSeal  shardedBaseScanSeal
	// scanFullValidations records whether startup had to take the complete
	// semantic/CRC/SHA fallback rather than the authenticated O(1) identity
	// path. It is retained as an exact regression-test counter; exact consumers
	// do not perform a redundant validation pass when scanSeal is strong.
	scanFullValidations atomic.Uint64
	metadata            shardedBaseMetadata
	scanMetadata        shardedBaseMetadata
	catalog             atomic.Pointer[PersistentExtentCatalog]
	router              PersistentIndexShardRouter
	closed              bool
}

func (shard *ShardedStreamBaseShard) ShardID() uint32 {
	if shard == nil {
		return 0
	}
	return shard.metadata.ShardID
}

func (shard *ShardedStreamBaseShard) Generation() uint64 {
	if shard == nil {
		return 0
	}
	return shard.metadata.Generation
}

func (shard *ShardedStreamBaseShard) NumKeys() uint64 {
	if shard == nil {
		return 0
	}
	return shard.metadata.KeyCount
}

// LookupCandidate resolves a packed location after enforcing shard routing and
// catalog-prefix compatibility. A true result is not exact membership proof.
func (shard *ShardedStreamBaseShard) LookupCandidate(pubkey solana.PublicKey) (AccountIndexEntry, bool, error) {
	if shard == nil {
		return AccountIndexEntry{}, false, nil
	}
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.lookupCandidatePinned(pubkey)
}

// lookupCandidatePinned skips the lifecycle mutex because its caller already
// owns the immutable generation that contains shard. Generation retirement
// cannot close the StreamHash mapping until that pin is released. Standalone
// callers without such a pin must use LookupCandidate.
func (shard *ShardedStreamBaseShard) lookupCandidatePinned(pubkey solana.PublicKey) (AccountIndexEntry, bool, error) {
	if shard == nil {
		return AccountIndexEntry{}, false, nil
	}
	catalog := shard.catalog.Load()
	if shard.closed || shard.idx == nil || catalog == nil {
		return AccountIndexEntry{}, false, nil
	}
	if routed := shard.router.Shard(pubkey); routed != shard.metadata.ShardID {
		return AccountIndexEntry{}, false, fmt.Errorf(
			"%w: key routes to shard %d, opened shard is %d",
			ErrInvalidShardedStreamBase,
			routed,
			shard.metadata.ShardID,
		)
	}
	hashed := streamIndexHashKey(pubkey, shard.metadata.HashSeed)
	_, locator, err := shard.idx.QueryPayload(hashed[:])
	if errors.Is(err, streamhash.ErrNotFound) {
		return AccountIndexEntry{}, false, nil
	}
	if err != nil {
		return AccountIndexEntry{}, false, fmt.Errorf("accountsdb: query sharded StreamHash base: %w", err)
	}
	entry, err := catalog.UnpackAccountIndexEntry(locator)
	if err != nil {
		return AccountIndexEntry{}, false, fmt.Errorf("%w: decode StreamHash locator: %v", ErrInvalidShardedStreamBase, err)
	}
	return entry, true, nil
}

// Scan enumerates exact keys and locations in bytewise key order without
// loading the sidecar into memory.
func (shard *ShardedStreamBaseShard) Scan(
	ctx context.Context,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) error {
	if shard == nil {
		return fmt.Errorf("%w: closed shard", ErrInvalidShardedStreamBase)
	}
	if visit == nil {
		return fmt.Errorf("%w: nil scan visitor", ErrInvalidShardedStreamBase)
	}
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	catalog := shard.catalog.Load()
	if catalog == nil {
		return fmt.Errorf("%w: closed shard", ErrInvalidShardedStreamBase)
	}
	scan, err := shard.openScanLockedContext(ctx)
	if err != nil {
		return err
	}
	scanErr := scanShardedBaseSidecar(
		ctx, scan, shard.scanPath, shard.scanMetadata, catalog, shard.router, visit,
	)
	identityErr := shard.validateOpenScanIdentity(scan)
	return errors.Join(scanErr, identityErr, scan.Close())
}

func (shard *ShardedStreamBaseShard) Close() error {
	if shard == nil {
		return nil
	}
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.closed {
		return nil
	}
	// Nil is the catalog pointer's closed sentinel. Publish it before closing
	// the mmap so a concurrent nonblocking rebind can never resurrect this
	// shard after teardown begins.
	shard.catalog.Store(nil)
	shard.closed = true
	var indexErr error
	if shard.idx != nil {
		indexErr = shard.idx.Close()
		shard.idx = nil
	}
	shard.indexInfo = nil
	shard.scanInfo = nil
	shard.scanSeal = shardedBaseScanSeal{}
	return indexErr
}

// rebindExtentCatalog moves a retained immutable base shard to a newer,
// append-compatible catalog object. Rolling rebases share untouched base
// resources across root generations; without rebinding, each of those shards
// would keep the complete catalog object from the generation in which it was
// last rebuilt. Since the catalog itself contains an O(extents) slice and map,
// that would retain as many as one full copy per shard.
//
// Existing locator ordinals are immutable within a lineage, and the shard's
// persisted prefix binding proves that catalog decodes every locator exactly
// as the catalog used at build time did. It is therefore safe for readers of a
// pinned older root generation to observe the newer superset too. Rebinding is
// an atomic pointer swap rather than a shard-lock writer: a slow disk scan or
// visitor may retain its prior immutable catalog locally, but cannot stall an
// unrelated shard's rebase publication. Close publishes nil and the CAS loop
// prevents any later rebind from resurrecting a closed shard.
func (shard *ShardedStreamBaseShard) rebindExtentCatalog(catalog *PersistentExtentCatalog) error {
	if shard == nil || catalog == nil {
		return fmt.Errorf("%w: nil shard/catalog rebind", ErrInvalidShardedStreamBase)
	}
	prior := shard.catalog.Load()
	if prior == nil {
		return fmt.Errorf("%w: rebind closed shard", ErrInvalidShardedStreamBase)
	}
	if err := catalog.validateBinding(shard.metadata.CatalogBinding); err != nil {
		return fmt.Errorf(
			"%w: rebind shard %d extent catalog: %v",
			ErrInvalidShardedStreamBase,
			shard.metadata.ShardID,
			err,
		)
	}
	for {
		if shard.catalog.CompareAndSwap(prior, catalog) {
			return nil
		}
		prior = shard.catalog.Load()
		if prior == nil {
			return fmt.Errorf("%w: rebind closed shard", ErrInvalidShardedStreamBase)
		}
	}
}

// openScanLocked opens the immutable exact sidecar only for the duration of a
// scan. The caller holds mu.RLock, excluding Close. Both path and opened-file
// identities must match the inode validated at generation open, so replacing
// a sidecar with even byte-identical content fails closed.
func (shard *ShardedStreamBaseShard) openScanLocked() (*os.File, error) {
	return shard.openScanLockedContext(context.Background())
}

func (shard *ShardedStreamBaseShard) openScanLockedContext(ctx context.Context) (*os.File, error) {
	if ctx == nil {
		return nil, errors.New("accountsdb: nil exact-sidecar validation context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if shard.closed || shard.scanPath == "" || shard.scanInfo == nil || shard.catalog.Load() == nil {
		return nil, fmt.Errorf("%w: closed shard", ErrInvalidShardedStreamBase)
	}
	file, openedInfo, err := openStableRegularFile(shard.scanPath)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: open sharded base scan %s: %w", shard.scanPath, err)
	}
	if !os.SameFile(shard.scanInfo, openedInfo) ||
		openedInfo.Size() != shard.scanInfo.Size() ||
		!openedInfo.ModTime().Equal(shard.scanInfo.ModTime()) {
		closeErr := file.Close()
		if closeErr != nil {
			return nil, fmt.Errorf(
				"accountsdb: verify opened sharded base scan %s: %w",
				shard.scanPath,
				closeErr,
			)
		}
		return nil, fmt.Errorf("%w: scan sidecar path identity changed", ErrInvalidShardedStreamBase)
	}
	if !shard.scanSeal.isZero() {
		current, identityErr := shardedBaseScanSealFromFile(file, shard.scanSeal.SHA256)
		if identityErr != nil || current != shard.scanSeal {
			closeErr := file.Close()
			return nil, errors.Join(
				fmt.Errorf("%w: exact sidecar strong identity changed", ErrInvalidShardedStreamBase),
				identityErr,
				closeErr,
			)
		}
	}
	return file, nil
}

func (shard *ShardedStreamBaseShard) validateOpenScanIdentity(file *os.File) error {
	if shard == nil || file == nil || shard.scanInfo == nil {
		return fmt.Errorf("%w: missing exact sidecar identity", ErrInvalidShardedStreamBase)
	}
	if err := validateStableRegularFile(file, shard.scanPath, shard.scanInfo); err != nil {
		return fmt.Errorf("%w: exact sidecar path identity changed while reading: %v", ErrInvalidShardedStreamBase, err)
	}
	if shard.scanSeal.isZero() {
		return nil
	}
	current, err := shardedBaseScanSealFromFile(file, shard.scanSeal.SHA256)
	if err != nil {
		return err
	}
	if current != shard.scanSeal {
		return fmt.Errorf("%w: exact sidecar strong identity changed while reading", ErrInvalidShardedStreamBase)
	}
	return nil
}

// validateRetainedArtifactPathsStable proves that both immutable paths still
// name the files which were completely verified when this shard was opened.
// Snapshot bootstrap calls this immediately before adopting the retained mmap
// into the live index.  It is intentionally O(1) metadata work per shard.
func (shard *ShardedStreamBaseShard) validateRetainedArtifactPathsStable() error {
	if shard == nil {
		return fmt.Errorf("%w: nil retained base shard", ErrInvalidShardedStreamBase)
	}
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if shard.closed || shard.idx == nil || shard.catalog.Load() == nil {
		return fmt.Errorf("%w: closed retained base shard", ErrInvalidShardedStreamBase)
	}
	if err := validateRegularFilePathIdentity(shard.indexPath, shard.indexInfo); err != nil {
		return fmt.Errorf("%w: retained StreamHash index identity: %v", ErrInvalidShardedStreamBase, err)
	}
	if err := validateRegularFilePathIdentity(shard.scanPath, shard.scanInfo); err != nil {
		return fmt.Errorf("%w: retained exact scan identity: %v", ErrInvalidShardedStreamBase, err)
	}
	if !shard.scanSeal.isZero() {
		file, _, err := openStableRegularFile(shard.scanPath)
		if err != nil {
			return fmt.Errorf("%w: reopen retained exact scan identity: %v", ErrInvalidShardedStreamBase, err)
		}
		identityErr := shard.validateOpenScanIdentity(file)
		closeErr := file.Close()
		if identityErr != nil || closeErr != nil {
			return fmt.Errorf(
				"%w: retained exact scan strong identity: %v",
				ErrInvalidShardedStreamBase,
				errors.Join(identityErr, closeErr),
			)
		}
	}
	return nil
}

func encodeShardedBaseMetadata(
	magic [8]byte,
	recordSize uint32,
	metadata shardedBaseMetadata,
) ([shardedBaseMetadataSize]byte, error) {
	var encoded [shardedBaseMetadataSize]byte
	if err := validateShardedBaseMetadata(metadata); err != nil {
		return encoded, err
	}
	if magic == shardedBaseIndexMetadataMagic {
		if recordSize != 0 || metadata.BodyCRC != 0 {
			return encoded, fmt.Errorf("%w: invalid index metadata fields", ErrInvalidShardedStreamBase)
		}
		if !metadata.ScanSeal.isZero() {
			if err := validateShardedBaseScanSealFields(metadata.ScanSeal); err != nil {
				return encoded, err
			}
		}
	} else if magic == shardedBaseScanMagic {
		if recordSize != shardedBaseScanRecordSize || !metadata.ScanSeal.isZero() {
			return encoded, fmt.Errorf("%w: invalid scan record size %d", ErrInvalidShardedStreamBase, recordSize)
		}
	} else {
		return encoded, fmt.Errorf("%w: unknown metadata magic", ErrInvalidShardedStreamBase)
	}

	copy(encoded[0:8], magic[:])
	binary.LittleEndian.PutUint32(encoded[8:12], shardedBaseFormatVersion)
	binary.LittleEndian.PutUint32(encoded[12:16], shardedBaseMetadataSize)
	binary.LittleEndian.PutUint32(encoded[16:20], recordSize)
	binary.LittleEndian.PutUint32(encoded[20:24], metadata.ShardCount)
	binary.LittleEndian.PutUint32(encoded[24:28], metadata.ShardID)
	binary.LittleEndian.PutUint64(encoded[32:40], metadata.Generation)
	binary.LittleEndian.PutUint64(encoded[40:48], metadata.KeyCount)
	binary.LittleEndian.PutUint64(encoded[48:56], metadata.HashSeed)
	binary.LittleEndian.PutUint64(encoded[56:64], metadata.CatalogGeneration)
	binary.LittleEndian.PutUint32(encoded[64:68], metadata.CatalogBinding.RequiredCount)
	copy(encoded[72:104], metadata.CatalogBinding.Lineage[:])
	copy(encoded[104:136], metadata.CatalogBinding.PrefixSHA256[:])
	copy(encoded[136:152], metadata.RoutingKey[:])
	copy(encoded[152:184], metadata.ScanSeal.SHA256[:])
	binary.LittleEndian.PutUint64(encoded[184:192], metadata.ScanSeal.Device)
	binary.LittleEndian.PutUint64(encoded[192:200], metadata.ScanSeal.Inode)
	binary.LittleEndian.PutUint64(encoded[200:208], metadata.ScanSeal.Size)
	binary.LittleEndian.PutUint64(encoded[208:216], uint64(metadata.ScanSeal.MTimeSeconds))
	binary.LittleEndian.PutUint64(encoded[216:224], uint64(metadata.ScanSeal.MTimeNanoseconds))
	binary.LittleEndian.PutUint64(encoded[224:232], uint64(metadata.ScanSeal.CTimeSeconds))
	binary.LittleEndian.PutUint64(encoded[232:240], uint64(metadata.ScanSeal.CTimeNanoseconds))
	binary.LittleEndian.PutUint32(encoded[248:252], metadata.BodyCRC)
	binary.LittleEndian.PutUint32(encoded[252:256], crc32.Checksum(encoded[:252], shardedBaseCRC))
	return encoded, nil
}

func decodeShardedBaseMetadata(
	encoded []byte,
	wantMagic [8]byte,
	wantRecordSize uint32,
) (shardedBaseMetadata, error) {
	var metadata shardedBaseMetadata
	if len(encoded) != shardedBaseMetadataSize {
		return metadata, fmt.Errorf("%w: metadata length %d", ErrInvalidShardedStreamBase, len(encoded))
	}
	if !bytes.Equal(encoded[0:8], wantMagic[:]) {
		return metadata, fmt.Errorf("%w: bad metadata magic", ErrInvalidShardedStreamBase)
	}
	if version := binary.LittleEndian.Uint32(encoded[8:12]); version != shardedBaseFormatVersion {
		return metadata, fmt.Errorf("%w: unsupported format version %d", ErrInvalidShardedStreamBase, version)
	}
	if size := binary.LittleEndian.Uint32(encoded[12:16]); size != shardedBaseMetadataSize {
		return metadata, fmt.Errorf("%w: metadata size %d", ErrInvalidShardedStreamBase, size)
	}
	if size := binary.LittleEndian.Uint32(encoded[16:20]); size != wantRecordSize {
		return metadata, fmt.Errorf("%w: record size %d", ErrInvalidShardedStreamBase, size)
	}
	if !allZero(encoded[28:32]) || !allZero(encoded[68:72]) || !allZero(encoded[240:248]) {
		return metadata, fmt.Errorf("%w: non-zero reserved metadata", ErrInvalidShardedStreamBase)
	}
	if got, want := crc32.Checksum(encoded[:252], shardedBaseCRC), binary.LittleEndian.Uint32(encoded[252:256]); got != want {
		return metadata, fmt.Errorf("%w: metadata CRC mismatch", ErrInvalidShardedStreamBase)
	}
	metadata.ShardCount = binary.LittleEndian.Uint32(encoded[20:24])
	metadata.ShardID = binary.LittleEndian.Uint32(encoded[24:28])
	metadata.Generation = binary.LittleEndian.Uint64(encoded[32:40])
	metadata.KeyCount = binary.LittleEndian.Uint64(encoded[40:48])
	metadata.HashSeed = binary.LittleEndian.Uint64(encoded[48:56])
	metadata.CatalogGeneration = binary.LittleEndian.Uint64(encoded[56:64])
	metadata.CatalogBinding.RequiredCount = binary.LittleEndian.Uint32(encoded[64:68])
	copy(metadata.CatalogBinding.Lineage[:], encoded[72:104])
	copy(metadata.CatalogBinding.PrefixSHA256[:], encoded[104:136])
	copy(metadata.RoutingKey[:], encoded[136:152])
	copy(metadata.ScanSeal.SHA256[:], encoded[152:184])
	metadata.ScanSeal.Device = binary.LittleEndian.Uint64(encoded[184:192])
	metadata.ScanSeal.Inode = binary.LittleEndian.Uint64(encoded[192:200])
	metadata.ScanSeal.Size = binary.LittleEndian.Uint64(encoded[200:208])
	metadata.ScanSeal.MTimeSeconds = int64(binary.LittleEndian.Uint64(encoded[208:216]))
	metadata.ScanSeal.MTimeNanoseconds = int64(binary.LittleEndian.Uint64(encoded[216:224]))
	metadata.ScanSeal.CTimeSeconds = int64(binary.LittleEndian.Uint64(encoded[224:232]))
	metadata.ScanSeal.CTimeNanoseconds = int64(binary.LittleEndian.Uint64(encoded[232:240]))
	metadata.BodyCRC = binary.LittleEndian.Uint32(encoded[248:252])
	if err := validateShardedBaseMetadata(metadata); err != nil {
		return shardedBaseMetadata{}, err
	}
	if wantMagic == shardedBaseIndexMetadataMagic {
		if metadata.BodyCRC != 0 {
			return shardedBaseMetadata{}, fmt.Errorf("%w: index metadata has invalid scan seal/body CRC", ErrInvalidShardedStreamBase)
		}
		if !metadata.ScanSeal.isZero() {
			if err := validateShardedBaseScanSealFields(metadata.ScanSeal); err != nil {
				return shardedBaseMetadata{}, err
			}
		}
	} else if !metadata.ScanSeal.isZero() {
		return shardedBaseMetadata{}, fmt.Errorf("%w: scan header contains an identity seal", ErrInvalidShardedStreamBase)
	}
	return metadata, nil
}

func validateShardedBaseMetadata(metadata shardedBaseMetadata) error {
	if err := ValidatePersistentIndexShardCount(int(metadata.ShardCount)); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidShardedStreamBase, err)
	}
	if metadata.ShardID >= metadata.ShardCount {
		return fmt.Errorf("%w: shard %d >= count %d", ErrInvalidShardedStreamBase, metadata.ShardID, metadata.ShardCount)
	}
	if metadata.Generation == 0 || metadata.CatalogGeneration == 0 {
		return fmt.Errorf("%w: zero generation", ErrInvalidShardedStreamBase)
	}
	if metadata.CatalogBinding.RequiredCount > streamIndexMaxExtents {
		return fmt.Errorf("%w: required extent count exceeds 24 bits", ErrInvalidShardedStreamBase)
	}
	if allZero(metadata.CatalogBinding.Lineage[:]) || allZero(metadata.CatalogBinding.PrefixSHA256[:]) {
		return fmt.Errorf("%w: zero catalog binding", ErrInvalidShardedStreamBase)
	}
	if allZero(metadata.RoutingKey[:]) {
		return fmt.Errorf("%w: zero routing key", ErrInvalidShardedStreamBase)
	}
	return nil
}

// BuildShardedStreamBase builds every shard using the fixed production router.
// Source is scanned twice regardless of account count, then each bounded-size
// sidecar independently feeds StreamHash. Empty shards produce valid artifacts.
func BuildShardedStreamBase(
	ctx context.Context,
	source StreamIndexSource,
	accountsDBRoot string,
	generation uint64,
	routingKey PersistentIndexRoutingKey,
	previousCatalog *PersistentExtentCatalog,
	workers int,
) (*ShardedStreamBaseBuildResult, error) {
	return BuildShardedStreamBaseWithShardCount(
		ctx, source, accountsDBRoot, generation,
		DefaultPersistentIndexShards, routingKey, previousCatalog, workers,
	)
}

// BuildShardedStreamBaseWithShardCount is the catalog-driven form used when a
// lineage explicitly persists a non-default power-of-two shard count.
func BuildShardedStreamBaseWithShardCount(
	ctx context.Context,
	source StreamIndexSource,
	accountsDBRoot string,
	generation uint64,
	shardCount int,
	routingKey PersistentIndexRoutingKey,
	previousCatalog *PersistentExtentCatalog,
	workers int,
) (*ShardedStreamBaseBuildResult, error) {
	router, err := NewPersistentIndexShardRouter(shardCount, routingKey)
	if err != nil {
		return nil, err
	}
	result, err := buildShardedStreamBase(ctx, source, accountsDBRoot, generation, previousCatalog, workers, router, nil)
	if err != nil {
		return nil, err
	}
	return &ShardedStreamBaseBuildResult{
		Generation:                result.generation,
		ShardCount:                uint32(router.Count()),
		RoutingKey:                router.RoutingKey(),
		ExtentCatalog:             result.catalog,
		ExtentCatalogArtifact:     result.catalogArtifact,
		OwnsExtentCatalogArtifact: result.ownsCatalogArtifact,
		Shards:                    result.shards,
	}, nil
}

// BuildShardedStreamBaseShard builds one production-routed shard for rolling
// rebase. Source must be an exact, repeatable, sorted view containing only keys
// routed to shardID. A newly returned extent catalog preserves every previous
// ordinal and appends any extents introduced by this source.
func BuildShardedStreamBaseShard(
	ctx context.Context,
	source StreamIndexSource,
	accountsDBRoot string,
	generation uint64,
	routingKey PersistentIndexRoutingKey,
	shardID uint32,
	previousCatalog *PersistentExtentCatalog,
	workers int,
) (*ShardedStreamBaseShardBuildResult, error) {
	return BuildShardedStreamBaseShardWithShardCount(
		ctx, source, accountsDBRoot, generation,
		DefaultPersistentIndexShards, routingKey, shardID, previousCatalog, workers,
	)
}

// BuildShardedStreamBaseShardWithShardCount is the rolling builder for a shard
// count already selected by the root catalog lineage.
func BuildShardedStreamBaseShardWithShardCount(
	ctx context.Context,
	source StreamIndexSource,
	accountsDBRoot string,
	generation uint64,
	shardCount int,
	routingKey PersistentIndexRoutingKey,
	shardID uint32,
	previousCatalog *PersistentExtentCatalog,
	workers int,
) (*ShardedStreamBaseShardBuildResult, error) {
	router, err := NewPersistentIndexShardRouter(shardCount, routingKey)
	if err != nil {
		return nil, err
	}
	if shardID >= uint32(router.Count()) {
		return nil, fmt.Errorf("%w: shard %d >= %d", ErrInvalidShardedStreamBase, shardID, router.Count())
	}
	result, err := buildShardedStreamBase(ctx, source, accountsDBRoot, generation, previousCatalog, workers, router, &shardID)
	if err != nil {
		return nil, err
	}
	if len(result.shards) != 1 {
		return nil, fmt.Errorf("%w: single-shard build returned %d shards", ErrInvalidShardedStreamBase, len(result.shards))
	}
	return &ShardedStreamBaseShardBuildResult{
		Generation:                result.generation,
		ShardCount:                uint32(router.Count()),
		RoutingKey:                router.RoutingKey(),
		ExtentCatalog:             result.catalog,
		ExtentCatalogArtifact:     result.catalogArtifact,
		OwnsExtentCatalogArtifact: result.ownsCatalogArtifact,
		Shard:                     result.shards[0],
	}, nil
}

type shardedBaseInternalBuildResult struct {
	generation          uint64
	catalog             *PersistentExtentCatalog
	catalogArtifact     IndexCatalogArtifact
	ownsCatalogArtifact bool
	shards              []ShardedStreamBaseShardArtifacts
}

type shardedBaseSourceSummary struct {
	counts []uint64
	total  uint64
	digest [sha256.Size]byte
}

func buildShardedStreamBase(
	ctx context.Context,
	source StreamIndexSource,
	accountsDBRoot string,
	generation uint64,
	previousCatalog *PersistentExtentCatalog,
	workers int,
	router PersistentIndexShardRouter,
	targetShard *uint32,
) (_ *shardedBaseInternalBuildResult, retErr error) {
	if ctx == nil {
		return nil, errors.New("accountsdb: nil sharded base build context")
	}
	if source == nil {
		return nil, errors.New("accountsdb: nil sharded base source")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidatePersistentIndexShardCount(router.Count()); err != nil {
		return nil, err
	}
	if targetShard != nil && *targetShard >= uint32(router.Count()) {
		return nil, fmt.Errorf("%w: target shard %d >= %d", ErrInvalidShardedStreamBase, *targetShard, router.Count())
	}

	extension, err := newPersistentExtentCatalogExtension(previousCatalog, generation)
	if err != nil {
		return nil, err
	}
	summary, err := inventoryShardedBaseSource(ctx, source, router, targetShard, extension)
	if err != nil {
		return nil, err
	}
	catalog, ownsCatalogArtifact, err := extension.finish()
	if err != nil {
		return nil, err
	}

	buildDir, relativeDir, err := createShardedBaseBuildDir(accountsDBRoot, generation)
	if err != nil {
		return nil, err
	}
	keepBuildDir := false
	defer func() {
		if !keepBuildDir {
			retErr = errors.Join(retErr, cleanupShardedStreamBaseBuildDir(buildDir, os.RemoveAll))
		}
	}()

	catalogArtifact := catalog.Artifact()
	if ownsCatalogArtifact {
		catalogPartial := filepath.Join(buildDir, "extents.cat.partial")
		catalogFinal := filepath.Join(buildDir, "extents.cat")
		if err := writePersistentExtentCatalog(ctx, catalogPartial, catalog); err != nil {
			return nil, err
		}
		if err := publishUniqueShardedBaseFile(catalogPartial, catalogFinal); err != nil {
			return nil, fmt.Errorf("accountsdb: publish extent catalog: %w", err)
		}
		catalogRelative := filepath.ToSlash(filepath.Join(relativeDir, "extents.cat"))
		verifiedCatalog, identifyErr := verifyWrittenPersistentExtentCatalog(
			ctx,
			accountsDBRoot,
			catalogRelative,
			catalog,
		)
		if identifyErr != nil {
			return nil, identifyErr
		}
		catalogArtifact = verifiedCatalog.artifact
		if err := catalog.adoptVerifiedArtifact(accountsDBRoot, verifiedCatalog); err != nil {
			return nil, fmt.Errorf("accountsdb: adopt built extent catalog: %w", err)
		}
		// The checked writer encoded this exact validated object, and the complete
		// identity pass above bound it to a stable, no-follow inode. Keep the object
		// rather than reopening our own output and allocating an equivalent second
		// extent slice and ordinal map.
	} else if catalogArtifact.isZero() {
		return nil, fmt.Errorf("%w: reused extent catalog has no artifact identity", ErrInvalidExtentCatalog)
	}
	binding, err := catalog.binding()
	if err != nil {
		return nil, err
	}

	scanArtifacts, scanPaths, err := buildShardedBaseScanSidecars(
		ctx, source, accountsDBRoot, buildDir, relativeDir, generation,
		catalog, binding, router, targetShard, summary,
	)
	if err != nil {
		return nil, err
	}

	shardIDs := selectedShardedBaseIDs(router, targetShard)
	shards := make([]ShardedStreamBaseShardArtifacts, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		indexName := fmt.Sprintf("shard-%04d.stmh", shardID)
		indexPartial := filepath.Join(buildDir, indexName+".partial")
		indexFinal := filepath.Join(buildDir, indexName)
		indexRelative := filepath.ToSlash(filepath.Join(relativeDir, indexName))
		scanSeal, err := captureShardedBaseScanSeal(scanPaths[shardID], scanArtifacts[shardID])
		if err != nil {
			return nil, fmt.Errorf("accountsdb: seal shard %d exact sidecar: %w", shardID, err)
		}
		metadata := shardedBaseMetadata{
			ShardCount:        uint32(router.Count()),
			ShardID:           shardID,
			Generation:        generation,
			KeyCount:          summary.counts[shardID],
			RoutingKey:        router.RoutingKey(),
			CatalogGeneration: catalog.generation,
			CatalogBinding:    binding,
			ScanSeal:          scanSeal,
		}
		if err := buildShardedBaseStreamHash(
			ctx, scanPaths[shardID], scanArtifacts[shardID], indexPartial, buildDir, metadata,
			catalog, router, workers,
		); err != nil {
			return nil, err
		}
		if err := publishUniqueShardedBaseFile(indexPartial, indexFinal); err != nil {
			return nil, fmt.Errorf("accountsdb: publish shard %d StreamHash: %w", shardID, err)
		}
		indexArtifact, err := ComputeIndexCatalogArtifact(accountsDBRoot, indexRelative)
		if err != nil {
			return nil, err
		}
		shards = append(shards, ShardedStreamBaseShardArtifacts{
			ShardID:    shardID,
			KeyCount:   metadata.KeyCount,
			Generation: generation,
			Index:      indexArtifact,
			Scan:       scanArtifacts[shardID],
		})
	}
	if err := fsyncDir(buildDir); err != nil {
		return nil, fmt.Errorf("accountsdb: sync sharded base generation directory: %w", err)
	}
	if err := fsyncDir(accountsDBRoot); err != nil {
		return nil, fmt.Errorf("accountsdb: sync AccountsDB root after sharded base build: %w", err)
	}
	keepBuildDir = true
	return &shardedBaseInternalBuildResult{
		generation:          generation,
		catalog:             catalog,
		catalogArtifact:     catalogArtifact,
		ownsCatalogArtifact: ownsCatalogArtifact,
		shards:              shards,
	}, nil
}

func cleanupShardedStreamBaseBuildDir(
	buildDir string,
	removeAll func(string) error,
) error {
	if err := removeAll(buildDir); err != nil {
		// Do not wrap the underlying I/O error: ENOSPC/EIO are ordinarily safe
		// rebase retry signals, whereas failure to remove this random-nonce build
		// directory must dominate and stop retries.
		return fmt.Errorf("%w: %v", ErrShardedStreamBaseBuildCleanup, err)
	}
	return nil
}

type shardedBaseExtentInventory interface {
	ensureExtent(AccountIndexEntry) error
}

func selectedShardedBaseIDs(router PersistentIndexShardRouter, targetShard *uint32) []uint32 {
	if targetShard != nil {
		return []uint32{*targetShard}
	}
	ids := make([]uint32, router.Count())
	for i := range ids {
		ids[i] = uint32(i)
	}
	return ids
}

func inventoryShardedBaseSource(
	ctx context.Context,
	source StreamIndexSource,
	router PersistentIndexShardRouter,
	targetShard *uint32,
	catalog shardedBaseExtentInventory,
) (shardedBaseSourceSummary, error) {
	summary := shardedBaseSourceSummary{counts: make([]uint64, router.Count())}
	hasher := sha256.New()
	var previous solana.PublicKey
	havePrevious := false
	err := source.Scan(ctx, func(key solana.PublicKey, entry AccountIndexEntry) error {
		if summary.total%streamIndexContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		shardID, err := validateShardedBaseSourceRecord(
			key, entry, previous, havePrevious, router, targetShard,
		)
		if err != nil {
			return err
		}
		if summary.total == math.MaxUint64 || summary.counts[shardID] == math.MaxUint64 {
			return fmt.Errorf("%w: source record count overflow", ErrInvalidShardedStreamBase)
		}
		if err := catalog.ensureExtent(entry); err != nil {
			return err
		}
		appendShardedBaseSourceDigest(hasher, key, entry)
		summary.total++
		summary.counts[shardID]++
		previous = key
		havePrevious = true
		return nil
	})
	if err != nil {
		return summary, fmt.Errorf("accountsdb: inventory sharded base source: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return summary, err
	}
	copy(summary.digest[:], hasher.Sum(nil))
	return summary, nil
}

func validateShardedBaseSourceRecord(
	key solana.PublicKey,
	entry AccountIndexEntry,
	previous solana.PublicKey,
	havePrevious bool,
	router PersistentIndexShardRouter,
	targetShard *uint32,
) (uint32, error) {
	if havePrevious {
		switch cmp := bytes.Compare(previous[:], key[:]); {
		case cmp == 0:
			return 0, fmt.Errorf("%w: duplicate source key %x", ErrInvalidShardedStreamBase, key)
		case cmp > 0:
			return 0, fmt.Errorf("%w: out-of-order source key %x", ErrInvalidShardedStreamBase, key)
		}
	}
	shardID := router.Shard(key)
	if shardID >= uint32(router.Count()) {
		return 0, fmt.Errorf("%w: routed shard %d is out of range", ErrInvalidShardedStreamBase, shardID)
	}
	if targetShard != nil && shardID != *targetShard {
		return 0, fmt.Errorf(
			"%w: key %x routes to shard %d, single-shard source is %d",
			ErrInvalidShardedStreamBase,
			key,
			shardID,
			*targetShard,
		)
	}
	if entry.Offset%8 != 0 {
		return 0, fmt.Errorf("%w: source offset %d is not eight-byte aligned", ErrInvalidShardedStreamBase, entry.Offset)
	}
	return shardID, nil
}

func appendShardedBaseSourceDigest(hasher hash.Hash, key solana.PublicKey, entry AccountIndexEntry) {
	var encoded [StreamIndexRunRecordSize]byte
	copy(encoded[:32], key[:])
	entry.Marshal((*[24]byte)(encoded[32:]))
	_, _ = hasher.Write(encoded[:])
}

func createShardedBaseBuildDir(root string, generation uint64) (absolute, relative string, err error) {
	if root == "" {
		return "", "", fmt.Errorf("%w: empty AccountsDB root", ErrInvalidShardedStreamBase)
	}
	if generation == 0 {
		return "", "", fmt.Errorf("%w: zero generation", ErrInvalidShardedStreamBase)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", "", fmt.Errorf("accountsdb: stat sharded base root: %w", err)
	}
	if !info.IsDir() {
		return "", "", fmt.Errorf("%w: AccountsDB root is not a directory", ErrInvalidShardedStreamBase)
	}
	for attempt := 0; attempt < shardedBaseBuildDirAttempts; attempt++ {
		var nonce [16]byte
		if _, err := io.ReadFull(cryptorand.Reader, nonce[:]); err != nil {
			return "", "", fmt.Errorf("accountsdb: generate sharded base artifact nonce: %w", err)
		}
		relative = fmt.Sprintf("accounts-index-v2-g%020d-%x", generation, nonce)
		absolute = filepath.Join(root, relative)
		if err := os.Mkdir(absolute, 0o755); err == nil {
			return absolute, relative, nil
		} else if !errors.Is(err, os.ErrExist) {
			return "", "", fmt.Errorf("accountsdb: create sharded base build directory: %w", err)
		}
	}
	return "", "", fmt.Errorf("%w: exhausted unique artifact directory attempts", ErrInvalidShardedStreamBase)
}

func publishUniqueShardedBaseFile(partialPath, finalPath string) error {
	if partialPath == "" || finalPath == "" || filepath.Dir(partialPath) != filepath.Dir(finalPath) {
		return fmt.Errorf("%w: invalid artifact publication paths", ErrInvalidShardedStreamBase)
	}
	if info, err := os.Lstat(partialPath); err != nil {
		return err
	} else if !info.Mode().IsRegular() || info.Size() <= 0 {
		return fmt.Errorf("%w: partial artifact is not a non-empty regular file", ErrInvalidShardedStreamBase)
	}
	// Link provides no-replace atomic publication on every supported Go
	// platform. The generation directory itself is unique, but retaining this
	// invariant prevents a future naming refactor from silently clobbering a
	// mapped immutable artifact.
	if err := os.Link(partialPath, finalPath); err != nil {
		return err
	}
	if err := os.Remove(partialPath); err != nil {
		return err
	}
	return nil
}

type shardedBaseScanWriter struct {
	file     *os.File
	buffer   *bufio.Writer
	path     string
	metadata shardedBaseMetadata
	bodyCRC  hash.Hash32
	written  uint64
	closed   bool
}

func newShardedBaseScanWriter(
	path string,
	metadata shardedBaseMetadata,
	bufferSize int,
) (*shardedBaseScanWriter, error) {
	metadata.HashSeed = 0
	metadata.BodyCRC = 0
	if err := validateShardedBaseMetadata(metadata); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: create shard %d scan sidecar: %w", metadata.ShardID, err)
	}
	var emptyHeader [shardedBaseMetadataSize]byte
	if n, err := file.Write(emptyHeader[:]); err != nil || n != len(emptyHeader) {
		_ = file.Close()
		return nil, fmt.Errorf("accountsdb: reserve shard %d scan header: %w", metadata.ShardID, errors.Join(err, io.ErrShortWrite))
	}
	return &shardedBaseScanWriter{
		file:     file,
		buffer:   bufio.NewWriterSize(file, bufferSize),
		path:     path,
		metadata: metadata,
		bodyCRC:  crc32.New(shardedBaseCRC),
	}, nil
}

func (writer *shardedBaseScanWriter) write(key solana.PublicKey, locator uint64) error {
	if writer == nil || writer.closed {
		return fmt.Errorf("%w: write closed scan sidecar", ErrInvalidShardedStreamBase)
	}
	if locator > packedAccountLocatorMask {
		return fmt.Errorf("%w: locator %#x exceeds 48 bits", ErrInvalidBaseLocator, locator)
	}
	if writer.written >= writer.metadata.KeyCount {
		return fmt.Errorf("%w: shard %d scan count exceeds %d", ErrInvalidShardedStreamBase, writer.metadata.ShardID, writer.metadata.KeyCount)
	}
	var record [shardedBaseScanRecordSize]byte
	copy(record[:32], key[:])
	putUint48(record[32:], locator)
	if _, err := writer.buffer.Write(record[:]); err != nil {
		return fmt.Errorf("accountsdb: write shard %d scan record: %w", writer.metadata.ShardID, err)
	}
	_, _ = writer.bodyCRC.Write(record[:])
	writer.written++
	return nil
}

func (writer *shardedBaseScanWriter) finish() (retErr error) {
	if writer == nil || writer.closed {
		return fmt.Errorf("%w: finish closed scan sidecar", ErrInvalidShardedStreamBase)
	}
	defer func() {
		if !writer.closed {
			retErr = errors.Join(retErr, writer.file.Close())
			writer.closed = true
		}
	}()
	if writer.written != writer.metadata.KeyCount {
		return fmt.Errorf(
			"%w: shard %d wrote %d scan records, expected %d",
			ErrInvalidShardedStreamBase,
			writer.metadata.ShardID,
			writer.written,
			writer.metadata.KeyCount,
		)
	}
	if err := writer.buffer.Flush(); err != nil {
		return fmt.Errorf("accountsdb: flush shard %d scan sidecar: %w", writer.metadata.ShardID, err)
	}
	writer.metadata.BodyCRC = writer.bodyCRC.Sum32()
	header, err := encodeShardedBaseMetadata(shardedBaseScanMagic, shardedBaseScanRecordSize, writer.metadata)
	if err != nil {
		return err
	}
	if n, err := writer.file.WriteAt(header[:], 0); err != nil || n != len(header) {
		return fmt.Errorf("accountsdb: write shard %d scan header: %w", writer.metadata.ShardID, errors.Join(err, io.ErrShortWrite))
	}
	if err := writer.file.Sync(); err != nil {
		return fmt.Errorf("accountsdb: sync shard %d scan sidecar: %w", writer.metadata.ShardID, err)
	}
	if err := writer.file.Close(); err != nil {
		return fmt.Errorf("accountsdb: close shard %d scan sidecar: %w", writer.metadata.ShardID, err)
	}
	writer.closed = true
	return nil
}

func (writer *shardedBaseScanWriter) abort() error {
	if writer == nil || writer.closed {
		return nil
	}
	writer.closed = true
	return writer.file.Close()
}

func buildShardedBaseScanSidecars(
	ctx context.Context,
	source StreamIndexSource,
	accountsDBRoot string,
	buildDir string,
	relativeDir string,
	generation uint64,
	catalog *PersistentExtentCatalog,
	binding extentCatalogBinding,
	router PersistentIndexShardRouter,
	targetShard *uint32,
	want shardedBaseSourceSummary,
) (map[uint32]IndexCatalogArtifact, map[uint32]string, error) {
	selected := selectedShardedBaseIDs(router, targetShard)
	selectedSet := make(map[uint32]struct{}, len(selected))
	for _, shardID := range selected {
		selectedSet[shardID] = struct{}{}
	}
	artifacts := make(map[uint32]IndexCatalogArtifact, len(selected))
	paths := make(map[uint32]string, len(selected))
	writers := make([]*shardedBaseScanWriter, router.Count())
	bufferSize := shardedBaseScanBufferBudget / max(1, len(selected))
	bufferSize = max(bufferSize, shardedBaseScanBufferMin)
	bufferSize = min(bufferSize, shardedBaseScanBufferMax)
	finishWriter := func(shardID uint32) error {
		writer := writers[shardID]
		if writer == nil {
			return nil
		}
		if err := writer.finish(); err != nil {
			return err
		}
		finalName := fmt.Sprintf("shard-%04d.scan", shardID)
		finalPath := filepath.Join(buildDir, finalName)
		if err := publishUniqueShardedBaseFile(writer.path, finalPath); err != nil {
			return fmt.Errorf("accountsdb: publish shard %d scan sidecar: %w", shardID, err)
		}
		relative := filepath.ToSlash(filepath.Join(relativeDir, finalName))
		artifact, err := ComputeIndexCatalogArtifact(accountsDBRoot, relative)
		if err != nil {
			return err
		}
		artifacts[shardID] = artifact
		paths[shardID] = finalPath
		writers[shardID] = nil
		return nil
	}
	newWriter := func(shardID uint32) error {
		metadata := shardedBaseMetadata{
			ShardCount:        uint32(router.Count()),
			ShardID:           shardID,
			Generation:        generation,
			KeyCount:          want.counts[shardID],
			RoutingKey:        router.RoutingKey(),
			CatalogGeneration: catalog.generation,
			CatalogBinding:    binding,
		}
		path := filepath.Join(buildDir, fmt.Sprintf("shard-%04d.scan.partial", shardID))
		writer, err := newShardedBaseScanWriter(path, metadata, bufferSize)
		if err != nil {
			return err
		}
		writers[shardID] = writer
		return nil
	}
	defer func() {
		for _, writer := range writers {
			_ = writer.abort()
		}
	}()

	actualCounts := make([]uint64, router.Count())
	actualHasher := sha256.New()
	var total uint64
	var previous solana.PublicKey
	havePrevious := false
	err := source.Scan(ctx, func(key solana.PublicKey, entry AccountIndexEntry) error {
		if total%streamIndexContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		shardID, err := validateShardedBaseSourceRecord(
			key, entry, previous, havePrevious, router, targetShard,
		)
		if err != nil {
			return err
		}
		if _, ok := selectedSet[shardID]; !ok {
			return fmt.Errorf("%w: source produced unselected shard %d", ErrInvalidShardedStreamBase, shardID)
		}
		if writers[shardID] == nil {
			if err := newWriter(shardID); err != nil {
				return err
			}
		}
		locator, err := catalog.PackAccountIndexEntry(entry)
		if err != nil {
			return err
		}
		if err := writers[shardID].write(key, locator); err != nil {
			return err
		}
		if total == math.MaxUint64 || actualCounts[shardID] == math.MaxUint64 {
			return fmt.Errorf("%w: source record count overflow", ErrInvalidShardedStreamBase)
		}
		appendShardedBaseSourceDigest(actualHasher, key, entry)
		total++
		actualCounts[shardID]++
		previous = key
		havePrevious = true
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("accountsdb: write sharded base scan sidecars: %w", err)
	}
	for _, shardID := range selected {
		if writers[shardID] == nil {
			if err := newWriter(shardID); err != nil {
				return nil, nil, err
			}
		}
		if err := finishWriter(shardID); err != nil {
			return nil, nil, err
		}
	}
	var actualDigest [sha256.Size]byte
	copy(actualDigest[:], actualHasher.Sum(nil))
	if total != want.total || actualDigest != want.digest {
		return nil, nil, fmt.Errorf(
			"%w: source changed between scans (records %d/%d)",
			ErrInvalidShardedStreamBase,
			total,
			want.total,
		)
	}
	for _, shardID := range selected {
		if actualCounts[shardID] != want.counts[shardID] {
			return nil, nil, fmt.Errorf(
				"%w: shard %d source count changed from %d to %d",
				ErrInvalidShardedStreamBase,
				shardID,
				want.counts[shardID],
				actualCounts[shardID],
			)
		}
	}
	return artifacts, paths, nil
}

func putUint48(dst []byte, value uint64) {
	_ = dst[5]
	dst[0] = byte(value)
	dst[1] = byte(value >> 8)
	dst[2] = byte(value >> 16)
	dst[3] = byte(value >> 24)
	dst[4] = byte(value >> 32)
	dst[5] = byte(value >> 40)
}

func uint48(src []byte) uint64 {
	_ = src[5]
	return uint64(src[0]) |
		uint64(src[1])<<8 |
		uint64(src[2])<<16 |
		uint64(src[3])<<24 |
		uint64(src[4])<<32 |
		uint64(src[5])<<40
}

func buildShardedBaseStreamHash(
	ctx context.Context,
	scanPath string,
	scanArtifact IndexCatalogArtifact,
	partialPath string,
	tempDir string,
	metadata shardedBaseMetadata,
	catalog *PersistentExtentCatalog,
	router PersistentIndexShardRouter,
	workers int,
) error {
	for attempt := 0; attempt < streamIndexMaxBuildAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.Remove(partialPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("accountsdb: remove shard %d StreamHash partial: %w", metadata.ShardID, err)
		}
		hashSeed, err := newStreamIndexHashSeed()
		if err != nil {
			return err
		}
		attemptMetadata := metadata
		attemptMetadata.HashSeed = hashSeed
		encodedMetadata, err := encodeShardedBaseMetadata(shardedBaseIndexMetadataMagic, 0, attemptMetadata)
		if err != nil {
			return err
		}
		opts := []streamhash.BuildOption{
			streamhash.WithAlgorithm(streamhash.AlgoPTRHash),
			streamhash.WithPayload(shardedBasePayloadBytes),
			streamhash.WithFingerprint(shardedBaseFingerprintBytes),
			streamhash.WithGlobalSeed(hashSeed ^ 0x9e3779b97f4a7c15),
			streamhash.WithMetadata(encodedMetadata[:]),
		}
		if workers > 0 {
			opts = append(opts, streamhash.WithWorkers(workers))
		}
		builder, err := streamhash.NewUnsortedBuilder(ctx, partialPath, metadata.KeyCount, tempDir, opts...)
		if err != nil {
			return fmt.Errorf("accountsdb: create shard %d StreamHash builder: %w", metadata.ShardID, err)
		}
		populateErr := populateShardedBaseStreamHash(
			ctx, scanPath, scanArtifact, builder, attemptMetadata, catalog, router,
		)
		closeErr := builder.Close()
		if populateErr != nil {
			err = errors.Join(populateErr, closeErr)
		} else {
			err = closeErr
		}
		if errors.Is(err, streamhash.ErrDuplicateKey) || errors.Is(err, streamhash.ErrIndistinguishableHashes) {
			_ = os.Remove(partialPath)
			continue
		}
		if err != nil {
			return fmt.Errorf("accountsdb: build shard %d StreamHash: %w", metadata.ShardID, err)
		}
		if err := verifyBuiltShardedBaseStreamHash(partialPath, attemptMetadata, catalog, router); err != nil {
			return err
		}
		if err := syncShardedBaseFile(partialPath); err != nil {
			return fmt.Errorf("accountsdb: sync shard %d StreamHash: %w", metadata.ShardID, err)
		}
		return nil
	}
	return fmt.Errorf(
		"accountsdb: shard %d failed to build collision-free StreamHash after %d attempts",
		metadata.ShardID,
		streamIndexMaxBuildAttempts,
	)
}

func populateShardedBaseStreamHash(
	ctx context.Context,
	scanPath string,
	scanArtifact IndexCatalogArtifact,
	builder *streamhash.UnsortedBuilder,
	metadata shardedBaseMetadata,
	catalog *PersistentExtentCatalog,
	router PersistentIndexShardRouter,
) error {
	file, scanMetadata, err := openShardedBaseScanFile(scanPath, catalog, router)
	if err != nil {
		return err
	}
	defer file.Close()
	if !sameShardedBaseGeneration(scanMetadata, metadata) {
		return fmt.Errorf("%w: scan/index metadata mismatch for shard %d", ErrInvalidShardedStreamBase, metadata.ShardID)
	}
	err = scanShardedBaseSidecarLocatorsWithArtifact(
		ctx,
		file,
		scanPath,
		scanMetadata,
		catalog,
		router,
		func(key solana.PublicKey, locator uint64, _ AccountIndexEntry) error {
			hashed := streamIndexHashKey(key, metadata.HashSeed)
			return builder.AddKey(hashed[:], locator)
		},
		&scanArtifact,
	)
	if err != nil {
		return err
	}
	return builder.Finish()
}

func verifyBuiltShardedBaseStreamHash(
	path string,
	want shardedBaseMetadata,
	catalog *PersistentExtentCatalog,
	router PersistentIndexShardRouter,
) error {
	idx, err := streamhash.OpenPayload(path)
	if err != nil {
		return fmt.Errorf("accountsdb: reopen built shard %d StreamHash: %w", want.ShardID, err)
	}
	defer idx.Close()
	stats := idx.Stats()
	if stats.Algorithm != streamhash.AlgoPTRHash ||
		stats.PayloadSize != shardedBasePayloadBytes ||
		stats.FingerprintSize != shardedBaseFingerprintBytes {
		return fmt.Errorf(
			"%w: incompatible StreamHash parameters algorithm=%s payload=%d fingerprint=%d",
			ErrInvalidShardedStreamBase,
			stats.Algorithm,
			stats.PayloadSize,
			stats.FingerprintSize,
		)
	}
	got, err := decodeShardedBaseMetadata(idx.UserMetadata(), shardedBaseIndexMetadataMagic, 0)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%w: built StreamHash metadata mismatch", ErrInvalidShardedStreamBase)
	}
	if idx.NumKeys() != want.KeyCount {
		return fmt.Errorf("%w: StreamHash key count %d, want %d", ErrInvalidShardedStreamBase, idx.NumKeys(), want.KeyCount)
	}
	if got.ShardCount != uint32(router.Count()) || got.RoutingKey != router.RoutingKey() {
		return fmt.Errorf("%w: router identity mismatch", ErrInvalidShardedStreamBase)
	}
	if got.CatalogGeneration > catalog.generation {
		return fmt.Errorf("%w: shard catalog generation is newer than catalog", ErrInvalidShardedStreamBase)
	}
	if err := catalog.validateBinding(got.CatalogBinding); err != nil {
		return err
	}
	if err := idx.Verify(); err != nil {
		return fmt.Errorf("accountsdb: verify shard %d StreamHash: %w", want.ShardID, err)
	}
	return nil
}

func syncShardedBaseFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(syncErr, closeErr)
}

func sameShardedBaseGeneration(left, right shardedBaseMetadata) bool {
	return left.ShardCount == right.ShardCount &&
		left.ShardID == right.ShardID &&
		left.Generation == right.Generation &&
		left.KeyCount == right.KeyCount &&
		left.RoutingKey == right.RoutingKey &&
		left.CatalogGeneration == right.CatalogGeneration &&
		left.CatalogBinding == right.CatalogBinding
}

func openShardedBaseScanFile(
	path string,
	catalog *PersistentExtentCatalog,
	router PersistentIndexShardRouter,
) (*os.File, shardedBaseMetadata, error) {
	if path == "" {
		return nil, shardedBaseMetadata{}, fmt.Errorf("%w: empty scan path", ErrInvalidShardedStreamBase)
	}
	file, openedInfo, err := openStableRegularFile(path)
	if err != nil {
		return nil, shardedBaseMetadata{}, fmt.Errorf("accountsdb: open sharded base scan: %w", err)
	}
	var encoded [shardedBaseMetadataSize]byte
	if _, err := file.ReadAt(encoded[:], 0); err != nil {
		_ = file.Close()
		return nil, shardedBaseMetadata{}, fmt.Errorf("%w: read scan header: %v", ErrInvalidShardedStreamBase, err)
	}
	metadata, err := decodeShardedBaseMetadata(encoded[:], shardedBaseScanMagic, shardedBaseScanRecordSize)
	if err != nil {
		_ = file.Close()
		return nil, shardedBaseMetadata{}, err
	}
	if metadata.HashSeed != 0 {
		_ = file.Close()
		return nil, shardedBaseMetadata{}, fmt.Errorf("%w: scan header has hash seed", ErrInvalidShardedStreamBase)
	}
	if metadata.ShardCount != uint32(router.Count()) {
		_ = file.Close()
		return nil, shardedBaseMetadata{}, fmt.Errorf("%w: scan/router shard-count mismatch", ErrInvalidShardedStreamBase)
	}
	if metadata.RoutingKey != router.RoutingKey() {
		_ = file.Close()
		return nil, shardedBaseMetadata{}, fmt.Errorf("%w: scan/router routing-key mismatch", ErrInvalidShardedStreamBase)
	}
	if metadata.CatalogGeneration > catalog.generation {
		_ = file.Close()
		return nil, shardedBaseMetadata{}, fmt.Errorf("%w: scan catalog generation is newer than catalog", ErrInvalidShardedStreamBase)
	}
	if err := catalog.validateBinding(metadata.CatalogBinding); err != nil {
		_ = file.Close()
		return nil, shardedBaseMetadata{}, err
	}
	if metadata.KeyCount > (math.MaxInt64-shardedBaseMetadataSize)/shardedBaseScanRecordSize {
		_ = file.Close()
		return nil, shardedBaseMetadata{}, fmt.Errorf("%w: scan record count overflows file size", ErrInvalidShardedStreamBase)
	}
	wantSize := int64(shardedBaseMetadataSize) + int64(metadata.KeyCount)*shardedBaseScanRecordSize
	if openedInfo.Size() != wantSize {
		_ = file.Close()
		return nil, shardedBaseMetadata{}, fmt.Errorf(
			"%w: scan size %d, want %d",
			ErrInvalidShardedStreamBase,
			openedInfo.Size(),
			wantSize,
		)
	}
	if err := validateStableRegularFile(file, path, openedInfo); err != nil {
		_ = file.Close()
		return nil, shardedBaseMetadata{}, fmt.Errorf("%w: scan changed while reading its header: %v", ErrInvalidShardedStreamBase, err)
	}
	return file, metadata, nil
}

// readShardedBaseScanBindingForStartup reads only the strict fixed-size scan
// header. Its result is used solely to batch extent-prefix hashing; the normal
// open path still verifies the complete root-selected artifact identity and
// all scan semantics before exposing a shard.
func readShardedBaseScanBindingForStartup(
	root string,
	artifact IndexCatalogArtifact,
	shardID uint32,
	catalogGeneration uint64,
	router PersistentIndexShardRouter,
) (extentCatalogBinding, error) {
	path, err := ResolveIndexCatalogArtifactPath(root, artifact)
	if err != nil {
		return extentCatalogBinding{}, err
	}
	if err := validateIndexCatalogArtifactPathComponents(root, artifact.RelativePath); err != nil {
		return extentCatalogBinding{}, fmt.Errorf("%w: inspect shard %d scan path: %v", ErrInvalidShardedStreamBase, shardID, err)
	}
	file, info, err := openStableRegularFile(path)
	if err != nil {
		return extentCatalogBinding{}, fmt.Errorf("accountsdb: open sharded base scan header: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() < shardedBaseMetadataSize ||
		uint64(info.Size()) != artifact.Size {
		_ = file.Close()
		return extentCatalogBinding{}, fmt.Errorf(
			"%w: shard %d scan header source has invalid size or type",
			ErrInvalidShardedStreamBase, shardID,
		)
	}
	var encoded [shardedBaseMetadataSize]byte
	_, readErr := file.ReadAt(encoded[:], 0)
	stableErr := validateStableRegularFile(file, path, info)
	closeErr := file.Close()
	if readErr != nil || stableErr != nil || closeErr != nil {
		return extentCatalogBinding{}, fmt.Errorf(
			"accountsdb: read sharded base scan header: %w",
			errors.Join(readErr, stableErr, closeErr),
		)
	}
	metadata, err := decodeShardedBaseMetadata(
		encoded[:], shardedBaseScanMagic, shardedBaseScanRecordSize,
	)
	if err != nil {
		return extentCatalogBinding{}, err
	}
	if metadata.HashSeed != 0 || metadata.ShardID != shardID ||
		metadata.ShardCount != uint32(router.Count()) ||
		metadata.RoutingKey != router.RoutingKey() ||
		metadata.CatalogGeneration > catalogGeneration {
		return extentCatalogBinding{}, fmt.Errorf(
			"%w: shard %d scan header does not match root routing/catalog identity",
			ErrInvalidShardedStreamBase, shardID,
		)
	}
	if metadata.KeyCount > (math.MaxInt64-shardedBaseMetadataSize)/shardedBaseScanRecordSize {
		return extentCatalogBinding{}, fmt.Errorf(
			"%w: shard %d scan record count overflows file size",
			ErrInvalidShardedStreamBase, shardID,
		)
	}
	wantSize := int64(shardedBaseMetadataSize) + int64(metadata.KeyCount)*shardedBaseScanRecordSize
	if wantSize != info.Size() {
		return extentCatalogBinding{}, fmt.Errorf(
			"%w: shard %d scan header count does not match artifact size",
			ErrInvalidShardedStreamBase, shardID,
		)
	}
	return metadata.CatalogBinding, nil
}

func scanShardedBaseSidecar(
	ctx context.Context,
	file *os.File,
	path string,
	metadata shardedBaseMetadata,
	catalog *PersistentExtentCatalog,
	router PersistentIndexShardRouter,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) error {
	return scanShardedBaseSidecarLocators(
		ctx, file, path, metadata, catalog, router,
		func(key solana.PublicKey, _ uint64, entry AccountIndexEntry) error {
			return visit(key, entry)
		},
	)
}

func scanShardedBaseSidecarLocators(
	ctx context.Context,
	file *os.File,
	path string,
	metadata shardedBaseMetadata,
	catalog *PersistentExtentCatalog,
	router PersistentIndexShardRouter,
	visit func(solana.PublicKey, uint64, AccountIndexEntry) error,
) error {
	return scanShardedBaseSidecarLocatorsWithArtifact(
		ctx, file, path, metadata, catalog, router, visit, nil,
	)
}

// scanShardedBaseSidecarLocatorsWithArtifact combines semantic/CRC validation
// with the root-selected SHA-256 identity check in one sequential pass. The
// ordinary enumeration path passes nil so repeated compaction/rebase scans do
// not pay for an unnecessary cryptographic hash.
func scanShardedBaseSidecarLocatorsWithArtifact(
	ctx context.Context,
	file *os.File,
	path string,
	metadata shardedBaseMetadata,
	catalog *PersistentExtentCatalog,
	router PersistentIndexShardRouter,
	visit func(solana.PublicKey, uint64, AccountIndexEntry) error,
	expected *IndexCatalogArtifact,
) (retErr error) {
	if ctx == nil {
		return errors.New("accountsdb: nil sharded base scan context")
	}
	if file == nil || visit == nil {
		return fmt.Errorf("%w: nil scan file or visitor", ErrInvalidShardedStreamBase)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Re-read the header and size for every enumeration so in-place truncation
	// or header replacement cannot be hidden by metadata cached at open.
	var encodedHeader [shardedBaseMetadataSize]byte
	if _, err := file.ReadAt(encodedHeader[:], 0); err != nil {
		return fmt.Errorf("%w: reread scan header: %v", ErrInvalidShardedStreamBase, err)
	}
	gotMetadata, err := decodeShardedBaseMetadata(encodedHeader[:], shardedBaseScanMagic, shardedBaseScanRecordSize)
	if err != nil {
		return err
	}
	if gotMetadata != metadata {
		return fmt.Errorf("%w: scan header changed: got %+v, opened %+v", ErrInvalidShardedStreamBase, gotMetadata, metadata)
	}
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("accountsdb: stat sharded base scan %s: %w", path, err)
	}
	wantSize := int64(shardedBaseMetadataSize) + int64(metadata.KeyCount)*shardedBaseScanRecordSize
	if !info.Mode().IsRegular() || info.Size() != wantSize {
		return fmt.Errorf("%w: scan file size changed", ErrInvalidShardedStreamBase)
	}
	// Detect in-place mutation or path replacement over the complete scan, not
	// merely at open. The fd pins bytes, while this check enforces the stronger
	// immutable-artifact pathname contract used by the root catalog.
	defer func() {
		retErr = errors.Join(retErr, validateStableRegularFile(file, path, info))
	}()
	if expected != nil && expected.Size != uint64(wantSize) {
		return fmt.Errorf(
			"%w: identity size mismatch for %q: got %d want %d",
			ErrInvalidCatalogArtifact, expected.RelativePath, wantSize, expected.Size,
		)
	}

	var bodyReader io.Reader = io.NewSectionReader(
		file, shardedBaseMetadataSize, wantSize-shardedBaseMetadataSize,
	)
	var identityHasher hash.Hash
	if expected != nil {
		identityHasher = sha256.New()
		_, _ = identityHasher.Write(encodedHeader[:])
		bodyReader = io.TeeReader(bodyReader, identityHasher)
	}
	reader := bufio.NewReaderSize(bodyReader, 1<<20)
	bodyHasher := crc32.New(shardedBaseCRC)
	var record [shardedBaseScanRecordSize]byte
	var previous solana.PublicKey
	havePrevious := false
	for i := uint64(0); i < metadata.KeyCount; i++ {
		if i%streamIndexContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if _, err := io.ReadFull(reader, record[:]); err != nil {
			return fmt.Errorf("%w: read shard %d scan record %d: %v", ErrInvalidShardedStreamBase, metadata.ShardID, i, err)
		}
		_, _ = bodyHasher.Write(record[:])
		key := solana.PublicKey(record[:32])
		if havePrevious {
			switch cmp := bytes.Compare(previous[:], key[:]); {
			case cmp == 0:
				return fmt.Errorf("%w: duplicate scan key %x", ErrInvalidShardedStreamBase, key)
			case cmp > 0:
				return fmt.Errorf("%w: out-of-order scan key %x", ErrInvalidShardedStreamBase, key)
			}
		}
		if routed := router.Shard(key); routed != metadata.ShardID {
			return fmt.Errorf(
				"%w: scan key %x routes to %d, file is shard %d",
				ErrInvalidShardedStreamBase,
				key,
				routed,
				metadata.ShardID,
			)
		}
		locator := uint48(record[32:])
		entry, err := catalog.UnpackAccountIndexEntry(locator)
		if err != nil {
			return fmt.Errorf("%w: scan record %d locator: %v", ErrInvalidShardedStreamBase, i, err)
		}
		if err := visit(key, locator, entry); err != nil {
			return err
		}
		previous = key
		havePrevious = true
	}
	if bodyHasher.Sum32() != metadata.BodyCRC {
		return fmt.Errorf("%w: scan body CRC mismatch", ErrInvalidShardedStreamBase)
	}
	if expected != nil && !bytes.Equal(identityHasher.Sum(nil), expected.SHA256[:]) {
		return fmt.Errorf("%w: identity mismatch for %q", ErrInvalidCatalogArtifact, expected.RelativePath)
	}
	return ctx.Err()
}

// authenticateOrVerifyShardedBaseScan proves that scanFile contains the exact
// sidecar selected by expected. The O(1) path is available only when the
// root-hashed StreamHash metadata carries the same SHA-256/size and the full
// Linux dev/inode/mtime/ctime identity still matches. Any absent or stale
// identity takes the complete semantic/CRC/SHA path. This makes copying a
// generation to a new filesystem safe (slower on that open) and ensures an
// inability to obtain strong metadata can never weaken validation.
func authenticateOrVerifyShardedBaseScan(
	scanFile *os.File,
	scanPath string,
	openedInfo os.FileInfo,
	scanMetadata shardedBaseMetadata,
	indexMetadata shardedBaseMetadata,
	catalog *PersistentExtentCatalog,
	router PersistentIndexShardRouter,
	expected *IndexCatalogArtifact,
	fullyValidated *bool,
) (os.FileInfo, shardedBaseScanSeal, error) {
	if fullyValidated != nil {
		*fullyValidated = false
	}
	seal := indexMetadata.ScanSeal
	if expected != nil && !seal.isZero() {
		if seal.Size != expected.Size || seal.SHA256 != expected.SHA256 {
			return nil, shardedBaseScanSeal{}, fmt.Errorf(
				"%w: root-selected scan identity mismatch with authenticated StreamHash metadata",
				ErrInvalidShardedStreamBase,
			)
		}
	}

	// A receipt is authenticated only when this call arrived through the
	// production root-artifact path: that path has just SHA-verified the
	// StreamHash file containing it. Internal path-only helpers must retain the
	// complete validation behavior even if their metadata happens to carry a
	// plausible receipt.
	if expected != nil && !seal.isZero() {
		current, identityErr := shardedBaseScanSealFromFile(scanFile, seal.SHA256)
		stableErr := validateStableRegularFile(scanFile, scanPath, openedInfo)
		if identityErr == nil && stableErr == nil && current == seal {
			return openedInfo, current, nil
		}
	}

	// The index seal is itself useful as a complete-content expectation for
	// focused/internal callers which have no root artifact descriptor. Public
	// production open always supplies expected, whose root-selected identity is
	// authoritative.
	verifyExpected := expected
	var sealArtifact IndexCatalogArtifact
	if verifyExpected == nil && !seal.isZero() {
		sealArtifact = IndexCatalogArtifact{
			RelativePath: filepath.Base(scanPath),
			Size:         seal.Size,
			SHA256:       seal.SHA256,
		}
		verifyExpected = &sealArtifact
	}
	before, beforeErr := shardedBaseScanSealFromFile(scanFile, func() [sha256.Size]byte {
		if verifyExpected == nil {
			return [sha256.Size]byte{}
		}
		return verifyExpected.SHA256
	}())
	if err := scanShardedBaseSidecarLocatorsWithArtifact(
		context.Background(), scanFile, scanPath, scanMetadata, catalog, router,
		func(solana.PublicKey, uint64, AccountIndexEntry) error { return nil },
		verifyExpected,
	); err != nil {
		return nil, shardedBaseScanSeal{}, err
	}
	info, err := scanFile.Stat()
	if err != nil {
		return nil, shardedBaseScanSeal{}, fmt.Errorf("accountsdb: stat completely verified exact sidecar: %w", err)
	}
	if err := validateStableRegularFile(scanFile, scanPath, info); err != nil {
		return nil, shardedBaseScanSeal{}, fmt.Errorf(
			"%w: exact sidecar changed after complete validation: %v",
			ErrInvalidShardedStreamBase, err,
		)
	}
	digest := seal.SHA256
	if verifyExpected != nil {
		digest = verifyExpected.SHA256
	}
	after, afterErr := shardedBaseScanSealFromFile(scanFile, digest)
	if beforeErr == nil && afterErr == nil && before != after {
		return nil, shardedBaseScanSeal{}, fmt.Errorf(
			"%w: exact sidecar identity changed during complete validation",
			ErrInvalidShardedStreamBase,
		)
	}
	if afterErr != nil {
		// Complete byte and semantic validation succeeded; retain a zero seal so
		// every later consumer repeats that proof on platforms without ctime.
		after = shardedBaseScanSeal{}
	}
	if fullyValidated != nil {
		*fullyValidated = true
	}
	return info, after, nil
}

// OpenShardedStreamBaseShard verifies the complete root-selected StreamHash
// identity, resolves both files below root, and authenticates the exact
// sidecar through the StreamHash-embedded strong identity receipt. A stale or
// unavailable receipt falls back to complete sidecar semantic/CRC/SHA-256
// validation before the shard is exposed.
func OpenShardedStreamBaseShard(
	root string,
	indexArtifact IndexCatalogArtifact,
	scanArtifact IndexCatalogArtifact,
	catalog *PersistentExtentCatalog,
	router PersistentIndexShardRouter,
) (*ShardedStreamBaseShard, error) {
	if indexArtifact.RelativePath == scanArtifact.RelativePath {
		return nil, fmt.Errorf("%w: index and scan artifacts share a path", ErrInvalidShardedStreamBase)
	}
	if err := VerifyIndexCatalogArtifact(root, indexArtifact); err != nil {
		return nil, err
	}
	if err := validateIndexCatalogArtifactPathComponents(root, scanArtifact.RelativePath); err != nil {
		return nil, fmt.Errorf("%w: inspect scan artifact path: %v", ErrInvalidShardedStreamBase, err)
	}
	indexPath, err := ResolveIndexCatalogArtifactPath(root, indexArtifact)
	if err != nil {
		return nil, err
	}
	scanPath, err := ResolveIndexCatalogArtifactPath(root, scanArtifact)
	if err != nil {
		return nil, err
	}
	return openShardedStreamBaseShardFilesWithScanArtifact(
		indexPath, scanPath, catalog, router, &scanArtifact,
	)
}

func openShardedStreamBaseShardFiles(
	indexPath string,
	scanPath string,
	catalog *PersistentExtentCatalog,
	router PersistentIndexShardRouter,
) (*ShardedStreamBaseShard, error) {
	return openShardedStreamBaseShardFilesWithScanArtifact(
		indexPath, scanPath, catalog, router, nil,
	)
}

func openShardedStreamBaseShardFilesWithScanArtifact(
	indexPath string,
	scanPath string,
	catalog *PersistentExtentCatalog,
	router PersistentIndexShardRouter,
	expectedScan *IndexCatalogArtifact,
) (*ShardedStreamBaseShard, error) {
	if catalog == nil {
		return nil, fmt.Errorf("%w: nil extent catalog", ErrInvalidShardedStreamBase)
	}
	if err := catalog.validate(); err != nil {
		return nil, err
	}
	if err := ValidatePersistentIndexShardCount(router.Count()); err != nil {
		return nil, err
	}
	if indexPath == "" {
		return nil, fmt.Errorf("%w: empty StreamHash path", ErrInvalidShardedStreamBase)
	}
	scanFile, scanMetadata, err := openShardedBaseScanFile(scanPath, catalog, router)
	if err != nil {
		return nil, err
	}
	scanFileInfo, err := scanFile.Stat()
	if err != nil {
		_ = scanFile.Close()
		return nil, fmt.Errorf("accountsdb: stat opened sharded base scan: %w", err)
	}
	cleanupScan := true
	defer func() {
		if cleanupScan {
			_ = scanFile.Close()
		}
	}()

	// Open the final component with O_NOFOLLOW and retain the exact inode
	// identity through StreamHash's mmap setup.  A plain os.Open after Lstat has
	// a rename-to-symlink race: a link to the just-renamed original inode would
	// still satisfy os.SameFile while changing the pathname contract.
	indexFile, openedInfo, err := openStableRegularFile(indexPath)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: open sharded StreamHash base: %w", err)
	}
	idx, openErr := streamhash.OpenPayloadFile(indexFile)
	stableErr := validateStableRegularFile(indexFile, indexPath, openedInfo)
	closeIndexFileErr := indexFile.Close()
	if openErr != nil || stableErr != nil || closeIndexFileErr != nil {
		if idx != nil {
			_ = idx.Close()
		}
		return nil, fmt.Errorf(
			"accountsdb: mmap sharded StreamHash base: %w",
			errors.Join(openErr, stableErr, closeIndexFileErr),
		)
	}
	cleanupIndex := true
	defer func() {
		if cleanupIndex {
			_ = idx.Close()
		}
	}()
	stats := idx.Stats()
	if stats.Algorithm != streamhash.AlgoPTRHash ||
		stats.PayloadSize != shardedBasePayloadBytes ||
		stats.FingerprintSize != shardedBaseFingerprintBytes {
		return nil, fmt.Errorf(
			"%w: incompatible StreamHash parameters algorithm=%s payload=%d fingerprint=%d",
			ErrInvalidShardedStreamBase,
			stats.Algorithm,
			stats.PayloadSize,
			stats.FingerprintSize,
		)
	}
	indexMetadata, err := decodeShardedBaseMetadata(idx.UserMetadata(), shardedBaseIndexMetadataMagic, 0)
	if err != nil {
		return nil, err
	}
	if !sameShardedBaseGeneration(scanMetadata, indexMetadata) {
		return nil, fmt.Errorf("%w: StreamHash and scan generation metadata differ", ErrInvalidShardedStreamBase)
	}
	if indexMetadata.ShardCount != uint32(router.Count()) || indexMetadata.KeyCount != idx.NumKeys() {
		return nil, fmt.Errorf("%w: StreamHash count metadata mismatch", ErrInvalidShardedStreamBase)
	}
	if indexMetadata.CatalogGeneration > catalog.generation {
		return nil, fmt.Errorf("%w: shard catalog generation is newer than catalog", ErrInvalidShardedStreamBase)
	}
	if err := catalog.validateBinding(indexMetadata.CatalogBinding); err != nil {
		return nil, err
	}
	if err := idx.Verify(); err != nil {
		return nil, fmt.Errorf("accountsdb: verify sharded StreamHash base: %w", err)
	}
	scanFullyValidated := false
	scanInfo, scanSeal, err := authenticateOrVerifyShardedBaseScan(
		scanFile,
		scanPath,
		scanFileInfo,
		scanMetadata,
		indexMetadata,
		catalog,
		router,
		expectedScan,
		&scanFullyValidated,
	)
	if err != nil {
		return nil, err
	}
	if err := scanFile.Close(); err != nil {
		return nil, fmt.Errorf("accountsdb: close authenticated sharded base scan: %w", err)
	}
	cleanupScan = false
	// Keep the lookup seed and the exact sidecar CRC together in the runtime
	// metadata. They live in different on-disk headers, but merge cursors need
	// both while a shard is pinned open.
	indexMetadata.BodyCRC = scanMetadata.BodyCRC
	cleanupIndex = false
	shard := &ShardedStreamBaseShard{
		idx:          idx,
		indexPath:    indexPath,
		indexInfo:    openedInfo,
		scanPath:     scanPath,
		scanInfo:     scanInfo,
		scanSeal:     scanSeal,
		metadata:     indexMetadata,
		scanMetadata: scanMetadata,
		router:       router,
	}
	if scanFullyValidated {
		shard.scanFullValidations.Add(1)
	}
	shard.catalog.Store(catalog)
	return shard, nil
}

// OpenShardedStreamBaseShardArtifacts authenticates both root-selected
// artifacts before opening their contents. See OpenShardedStreamBaseShard for
// the sidecar's O(1)-receipt/full-validation fallback rule.
func OpenShardedStreamBaseShardArtifacts(
	root string,
	artifacts ShardedStreamBaseShardArtifacts,
	catalog *PersistentExtentCatalog,
	router PersistentIndexShardRouter,
) (*ShardedStreamBaseShard, error) {
	if artifacts.Generation == 0 || artifacts.Index.RelativePath == artifacts.Scan.RelativePath {
		return nil, fmt.Errorf("%w: invalid shard artifact descriptor", ErrInvalidShardedStreamBase)
	}
	shard, err := OpenShardedStreamBaseShard(root, artifacts.Index, artifacts.Scan, catalog, router)
	if err != nil {
		return nil, err
	}
	if shard.metadata.ShardID != artifacts.ShardID ||
		shard.metadata.Generation != artifacts.Generation ||
		shard.metadata.KeyCount != artifacts.KeyCount {
		_ = shard.Close()
		return nil, fmt.Errorf("%w: opened shard does not match artifact descriptor", ErrInvalidShardedStreamBase)
	}
	return shard, nil
}
