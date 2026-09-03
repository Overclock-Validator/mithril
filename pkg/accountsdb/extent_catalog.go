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
	"sync"
)

const (
	extentCatalogVersion    = uint32(1)
	extentCatalogHeaderSize = 96
	extentCatalogRecordSize = 24

	packedAccountLocatorBits = streamIndexExtentOrdinalBits + streamIndexRelativeBits
	packedAccountLocatorMask = uint64(1<<packedAccountLocatorBits) - 1
)

var (
	extentCatalogMagic = [8]byte{'M', 'I', 'T', 'H', 'X', 'E', '0', '1'}
	extentCatalogCRC   = crc32.MakeTable(crc32.Castagnoli)

	ErrInvalidExtentCatalog = errors.New("accountsdb: invalid persistent extent catalog")
	ErrInvalidBaseLocator   = errors.New("accountsdb: invalid packed account locator")
)

// PersistentExtentCatalog assigns stable 24-bit ordinals to 128 MiB appendvec
// extents. A replacement catalog is append-only with respect to its predecessor:
// existing ordinals never move, while newly observed extents are appended.
//
// The catalog is shared by every immutable shard in one lineage. Each shard
// binds the exact catalog prefix it needs, so a shard can never silently decode
// its six-byte payload against reordered ordinals while later append-only
// catalog extensions remain compatible.
type PersistentExtentCatalog struct {
	generation uint64
	lineage    [sha256.Size]byte
	extents    []streamIndexExtent
	ordinals   map[streamIndexExtent]uint32
	identity   extentCatalogIdentity
	artifact   IndexCatalogArtifact
	// verifiedPath/verifiedInfo are populated only by an artifact-bound open or
	// by adopting the exact inode from the builder's complete identity pass. They
	// let snapshot bootstrap hand the already-verified immutable generation to
	// AccountsDB without hashing this file a second time.
	verifiedPath string
	verifiedInfo os.FileInfo
	frozen       bool

	prefixMu     sync.Mutex
	prefixHashes map[uint32][sha256.Size]byte
}

type extentCatalogIdentity struct {
	Size   uint64
	SHA256 [sha256.Size]byte
}

// verifiedPersistentExtentCatalogArtifact is issued only after one stable,
// no-follow read has both hashed the complete file and proved that every byte
// semantically encodes the supplied in-memory catalog. Keeping this token
// distinct from a generic artifact identity prevents adoption of a file whose
// hash is valid but whose records disagree with the object used by this process.
type verifiedPersistentExtentCatalogArtifact struct {
	artifact     IndexCatalogArtifact
	absolutePath string
	fileInfo     os.FileInfo
	catalog      *PersistentExtentCatalog
}

// persistentExtentCatalogExtension inventories a prospective build without
// copying the complete immutable catalog first. Most rolling shard rebases do
// not discover a new appendvec extent: another shard has usually introduced
// the same fold segment already. In that case finish returns the exact prior
// catalog/artifact and the build performs O(1) catalog work.
type persistentExtentCatalogExtension struct {
	previous         *PersistentExtentCatalog
	generation       uint64
	additions        []streamIndexExtent
	additionOrdinals map[streamIndexExtent]uint32
}

// extentCatalogBinding is embedded in immutable shard artifacts. It binds the
// ordinal prefix visible when the shard was built rather than the complete
// catalog file: a later append-only catalog can therefore serve old and newly
// rebuilt shards simultaneously, while reordering any old ordinal fails.
type extentCatalogBinding struct {
	Lineage       [sha256.Size]byte
	RequiredCount uint32
	PrefixSHA256  [sha256.Size]byte
}

func (catalog *PersistentExtentCatalog) Generation() uint64 {
	if catalog == nil {
		return 0
	}
	return catalog.generation
}

func (catalog *PersistentExtentCatalog) Len() int {
	if catalog == nil {
		return 0
	}
	return len(catalog.extents)
}

// Artifact returns the root-relative identity when the catalog was built or
// opened through an IndexCatalogArtifact. OpenPersistentExtentCatalog, which
// accepts only an absolute path, returns an identity with an empty path.
func (catalog *PersistentExtentCatalog) Artifact() IndexCatalogArtifact {
	if catalog == nil {
		return IndexCatalogArtifact{}
	}
	return catalog.artifact
}

func (catalog *PersistentExtentCatalog) binding() (extentCatalogBinding, error) {
	if err := catalog.validate(); err != nil {
		return extentCatalogBinding{}, err
	}
	count := uint32(len(catalog.extents))
	digest, err := catalog.prefixHash(count)
	if err != nil {
		return extentCatalogBinding{}, err
	}
	return extentCatalogBinding{
		Lineage:       catalog.lineage,
		RequiredCount: count,
		PrefixSHA256:  digest,
	}, nil
}

func (catalog *PersistentExtentCatalog) validateBinding(binding extentCatalogBinding) error {
	if err := catalog.validate(); err != nil {
		return err
	}
	if binding.Lineage != catalog.lineage {
		return fmt.Errorf("%w: shard extent-catalog lineage mismatch", ErrInvalidExtentCatalog)
	}
	if uint64(binding.RequiredCount) > uint64(len(catalog.extents)) {
		return fmt.Errorf(
			"%w: shard requires %d extents, catalog has %d",
			ErrInvalidExtentCatalog,
			binding.RequiredCount,
			len(catalog.extents),
		)
	}
	digest, err := catalog.prefixHash(binding.RequiredCount)
	if err != nil {
		return err
	}
	if digest != binding.PrefixSHA256 {
		return fmt.Errorf("%w: shard extent-prefix digest mismatch", ErrInvalidExtentCatalog)
	}
	return nil
}

func (catalog *PersistentExtentCatalog) prefixHash(count uint32) ([sha256.Size]byte, error) {
	if catalog == nil || uint64(count) > uint64(len(catalog.extents)) {
		return [sha256.Size]byte{}, fmt.Errorf("%w: invalid prefix length %d", ErrInvalidExtentCatalog, count)
	}
	catalog.prefixMu.Lock()
	defer catalog.prefixMu.Unlock()
	if digest, ok := catalog.prefixHashes[count]; ok {
		return digest, nil
	}
	hasher := sha256.New()
	var encoded [extentCatalogRecordSize]byte
	for _, extent := range catalog.extents[:count] {
		encodeExtentCatalogRecord(encoded[:], extent)
		_, _ = hasher.Write(encoded[:])
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	if catalog.prefixHashes == nil {
		catalog.prefixHashes = make(map[uint32][sha256.Size]byte)
	}
	catalog.prefixHashes[count] = digest
	return digest, nil
}

// precomputePrefixHashes hashes every requested prefix in one ascending pass.
// It returns the number of extent records fed to SHA-256, which is at most the
// largest requested prefix (and therefore at most the catalog length). Startup
// uses this before opening shards so distinct rolling-shard prefix lengths do
// not turn validation into O(shards*extents) work.
func (catalog *PersistentExtentCatalog) precomputePrefixHashes(counts []uint32) (uint32, error) {
	if err := catalog.validate(); err != nil {
		return 0, err
	}
	requested := append([]uint32(nil), counts...)
	for _, count := range requested {
		if uint64(count) > uint64(len(catalog.extents)) {
			return 0, fmt.Errorf("%w: invalid prefix length %d", ErrInvalidExtentCatalog, count)
		}
	}
	sort.Slice(requested, func(i, j int) bool { return requested[i] < requested[j] })

	catalog.prefixMu.Lock()
	defer catalog.prefixMu.Unlock()
	if catalog.prefixHashes == nil {
		catalog.prefixHashes = make(map[uint32][sha256.Size]byte)
	}
	allCached := true
	for _, count := range requested {
		if _, ok := catalog.prefixHashes[count]; !ok {
			allCached = false
			break
		}
	}
	if allCached {
		return 0, nil
	}

	hasher := sha256.New()
	var encoded [extentCatalogRecordSize]byte
	advanced := uint32(0)
	var previous uint32
	havePrevious := false
	for _, count := range requested {
		if havePrevious && count == previous {
			continue
		}
		for advanced < count {
			encodeExtentCatalogRecord(encoded[:], catalog.extents[advanced])
			_, _ = hasher.Write(encoded[:])
			advanced++
		}
		var digest [sha256.Size]byte
		copy(digest[:], hasher.Sum(nil))
		catalog.prefixHashes[count] = digest
		previous = count
		havePrevious = true
	}
	return advanced, nil
}

// precomputeBindingPrefixes prepares every digest needed to validate a set of
// immutable shard bindings in one ascending pass. Rolling publication must use
// this before calling validateBinding in a loop: independently hashing many
// staggered shard prefixes would otherwise be O(shards*extents).
func (catalog *PersistentExtentCatalog) precomputeBindingPrefixes(
	bindings []extentCatalogBinding,
) (uint32, error) {
	counts := make([]uint32, len(bindings))
	for i := range bindings {
		counts[i] = bindings[i].RequiredCount
	}
	return catalog.precomputePrefixHashes(counts)
}

// PackAccountIndexEntry encodes an appendvec location as a 24-bit stable extent
// ordinal followed by a 24-bit offset in eight-byte units.
func (catalog *PersistentExtentCatalog) PackAccountIndexEntry(entry AccountIndexEntry) (uint64, error) {
	if catalog == nil {
		return 0, fmt.Errorf("%w: nil extent catalog", ErrInvalidBaseLocator)
	}
	if entry.Offset%8 != 0 {
		return 0, fmt.Errorf("%w: offset %d is not eight-byte aligned", ErrInvalidBaseLocator, entry.Offset)
	}
	extent := extentForAccountIndexEntry(entry)
	ordinal, ok := catalog.ordinals[extent]
	if !ok {
		return 0, fmt.Errorf(
			"%w: no extent for slot=%d file=%d base=%d",
			ErrInvalidBaseLocator,
			extent.Slot,
			extent.FileID,
			extent.BaseOffset,
		)
	}
	relativeUnits := (entry.Offset - extent.BaseOffset) / 8
	if relativeUnits > streamIndexPayloadMask {
		return 0, fmt.Errorf("%w: relative offset %d overflows 24 bits", ErrInvalidBaseLocator, relativeUnits)
	}
	return uint64(ordinal)<<streamIndexRelativeBits | relativeUnits, nil
}

// UnpackAccountIndexEntry decodes one six-byte locator. The returned location
// remains a candidate until its complete pubkey is verified in the appendvec.
func (catalog *PersistentExtentCatalog) UnpackAccountIndexEntry(locator uint64) (AccountIndexEntry, error) {
	if catalog == nil {
		return AccountIndexEntry{}, fmt.Errorf("%w: nil extent catalog", ErrInvalidBaseLocator)
	}
	if locator > packedAccountLocatorMask {
		return AccountIndexEntry{}, fmt.Errorf("%w: locator %#x exceeds 48 bits", ErrInvalidBaseLocator, locator)
	}
	ordinal := locator >> streamIndexRelativeBits
	if ordinal >= uint64(len(catalog.extents)) {
		return AccountIndexEntry{}, fmt.Errorf(
			"%w: extent ordinal %d >= catalog size %d",
			ErrInvalidBaseLocator,
			ordinal,
			len(catalog.extents),
		)
	}
	extent := catalog.extents[ordinal]
	relativeOffset := (locator & streamIndexPayloadMask) * 8
	if extent.BaseOffset > ^uint64(0)-relativeOffset {
		return AccountIndexEntry{}, fmt.Errorf(
			"%w: extent base %d plus relative offset %d overflows uint64",
			ErrInvalidBaseLocator,
			extent.BaseOffset,
			relativeOffset,
		)
	}
	return AccountIndexEntry{
		Slot:   extent.Slot,
		FileId: extent.FileID,
		Offset: extent.BaseOffset + relativeOffset,
	}, nil
}

func clonePersistentExtentCatalog(previous *PersistentExtentCatalog, generation uint64) (*PersistentExtentCatalog, error) {
	if generation == 0 {
		return nil, fmt.Errorf("%w: zero generation", ErrInvalidExtentCatalog)
	}
	if previous != nil {
		if err := previous.validate(); err != nil {
			return nil, err
		}
		if generation <= previous.generation {
			return nil, fmt.Errorf(
				"%w: generation %d does not follow %d",
				ErrInvalidExtentCatalog,
				generation,
				previous.generation,
			)
		}
	}
	var lineage [sha256.Size]byte
	if previous != nil {
		lineage = previous.lineage
	} else if _, err := io.ReadFull(cryptorand.Reader, lineage[:]); err != nil {
		return nil, fmt.Errorf("%w: generate lineage: %v", ErrInvalidExtentCatalog, err)
	}

	extentCapacity := 1024
	if previous != nil && len(previous.extents) > extentCapacity {
		extentCapacity = len(previous.extents)
	}
	catalog := &PersistentExtentCatalog{
		generation:   generation,
		lineage:      lineage,
		extents:      make([]streamIndexExtent, 0, extentCapacity),
		ordinals:     make(map[streamIndexExtent]uint32, extentCapacity),
		prefixHashes: make(map[uint32][sha256.Size]byte),
	}
	if previous != nil {
		catalog.extents = append(catalog.extents, previous.extents...)
		for extent, ordinal := range previous.ordinals {
			catalog.ordinals[extent] = ordinal
		}
		// Every predecessor prefix is byte-identical in an append-only
		// successor. Carry its verified digest cache forward so rolling
		// publication does not rehash the complete catalog while holding the
		// root publication lock. prefixMu also makes this safe if a concurrent
		// scan is filling another predecessor prefix.
		previous.prefixMu.Lock()
		for count, digest := range previous.prefixHashes {
			catalog.prefixHashes[count] = digest
		}
		previous.prefixMu.Unlock()
	}
	return catalog, nil
}

func newPersistentExtentCatalogExtension(
	previous *PersistentExtentCatalog,
	generation uint64,
) (*persistentExtentCatalogExtension, error) {
	if generation == 0 {
		return nil, fmt.Errorf("%w: zero generation", ErrInvalidExtentCatalog)
	}
	if previous != nil {
		if err := previous.validate(); err != nil {
			return nil, err
		}
		if generation <= previous.generation {
			return nil, fmt.Errorf(
				"%w: generation %d does not follow %d",
				ErrInvalidExtentCatalog,
				generation,
				previous.generation,
			)
		}
	}
	return &persistentExtentCatalogExtension{previous: previous, generation: generation}, nil
}

func (extension *persistentExtentCatalogExtension) ensureExtent(entry AccountIndexEntry) error {
	if extension == nil {
		return fmt.Errorf("%w: nil catalog extension", ErrInvalidExtentCatalog)
	}
	if entry.Offset%8 != 0 {
		return fmt.Errorf("%w: offset %d is not eight-byte aligned", ErrInvalidBaseLocator, entry.Offset)
	}
	extent := extentForAccountIndexEntry(entry)
	if extension.previous != nil {
		if _, exists := extension.previous.ordinals[extent]; exists {
			return nil
		}
	}
	if _, exists := extension.additionOrdinals[extent]; exists {
		return nil
	}
	previousCount := 0
	if extension.previous != nil {
		previousCount = len(extension.previous.extents)
	}
	if previousCount+len(extension.additions) >= streamIndexMaxExtents {
		return fmt.Errorf("%w: extent count exceeds %d", ErrInvalidExtentCatalog, streamIndexMaxExtents)
	}
	if extension.additionOrdinals == nil {
		extension.additionOrdinals = make(map[streamIndexExtent]uint32)
	}
	ordinal := uint32(previousCount + len(extension.additions))
	extension.additions = append(extension.additions, extent)
	extension.additionOrdinals[extent] = ordinal
	return nil
}

// finish returns ownsArtifact=false only when the prior immutable catalog is
// already complete for this build. Otherwise it materializes one append-only
// successor; callers must persist and own that new artifact.
func (extension *persistentExtentCatalogExtension) finish() (
	catalog *PersistentExtentCatalog,
	ownsArtifact bool,
	err error,
) {
	if extension == nil {
		return nil, false, fmt.Errorf("%w: nil catalog extension", ErrInvalidExtentCatalog)
	}
	if extension.previous != nil && len(extension.additions) == 0 {
		return extension.previous, false, nil
	}
	catalog, err = clonePersistentExtentCatalog(extension.previous, extension.generation)
	if err != nil {
		return nil, false, err
	}
	for _, extent := range extension.additions {
		if err := catalog.ensureRawExtent(extent); err != nil {
			return nil, false, err
		}
	}
	return catalog, true, nil
}

func (catalog *PersistentExtentCatalog) ensureExtent(entry AccountIndexEntry) error {
	if entry.Offset%8 != 0 {
		return fmt.Errorf("%w: offset %d is not eight-byte aligned", ErrInvalidBaseLocator, entry.Offset)
	}
	return catalog.ensureRawExtent(extentForAccountIndexEntry(entry))
}

func (catalog *PersistentExtentCatalog) ensureRawExtent(extent streamIndexExtent) error {
	if catalog == nil {
		return fmt.Errorf("%w: nil catalog", ErrInvalidExtentCatalog)
	}
	if extent.BaseOffset%streamIndexExtentSize != 0 {
		return fmt.Errorf("%w: unaligned extent base %d", ErrInvalidExtentCatalog, extent.BaseOffset)
	}
	if _, exists := catalog.ordinals[extent]; exists {
		return nil
	}
	if len(catalog.extents) >= streamIndexMaxExtents {
		return fmt.Errorf("%w: extent count exceeds %d", ErrInvalidExtentCatalog, streamIndexMaxExtents)
	}
	ordinal := uint32(len(catalog.extents))
	catalog.extents = append(catalog.extents, extent)
	catalog.ordinals[extent] = ordinal
	return nil
}

func (catalog *PersistentExtentCatalog) validate() error {
	if catalog == nil {
		return fmt.Errorf("%w: nil catalog", ErrInvalidExtentCatalog)
	}
	if catalog.frozen {
		return nil
	}
	if catalog.generation == 0 {
		return fmt.Errorf("%w: zero generation", ErrInvalidExtentCatalog)
	}
	if allZero(catalog.lineage[:]) {
		return fmt.Errorf("%w: zero lineage", ErrInvalidExtentCatalog)
	}
	if len(catalog.extents) > streamIndexMaxExtents {
		return fmt.Errorf("%w: extent count %d exceeds %d", ErrInvalidExtentCatalog, len(catalog.extents), streamIndexMaxExtents)
	}
	if len(catalog.ordinals) != len(catalog.extents) {
		return fmt.Errorf("%w: extent and ordinal counts differ", ErrInvalidExtentCatalog)
	}
	for i, extent := range catalog.extents {
		if extent.BaseOffset%streamIndexExtentSize != 0 {
			return fmt.Errorf("%w: extent %d has unaligned base %d", ErrInvalidExtentCatalog, i, extent.BaseOffset)
		}
		ordinal, exists := catalog.ordinals[extent]
		if !exists || ordinal != uint32(i) {
			return fmt.Errorf("%w: extent %d has inconsistent ordinal", ErrInvalidExtentCatalog, i)
		}
	}
	return nil
}

// verifyWrittenPersistentExtentCatalog performs the catalog-specific durable
// handoff in one streaming pass. Besides producing the complete root-catalog
// SHA-256 identity, it validates the on-disk header and CRCs and byte-compares
// every extent record to the in-memory successor. This is the semantic check
// which makes adopting that existing object equivalent to reopening the file,
// without allocating and populating a duplicate extent slice and ordinal map.
func verifyWrittenPersistentExtentCatalog(
	ctx context.Context,
	root string,
	relative string,
	catalog *PersistentExtentCatalog,
) (_ verifiedPersistentExtentCatalogArtifact, retErr error) {
	if ctx == nil {
		return verifiedPersistentExtentCatalogArtifact{}, errors.New("accountsdb: nil written extent-catalog verification context")
	}
	if err := ctx.Err(); err != nil {
		return verifiedPersistentExtentCatalogArtifact{}, err
	}
	if catalog == nil {
		return verifiedPersistentExtentCatalogArtifact{}, fmt.Errorf("%w: cannot verify a nil catalog", ErrInvalidExtentCatalog)
	}
	if err := catalog.validate(); err != nil {
		return verifiedPersistentExtentCatalogArtifact{}, err
	}
	artifact := IndexCatalogArtifact{RelativePath: relative, Size: 1}
	artifact.SHA256[0] = 1
	resolved, err := ResolveIndexCatalogArtifactPath(root, artifact)
	if err != nil {
		return verifiedPersistentExtentCatalogArtifact{}, err
	}
	if err := validateIndexCatalogArtifactPathComponents(root, relative); err != nil {
		return verifiedPersistentExtentCatalogArtifact{}, fmt.Errorf(
			"%w: inspect %q: %v",
			ErrInvalidExtentCatalog,
			relative,
			err,
		)
	}

	file, info, err := openStableRegularFile(resolved)
	if err != nil {
		return verifiedPersistentExtentCatalogArtifact{}, fmt.Errorf("%w: open written artifact: %v", ErrInvalidExtentCatalog, err)
	}
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	expectedSize := uint64(extentCatalogHeaderSize) + uint64(len(catalog.extents))*extentCatalogRecordSize
	if info.Size() < 0 || uint64(info.Size()) != expectedSize {
		return verifiedPersistentExtentCatalogArtifact{}, fmt.Errorf(
			"%w: written artifact size %d does not match %d catalog bytes",
			ErrInvalidExtentCatalog,
			info.Size(),
			expectedSize,
		)
	}

	hasher := sha256.New()
	reader := bufio.NewReaderSize(io.TeeReader(file, hasher), 1<<20)
	var header [extentCatalogHeaderSize]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return verifiedPersistentExtentCatalogArtifact{}, fmt.Errorf("%w: read written header: %v", ErrInvalidExtentCatalog, err)
	}
	generation, count, bodyCRC, lineage, err := decodeExtentCatalogHeader(header[:], uint64(info.Size()))
	if err != nil {
		return verifiedPersistentExtentCatalogArtifact{}, err
	}
	if generation != catalog.generation || count != uint64(len(catalog.extents)) || lineage != catalog.lineage {
		return verifiedPersistentExtentCatalogArtifact{}, fmt.Errorf(
			"%w: written header generation/count/lineage disagrees with in-memory catalog",
			ErrInvalidExtentCatalog,
		)
	}

	bodyHasher := crc32.New(extentCatalogCRC)
	var encoded [extentCatalogRecordSize]byte
	var expected [extentCatalogRecordSize]byte
	for ordinal, extent := range catalog.extents {
		if ordinal%streamIndexContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return verifiedPersistentExtentCatalogArtifact{}, err
			}
		}
		if _, err := io.ReadFull(reader, encoded[:]); err != nil {
			return verifiedPersistentExtentCatalogArtifact{}, fmt.Errorf(
				"%w: read written extent %d: %v",
				ErrInvalidExtentCatalog,
				ordinal,
				err,
			)
		}
		_, _ = bodyHasher.Write(encoded[:])
		encodeExtentCatalogRecord(expected[:], extent)
		if !bytes.Equal(encoded[:], expected[:]) {
			return verifiedPersistentExtentCatalogArtifact{}, fmt.Errorf(
				"%w: written extent %d disagrees with in-memory catalog",
				ErrInvalidExtentCatalog,
				ordinal,
			)
		}
	}
	if bodyHasher.Sum32() != bodyCRC {
		return verifiedPersistentExtentCatalogArtifact{}, fmt.Errorf("%w: written body CRC mismatch", ErrInvalidExtentCatalog)
	}
	if _, err := reader.Peek(1); !errors.Is(err, io.EOF) {
		if err == nil {
			return verifiedPersistentExtentCatalogArtifact{}, fmt.Errorf("%w: written artifact has trailing bytes", ErrInvalidExtentCatalog)
		}
		return verifiedPersistentExtentCatalogArtifact{}, fmt.Errorf("%w: inspect written trailing bytes: %v", ErrInvalidExtentCatalog, err)
	}
	if err := validateStableRegularFile(file, resolved, info); err != nil {
		return verifiedPersistentExtentCatalogArtifact{}, fmt.Errorf("%w: written artifact changed during verification: %v", ErrInvalidExtentCatalog, err)
	}
	if err := ctx.Err(); err != nil {
		return verifiedPersistentExtentCatalogArtifact{}, err
	}

	artifact.Size = uint64(info.Size())
	copy(artifact.SHA256[:], hasher.Sum(nil))
	return verifiedPersistentExtentCatalogArtifact{
		artifact:     artifact,
		absolutePath: resolved,
		fileInfo:     info,
		catalog:      catalog,
	}, nil
}

// adoptVerifiedArtifact freezes a newly written catalog in place and binds it
// to the exact inode and complete SHA-256 identity measured after publication.
// The catalog writer already validated and encoded this same in-memory object;
// retaining it avoids allocating and populating an equivalent second slice and
// map solely to read our own immutable output back.
func (catalog *PersistentExtentCatalog) adoptVerifiedArtifact(
	root string,
	verified verifiedPersistentExtentCatalogArtifact,
) error {
	if catalog == nil {
		return fmt.Errorf("%w: cannot adopt a nil catalog", ErrInvalidExtentCatalog)
	}
	if catalog.frozen || !catalog.artifact.isZero() || catalog.identity.Size != 0 ||
		!allZero(catalog.identity.SHA256[:]) || catalog.verifiedPath != "" || catalog.verifiedInfo != nil {
		return fmt.Errorf("%w: catalog already has an immutable artifact identity", ErrInvalidExtentCatalog)
	}
	if verified.catalog != catalog {
		return fmt.Errorf("%w: verified artifact belongs to a different in-memory catalog", ErrInvalidExtentCatalog)
	}
	if err := validateRequiredCatalogArtifact(verified.artifact); err != nil {
		return fmt.Errorf("%w: adopt artifact: %v", ErrInvalidExtentCatalog, err)
	}
	if verified.absolutePath == "" || verified.fileInfo == nil {
		return fmt.Errorf("%w: incomplete verified artifact identity", ErrInvalidExtentCatalog)
	}
	resolved, err := ResolveIndexCatalogArtifactPath(root, verified.artifact)
	if err != nil {
		return fmt.Errorf("%w: resolve adopted artifact: %v", ErrInvalidExtentCatalog, err)
	}
	if err := validateIndexCatalogArtifactPathComponents(root, verified.artifact.RelativePath); err != nil {
		return fmt.Errorf("%w: inspect adopted artifact path: %v", ErrInvalidExtentCatalog, err)
	}
	verifiedAbsolute, err := filepath.Abs(verified.absolutePath)
	if err != nil {
		return fmt.Errorf("%w: resolve verified artifact path: %v", ErrInvalidExtentCatalog, err)
	}
	if filepath.Clean(resolved) != filepath.Clean(verifiedAbsolute) {
		return fmt.Errorf(
			"%w: verified artifact path %q does not match selected path %q",
			ErrInvalidExtentCatalog,
			verifiedAbsolute,
			resolved,
		)
	}
	expectedSize := uint64(extentCatalogHeaderSize) + uint64(len(catalog.extents))*extentCatalogRecordSize
	if verified.artifact.Size != expectedSize || verified.fileInfo.Size() < 0 ||
		uint64(verified.fileInfo.Size()) != expectedSize {
		return fmt.Errorf(
			"%w: adopted artifact size %d/inode size %d does not match %d catalog bytes",
			ErrInvalidExtentCatalog,
			verified.artifact.Size,
			verified.fileInfo.Size(),
			expectedSize,
		)
	}
	if err := validateRegularFilePathIdentity(resolved, verified.fileInfo); err != nil {
		return fmt.Errorf("%w: adopt verified artifact: %v", ErrInvalidExtentCatalog, err)
	}

	catalog.identity = extentCatalogIdentity{
		Size:   verified.artifact.Size,
		SHA256: verified.artifact.SHA256,
	}
	catalog.artifact = verified.artifact
	catalog.verifiedPath = resolved
	catalog.verifiedInfo = verified.fileInfo
	catalog.frozen = true
	return nil
}

// OpenPersistentExtentCatalog reads and validates a catalog at path. It
// verifies the fixed header, both CRCs, exact length, extent alignment and
// duplicate ordinals before returning it.
func OpenPersistentExtentCatalog(path string) (*PersistentExtentCatalog, error) {
	return openPersistentExtentCatalog(path, nil)
}

// OpenPersistentExtentCatalogArtifact additionally binds the decoded file to
// the complete size/SHA-256 identity selected by a root catalog.
func OpenPersistentExtentCatalogArtifact(root string, artifact IndexCatalogArtifact) (*PersistentExtentCatalog, error) {
	path, err := ResolveIndexCatalogArtifactPath(root, artifact)
	if err != nil {
		return nil, err
	}
	if err := validateIndexCatalogArtifactPathComponents(root, artifact.RelativePath); err != nil {
		return nil, fmt.Errorf("%w: inspect artifact path: %v", ErrInvalidExtentCatalog, err)
	}
	return openPersistentExtentCatalog(path, &artifact)
}

func openPersistentExtentCatalog(path string, expected *IndexCatalogArtifact) (*PersistentExtentCatalog, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: empty path", ErrInvalidExtentCatalog)
	}
	file, info, err := openStableRegularFile(path)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: open extent catalog: %w", err)
	}
	defer file.Close()
	if expected != nil && (info.Size() < 0 || uint64(info.Size()) != expected.Size) {
		return nil, fmt.Errorf(
			"%w: artifact size %d does not match selected size %d",
			ErrInvalidExtentCatalog, info.Size(), expected.Size,
		)
	}

	hasher := sha256.New()
	reader := bufio.NewReaderSize(io.TeeReader(file, hasher), 1<<20)
	var header [extentCatalogHeaderSize]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, fmt.Errorf("%w: read header: %v", ErrInvalidExtentCatalog, err)
	}
	generation, count, bodyCRC, lineage, err := decodeExtentCatalogHeader(header[:], uint64(info.Size()))
	if err != nil {
		return nil, err
	}

	catalog := &PersistentExtentCatalog{
		generation:   generation,
		lineage:      lineage,
		extents:      make([]streamIndexExtent, int(count)),
		ordinals:     make(map[streamIndexExtent]uint32, int(count)),
		prefixHashes: make(map[uint32][sha256.Size]byte),
	}
	bodyHasher := crc32.New(extentCatalogCRC)
	var encoded [extentCatalogRecordSize]byte
	for i := range catalog.extents {
		if _, err := io.ReadFull(reader, encoded[:]); err != nil {
			return nil, fmt.Errorf("%w: read extent %d: %v", ErrInvalidExtentCatalog, i, err)
		}
		_, _ = bodyHasher.Write(encoded[:])
		extent := streamIndexExtent{
			Slot:       binary.LittleEndian.Uint64(encoded[0:8]),
			FileID:     binary.LittleEndian.Uint64(encoded[8:16]),
			BaseOffset: binary.LittleEndian.Uint64(encoded[16:24]),
		}
		if extent.BaseOffset%streamIndexExtentSize != 0 {
			return nil, fmt.Errorf("%w: extent %d has unaligned base %d", ErrInvalidExtentCatalog, i, extent.BaseOffset)
		}
		if previous, duplicate := catalog.ordinals[extent]; duplicate {
			return nil, fmt.Errorf("%w: extent %d duplicates ordinal %d", ErrInvalidExtentCatalog, i, previous)
		}
		catalog.extents[i] = extent
		catalog.ordinals[extent] = uint32(i)
	}
	if bodyHasher.Sum32() != bodyCRC {
		return nil, fmt.Errorf("%w: body CRC mismatch", ErrInvalidExtentCatalog)
	}
	if _, err := reader.Peek(1); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("%w: trailing bytes", ErrInvalidExtentCatalog)
		}
		return nil, fmt.Errorf("%w: inspect trailing bytes: %v", ErrInvalidExtentCatalog, err)
	}
	if err := validateStableRegularFile(file, path, info); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidExtentCatalog, err)
	}

	catalog.identity.Size = uint64(info.Size())
	copy(catalog.identity.SHA256[:], hasher.Sum(nil))
	if expected != nil {
		if expected.Size != catalog.identity.Size || !bytes.Equal(expected.SHA256[:], catalog.identity.SHA256[:]) {
			return nil, fmt.Errorf("%w: artifact identity mismatch", ErrInvalidExtentCatalog)
		}
		catalog.artifact = *expected
		catalog.verifiedPath = path
		catalog.verifiedInfo = info
	}
	if err := catalog.validate(); err != nil {
		return nil, err
	}
	catalog.frozen = true
	return catalog, nil
}

func decodeExtentCatalogHeader(header []byte, fileSize uint64) (generation, count uint64, bodyCRC uint32, lineage [sha256.Size]byte, err error) {
	if len(header) != extentCatalogHeaderSize {
		return 0, 0, 0, lineage, fmt.Errorf("%w: header length %d", ErrInvalidExtentCatalog, len(header))
	}
	if !bytes.Equal(header[0:8], extentCatalogMagic[:]) {
		return 0, 0, 0, lineage, fmt.Errorf("%w: bad magic", ErrInvalidExtentCatalog)
	}
	if version := binary.LittleEndian.Uint32(header[8:12]); version != extentCatalogVersion {
		return 0, 0, 0, lineage, fmt.Errorf("%w: unsupported version %d", ErrInvalidExtentCatalog, version)
	}
	if size := binary.LittleEndian.Uint32(header[12:16]); size != extentCatalogHeaderSize {
		return 0, 0, 0, lineage, fmt.Errorf("%w: header size %d", ErrInvalidExtentCatalog, size)
	}
	if size := binary.LittleEndian.Uint32(header[16:20]); size != extentCatalogRecordSize {
		return 0, 0, 0, lineage, fmt.Errorf("%w: record size %d", ErrInvalidExtentCatalog, size)
	}
	if !allZero(header[20:24]) || !allZero(header[52:56]) || !allZero(header[88:92]) {
		return 0, 0, 0, lineage, fmt.Errorf("%w: non-zero reserved bytes", ErrInvalidExtentCatalog)
	}
	generation = binary.LittleEndian.Uint64(header[24:32])
	if generation == 0 {
		return 0, 0, 0, lineage, fmt.Errorf("%w: zero generation", ErrInvalidExtentCatalog)
	}
	count = binary.LittleEndian.Uint64(header[32:40])
	if count > streamIndexMaxExtents {
		return 0, 0, 0, lineage, fmt.Errorf("%w: extent count %d exceeds %d", ErrInvalidExtentCatalog, count, streamIndexMaxExtents)
	}
	bodySize := binary.LittleEndian.Uint64(header[40:48])
	if bodySize != count*extentCatalogRecordSize {
		return 0, 0, 0, lineage, fmt.Errorf("%w: body size %d disagrees with count %d", ErrInvalidExtentCatalog, bodySize, count)
	}
	if bodySize > ^uint64(0)-extentCatalogHeaderSize || fileSize != extentCatalogHeaderSize+bodySize {
		return 0, 0, 0, lineage, fmt.Errorf("%w: file size %d disagrees with body size %d", ErrInvalidExtentCatalog, fileSize, bodySize)
	}
	bodyCRC = binary.LittleEndian.Uint32(header[48:52])
	copy(lineage[:], header[56:88])
	if allZero(lineage[:]) {
		return 0, 0, 0, lineage, fmt.Errorf("%w: zero lineage", ErrInvalidExtentCatalog)
	}
	wantHeaderCRC := binary.LittleEndian.Uint32(header[92:96])
	if got := crc32.Checksum(header[:92], extentCatalogCRC); got != wantHeaderCRC {
		return 0, 0, 0, lineage, fmt.Errorf("%w: header CRC mismatch", ErrInvalidExtentCatalog)
	}
	return generation, count, bodyCRC, lineage, nil
}

func encodeExtentCatalogHeader(generation, count uint64, bodyCRC uint32, lineage [sha256.Size]byte) ([extentCatalogHeaderSize]byte, error) {
	var header [extentCatalogHeaderSize]byte
	if generation == 0 {
		return header, fmt.Errorf("%w: zero generation", ErrInvalidExtentCatalog)
	}
	if count > streamIndexMaxExtents {
		return header, fmt.Errorf("%w: extent count %d exceeds %d", ErrInvalidExtentCatalog, count, streamIndexMaxExtents)
	}
	if allZero(lineage[:]) {
		return header, fmt.Errorf("%w: zero lineage", ErrInvalidExtentCatalog)
	}
	copy(header[0:8], extentCatalogMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], extentCatalogVersion)
	binary.LittleEndian.PutUint32(header[12:16], extentCatalogHeaderSize)
	binary.LittleEndian.PutUint32(header[16:20], extentCatalogRecordSize)
	binary.LittleEndian.PutUint64(header[24:32], generation)
	binary.LittleEndian.PutUint64(header[32:40], count)
	binary.LittleEndian.PutUint64(header[40:48], count*extentCatalogRecordSize)
	binary.LittleEndian.PutUint32(header[48:52], bodyCRC)
	copy(header[56:88], lineage[:])
	binary.LittleEndian.PutUint32(header[92:96], crc32.Checksum(header[:92], extentCatalogCRC))
	return header, nil
}

func writePersistentExtentCatalog(ctx context.Context, path string, catalog *PersistentExtentCatalog) (retErr error) {
	if ctx == nil {
		return errors.New("accountsdb: nil extent catalog write context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := catalog.validate(); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("accountsdb: create extent catalog: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, file.Close())
		}
	}()
	if n, err := file.Write(make([]byte, extentCatalogHeaderSize)); err != nil {
		return fmt.Errorf("accountsdb: reserve extent catalog header: %w", err)
	} else if n != extentCatalogHeaderSize {
		return fmt.Errorf("accountsdb: reserve extent catalog header: wrote %d of %d bytes: %w", n, extentCatalogHeaderSize, io.ErrShortWrite)
	}

	writer := bufio.NewWriterSize(file, 1<<20)
	bodyHasher := crc32.New(extentCatalogCRC)
	var encoded [extentCatalogRecordSize]byte
	for i, extent := range catalog.extents {
		if i%streamIndexContextCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		encodeExtentCatalogRecord(encoded[:], extent)
		if _, err := writer.Write(encoded[:]); err != nil {
			return fmt.Errorf("accountsdb: write extent catalog record %d: %w", i, err)
		}
		_, _ = bodyHasher.Write(encoded[:])
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("accountsdb: flush extent catalog: %w", err)
	}
	header, err := encodeExtentCatalogHeader(catalog.generation, uint64(len(catalog.extents)), bodyHasher.Sum32(), catalog.lineage)
	if err != nil {
		return err
	}
	if n, err := file.WriteAt(header[:], 0); err != nil {
		return fmt.Errorf("accountsdb: write extent catalog header: %w", err)
	} else if n != len(header) {
		return fmt.Errorf("accountsdb: write extent catalog header: %w", io.ErrShortWrite)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("accountsdb: sync extent catalog: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("accountsdb: close extent catalog: %w", err)
	}
	closed = true
	return nil
}

func encodeExtentCatalogRecord(encoded []byte, extent streamIndexExtent) {
	binary.LittleEndian.PutUint64(encoded[0:8], extent.Slot)
	binary.LittleEndian.PutUint64(encoded[8:16], extent.FileID)
	binary.LittleEndian.PutUint64(encoded[16:24], extent.BaseOffset)
}
