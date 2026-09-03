package accountsdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/gagliardetto/solana-go"
)

// ShardedImmutableIndex is the runtime payload selected by one root catalog.
// Its shard objects are owned by generation resources; callers may use them
// only while holding the corresponding IndexReadView.
type ShardedImmutableIndex struct {
	root            string
	catalog         *RootIndexCatalog
	router          PersistentIndexShardRouter
	extents         *PersistentExtentCatalog
	catalogResource *IndexGenerationResource
	shards          []shardedImmutableIndexShard
	resources       []*IndexGenerationResource
}

type shardedImmutableIndexShard struct {
	base          *ShardedStreamBaseShard
	baseResource  *IndexGenerationResource
	delta         *DeltaCheckpoint
	deltaHandle   *ShardedDeltaCheckpointHandle
	deltaResource *IndexGenerationResource
}

// ShardedDeltaCheckpointArtifactSet binds all three files in one exact delta
// checkpoint publication. The root catalog selects Index and Records;
// Descriptor is retained here as a generation resource so its deletion is
// deferred by the same reader lifetime.
type ShardedDeltaCheckpointArtifactSet struct {
	Generation      uint64
	CoveredSequence uint64
	RecordCount     uint64
	Index           IndexCatalogArtifact
	Records         IndexCatalogArtifact
	Descriptor      IndexCatalogArtifact
}

func immutableBaseResourceName(shard IndexCatalogShard) string {
	return "base:" + shard.BaseIndex.RelativePath + ":" + shard.BaseRecords.RelativePath
}

func immutableDeltaResourceName(shard IndexCatalogShard) string {
	return "delta:" + shard.DeltaIndex.RelativePath + ":" + shard.DeltaRecords.RelativePath
}

func immutableExtentResourceName(artifact IndexCatalogArtifact) string {
	return "extents:" + artifact.RelativePath
}

// OpenShardedImmutableIndex opens and fully verifies every artifact selected
// by catalog. This is the startup path; rolling publication reuses unchanged
// resource objects and opens only the replacement shard.
func OpenShardedImmutableIndex(root string, catalog *RootIndexCatalog) (_ *ShardedImmutableIndex, retErr error) {
	if err := catalog.Validate(); err != nil {
		return nil, err
	}
	router, err := NewPersistentIndexShardRouter(int(catalog.ShardCount), catalog.RoutingKey)
	if err != nil {
		return nil, err
	}
	extents, err := OpenPersistentExtentCatalogArtifact(root, catalog.SharedExtentCatalog)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: open root extent catalog: %w", err)
	}
	requiredExtentCounts := make([]uint32, len(catalog.Shards))
	for shardID := range catalog.Shards {
		binding, err := readShardedBaseScanBindingForStartup(
			root,
			catalog.Shards[shardID].BaseRecords,
			uint32(shardID),
			extents.Generation(),
			router,
		)
		if err != nil {
			return nil, fmt.Errorf("accountsdb: read base shard %d extent binding: %w", shardID, err)
		}
		requiredExtentCounts[shardID] = binding.RequiredCount
	}
	if _, err := extents.precomputePrefixHashes(requiredExtentCounts); err != nil {
		return nil, fmt.Errorf("accountsdb: precompute root extent-prefix identities: %w", err)
	}

	opened := &ShardedImmutableIndex{
		root:    root,
		catalog: catalog.Clone(),
		router:  router,
		extents: extents,
		shards:  make([]shardedImmutableIndexShard, len(catalog.Shards)),
	}
	cleanup := true
	defer func() {
		if cleanup {
			retErr = errors.Join(retErr, opened.closeUnmanaged())
		}
	}()

	opened.catalogResource, err = newImmutableArtifactResource(
		root,
		immutableExtentResourceName(catalog.SharedExtentCatalog),
		extents,
		nil,
		[]IndexCatalogArtifact{catalog.SharedExtentCatalog},
	)
	if err != nil {
		return nil, err
	}
	opened.resources = append(opened.resources, opened.catalogResource)

	for shardID := range catalog.Shards {
		selected := catalog.Shards[shardID]
		base, err := OpenShardedStreamBaseShard(
			root, selected.BaseIndex, selected.BaseRecords, extents, router,
		)
		if err != nil {
			return nil, fmt.Errorf("accountsdb: open base shard %d: %w", shardID, err)
		}
		if base.ShardID() != uint32(shardID) || base.Generation() != selected.BaseGeneration {
			_ = base.Close()
			return nil, fmt.Errorf(
				"%w: base shard %d identity is shard=%d generation=%d, want generation=%d",
				ErrInvalidRootIndexCatalog,
				shardID,
				base.ShardID(),
				base.Generation(),
				selected.BaseGeneration,
			)
		}
		baseResource, err := newImmutableArtifactResource(
			root,
			immutableBaseResourceName(selected),
			base,
			base.Close,
			[]IndexCatalogArtifact{selected.BaseIndex, selected.BaseRecords},
		)
		if err != nil {
			_ = base.Close()
			return nil, err
		}
		opened.shards[shardID].base = base
		opened.shards[shardID].baseResource = baseResource
		opened.resources = append(opened.resources, baseResource)

		if selected.DeltaGeneration == 0 {
			continue
		}
		delta, descriptorArtifact, err := openRootSelectedDeltaCheckpoint(root, selected)
		if err != nil {
			return nil, fmt.Errorf("accountsdb: open delta shard %d: %w", shardID, err)
		}
		if err := validateDeltaCheckpointShardRouting(delta, router, uint32(shardID)); err != nil {
			_ = delta.Close()
			return nil, fmt.Errorf("accountsdb: validate delta shard %d routing: %w", shardID, err)
		}
		deltaHandle, err := NewShardedDeltaCheckpointHandle(delta)
		if err != nil {
			_ = delta.Close()
			return nil, fmt.Errorf("accountsdb: own delta shard %d: %w", shardID, err)
		}
		deltaResource, err := newImmutableArtifactResource(
			root,
			immutableDeltaResourceName(selected),
			deltaHandle,
			func() error { return releaseShardedCheckpointHandleAndWait(deltaHandle) },
			[]IndexCatalogArtifact{selected.DeltaIndex, selected.DeltaRecords, descriptorArtifact},
		)
		if err != nil {
			_ = releaseShardedCheckpointHandleAndWait(deltaHandle)
			return nil, err
		}
		opened.shards[shardID].delta = delta
		opened.shards[shardID].deltaHandle = deltaHandle
		opened.shards[shardID].deltaResource = deltaResource
		opened.resources = append(opened.resources, deltaResource)
	}
	opened.resources, err = immutableGenerationResources(opened)
	if err != nil {
		return nil, err
	}

	cleanup = false
	return opened, nil
}

// validateDeltaCheckpointShardRouting semantically binds an older generic
// delta-checkpoint artifact to the keyed V2 shard selected by the root. Root
// startup already streams every selected artifact for its SHA-256 identity;
// this mmap-backed pass additionally proves strict order, every record CRC and
// the persisted router assignment before any point lookup can observe it.
func validateDeltaCheckpointShardRouting(
	checkpoint *DeltaCheckpoint,
	router PersistentIndexShardRouter,
	shardID uint32,
) error {
	var previous solana.PublicKey
	havePrevious := false
	return checkpoint.ForEachSorted(func(key solana.PublicKey, _ deltaIndexValue) error {
		if havePrevious && bytes.Compare(previous[:], key[:]) >= 0 {
			return fmt.Errorf("delta records are not strictly sorted at key %x", key)
		}
		if routed := router.Shard(key); routed != shardID {
			return fmt.Errorf("delta key %x routes to shard %d, selected by shard %d", key, routed, shardID)
		}
		previous = key
		havePrevious = true
		return nil
	})
}

func openRootSelectedDeltaCheckpoint(
	root string,
	selected IndexCatalogShard,
) (*DeltaCheckpoint, IndexCatalogArtifact, error) {
	if err := validateIndexCatalogArtifactPathComponents(root, selected.DeltaIndex.RelativePath); err != nil {
		return nil, IndexCatalogArtifact{}, fmt.Errorf("%w: inspect delta index path: %v", ErrInvalidRootIndexCatalog, err)
	}
	if err := validateIndexCatalogArtifactPathComponents(root, selected.DeltaRecords.RelativePath); err != nil {
		return nil, IndexCatalogArtifact{}, fmt.Errorf("%w: inspect delta records path: %v", ErrInvalidRootIndexCatalog, err)
	}
	indexPath, err := ResolveIndexCatalogArtifactPath(root, selected.DeltaIndex)
	if err != nil {
		return nil, IndexCatalogArtifact{}, err
	}
	recordPath, err := ResolveIndexCatalogArtifactPath(root, selected.DeltaRecords)
	if err != nil {
		return nil, IndexCatalogArtifact{}, err
	}
	if filepath.Dir(indexPath) != filepath.Dir(recordPath) {
		return nil, IndexCatalogArtifact{}, fmt.Errorf("%w: delta artifacts are in different directories", ErrInvalidRootIndexCatalog)
	}
	paths := makeDeltaCheckpointPaths(filepath.Dir(indexPath), selected.DeltaGeneration)
	if filepath.Clean(paths.index) != filepath.Clean(indexPath) || filepath.Clean(paths.records) != filepath.Clean(recordPath) {
		return nil, IndexCatalogArtifact{}, fmt.Errorf(
			"%w: delta artifact names do not match generation %d",
			ErrInvalidRootIndexCatalog,
			selected.DeltaGeneration,
		)
	}
	checkpoint, err := openDeltaCheckpointGenerationWithCatalogArtifacts(
		filepath.Dir(indexPath),
		selected.DeltaGeneration,
		&selected.DeltaIndex,
		&selected.DeltaRecords,
	)
	if err != nil {
		return nil, IndexCatalogArtifact{}, err
	}
	if checkpoint.CoveredSeq() > selected.DeltaCoveredSequence {
		_ = checkpoint.Close()
		return nil, IndexCatalogArtifact{}, fmt.Errorf(
			"%w: delta generation %d physically covers sequence %d beyond catalog logical coverage %d",
			ErrInvalidRootIndexCatalog,
			selected.DeltaGeneration,
			checkpoint.CoveredSeq(),
			selected.DeltaCoveredSequence,
		)
	}
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		_ = checkpoint.Close()
		return nil, IndexCatalogArtifact{}, fmt.Errorf("accountsdb: resolve AccountsDB root: %w", err)
	}
	descriptorRelative, err := filepath.Rel(rootAbsolute, paths.descriptor)
	if err != nil {
		_ = checkpoint.Close()
		return nil, IndexCatalogArtifact{}, fmt.Errorf("accountsdb: resolve delta descriptor path: %w", err)
	}
	descriptorArtifact, err := ComputeIndexCatalogArtifact(root, filepath.ToSlash(descriptorRelative))
	if err != nil {
		_ = checkpoint.Close()
		return nil, IndexCatalogArtifact{}, err
	}
	return checkpoint, descriptorArtifact, nil
}

// IdentifyShardedDeltaCheckpointArtifacts measures the exact immutable files
// behind a retained checkpoint handle. It also verifies that the descriptor
// binds those identities and that the handle was opened from directory.
func IdentifyShardedDeltaCheckpointArtifacts(
	root string,
	directory string,
	handle *ShardedDeltaCheckpointHandle,
) (ShardedDeltaCheckpointArtifactSet, error) {
	var result ShardedDeltaCheckpointArtifactSet
	if handle == nil {
		return result, errors.New("accountsdb: nil sharded delta checkpoint handle")
	}
	if err := handle.Retain(); err != nil {
		return result, err
	}
	defer func() { _ = handle.Release() }()

	checkpoint := handle.Checkpoint()
	if checkpoint == nil {
		return result, errors.New("accountsdb: released sharded delta checkpoint handle")
	}
	checkpoint.mu.RLock()
	if checkpoint.closed || checkpoint.index == nil || checkpoint.records == nil || checkpoint.recordPath == "" {
		checkpoint.mu.RUnlock()
		return result, errors.New("accountsdb: closed delta checkpoint")
	}
	generation := checkpoint.generation
	coveredSequence := checkpoint.coveredSeq
	recordCount := checkpoint.recordCount
	openedRecordPath := checkpoint.recordPath
	checkpoint.mu.RUnlock()

	paths := makeDeltaCheckpointPaths(directory, generation)
	openedRecordAbsolute, err := filepath.Abs(openedRecordPath)
	if err != nil {
		return result, fmt.Errorf("accountsdb: resolve opened delta record path: %w", err)
	}
	expectedRecordAbsolute, err := filepath.Abs(paths.records)
	if err != nil {
		return result, fmt.Errorf("accountsdb: resolve expected delta record path: %w", err)
	}
	if filepath.Clean(openedRecordAbsolute) != filepath.Clean(expectedRecordAbsolute) {
		return result, fmt.Errorf(
			"%w: checkpoint record path %q does not belong to directory %q",
			ErrInvalidRootIndexCatalog,
			openedRecordPath,
			directory,
		)
	}

	indexArtifact, err := computeImmutableArtifactAtPath(root, paths.index)
	if err != nil {
		return result, fmt.Errorf("accountsdb: identify delta checkpoint index: %w", err)
	}
	recordArtifact, err := computeImmutableArtifactAtPath(root, paths.records)
	if err != nil {
		return result, fmt.Errorf("accountsdb: identify delta checkpoint records: %w", err)
	}
	descriptorArtifact, err := computeImmutableArtifactAtPath(root, paths.descriptor)
	if err != nil {
		return result, fmt.Errorf("accountsdb: identify delta checkpoint descriptor: %w", err)
	}
	descriptorBytes, err := readDeltaCheckpointDescriptor(paths.descriptor)
	if err != nil {
		return result, fmt.Errorf("accountsdb: read delta checkpoint descriptor: %w", err)
	}
	descriptor, err := decodeDeltaCheckpointDescriptor(descriptorBytes)
	if err != nil {
		return result, fmt.Errorf("accountsdb: decode delta checkpoint descriptor: %w", err)
	}
	if descriptor.Generation != generation ||
		descriptor.CoveredSeq != coveredSequence ||
		descriptor.RecordCount != recordCount ||
		descriptor.Index.Size != indexArtifact.Size ||
		descriptor.Index.SHA256 != indexArtifact.SHA256 ||
		descriptor.Records.Size != recordArtifact.Size ||
		descriptor.Records.SHA256 != recordArtifact.SHA256 {
		return result, fmt.Errorf("%w: checkpoint descriptor, files and retained handle disagree", ErrInvalidRootIndexCatalog)
	}
	return ShardedDeltaCheckpointArtifactSet{
		Generation:      generation,
		CoveredSequence: coveredSequence,
		RecordCount:     recordCount,
		Index:           indexArtifact,
		Records:         recordArtifact,
		Descriptor:      descriptorArtifact,
	}, nil
}

func computeImmutableArtifactAtPath(root string, absolutePath string) (IndexCatalogArtifact, error) {
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return IndexCatalogArtifact{}, fmt.Errorf("accountsdb: resolve AccountsDB root: %w", err)
	}
	artifactAbsolute, err := filepath.Abs(absolutePath)
	if err != nil {
		return IndexCatalogArtifact{}, fmt.Errorf("accountsdb: resolve immutable artifact: %w", err)
	}
	relative, err := filepath.Rel(rootAbsolute, artifactAbsolute)
	if err != nil {
		return IndexCatalogArtifact{}, fmt.Errorf("accountsdb: relativize immutable artifact: %w", err)
	}
	return ComputeIndexCatalogArtifact(rootAbsolute, filepath.ToSlash(relative))
}

func newImmutableArtifactResource(
	root string,
	name string,
	value any,
	closeFn func() error,
	artifacts []IndexCatalogArtifact,
) (*IndexGenerationResource, error) {
	deleteFn := func() error { return removeImmutableArtifacts(root, artifacts) }
	return NewIndexGenerationResource(name, value, closeFn, deleteFn)
}

// releaseShardedCheckpointHandleAndWait is used as a generation resource's
// close callback. Waiting is intentional: IndexGenerationResource invokes the
// callback on its asynchronous finalizer, and must not unlink the checkpoint
// artifacts until every independent handle owner has released the mmap.
func releaseShardedCheckpointHandleAndWait(handle *ShardedDeltaCheckpointHandle) error {
	if handle == nil {
		return nil
	}
	if err := handle.Release(); err != nil {
		return err
	}
	<-handle.Done()
	return handle.Err()
}

// removeImmutableArtifacts removes only fully resolved catalog-selected files.
// It never recursively removes a directory. Empty generation directories are
// opportunistically removed after their final artifact disappears.
func removeImmutableArtifacts(root string, artifacts []IndexCatalogArtifact) error {
	paths := make([]string, 0, len(artifacts))
	directories := make(map[string]struct{})
	seen := make(map[string]struct{}, len(artifacts))
	for _, artifact := range artifacts {
		resolved, err := ResolveIndexCatalogArtifactPath(root, artifact)
		if err != nil {
			return err
		}
		if _, duplicate := seen[resolved]; duplicate {
			return fmt.Errorf("%w: duplicate immutable cleanup path %q", ErrInvalidCatalogArtifact, artifact.RelativePath)
		}
		seen[resolved] = struct{}{}
		info, err := os.Lstat(resolved)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("accountsdb: inspect immutable cleanup artifact %q: %w", artifact.RelativePath, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: refusing to remove non-regular artifact %q", ErrInvalidCatalogArtifact, artifact.RelativePath)
		}
		// Preflight every surviving file before unlinking any of them. Immutable
		// names are unique and never reused, so identity agreement makes this an
		// exact deletion rather than a path-only deletion.
		if err := VerifyIndexCatalogArtifact(root, artifact); err != nil {
			return fmt.Errorf("accountsdb: verify immutable cleanup artifact %q: %w", artifact.RelativePath, err)
		}
		paths = append(paths, resolved)
		directories[filepath.Dir(resolved)] = struct{}{}
	}
	var result error
	for _, resolved := range paths {
		if err := os.Remove(resolved); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	for directory := range directories {
		if err := fsyncDir(directory); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
		if filepath.Clean(directory) != filepath.Clean(root) {
			if err := os.Remove(directory); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, os.ErrExist) {
				// ENOTEMPTY is expected while another selected artifact shares the
				// generation directory. Avoid platform-specific syscall matching.
				if entries, readErr := os.ReadDir(directory); readErr == nil && len(entries) == 0 {
					result = errors.Join(result, err)
				}
			}
		}
	}
	return result
}

// closeUnmanaged is used only while startup construction is still private.
// Once installed in IndexViewManager, resource reference counts own closure.
func (index *ShardedImmutableIndex) closeUnmanaged() error {
	if index == nil {
		return nil
	}
	var result error
	for i := range index.shards {
		if index.shards[i].deltaHandle != nil {
			result = errors.Join(result, releaseShardedCheckpointHandleAndWait(index.shards[i].deltaHandle))
			index.shards[i].deltaHandle = nil
			index.shards[i].delta = nil
		} else if index.shards[i].delta != nil {
			// Kept for defensive compatibility with an incompletely constructed
			// pre-handle startup shard.
			result = errors.Join(result, index.shards[i].delta.Close())
			index.shards[i].delta = nil
		}
		if index.shards[i].base != nil {
			result = errors.Join(result, index.shards[i].base.Close())
			index.shards[i].base = nil
		}
	}
	return result
}

// validatePreparedSnapshotArtifactPathsStable performs the cheap half of the
// snapshot verifier-to-runtime handoff. Every artifact was already fully
// hashed and semantically verified by OpenShardedImmutableIndex; while the
// store lock remains continuously held, this confirms that no selected path
// changed before the same opened generation is adopted by AccountsDB.
func (index *ShardedImmutableIndex) validatePreparedSnapshotArtifactPathsStable() error {
	if index == nil || index.catalog == nil || index.extents == nil {
		return errors.New("accountsdb: incomplete prepared immutable account index")
	}
	if index.extents.verifiedPath == "" || index.extents.verifiedInfo == nil {
		return errors.New("accountsdb: prepared extent catalog has no verified path identity")
	}
	if err := validateRegularFilePathIdentity(
		index.extents.verifiedPath,
		index.extents.verifiedInfo,
	); err != nil {
		return fmt.Errorf("accountsdb: retained extent-catalog identity: %w", err)
	}
	for shardID := range index.shards {
		shard := &index.shards[shardID]
		// Snapshot preparation accepts only a fresh generation, which cannot
		// select a delta checkpoint. Keep this check here so a future caller
		// cannot accidentally use the lightweight handoff for mutable state.
		if shard.delta != nil || shard.deltaHandle != nil || shard.deltaResource != nil {
			return fmt.Errorf(
				"accountsdb: prepared snapshot shard %d unexpectedly selects a delta checkpoint",
				shardID,
			)
		}
		if err := shard.base.validateRetainedArtifactPathsStable(); err != nil {
			return fmt.Errorf("accountsdb: retained base shard %d: %w", shardID, err)
		}
	}
	return nil
}

func (index *ShardedImmutableIndex) Resources() []*IndexGenerationResource {
	if index == nil {
		return nil
	}
	return append([]*IndexGenerationResource(nil), index.resources...)
}

func (index *ShardedImmutableIndex) Router() PersistentIndexShardRouter {
	if index == nil {
		return PersistentIndexShardRouter{}
	}
	return index.router
}

func (index *ShardedImmutableIndex) ExtentCatalog() *PersistentExtentCatalog {
	if index == nil {
		return nil
	}
	return index.extents
}

// RootCatalog returns a deep copy suitable for constructing the immediate
// successor catalog. Mutating the returned value cannot change this pinned
// generation.
func (index *ShardedImmutableIndex) RootCatalog() *RootIndexCatalog {
	if index == nil || index.catalog == nil {
		return nil
	}
	return index.catalog.Clone()
}

func (index *ShardedImmutableIndex) BaseShard(shardID uint32) *ShardedStreamBaseShard {
	if index == nil || shardID >= uint32(len(index.shards)) {
		return nil
	}
	return index.shards[shardID].base
}

func (index *ShardedImmutableIndex) DeltaShard(shardID uint32) *DeltaCheckpoint {
	if index == nil || shardID >= uint32(len(index.shards)) {
		return nil
	}
	return index.shards[shardID].delta
}

// InitialCheckpointHandles returns one independently owned handle per shard
// for initializing ShardedMutableAccountIndex. Nil entries mean the root has
// no delta for that shard. The caller must Release every non-nil entry.
//
// This method must be called while the immutable payload's generation is
// pinned. On any retain failure it releases everything retained so far.
func (index *ShardedImmutableIndex) InitialCheckpointHandles() ([]*ShardedDeltaCheckpointHandle, error) {
	if index == nil || index.catalog == nil {
		return nil, errors.New("accountsdb: nil sharded immutable index")
	}
	handles := make([]*ShardedDeltaCheckpointHandle, len(index.shards))
	releaseRetained := func() {
		for _, retained := range handles {
			_ = retained.Release()
		}
	}
	for shardID := range index.shards {
		shard := &index.shards[shardID]
		if shard.delta == nil {
			if shard.deltaHandle != nil || shard.deltaResource != nil {
				releaseRetained()
				return nil, fmt.Errorf("%w: shard %d has incomplete delta ownership", ErrInvalidRootIndexCatalog, shardID)
			}
			continue
		}
		if shard.deltaHandle == nil || shard.deltaResource == nil || shard.deltaHandle.Checkpoint() != shard.delta {
			releaseRetained()
			return nil, fmt.Errorf("%w: shard %d delta has no matching shared handle", ErrInvalidRootIndexCatalog, shardID)
		}
		if err := shard.deltaHandle.Retain(); err != nil {
			releaseRetained()
			return nil, fmt.Errorf("accountsdb: retain initial delta shard %d: %w", shardID, err)
		}
		handles[shardID] = shard.deltaHandle
	}
	return handles, nil
}

// DeriveWithCheckpoint constructs the immutable payload for a checkpoint
// publication without reopening any existing mmap. Unchanged resources are
// shared with the current generation; the returned obsolete resource (when
// present) is safe to pass directly to IndexViewManager.Publish.
//
// publication.Next remains owned by its caller. This method retains a separate
// owner whose Release is tied to the replacement generation resource.
func (index *ShardedImmutableIndex) DeriveWithCheckpoint(
	rootNext *RootIndexCatalog,
	publication ShardedMutableCheckpointPublication,
) (
	next *ShardedImmutableIndex,
	resources []*IndexGenerationResource,
	obsolete []*IndexGenerationResource,
	err error,
) {
	if err := index.validateSuccessorRoot(rootNext); err != nil {
		return nil, nil, nil, err
	}
	if publication.ShardID >= uint32(len(index.shards)) {
		return nil, nil, nil, fmt.Errorf("%w: checkpoint shard %d is out of range", ErrInvalidRootIndexCatalog, publication.ShardID)
	}
	if len(publication.ProposedCoveredSequences) != len(index.shards) {
		return nil, nil, nil, fmt.Errorf(
			"%w: checkpoint proposed %d shard coverages, want %d",
			ErrInvalidRootIndexCatalog,
			len(publication.ProposedCoveredSequences),
			len(index.shards),
		)
	}
	currentShard := &index.shards[publication.ShardID]
	if currentShard.deltaHandle != publication.Previous {
		return nil, nil, nil, fmt.Errorf("%w: checkpoint publication is based on a stale previous delta", ErrInvalidRootIndexCatalog)
	}
	if publication.Next == nil || publication.Next == publication.Previous {
		return nil, nil, nil, fmt.Errorf("%w: checkpoint publication has no distinct replacement handle", ErrInvalidRootIndexCatalog)
	}
	expectedDirectory := ShardedMutableCheckpointDirectory(index.root, publication.ShardID)
	if !sameImmutablePath(expectedDirectory, publication.Directory) {
		return nil, nil, nil, fmt.Errorf(
			"%w: checkpoint directory %q, want %q",
			ErrInvalidRootIndexCatalog,
			publication.Directory,
			expectedDirectory,
		)
	}
	artifacts, err := IdentifyShardedDeltaCheckpointArtifacts(index.root, publication.Directory, publication.Next)
	if err != nil {
		return nil, nil, nil, err
	}
	if artifacts.CoveredSequence != publication.CoveredSequence {
		return nil, nil, nil, fmt.Errorf(
			"%w: checkpoint physically covers %d, publication says %d",
			ErrInvalidRootIndexCatalog,
			artifacts.CoveredSequence,
			publication.CoveredSequence,
		)
	}
	if rootNext.SharedExtentCatalog != index.catalog.SharedExtentCatalog {
		return nil, nil, nil, fmt.Errorf("%w: checkpoint publication replaced the shared extent catalog", ErrInvalidRootIndexCatalog)
	}

	for shardID := range index.catalog.Shards {
		currentSelected := index.catalog.Shards[shardID]
		nextSelected := rootNext.Shards[shardID]
		if !sameImmutableBaseSelection(currentSelected, nextSelected) {
			return nil, nil, nil, fmt.Errorf("%w: checkpoint publication changed base shard %d", ErrInvalidRootIndexCatalog, shardID)
		}
		if got := nextSelected.effectiveCoveredSequence(); got != publication.ProposedCoveredSequences[shardID] {
			return nil, nil, nil, fmt.Errorf(
				"%w: shard %d logical coverage %d, publication proposes %d",
				ErrInvalidRootIndexCatalog,
				shardID,
				got,
				publication.ProposedCoveredSequences[shardID],
			)
		}
		currentCoverage := currentSelected.effectiveCoveredSequence()
		if proposed := publication.ProposedCoveredSequences[shardID]; proposed < currentCoverage ||
			proposed > max(currentCoverage, publication.CoveredSequence) {
			return nil, nil, nil, fmt.Errorf(
				"%w: shard %d proposed coverage %d is outside [%d,%d]",
				ErrInvalidRootIndexCatalog,
				shardID,
				proposed,
				currentCoverage,
				max(currentCoverage, publication.CoveredSequence),
			)
		}
		if uint32(shardID) == publication.ShardID {
			if nextSelected.BaseCoveredSequence != currentSelected.BaseCoveredSequence ||
				nextSelected.DeltaGeneration != artifacts.Generation ||
				nextSelected.DeltaCoveredSequence != artifacts.CoveredSequence ||
				nextSelected.DeltaIndex != artifacts.Index ||
				nextSelected.DeltaRecords != artifacts.Records {
				return nil, nil, nil, fmt.Errorf("%w: replacement delta selection does not match exact checkpoint artifacts", ErrInvalidRootIndexCatalog)
			}
			continue
		}
		if !sameImmutableDeltaSelection(currentSelected, nextSelected) {
			return nil, nil, nil, fmt.Errorf("%w: checkpoint publication changed delta shard %d", ErrInvalidRootIndexCatalog, shardID)
		}
		if currentSelected.DeltaGeneration != 0 && nextSelected.BaseCoveredSequence != currentSelected.BaseCoveredSequence {
			return nil, nil, nil, fmt.Errorf("%w: checkpoint publication changed covered base beneath delta shard %d", ErrInvalidRootIndexCatalog, shardID)
		}
	}
	if currentSelected := index.catalog.Shards[publication.ShardID]; artifacts.Generation <= currentSelected.DeltaGeneration {
		return nil, nil, nil, fmt.Errorf("%w: replacement delta generation did not advance", ErrInvalidRootIndexCatalog)
	}

	if err := publication.Next.Retain(); err != nil {
		return nil, nil, nil, fmt.Errorf("accountsdb: retain replacement delta: %w", err)
	}
	retained := true
	defer func() {
		if retained {
			_ = publication.Next.Release()
		}
	}()
	checkpoint := publication.Next.Checkpoint()
	if checkpoint == nil {
		return nil, nil, nil, errors.New("accountsdb: replacement delta handle was released")
	}
	replacementResource, err := newImmutableArtifactResource(
		index.root,
		immutableDeltaResourceName(rootNext.Shards[publication.ShardID]),
		publication.Next,
		func() error { return releaseShardedCheckpointHandleAndWait(publication.Next) },
		[]IndexCatalogArtifact{artifacts.Index, artifacts.Records, artifacts.Descriptor},
	)
	if err != nil {
		return nil, nil, nil, err
	}
	if currentShard.deltaResource != nil && replacementResource.Name() == currentShard.deltaResource.Name() {
		return nil, nil, nil, fmt.Errorf("%w: replacement delta reused the current immutable artifact name", ErrInvalidRootIndexCatalog)
	}

	next = index.cloneForSuccessor(rootNext)
	next.shards[publication.ShardID].delta = checkpoint
	next.shards[publication.ShardID].deltaHandle = publication.Next
	next.shards[publication.ShardID].deltaResource = replacementResource
	next.resources, err = immutableGenerationResources(next)
	if err != nil {
		return nil, nil, nil, err
	}
	if currentShard.deltaResource != nil {
		obsolete = []*IndexGenerationResource{currentShard.deltaResource}
	}
	retained = false
	return next, next.Resources(), obsolete, nil
}

// DeriveWithRebase constructs a successor that installs one newly built base
// shard, clears that shard's exact delta, and shares every other open resource.
// A build that discovered new extents installs an append-only catalog
// successor; a build containing only known extents retains the existing
// catalog resource. The view manager defers exact deletion of replaced
// resources until pinned old generations drain.
func (index *ShardedImmutableIndex) DeriveWithRebase(
	rootNext *RootIndexCatalog,
	build *ShardedStreamBaseShardBuildResult,
) (
	next *ShardedImmutableIndex,
	resources []*IndexGenerationResource,
	obsolete []*IndexGenerationResource,
	err error,
) {
	if err := index.validateSuccessorRoot(rootNext); err != nil {
		return nil, nil, nil, err
	}
	if build == nil || build.ExtentCatalog == nil {
		return nil, nil, nil, fmt.Errorf("%w: nil rolling-rebase build", ErrInvalidRootIndexCatalog)
	}
	shardID := build.Shard.ShardID
	if shardID >= uint32(len(index.shards)) || build.ShardCount != index.catalog.ShardCount ||
		build.RoutingKey != index.catalog.RoutingKey {
		return nil, nil, nil, fmt.Errorf("%w: rolling-rebase shard/count mismatch", ErrInvalidRootIndexCatalog)
	}
	if build.Generation == 0 || build.Shard.Generation != build.Generation ||
		build.ExtentCatalogArtifact != build.ExtentCatalog.Artifact() {
		return nil, nil, nil, fmt.Errorf("%w: rolling-rebase build generations or extent identity disagree", ErrInvalidRootIndexCatalog)
	}
	if err := build.ExtentCatalog.validate(); err != nil {
		return nil, nil, nil, err
	}
	reusesExtentCatalog := build.ExtentCatalogArtifact == index.catalog.SharedExtentCatalog
	if reusesExtentCatalog == build.OwnsExtentCatalogArtifact {
		return nil, nil, nil, fmt.Errorf("%w: rolling-rebase extent artifact ownership is inconsistent", ErrInvalidRootIndexCatalog)
	}
	if reusesExtentCatalog {
		if build.ExtentCatalog != index.extents ||
			build.ExtentCatalog.Generation() != index.extents.Generation() {
			return nil, nil, nil, fmt.Errorf("%w: rolling-rebase did not reuse the selected catalog object exactly", ErrInvalidRootIndexCatalog)
		}
	} else if build.ExtentCatalog.Generation() != build.Generation ||
		build.ExtentCatalog.Generation() <= index.extents.Generation() {
		return nil, nil, nil, fmt.Errorf("%w: rolling-rebase extent generation did not advance", ErrInvalidRootIndexCatalog)
	}
	priorBinding, err := index.extents.binding()
	if err != nil {
		return nil, nil, nil, err
	}
	bindings := make([]extentCatalogBinding, 1, len(index.shards))
	bindings[0] = priorBinding
	for candidateID := range index.shards {
		if uint32(candidateID) == shardID {
			continue
		}
		bindings = append(bindings, index.shards[candidateID].base.metadata.CatalogBinding)
	}
	if _, err := build.ExtentCatalog.precomputeBindingPrefixes(bindings); err != nil {
		return nil, nil, nil, fmt.Errorf(
			"%w: precompute rolling-rebase extent prefixes: %v",
			ErrInvalidRootIndexCatalog,
			err,
		)
	}
	if err := build.ExtentCatalog.validateBinding(priorBinding); err != nil {
		return nil, nil, nil, fmt.Errorf("%w: rebuilt extent catalog is not append-compatible: %v", ErrInvalidRootIndexCatalog, err)
	}
	if rootNext.SharedExtentCatalog != build.ExtentCatalogArtifact {
		return nil, nil, nil, fmt.Errorf("%w: successor does not select rebuilt extent catalog", ErrInvalidRootIndexCatalog)
	}
	if !reusesExtentCatalog {
		if err := VerifyIndexCatalogArtifact(index.root, build.ExtentCatalogArtifact); err != nil {
			return nil, nil, nil, fmt.Errorf("accountsdb: verify rebuilt extent catalog: %w", err)
		}
	}

	for candidateID := range index.catalog.Shards {
		currentSelected := index.catalog.Shards[candidateID]
		nextSelected := rootNext.Shards[candidateID]
		if uint32(candidateID) == shardID {
			if nextSelected.BaseGeneration != build.Shard.Generation ||
				nextSelected.BaseGeneration <= currentSelected.BaseGeneration ||
				nextSelected.BaseIndex != build.Shard.Index ||
				nextSelected.BaseRecords != build.Shard.Scan ||
				nextSelected.DeltaGeneration != 0 ||
				nextSelected.DeltaCoveredSequence != 0 ||
				!nextSelected.DeltaIndex.isZero() ||
				!nextSelected.DeltaRecords.isZero() {
				return nil, nil, nil, fmt.Errorf("%w: successor does not select rebuilt base and clear its delta", ErrInvalidRootIndexCatalog)
			}
			continue
		}
		if currentSelected != nextSelected {
			return nil, nil, nil, fmt.Errorf("%w: rolling rebase changed untouched shard %d", ErrInvalidRootIndexCatalog, candidateID)
		}
		if err := build.ExtentCatalog.validateBinding(index.shards[candidateID].base.metadata.CatalogBinding); err != nil {
			return nil, nil, nil, fmt.Errorf("%w: rebuilt extent catalog is incompatible with base shard %d: %v", ErrInvalidRootIndexCatalog, candidateID, err)
		}
	}

	// Retained base resources are shared with older pinned root generations.
	// Rebind each untouched shard to the validated append-compatible superset so
	// it does not keep the full catalog object from the generation in which it
	// was last rebuilt. Pinned old readers remain correct because ordinals in
	// the shard's authenticated prefix never change.
	for candidateID := range index.shards {
		if uint32(candidateID) == shardID {
			continue
		}
		if err := index.shards[candidateID].base.rebindExtentCatalog(build.ExtentCatalog); err != nil {
			return nil, nil, nil, fmt.Errorf(
				"%w: rebind retained base shard %d: %v",
				ErrInvalidRootIndexCatalog,
				candidateID,
				err,
			)
		}
	}

	replacementBase, err := OpenShardedStreamBaseShardArtifacts(
		index.root,
		build.Shard,
		build.ExtentCatalog,
		index.router,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("accountsdb: open rolling-rebase shard %d: %w", shardID, err)
	}
	keepBase := false
	defer func() {
		if !keepBase {
			err = errors.Join(err, replacementBase.Close())
		}
	}()
	replacementCatalogResource := index.catalogResource
	if !reusesExtentCatalog {
		replacementCatalogResource, err = newImmutableArtifactResource(
			index.root,
			immutableExtentResourceName(build.ExtentCatalogArtifact),
			build.ExtentCatalog,
			nil,
			[]IndexCatalogArtifact{build.ExtentCatalogArtifact},
		)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	replacementBaseResource, err := newImmutableArtifactResource(
		index.root,
		immutableBaseResourceName(rootNext.Shards[shardID]),
		replacementBase,
		replacementBase.Close,
		[]IndexCatalogArtifact{build.Shard.Index, build.Shard.Scan},
	)
	if err != nil {
		return nil, nil, nil, err
	}
	currentShard := &index.shards[shardID]
	if (!reusesExtentCatalog && replacementCatalogResource.Name() == index.catalogResource.Name()) ||
		replacementBaseResource.Name() == currentShard.baseResource.Name() {
		return nil, nil, nil, fmt.Errorf("%w: rolling rebase reused an obsolete resource name", ErrInvalidRootIndexCatalog)
	}

	next = index.cloneForSuccessor(rootNext)
	next.extents = build.ExtentCatalog
	next.catalogResource = replacementCatalogResource
	next.shards[shardID] = shardedImmutableIndexShard{
		base:         replacementBase,
		baseResource: replacementBaseResource,
	}
	next.resources, err = immutableGenerationResources(next)
	if err != nil {
		return nil, nil, nil, err
	}
	obsolete = []*IndexGenerationResource{currentShard.baseResource}
	if !reusesExtentCatalog {
		obsolete = append([]*IndexGenerationResource{index.catalogResource}, obsolete...)
	}
	if currentShard.deltaResource != nil {
		obsolete = append(obsolete, currentShard.deltaResource)
	}
	keepBase = true
	return next, next.Resources(), obsolete, nil
}

func (index *ShardedImmutableIndex) validateSuccessorRoot(rootNext *RootIndexCatalog) error {
	if index == nil || index.catalog == nil || rootNext == nil {
		return fmt.Errorf("%w: nil immutable root transition", ErrInvalidRootIndexCatalog)
	}
	if err := rootNext.Validate(); err != nil {
		return err
	}
	if index.catalog.Generation == ^uint64(0) || rootNext.Generation != index.catalog.Generation+1 {
		return fmt.Errorf(
			"%w: root generation %d is not the immediate successor of %d",
			ErrInvalidRootIndexCatalog,
			rootNext.Generation,
			index.catalog.Generation,
		)
	}
	if rootNext.ShardCount != index.catalog.ShardCount || len(rootNext.Shards) != len(index.shards) ||
		rootNext.RoutingKey != index.catalog.RoutingKey {
		return fmt.Errorf("%w: immutable shard routing identity changed", ErrInvalidRootIndexCatalog)
	}
	if rootNext.Lineage != index.catalog.Lineage ||
		rootNext.RootedBatchSequence != index.catalog.RootedBatchSequence ||
		rootNext.RootedSlot != index.catalog.RootedSlot {
		return fmt.Errorf("%w: index-only publication changed rooted lineage or bank position", ErrInvalidRootIndexCatalog)
	}
	if rootNext.CoveredSequence < index.catalog.CoveredSequence {
		return fmt.Errorf("%w: root logical coverage moved backwards", ErrInvalidRootIndexCatalog)
	}
	for shardID := range index.catalog.Shards {
		if rootNext.Shards[shardID].effectiveCoveredSequence() < index.catalog.Shards[shardID].effectiveCoveredSequence() {
			return fmt.Errorf("%w: shard %d logical coverage moved backwards", ErrInvalidRootIndexCatalog, shardID)
		}
	}
	return nil
}

func (index *ShardedImmutableIndex) cloneForSuccessor(rootNext *RootIndexCatalog) *ShardedImmutableIndex {
	return &ShardedImmutableIndex{
		root:            index.root,
		catalog:         rootNext.Clone(),
		router:          index.router,
		extents:         index.extents,
		catalogResource: index.catalogResource,
		shards:          append([]shardedImmutableIndexShard(nil), index.shards...),
	}
}

func sameImmutableBaseSelection(left, right IndexCatalogShard) bool {
	return left.BaseGeneration == right.BaseGeneration &&
		left.BaseIndex == right.BaseIndex &&
		left.BaseRecords == right.BaseRecords
}

func sameImmutableDeltaSelection(left, right IndexCatalogShard) bool {
	return left.DeltaGeneration == right.DeltaGeneration &&
		left.DeltaIndex == right.DeltaIndex &&
		left.DeltaRecords == right.DeltaRecords
}

func sameImmutablePath(left, right string) bool {
	leftAbsolute, leftErr := filepath.Abs(left)
	rightAbsolute, rightErr := filepath.Abs(right)
	return leftErr == nil && rightErr == nil && filepath.Clean(leftAbsolute) == filepath.Clean(rightAbsolute)
}

func immutableGenerationResources(index *ShardedImmutableIndex) ([]*IndexGenerationResource, error) {
	if index == nil || index.catalogResource == nil || len(index.shards) == 0 {
		return nil, fmt.Errorf("%w: incomplete immutable generation", ErrInvalidRootIndexCatalog)
	}
	resources := make([]*IndexGenerationResource, 0, 1+len(index.shards)*2)
	resources = append(resources, index.catalogResource)
	seenPointers := map[*IndexGenerationResource]struct{}{index.catalogResource: {}}
	seenNames := map[string]struct{}{index.catalogResource.Name(): {}}
	for shardID := range index.shards {
		shard := &index.shards[shardID]
		if shard.base == nil || shard.baseResource == nil {
			return nil, fmt.Errorf("%w: shard %d has no base resource", ErrInvalidRootIndexCatalog, shardID)
		}
		for _, resource := range []*IndexGenerationResource{shard.baseResource, shard.deltaResource} {
			if resource == nil {
				continue
			}
			if _, duplicate := seenPointers[resource]; duplicate {
				return nil, fmt.Errorf("%w: duplicate immutable resource pointer %q", ErrInvalidRootIndexCatalog, resource.Name())
			}
			if _, duplicate := seenNames[resource.Name()]; duplicate {
				return nil, fmt.Errorf("%w: duplicate immutable resource name %q", ErrInvalidRootIndexCatalog, resource.Name())
			}
			seenPointers[resource] = struct{}{}
			seenNames[resource.Name()] = struct{}{}
			resources = append(resources, resource)
		}
		if (shard.delta == nil) != (shard.deltaHandle == nil) ||
			(shard.delta == nil) != (shard.deltaResource == nil) {
			return nil, fmt.Errorf("%w: shard %d has incomplete delta resource", ErrInvalidRootIndexCatalog, shardID)
		}
		if shard.deltaHandle != nil && shard.deltaHandle.Checkpoint() != shard.delta {
			return nil, fmt.Errorf("%w: shard %d delta handle owns a different checkpoint", ErrInvalidRootIndexCatalog, shardID)
		}
	}
	return resources, nil
}

// LookupBaseCandidate probes only the cold base. The production lookup path
// uses this after its ShardedMutableAccountIndex snapshot has already checked
// the exact checkpoint, avoiding a duplicate probe of the same delta artifact
// (the root copy exists for crash recovery, generation ownership and tooling).
func (index *ShardedImmutableIndex) LookupBaseCandidate(
	key solana.PublicKey,
) (AccountIndexEntry, bool, error) {
	if index == nil {
		return AccountIndexEntry{}, false, nil
	}
	shardID := index.router.Shard(key)
	return index.shards[shardID].base.lookupCandidatePinned(key)
}

// LookupCandidate resolves the exact immutable delta before the probabilistic
// base. Exact tombstones suppress the base and are returned as found=true.
func (index *ShardedImmutableIndex) LookupCandidate(
	key solana.PublicKey,
) (deltaIndexValue, accountIndexSource, bool, error) {
	if index == nil {
		return deltaIndexValue{}, accountIndexSourceNone, false, nil
	}
	shardID := index.router.Shard(key)
	shard := &index.shards[shardID]
	if shard.delta != nil {
		value, found, err := shard.delta.lookupPinned(key)
		if err != nil {
			return deltaIndexValue{}, accountIndexSourceNone, false, err
		}
		if found {
			return value, accountIndexSourceDelta, true, nil
		}
	}
	entry, found, err := shard.base.lookupCandidatePinned(key)
	if err != nil || !found {
		return deltaIndexValue{}, accountIndexSourceNone, false, err
	}
	return deltaIndexValue{Entry: entry}, accountIndexSourceBase, true, nil
}

func (index *ShardedImmutableIndex) lookupBatchCandidates(
	ctx context.Context,
	count int,
	keyAt func(int) solana.PublicKey,
	skip func(int) bool,
	accept func(int, deltaIndexValue, accountIndexSource, bool),
) error {
	if ctx == nil {
		return errors.New("accountsdb: nil sharded immutable batch context")
	}
	if index == nil || count == 0 {
		return ctx.Err()
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
			value, source, found, err := index.LookupCandidate(keyAt(job))
			if err != nil {
				return fmt.Errorf("accountsdb: immutable shard lookup %d: %w", job, err)
			}
			accept(job, value, source, found)
		}
		return nil
	})
}

func (index *ShardedImmutableIndex) lookupBatchBaseCandidates(
	ctx context.Context,
	count int,
	keyAt func(int) solana.PublicKey,
	skip func(int) bool,
	accept func(int, AccountIndexEntry, bool),
) error {
	if ctx == nil {
		return errors.New("accountsdb: nil sharded base batch context")
	}
	if index == nil || count == 0 {
		return ctx.Err()
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
			entry, found, err := index.LookupBaseCandidate(keyAt(job))
			if err != nil {
				return fmt.Errorf("accountsdb: immutable base lookup %d: %w", job, err)
			}
			accept(job, entry, found)
		}
		return nil
	})
}

// referencedArtifactPaths returns a sorted set used by startup orphan GC.
func (index *ShardedImmutableIndex) referencedArtifactPaths() []string {
	if index == nil || index.catalog == nil {
		return nil
	}
	paths := []string{index.catalog.SharedExtentCatalog.RelativePath}
	for _, shard := range index.catalog.Shards {
		paths = append(paths, shard.BaseIndex.RelativePath, shard.BaseRecords.RelativePath)
		if shard.DeltaGeneration != 0 {
			paths = append(paths, shard.DeltaIndex.RelativePath, shard.DeltaRecords.RelativePath)
			paths = append(paths, filepath.ToSlash(filepath.Join(
				filepath.Dir(shard.DeltaIndex.RelativePath),
				filepath.Base(makeDeltaCheckpointPaths("", shard.DeltaGeneration).descriptor),
			)))
		}
	}
	sort.Strings(paths)
	return paths
}

// GarbageCollectShardedImmutableIndexOrphans removes stale, unselected V2
// immutable artifacts after the root catalog has been read but before any
// account-index writer starts. It recognizes only the exact base-generation
// and sharded-delta naming schemes. Unknown files, symlinks, directories and
// every catalog-selected path are left untouched.
//
// This is deliberately a startup-only operation; it does not coordinate with
// live generation resources. Runtime retirement must use the obsolete list
// returned by DeriveWithCheckpoint or DeriveWithRebase instead.
func GarbageCollectShardedImmutableIndexOrphans(
	root string,
	selected *RootIndexCatalog,
) (removed []string, retErr error) {
	storeLock, err := acquireProductionAccountIndexStoreLock(root, true)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, storeLock.Close()) }()
	// A caller may have read selected before acquiring the store lock. Once a
	// canonical selector exists, bind GC reachability to the selector re-read
	// under this lock; otherwise a publication in between those operations could
	// make newly selected artifacts look orphaned. The missing-root case is kept
	// for conservative cleanup of unpublished test/offline builds.
	selectorPath := filepath.Join(root, RootIndexCatalogFileName)
	_, selectorErr := os.Lstat(selectorPath)
	if selectorErr == nil {
		current, err := ReadRootIndexCatalog(root)
		if err != nil {
			return nil, err
		}
		selectedBytes, selectedErr := selected.MarshalBinary()
		if selectedErr != nil {
			return nil, selectedErr
		}
		currentBytes, currentErr := current.MarshalBinary()
		if currentErr != nil {
			return nil, currentErr
		}
		if !bytes.Equal(selectedBytes, currentBytes) {
			return nil, fmt.Errorf(
				"%w: orphan-GC selector was superseded before exclusive ownership",
				ErrInvalidRootIndexCatalog,
			)
		}
		selected = current
	} else if !errors.Is(selectorErr, os.ErrNotExist) {
		return nil, fmt.Errorf("accountsdb: inspect root selector before orphan GC: %w", selectorErr)
	}
	return garbageCollectShardedImmutableIndexOrphansLocked(root, selected)
}

// garbageCollectShardedImmutableIndexOrphansLocked requires the caller to
// hold the store-wide exclusive lock for the complete selector-read/GC
// operation. OpenProductionAccountIndex retains that lock for its lifetime.
func garbageCollectShardedImmutableIndexOrphansLocked(
	root string,
	selected *RootIndexCatalog,
) ([]string, error) {
	if selected == nil {
		return nil, fmt.Errorf("%w: nil selected root for orphan GC", ErrInvalidRootIndexCatalog)
	}
	if err := selected.Validate(); err != nil {
		return nil, err
	}
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: resolve orphan-GC root: %w", err)
	}
	rootInfo, err := os.Lstat(rootAbsolute)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: inspect orphan-GC root: %w", err)
	}
	if !rootInfo.IsDir() {
		return nil, fmt.Errorf("%w: orphan-GC root is not a real directory", ErrInvalidRootIndexCatalog)
	}

	selectedPaths := make(map[string]struct{})
	selectedPaths[selected.SharedExtentCatalog.RelativePath] = struct{}{}
	for _, shard := range selected.Shards {
		selectedPaths[shard.BaseIndex.RelativePath] = struct{}{}
		selectedPaths[shard.BaseRecords.RelativePath] = struct{}{}
		if shard.DeltaGeneration == 0 {
			continue
		}
		selectedPaths[shard.DeltaIndex.RelativePath] = struct{}{}
		selectedPaths[shard.DeltaRecords.RelativePath] = struct{}{}
		descriptorName := filepath.Base(makeDeltaCheckpointPaths("", shard.DeltaGeneration).descriptor)
		descriptorRelative := filepath.ToSlash(filepath.Join(filepath.Dir(shard.DeltaIndex.RelativePath), descriptorName))
		selectedPaths[descriptorRelative] = struct{}{}
	}

	var removed []string
	touchedDirectories := make(map[string]struct{})
	removeKnownFile := func(relative string) error {
		relative = filepath.ToSlash(relative)
		if _, retained := selectedPaths[relative]; retained {
			return nil
		}
		absolute := filepath.Join(rootAbsolute, filepath.FromSlash(relative))
		info, err := os.Lstat(absolute)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("accountsdb: inspect orphan artifact %q: %w", relative, err)
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if err := os.Remove(absolute); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("accountsdb: remove orphan artifact %q: %w", relative, err)
		}
		removed = append(removed, absolute)
		touchedDirectories[filepath.Dir(absolute)] = struct{}{}
		return nil
	}

	rootEntries, err := os.ReadDir(rootAbsolute)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: list orphan-GC root: %w", err)
	}
	var candidateDirectories []string
	for _, entry := range rootEntries {
		if !entry.IsDir() || !isShardedBaseGenerationDirectoryName(entry.Name()) {
			continue
		}
		directory := filepath.Join(rootAbsolute, entry.Name())
		children, err := os.ReadDir(directory)
		if err != nil {
			return removed, fmt.Errorf("accountsdb: list base generation %q for orphan GC: %w", entry.Name(), err)
		}
		for _, child := range children {
			if isStreamHashBuildDirectoryName(child.Name()) {
				path, err := removeRecognizedPrivateBuildDirectory(directory, child.Name())
				if err != nil {
					return removed, fmt.Errorf(
						"accountsdb: remove base-generation StreamHash spill directory %q: %w",
						child.Name(), err,
					)
				}
				removed = append(removed, path)
				touchedDirectories[directory] = struct{}{}
				continue
			}
			if !isShardedBaseArtifactName(child.Name()) {
				continue
			}
			if err := removeKnownFile(filepath.Join(entry.Name(), child.Name())); err != nil {
				return removed, err
			}
		}
		candidateDirectories = append(candidateDirectories, directory)
	}

	deltaRoot := filepath.Join(rootAbsolute, ShardedDeltaCheckpointDirName)
	if info, statErr := os.Lstat(deltaRoot); statErr == nil && info.IsDir() {
		shardDirectories, readErr := os.ReadDir(deltaRoot)
		if readErr != nil {
			return removed, fmt.Errorf("accountsdb: list sharded delta root for orphan GC: %w", readErr)
		}
		for _, shardDirectory := range shardDirectories {
			if !shardDirectory.IsDir() || !isShardedDeltaDirectoryName(shardDirectory.Name()) {
				continue
			}
			directory := filepath.Join(deltaRoot, shardDirectory.Name())
			children, readErr := os.ReadDir(directory)
			if readErr != nil {
				return removed, fmt.Errorf("accountsdb: list delta shard %q for orphan GC: %w", shardDirectory.Name(), readErr)
			}
			for _, child := range children {
				if isDeltaCheckpointBuildDirectoryName(child.Name()) {
					path, err := removeRecognizedPrivateBuildDirectory(directory, child.Name())
					if err != nil {
						return removed, fmt.Errorf(
							"accountsdb: remove delta-checkpoint spill directory %q: %w",
							child.Name(), err,
						)
					}
					removed = append(removed, path)
					touchedDirectories[directory] = struct{}{}
					continue
				}
				if _, _, _, ok := parseDeltaCheckpointArtifactName(child.Name()); !ok {
					continue
				}
				relative := filepath.Join(ShardedDeltaCheckpointDirName, shardDirectory.Name(), child.Name())
				if err := removeKnownFile(relative); err != nil {
					return removed, err
				}
			}
			candidateDirectories = append(candidateDirectories, directory)
		}
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return removed, fmt.Errorf("accountsdb: inspect sharded delta root for orphan GC: %w", statErr)
	}

	for directory := range touchedDirectories {
		if err := fsyncDir(directory); err != nil && !errors.Is(err, os.ErrNotExist) {
			return removed, fmt.Errorf("accountsdb: sync orphan-GC directory %q: %w", directory, err)
		}
	}
	parentDirectories := make(map[string]struct{})
	for _, directory := range candidateDirectories {
		children, readErr := os.ReadDir(directory)
		if errors.Is(readErr, os.ErrNotExist) || readErr == nil && len(children) != 0 {
			continue
		}
		if readErr != nil {
			return removed, fmt.Errorf("accountsdb: inspect empty orphan directory %q: %w", directory, readErr)
		}
		if err := os.Remove(directory); err != nil && !errors.Is(err, os.ErrNotExist) {
			return removed, fmt.Errorf("accountsdb: remove empty orphan directory %q: %w", directory, err)
		}
		parentDirectories[filepath.Dir(directory)] = struct{}{}
	}
	for directory := range parentDirectories {
		if err := fsyncDir(directory); err != nil && !errors.Is(err, os.ErrNotExist) {
			return removed, fmt.Errorf("accountsdb: sync orphan-GC parent %q: %w", directory, err)
		}
	}
	sort.Strings(removed)
	return removed, nil
}

// removeRecognizedPrivateBuildDirectory removes one exact, implementation-
// owned temporary directory without following a final symlink. Callers first
// validate name against the corresponding canonical MkdirTemp pattern. The
// explicit containment and real-directory checks keep a corrupt/lookalike
// artifact from widening startup GC beyond its validated parent.
func removeRecognizedPrivateBuildDirectory(parent, name string) (string, error) {
	if filepath.Base(name) != name || name == "." || name == ".." {
		return "", fmt.Errorf("invalid private build directory name %q", name)
	}
	parentAbsolute, err := filepath.Abs(parent)
	if err != nil {
		return "", fmt.Errorf("resolve private build parent: %w", err)
	}
	parentInfo, err := os.Lstat(parentAbsolute)
	if err != nil {
		return "", fmt.Errorf("inspect private build parent: %w", err)
	}
	if !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("private build parent is not a real directory")
	}
	path := filepath.Join(parentAbsolute, name)
	relative, err := filepath.Rel(parentAbsolute, path)
	if err != nil || relative != name {
		return "", fmt.Errorf("private build directory %q escapes its parent", name)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("inspect private build directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("recognized private build path is not a real directory")
	}
	// os.RemoveAll does not follow a final symlink. There is no concurrent
	// legitimate writer while the caller holds the store-wide exclusive lock;
	// the Lstat above therefore also detects persistent path substitution.
	if err := os.RemoveAll(path); err != nil {
		return "", err
	}
	return path, nil
}

func isStreamHashBuildDirectoryName(name string) bool {
	const prefix = "streamhash-parts-"
	suffix := strings.TrimPrefix(name, prefix)
	if suffix == name || len(suffix) == 0 || len(suffix) > 10 || !allDecimalDigits(suffix) {
		return false
	}
	value, err := strconv.ParseUint(suffix, 10, 32)
	return err == nil && strconv.FormatUint(value, 10) == suffix
}

func isShardedBaseGenerationDirectoryName(name string) bool {
	const prefix = "accounts-index-v2-g"
	if !strings.HasPrefix(name, prefix) || len(name) != len(prefix)+20+1+32 {
		return false
	}
	generation := name[len(prefix) : len(prefix)+20]
	if name[len(prefix)+20] != '-' || !allDecimalDigits(generation) {
		return false
	}
	parsed, err := strconv.ParseUint(generation, 10, 64)
	if err != nil || parsed == 0 {
		return false
	}
	for _, character := range name[len(prefix)+21:] {
		if !('0' <= character && character <= '9') && !('a' <= character && character <= 'f') {
			return false
		}
	}
	return true
}

func isShardedBaseArtifactName(name string) bool {
	if strings.HasSuffix(name, ".partial") {
		name = strings.TrimSuffix(name, ".partial")
	}
	if name == "extents.cat" {
		return true
	}
	if !strings.HasPrefix(name, "shard-") {
		return false
	}
	for _, suffix := range []string{".stmh", ".scan"} {
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		ordinal := strings.TrimSuffix(strings.TrimPrefix(name, "shard-"), suffix)
		if len(ordinal) != 4 || !allDecimalDigits(ordinal) {
			return false
		}
		parsed, err := strconv.ParseUint(ordinal, 10, 32)
		return err == nil && parsed < MaxPersistentIndexShards
	}
	return false
}

func isShardedDeltaDirectoryName(name string) bool {
	if len(name) != 4 || !allDecimalDigits(name) {
		return false
	}
	parsed, err := strconv.ParseUint(name, 10, 32)
	return err == nil && parsed < MaxPersistentIndexShards
}

func allDecimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}
