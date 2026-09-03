package accountsdb

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestShardedImmutableDerivationsKeepPinnedGenerationsReadable(t *testing.T) {
	root := t.TempDir()
	router, err := NewPersistentIndexShardRouter(2, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	baseKey0 := shardedBaseTestKeysForShard(router, 0, 1)[0]
	baseKey1 := shardedBaseTestKeysForShard(router, 1, 1)[0]
	baseRecords := []streamIndexTestRecord{
		{key: baseKey0, entry: AccountIndexEntry{Slot: 10, FileId: 100, Offset: 8}},
		{key: baseKey1, entry: AccountIndexEntry{Slot: 11, FileId: 101, Offset: 16}},
	}
	if bytes.Compare(baseRecords[0].key[:], baseRecords[1].key[:]) > 0 {
		baseRecords[0], baseRecords[1] = baseRecords[1], baseRecords[0]
	}
	baseBuild, err := BuildShardedStreamBaseWithShardCount(
		t.Context(),
		&streamIndexTestSource{records: baseRecords},
		root,
		1,
		2,
		testPersistentIndexRoutingKey(),
		nil,
		1,
	)
	require.NoError(t, err)
	rootOne := rootCatalogForImmutableBuildTest(t, baseBuild, 1)
	initial, err := OpenShardedImmutableIndex(root, rootOne)
	require.NoError(t, err)
	manager, err := NewIndexViewManager(rootOne, initial, initial.Resources())
	require.NoError(t, err)

	deltaDirectory := ShardedMutableCheckpointDirectory(root, 0)
	require.NoError(t, os.MkdirAll(deltaDirectory, 0o755))
	firstValue := deltaIndexValue{Entry: AccountIndexEntry{Slot: 20, FileId: 200, Offset: 8}}
	checkpointOne, err := BuildDeltaCheckpoint(
		t.Context(), deltaDirectory, nil,
		map[solana.PublicKey]deltaIndexValue{baseKey0: firstValue},
		5, 1,
	)
	require.NoError(t, err)
	handleOne, err := NewShardedDeltaCheckpointHandle(checkpointOne)
	require.NoError(t, err)
	handleOneOwned := true
	defer func() {
		if handleOneOwned {
			_ = handleOne.Release()
		}
	}()
	firstArtifacts, err := IdentifyShardedDeltaCheckpointArtifacts(root, deltaDirectory, handleOne)
	require.NoError(t, err)

	rootTwo := initial.RootCatalog()
	rootTwo.Generation = 2
	rootTwo.Shards[0].DeltaGeneration = firstArtifacts.Generation
	rootTwo.Shards[0].DeltaCoveredSequence = firstArtifacts.CoveredSequence
	rootTwo.Shards[0].DeltaIndex = firstArtifacts.Index
	rootTwo.Shards[0].DeltaRecords = firstArtifacts.Records
	rootTwo.Shards[1].BaseCoveredSequence = 5
	rootTwo.CoveredSequence = 5
	publicationOne := ShardedMutableCheckpointPublication{
		ShardID:                  0,
		Directory:                deltaDirectory,
		Next:                     handleOne,
		CoveredSequence:          5,
		ProposedCoveredSequences: []uint64{5, 5},
	}
	second, resources, obsolete, err := initial.DeriveWithCheckpoint(rootTwo, publicationOne)
	require.NoError(t, err)
	require.Empty(t, obsolete)
	require.NoError(t, manager.Publish(rootTwo, second, resources, obsolete))

	viewTwo, err := manager.Acquire()
	require.NoError(t, err)
	pinnedTwo, ok := viewTwo.Payload().(*ShardedImmutableIndex)
	require.True(t, ok)
	initialHandles, err := pinnedTwo.InitialCheckpointHandles()
	require.NoError(t, err)
	require.Same(t, handleOne, initialHandles[0])
	require.Nil(t, initialHandles[1])
	require.NoError(t, initialHandles[0].Release())

	secondKey := shardedBaseTestKeysForShard(router, 0, 2)[1]
	secondValue := deltaIndexValue{Entry: AccountIndexEntry{Slot: 21, FileId: 201, Offset: 16}}
	checkpointTwo, err := BuildDeltaCheckpoint(
		t.Context(), deltaDirectory, handleOne.Checkpoint(),
		map[solana.PublicKey]deltaIndexValue{secondKey: secondValue},
		9, 1,
	)
	require.NoError(t, err)
	handleTwo, err := NewShardedDeltaCheckpointHandle(checkpointTwo)
	require.NoError(t, err)
	handleTwoOwned := true
	defer func() {
		if handleTwoOwned {
			_ = handleTwo.Release()
		}
	}()
	secondArtifacts, err := IdentifyShardedDeltaCheckpointArtifacts(root, deltaDirectory, handleTwo)
	require.NoError(t, err)

	rootThree := pinnedTwo.RootCatalog()
	rootThree.Generation = 3
	rootThree.Shards[0].DeltaGeneration = secondArtifacts.Generation
	rootThree.Shards[0].DeltaCoveredSequence = secondArtifacts.CoveredSequence
	rootThree.Shards[0].DeltaIndex = secondArtifacts.Index
	rootThree.Shards[0].DeltaRecords = secondArtifacts.Records
	rootThree.Shards[1].BaseCoveredSequence = 9
	rootThree.CoveredSequence = 9
	publicationTwo := ShardedMutableCheckpointPublication{
		ShardID:                  0,
		Directory:                deltaDirectory,
		Previous:                 handleOne,
		Next:                     handleTwo,
		CoveredSequence:          9,
		ProposedCoveredSequences: []uint64{9, 9},
	}
	third, resources, obsolete, err := pinnedTwo.DeriveWithCheckpoint(rootThree, publicationTwo)
	require.NoError(t, err)
	require.Len(t, obsolete, 1)
	oldDeltaResource := obsolete[0]
	require.NoError(t, manager.Publish(rootThree, third, resources, obsolete))
	require.NoError(t, handleOne.Release())
	handleOneOwned = false

	assertImmutableArtifactsExist(t, root, firstArtifacts.Index, firstArtifacts.Records, firstArtifacts.Descriptor)
	oldValue, source, found, err := pinnedTwo.LookupCandidate(baseKey0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, accountIndexSourceDelta, source)
	require.Equal(t, firstValue, oldValue)
	select {
	case <-oldDeltaResource.Done():
		t.Fatal("old delta finalized while generation two remained pinned")
	default:
	}
	require.NoError(t, viewTwo.Close())
	waitImmutableResource(t, oldDeltaResource)
	waitImmutableHandle(t, handleOne)
	assertImmutableArtifactsMissing(t, root, firstArtifacts.Index, firstArtifacts.Records, firstArtifacts.Descriptor)

	viewThree, err := manager.Acquire()
	require.NoError(t, err)
	pinnedThree, ok := viewThree.Payload().(*ShardedImmutableIndex)
	require.True(t, ok)
	newValue, source, found, err := pinnedThree.LookupCandidate(secondKey)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, accountIndexSourceDelta, source)
	require.Equal(t, secondValue, newValue)

	mergeSource, err := newMergedShardIndexSource(pinnedThree.BaseShard(0), handleTwo.Checkpoint())
	require.NoError(t, err)
	rebaseBuild, err := BuildShardedStreamBaseShardWithShardCount(
		t.Context(), mergeSource, root, 2, 2, testPersistentIndexRoutingKey(),
		0, pinnedThree.ExtentCatalog(), 1,
	)
	require.NoError(t, err)
	oldExtentCatalog := pinnedThree.ExtentCatalog()
	require.Same(t, oldExtentCatalog, pinnedThree.shards[1].base.catalog.Load())
	rootFour := pinnedThree.RootCatalog()
	rootFour.Generation = 4
	rootFour.SharedExtentCatalog = rebaseBuild.ExtentCatalogArtifact
	rootFour.Shards[0].BaseGeneration = rebaseBuild.Shard.Generation
	rootFour.Shards[0].BaseCoveredSequence = 9
	rootFour.Shards[0].BaseIndex = rebaseBuild.Shard.Index
	rootFour.Shards[0].BaseRecords = rebaseBuild.Shard.Scan
	rootFour.Shards[0].DeltaGeneration = 0
	rootFour.Shards[0].DeltaCoveredSequence = 0
	rootFour.Shards[0].DeltaIndex = IndexCatalogArtifact{}
	rootFour.Shards[0].DeltaRecords = IndexCatalogArtifact{}
	rootFour.CoveredSequence = 9
	fourth, resources, obsolete, err := pinnedThree.DeriveWithRebase(rootFour, rebaseBuild)
	require.NoError(t, err)
	require.Len(t, obsolete, 3)
	require.Same(t, pinnedThree.shards[1].baseResource, fourth.shards[1].baseResource)
	require.Same(t, pinnedThree.shards[1].base, fourth.shards[1].base)
	require.Same(t, rebaseBuild.ExtentCatalog, fourth.shards[1].base.catalog.Load())
	require.Same(t, rebaseBuild.ExtentCatalog, pinnedThree.shards[1].base.catalog.Load(),
		"a retained base shared with a pinned generation must not retain its old full catalog")
	require.NotSame(t, oldExtentCatalog, fourth.shards[1].base.catalog.Load())
	require.NotSame(t, pinnedThree.shards[0].base, fourth.shards[0].base)
	require.Nil(t, fourth.shards[0].delta)

	oldExtentArtifact := pinnedThree.catalog.SharedExtentCatalog
	oldBaseArtifacts := []IndexCatalogArtifact{
		pinnedThree.catalog.Shards[0].BaseIndex,
		pinnedThree.catalog.Shards[0].BaseRecords,
	}
	oldUntouchedBase := pinnedThree.catalog.Shards[1].BaseIndex
	require.NoError(t, manager.Publish(rootFour, fourth, resources, obsolete))
	require.NoError(t, handleTwo.Release())
	handleTwoOwned = false
	assertImmutableArtifactsExist(t, root, oldExtentArtifact, oldBaseArtifacts[0], oldBaseArtifacts[1])
	assertImmutableArtifactsExist(t, root, secondArtifacts.Index, secondArtifacts.Records, secondArtifacts.Descriptor)
	for _, resource := range obsolete {
		select {
		case <-resource.Done():
			t.Fatal("rebase resource finalized while generation three remained pinned")
		default:
		}
	}
	oldValue, source, found, err = pinnedThree.LookupCandidate(secondKey)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, accountIndexSourceDelta, source)
	require.Equal(t, secondValue, oldValue)
	require.NoError(t, viewThree.Close())
	for _, resource := range obsolete {
		waitImmutableResource(t, resource)
	}
	waitImmutableHandle(t, handleTwo)
	assertImmutableArtifactsMissing(t, root, oldExtentArtifact, oldBaseArtifacts[0], oldBaseArtifacts[1])
	assertImmutableArtifactsMissing(t, root, secondArtifacts.Index, secondArtifacts.Records, secondArtifacts.Descriptor)
	assertImmutableArtifactsExist(t, root, oldUntouchedBase)

	viewFour, err := manager.Acquire()
	require.NoError(t, err)
	pinnedFour := viewFour.Payload().(*ShardedImmutableIndex)
	entry, found, err := pinnedFour.LookupBaseCandidate(secondKey)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, secondValue.Entry, entry)
	require.NoError(t, viewFour.Close())
	require.NoError(t, manager.Shutdown(context.Background()))
}

func TestShardedImmutableRebaseReusesCatalogResourceWithoutDeletingArtifact(t *testing.T) {
	root := t.TempDir()
	baseBuild, err := BuildShardedStreamBaseWithShardCount(
		t.Context(),
		&streamIndexTestSource{records: shardedBaseTestRecords()},
		root, 1, 4, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	rootOne := rootCatalogForImmutableBuildTest(t, baseBuild, 1)
	initial, err := OpenShardedImmutableIndex(root, rootOne)
	require.NoError(t, err)
	manager, err := NewIndexViewManager(rootOne, initial, initial.Resources())
	require.NoError(t, err)

	target := uint32(1)
	rebaseBuild, err := BuildShardedStreamBaseShardWithShardCount(
		t.Context(), initial.BaseShard(target), root, 2, 4,
		testPersistentIndexRoutingKey(), target,
		initial.ExtentCatalog(), 1,
	)
	require.NoError(t, err)
	require.False(t, rebaseBuild.OwnsExtentCatalogArtifact)
	require.Same(t, initial.ExtentCatalog(), rebaseBuild.ExtentCatalog)

	rootTwo := initial.RootCatalog()
	rootTwo.Generation++
	selected := &rootTwo.Shards[target]
	selected.BaseGeneration = rebaseBuild.Shard.Generation
	selected.BaseIndex = rebaseBuild.Shard.Index
	selected.BaseRecords = rebaseBuild.Shard.Scan
	require.Equal(t, rootOne.SharedExtentCatalog, rootTwo.SharedExtentCatalog)
	require.NoError(t, rootTwo.Validate())

	second, resources, obsolete, err := initial.DeriveWithRebase(rootTwo, rebaseBuild)
	require.NoError(t, err)
	require.Len(t, obsolete, 1, "only the replaced base is obsolete when the catalog is reused")
	require.Same(t, initial.catalogResource, second.catalogResource)
	require.Same(t, initial.extents, second.extents)
	require.NoError(t, manager.Publish(rootTwo, second, resources, obsolete))
	waitImmutableResource(t, obsolete[0])
	assertImmutableArtifactsExist(t, root, rootOne.SharedExtentCatalog)

	view, err := manager.Acquire()
	require.NoError(t, err)
	pinned := view.Payload().(*ShardedImmutableIndex)
	var checked int
	require.NoError(t, pinned.BaseShard(target).Scan(t.Context(), func(key solana.PublicKey, want AccountIndexEntry) error {
		got, found, err := pinned.LookupBaseCandidate(key)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, want, got)
		checked++
		return nil
	}))
	require.Positive(t, checked)
	require.NoError(t, view.Close())
	require.NoError(t, manager.Shutdown(context.Background()))
}

func TestShardedImmutableRejectsSuccessorRoutingKeyChange(t *testing.T) {
	root := t.TempDir()
	baseBuild, err := BuildShardedStreamBaseWithShardCount(
		t.Context(), &streamIndexTestSource{records: shardedBaseTestRecords()},
		root, 1, 4, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	rootOne := rootCatalogForImmutableBuildTest(t, baseBuild, 1)
	initial, err := OpenShardedImmutableIndex(root, rootOne)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, initial.closeUnmanaged()) })

	next := rootOne.Clone()
	next.Generation++
	next.RoutingKey[0] ^= 0xff
	require.NoError(t, next.Validate(), "a non-zero alternate key is structurally valid")
	require.ErrorContains(t, initial.validateSuccessorRoot(next), "routing identity changed")
}

func TestOpenShardedImmutableRejectsMisroutedDeltaArtifact(t *testing.T) {
	root := t.TempDir()
	baseBuild, err := BuildShardedStreamBaseWithShardCount(
		t.Context(), &streamIndexTestSource{records: nil},
		root, 1, 2, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	catalog := rootCatalogForImmutableBuildTest(t, baseBuild, 1)
	router, err := NewPersistentIndexShardRouter(2, catalog.RoutingKey)
	require.NoError(t, err)
	keyForShardOne := shardedBaseTestKeysForShard(router, 1, 1)[0]
	directory := ShardedMutableCheckpointDirectory(root, 0)
	require.NoError(t, os.MkdirAll(directory, 0o755))
	checkpoint, err := BuildDeltaCheckpoint(
		t.Context(), directory, nil,
		map[solana.PublicKey]deltaIndexValue{
			keyForShardOne: {Entry: AccountIndexEntry{Slot: 2, FileId: 3, Offset: 8}},
		},
		5, 1,
	)
	require.NoError(t, err)
	handle, err := NewShardedDeltaCheckpointHandle(checkpoint)
	require.NoError(t, err)
	artifacts, err := IdentifyShardedDeltaCheckpointArtifacts(root, directory, handle)
	require.NoError(t, err)
	selected := &catalog.Shards[0]
	selected.DeltaGeneration = artifacts.Generation
	selected.DeltaCoveredSequence = artifacts.CoveredSequence
	selected.DeltaIndex = artifacts.Index
	selected.DeltaRecords = artifacts.Records
	require.NoError(t, catalog.Validate())
	require.NoError(t, handle.Release())
	waitImmutableHandle(t, handle)

	opened, err := OpenShardedImmutableIndex(root, catalog)
	require.ErrorContains(t, err, "routes to shard 1, selected by shard 0")
	require.Nil(t, opened)
}

func TestOpenShardedImmutableAcceptsLogicalCoverageAboveCheckpoint(t *testing.T) {
	root := t.TempDir()
	key := streamIndexTestKey(0x00, 1)
	baseBuild, err := BuildShardedStreamBaseWithShardCount(
		t.Context(),
		&streamIndexTestSource{records: []streamIndexTestRecord{{key: key, entry: AccountIndexEntry{Slot: 1, FileId: 1, Offset: 8}}}},
		root, 1, 1, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	catalog := rootCatalogForImmutableBuildTest(t, baseBuild, 1)
	directory := ShardedMutableCheckpointDirectory(root, 0)
	require.NoError(t, os.MkdirAll(directory, 0o755))
	checkpoint, err := BuildDeltaCheckpoint(
		t.Context(), directory, nil,
		map[solana.PublicKey]deltaIndexValue{key: {Entry: AccountIndexEntry{Slot: 2, FileId: 2, Offset: 8}}},
		5, 1,
	)
	require.NoError(t, err)
	handle, err := NewShardedDeltaCheckpointHandle(checkpoint)
	require.NoError(t, err)
	artifacts, err := IdentifyShardedDeltaCheckpointArtifacts(root, directory, handle)
	require.NoError(t, err)
	catalog.Shards[0].DeltaGeneration = artifacts.Generation
	catalog.Shards[0].DeltaCoveredSequence = 9
	catalog.Shards[0].DeltaIndex = artifacts.Index
	catalog.Shards[0].DeltaRecords = artifacts.Records
	catalog.CoveredSequence = 9
	opened, err := OpenShardedImmutableIndex(root, catalog)
	require.NoError(t, err)
	require.NoError(t, opened.closeUnmanaged())
	require.NoError(t, handle.Release())
	waitImmutableHandle(t, handle)
}

func TestGarbageCollectShardedImmutableIndexOrphansIsConservative(t *testing.T) {
	root := t.TempDir()
	baseBuild, err := BuildShardedStreamBaseWithShardCount(
		t.Context(),
		&streamIndexTestSource{records: []streamIndexTestRecord{{
			key: streamIndexTestKey(0x00, 1), entry: AccountIndexEntry{Slot: 1, FileId: 1, Offset: 8},
		}}},
		root, 1, 1, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	catalog := rootCatalogForImmutableBuildTest(t, baseBuild, 1)

	orphanDirectory := filepath.Join(root, "accounts-index-v2-g00000000000000000002-0123456789abcdef0123456789abcdef")
	require.NoError(t, os.Mkdir(orphanDirectory, 0o755))
	orphanBase := filepath.Join(orphanDirectory, "shard-0000.stmh")
	orphanPartial := filepath.Join(orphanDirectory, "extents.cat.partial")
	unknown := filepath.Join(orphanDirectory, "keep-me")
	for _, path := range []string{orphanBase, orphanPartial, unknown} {
		require.NoError(t, os.WriteFile(path, []byte("x"), 0o600))
	}
	baseSpill := filepath.Join(orphanDirectory, "streamhash-parts-123")
	require.NoError(t, os.Mkdir(baseSpill, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(baseSpill, "part-0"), []byte("spill"), 0o600))
	baseSpillLookalike := filepath.Join(orphanDirectory, "streamhash-parts-operator-notes")
	require.NoError(t, os.Mkdir(baseSpillLookalike, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(baseSpillLookalike, "keep"), []byte("x"), 0o600))
	malformedDirectory := filepath.Join(root, "accounts-index-v2-g2-not-a-generation")
	require.NoError(t, os.Mkdir(malformedDirectory, 0o755))
	malformedArtifact := filepath.Join(malformedDirectory, "shard-0000.stmh")
	require.NoError(t, os.WriteFile(malformedArtifact, []byte("x"), 0o600))
	symlink := filepath.Join(orphanDirectory, "shard-0000.scan")
	require.NoError(t, os.Symlink(unknown, symlink))

	deltaDirectory := ShardedMutableCheckpointDirectory(root, 0)
	require.NoError(t, os.MkdirAll(deltaDirectory, 0o755))
	orphanDelta := makeDeltaCheckpointPaths(deltaDirectory, 7).index
	unknownDelta := filepath.Join(deltaDirectory, "unknown.stmh")
	require.NoError(t, os.WriteFile(orphanDelta, []byte("x"), 0o600))
	require.NoError(t, os.WriteFile(unknownDelta, []byte("x"), 0o600))
	deltaSpill := filepath.Join(deltaDirectory, deltaCheckpointBuildDirPrefix+"456")
	require.NoError(t, os.Mkdir(deltaSpill, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(deltaSpill, "part-0"), []byte("spill"), 0o600))
	deltaSpillLookalike := filepath.Join(deltaDirectory, deltaCheckpointBuildDirPrefix+"operator-notes")
	require.NoError(t, os.Mkdir(deltaSpillLookalike, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(deltaSpillLookalike, "keep"), []byte("x"), 0o600))

	removed, err := GarbageCollectShardedImmutableIndexOrphans(root, catalog)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{orphanBase, orphanPartial, baseSpill, orphanDelta, deltaSpill}, removed)
	for _, removedPath := range []string{baseSpill, deltaSpill} {
		_, err := os.Lstat(removedPath)
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	for _, selected := range append(
		[]IndexCatalogArtifact{catalog.SharedExtentCatalog},
		catalog.Shards[0].BaseIndex,
		catalog.Shards[0].BaseRecords,
	) {
		assertImmutableArtifactsExist(t, root, selected)
	}
	for _, retained := range []string{
		unknown, malformedArtifact, symlink, unknownDelta,
		baseSpillLookalike, deltaSpillLookalike,
	} {
		_, err := os.Lstat(retained)
		require.NoError(t, err)
	}
}

func TestGarbageCollectShardedImmutableIndexOrphansDoesNotFollowPrivateBuildSymlink(t *testing.T) {
	root := t.TempDir()
	baseBuild, err := BuildShardedStreamBaseWithShardCount(
		t.Context(),
		&streamIndexTestSource{records: []streamIndexTestRecord{{
			key: streamIndexTestKey(0x00, 1), entry: AccountIndexEntry{Slot: 1, FileId: 1, Offset: 8},
		}}},
		root, 1, 1, testPersistentIndexRoutingKey(), nil, 1,
	)
	require.NoError(t, err)
	catalog := rootCatalogForImmutableBuildTest(t, baseBuild, 1)

	deltaDirectory := ShardedMutableCheckpointDirectory(root, 0)
	require.NoError(t, os.MkdirAll(deltaDirectory, 0o755))
	target := filepath.Join(root, "must-survive")
	require.NoError(t, os.Mkdir(target, 0o755))
	sentinel := filepath.Join(target, "sentinel")
	require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0o600))
	link := filepath.Join(deltaDirectory, deltaCheckpointBuildDirPrefix+"123")
	require.NoError(t, os.Symlink(target, link))

	_, err = GarbageCollectShardedImmutableIndexOrphans(root, catalog)
	require.ErrorContains(t, err, "not a real directory")
	require.FileExists(t, sentinel)
	info, err := os.Lstat(link)
	require.NoError(t, err)
	require.True(t, info.Mode()&os.ModeSymlink != 0)
}

func rootCatalogForImmutableBuildTest(
	t *testing.T,
	build *ShardedStreamBaseBuildResult,
	generation uint64,
) *RootIndexCatalog {
	t.Helper()
	catalog, err := NewRootIndexCatalog(int(build.ShardCount))
	require.NoError(t, err)
	catalog.RoutingKey = build.RoutingKey
	catalog.Generation = generation
	catalog.Lineage[0] = 1
	catalog.SharedExtentCatalog = build.ExtentCatalogArtifact
	for shardID, shard := range build.Shards {
		catalog.Shards[shardID].BaseGeneration = shard.Generation
		catalog.Shards[shardID].BaseIndex = shard.Index
		catalog.Shards[shardID].BaseRecords = shard.Scan
	}
	require.NoError(t, catalog.Validate())
	return catalog
}

func assertImmutableArtifactsExist(t *testing.T, root string, artifacts ...IndexCatalogArtifact) {
	t.Helper()
	for _, artifact := range artifacts {
		path, err := ResolveIndexCatalogArtifactPath(root, artifact)
		require.NoError(t, err)
		_, err = os.Lstat(path)
		require.NoError(t, err, path)
	}
}

func assertImmutableArtifactsMissing(t *testing.T, root string, artifacts ...IndexCatalogArtifact) {
	t.Helper()
	for _, artifact := range artifacts {
		path, err := ResolveIndexCatalogArtifactPath(root, artifact)
		require.NoError(t, err)
		_, err = os.Lstat(path)
		require.ErrorIs(t, err, os.ErrNotExist, path)
	}
}

func waitImmutableResource(t *testing.T, resource *IndexGenerationResource) {
	t.Helper()
	select {
	case <-resource.Done():
		require.NoError(t, resource.Err())
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for immutable resource %q", resource.Name())
	}
}

func waitImmutableHandle(t *testing.T, handle *ShardedDeltaCheckpointHandle) {
	t.Helper()
	select {
	case <-handle.Done():
		require.NoError(t, handle.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for shared checkpoint handle")
	}
}
