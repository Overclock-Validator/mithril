package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stellar/streamhash"
	"golang.org/x/sys/unix"
)

func TestDeltaCheckpointBuildLookupEnumerateMergeAndOldGenerationSafety(t *testing.T) {
	dir := t.TempDir()
	missing, err := OpenLatestDeltaCheckpoint(dir)
	if err != nil {
		t.Fatalf("open empty checkpoint directory: %v", err)
	}
	if missing != nil {
		t.Fatal("empty checkpoint directory returned a checkpoint")
	}

	keyA := deltaCheckpointTestKey(1)
	keyB := deltaCheckpointTestKey(2)
	keyC := deltaCheckpointTestKey(3)
	keyD := deltaCheckpointTestKey(4)
	entryA := AccountIndexEntry{Slot: 101, FileId: 201, Offset: 304}
	entryB := AccountIndexEntry{Slot: 102, FileId: 202, Offset: 408}
	entryC := AccountIndexEntry{Slot: 103, FileId: 203, Offset: 512}

	first, err := BuildDeltaCheckpoint(context.Background(), dir, nil, map[solana.PublicKey]deltaIndexValue{
		keyC: {Entry: entryC},
		keyA: {Entry: entryA},
		keyB: {Tombstone: true, Entry: AccountIndexEntry{Slot: 999, FileId: 999, Offset: 999}},
	}, 7, 2)
	if err != nil {
		t.Fatalf("build first checkpoint: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })
	if first.Generation() != 1 || first.CoveredSeq() != 7 || first.Len() != 3 {
		t.Fatalf("first checkpoint identity = generation %d covered %d len %d", first.Generation(), first.CoveredSeq(), first.Len())
	}
	requireDeltaCheckpointValue(t, first, keyA, deltaIndexValue{Entry: entryA})
	requireDeltaCheckpointValue(t, first, keyB, deltaIndexValue{Tombstone: true})
	requireDeltaCheckpointValue(t, first, keyC, deltaIndexValue{Entry: entryC})
	requireDeltaCheckpointMissing(t, first, keyD)

	var firstKeys []solana.PublicKey
	if err := first.ForEachSorted(func(key solana.PublicKey, value deltaIndexValue) error {
		firstKeys = append(firstKeys, key)
		return nil
	}); err != nil {
		t.Fatalf("enumerate first checkpoint: %v", err)
	}
	if !sort.SliceIsSorted(firstKeys, func(i, j int) bool {
		return bytes.Compare(firstKeys[i][:], firstKeys[j][:]) < 0
	}) {
		t.Fatalf("checkpoint records are not sorted: %x", firstKeys)
	}

	second, err := BuildDeltaCheckpoint(context.Background(), dir, first, map[solana.PublicKey]deltaIndexValue{
		keyA: {Tombstone: true}, // newest deletion replaces old live value
		keyB: {Entry: entryB},   // newest live value replaces old tombstone
		keyD: {Entry: AccountIndexEntry{Slot: 104, FileId: 204, Offset: 616}},
	}, 11, 3)
	if err != nil {
		t.Fatalf("build merged checkpoint: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if second.Generation() != 2 || second.CoveredSeq() != 11 || second.Len() != 4 {
		t.Fatalf("second checkpoint identity = generation %d covered %d len %d", second.Generation(), second.CoveredSeq(), second.Len())
	}
	requireDeltaCheckpointValue(t, second, keyA, deltaIndexValue{Tombstone: true})
	requireDeltaCheckpointValue(t, second, keyB, deltaIndexValue{Entry: entryB})
	requireDeltaCheckpointValue(t, second, keyC, deltaIndexValue{Entry: entryC})
	requireDeltaCheckpointValue(t, second, keyD, deltaIndexValue{Entry: AccountIndexEntry{Slot: 104, FileId: 204, Offset: 616}})

	// A new generation never overwrites or truncates its still-open parent.
	requireDeltaCheckpointValue(t, first, keyA, deltaIndexValue{Entry: entryA})
	requireDeltaCheckpointValue(t, first, keyB, deltaIndexValue{Tombstone: true})
	for _, path := range []string{
		makeDeltaCheckpointPaths(dir, first.Generation()).index,
		makeDeltaCheckpointPaths(dir, first.Generation()).records,
		makeDeltaCheckpointPaths(dir, first.Generation()).descriptor,
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("old generation artifact disappeared while open: %s: %v", path, err)
		}
	}

	latest, err := OpenLatestDeltaCheckpoint(dir)
	if err != nil {
		t.Fatalf("open latest merged checkpoint: %v", err)
	}
	if latest == nil {
		t.Fatal("latest merged checkpoint is nil")
	}
	defer latest.Close()
	if latest.Generation() != second.Generation() {
		t.Fatalf("latest generation = %d, want %d", latest.Generation(), second.Generation())
	}
	requireDeltaCheckpointValue(t, latest, keyA, deltaIndexValue{Tombstone: true})
}

func TestDeltaCheckpointLookupBatchExactOrderedAndCancelable(t *testing.T) {
	dir := t.TempDir()
	liveA := deltaCheckpointTestKey(10)
	tombstone := deltaCheckpointTestKey(11)
	liveB := deltaCheckpointTestKey(12)
	missingA := deltaCheckpointTestKey(13)
	missingB := deltaCheckpointTestKey(14)
	entryA := AccountIndexEntry{Slot: 110, FileId: 210, Offset: 320}
	entryB := AccountIndexEntry{Slot: 120, FileId: 220, Offset: 424}
	checkpoint, err := BuildDeltaCheckpoint(context.Background(), dir, nil, map[solana.PublicKey]deltaIndexValue{
		liveA:     {Entry: entryA},
		tombstone: {Tombstone: true},
		liveB:     {Entry: entryB},
	}, 3, 2)
	if err != nil {
		t.Fatalf("build checkpoint: %v", err)
	}
	defer checkpoint.Close()

	keys := []solana.PublicKey{missingA, liveB, tombstone, liveA, missingB, liveB}
	values := make([]deltaIndexValue, len(keys))
	found := make([]bool, len(keys))
	if err := checkpoint.LookupBatch(context.Background(), keys, values, found); err != nil {
		t.Fatalf("LookupBatch: %v", err)
	}
	wantValues := []deltaIndexValue{
		{}, {Entry: entryB}, {Tombstone: true}, {Entry: entryA}, {}, {Entry: entryB},
	}
	wantFound := []bool{false, true, true, true, false, true}
	for i := range keys {
		if values[i] != wantValues[i] || found[i] != wantFound[i] {
			t.Fatalf("LookupBatch result %d = (%+v, %v), want (%+v, %v)", i, values[i], found[i], wantValues[i], wantFound[i])
		}
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := checkpoint.LookupBatch(cancelled, keys, values, found); !errors.Is(err, context.Canceled) {
		t.Fatalf("LookupBatch cancelled error = %v, want context.Canceled", err)
	}
	if err := checkpoint.LookupBatch(context.Background(), keys, values[:len(values)-1], found); err == nil || !strings.Contains(err.Error(), "buffer mismatch") {
		t.Fatalf("LookupBatch mismatched buffers error = %v", err)
	}
}

func TestDeltaCheckpointExactRecordRejectsForcedTombstoneFalsePositive(t *testing.T) {
	dir := t.TempDir()
	tombstoneKey := deltaCheckpointTestKey(20)
	missingKey := deltaCheckpointTestKey(21)
	checkpoint, err := BuildDeltaCheckpoint(context.Background(), dir, nil, map[solana.PublicKey]deltaIndexValue{
		tombstoneKey: {Tombstone: true},
	}, 1, 1)
	if err != nil {
		t.Fatalf("build tombstone checkpoint: %v", err)
	}
	defer checkpoint.Close()

	originalHash := deltaCheckpointHashKey
	deltaCheckpointHashKey = func(key solana.PublicKey, seed uint64) [streamhash.MinKeySize]byte {
		if key == missingKey {
			// Force the missing key through the exact member's MPHF rank and
			// fingerprint. Lookup must still compare the 32-byte record key.
			return originalHash(tombstoneKey, seed)
		}
		return originalHash(key, seed)
	}
	defer func() { deltaCheckpointHashKey = originalHash }()

	requireDeltaCheckpointValue(t, checkpoint, tombstoneKey, deltaIndexValue{Tombstone: true})
	requireDeltaCheckpointMissing(t, checkpoint, missingKey)
}

func TestDeltaCheckpointRetriesPrehashCollision(t *testing.T) {
	dir := t.TempDir()
	originalSeedSource := deltaCheckpointSeedSource
	originalHash := deltaCheckpointHashKey
	seedCalls := 0
	deltaCheckpointSeedSource = func() (uint64, error) {
		seedCalls++
		return uint64(seedCalls), nil
	}
	deltaCheckpointHashKey = func(key solana.PublicKey, seed uint64) (hash [streamhash.MinKeySize]byte) {
		if seed == 1 {
			return hash // both distinct keys collide on the first attempt
		}
		return originalHash(key, seed)
	}
	defer func() {
		deltaCheckpointSeedSource = originalSeedSource
		deltaCheckpointHashKey = originalHash
	}()

	left := deltaCheckpointTestKey(30)
	right := deltaCheckpointTestKey(31)
	checkpoint, err := BuildDeltaCheckpoint(context.Background(), dir, nil, map[solana.PublicKey]deltaIndexValue{
		left:  {Entry: AccountIndexEntry{Slot: 30, FileId: 40, Offset: 48}},
		right: {Entry: AccountIndexEntry{Slot: 31, FileId: 41, Offset: 56}},
	}, 2, 2)
	if err != nil {
		t.Fatalf("build after deterministic collision: %v", err)
	}
	defer checkpoint.Close()
	if seedCalls < 2 {
		t.Fatalf("seed source called %d time(s), want collision retry", seedCalls)
	}
	requireDeltaCheckpointValue(t, checkpoint, left, deltaIndexValue{Entry: AccountIndexEntry{Slot: 30, FileId: 40, Offset: 48}})
	requireDeltaCheckpointValue(t, checkpoint, right, deltaIndexValue{Entry: AccountIndexEntry{Slot: 31, FileId: 41, Offset: 56}})
}

func TestDeltaCheckpointPublishedCorruptionFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		path func(deltaCheckpointPaths) string
		want string
	}{
		{name: "record artifact", path: func(paths deltaCheckpointPaths) string { return paths.records }, want: "SHA-256 mismatch"},
		{name: "StreamHash artifact", path: func(paths deltaCheckpointPaths) string { return paths.index }, want: "SHA-256 mismatch"},
		{name: "descriptor", path: func(paths deltaCheckpointPaths) string { return paths.descriptor }, want: "descriptor CRC mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			checkpoint, err := BuildDeltaCheckpoint(context.Background(), dir, nil, map[solana.PublicKey]deltaIndexValue{
				deltaCheckpointTestKey(40): {Entry: AccountIndexEntry{Slot: 40, FileId: 50, Offset: 64}},
			}, 4, 1)
			if err != nil {
				t.Fatalf("build checkpoint: %v", err)
			}
			generation := checkpoint.Generation()
			if err := checkpoint.Close(); err != nil {
				t.Fatalf("close checkpoint before corruption: %v", err)
			}
			path := tc.path(makeDeltaCheckpointPaths(dir, generation))
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("open artifact for corruption: %v", err)
			}
			var one [1]byte
			if _, err := file.ReadAt(one[:], 20); err != nil {
				_ = file.Close()
				t.Fatalf("read byte for corruption: %v", err)
			}
			one[0] ^= 0x80
			if _, err := file.WriteAt(one[:], 20); err != nil {
				_ = file.Close()
				t.Fatalf("write corrupt byte: %v", err)
			}
			if err := file.Sync(); err != nil {
				_ = file.Close()
				t.Fatalf("sync corrupt byte: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatalf("close corrupt artifact: %v", err)
			}

			opened, err := OpenLatestDeltaCheckpoint(dir)
			if err == nil {
				_ = opened.Close()
				t.Fatal("published corrupt checkpoint opened successfully")
			}
			if opened != nil {
				t.Fatal("failed checkpoint open returned a usable object")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("open error = %q, want %q", err, tc.want)
			}
			if !errors.Is(err, ErrInvalidDeltaCheckpoint) {
				t.Fatalf("open error = %v, want ErrInvalidDeltaCheckpoint", err)
			}
		})
	}
}

func TestDeltaCheckpointLookupDetectsRecordCorruptionAfterOpen(t *testing.T) {
	dir := t.TempDir()
	key := deltaCheckpointTestKey(45)
	checkpoint, err := BuildDeltaCheckpoint(context.Background(), dir, nil, map[solana.PublicKey]deltaIndexValue{
		key: {Entry: AccountIndexEntry{Slot: 45, FileId: 55, Offset: 64}},
	}, 4, 1)
	if err != nil {
		t.Fatalf("build checkpoint: %v", err)
	}
	defer checkpoint.Close()
	requireDeltaCheckpointValue(t, checkpoint, key, deltaIndexValue{Entry: AccountIndexEntry{Slot: 45, FileId: 55, Offset: 64}})

	path := makeDeltaCheckpointPaths(dir, checkpoint.Generation()).records
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open mapped record file for corruption: %v", err)
	}
	// Corrupt the exact key without updating the per-record CRC. The mmap is
	// MAP_SHARED, so a subsequent lookup must report corruption rather than
	// turn it into a false miss and fall through to an older generation.
	var one [1]byte
	offset := int64(deltaCheckpointRecordHeaderSize + 7)
	if _, err := file.ReadAt(one[:], offset); err != nil {
		_ = file.Close()
		t.Fatalf("read record byte: %v", err)
	}
	one[0] ^= 0x40
	if _, err := file.WriteAt(one[:], offset); err != nil {
		_ = file.Close()
		t.Fatalf("corrupt mapped record byte: %v", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatalf("sync mapped record corruption: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close corrupted mapped record: %v", err)
	}

	_, found, err := checkpoint.Lookup(key)
	if err == nil || !strings.Contains(err.Error(), "record 0 CRC mismatch") {
		t.Fatalf("Lookup after mapped record corruption = found %v error %v, want CRC failure", found, err)
	}
	if !errors.Is(err, ErrInvalidDeltaCheckpoint) {
		t.Fatalf("Lookup corruption error = %v, want ErrInvalidDeltaCheckpoint", err)
	}
	values := make([]deltaIndexValue, 1)
	foundBatch := make([]bool, 1)
	err = checkpoint.LookupBatch(context.Background(), []solana.PublicKey{key}, values, foundBatch)
	if err == nil || !strings.Contains(err.Error(), "record 0 CRC mismatch") {
		t.Fatalf("LookupBatch after mapped record corruption = found %v error %v, want CRC failure", foundBatch[0], err)
	}
}

func TestDeltaCheckpointIgnoresPartialAndUnpublishedHigherGeneration(t *testing.T) {
	dir := t.TempDir()
	checkpoint, err := BuildDeltaCheckpoint(context.Background(), dir, nil, map[solana.PublicKey]deltaIndexValue{
		deltaCheckpointTestKey(50): {Entry: AccountIndexEntry{Slot: 50, FileId: 60, Offset: 72}},
	}, 5, 1)
	if err != nil {
		t.Fatalf("build checkpoint: %v", err)
	}
	defer checkpoint.Close()

	higher := makeDeltaCheckpointPaths(dir, checkpoint.Generation()+1)
	for _, path := range []string{higher.records, higher.index, higher.descriptorPartial} {
		if err := os.WriteFile(path, []byte("interrupted-build"), 0o644); err != nil {
			t.Fatalf("write interrupted artifact %s: %v", path, err)
		}
	}
	latest, err := OpenLatestDeltaCheckpoint(dir)
	if err != nil {
		t.Fatalf("open latest with unpublished higher artifacts: %v", err)
	}
	if latest == nil {
		t.Fatal("latest checkpoint is nil")
	}
	defer latest.Close()
	if latest.Generation() != checkpoint.Generation() {
		t.Fatalf("unpublished generation became visible: got %d want %d", latest.Generation(), checkpoint.Generation())
	}
}

func TestDeltaCheckpointHighestPublishedDescriptorNeverFallsBack(t *testing.T) {
	dir := t.TempDir()
	checkpoint, err := BuildDeltaCheckpoint(context.Background(), dir, nil, map[solana.PublicKey]deltaIndexValue{
		deltaCheckpointTestKey(60): {Entry: AccountIndexEntry{Slot: 60, FileId: 70, Offset: 80}},
	}, 6, 1)
	if err != nil {
		t.Fatalf("build checkpoint: %v", err)
	}
	defer checkpoint.Close()

	oldDescriptor, err := os.ReadFile(makeDeltaCheckpointPaths(dir, checkpoint.Generation()).descriptor)
	if err != nil {
		t.Fatalf("read old descriptor: %v", err)
	}
	higher := makeDeltaCheckpointPaths(dir, checkpoint.Generation()+1)
	if err := os.WriteFile(higher.descriptor, oldDescriptor, 0o644); err != nil {
		t.Fatalf("write mismatched higher descriptor: %v", err)
	}
	opened, err := OpenLatestDeltaCheckpoint(dir)
	if err == nil {
		_ = opened.Close()
		t.Fatal("mismatched highest descriptor silently fell back")
	}
	if !strings.Contains(err.Error(), "names generation") {
		t.Fatalf("open error = %q, want generation mismatch", err)
	}
}

func TestDeltaCheckpointDescriptorReadIsBoundedAndRejectsSymlink(t *testing.T) {
	t.Run("sparse oversized", func(t *testing.T) {
		dir := t.TempDir()
		path := makeDeltaCheckpointPaths(dir, 1).descriptor
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Truncate(16 << 30); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		opened, err := OpenLatestDeltaCheckpoint(dir)
		if opened != nil {
			_ = opened.Close()
		}
		if !errors.Is(err, ErrInvalidDeltaCheckpoint) || !strings.Contains(err.Error(), "want 128") {
			t.Fatalf("open oversized descriptor error = %v", err)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		dir := t.TempDir()
		external := filepath.Join(t.TempDir(), "descriptor")
		if err := os.WriteFile(external, make([]byte, deltaCheckpointDescriptorSize), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(external, makeDeltaCheckpointPaths(dir, 1).descriptor); err != nil {
			t.Fatal(err)
		}
		opened, err := OpenLatestDeltaCheckpoint(dir)
		if opened != nil {
			_ = opened.Close()
		}
		if !errors.Is(err, ErrInvalidDeltaCheckpoint) || !strings.Contains(err.Error(), "not a regular file") {
			t.Fatalf("open symlink descriptor error = %v", err)
		}
	})
}

func TestDeltaCheckpointPostRenameSyncFailureIsCommitDecided(t *testing.T) {
	dir := t.TempDir()
	paths := makeDeltaCheckpointPaths(dir, 1)
	descriptor := deltaCheckpointDescriptor{
		Generation:  1,
		RecordCount: 0,
		Index:       deltaCheckpointIdentity{Size: 1},
		Records:     deltaCheckpointIdentity{Size: deltaCheckpointRecordHeaderSize},
	}
	descriptor.Index.SHA256[0] = 1
	descriptor.Records.SHA256[0] = 2
	injected := error(unix.EIO)
	err := publishDeltaCheckpointDescriptor(
		dir,
		paths.descriptorPartial,
		paths.descriptor,
		descriptor,
		func(string) error { return injected },
	)
	if !errors.Is(err, ErrDeltaCheckpointCommitDecided) || !errors.Is(err, injected) {
		t.Fatalf("publication error = %v, want commit-decided and injected errors", err)
	}
	if !isFatalShardedMutableSealError(err) {
		t.Fatalf("post-rename failure must poison maintenance, got %v", err)
	}
	if _, statErr := os.Lstat(paths.descriptor); statErr != nil {
		t.Fatalf("published descriptor was not retained: %v", statErr)
	}
	if _, statErr := os.Lstat(paths.descriptorPartial); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("descriptor partial unexpectedly remains: %v", statErr)
	}
}

func TestDeltaCheckpointGarbageCollectionKeepsCurrentAndUnknownFiles(t *testing.T) {
	dir := t.TempDir()
	first, err := BuildDeltaCheckpoint(context.Background(), dir, nil, map[solana.PublicKey]deltaIndexValue{
		deltaCheckpointTestKey(70): {Entry: AccountIndexEntry{Slot: 70, FileId: 80, Offset: 88}},
	}, 7, 1)
	if err != nil {
		t.Fatalf("build first checkpoint: %v", err)
	}
	second, err := BuildDeltaCheckpoint(context.Background(), dir, first, map[solana.PublicKey]deltaIndexValue{
		deltaCheckpointTestKey(71): {Tombstone: true},
	}, 8, 1)
	if err != nil {
		_ = first.Close()
		t.Fatalf("build second checkpoint: %v", err)
	}
	if err := first.Close(); err != nil {
		_ = second.Close()
		t.Fatalf("close old checkpoint before GC: %v", err)
	}
	defer second.Close()
	unknown := filepath.Join(dir, "accounts_delta_checkpoint.user-notes")
	if err := os.WriteFile(unknown, []byte("keep"), 0o644); err != nil {
		t.Fatalf("write unknown file: %v", err)
	}
	staleBuildDir, err := os.MkdirTemp(dir, deltaCheckpointBuildDirPrefix)
	if err != nil {
		t.Fatalf("create stale build directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staleBuildDir, "partition"), []byte("spill"), 0o644); err != nil {
		t.Fatalf("write stale build spill: %v", err)
	}
	lookalikeBuildDir := filepath.Join(dir, deltaCheckpointBuildDirPrefix+"operator-data")
	if err := os.Mkdir(lookalikeBuildDir, 0o755); err != nil {
		t.Fatalf("create build-directory lookalike: %v", err)
	}
	if err := os.WriteFile(filepath.Join(lookalikeBuildDir, "keep"), []byte("important"), 0o644); err != nil {
		t.Fatalf("write build-directory lookalike: %v", err)
	}

	removed, err := GarbageCollectDeltaCheckpointFiles(dir, second.Generation())
	if err != nil {
		t.Fatalf("garbage collect checkpoints: %v", err)
	}
	if len(removed) != 4 {
		t.Fatalf("removed %d checkpoint paths, want old triplet plus stale build dir: %v", len(removed), removed)
	}
	for _, path := range []string{
		makeDeltaCheckpointPaths(dir, second.Generation()).index,
		makeDeltaCheckpointPaths(dir, second.Generation()).records,
		makeDeltaCheckpointPaths(dir, second.Generation()).descriptor,
		unknown,
		lookalikeBuildDir,
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("retained file is missing: %s: %v", path, err)
		}
	}
	latest, err := OpenLatestDeltaCheckpoint(dir)
	if err != nil {
		t.Fatalf("open checkpoint after GC: %v", err)
	}
	if latest == nil {
		t.Fatal("checkpoint after GC is nil")
	}
	defer latest.Close()
	if latest.Generation() != second.Generation() {
		t.Fatalf("latest generation after GC = %d, want %d", latest.Generation(), second.Generation())
	}
}

func TestDeltaCheckpointRejectsCoveredSequenceRegressionAndCancellation(t *testing.T) {
	dir := t.TempDir()
	checkpoint, err := BuildDeltaCheckpoint(context.Background(), dir, nil, map[solana.PublicKey]deltaIndexValue{}, 10, 1)
	if err != nil {
		t.Fatalf("build empty checkpoint: %v", err)
	}
	defer checkpoint.Close()
	if checkpoint.Len() != 0 {
		t.Fatalf("empty checkpoint Len = %d, want 0", checkpoint.Len())
	}
	if _, err := BuildDeltaCheckpoint(context.Background(), dir, checkpoint, nil, 9, 1); err == nil || !strings.Contains(err.Error(), "regressed") {
		t.Fatalf("covered-sequence regression error = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := BuildDeltaCheckpoint(cancelled, dir, checkpoint, nil, 11, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled build error = %v, want context.Canceled", err)
	}
}

func TestDeltaCheckpointCapacityFailurePublishesNothing(t *testing.T) {
	dir := t.TempDir()
	_, err := buildDeltaCheckpoint(
		context.Background(),
		dir,
		nil,
		map[solana.PublicKey]deltaIndexValue{
			deltaCheckpointTestKey(90): {Entry: AccountIndexEntry{Slot: 90, FileId: 91, Offset: 8}},
			deltaCheckpointTestKey(91): {Entry: AccountIndexEntry{Slot: 91, FileId: 92, Offset: 16}},
		},
		1,
		1,
		1,
	)
	if !errors.Is(err, ErrDeltaCheckpointCapacity) {
		t.Fatalf("capacity-limited build error = %v, want %v", err, ErrDeltaCheckpointCapacity)
	}
	checkpoint, openErr := OpenLatestDeltaCheckpoint(dir)
	if openErr != nil {
		t.Fatalf("open after rejected checkpoint: %v", openErr)
	}
	if checkpoint != nil {
		_ = checkpoint.Close()
		t.Fatal("capacity-rejected checkpoint was published")
	}
}

func requireDeltaCheckpointValue(t *testing.T, checkpoint *DeltaCheckpoint, key solana.PublicKey, want deltaIndexValue) {
	t.Helper()
	got, found, err := checkpoint.Lookup(key)
	if err != nil {
		t.Fatalf("Lookup(%s): %v", key, err)
	}
	if !found {
		t.Fatalf("Lookup(%s) did not find expected value %+v", key, want)
	}
	if got != want {
		t.Fatalf("Lookup(%s) = %+v, want %+v", key, got, want)
	}
}

func requireDeltaCheckpointMissing(t *testing.T, checkpoint *DeltaCheckpoint, key solana.PublicKey) {
	t.Helper()
	got, found, err := checkpoint.Lookup(key)
	if err != nil {
		t.Fatalf("Lookup missing %s: %v", key, err)
	}
	if found {
		t.Fatalf("Lookup missing %s = %+v, found=true", key, got)
	}
}

func deltaCheckpointTestKey(discriminator byte) solana.PublicKey {
	var key solana.PublicKey
	key[0] = discriminator
	key[7] = discriminator ^ 0x5a
	key[16] = discriminator + 17
	key[31] = discriminator ^ 0xa5
	return key
}

func BenchmarkDeltaCheckpointLookupBatch(b *testing.B) {
	const keyCount = 1 << 16
	dir := b.TempDir()
	entries := make(map[solana.PublicKey]deltaIndexValue, keyCount)
	members := make([]solana.PublicKey, keyCount)
	for i := range members {
		binary.LittleEndian.PutUint64(members[i][0:8], uint64(i+1))
		binary.LittleEndian.PutUint64(members[i][16:24], uint64(i+1)^0xa5a5a5a5a5a5a5a5)
		entries[members[i]] = deltaIndexValue{Entry: AccountIndexEntry{Slot: 100, FileId: 200, Offset: uint64(i * 8)}}
	}
	checkpoint, err := BuildDeltaCheckpoint(context.Background(), dir, nil, entries, 1, 0)
	if err != nil {
		b.Fatalf("build checkpoint: %v", err)
	}
	b.Cleanup(func() { _ = checkpoint.Close() })

	// Half members and half non-members, permuted across the complete index.
	queries := make([]solana.PublicKey, keyCount)
	for i := range queries {
		queries[i] = members[(i*4051)&(keyCount-1)]
		if i&1 != 0 {
			queries[i][31] ^= 0xff
		}
	}

	b.Run("per-key-lock", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(keyCount, "keys/op")
		for range b.N {
			foundCount := 0
			for _, key := range queries {
				_, found, err := checkpoint.Lookup(key)
				if err != nil {
					b.Fatal(err)
				}
				if found {
					foundCount++
				}
			}
			if foundCount != keyCount/2 {
				b.Fatalf("found %d keys, want %d", foundCount, keyCount/2)
			}
		}
	})
	b.Run("parallel-static-batch", func(b *testing.B) {
		values := make([]deltaIndexValue, len(queries))
		found := make([]bool, len(queries))
		b.ReportAllocs()
		b.ReportMetric(keyCount, "keys/op")
		b.ResetTimer()
		for range b.N {
			if err := checkpoint.LookupBatch(context.Background(), queries, values, found); err != nil {
				b.Fatal(err)
			}
			foundCount := 0
			for _, ok := range found {
				if ok {
					foundCount++
				}
			}
			if foundCount != keyCount/2 {
				b.Fatalf("found %d keys, want %d", foundCount, keyCount/2)
			}
		}
	})

	smallQueries := queries[:16]
	b.Run("small-per-key-lock", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(smallQueries)), "keys/op")
		for range b.N {
			for _, key := range smallQueries {
				if _, _, err := checkpoint.Lookup(key); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("small-inline-batch", func(b *testing.B) {
		values := make([]deltaIndexValue, len(smallQueries))
		found := make([]bool, len(smallQueries))
		b.ReportAllocs()
		b.ReportMetric(float64(len(smallQueries)), "keys/op")
		b.ResetTimer()
		for range b.N {
			if err := checkpoint.LookupBatch(context.Background(), smallQueries, values, found); err != nil {
				b.Fatal(err)
			}
		}
	})
}
