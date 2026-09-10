package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

func shardedMutableTestKey(shard, ordinal byte) solana.PublicKey {
	router, err := NewPersistentIndexShardRouter(4, testPersistentIndexRoutingKey())
	if err != nil {
		panic(err)
	}
	return shardedMutableTestKeyForRouter(router, uint32(shard), uint64(ordinal))
}

func shardedMutableTestKeyForRouter(
	router PersistentIndexShardRouter,
	shard uint32,
	ordinal uint64,
) solana.PublicKey {
	if shard >= uint32(router.Count()) {
		panic("accountsdb test: requested shard is out of range")
	}
	for nonce := uint64(0); ; nonce++ {
		var key solana.PublicKey
		binary.LittleEndian.PutUint32(key[0:4], shard)
		binary.LittleEndian.PutUint64(key[8:16], ordinal)
		binary.LittleEndian.PutUint64(key[24:32], nonce)
		if router.Shard(key) == shard {
			return key
		}
	}
}

func shardedMutableTestConfig(
	t *testing.T,
	dir string,
	covered []uint64,
	checkpoints []*ShardedDeltaCheckpointHandle,
) ShardedMutableIndexConfig {
	t.Helper()
	router, err := NewPersistentIndexShardRouter(4, testPersistentIndexRoutingKey())
	if err != nil {
		t.Fatal(err)
	}
	if covered == nil {
		covered = make([]uint64, router.Count())
	}
	if checkpoints == nil {
		checkpoints = make([]*ShardedDeltaCheckpointHandle, router.Count())
	}
	return ShardedMutableIndexConfig{
		Directory:           dir,
		Router:              router,
		CoveredSequences:    covered,
		InitialCheckpoints:  checkpoints,
		MaxHotKeys:          1024,
		MaxHotBytes:         1024 * DefaultShardedMutableBytesPerKey,
		BytesPerKey:         DefaultShardedMutableBytesPerKey,
		BytesPerRetired:     DefaultShardedMutableBytesPerRetired,
		SealKeys:            1024,
		SealMaxAge:          time.Hour,
		RebaseKeys:          1 << 62,
		JournalRewriteBytes: 1 << 62,
		CheckpointWorkers:   1,
		MaxConcurrentSeals:  1,
		Callbacks: ShardedMutableIndexCallbacks{
			PublishCheckpoint: func(context.Context, ShardedMutableCheckpointPublication) error { return nil },
			RequestRebase:     func(context.Context, ShardedMutableRebaseRequest) error { return nil },
		},
	}
}

func TestShardedMutableDirectConfigDefaultsToProductionSealAge(t *testing.T) {
	router, err := NewPersistentIndexShardRouter(4, testPersistentIndexRoutingKey())
	if err != nil {
		t.Fatal(err)
	}
	config, err := normalizeShardedMutableConfig(ShardedMutableIndexConfig{
		Directory: t.TempDir(),
		Router:    router,
		Callbacks: ShardedMutableIndexCallbacks{
			PublishCheckpoint: func(context.Context, ShardedMutableCheckpointPublication) error { return nil },
			RequestRebase:     func(context.Context, ShardedMutableRebaseRequest) error { return nil },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.SealMaxAge != DefaultProductionAccountIndexSealMaxAge {
		t.Fatalf("direct mutable seal age = %s, want production default %s", config.SealMaxAge, DefaultProductionAccountIndexSealMaxAge)
	}
	if config.SealMaxAge != 30*time.Second {
		t.Fatalf("direct mutable seal age = %s, want 30s", config.SealMaxAge)
	}
	wantConcurrent := min(DefaultShardedMutableMaxConcurrentSeals(), router.Count())
	if config.MaxConcurrentSeals != wantConcurrent {
		t.Fatalf("direct mutable concurrent seals = %d, want %d", config.MaxConcurrentSeals, wantConcurrent)
	}
}

func openShardedMutableForTest(t *testing.T, config ShardedMutableIndexConfig) *ShardedMutableAccountIndex {
	t.Helper()
	idx, err := OpenShardedMutableAccountIndex(config)
	if err != nil {
		t.Fatalf("open sharded mutable index: %v", err)
	}
	return idx
}

func TestOpenShardedMutableRejectsSymlinkedJournalWithoutMutatingTarget(t *testing.T) {
	externalDir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(externalDir); err != nil {
		t.Fatal(err)
	}
	externalJournal := filepath.Join(externalDir, ShardedDeltaIndexJournalFileName)
	before, err := os.ReadFile(externalJournal)
	if err != nil {
		t.Fatal(err)
	}

	storeDir := t.TempDir()
	journalPath := filepath.Join(storeDir, ShardedDeltaIndexJournalFileName)
	if err := os.Symlink(externalJournal, journalPath); err != nil {
		t.Fatal(err)
	}
	_, err = OpenShardedMutableAccountIndex(shardedMutableTestConfig(t, storeDir, nil, nil))
	if err == nil {
		t.Fatal("opened a sharded mutable journal through a symlink")
	}
	after, readErr := os.ReadFile(externalJournal)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("rejected symlink open mutated the external journal target")
	}
	if info, statErr := os.Lstat(journalPath); statErr != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("rejected journal symlink was changed: info=%v err=%v", info, statErr)
	}
}

func requireShardedMutableValue(
	t *testing.T,
	idx *ShardedMutableAccountIndex,
	key solana.PublicKey,
	want deltaIndexValue,
) {
	t.Helper()
	got, found, err := idx.LookupWithError(key)
	if err != nil {
		t.Fatalf("lookup %s: %v", key, err)
	}
	if !found || got != want {
		t.Fatalf("lookup %s = (%+v,%t), want (%+v,true)", key, got, found, want)
	}
}

func TestShardedMutableApplyReplayAndGlobalMetadata(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	idx := openShardedMutableForTest(t, config)
	key0, key3 := shardedMutableTestKey(0, 1), shardedMutableTestKey(3, 2)
	want0 := deltaIndexValue{Entry: AccountIndexEntry{Slot: 10, FileId: 20, Offset: 32}}
	want3 := deltaIndexValue{Tombstone: true}
	wantMeta := foldMeta{BatchSeq: 9, ThroughSlot: 99, FileId: 7}
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(key0, want0.Entry),
		tombstoneDeltaMutation(key3),
		retireDeltaMutation(8, 12),
	}, &wantMeta, true); err != nil {
		t.Fatal(err)
	}
	requireShardedMutableValue(t, idx, key0, want0)
	requireShardedMutableValue(t, idx, key3, want3)
	if !idx.IsRetired(8, 12) {
		t.Fatal("retirement was not visible")
	}
	if got, ok := idx.ReadFoldMeta(); !ok || got != wantMeta {
		t.Fatalf("fold meta = (%+v,%t), want (%+v,true)", got, ok, wantMeta)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openShardedMutableForTest(t, config)
	t.Cleanup(func() { _ = reopened.Close() })
	requireShardedMutableValue(t, reopened, key0, want0)
	requireShardedMutableValue(t, reopened, key3, want3)
	if !reopened.IsRetired(8, 12) {
		t.Fatal("retirement did not survive replay")
	}
	if got, ok := reopened.ReadFoldMeta(); !ok || got != wantMeta {
		t.Fatalf("replayed fold meta = (%+v,%t), want (%+v,true)", got, ok, wantMeta)
	}
}

func TestShardedMutableRejectsNonDurableAndOversizedAtomicFrameBeforeWAL(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	config.MaxHotKeys = 2
	config.MaxHotBytes = 2 * config.BytesPerKey
	idx := openShardedMutableForTest(t, config)
	t.Cleanup(func() { _ = idx.Close() })
	key := shardedMutableTestKey(0, 1)
	if err := idx.Apply([]deltaIndexMutation{liveDeltaMutation(key, AccountIndexEntry{})}, nil, false); !errors.Is(err, ErrShardedMutableDurability) {
		t.Fatalf("non-durable Apply error = %v", err)
	}
	mutations := make([]deltaIndexMutation, 3)
	for i := range mutations {
		mutations[i] = liveDeltaMutation(shardedMutableTestKey(byte(i), byte(i+1)), AccountIndexEntry{Slot: uint64(i)})
	}
	if err := idx.Apply(mutations, nil, true); !errors.Is(err, ErrShardedMutableCapacity) {
		t.Fatalf("oversized Apply error = %v", err)
	}
	stats := idx.Stats()
	if stats.JournalSequence != 0 || stats.JournalBytes != deltaJournalHeaderSize {
		t.Fatalf("rejected frame changed WAL: sequence=%d bytes=%d", stats.JournalSequence, stats.JournalBytes)
	}
}

func TestShardedMutableFrameFootprintIsReusedWithoutProjectionAllocations(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	idx := openShardedMutableForTest(t, config)
	t.Cleanup(func() { _ = idx.Close() })

	existingKey := shardedMutableTestKey(0, 1)
	newKey := shardedMutableTestKey(1, 2)
	existingRetirement := retiredAppendVec{Slot: 10, FileID: 20}
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(existingKey, AccountIndexEntry{Slot: 1}),
		retireDeltaMutation(existingRetirement.Slot, existingRetirement.FileID),
	}, nil, true); err != nil {
		t.Fatal(err)
	}

	mutations := []deltaIndexMutation{
		liveDeltaMutation(existingKey, AccountIndexEntry{Slot: 2}),
		tombstoneDeltaMutation(existingKey),
		liveDeltaMutation(newKey, AccountIndexEntry{Slot: 3}),
		liveDeltaMutation(newKey, AccountIndexEntry{Slot: 4}),
		retireDeltaMutation(existingRetirement.Slot, existingRetirement.FileID),
		retireDeltaMutation(existingRetirement.Slot, existingRetirement.FileID),
	}
	footprint, err := idx.frameFootprint(mutations)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(footprint.accountKeys); got != 2 {
		t.Fatalf("unique account keys = %d, want 2", got)
	}
	if got := len(footprint.retirements); got != 1 {
		t.Fatalf("unique retirements = %d, want 1", got)
	}
	wantBytes := 2*config.BytesPerKey + config.BytesPerRetired
	if footprint.bytes != wantBytes {
		t.Fatalf("footprint bytes = %d, want %d", footprint.bytes, wantBytes)
	}

	idx.stateMu.RLock()
	addKeys, addBytes := idx.projectedGrowthLocked(footprint)
	allocations := testing.AllocsPerRun(100, func() {
		idx.projectedGrowthLocked(footprint)
	})
	idx.stateMu.RUnlock()
	if addKeys != 1 || addBytes != config.BytesPerKey {
		t.Fatalf("projected growth = %d keys/%d bytes, want 1/%d", addKeys, addBytes, config.BytesPerKey)
	}
	if allocations != 0 {
		t.Fatalf("prepared footprint projection allocated %.2f objects per call, want 0", allocations)
	}
}

func BenchmarkShardedMutableFramePlanning(b *testing.B) {
	router, err := NewPersistentIndexShardRouter(64, testPersistentIndexRoutingKey())
	if err != nil {
		b.Fatal(err)
	}
	const uniqueKeys = 30_000
	idx := &ShardedMutableAccountIndex{
		config: ShardedMutableIndexConfig{
			MaxHotKeys:      uniqueKeys * 2,
			MaxHotBytes:     uniqueKeys * 2 * DefaultShardedMutableBytesPerKey,
			BytesPerKey:     DefaultShardedMutableBytesPerKey,
			BytesPerRetired: DefaultShardedMutableBytesPerRetired,
		},
		router:  router,
		shards:  make([]shardedMutableShard, router.Count()),
		retired: make(map[retiredAppendVec]uint64),
	}
	for shard := range idx.shards {
		idx.shards[shard].active = make(map[solana.PublicKey]deltaIndexValue)
	}
	mutations := make([]deltaIndexMutation, 0, uniqueKeys+uniqueKeys/8)
	for ordinal := 0; ordinal < uniqueKeys; ordinal++ {
		var key solana.PublicKey
		binary.LittleEndian.PutUint64(key[:8], uint64(ordinal+1))
		mutation := liveDeltaMutation(key, AccountIndexEntry{Slot: uint64(ordinal + 1)})
		mutations = append(mutations, mutation)
		if ordinal%8 == 0 {
			mutations = append(mutations, mutation)
		}
		if ordinal%2 == 0 {
			idx.shards[idx.router.Shard(key)].active[key] = mutation.Value
		}
	}

	b.Run("prepare-and-project-30k-keys", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			footprint, err := idx.frameFootprint(mutations)
			if err != nil {
				b.Fatal(err)
			}
			keys, bytes := idx.projectedGrowthLocked(footprint)
			if keys != uniqueKeys/2 || bytes != keys*idx.config.BytesPerKey {
				b.Fatalf("projection = %d/%d", keys, bytes)
			}
		}
	})

	footprint, err := idx.frameFootprint(mutations)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("reproject-prepared-30k-keys", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			keys, bytes := idx.projectedGrowthLocked(footprint)
			if keys != uniqueKeys/2 || bytes != keys*idx.config.BytesPerKey {
				b.Fatalf("projection = %d/%d", keys, bytes)
			}
		}
	})
}

func TestShardedMutableFrameIsAtomicallyVisibleAcrossShards(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	idx := openShardedMutableForTest(t, config)
	t.Cleanup(func() { _ = idx.Close() })
	keys := []solana.PublicKey{shardedMutableTestKey(0, 1), shardedMutableTestKey(3, 1)}
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(keys[0], AccountIndexEntry{}),
		liveDeltaMutation(keys[1], AccountIndexEntry{}),
	}, nil, true); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mismatch atomic.Bool
	var readers sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			values := make([]deltaIndexValue, 2)
			found := make([]bool, 2)
			for ctx.Err() == nil {
				if err := idx.LookupBatch(ctx, keys, values, found); err != nil {
					if !errors.Is(err, context.Canceled) {
						mismatch.Store(true)
					}
					return
				}
				if !found[0] || !found[1] || values[0].Entry.Slot != values[1].Entry.Slot {
					mismatch.Store(true)
					return
				}
			}
		}()
	}
	for sequence := uint64(1); sequence <= 40; sequence++ {
		if err := idx.Apply([]deltaIndexMutation{
			liveDeltaMutation(keys[0], AccountIndexEntry{Slot: sequence}),
			liveDeltaMutation(keys[1], AccountIndexEntry{Slot: sequence}),
		}, nil, true); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	readers.Wait()
	if mismatch.Load() {
		t.Fatal("a reader observed only part of a multi-shard WAL frame")
	}
}

func TestShardedMutableSealPublishesBeforeDroppingFrozenAndAdvancesCleanCoverage(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	config.SealKeys = 1
	entered := make(chan ShardedMutableCheckpointPublication, 1)
	release := make(chan struct{})
	var rootHandle *ShardedDeltaCheckpointHandle
	config.Callbacks.PublishCheckpoint = func(_ context.Context, publication ShardedMutableCheckpointPublication) error {
		if err := publication.Next.Retain(); err != nil {
			return err
		}
		rootHandle = publication.Next
		entered <- publication
		<-release
		return nil
	}
	idx := openShardedMutableForTest(t, config)
	key := shardedMutableTestKey(2, 1)
	want := deltaIndexValue{Entry: AccountIndexEntry{Slot: 1, FileId: 2, Offset: 8}}
	if err := idx.Apply([]deltaIndexMutation{liveDeltaMutation(key, want.Entry)}, nil, true); err != nil {
		t.Fatal(err)
	}
	publication := <-entered
	if publication.ShardID != 2 || publication.CoveredSequence != 1 {
		t.Fatalf("publication = shard %d sequence %d", publication.ShardID, publication.CoveredSequence)
	}
	if len(publication.ProposedCoveredSequences) != 4 {
		t.Fatalf("coverage vector length = %d", len(publication.ProposedCoveredSequences))
	}
	for shard, covered := range publication.ProposedCoveredSequences {
		if covered != 1 {
			t.Fatalf("proposed clean shard %d coverage = %d, want 1", shard, covered)
		}
	}
	// The callback has not committed: coverage is old and frozen RAM remains
	// both visible and non-blocking.
	for shard, covered := range idx.CoveredSequences() {
		if covered != 0 {
			t.Fatalf("coverage committed before callback for shard %d: %d", shard, covered)
		}
	}
	requireShardedMutableValue(t, idx, key, want)
	close(release)
	if err := idx.ForceSeal(context.Background()); err != nil {
		t.Fatal(err)
	}
	for shard, covered := range idx.CoveredSequences() {
		if covered != 1 {
			t.Fatalf("committed coverage for shard %d = %d, want 1", shard, covered)
		}
	}
	requireShardedMutableValue(t, idx, key, want)
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	// The root's retained owner keeps the shared mapping usable after the
	// mutable owner closes; no second mmap is required.
	got, found, err := rootHandle.Checkpoint().Lookup(key)
	if err != nil || !found || got != want {
		t.Fatalf("root handle lookup after mutable close = (%+v,%t,%v)", got, found, err)
	}
	if err := rootHandle.Release(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-rootHandle.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("shared checkpoint did not close after final owner")
	}
}

func TestShardedMutableBackpressuresOnlyWhileSealPublicationProgresses(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	config.MaxHotKeys = 2
	config.MaxHotBytes = 2 * config.BytesPerKey
	config.SealKeys = 100
	entered := make(chan struct{})
	release := make(chan struct{})
	config.Callbacks.PublishCheckpoint = func(context.Context, ShardedMutableCheckpointPublication) error {
		close(entered)
		<-release
		return nil
	}
	idx := openShardedMutableForTest(t, config)
	defer idx.Close()
	first := []deltaIndexMutation{
		liveDeltaMutation(shardedMutableTestKey(0, 1), AccountIndexEntry{Slot: 1}),
		liveDeltaMutation(shardedMutableTestKey(0, 2), AccountIndexEntry{Slot: 1}),
	}
	if err := idx.Apply(first, nil, true); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- idx.Apply([]deltaIndexMutation{
			liveDeltaMutation(shardedMutableTestKey(1, 3), AccountIndexEntry{Slot: 2}),
		}, nil, true)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("capacity pressure did not start a seal")
	}
	select {
	case err := <-done:
		t.Fatalf("writer escaped bounded backpressure before publication: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not resume after frozen memory was released")
	}
}

func TestShardedMutableUniform1024ShardPressureUsesBoundedConcurrentSeals(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	router, err := NewPersistentIndexShardRouter(1024, testPersistentIndexRoutingKey())
	if err != nil {
		t.Fatal(err)
	}
	const (
		hotLimit        = uint64(64)
		concurrentSeals = 8
	)
	config := ShardedMutableIndexConfig{
		Directory:            dir,
		Router:               router,
		CoveredSequences:     make([]uint64, router.Count()),
		InitialCheckpoints:   make([]*ShardedDeltaCheckpointHandle, router.Count()),
		MaxHotKeys:           hotLimit,
		MaxHotBytes:          hotLimit * DefaultShardedMutableBytesPerKey,
		BytesPerKey:          DefaultShardedMutableBytesPerKey,
		BytesPerRetired:      DefaultShardedMutableBytesPerRetired,
		SealKeys:             DefaultShardedMutableSealKeys,
		SealMaxAge:           time.Hour,
		RebaseKeys:           1 << 62,
		JournalRewriteBytes:  1 << 62,
		CheckpointWorkers:    concurrentSeals,
		MaxConcurrentSeals:   concurrentSeals,
		MaxConcurrentRebases: 1,
	}
	entered := make(chan uint32, concurrentSeals)
	release := make(chan struct{})
	var released atomic.Bool
	var activeCallbacks atomic.Int64
	var maximumCallbacks atomic.Int64
	var publications atomic.Uint64
	config.Callbacks.PublishCheckpoint = func(
		ctx context.Context,
		publication ShardedMutableCheckpointPublication,
	) error {
		publications.Add(1)
		active := activeCallbacks.Add(1)
		defer activeCallbacks.Add(-1)
		for {
			prior := maximumCallbacks.Load()
			if active <= prior || maximumCallbacks.CompareAndSwap(prior, active) {
				break
			}
		}
		if !released.Load() {
			entered <- publication.ShardID
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	config.Callbacks.RequestRebase = func(context.Context, ShardedMutableRebaseRequest) error { return nil }
	idx := openShardedMutableForTest(t, config)
	t.Cleanup(func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
		_ = idx.Close()
	})

	mutations := make([]deltaIndexMutation, hotLimit)
	keys := make([]solana.PublicKey, hotLimit+1)
	for ordinal := uint64(0); ordinal < hotLimit; ordinal++ {
		// Sample the entire 1024-way routing range evenly. No shard reaches its
		// 8192-key threshold; only proactive global pressure can start a seal.
		shard := uint32(ordinal * (uint64(router.Count()) / hotLimit))
		keys[ordinal] = shardedMutableTestKeyForRouter(router, shard, ordinal+1)
		mutations[ordinal] = liveDeltaMutation(keys[ordinal], AccountIndexEntry{Slot: ordinal + 1})
	}
	if err := idx.Apply(mutations, nil, true); err != nil {
		t.Fatal(err)
	}
	for count := 0; count < concurrentSeals; count++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d/%d pressure seals reached publication", count, concurrentSeals)
		}
	}
	stats := idx.Stats()
	if stats.HotKeys != hotLimit || stats.SealsInProgress != concurrentSeals ||
		stats.MaxConcurrentSeals != concurrentSeals {
		t.Fatalf("bounded pressure stats = %+v", stats)
	}

	keys[hotLimit] = shardedMutableTestKeyForRouter(router, 1, hotLimit+1)
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- idx.Apply([]deltaIndexMutation{
			liveDeltaMutation(keys[hotLimit], AccountIndexEntry{Slot: hotLimit + 1}),
		}, nil, true)
	}()
	select {
	case err := <-writerDone:
		t.Fatalf("writer escaped the global hot bound before publication: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	released.Store(true)
	close(release)
	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer made no forward progress after concurrent publication")
	}
	if err := idx.ForceSeal(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats = idx.Stats()
	if stats.HotKeys != 0 || stats.SealsInProgress != 0 {
		t.Fatalf("pressure drain retained mutable generations: %+v", stats)
	}
	if stats.SealCount != hotLimit+1 || stats.RebasesInProgress != 0 || stats.MaintenanceErrors != 0 {
		t.Fatalf("pressure maintenance counters = %+v", stats)
	}
	if got := maximumCallbacks.Load(); got != concurrentSeals {
		t.Fatalf("maximum concurrent publications = %d, want %d", got, concurrentSeals)
	}
	if got := publications.Load(); got != hotLimit+1 {
		t.Fatalf("checkpoint publications = %d, want %d", got, hotLimit+1)
	}
	for ordinal, key := range keys {
		requireShardedMutableValue(t, idx, key, deltaIndexValue{
			Entry: AccountIndexEntry{Slot: uint64(ordinal + 1)},
		})
	}
}

func TestDeltaCheckpointBuildLockSerializesOnlyOneDirectory(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	unlockA, err := lockDeltaCheckpointDirectory(dirA)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if unlockA != nil {
			unlockA()
		}
	}()

	differentDone := make(chan error, 1)
	go func() {
		unlock, lockErr := lockDeltaCheckpointDirectory(dirB)
		if lockErr == nil {
			unlock()
		}
		differentDone <- lockErr
	}()
	select {
	case err := <-differentDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("different checkpoint directories serialized globally")
	}

	sameDone := make(chan error, 1)
	go func() {
		unlock, lockErr := lockDeltaCheckpointDirectory(filepath.Join(dirA, "."))
		if lockErr == nil {
			unlock()
		}
		sameDone <- lockErr
	}()
	select {
	case err := <-sameDone:
		t.Fatalf("same checkpoint directory was not serialized: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	unlockA()
	unlockA = nil
	select {
	case err := <-sameDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("same-directory checkpoint waiter did not resume")
	}
}

func TestShardedMutableAgeTriggersAsynchronousSeal(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	config.SealMaxAge = 20 * time.Millisecond
	sealed := make(chan struct{}, 1)
	config.Callbacks.PublishCheckpoint = func(context.Context, ShardedMutableCheckpointPublication) error {
		sealed <- struct{}{}
		return nil
	}
	idx := openShardedMutableForTest(t, config)
	t.Cleanup(func() { _ = idx.Close() })
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(shardedMutableTestKey(0, 1), AccountIndexEntry{Slot: 1}),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sealed:
	case <-time.After(5 * time.Second):
		t.Fatal("age threshold did not seal a dirty shard")
	}
}

func TestShardedMutableDurableFrameSurvivesProcessExitWithoutClose(t *testing.T) {
	const childEnv = "MITHRIL_SHARDED_MUTABLE_CRASH_CHILD"
	if os.Getenv(childEnv) != "" {
		dir := os.Getenv(childEnv)
		config := shardedMutableTestConfig(t, dir, nil, nil)
		idx := openShardedMutableForTest(t, config)
		key := shardedMutableTestKey(3, 44)
		if err := idx.Apply([]deltaIndexMutation{
			liveDeltaMutation(key, AccountIndexEntry{Slot: 44, FileId: 55, Offset: 64}),
		}, nil, true); err != nil {
			t.Fatal(err)
		}
		// Deliberately bypass Close: Apply's fsync is the commit boundary.
		os.Exit(0)
	}

	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestShardedMutableDurableFrameSurvivesProcessExitWithoutClose$")
	command.Env = append(os.Environ(), childEnv+"="+dir)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("crash child: %v\n%s", err, output)
	}
	idx := openShardedMutableForTest(t, shardedMutableTestConfig(t, dir, nil, nil))
	t.Cleanup(func() { _ = idx.Close() })
	requireShardedMutableValue(t, idx, shardedMutableTestKey(3, 44), deltaIndexValue{
		Entry: AccountIndexEntry{Slot: 44, FileId: 55, Offset: 64},
	})
}

func TestShardedMutableTornTailTruncatesWholeAtomicFrame(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	idx := openShardedMutableForTest(t, config)
	firstKey, secondKey := shardedMutableTestKey(0, 1), shardedMutableTestKey(1, 2)
	first := deltaIndexValue{Entry: AccountIndexEntry{Slot: 1}}
	second := deltaIndexValue{Entry: AccountIndexEntry{Slot: 2}}
	if err := idx.Apply([]deltaIndexMutation{liveDeltaMutation(firstKey, first.Entry)}, nil, true); err != nil {
		t.Fatal(err)
	}
	firstEnd := idx.Stats().JournalBytes
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(secondKey, second.Entry),
		retireDeltaMutation(20, 21),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ShardedDeltaIndexJournalFileName)
	if err := os.Truncate(path, int64(firstEnd)+deltaFrameHeaderSize+17); err != nil {
		t.Fatal(err)
	}
	reopened := openShardedMutableForTest(t, config)
	t.Cleanup(func() { _ = reopened.Close() })
	requireShardedMutableValue(t, reopened, firstKey, first)
	if _, found := reopened.Lookup(secondKey); found {
		t.Fatal("partially durable multi-mutation frame became visible")
	}
	if reopened.IsRetired(20, 21) {
		t.Fatal("retirement from partial frame became visible")
	}
	if got := reopened.Stats().JournalBytes; got != firstEnd {
		t.Fatalf("truncated journal bytes = %d, want %d", got, firstEnd)
	}
}

func TestShardedMutablePerShardCoverageSkipsOnlyCoveredMutations(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	baseConfig := shardedMutableTestConfig(t, dir, nil, nil)
	idx := openShardedMutableForTest(t, baseConfig)
	key0, key1 := shardedMutableTestKey(0, 1), shardedMutableTestKey(1, 1)
	one0 := deltaIndexValue{Entry: AccountIndexEntry{Slot: 1, FileId: 10}}
	one1 := deltaIndexValue{Entry: AccountIndexEntry{Slot: 1, FileId: 11}}
	two0 := deltaIndexValue{Entry: AccountIndexEntry{Slot: 2, FileId: 20}}
	two1 := deltaIndexValue{Entry: AccountIndexEntry{Slot: 2, FileId: 21}}
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(key0, one0.Entry), liveDeltaMutation(key1, one1.Entry),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(key0, two0.Entry), liveDeltaMutation(key1, two1.Entry),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}

	for shard := uint32(0); shard < 2; shard++ {
		if err := os.MkdirAll(ShardedMutableCheckpointDirectory(dir, shard), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cp0, err := BuildDeltaCheckpoint(context.Background(), ShardedMutableCheckpointDirectory(dir, 0), nil,
		map[solana.PublicKey]deltaIndexValue{key0: two0}, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	handle0, err := NewShardedDeltaCheckpointHandle(cp0)
	if err != nil {
		t.Fatal(err)
	}
	cp1, err := BuildDeltaCheckpoint(context.Background(), ShardedMutableCheckpointDirectory(dir, 1), nil,
		map[solana.PublicKey]deltaIndexValue{key1: one1}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	handle1, err := NewShardedDeltaCheckpointHandle(cp1)
	if err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, []uint64{2, 1, 0, 0}, []*ShardedDeltaCheckpointHandle{handle0, handle1, nil, nil})
	reopened := openShardedMutableForTest(t, config)
	requireShardedMutableValue(t, reopened, key0, two0)
	requireShardedMutableValue(t, reopened, key1, two1)
	stats := reopened.Stats()
	if stats.Shards[0].ActiveKeys != 0 || stats.Shards[1].ActiveKeys != 1 {
		t.Fatalf("replayed active keys = shard0:%d shard1:%d, want 0/1", stats.Shards[0].ActiveKeys, stats.Shards[1].ActiveKeys)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	_ = handle0.Release()
	_ = handle1.Release()
}

func TestShardedMutableSealDoesNotAdvanceAnotherDirtyShard(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	publications := make(chan ShardedMutableCheckpointPublication, 2)
	config.Callbacks.PublishCheckpoint = func(_ context.Context, publication ShardedMutableCheckpointPublication) error {
		publications <- publication
		return nil
	}
	idx := openShardedMutableForTest(t, config)
	t.Cleanup(func() { _ = idx.Close() })
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(shardedMutableTestKey(0, 1), AccountIndexEntry{Slot: 1}),
		liveDeltaMutation(shardedMutableTestKey(1, 1), AccountIndexEntry{Slot: 1}),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := idx.ForceSealShard(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	publication := <-publications
	want := []uint64{1, 0, 1, 1}
	for shard := range want {
		if publication.ProposedCoveredSequences[shard] != want[shard] {
			t.Fatalf("proposed coverage[%d] = %d, want %d", shard, publication.ProposedCoveredSequences[shard], want[shard])
		}
		if idx.CoveredSequences()[shard] != want[shard] {
			t.Fatalf("committed coverage[%d] = %d, want %d", shard, idx.CoveredSequences()[shard], want[shard])
		}
	}
	if err := idx.ForceSealShard(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
}

func TestShardedMutableSnapshotPinsCheckpointAcrossRebasePublication(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	config.SealKeys = 1
	config.RebaseKeys = 1
	rebaseEntered := make(chan ShardedMutableRebaseRequest, 1)
	releaseRebase := make(chan struct{})
	var rootHandle *ShardedDeltaCheckpointHandle
	config.Callbacks.PublishCheckpoint = func(_ context.Context, publication ShardedMutableCheckpointPublication) error {
		if err := publication.Next.Retain(); err != nil {
			return err
		}
		rootHandle = publication.Next
		return nil
	}
	config.Callbacks.RequestRebase = func(_ context.Context, request ShardedMutableRebaseRequest) error {
		rebaseEntered <- request
		<-releaseRebase
		// Simulate root publication replacing its delta resource with a base.
		return rootHandle.Release()
	}
	idx := openShardedMutableForTest(t, config)
	key := shardedMutableTestKey(0, 9)
	want := deltaIndexValue{Entry: AccountIndexEntry{Slot: 9, FileId: 8, Offset: 16}}
	if err := idx.Apply([]deltaIndexMutation{liveDeltaMutation(key, want.Entry)}, nil, true); err != nil {
		t.Fatal(err)
	}
	request := <-rebaseEntered
	if request.Checkpoint != rootHandle || request.CoveredSequence != 1 {
		t.Fatalf("rebase request does not pin published checkpoint")
	}
	snapshot, err := idx.NewSnapshot([]solana.PublicKey{key})
	if err != nil {
		t.Fatal(err)
	}
	close(releaseRebase)
	if err := idx.ForceSeal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stats := idx.Stats(); stats.Shards[0].CheckpointKeys != 0 || stats.Shards[0].Rebasing {
		t.Fatalf("checkpoint was not retired after rebase: %+v", stats.Shards[0])
	}
	got, found, err := snapshot.LookupAt(0)
	if err != nil || !found || got != want {
		t.Fatalf("pinned snapshot lookup after rebase = (%+v,%t,%v)", got, found, err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-rootHandle.Done():
		t.Fatal("checkpoint closed while lookup snapshot still pinned it")
	default:
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-rootHandle.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("rebased checkpoint did not close after snapshot released")
	}
}

func TestShardedMutableJournalCompactionPreservesRetirementSequenceForPruning(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	idx := openShardedMutableForTest(t, config)
	key := shardedMutableTestKey(2, 4)
	want := deltaIndexValue{Entry: AccountIndexEntry{Slot: 4, FileId: 5, Offset: 8}}
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(key, want.Entry), retireDeltaMutation(99, 100),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := idx.CompactJournal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sequence := idx.retired[retiredAppendVec{Slot: 99, FileID: 100}]; sequence != 1 {
		t.Fatalf("retirement sequence after rewrite = %d, want 1", sequence)
	}
	if err := idx.PruneRetiredThrough(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if idx.IsRetired(99, 100) {
		t.Fatal("eligible retirement remained visible after durable prune")
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openShardedMutableForTest(t, config)
	t.Cleanup(func() { _ = reopened.Close() })
	if reopened.IsRetired(99, 100) {
		t.Fatal("pruned retirement reappeared after recovery")
	}
	requireShardedMutableValue(t, reopened, key, want)
}

func TestShardedMutableInteriorCorruptionFailsWithoutTruncating(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	idx := openShardedMutableForTest(t, config)
	for ordinal := byte(1); ordinal <= 2; ordinal++ {
		if err := idx.Apply([]deltaIndexMutation{
			liveDeltaMutation(shardedMutableTestKey(ordinal-1, ordinal), AccountIndexEntry{Slot: uint64(ordinal)}),
		}, nil, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ShardedDeltaIndexJournalFileName)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Make the first frame claim a huge, internally consistent count/length.
	// The valid second-frame magic proves this is corruption, not a torn tail.
	var encoded [8]byte
	count := uint32(1 << 20)
	binary.LittleEndian.PutUint64(encoded[:], deltaFrameHeaderSize+uint64(count)*deltaMutationSize)
	if _, err := file.WriteAt(encoded[:], deltaJournalHeaderSize+16); err != nil {
		t.Fatal(err)
	}
	var encodedCount [4]byte
	binary.LittleEndian.PutUint32(encodedCount[:], count)
	if _, err := file.WriteAt(encodedCount[:], deltaJournalHeaderSize+56); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenShardedMutableAccountIndex(config); err == nil {
		t.Fatal("interior corruption was accepted")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("interior corruption was truncated: %d -> %d", before.Size(), after.Size())
	}
}

type shardedMutableTestPin struct{ closed atomic.Bool }

func (pin *shardedMutableTestPin) Close() error {
	pin.closed.Store(true)
	return nil
}

func TestShardedMutableBatchNewestWinsAndPinsExternalRoot(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndexAtSequence(dir, 5); err != nil {
		t.Fatal(err)
	}
	checkpointDir := ShardedMutableCheckpointDirectory(dir, 0)
	if err := os.MkdirAll(checkpointDir, 0o755); err != nil {
		t.Fatal(err)
	}
	key0 := shardedMutableTestKey(0, 1)
	key1 := shardedMutableTestKey(0, 2)
	missing := shardedMutableTestKey(0, 3)
	old := deltaIndexValue{Entry: AccountIndexEntry{Slot: 5, FileId: 5}}
	checkpoint, err := BuildDeltaCheckpoint(context.Background(), checkpointDir, nil,
		map[solana.PublicKey]deltaIndexValue{key0: old, key1: old}, 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := NewShardedDeltaCheckpointHandle(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, []uint64{5, 5, 5, 5}, []*ShardedDeltaCheckpointHandle{handle, nil, nil, nil})
	pin := &shardedMutableTestPin{}
	config.Callbacks.AcquireImmutable = func() (ShardedMutableImmutablePin, error) { return pin, nil }
	idx := openShardedMutableForTest(t, config)
	newValue := deltaIndexValue{Entry: AccountIndexEntry{Slot: 6, FileId: 6}}
	if err := idx.Apply([]deltaIndexMutation{
		tombstoneDeltaMutation(key0), liveDeltaMutation(key1, newValue.Entry),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	keys := []solana.PublicKey{key0, key1, missing, key1}
	values := make([]deltaIndexValue, len(keys))
	found := make([]bool, len(keys))
	if err := idx.LookupBatch(context.Background(), keys, values, found); err != nil {
		t.Fatal(err)
	}
	want := []deltaIndexValue{{Tombstone: true}, newValue, {}, newValue}
	wantFound := []bool{true, true, false, true}
	for i := range keys {
		if found[i] != wantFound[i] || values[i] != want[i] {
			t.Fatalf("batch[%d] = (%+v,%t), want (%+v,%t)", i, values[i], found[i], want[i], wantFound[i])
		}
	}
	if !pin.closed.Load() {
		t.Fatal("batch did not release its externally pinned immutable root")
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	_ = handle.Release()
}

func TestShardedMutableFailedRootPublicationKeepsFrozenAuthoritative(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	config.SealKeys = 1
	publishErr := errors.New("injected root fsync failure")
	called := make(chan struct{})
	config.Callbacks.PublishCheckpoint = func(context.Context, ShardedMutableCheckpointPublication) error {
		close(called)
		return publishErr
	}
	idx := openShardedMutableForTest(t, config)
	key := shardedMutableTestKey(3, 8)
	want := deltaIndexValue{Entry: AccountIndexEntry{Slot: 8}}
	if err := idx.Apply([]deltaIndexMutation{liveDeltaMutation(key, want.Entry)}, nil, true); err != nil {
		t.Fatal(err)
	}
	<-called
	deadline := time.Now().Add(5 * time.Second)
	for idx.Stats().FatalError == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if idx.Stats().FatalError == "" {
		t.Fatal("failed publication did not poison future writes")
	}
	requireShardedMutableValue(t, idx, key, want)
	if stats := idx.Stats(); stats.Shards[3].FrozenKeys != 1 {
		t.Fatalf("frozen keys after failed publication = %d, want 1", stats.Shards[3].FrozenKeys)
	}
	if err := idx.Apply([]deltaIndexMutation{tombstoneDeltaMutation(key)}, nil, true); !errors.Is(err, publishErr) {
		t.Fatalf("write after failed publication error = %v", err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	// The callback never committed a root. WAL replay alone recovers the key.
	reopened := openShardedMutableForTest(t, shardedMutableTestConfig(t, dir, nil, nil))
	t.Cleanup(func() { _ = reopened.Close() })
	requireShardedMutableValue(t, reopened, key, want)
}

func TestShardedMutableApplyRejectsDuplicateHeavyFrameBeforeEncoding(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	config.MaxHotKeys = 2
	config.MaxHotBytes = 2 * config.BytesPerKey
	idx := openShardedMutableForTest(t, config)
	t.Cleanup(func() { _ = idx.Close() })
	if maximum := idx.maxFrameMutations(); maximum != 5 {
		t.Fatalf("derived maximum frame mutations = %d, want 5", maximum)
	}
	key := shardedMutableTestKey(0, 77)
	mutations := make([]deltaIndexMutation, idx.maxFrameMutations()+1)
	for i := range mutations {
		// All mutations target one key: distinct-key accounting alone would
		// admit this adversarially duplicate-heavy frame.
		mutations[i] = liveDeltaMutation(key, AccountIndexEntry{Slot: uint64(i)})
	}
	if err := idx.Apply(mutations, nil, true); !errors.Is(err, ErrShardedMutableCapacity) {
		t.Fatalf("duplicate-heavy frame error = %v", err)
	}
	stats := idx.Stats()
	if stats.JournalSequence != 0 || stats.JournalBytes != deltaJournalHeaderSize || stats.HotKeys != 0 {
		t.Fatalf("rejected frame changed state: %+v", stats)
	}
}

func TestShardedMutableRecoveryRejectsHugeDeclaredFrameWithoutAllocationOrTruncation(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	frame, err := encodeDeltaFrame(1, nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	declaredCount := uint32((deltaMaxFrameBytes - deltaFrameHeaderSize) / deltaMutationSize)
	binary.LittleEndian.PutUint64(frame[16:24], deltaMaxFrameBytes)
	binary.LittleEndian.PutUint32(frame[56:60], declaredCount)
	path := filepath.Join(dir, ShardedDeltaIndexJournalFileName)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFullAt(file, frame, deltaJournalHeaderSize); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = OpenShardedMutableAccountIndex(shardedMutableTestConfig(t, dir, nil, nil))
	if !errors.Is(err, ErrShardedMutableCapacity) {
		t.Fatalf("huge declared recovery frame error = %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("oversize corrupt frame was mistaken for a torn tail: %d -> %d", before.Size(), after.Size())
	}
}

func TestShardedMutableRecoveryRejectsConfigOversizeCompleteFrame(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, dir, nil, nil)
	config.MaxHotKeys = 2
	config.MaxHotBytes = 2 * config.BytesPerKey
	key := shardedMutableTestKey(0, 78)
	mutations := make([]deltaIndexMutation, 6)
	for i := range mutations {
		mutations[i] = liveDeltaMutation(key, AccountIndexEntry{Slot: uint64(i)})
	}
	frame, err := encodeDeltaFrame(1, mutations, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ShardedDeltaIndexJournalFileName)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFullAt(file, frame, deltaJournalHeaderSize); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = OpenShardedMutableAccountIndex(config)
	if !errors.Is(err, ErrShardedMutableCapacity) {
		t.Fatalf("complete config-oversize recovery frame error = %v", err)
	}
}

func TestShardedMutableReplayStagingDoesNotPublishBeforeCRCAndStructureValidation(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	idx := openShardedMutableForTest(t, shardedMutableTestConfig(t, dir, nil, nil))
	t.Cleanup(func() { _ = idx.Close() })
	mutations := []deltaIndexMutation{
		liveDeltaMutation(shardedMutableTestKey(0, 80), AccountIndexEntry{Slot: 1}),
		liveDeltaMutation(shardedMutableTestKey(1, 81), AccountIndexEntry{Slot: 2}),
	}
	valid, err := encodeDeltaFrame(1, mutations, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("bad CRC after valid records", func(t *testing.T) {
		frame := append([]byte(nil), valid...)
		frame[deltaFrameHeaderSize+40] ^= 0x80
		path := filepath.Join(t.TempDir(), "frame")
		if err := os.WriteFile(path, frame, 0o644); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		_, replayErr := idx.readAndStageReplayFrame(file, deltaFrameHeaderSize, frame[:deltaFrameHeaderSize], 2, 1, false)
		_ = file.Close()
		if replayErr == nil || !bytes.Contains([]byte(replayErr.Error()), []byte("CRC mismatch")) {
			t.Fatalf("bad CRC replay error = %v", replayErr)
		}
		if idx.Stats().HotKeys != 0 {
			t.Fatal("CRC-invalid frame partially mutated RAM")
		}
	})

	t.Run("valid CRC with malformed second record", func(t *testing.T) {
		frame := append([]byte(nil), valid...)
		frame[deltaFrameHeaderSize+deltaMutationSize+57] = 1
		binary.LittleEndian.PutUint32(
			frame[60:64],
			checksumDeltaFrame(frame[:deltaFrameHeaderSize], frame[deltaFrameHeaderSize:]),
		)
		path := filepath.Join(t.TempDir(), "frame")
		if err := os.WriteFile(path, frame, 0o644); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		_, replayErr := idx.readAndStageReplayFrame(file, deltaFrameHeaderSize, frame[:deltaFrameHeaderSize], 2, 1, false)
		_ = file.Close()
		if replayErr == nil || !bytes.Contains([]byte(replayErr.Error()), []byte("reserved")) {
			t.Fatalf("malformed record replay error = %v", replayErr)
		}
		if idx.Stats().HotKeys != 0 {
			t.Fatal("structurally invalid frame partially mutated RAM")
		}
	})
}
