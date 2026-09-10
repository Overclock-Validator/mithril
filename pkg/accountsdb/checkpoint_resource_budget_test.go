package accountsdb

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckpointResourceBudgetRejectsOversizedStartupSelection(t *testing.T) {
	selected := make([]uint64, DefaultPersistentIndexShards)
	for shardID := range selected {
		selected[shardID] = 1 << 20
	}
	_, err := newCheckpointResourceBudget(512<<20, 1<<30, selected)
	require.ErrorIs(t, err, ErrCheckpointResourceBudget)
	require.ErrorContains(t, err, "root selects")

	_, err = newCheckpointResourceBudget(math.MaxUint64, math.MaxUint64, []uint64{math.MaxUint64, 1})
	require.ErrorIs(t, err, ErrCheckpointResourceAccounting)
}

func TestCheckpointBudgetPressureRebasesLargestSubthresholdShardWithoutFreezingHotState(t *testing.T) {
	const shardCount = 64
	router, err := NewPersistentIndexShardRouter(shardCount, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	selected := make([]uint64, shardCount)
	for shardID := range selected {
		selected[shardID] = 100
	}
	reservationBytes, err := checkpointBuildReservationBytes(2)
	require.NoError(t, err)
	selectedTotal := uint64(shardCount * 100)
	// Replacing the dirty shard would need selectedTotal-old+reservation.
	// Being one byte short must trigger a rebase, not an unaccounted freeze.
	selectedLimit := selectedTotal - 100 + reservationBytes - 1
	budget, err := newCheckpointResourceBudget(
		selectedLimit,
		selectedTotal+reservationBytes+1,
		selected,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	rebaseEntered := make(chan ShardedMutableRebaseRequest, 1)
	idx := &ShardedMutableAccountIndex{
		config: ShardedMutableIndexConfig{
			Router:               router,
			RebaseKeys:           100,
			MaxConcurrentSeals:   1,
			MaxConcurrentRebases: 1,
			checkpointBudget:     budget,
		},
		router:          router,
		shards:          make([]shardedMutableShard, shardCount),
		progress:        make(chan struct{}),
		wakeMaintenance: make(chan struct{}, 1),
		ctx:             ctx,
		cancel:          cancel,
		retired:         make(map[retiredAppendVec]uint64),
	}
	idx.config.Callbacks.RequestRebase = func(
		ctx context.Context,
		request ShardedMutableRebaseRequest,
	) error {
		rebaseEntered <- request
		<-ctx.Done()
		return ctx.Err()
	}
	for shardID := range idx.shards {
		checkpoint := &DeltaCheckpoint{recordCount: 1, coveredSeq: 1}
		handle, handleErr := NewShardedDeltaCheckpointHandle(checkpoint)
		require.NoError(t, handleErr)
		idx.shards[shardID] = shardedMutableShard{
			active:         make(map[solana.PublicKey]deltaIndexValue),
			coveredSeq:     1,
			baseCoveredSeq: 0,
			checkpoint:     handle,
		}
	}
	dirtyShard := uint32(shardCount - 1)
	dirtyKey := shardedMutableTestKeyForRouter(router, dirtyShard, 1)
	idx.shards[dirtyShard].active[dirtyKey] = deltaIndexValue{Entry: AccountIndexEntry{Slot: 1}}
	idx.hotKeys = 1
	idx.hotBytes = DefaultShardedMutableBytesPerKey

	idx.stateMu.Lock()
	scheduled := idx.maybeScheduleSealLocked(time.Now(), true, nil)
	idx.stateMu.Unlock()
	require.True(t, scheduled)

	select {
	case request := <-rebaseEntered:
		assert.Equal(t, uint32(0), request.ShardID, "equal-size checkpoints use deterministic shard order")
		assert.Less(t, request.Checkpoint.Checkpoint().Len(), idx.config.RebaseKeys)
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint pressure did not schedule a subthreshold rebase")
	}
	idx.stateMu.RLock()
	assert.Len(t, idx.shards[dirtyShard].active, 1)
	assert.Empty(t, idx.shards[dirtyShard].frozen)
	assert.Nil(t, idx.shards[dirtyShard].sealReservation)
	assert.Equal(t, uint64(1), idx.checkpointPressureRebases)
	idx.stateMu.RUnlock()
	stats := budget.snapshot()
	assert.Zero(t, stats.BuildReservedBytes)
	assert.Equal(t, uint64(1), stats.ReservationRejects)
	require.NoError(t, idx.Close())
}

func TestRetirementFrontierRebasesSubthresholdCheckpoint(t *testing.T) {
	router, err := NewPersistentIndexShardRouter(2, testPersistentIndexRoutingKey())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan ShardedMutableRebaseRequest, 1)
	idx := &ShardedMutableAccountIndex{
		config: ShardedMutableIndexConfig{
			Router:               router,
			RebaseKeys:           1_000,
			MaxConcurrentSeals:   1,
			MaxConcurrentRebases: 1,
			Callbacks: ShardedMutableIndexCallbacks{
				RequestRebase: func(ctx context.Context, request ShardedMutableRebaseRequest) error {
					entered <- request
					<-ctx.Done()
					return ctx.Err()
				},
			},
		},
		router:          router,
		shards:          make([]shardedMutableShard, router.Count()),
		progress:        make(chan struct{}),
		wakeMaintenance: make(chan struct{}, 1),
		ctx:             ctx,
		cancel:          cancel,
		retired:         map[retiredAppendVec]uint64{{Slot: 9, FileID: 10}: 1},
	}
	for shardID := range idx.shards {
		handle, handleErr := NewShardedDeltaCheckpointHandle(&DeltaCheckpoint{recordCount: 1, coveredSeq: 1})
		require.NoError(t, handleErr)
		idx.shards[shardID] = shardedMutableShard{
			active:         make(map[solana.PublicKey]deltaIndexValue),
			coveredSeq:     1,
			baseCoveredSeq: 0,
			checkpoint:     handle,
		}
	}
	idx.stateMu.Lock()
	require.True(t, idx.maybeScheduleRebasesLocked())
	idx.stateMu.Unlock()
	select {
	case request := <-entered:
		assert.Equal(t, uint32(0), request.ShardID)
		assert.Less(t, request.Checkpoint.Checkpoint().Len(), idx.config.RebaseKeys)
	case <-time.After(5 * time.Second):
		t.Fatal("retirement marker did not advance the base frontier")
	}
	require.NoError(t, idx.Close())
}

func TestShardedMutableCloseReleasesInFlightCheckpointReservation(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, InitializeShardedMutableAccountIndex(directory))
	config := shardedMutableTestConfig(t, directory, nil, nil)
	config.SealKeys = 1
	budget, err := newCheckpointResourceBudget(16<<20, 16<<20, make([]uint64, config.Router.Count()))
	require.NoError(t, err)
	config.checkpointBudget = budget
	idx := openShardedMutableForTest(t, config)

	buildEntered := make(chan struct{})
	idx.stateMu.Lock()
	idx.buildCheckpoint = func(
		ctx context.Context,
		_ string,
		_ *DeltaCheckpoint,
		_ map[solana.PublicKey]deltaIndexValue,
		_ uint64,
		_ int,
	) (*DeltaCheckpoint, error) {
		close(buildEntered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	idx.stateMu.Unlock()

	key := shardedMutableTestKey(0, 1)
	require.NoError(t, idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(key, AccountIndexEntry{Slot: 1}),
	}, nil, true))
	select {
	case <-buildEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint build did not start")
	}
	assert.Positive(t, budget.snapshot().BuildReservedBytes)
	require.NoError(t, idx.Close())
	assert.Zero(t, budget.snapshot().BuildReservedBytes)
}

func TestCheckpointResourceBudgetReservesGlobalSelectedGrowth(t *testing.T) {
	budget, err := newCheckpointResourceBudget(150, 1_000, []uint64{40, 40})
	require.NoError(t, err)

	first, err := budget.reserveBuild(0, 80)
	require.NoError(t, err)
	_, err = budget.reserveBuild(1, 80)
	require.ErrorIs(t, err, ErrCheckpointResourceBudget)

	stats := budget.snapshot()
	assert.Equal(t, uint64(80), stats.SelectedBytes)
	assert.Equal(t, uint64(80), stats.BuildReservedBytes)
	assert.Equal(t, uint64(160), stats.PhysicalBytes)
	assert.Equal(t, uint64(1), stats.ReservationRejects)

	changed := budget.changedChannel()
	budget.releaseReservation(first)
	select {
	case <-changed:
	default:
		t.Fatal("reservation release did not wake budget waiters")
	}
	second, err := budget.reserveBuild(1, 80)
	require.NoError(t, err)
	budget.releaseReservation(second)
	assert.Zero(t, budget.snapshot().BuildReservedBytes)
}

func TestCheckpointResourceBudgetTransfersAndPinsObsoleteBytes(t *testing.T) {
	budget, err := newCheckpointResourceBudget(250, 250, []uint64{100})
	require.NoError(t, err)

	_, err = budget.reserveBuild(0, 151)
	require.ErrorIs(t, err, ErrCheckpointResourceBudget)
	reservation, err := budget.reserveBuild(0, 150)
	require.NoError(t, err)
	require.ErrorIs(t, budget.validatePublication(reservation, 0, 151), ErrCheckpointResourceAccounting)
	require.NoError(t, budget.validatePublication(reservation, 0, 80))
	assert.Equal(t, uint64(100), budget.commitPublication(reservation))

	stats := budget.snapshot()
	assert.Equal(t, uint64(80), stats.SelectedBytes)
	assert.Zero(t, stats.BuildReservedBytes)
	assert.Equal(t, uint64(100), stats.ObsoleteBytes)
	assert.Equal(t, uint64(180), stats.PhysicalBytes)
	assert.Equal(t, uint64(250), stats.PhysicalHighWaterBytes)

	require.NoError(t, budget.releaseObsolete(100))
	stats = budget.snapshot()
	assert.Zero(t, stats.ObsoleteBytes)
	assert.Equal(t, uint64(80), stats.PhysicalBytes)
}

func TestCheckpointResourceBudgetRebaseKeepsBytesPhysicalUntilRelease(t *testing.T) {
	budget, err := newCheckpointResourceBudget(500, 500, []uint64{90, 10})
	require.NoError(t, err)
	require.NoError(t, budget.validateRebase(0, 90))
	assert.Equal(t, uint64(90), budget.commitRebase(0))

	stats := budget.snapshot()
	assert.Equal(t, uint64(10), stats.SelectedBytes)
	assert.Equal(t, uint64(90), stats.ObsoleteBytes)
	assert.Equal(t, uint64(100), stats.PhysicalBytes)
	require.NoError(t, budget.releaseObsolete(90))
	assert.Equal(t, uint64(10), budget.snapshot().PhysicalBytes)
}

func TestCheckpointResourceBudgetArithmeticFailsClosed(t *testing.T) {
	_, err := checkpointBuildReservationBytes(math.MaxUint64)
	require.ErrorIs(t, err, ErrCheckpointResourceAccounting)
	_, err = checkedCheckpointSum(math.MaxUint64, 1)
	require.ErrorIs(t, err, ErrCheckpointResourceAccounting)

	catalog := &RootIndexCatalog{Shards: []IndexCatalogShard{{
		DeltaGeneration: 1,
		DeltaIndex:      IndexCatalogArtifact{Size: math.MaxUint64},
		DeltaRecords:    IndexCatalogArtifact{Size: 1},
	}}}
	_, err = checkpointSelectedBytesFromCatalog(catalog)
	require.True(t, errors.Is(err, ErrCheckpointResourceAccounting))
}

func TestProductionCheckpointBudgetFailsClosedWhenRootSelectionExceedsLimit(t *testing.T) {
	config := productionIndexTestConfig()
	root, _ := initializeProductionIndexFixture(t, config)
	catalog, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	catalog.Generation++
	for shardID := range catalog.Shards {
		indexArtifact := catalogTestArtifact("fake-delta-"+string(rune('a'+shardID))+".stmh", byte(shardID+1))
		recordArtifact := catalogTestArtifact("fake-delta-"+string(rune('a'+shardID))+".rec", byte(shardID+11))
		indexArtifact.Size = 1 << 20
		recordArtifact.Size = 1 << 20
		catalog.Shards[shardID].DeltaGeneration = 1
		catalog.Shards[shardID].DeltaCoveredSequence = catalog.Shards[shardID].BaseCoveredSequence
		catalog.Shards[shardID].DeltaIndex = indexArtifact
		catalog.Shards[shardID].DeltaRecords = recordArtifact
	}
	renamed, err := writeRootIndexCatalogAtomic(root, catalog)
	require.NoError(t, err)
	require.True(t, renamed)

	config.MaxHotKeys = 1
	config.MaxHotBytes = DefaultShardedMutableBytesPerKey
	config.SealKeys = 1
	config.RebaseKeys = 1
	config.MaxCheckpointSelectedBytes = 5 << 20
	config.MaxCheckpointPhysicalBytes = 10 << 20
	_, err = OpenProductionAccountIndex(root, config)
	require.ErrorIs(t, err, ErrCheckpointResourceBudget)
}

func TestProductionWriteRootCatalogInvokesPublisherExactlyOnce(t *testing.T) {
	calls := 0
	index := &ProductionAccountIndex{
		root: "unused-test-root",
		publishRoot: func(root string, catalog *RootIndexCatalog) (bool, error) {
			calls++
			assert.Equal(t, "unused-test-root", root)
			require.NotNil(t, catalog)
			return true, nil
		},
	}
	renamed, err := index.writeRootCatalog(&RootIndexCatalog{})
	require.NoError(t, err)
	assert.True(t, renamed)
	assert.Equal(t, 1, calls)
}
