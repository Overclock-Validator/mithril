package accountsdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"golang.org/x/sys/unix"
)

func openShardedMutableSealRetryTest(
	t *testing.T,
	maxHotKeys uint64,
	initial time.Duration,
	maximum time.Duration,
	builder shardedMutableCheckpointBuilder,
) *ShardedMutableAccountIndex {
	t.Helper()
	directory := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(directory); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, directory, nil, nil)
	config.MaxHotKeys = maxHotKeys
	config.MaxHotBytes = maxHotKeys * config.BytesPerKey
	config.SealKeys = 1
	idx := openShardedMutableForTest(t, config)
	idx.stateMu.Lock()
	idx.sealRetryInitial = initial
	idx.sealRetryMaximum = maximum
	idx.buildCheckpoint = builder
	idx.stateMu.Unlock()
	t.Cleanup(func() { _ = idx.Close() })
	return idx
}

func transientSealTestError(attempt int32) error {
	return &os.PathError{
		Op:   "sync",
		Path: fmt.Sprintf("injected-checkpoint-%d", attempt),
		Err:  unix.ENOSPC,
	}
}

func leaveUnreclaimableCheckpointPartial(directory string, generation uint64) error {
	// Cleanup deliberately refuses to follow or unlink a symlink masquerading as
	// a builder-owned partial. That gives these tests a deterministic reclamation
	// failure without relying on filesystem permissions or privileged execution.
	return os.Symlink(os.DevNull, makeDeltaCheckpointPaths(directory, generation).indexPartial)
}

func requireFatalSealCleanupStopsRetries(
	t *testing.T,
	idx *ShardedMutableAccountIndex,
	attempts *atomic.Int32,
) {
	t.Helper()
	stats := waitForShardedMutableRetryStats(t, idx, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.FatalError != ""
	})
	if !strings.Contains(stats.FatalError, errShardedMutableCandidateCleanup.Error()) {
		t.Fatalf("fatal seal error = %q, want candidate cleanup failure", stats.FatalError)
	}
	idx.stateMu.RLock()
	poison := idx.poison
	idx.stateMu.RUnlock()
	if !errors.Is(poison, errShardedMutableCandidateCleanup) {
		t.Fatalf("mutable poison = %v, want candidate cleanup sentinel", poison)
	}

	// Once reclamation is uncertain, maintenance must not retry and strand one
	// more candidate on every backoff interval.
	for range 100 {
		idx.wakeMaintenanceLoop()
	}
	time.Sleep(50 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("seal attempts after cleanup failure = %d, want 1", got)
	}
}

func TestShardedMutableRetryableBuildCleanupFailurePoisonsWithoutRetry(t *testing.T) {
	var attempts atomic.Int32
	builder := func(
		_ context.Context,
		directory string,
		_ *DeltaCheckpoint,
		_ map[solana.PublicKey]deltaIndexValue,
		_ uint64,
		_ int,
	) (*DeltaCheckpoint, error) {
		attempt := attempts.Add(1)
		if err := leaveUnreclaimableCheckpointPartial(directory, 999); err != nil {
			return nil, err
		}
		return nil, transientSealTestError(attempt)
	}
	idx := openShardedMutableSealRetryTest(t, 8, 2*time.Millisecond, 2*time.Millisecond, builder)
	key := shardedMutableTestKey(0, 91)
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(key, AccountIndexEntry{Slot: 91}),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	requireFatalSealCleanupStopsRetries(t, idx, &attempts)
}

func TestShardedMutableRetryablePublicationCleanupFailurePoisonsWithoutRetry(t *testing.T) {
	var attempts atomic.Int32
	directory := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(directory); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, directory, nil, nil)
	config.SealKeys = 1
	config.Callbacks.PublishCheckpoint = func(
		_ context.Context,
		publication ShardedMutableCheckpointPublication,
	) error {
		attempt := attempts.Add(1)
		checkpoint := publication.Next.Checkpoint()
		if checkpoint == nil {
			return errors.New("test checkpoint handle unexpectedly released")
		}
		if err := leaveUnreclaimableCheckpointPartial(publication.Directory, checkpoint.Generation()); err != nil {
			return err
		}
		return fmt.Errorf("injected root race %d: %w", attempt, ErrIndexGenerationRejected)
	}
	idx := openShardedMutableForTest(t, config)
	idx.stateMu.Lock()
	idx.sealRetryInitial = 2 * time.Millisecond
	idx.sealRetryMaximum = 2 * time.Millisecond
	idx.stateMu.Unlock()
	t.Cleanup(func() { _ = idx.Close() })

	key := shardedMutableTestKey(0, 92)
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(key, AccountIndexEntry{Slot: 92}),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	requireFatalSealCleanupStopsRetries(t, idx, &attempts)
}

func TestShardedMutableSizeFailureDoesNotUnlinkCandidateAfterDrainFailure(t *testing.T) {
	var attempts atomic.Int32
	var builtGeneration atomic.Uint64
	builder := func(
		ctx context.Context,
		directory string,
		old *DeltaCheckpoint,
		frozen map[solana.PublicKey]deltaIndexValue,
		coveredSequence uint64,
		workers int,
	) (*DeltaCheckpoint, error) {
		attempts.Add(1)
		checkpoint, err := BuildDeltaCheckpoint(
			ctx, directory, old, frozen, coveredSequence, workers,
		)
		if err != nil {
			return nil, err
		}
		builtGeneration.Store(checkpoint.Generation())

		// Unmap an alias while retaining the original slice in checkpoint. Its
		// eventual Close deterministically attempts a second munmap and fails.
		// Generation zero then drives the post-build size-validation branch.
		staleMapping := checkpoint.records
		if err := staleMapping.Unmap(); err != nil {
			_ = checkpoint.Close()
			return nil, err
		}
		checkpoint.generation = 0
		return checkpoint, nil
	}
	idx := openShardedMutableSealRetryTest(t, 8, 2*time.Millisecond, 2*time.Millisecond, builder)
	key := shardedMutableTestKey(0, 93)
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(key, AccountIndexEntry{Slot: 93}),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	waitForShardedMutableRetryStats(t, idx, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.FatalError != ""
	})
	idx.stateMu.RLock()
	poison := idx.poison
	idx.stateMu.RUnlock()
	if !errors.Is(poison, errShardedMutableCandidateDrain) {
		t.Fatalf("mutable poison = %v, want candidate drain sentinel", poison)
	}
	for range 100 {
		idx.wakeMaintenanceLoop()
	}
	time.Sleep(50 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("seal attempts after candidate drain failure = %d, want 1", got)
	}

	generation := builtGeneration.Load()
	if generation == 0 {
		t.Fatal("builder did not record its physical generation")
	}
	paths := makeDeltaCheckpointPaths(ShardedMutableCheckpointDirectory(idx.config.Directory, 0), generation)
	for _, path := range []string{paths.index, paths.records, paths.descriptor} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("candidate artifact %s was removed after failed mmap drain: %v", path, err)
		}
	}
}

func TestShardedMutableFatalPublicationDrainsCandidateBeforeCloseReturns(t *testing.T) {
	directory := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(directory); err != nil {
		t.Fatal(err)
	}
	candidatePinned := make(chan struct{})
	releaseCandidate := make(chan struct{})
	config := shardedMutableTestConfig(t, directory, nil, nil)
	config.SealKeys = 1
	config.Callbacks.PublishCheckpoint = func(
		_ context.Context,
		publication ShardedMutableCheckpointPublication,
	) error {
		checkpoint := publication.Next.Checkpoint()
		if checkpoint == nil {
			return errors.New("test checkpoint handle unexpectedly released")
		}
		// Hold an active checkpoint reader beyond the callback return. Final Close
		// must wait on this lock; it gives the test a deterministic candidate mmap
		// drain instead of relying on scheduler timing around munmap.
		checkpoint.mu.RLock()
		close(candidatePinned)
		go func() {
			<-releaseCandidate
			checkpoint.mu.RUnlock()
		}()
		return ErrInvalidRootIndexCatalog
	}
	idx := openShardedMutableForTest(t, config)
	cleanupReleased := false
	defer func() {
		if !cleanupReleased {
			close(releaseCandidate)
		}
		_ = idx.Close()
	}()

	key := shardedMutableTestKey(0, 94)
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(key, AccountIndexEntry{Slot: 94}),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-candidatePinned:
	case <-time.After(5 * time.Second):
		t.Fatal("fatal publication did not pin its candidate checkpoint")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- idx.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("mutable Close returned before rejected candidate drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseCandidate)
	cleanupReleased = true
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("mutable Close after candidate drain: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mutable Close did not finish after rejected candidate drained")
	}
}

func TestShardedMutableSealBackoffIsBoundedExponential(t *testing.T) {
	idx := &ShardedMutableAccountIndex{
		sealRetryInitial: time.Second,
		sealRetryMaximum: 5 * time.Second,
	}
	shard := &shardedMutableShard{}
	now := time.Unix(1_700_000_000, 0)
	for attempt, want := range []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		5 * time.Second,
		5 * time.Second,
	} {
		idx.recordShardSealFailureLocked(shard, now)
		if got := shard.sealRetryAt.Sub(now); got != want {
			t.Fatalf("retry delay after failure %d = %s, want %s", attempt+1, got, want)
		}
	}
	idx.resetShardSealBackoffLocked(shard)
	if shard.sealFailures != 0 || !shard.sealRetryAt.IsZero() {
		t.Fatalf("reset retained seal retry state: %+v", shard)
	}
}

func TestShardedMutableTransientSealRetriesSameFrozenEpochUntilSuccess(t *testing.T) {
	const failures = int32(3)
	key := shardedMutableTestKey(0, 1)
	want := deltaIndexValue{Entry: AccountIndexEntry{Slot: 7, FileId: 8, Offset: 16}}
	var attempts atomic.Int32
	builder := func(
		ctx context.Context,
		directory string,
		old *DeltaCheckpoint,
		frozen map[solana.PublicKey]deltaIndexValue,
		coveredSequence uint64,
		workers int,
	) (*DeltaCheckpoint, error) {
		attempt := attempts.Add(1)
		if old != nil || coveredSequence != 1 || len(frozen) != 1 || frozen[key] != want {
			return nil, fmt.Errorf(
				"frozen epoch changed on attempt %d: old=%p covered=%d frozen=%v",
				attempt,
				old,
				coveredSequence,
				frozen,
			)
		}
		if attempt <= failures {
			return nil, transientSealTestError(attempt)
		}
		return BuildDeltaCheckpoint(ctx, directory, old, frozen, coveredSequence, workers)
	}
	idx := openShardedMutableSealRetryTest(t, 8, 5*time.Millisecond, 20*time.Millisecond, builder)
	if err := idx.Apply([]deltaIndexMutation{liveDeltaMutation(key, want.Entry)}, nil, true); err != nil {
		t.Fatal(err)
	}
	stats := waitForShardedMutableRetryStats(t, idx, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.SealCount == 1 && stats.HotKeys == 0
	})
	if got := attempts.Load(); got != failures+1 {
		t.Fatalf("seal attempts = %d, want %d", got, failures+1)
	}
	if stats.MaintenanceErrors != uint64(failures) {
		t.Fatalf("maintenance errors = %d, want %d", stats.MaintenanceErrors, failures)
	}
	if shard := stats.Shards[0]; shard.SealFailures != 0 || shard.SealRetryPending || shard.LastSealError != "" {
		t.Fatalf("successful seal retained retry state: %+v", shard)
	}
	requireShardedMutableValue(t, idx, key, want)
}

func TestShardedMutableStaleCheckpointPublicationRetries(t *testing.T) {
	var publications atomic.Int32
	directory := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(directory); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, directory, nil, nil)
	config.SealKeys = 1
	config.Callbacks.PublishCheckpoint = func(
		context.Context,
		ShardedMutableCheckpointPublication,
	) error {
		if publications.Add(1) == 1 {
			return fmt.Errorf("injected root race: %w", ErrIndexGenerationRejected)
		}
		return nil
	}
	idx := openShardedMutableForTest(t, config)
	idx.stateMu.Lock()
	idx.sealRetryInitial = 5 * time.Millisecond
	idx.sealRetryMaximum = 5 * time.Millisecond
	idx.stateMu.Unlock()
	t.Cleanup(func() { _ = idx.Close() })

	key := shardedMutableTestKey(0, 1)
	want := deltaIndexValue{Entry: AccountIndexEntry{Slot: 1}}
	if err := idx.Apply([]deltaIndexMutation{liveDeltaMutation(key, want.Entry)}, nil, true); err != nil {
		t.Fatal(err)
	}
	waitForShardedMutableRetryStats(t, idx, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.SealCount == 1 && stats.HotKeys == 0
	})
	if got := publications.Load(); got != 2 {
		t.Fatalf("checkpoint publications = %d, want 2", got)
	}
	requireShardedMutableValue(t, idx, key, want)
	entries, err := os.ReadDir(ShardedMutableCheckpointDirectory(directory, 0))
	if err != nil {
		t.Fatal(err)
	}
	recognized := 0
	for _, entry := range entries {
		if _, _, _, ok := parseDeltaCheckpointArtifactName(entry.Name()); ok {
			recognized++
		}
	}
	if recognized != 3 {
		t.Fatalf("recognized checkpoint artifacts after stale retry = %d, want one three-file generation", recognized)
	}
}

func TestShardedMutablePersistentSealFailureBackpressuresWithoutBusyLoop(t *testing.T) {
	var attempts atomic.Int32
	var active atomic.Int32
	var maximumActive atomic.Int32
	firstFailed := make(chan struct{})
	builder := func(
		context.Context,
		string,
		*DeltaCheckpoint,
		map[solana.PublicKey]deltaIndexValue,
		uint64,
		int,
	) (*DeltaCheckpoint, error) {
		current := active.Add(1)
		for {
			prior := maximumActive.Load()
			if current <= prior || maximumActive.CompareAndSwap(prior, current) {
				break
			}
		}
		attempt := attempts.Add(1)
		active.Add(-1)
		if attempt == 1 {
			close(firstFailed)
		}
		return nil, transientSealTestError(attempt)
	}
	idx := openShardedMutableSealRetryTest(t, 1, 100*time.Millisecond, 100*time.Millisecond, builder)
	firstKey := shardedMutableTestKey(0, 1)
	firstValue := deltaIndexValue{Entry: AccountIndexEntry{Slot: 1}}
	if err := idx.Apply([]deltaIndexMutation{liveDeltaMutation(firstKey, firstValue.Entry)}, nil, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstFailed:
	case <-time.After(5 * time.Second):
		t.Fatal("initial seal attempt did not fail")
	}
	waitForShardedMutableRetryStats(t, idx, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.Shards[0].SealRetryPending
	})

	started := time.Now()
	err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(shardedMutableTestKey(1, 2), AccountIndexEntry{Slot: 2}),
	}, nil, true)
	if !errors.Is(err, ErrShardedMutableCapacity) {
		t.Fatalf("capacity write error = %v, want ErrShardedMutableCapacity", err)
	}
	if !strings.Contains(err.Error(), "checkpoint failure") || !strings.Contains(err.Error(), "no space left") {
		t.Fatalf("capacity write did not report pending seal failure: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 50*time.Millisecond {
		t.Fatalf("capacity write blocked through retry sleep: %s", elapsed)
	}
	requireShardedMutableValue(t, idx, firstKey, firstValue)

	// Repeated wakeups share the one maintenance timer and cannot bypass the
	// deadline or create overlapping builders.
	for i := 0; i < 100; i++ {
		idx.wakeMaintenanceLoop()
	}
	time.Sleep(350 * time.Millisecond)
	gotAttempts := attempts.Load()
	if gotAttempts < 3 || gotAttempts > 5 {
		t.Fatalf("persistent retry attempts in 350ms = %d, want [3,5]", gotAttempts)
	}
	if got := maximumActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent seal builders = %d, want 1", got)
	}
}

func TestShardedMutableCloseCancelsActiveSealRetry(t *testing.T) {
	var attempts atomic.Int32
	retryEntered := make(chan struct{})
	builder := func(
		ctx context.Context,
		_ string,
		_ *DeltaCheckpoint,
		_ map[solana.PublicKey]deltaIndexValue,
		_ uint64,
		_ int,
	) (*DeltaCheckpoint, error) {
		if attempts.Add(1) == 1 {
			return nil, transientSealTestError(1)
		}
		close(retryEntered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	idx := openShardedMutableSealRetryTest(t, 4, 5*time.Millisecond, 5*time.Millisecond, builder)
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(shardedMutableTestKey(0, 1), AccountIndexEntry{Slot: 1}),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-retryEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("seal retry did not start")
	}
	started := time.Now()
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("close did not cancel active seal retry promptly: %s", elapsed)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("seal attempts after close = %d, want 2", got)
	}
}

func TestShardedMutableFatalSealErrorPoisonsWithoutRetry(t *testing.T) {
	var attempts atomic.Int32
	builder := func(
		context.Context,
		string,
		*DeltaCheckpoint,
		map[solana.PublicKey]deltaIndexValue,
		uint64,
		int,
	) (*DeltaCheckpoint, error) {
		attempts.Add(1)
		return nil, fmt.Errorf("injected corruption: %w", ErrInvalidDeltaCheckpoint)
	}
	idx := openShardedMutableSealRetryTest(t, 4, 5*time.Millisecond, 10*time.Millisecond, builder)
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(shardedMutableTestKey(0, 1), AccountIndexEntry{Slot: 1}),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	stats := waitForShardedMutableRetryStats(t, idx, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.FatalError != ""
	})
	if shard := stats.Shards[0]; shard.SealRetryPending {
		t.Fatalf("fatal seal scheduled a retry: %+v", shard)
	}
	time.Sleep(40 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("fatal seal attempts = %d, want 1", got)
	}
	err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(shardedMutableTestKey(1, 2), AccountIndexEntry{Slot: 2}),
	}, nil, true)
	if !errors.Is(err, ErrInvalidDeltaCheckpoint) {
		t.Fatalf("write after fatal seal = %v, want invalid checkpoint poison", err)
	}
}

func TestShardedMutableFatalSealErrorClassification(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		fatal bool
	}{
		{name: "nil"},
		{name: "no space", err: unix.ENOSPC},
		{name: "quota", err: unix.EDQUOT},
		{name: "transient io", err: unix.EIO},
		{name: "stale compare and swap", err: ErrIndexGenerationRejected},
		{name: "cancellation", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "unknown", err: errors.New("unknown checkpoint failure"), fatal: true},
		{name: "poison", err: ErrProductionAccountIndexPoisoned, fatal: true},
		{name: "root catalog", err: ErrInvalidRootIndexCatalog, fatal: true},
		{name: "catalog artifact", err: ErrInvalidCatalogArtifact, fatal: true},
		{name: "extent catalog", err: ErrInvalidExtentCatalog, fatal: true},
		{name: "base locator", err: ErrInvalidBaseLocator, fatal: true},
		{name: "stream base", err: ErrInvalidShardedStreamBase, fatal: true},
		{name: "stream manifest", err: ErrInvalidStreamIndexManifest, fatal: true},
		{name: "delta checkpoint", err: ErrInvalidDeltaCheckpoint, fatal: true},
		{name: "delta capacity", err: ErrDeltaCheckpointCapacity, fatal: true},
		{
			name:  "delta checkpoint commit decided with io failure",
			err:   errors.Join(ErrDeltaCheckpointCommitDecided, unix.EIO),
			fatal: true,
		},
		{name: "released generation", err: ErrIndexResourceReleased, fatal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wrapped := test.err
			if wrapped != nil {
				wrapped = fmt.Errorf("outer context: %w", wrapped)
			}
			if got := isFatalShardedMutableSealError(wrapped); got != test.fatal {
				t.Fatalf("isFatalShardedMutableSealError(%v) = %t, want %t", test.err, got, test.fatal)
			}
		})
	}
}
