package accountsdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func openShardedMutableRebaseRetryTest(
	t *testing.T,
	initial time.Duration,
	maximum time.Duration,
	requestRebase func(context.Context, ShardedMutableRebaseRequest) error,
) *ShardedMutableAccountIndex {
	t.Helper()
	directory := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(directory); err != nil {
		t.Fatal(err)
	}
	config := shardedMutableTestConfig(t, directory, nil, nil)
	config.SealKeys = 1
	config.RebaseKeys = 1
	config.Callbacks.RequestRebase = requestRebase
	idx := openShardedMutableForTest(t, config)
	idx.stateMu.Lock()
	idx.rebaseRetryInitial = initial
	idx.rebaseRetryMaximum = maximum
	idx.stateMu.Unlock()
	t.Cleanup(func() { _ = idx.Close() })
	return idx
}

func waitForShardedMutableRetryStats(
	t *testing.T,
	idx *ShardedMutableAccountIndex,
	condition func(ShardedMutableAccountIndexStats) bool,
) ShardedMutableAccountIndexStats {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		stats := idx.Stats()
		if condition(stats) {
			return stats
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf("timed out waiting for sharded mutable retry state: %+v", stats)
		}
	}
}

func applyShardedMutableRetryTestMutation(
	t *testing.T,
	idx *ShardedMutableAccountIndex,
	ordinal byte,
) {
	t.Helper()
	if err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(
			shardedMutableTestKey(0, ordinal),
			AccountIndexEntry{Slot: uint64(ordinal), FileId: uint64(ordinal)},
		),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
}

func TestShardedMutableRebaseBackoffIsBoundedExponential(t *testing.T) {
	idx := &ShardedMutableAccountIndex{
		rebaseRetryInitial: time.Second,
		rebaseRetryMaximum: 5 * time.Second,
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
		idx.recordShardRebaseFailureLocked(shard, now)
		if got := shard.rebaseRetryAt.Sub(now); got != want {
			t.Fatalf("retry delay after failure %d = %s, want %s", attempt+1, got, want)
		}
	}
	idx.resetShardRebaseBackoffLocked(shard)
	if shard.rebaseFailures != 0 || !shard.rebaseRetryAt.IsZero() {
		t.Fatalf("reset retained retry state: %+v", shard)
	}
}

func TestShardedMutableTransientRebaseRetriesWithoutNewWrites(t *testing.T) {
	const failures = int32(3)
	var attempts atomic.Int32
	succeeded := make(chan struct{})
	var succeededOnce sync.Once
	idx := openShardedMutableRebaseRetryTest(
		t,
		5*time.Millisecond,
		20*time.Millisecond,
		func(context.Context, ShardedMutableRebaseRequest) error {
			attempt := attempts.Add(1)
			if attempt <= failures {
				return fmt.Errorf("injected transient rebase failure %d: %w", attempt, unix.EIO)
			}
			succeededOnce.Do(func() { close(succeeded) })
			return nil
		},
	)
	applyShardedMutableRetryTestMutation(t, idx, 1)
	select {
	case <-succeeded:
	case <-time.After(5 * time.Second):
		t.Fatal("transient rebase was never retried successfully")
	}
	stats := waitForShardedMutableRetryStats(t, idx, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.RebaseCount == 1 && stats.Shards[0].CheckpointKeys == 0
	})
	if got := attempts.Load(); got != failures+1 {
		t.Fatalf("rebase attempts = %d, want %d", got, failures+1)
	}
	if stats.MaintenanceErrors != uint64(failures) {
		t.Fatalf("maintenance errors = %d, want %d", stats.MaintenanceErrors, failures)
	}
	shard := stats.Shards[0]
	if shard.RebaseFailures != 0 || shard.RebaseRetryPending || shard.LastRebaseError != "" {
		t.Fatalf("successful rebase did not reset retry state: %+v", shard)
	}
}

func TestShardedMutableCloseCancelsPendingRebaseRetry(t *testing.T) {
	var attempts atomic.Int32
	attempted := make(chan struct{}, 1)
	idx := openShardedMutableRebaseRetryTest(
		t,
		10*time.Second,
		10*time.Second,
		func(context.Context, ShardedMutableRebaseRequest) error {
			attempts.Add(1)
			attempted <- struct{}{}
			return fmt.Errorf("injected transient rebase failure: %w", unix.EIO)
		},
	)
	applyShardedMutableRetryTestMutation(t, idx, 1)
	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatal("initial rebase did not run")
	}
	waitForShardedMutableRetryStats(t, idx, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.Shards[0].RebaseRetryPending
	})
	started := time.Now()
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("close waited for pending retry deadline: %s", elapsed)
	}
	time.Sleep(25 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("rebase attempts after close = %d, want 1", got)
	}
}

func TestShardedMutableNeverRunsConcurrentRetriesForOneShard(t *testing.T) {
	var attempts atomic.Int32
	var active atomic.Int32
	var maximumActive atomic.Int32
	entered := make(chan int32, 2)
	releaseRetry := make(chan struct{})
	idx := openShardedMutableRebaseRetryTest(
		t,
		5*time.Millisecond,
		10*time.Millisecond,
		func(context.Context, ShardedMutableRebaseRequest) error {
			current := active.Add(1)
			for {
				prior := maximumActive.Load()
				if current <= prior || maximumActive.CompareAndSwap(prior, current) {
					break
				}
			}
			attempt := attempts.Add(1)
			entered <- attempt
			if attempt == 1 {
				active.Add(-1)
				return fmt.Errorf("injected transient rebase failure: %w", unix.EIO)
			}
			<-releaseRetry
			active.Add(-1)
			return nil
		},
	)
	applyShardedMutableRetryTestMutation(t, idx, 1)
	for want := int32(1); want <= 2; want++ {
		select {
		case got := <-entered:
			if got != want {
				t.Fatalf("rebase attempt order = %d, want %d", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("rebase attempt %d did not start", want)
		}
	}
	for i := 0; i < 100; i++ {
		idx.wakeMaintenanceLoop()
	}
	time.Sleep(40 * time.Millisecond)
	if got := attempts.Load(); got != 2 {
		t.Fatalf("rebase attempts while retry callback blocked = %d, want 2", got)
	}
	if got := maximumActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent callbacks for one shard = %d, want 1", got)
	}
	close(releaseRetry)
	waitForShardedMutableRetryStats(t, idx, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.RebaseCount == 1 && stats.RebasesInProgress == 0
	})
}

func TestShardedMutableNewCheckpointResetsRebaseBackoff(t *testing.T) {
	var attempts atomic.Int32
	firstFailed := make(chan struct{}, 1)
	succeeded := make(chan struct{}, 1)
	idx := openShardedMutableRebaseRetryTest(
		t,
		10*time.Second,
		10*time.Second,
		func(context.Context, ShardedMutableRebaseRequest) error {
			if attempts.Add(1) == 1 {
				firstFailed <- struct{}{}
				return fmt.Errorf("injected transient rebase failure: %w", unix.EIO)
			}
			succeeded <- struct{}{}
			return nil
		},
	)
	applyShardedMutableRetryTestMutation(t, idx, 1)
	select {
	case <-firstFailed:
	case <-time.After(5 * time.Second):
		t.Fatal("initial rebase did not fail")
	}
	waitForShardedMutableRetryStats(t, idx, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.Shards[0].RebaseRetryPending
	})
	// A later checkpoint is concrete progress and should trigger a fresh rebase
	// immediately, rather than inherit the failed checkpoint's ten-second wait.
	applyShardedMutableRetryTestMutation(t, idx, 2)
	select {
	case <-succeeded:
	case <-time.After(5 * time.Second):
		t.Fatal("new checkpoint did not reset the old rebase backoff")
	}
	stats := waitForShardedMutableRetryStats(t, idx, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.RebaseCount == 1 && stats.Shards[0].CheckpointKeys == 0
	})
	if got := attempts.Load(); got != 2 {
		t.Fatalf("rebase attempts = %d, want 2", got)
	}
	if shard := stats.Shards[0]; shard.RebaseFailures != 0 || shard.RebaseRetryPending {
		t.Fatalf("new checkpoint/success retained retry backoff: %+v", shard)
	}
}

func TestShardedMutableFatalRebaseErrorPoisonsWithoutRetry(t *testing.T) {
	var attempts atomic.Int32
	idx := openShardedMutableRebaseRetryTest(
		t,
		5*time.Millisecond,
		10*time.Millisecond,
		func(context.Context, ShardedMutableRebaseRequest) error {
			attempts.Add(1)
			return fmt.Errorf("injected ambiguous publication: %w", ErrProductionAccountIndexPoisoned)
		},
	)
	applyShardedMutableRetryTestMutation(t, idx, 1)
	stats := waitForShardedMutableRetryStats(t, idx, func(stats ShardedMutableAccountIndexStats) bool {
		return stats.FatalError != ""
	})
	if shard := stats.Shards[0]; shard.RebaseRetryPending {
		t.Fatalf("fatal rebase scheduled a retry: %+v", shard)
	}
	time.Sleep(40 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("fatal rebase attempts = %d, want 1", got)
	}
	err := idx.Apply([]deltaIndexMutation{
		liveDeltaMutation(shardedMutableTestKey(0, 2), AccountIndexEntry{Slot: 2}),
	}, nil, true)
	if err == nil || !errors.Is(err, ErrProductionAccountIndexPoisoned) {
		t.Fatalf("write after fatal rebase error = %v, want production poison", err)
	}
}

func TestShardedMutableFatalRebaseErrorClassification(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		fatal bool
	}{
		{name: "nil", err: nil},
		{name: "transient io", err: unix.EIO},
		{name: "stale compare and swap", err: ErrIndexGenerationRejected},
		{name: "cancellation", err: context.Canceled},
		{name: "unknown", err: errors.New("unexpected library failure"), fatal: true},
		{name: "poison", err: ErrProductionAccountIndexPoisoned, fatal: true},
		{
			name:  "failed private base build cleanup",
			err:   errors.Join(unix.ENOSPC, ErrShardedStreamBaseBuildCleanup),
			fatal: true,
		},
		{name: "root catalog", err: ErrInvalidRootIndexCatalog, fatal: true},
		{name: "catalog artifact", err: ErrInvalidCatalogArtifact, fatal: true},
		{name: "extent catalog", err: ErrInvalidExtentCatalog, fatal: true},
		{name: "base locator", err: ErrInvalidBaseLocator, fatal: true},
		{name: "stream base", err: ErrInvalidShardedStreamBase, fatal: true},
		{name: "stream manifest", err: ErrInvalidStreamIndexManifest, fatal: true},
		{name: "delta checkpoint", err: ErrInvalidDeltaCheckpoint, fatal: true},
		{name: "delta checkpoint capacity", err: ErrDeltaCheckpointCapacity, fatal: true},
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
			if got := isFatalShardedMutableRebaseError(wrapped); got != test.fatal {
				t.Fatalf("isFatalShardedMutableRebaseError(%v) = %t, want %t", test.err, got, test.fatal)
			}
		})
	}
}

func TestShardedStreamBaseBuildCleanupFailureDominatesRetryableIO(t *testing.T) {
	cleanupErr := cleanupShardedStreamBaseBuildDir(
		"unused-test-build-directory",
		func(string) error { return unix.EIO },
	)
	if !errors.Is(cleanupErr, ErrShardedStreamBaseBuildCleanup) {
		t.Fatalf("cleanup error = %v, want build cleanup sentinel", cleanupErr)
	}
	if errors.Is(cleanupErr, unix.EIO) {
		t.Fatalf("cleanup error retained retryable I/O identity: %v", cleanupErr)
	}
	buildErr := errors.Join(unix.ENOSPC, cleanupErr)
	if !isFatalShardedMutableRebaseError(buildErr) {
		t.Fatalf("build plus cleanup error remained retryable: %v", buildErr)
	}
}
