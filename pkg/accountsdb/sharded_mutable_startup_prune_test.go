package accountsdb

import (
	"context"
	"errors"
	"testing"
)

func TestShardedMutableStartupPrunesCrashSurvivingRetirements(t *testing.T) {
	dir := t.TempDir()
	if err := InitializeShardedMutableAccountIndex(dir); err != nil {
		t.Fatal(err)
	}
	writerConfig := shardedMutableTestConfig(t, dir, nil, nil)
	writer := openShardedMutableForTest(t, writerConfig)
	if err := writer.Apply([]deltaIndexMutation{
		retireDeltaMutation(101, 1),
		retireDeltaMutation(102, 2),
		retireDeltaMutation(103, 3),
		retireDeltaMutation(104, 4),
	}, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	// Model a crash after the new bases covering sequence 1 became durable but
	// before the ordinary post-publication prune. Four markers exceed this
	// deliberately tight restart budget, so an opener that retains them fails.
	restartConfig := shardedMutableTestConfig(t, dir, []uint64{1, 1, 1, 1}, nil)
	restartConfig.MaxHotBytes = restartConfig.BytesPerKey
	_, err := OpenShardedMutableAccountIndex(restartConfig)
	if !errors.Is(err, ErrShardedMutableCapacity) {
		t.Fatalf("open without startup prune error = %v, want capacity", err)
	}

	restartConfig.StartupRetirementPruneThrough = 1
	restarted := openShardedMutableForTest(t, restartConfig)
	stats := restarted.Stats()
	if stats.RetiredAppendVecs != 0 || stats.HotBytes != 0 || stats.RewriteCount != 1 {
		t.Fatalf("startup-pruned stats = %+v", stats)
	}
	for fileID := uint64(1); fileID <= 4; fileID++ {
		if restarted.IsRetired(100+fileID, fileID) {
			t.Fatalf("safe retirement %d remained visible", fileID)
		}
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}

	// The prune is a crash-atomic WAL rewrite, not merely an in-memory filter.
	restartConfig.StartupRetirementPruneThrough = 0
	reopened := openShardedMutableForTest(t, restartConfig)
	t.Cleanup(func() { _ = reopened.Close() })
	if stats := reopened.Stats(); stats.RetiredAppendVecs != 0 || stats.HotBytes != 0 {
		t.Fatalf("durably pruned restart stats = %+v", stats)
	}
	if err := reopened.CompactJournal(context.Background()); err != nil {
		t.Fatal(err)
	}
}
