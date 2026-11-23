package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/cockroachdb/pebble/v2"
	"github.com/gagliardetto/solana-go"
)

// TestPartialFlushEvidence demonstrates the shard/ingest behavior independently
// of the full snapshot builder; it does not exercise the builder's return path.
func TestPartialFlushEvidence(t *testing.T) {
	oldFlushers, oldSort := MaxConcurrentFlushers, SnapshotFlushSortWorkers
	MaxConcurrentFlushers, SnapshotFlushSortWorkers = 1, 1
	t.Cleanup(func() { MaxConcurrentFlushers, SnapshotFlushSortWorkers = oldFlushers, oldSort })
	dir := t.TempDir()
	sl := NewShardLogger(2, dir)
	var a, b solana.PublicKey
	b[0] = 255
	sl.EnqueueRequest(a, accountsdb.AccountIndexEntry{Slot: 1})
	sl.EnqueueRequest(b, accountsdb.AccountIndexEntry{Slot: 2})
	if err := os.Remove(filepath.Join(dir, "001")); err != nil {
		t.Fatal(err)
	}
	p := progress.NewIndexingProgress("Flush (shard logs)")
	p.Start(2)
	if err := closeSnapshotShardLogger(context.Background(), sl, p); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing shard error not propagated: %v", err)
	}
	db, err := ingestSSTFiles(filepath.Join(t.TempDir(), "index"), dir)
	if err != nil {
		t.Fatalf("partial SST ingestion failed, so this fixture did not reproduce the concern: %v", err)
	}
	defer db.Close()
	if _, closer, err := db.Get(a[:]); err != nil {
		t.Fatalf("first shard absent: %v", err)
	} else {
		closer.Close()
	}
	if _, closer, err := db.Get(b[:]); err == nil {
		closer.Close()
		t.Fatal("missing shard unexpectedly present")
	} else if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("failed to check missing shard: %v", err)
	}
}
