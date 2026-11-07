package snapshot

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/Overclock-Validator/mithril/pkg/snapshotdl"
	"github.com/klauspost/compress/zstd"
)

// This exercises the automatic builder, not only the shard logger helper.
func TestAutoBuilderStopsBeforeIngestOnShardFailure(t *testing.T) {
	oldShards, oldFlush, oldSort := SnapshotIndexShards, MaxConcurrentFlushers, SnapshotFlushSortWorkers
	oldCopy, oldBuild, oldCommit := SnapshotAppendVecCopyingWorkers, SnapshotIndexEntryBuilderWorkers, SnapshotIndexEntryCommitterWorkers
	oldTemp, oldDecoder, oldSync := SnapshotIndexTempDir, ZstdDecoderConcurrency, dirSyncFilesystem
	SnapshotIndexShards, MaxConcurrentFlushers, SnapshotFlushSortWorkers = 1, 1, 1
	SnapshotAppendVecCopyingWorkers, SnapshotIndexEntryBuilderWorkers, SnapshotIndexEntryCommitterWorkers = 1, 1, 1
	SnapshotIndexTempDir, ZstdDecoderConcurrency = "", 1
	// The builder otherwise calls syncfs (Linux) or system-wide sync (macOS).
	dirSyncFilesystem = func(string) error { return nil }
	t.Cleanup(func() {
		SnapshotIndexShards, MaxConcurrentFlushers, SnapshotFlushSortWorkers = oldShards, oldFlush, oldSort
		SnapshotAppendVecCopyingWorkers, SnapshotIndexEntryBuilderWorkers, SnapshotIndexEntryCommitterWorkers = oldCopy, oldBuild, oldCommit
		SnapshotIndexTempDir, ZstdDecoderConcurrency, dirSyncFilesystem = oldTemp, oldDecoder, oldSync
	})

	var compressed bytes.Buffer
	zw, err := zstd.NewWriter(&compressed, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(zw)
	for _, entry := range []struct {
		name string
		data []byte
	}{{"snapshots/1/1", make([]byte, 687)}, {agaveStatusCacheArchiveMember, make([]byte, 8)}} {
		if err := tw.WriteHeader(&tar.Header{Name: entry.name, Mode: 0o600, Size: int64(len(entry.data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(entry.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	archive := compressed.Bytes()
	if len(archive) > 64<<10 {
		t.Fatalf("fixture unexpectedly large: %d bytes", len(archive))
	}

	accounts := filepath.Join(t.TempDir(), "accounts-db")
	shard := filepath.Join(accounts, "mithril_db_log_shards", "000")
	removed := make(chan error, 1)
	var incrementalGETs atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/snapshot-1-abc.tar.zst":
		case "/incremental-snapshot.tar.zst":
			w.Header().Set("Content-Disposition", `attachment; filename="incremental-snapshot-1-2-abc.tar.zst"`)
			if r.Method == http.MethodGet && incrementalGETs.Add(1) == 2 {
				removed <- os.Remove(shard)
			}
		default:
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(archive)))
		if r.Method == http.MethodGet {
			_, _ = w.Write(archive)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, _, buildErr := BuildAccountsDbAuto(ctx, server.URL+"/snapshot-1-abc.tar.zst", "", 1, 1,
		[]string{accounts}, nil, "", snapshotdl.SnapshotConfig{}, progress.NewDualProgress())
	if db != nil {
		defer db.CloseDb()
	}
	select {
	case err := <-removed:
		if err != nil {
			t.Fatalf("test did not remove its disposable shard: %v", err)
		}
	default:
		t.Fatalf("test never reached incremental archive read; builder error: %v", buildErr)
	}
	if !errors.Is(buildErr, os.ErrNotExist) || !strings.Contains(buildErr.Error(), "closing shard logger: failed to reopen file for reading") {
		t.Errorf("expected wrapped shard-close ENOENT, got %v", buildErr)
	} else {
		t.Log("builder returned the shard-reopen error wrapping ENOENT")
	}
	if _, err := os.Stat(filepath.Join(accounts, "mithril_db")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("index ingestion started after failed shard flush: %v", err)
	} else {
		t.Log("no index directory was created")
	}
}
