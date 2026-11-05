package snapshot

import (
	"archive/tar"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/Overclock-Validator/mithril/pkg/snapshotdl"
	"github.com/klauspost/compress/zstd"
)

func TestDashboardManifestFailureDoesNotPublish(t *testing.T) {
	for _, mode := range []string{"paths", "auto"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			archive := filepath.Join(dir, "snapshot-1-invalid.tar.zst")
			if err := os.WriteFile(archive, []byte("invalid snapshot"), 0600); err != nil {
				t.Fatal(err)
			}
			dp := progress.NewDualProgress()
			calls := 0
			if !dp.SetManifestObserver(func(bool, uint64, time.Time) { calls++ }) {
				t.Fatal("failed to install manifest observer")
			}
			accounts := []string{filepath.Join(dir, "db")}
			var err error
			if mode == "paths" {
				_, _, err = BuildAccountsDbPaths(context.Background(), archive, "", accounts, dp)
			} else {
				_, _, err = BuildAccountsDbAuto(context.Background(), archive, "", 0, 0, accounts, nil, "", snapshotdl.SnapshotConfig{}, dp)
			}
			if err == nil || !strings.Contains(err.Error(), "reading snapshot manifest") {
				t.Fatalf("expected manifest read error, got %v", err)
			}
			if calls != 0 {
				t.Fatalf("failed parse published %d manifest observations", calls)
			}
		})
	}
}

func TestDashboardManifestBuilderPublishesOnlySuccessfulParses(t *testing.T) {
	for _, tc := range []struct {
		name      string
		manifest  []byte
		wantCalls int
		wantError string
	}{
		{"valid-full", make([]byte, 687), 1, "reading incremental snapshot manifest"},
		{"truncated-optional", append(make([]byte, 687), 1), 0, "reading snapshot manifest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			full := filepath.Join(dir, "full.tar.zst")
			f, err := os.Create(full)
			if err != nil {
				t.Fatal(err)
			}
			z, err := zstd.NewWriter(f, zstd.WithEncoderConcurrency(1))
			if err != nil {
				t.Fatal(err)
			}
			tw := tar.NewWriter(z)
			if err := tw.WriteHeader(&tar.Header{Name: "snapshots/1/1", Mode: 0600, Size: int64(len(tc.manifest))}); err != nil {
				t.Fatal(err)
			}
			if _, err := tw.Write(tc.manifest); err != nil {
				t.Fatal(err)
			}
			if err := tw.Close(); err != nil {
				t.Fatal(err)
			}
			if err := z.Close(); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			incremental := filepath.Join(dir, "invalid.tar.zst")
			if err := os.WriteFile(incremental, []byte("invalid"), 0600); err != nil {
				t.Fatal(err)
			}
			dp := progress.NewDualProgress()
			calls := 0
			if !dp.SetManifestObserver(func(incremental bool, _ uint64, _ time.Time) {
				if incremental {
					t.Error("invalid incremental was reported as parsed")
				}
				calls++
			}) {
				t.Fatal("failed to install manifest observer")
			}
			_, _, err = BuildAccountsDbPaths(context.Background(), full, incremental, []string{filepath.Join(dir, "db")}, dp)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) || calls != tc.wantCalls {
				t.Fatalf("got error %v and %d callbacks; want %q and %d callbacks", err, calls, tc.wantError, tc.wantCalls)
			}
		})
	}
}
