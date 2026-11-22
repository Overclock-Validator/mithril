//go:build linux || darwin

package accountsdb

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestPhysicalUsageCompleteOrUnknown(t *testing.T) {
	fixture := func(t *testing.T) string {
		t.Helper()
		p, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	limits := PhysicalUsageLimits{MaxEntries: 1000, MaxDuration: 5 * time.Second}
	t.Run("complete_two_roots_includes_dirs_empty_and_sparse", func(t *testing.T) {
		a, b := fixture(t), fixture(t)
		if err := os.Mkdir(filepath.Join(a, "accounts"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(a, "state.json"), []byte("metadata"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(b, "empty"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(filepath.Join(a, "accounts", "sparse"))
		if err != nil {
			t.Fatal(err)
		}
		if err = f.Truncate(16 << 20); err != nil {
			t.Fatal(err)
		}
		if _, err = f.WriteAt([]byte{1}, 8<<20); err != nil {
			t.Fatal(err)
		}
		f.Close()
		var expected PhysicalUsage
		for _, root := range []string{a, b} {
			err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				info, err := d.Info()
				if err != nil {
					return err
				}
				s, ok := physicalCensusSignature(info)
				if !ok {
					t.Fatal("unsupported stat")
				}
				expected.AllocatedBytes += s.blocks * 512
				if d.IsDir() {
					expected.Directories++
				} else {
					expected.RegularFiles++
					expected.LogicalFileBytes += uint64(info.Size())
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		got, reason := testedPhysicalUsage(context.Background(), []string{a, b}, limits, nil)
		if reason != "" || got.AllocatedBytes != expected.AllocatedBytes || got.LogicalFileBytes != expected.LogicalFileBytes || got.RegularFiles != expected.RegularFiles || got.Directories != expected.Directories || got.DurationNanoseconds == 0 || got.CompletedAt.Before(got.StartedAt) {
			t.Fatalf("got %+v / %s, want %+v", got, reason, expected)
		}
	})
	t.Run("atomic_replace_between_passes_withholds_every_total", func(t *testing.T) {
		root := fixture(t)
		path := filepath.Join(root, "state")
		os.WriteFile(path, []byte("old"), 0600)
		got, reason := testedPhysicalUsage(context.Background(), []string{root}, limits, func() {
			os.WriteFile(filepath.Join(root, "pending"), []byte("new"), 0600)
			if err := os.Rename(filepath.Join(root, "pending"), path); err != nil {
				t.Fatal(err)
			}
		})
		if reason != "changed_between_passes" || got != (PhysicalUsage{}) {
			t.Fatalf("partial or changed total escaped: %+v / %s", got, reason)
		}
	})
	for _, kind := range []string{"symlink", "ancestor_symlink", "hardlink", "overlap", "entry_limit", "deadline", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			root := fixture(t)
			os.WriteFile(filepath.Join(root, "file"), []byte("x"), 0600)
			roots := []string{root}
			bound := limits
			ctx := context.Background()
			switch kind {
			case "symlink":
				if err := os.Symlink("file", filepath.Join(root, "link")); err != nil {
					t.Fatal(err)
				}
			case "ancestor_symlink":
				parent := fixture(t)
				if err := os.Symlink(root, filepath.Join(parent, "link")); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(root, "child"), 0700); err != nil {
					t.Fatal(err)
				}
				roots = []string{filepath.Join(parent, "link", "child")}
			case "hardlink":
				if err := os.Link(filepath.Join(root, "file"), filepath.Join(root, "link")); err != nil {
					t.Fatal(err)
				}
			case "overlap":
				os.Mkdir(filepath.Join(root, "child"), 0700)
				roots = append(roots, filepath.Join(root, "child"))
			case "entry_limit":
				bound.MaxEntries = 1
			case "deadline":
				bound.MaxDuration = time.Nanosecond
			case "cancelled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			got, reason := testedPhysicalUsage(ctx, roots, bound, nil)
			if reason == "" || got != (PhysicalUsage{}) {
				t.Fatalf("partial or unsafe total escaped: %+v / %s", got, reason)
			}
		})
	}
}

func testedPhysicalUsage(ctx context.Context, roots []string, limits PhysicalUsageLimits, between func()) (PhysicalUsage, string) {
	hooks := &physicalCensusTestHooks{betweenPasses: between}
	if runtime.GOOS == "darwin" {
		hooks.mountIdentity = func(*os.File) (uint64, bool) { return 1, true }
	}
	return observePhysicalUsage(ctx, roots, limits, hooks)
}
func TestPhysicalUsageMountBoundary(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	os.Mkdir(filepath.Join(root, "nested"), 0700)
	hooks := &physicalCensusTestHooks{mountIdentity: func(f *os.File) (uint64, bool) {
		if f.Name() == "nested" {
			return 2, true
		}
		return 1, true
	}}
	got, reason := observePhysicalUsage(context.Background(), []string{root}, defaultPhysicalUsageLimits, hooks)
	if reason != "mount_changed" || got != (PhysicalUsage{}) {
		t.Fatalf("nested same-device mount not refused: %+v / %s", got, reason)
	}
	if runtime.GOOS == "darwin" {
		got, reason = ObservePhysicalUsage(context.Background(), []string{root}, defaultPhysicalUsageLimits)
		if reason != "unsupported_mount_identity" || got != (PhysicalUsage{}) {
			t.Fatal("unsupported native mount semantics admitted")
		}
	}
}
