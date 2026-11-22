//go:build linux

package metrics

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestCgroupLinuxFileSafety(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "value"), []byte("0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "large"), []byte(strings.Repeat("a", cgroupFileLimit+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("value", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(root, "pipe"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "directory"), 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := openCgroupDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if data, err := readCgroupFile(dir, "value"); err != nil || string(data) != "0\n" {
		t.Fatalf("value = %q, %v", data, err)
	}
	for _, name := range []string{"large", "link", "pipe", "directory", "..", "../value", "/value", "missing"} {
		start := time.Now()
		if data, err := readCgroupFile(dir, name); err == nil {
			t.Fatalf("unsafe %q returned %q", name, data)
		}
		if time.Since(start) > time.Second {
			t.Fatalf("invalid object %q blocked", name)
		}
	}
	if err := os.Symlink("directory", filepath.Join(root, "directory-link")); err != nil {
		t.Fatal(err)
	}
	if got, err := openCgroupDirectory(filepath.Join(root, "directory-link")); err == nil {
		got.Close()
		t.Fatal("followed directory symlink")
	}
	if _, err := cgroupDirectoryBinding(cgroupLocation{}, dir, dir); err == nil {
		t.Fatal("accepted ordinary filesystem as cgroup v2")
	}
	if err := os.Rename(root, root+"-old"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root + "-old"); err != nil {
			t.Error(err)
		}
	})
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "value"), []byte("999\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if data, err := readCgroupFile(dir, "value"); err != nil || string(data) != "0\n" {
		t.Fatalf("held directory redirected after replacement: %q, %v", data, err)
	}
}
