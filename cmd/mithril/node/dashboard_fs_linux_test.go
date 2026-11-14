package node

import (
	"syscall"
	"testing"
)

func TestFilesystemBlockSize(t *testing.T) {
	stat := syscall.Statfs_t{Bsize: 4096, Frsize: 1024}
	if got := filesystemBlockSize(stat); got != 1024 {
		t.Fatalf("fragment size: got %d", got)
	}
	stat.Frsize = 0
	if got := filesystemBlockSize(stat); got != 4096 {
		t.Fatalf("fallback block size: got %d", got)
	}
	stat.Frsize = -1
	if got := filesystemBlockSize(stat); got != 0 {
		t.Fatalf("invalid fragment size: got %d", got)
	}
}

func TestFilesystemFromStatBounds(t *testing.T) {
	stat := syscall.Statfs_t{Bsize: 4096, Frsize: 1024, Blocks: 3, Bfree: 2, Bavail: 1}
	if got := filesystemFromStat(stat); got == nil || got.TotalBytes != 3072 || got.AvailableBytes != 1024 || got.UsedBytes == nil || *got.UsedBytes != 1024 {
		t.Fatalf("fragment accounting: %+v", got)
	}
	stat.Bfree, stat.Bavail = 3, 2
	if got := filesystemFromStat(stat); got == nil || got.UsedBytes == nil || *got.UsedBytes != 0 {
		t.Fatalf("reserved space counted as used: %+v", got)
	}
	stat.Bfree = 4
	if filesystemFromStat(stat) != nil {
		t.Fatal("free blocks exceed total blocks")
	}
	stat.Bfree, stat.Bavail = 1, 2
	if filesystemFromStat(stat) != nil {
		t.Fatal("available blocks exceed free blocks")
	}
	stat.Bfree, stat.Bavail = 2, 1
	stat.Blocks = ^uint64(0)
	if filesystemFromStat(stat) != nil {
		t.Fatal("overflowing total must be unavailable")
	}
	stat.Blocks, stat.Bavail = 3, ^uint64(0)
	if filesystemFromStat(stat) != nil {
		t.Fatal("overflowing available space must be unavailable")
	}
	stat.Frsize = -1
	if filesystemFromStat(stat) != nil {
		t.Fatal("invalid unit must be unavailable")
	}
}
