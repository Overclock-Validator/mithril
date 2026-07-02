package snapshot

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestShardWriterReadback verifies that bytes appended to a shardWriter can be read
// back at the base offset it returned, for both buffered and O_DIRECT modes and
// including a record larger than the staging buffer. Runs on an ext4 mount because
// O_DIRECT is not supported on tmpfs.
func TestShardWriterReadback(t *testing.T) {
	dir := "/mnt/disk0"
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("shard writer test needs an ext4 mount at %s: %v", dir, err)
	}

	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct=%v", direct), func(t *testing.T) {
			path := filepath.Join(dir, fmt.Sprintf("shardwriter_test_%v", direct))
			defer os.Remove(path)

			w, err := newShardWriter(path, direct)
			if err != nil {
				if direct {
					t.Skipf("O_DIRECT unsupported here: %v", err)
				}
				t.Fatal(err)
			}

			// varied sizes: tiny, unaligned, and one larger than the staging buffer
			sizes := []int{1, 100, 4095, 4096, 4097, 1 << 20, shardBufSize + 1234, 7, 500000}
			type rec struct {
				base uint64
				data []byte
			}
			var recs []rec
			for i, sz := range sizes {
				data := make([]byte, sz)
				for j := range data {
					data[j] = byte((i*31 + j) & 0xff)
				}
				base, err := w.append(data)
				if err != nil {
					t.Fatalf("append %d: %v", i, err)
				}
				recs = append(recs, rec{base, data})
			}
			if err := w.close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			for i, r := range recs {
				got := make([]byte, len(r.data))
				if _, err := f.ReadAt(got, int64(r.base)); err != nil {
					t.Fatalf("readAt rec %d (base=%d len=%d): %v", i, r.base, len(r.data), err)
				}
				if !bytes.Equal(got, r.data) {
					t.Fatalf("rec %d mismatch at base=%d", i, r.base)
				}
			}
		})
	}
}

// TestShardBigFilesPlacement drives shardBigFiles across three real ext4 disks in
// both buffered and O_DIRECT modes. It asserts (1) placement fans records across
// more than one shard (queued steering plus the even-split fallback never collapse
// onto shard 0) and (2) every record reads back at the path+offset its returned
// fileId/base resolve to.
func TestShardBigFilesPlacement(t *testing.T) {
	dirs := []string{"/mnt/disk0", "/mnt/disk1", "/mnt/disk2"}
	shardDirs := make([]string, len(dirs))
	for i, d := range dirs {
		shardDirs[i] = filepath.Join(d, "shardplace_test")
		if err := os.MkdirAll(shardDirs[i], 0755); err != nil {
			t.Skipf("need writable ext4 dirs (%s): %v", shardDirs[i], err)
		}
		defer os.RemoveAll(shardDirs[i])
	}

	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct=%v", direct), func(t *testing.T) {
			old := SnapshotDirectIO
			SnapshotDirectIO = direct
			defer func() { SnapshotDirectIO = old }()

			sb, err := openShardBigFiles(shardDirs)
			if err != nil {
				if direct {
					t.Skipf("O_DIRECT unsupported here: %v", err)
				}
				t.Fatal(err)
			}

			type rec struct {
				fileId, base uint64
				data         []byte
			}
			var recs []rec
			shardHits := map[uint64]int{}
			// Mix of sizes, including several larger than the staging buffer so the
			// writer actually issues writes (and updates queued backlog) mid-run.
			sizes := []int{1000, 40000, 500000, shardBufSize + 777, 2 << 20}
			for i := 0; i < 120; i++ {
				sz := sizes[i%len(sizes)]
				data := make([]byte, sz)
				for j := range data {
					data[j] = byte((i*7 + j) & 0xff)
				}
				fileId, base, err := sb.write(data)
				if err != nil {
					t.Fatalf("write %d: %v", i, err)
				}
				recs = append(recs, rec{fileId, base, data})
				shardHits[fileId%sb.n()]++
			}
			if err := sb.close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			if len(shardHits) < 2 {
				t.Fatalf("placement collapsed onto %d shard(s): %v", len(shardHits), shardHits)
			}
			t.Logf("direct=%v fan-out by shard: %v", direct, shardHits)

			n := sb.n()
			for i, r := range recs {
				p := filepath.Join(shardDirs[r.fileId%n], "data") // segment 0 = coalesced big file
				f, err := os.Open(p)
				if err != nil {
					t.Fatalf("open %s: %v", p, err)
				}
				got := make([]byte, len(r.data))
				_, err = f.ReadAt(got, int64(r.base))
				f.Close()
				if err != nil {
					t.Fatalf("rec %d readAt %s base=%d len=%d: %v", i, p, r.base, len(r.data), err)
				}
				if !bytes.Equal(got, r.data) {
					t.Fatalf("rec %d mismatch: shard=%d seg=%d base=%d", i, r.fileId%n, r.fileId/n, r.base)
				}
			}
		})
	}
}
