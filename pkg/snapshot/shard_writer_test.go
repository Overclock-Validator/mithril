package snapshot

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestShardWriterReadback verifies that bytes appended to a shardWriter can be read
// back at the base offset it returned, for both buffered and O_DIRECT modes and
// including a record larger than the staging buffer. Set TMPDIR to a filesystem
// supporting O_DIRECT to exercise direct mode; buffered mode always runs.
func TestShardWriterReadback(t *testing.T) {
	dir := t.TempDir()

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
	shardDirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}

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

// TestShardBigFilesRespectsFreeSpace gives one directory 3 MiB of room above
// the reserve and the other plenty. Records must stop landing on the small one
// once its room is used, and a build whose every disk is at its reserve must
// still write rather than fail early.
func TestShardBigFilesRespectsFreeSpace(t *testing.T) {
	small, large := t.TempDir(), t.TempDir()
	const reserve = uint64(64 << 20)
	capacity := map[string]uint64{small: reserve + 3<<20, large: 1 << 40}
	oldAvail, oldReserve, oldInterval := shardAvailableBytes, SnapshotShardMinFreeBytes, shardFreeRefreshInterval
	t.Cleanup(func() {
		shardAvailableBytes, SnapshotShardMinFreeBytes, shardFreeRefreshInterval = oldAvail, oldReserve, oldInterval
	})
	// Like statfs: bytes already written to the data file are no longer free.
	shardAvailableBytes = func(dir string) (uint64, error) {
		used := uint64(0)
		if info, err := os.Stat(filepath.Join(dir, "data")); err == nil {
			used = uint64(info.Size())
		}
		if used > capacity[dir] {
			return 0, nil
		}
		return capacity[dir] - used, nil
	}
	SnapshotShardMinFreeBytes = reserve
	shardFreeRefreshInterval = time.Millisecond

	sb, err := openShardBigFiles([]string{small, large})
	if err != nil {
		t.Fatal(err)
	}
	record := make([]byte, 256<<10)
	perShard := map[uint64]int{}
	for i := 0; i < 200; i++ {
		fileId, _, err := sb.write(record)
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		perShard[fileId]++
		if i%10 == 0 {
			time.Sleep(2 * time.Millisecond) // let refreshes interleave with writes
		}
	}
	if err := sb.close(); err != nil {
		t.Fatal(err)
	}
	// 3 MiB of room is 12 records; one more may be in flight when it runs out.
	if perShard[0] > 13 || perShard[1] == 0 {
		t.Fatalf("small shard took %d records, large took %d", perShard[0], perShard[1])
	}

	// Both directories below the reserve (unknown to capacity, so 0 bytes free):
	// placement falls back instead of failing.
	sb, err = openShardBigFiles([]string{t.TempDir(), t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := sb.write(record); err != nil {
		t.Fatalf("write with every shard at its reserve: %v", err)
	}
	if err := sb.close(); err != nil {
		t.Fatal(err)
	}
}
