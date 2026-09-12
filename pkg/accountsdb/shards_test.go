package accountsdb

import (
	"testing"
)

func TestShardsPathAddressing(t *testing.T) {
	dirs := []string{"/d0", "/d1", "/d2"}
	s := newShards(dirs)

	cases := []struct {
		fileId uint64
		want   string
	}{
		{0, "/d0/data"},  // segment 0 (coalesced big file), shard 0
		{1, "/d1/data"},  // shard 1
		{2, "/d2/data"},  // shard 2
		{3, "/d0/100.3"}, // segment 1 -> runtime per-file, shard 0
		{6, "/d0/100.6"}, // segment 2, shard 0
		{8, "/d2/100.8"}, // shard 2
		{10, "/d1/100.10"},
	}
	for _, c := range cases {
		if got := s.path(100, c.fileId); got != c.want {
			t.Errorf("path(100,%d) = %q, want %q", c.fileId, got, c.want)
		}
	}
}

func TestShardsMintEncodesShardAndSegment(t *testing.T) {
	s := newShards([]string{"/d0", "/d1", "/d2"})
	// counter left at 0 (matches OpenDb): first mint lands in segment 1.

	seen := map[uint64]bool{}
	for i := 0; i < 30; i++ {
		shard := i % 3
		id := s.mint(shard)
		if int(id%s.n()) != shard {
			t.Fatalf("mint(shard=%d) gave id=%d with id%%N=%d", shard, id, id%s.n())
		}
		if id/s.n() < 1 {
			t.Fatalf("runtime id=%d landed in the segment-0 big file", id)
		}
		if seen[id] {
			t.Fatalf("mint produced duplicate id=%d", id)
		}
		seen[id] = true
	}
}
