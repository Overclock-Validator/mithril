package accountsdb

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
)

// A fileId encodes both the disk and the file holding an append-vec:
//
//	shard   = fileId % N
//	segment = fileId / N
//
// Segment 0 is the coalesced snapshot big file ("data", holding both the full and
// incremental snapshots); any higher segment is a runtime append-vec written as
// "<slot>.<fileId>" in the shard dir. Offsets stored in the index are absolute
// within the resolved file.
const segData = 0

// Shards resolves append-vec file ids onto one directory per physical disk and
// mints new ids for runtime writes.
type Shards struct {
	dirs    []string // one per disk, each is "<mount>/accounts"
	counter atomic.Uint64
	rr      atomic.Uint64
}

func newShards(dirs []string) *Shards {
	return &Shards{dirs: dirs}
}

func (s *Shards) n() uint64 { return uint64(len(s.dirs)) }

// choose round-robins runtime writes across shards. Runtime append-vec volume is
// low, so an even file-count spread is enough; bulk unpack placement lives in the
// build-time writer.
func (s *Shards) choose() int {
	return int(s.rr.Add(1) % s.n())
}

// mint allocates a fresh runtime fileId (segment >= 1) on shard. The counter starts
// at 0, so the first minted id lands in segment 1, just past the segment-0 big file.
func (s *Shards) mint(shard int) uint64 {
	return s.counter.Add(1)*s.n() + uint64(shard)
}

// path returns the on-disk file holding the data addressed by (slot, fileId).
func (s *Shards) path(slot, fileId uint64) string {
	dir := s.dirs[fileId%s.n()]
	if fileId/s.n() == segData {
		return filepath.Join(dir, "data")
	}
	return filepath.Join(dir, fmt.Sprintf("%d.%d", slot, fileId))
}

// appendVecPath keeps legacy stores readable and resolves coalesced snapshot data.
func (db *AccountsDb) appendVecPath(slot, fileId uint64) string {
	if db.Shards != nil {
		return db.Shards.path(slot, fileId)
	}
	return filepath.Join(db.AcctsDir, fmt.Sprintf("%d.%d", slot, fileId))
}

func (db *AccountsDb) chooseShard() int {
	if db.Shards == nil {
		return 0
	}
	return db.Shards.choose()
}

// nextFileId shares the durable high-water mark with fold and compaction.
// Fold segments stay on the primary disk alongside their recovery manifests.
func (db *AccountsDb) nextFileId(shard int) uint64 {
	n := uint64(1)
	if db.Shards != nil {
		n = db.Shards.n()
	}
	for {
		prev := db.LargestFileId.Load()
		next := (prev/n+1)*n + uint64(shard)
		if db.LargestFileId.CompareAndSwap(prev, next) {
			return next
		}
	}
}
