package history

import (
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRealHistoryReads measures account reads against a recorded history
// directory (a copy: Open may repair it), in the pattern of a whole-block
// query: the block's accounts read as of its parent in small calls, then the
// accounts it wrote read as of the block in one call. Set HISTORY_READ_DIR.
func TestRealHistoryReads(t *testing.T) {
	dir := os.Getenv("HISTORY_READ_DIR")
	if dir == "" {
		t.Skip("HISTORY_READ_DIR not set")
	}
	var cache int64
	if v, err := strconv.ParseInt(os.Getenv("HISTORY_READ_CACHE"), 10, 64); err == nil {
		cache = v
	}
	s, err := Open(dir, Options{ReadCacheBytes: cache})
	require.NoError(t, err)
	defer s.Close()
	from, through, ok := s.Range()
	require.True(t, ok)
	t.Logf("history %d..%d, %d segments", from, through, len(s.segments))

	// Blocks to query: slots written in the later chunks, as a real query
	// asks for recent blocks.
	type blockKeys struct {
		slot    uint64
		written [][32]byte
	}
	byslot := map[uint64]map[[32]byte]bool{}
	all := map[[32]byte]bool{}
	seg := s.segments[len(s.segments)-1]
	for _, c := range seg.chunks[max(0, len(seg.chunks)-6):] {
		require.NoError(t, c.reader.records(func(r *Record) {
			all[r.Key] = true
			for _, d := range r.Diffs {
				if byslot[d.Slot] == nil {
					byslot[d.Slot] = map[[32]byte]bool{}
				}
				byslot[d.Slot][r.Key] = true
			}
		}))
	}
	var slots []uint64
	for sl := range byslot {
		if sl > from {
			slots = append(slots, sl)
		}
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	n := 10
	if v, err := strconv.Atoi(os.Getenv("HISTORY_READ_BLOCKS")); err == nil {
		n = v
	}
	rng := rand.New(rand.NewSource(1))
	var allKeys [][32]byte
	for k := range all {
		allKeys = append(allKeys, k)
	}
	sort.Slice(allKeys, func(i, j int) bool { return lessKey(allKeys[i], allKeys[j]) })
	var blocks []blockKeys
	for i := 0; i < n && len(slots) > 0; i++ {
		sl := slots[rng.Intn(len(slots))]
		var w [][32]byte
		for k := range byslot[sl] {
			w = append(w, k)
		}
		blocks = append(blocks, blockKeys{sl, w})
	}

	var parentTime, compareTime time.Duration
	var parentKeys, compareKeys, calls int
	for _, b := range blocks {
		// Reads as of the parent: the written keys plus as many again that
		// were only read, in calls of about four keys.
		keys := append([][32]byte(nil), b.written...)
		for i := 0; i < len(b.written); i++ {
			keys = append(keys, allKeys[rng.Intn(len(allKeys))])
		}
		rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		start := time.Now()
		for i := 0; i < len(keys); i += 4 {
			_, errs, _ := s.AccountsAt(keys[i:min(i+4, len(keys))], b.slot-1)
			for _, e := range errs {
				if e != nil && e != ErrUnchanged {
					require.NoError(t, e)
				}
			}
			calls++
		}
		parentTime += time.Since(start)
		parentKeys += len(keys)
		start = time.Now()
		_, errs, _ := s.AccountsAt(b.written, b.slot)
		compareTime += time.Since(start)
		compareKeys += len(b.written)
		for _, e := range errs {
			if e != nil && e != ErrUnchanged {
				require.NoError(t, e)
			}
		}
	}
	hits, misses, size := s.ReadCacheStats()
	t.Logf("block cache: %d hits, %d misses, %.0f MiB held", hits, misses, float64(size)/(1<<20))
	t.Logf("%d blocks: parent reads %d keys in %d calls, %.1f ms a block, %.1f us a key; compare %d keys, %.1f ms a block, %.1f us a key",
		len(blocks), parentKeys, calls, float64(parentTime.Microseconds())/1e3/float64(len(blocks)), float64(parentTime.Microseconds())/float64(parentKeys),
		compareKeys, float64(compareTime.Microseconds())/1e3/float64(len(blocks)), float64(compareTime.Microseconds())/float64(compareKeys))
}

// TestRealHistoryWalks reports how many chunk lookups one account read takes
// in a recorded history (HISTORY_READ_DIR, a copy), by key kind.
func TestRealHistoryWalks(t *testing.T) {
	dir := os.Getenv("HISTORY_READ_DIR")
	if dir == "" {
		t.Skip("HISTORY_READ_DIR not set")
	}
	s, err := Open(dir, Options{ReadCacheBytes: 1 << 30})
	require.NoError(t, err)
	defer s.Close()
	seg := s.segments[len(s.segments)-1]
	last := seg.chunks[len(seg.chunks)-1]
	slot := last.from + (last.to-last.from)/2
	// Every key the last chunk holds, read as of the middle of that chunk.
	type info struct{ full, absentPrev bool }
	keys := map[[32]byte]info{}
	require.NoError(t, last.reader.records(func(r *Record) { keys[r.Key] = info{full: r.Full != nil} }))
	hist := map[int]int{}
	byKind := map[string][2]int{}
	for k, in := range keys {
		h0, m0, _ := s.ReadCacheStats()
		_, errs, _ := s.AccountsAt([][32]byte{k}, slot)
		require.True(t, errs[0] == nil || errs[0] == ErrUnchanged)
		h1, m1, _ := s.ReadCacheStats()
		n := int(h1 - h0 + m1 - m0)
		hist[n]++
		kind := "no full copy in last chunk"
		if in.full {
			kind = "full copy in last chunk"
		}
		v := byKind[kind]
		byKind[kind] = [2]int{v[0] + 1, v[1] + n}
	}
	var ns []int
	for n := range hist {
		ns = append(ns, n)
	}
	sort.Ints(ns)
	for _, n := range ns {
		t.Logf("%3d lookups: %d keys", n, hist[n])
	}
	for k, v := range byKind {
		t.Logf("%s: %d keys, %.1f lookups a key", k, v[0], float64(v[1])/float64(v[0]))
	}
	t.Logf("%d chunks in the segment", len(seg.chunks))
}

// TestRealHistoryRewrite copies a recorded history (HISTORY_READ_DIR) to
// HISTORY_REWRITE_DIR with the absent-before flag set where the account did
// not exist before the chunk, and with HISTORY_REWRITE_BLOCK-byte blocks, so
// reads of the same data can be compared across formats.
func TestRealHistoryRewrite(t *testing.T) {
	dir, out := os.Getenv("HISTORY_READ_DIR"), os.Getenv("HISTORY_REWRITE_DIR")
	if dir == "" || out == "" {
		t.Skip("HISTORY_READ_DIR or HISTORY_REWRITE_DIR not set")
	}
	if v, err := strconv.Atoi(os.Getenv("HISTORY_REWRITE_BLOCK")); err == nil {
		old := targetBlockSize
		targetBlockSize = v
		defer func() { targetBlockSize = old }()
	}
	s, err := Open(dir, Options{ReadCacheBytes: 1 << 30})
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, os.RemoveAll(out))
	flagged, copies, total := 0, 0, 0
	var inBytes, outBytes int64
	for _, seg := range s.segments {
		segOut := filepath.Join(out, filepath.Base(seg.dir))
		require.NoError(t, os.MkdirAll(segOut, 0o755))
		entries, err := os.ReadDir(seg.dir)
		require.NoError(t, err)
		for _, e := range entries {
			if filepath.Ext(e.Name()) == ".hst" || e.Name() == segmentIndexName {
				continue
			}
			b, err := os.ReadFile(filepath.Join(seg.dir, e.Name()))
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(segOut, e.Name()), b, 0o644))
		}
		since := map[[32]byte]sinceFull{}
		for _, c := range seg.chunks {
			var recs []*Record
			require.NoError(t, c.reader.records(func(r *Record) { recs = append(recs, r) }))
			for _, r := range recs {
				total++
				if c.from == seg.from() {
					if r.Full == nil {
						r.AbsentBefore = true // a segment's first record without a copy
						flagged++
					}
					since[r.Key] = since[r.Key].after(r)
					continue
				}
				// As AppendKeys decides now: flag an account that did not
				// exist, repeat the copy of one whose reads grew long.
				prev, _, err := s.AccountAt(r.Key, c.from-1)
				require.True(t, err == nil || err == ErrUnchanged, "%v", err)
				if err == nil && prev.Absent {
					r.Full = nil
					if os.Getenv("HISTORY_REWRITE_NOFLAG") == "" {
						r.AbsentBefore = true
						flagged++
					}
				} else if err == nil && r.Full == nil && since[r.Key].needsFull(prev, diffBytes(r)) {
					r.Full = prev
					copies++
				}
				since[r.Key] = since[r.Key].after(r)
			}
			st, _ := os.Stat(c.path)
			inBytes += st.Size()
			p := filepath.Join(segOut, filepath.Base(c.path))
			_, err := writeChunk(p, c.from, c.to, recs)
			require.NoError(t, err)
			st, _ = os.Stat(p)
			outBytes += st.Size()
		}
	}
	t.Logf("%d of %d records flagged absent before, %d full copies added; chunks %.1f MB -> %.1f MB", flagged, total, copies, float64(inBytes)/1e6, float64(outBytes)/1e6)
}
