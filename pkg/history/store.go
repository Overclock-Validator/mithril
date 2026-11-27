package history

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Defaults: a segment is 64 chunks (8,192 slots at 128-slot folds) and the
// store keeps about a day of closed segments at today's slot rate.
const (
	DefaultSegmentChunks = 64
	DefaultKeepSegments  = 48
	maxSegmentChunks     = 64 // chunk membership is a 64-bit mask
)

var (
	// ErrBeforeHistory: the slot is older than the oldest retained chunk.
	ErrBeforeHistory = errors.New("history: slot is older than retained history")
	// ErrAfterHistory: the slot is newer than the last chunk written; read
	// current state instead.
	ErrAfterHistory = errors.New("history: slot is newer than recorded history")
	// ErrUnchanged: history records no change to the account after the slot,
	// so its value then equals its value in the accounts store as of Through.
	ErrUnchanged = errors.New("history: no change recorded after the slot")
)

type chunkMeta struct {
	from, to uint64
	path     string
	reader   *chunkReader
}

type segment struct {
	dir    string
	chunks []*chunkMeta
	index  *segmentIndex       // closed segments
	masks  map[[32]byte]uint64 // open segment
}

func (s *segment) from() uint64 { return s.chunks[0].from }
func (s *segment) to() uint64   { return s.chunks[len(s.chunks)-1].to }

func (s *segment) mask(key [32]byte) (uint64, error) {
	if s.masks != nil {
		return s.masks[key], nil
	}
	return s.index.lookup(key)
}

// Store is the history of account versions for a window of recent slots. One
// goroutine calls Append; readers may call AccountAt concurrently.
type Store struct {
	mu            sync.RWMutex
	dir           string
	segmentChunks int
	keepSegments  int
	segments      []*segment // ascending; the last one is open unless empty
}

// Options configure a Store.
type Options struct {
	SegmentChunks int // chunks per segment, 1..64; default 64
	KeepSegments  int // closed segments retained; default 48
}

// Open opens or creates a history store in dir.
func Open(dir string, o Options) (*Store, error) {
	if o.SegmentChunks <= 0 {
		o.SegmentChunks = DefaultSegmentChunks
	}
	if o.SegmentChunks > maxSegmentChunks {
		return nil, fmt.Errorf("history: at most %d chunks per segment", maxSegmentChunks)
	}
	if o.KeepSegments <= 0 {
		o.KeepSegments = DefaultKeepSegments
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, segmentChunks: o.SegmentChunks, keepSegments: o.KeepSegments}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segDirs []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "seg-") {
			segDirs = append(segDirs, e.Name())
		}
	}
	sort.Strings(segDirs) // names are zero-padded slots
	for i, name := range segDirs {
		seg, err := s.loadSegment(filepath.Join(dir, name), i == len(segDirs)-1)
		if err != nil {
			s.Close()
			return nil, err
		}
		if seg != nil {
			s.segments = append(s.segments, seg)
		}
	}
	return s, nil
}

func segmentDirName(from uint64) string    { return fmt.Sprintf("seg-%020d", from) }
func chunkFileName(from, to uint64) string { return fmt.Sprintf("chunk-%020d-%020d.hst", from, to) }

func (s *Store) loadSegment(dir string, last bool) (*segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	seg := &segment{dir: dir}
	hasIndex := false
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".tmp"):
			_ = os.Remove(filepath.Join(dir, name))
		case name == segmentIndexName:
			hasIndex = true
		case strings.HasPrefix(name, "chunk-") && strings.HasSuffix(name, ".hst"):
			parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(name, "chunk-"), ".hst"), "-")
			if len(parts) != 2 {
				continue
			}
			from, err1 := strconv.ParseUint(parts[0], 10, 64)
			to, err2 := strconv.ParseUint(parts[1], 10, 64)
			if err1 != nil || err2 != nil {
				continue
			}
			seg.chunks = append(seg.chunks, &chunkMeta{from: from, to: to, path: filepath.Join(dir, name)})
		}
	}
	if len(seg.chunks) == 0 {
		_ = os.RemoveAll(dir)
		return nil, nil
	}
	sort.Slice(seg.chunks, func(i, j int) bool { return seg.chunks[i].from < seg.chunks[j].from })
	for _, c := range seg.chunks {
		r, err := openChunk(c.path)
		if err != nil {
			return nil, err
		}
		c.reader = r
	}
	if !hasIndex || last && len(seg.chunks) < s.segmentChunks {
		// An open segment, or one whose close was interrupted: rebuild the
		// table from the chunks themselves.
		seg.masks = make(map[[32]byte]uint64)
		for i, c := range seg.chunks {
			bit := uint64(1) << i
			if err := c.reader.keys(func(k [32]byte) { seg.masks[k] |= bit }); err != nil {
				return nil, err
			}
		}
		if !last || len(seg.chunks) >= s.segmentChunks {
			if err := closeSegment(seg); err != nil {
				return nil, err
			}
		}
		return seg, nil
	}
	idx, err := openSegmentIndex(filepath.Join(dir, segmentIndexName))
	if err != nil {
		return nil, err
	}
	seg.index = idx
	return seg, nil
}

// Close releases open files.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seg := range s.segments {
		for _, c := range seg.chunks {
			if c.reader != nil {
				c.reader.close()
			}
		}
		if seg.index != nil {
			seg.index.close()
		}
	}
	s.segments = nil
	return nil
}

// Range returns the first and last slot covered, and whether any history exists.
func (s *Store) Range() (from, through uint64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.segments) == 0 {
		return 0, 0, false
	}
	return s.segments[0].from(), s.segments[len(s.segments)-1].to(), true
}

// SlotWrites is every account version a slot produced, in any order.
type SlotWrites struct {
	Slot     uint64
	Keys     [][32]byte
	Versions []*Version
}

// KeyHistory is one account's changes in a fold: its version before the fold
// and one difference per slot that changed it (AppendXor of consecutive
// versions, starting from Prev), ascending by slot.
type KeyHistory struct {
	Key   [32]byte
	Prev  *Version
	Diffs []Diff
}

// Append records one fold covering slots [from, to]. slots must be ascending
// within the range. prev returns each key's version before from (Absent when
// the account did not exist); it is called once with every key the fold wrote.
// Append must follow the previous call's range without a gap.
func (s *Store) Append(from, to uint64, slots []SlotWrites, prev func(keys [][32]byte) ([]*Version, error)) error {
	type change struct {
		slot uint64
		v    *Version
	}
	byKey := make(map[[32]byte][]change)
	var order [][32]byte
	for _, sw := range slots {
		if sw.Slot < from || sw.Slot > to {
			return fmt.Errorf("history: slot %d outside %d..%d", sw.Slot, from, to)
		}
		for i, k := range sw.Keys {
			if _, ok := byKey[k]; !ok {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], change{sw.Slot, sw.Versions[i]})
		}
	}
	before, err := prev(order)
	if err != nil {
		return err
	}
	if len(before) != len(order) {
		return fmt.Errorf("history: prev returned %d versions for %d keys", len(before), len(order))
	}
	keys := make([]KeyHistory, 0, len(order))
	for i, k := range order {
		start := before[i]
		if start == nil {
			start = &Version{Absent: true}
		}
		cur := start
		kh := KeyHistory{Key: k, Prev: start}
		for _, c := range byKey[k] {
			if Same(cur, c.v) {
				continue
			}
			kh.Diffs = append(kh.Diffs, Diff{Slot: c.slot, Bytes: AppendXor(nil, cur, c.v)})
			cur = c.v
		}
		keys = append(keys, kh)
	}
	return s.AppendKeys(from, to, keys)
}

// AppendKeys records one fold covering slots [from, to] from per-key changes
// the caller has already turned into differences. Keys without differences
// are skipped. It must follow the previous call's range without a gap.
func (s *Store) AppendKeys(from, to uint64, keys []KeyHistory) error {
	if from > to {
		return fmt.Errorf("history: empty range %d..%d", from, to)
	}
	s.mu.RLock()
	var last *segment
	if n := len(s.segments); n > 0 {
		last = s.segments[n-1]
	}
	s.mu.RUnlock()
	if last != nil && from != last.to()+1 {
		return fmt.Errorf("history: append %d..%d does not follow %d", from, to, last.to())
	}
	sort.Slice(keys, func(i, j int) bool { return lessKey(keys[i].Key, keys[j].Key) })

	// A new segment starts when the open one is full.
	newSegment := last == nil || last.masks == nil || len(last.chunks) >= s.segmentChunks
	var seen map[[32]byte]uint64
	if !newSegment {
		seen = last.masks
	}
	records := make([]*Record, 0, len(keys))
	for _, kh := range keys {
		if len(kh.Diffs) == 0 {
			continue
		}
		for _, d := range kh.Diffs {
			if d.Slot < from || d.Slot > to {
				return fmt.Errorf("history: difference at slot %d outside %d..%d", d.Slot, from, to)
			}
		}
		r := &Record{Key: kh.Key, Diffs: kh.Diffs}
		if seen[kh.Key] == 0 && kh.Prev != nil && !kh.Prev.Absent {
			r.Full = kh.Prev
		}
		records = append(records, r)
	}

	segDir := ""
	if newSegment {
		segDir = filepath.Join(s.dir, segmentDirName(from))
		if err := os.MkdirAll(segDir, 0o755); err != nil {
			return err
		}
	} else {
		segDir = last.dir
	}
	path := filepath.Join(segDir, chunkFileName(from, to))
	if err := writeChunk(path, from, to, records); err != nil {
		return err
	}
	reader, err := openChunk(path)
	if err != nil {
		return err
	}
	meta := &chunkMeta{from: from, to: to, path: path, reader: reader}

	s.mu.Lock()
	defer s.mu.Unlock()
	if newSegment {
		if last != nil && last.masks != nil {
			if err := closeSegment(last); err != nil {
				return err
			}
		}
		last = &segment{dir: segDir, masks: make(map[[32]byte]uint64)}
		s.segments = append(s.segments, last)
	}
	bit := uint64(1) << len(last.chunks)
	for _, r := range records {
		last.masks[r.Key] |= bit
	}
	last.chunks = append(last.chunks, meta)
	s.prune()
	return nil
}

// prune drops the oldest closed segments beyond the retention count.
func (s *Store) prune() {
	closed := 0
	for _, seg := range s.segments {
		if seg.masks == nil {
			closed++
		}
	}
	for closed > s.keepSegments && len(s.segments) > 1 {
		old := s.segments[0]
		s.segments = s.segments[1:]
		for _, c := range old.chunks {
			c.reader.close()
		}
		if old.index != nil {
			old.index.close()
		}
		_ = os.RemoveAll(old.dir)
		closed--
	}
}

func lessKey(a, b [32]byte) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// AccountAt returns the account's version at the end of slot. With
// ErrUnchanged, the account did not change after slot through the returned
// slot, so the caller reads the accounts store and must check that its
// durable watermark still equals that slot.
func (s *Store) AccountAt(key [32]byte, slot uint64) (*Version, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := len(s.segments)
	if n == 0 {
		return nil, 0, ErrAfterHistory
	}
	through := s.segments[n-1].to()
	if slot < s.segments[0].from() {
		return nil, through, ErrBeforeHistory
	}
	if slot > through {
		return nil, through, ErrAfterHistory
	}
	i := sort.Search(n, func(i int) bool { return s.segments[i].to() >= slot })
	for j := i; j < n; j++ {
		seg := s.segments[j]
		mask, err := seg.mask(key)
		if err != nil {
			return nil, through, err
		}
		if mask == 0 {
			continue
		}
		var recs []*Record
		for m := mask; m != 0; m &= m - 1 {
			c := bits.TrailingZeros64(m)
			if c >= len(seg.chunks) {
				break
			}
			r, err := seg.chunks[c].reader.lookup(key)
			if err != nil {
				return nil, through, err
			}
			if r != nil {
				recs = append(recs, r)
			}
		}
		if len(recs) == 0 {
			continue // a prefix collision in a closed segment's index
		}
		v := &Version{Absent: true}
		if recs[0].Full != nil {
			v = recs[0].Full
		}
		if j > i {
			return v, through, nil
		}
		v = v.Clone()
		for _, r := range recs {
			for _, d := range r.Diffs {
				if d.Slot > slot {
					return v, through, nil
				}
				if err := ApplyXor(v, d.Bytes, true); err != nil {
					return nil, through, fmt.Errorf("history: account %x slot %d: %w", key[:4], d.Slot, err)
				}
			}
		}
		return v, through, nil
	}
	return nil, through, ErrUnchanged
}

// The closed-segment index maps an 8-byte key prefix to the 64-bit mask of
// chunks holding that key: magic, count u64, then sorted (prefix u64 BE, mask
// u64) pairs. Prefixes can collide; masks of colliding keys are merged and a
// reader checks the full key in the chunk.
const (
	segmentIndexName  = "index.hsi"
	segmentIndexMagic = "MHSEGIX1"
)

func keyPrefix(k [32]byte) uint64 { return binary.BigEndian.Uint64(k[:8]) }

func closeSegment(seg *segment) error {
	type entry struct{ prefix, mask uint64 }
	entries := make([]entry, 0, len(seg.masks))
	for k, m := range seg.masks {
		entries = append(entries, entry{keyPrefix(k), m})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].prefix < entries[j].prefix })
	merged := entries[:0]
	for _, e := range entries {
		if n := len(merged); n > 0 && merged[n-1].prefix == e.prefix {
			merged[n-1].mask |= e.mask
			continue
		}
		merged = append(merged, e)
	}
	buf := make([]byte, 0, 16+16*len(merged))
	buf = append(buf, segmentIndexMagic...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(merged)))
	for _, e := range merged {
		buf = binary.BigEndian.AppendUint64(buf, e.prefix)
		buf = binary.BigEndian.AppendUint64(buf, e.mask)
	}
	path := filepath.Join(seg.dir, segmentIndexName)
	if err := writeFileSync(path, buf); err != nil {
		return err
	}
	idx, err := openSegmentIndex(path)
	if err != nil {
		return err
	}
	seg.index = idx
	seg.masks = nil
	return nil
}

type segmentIndex struct {
	f *os.File
	n int64
}

func openSegmentIndex(path string) (*segmentIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	hdr := make([]byte, 16)
	if _, err := f.ReadAt(hdr, 0); err != nil || string(hdr[:8]) != segmentIndexMagic {
		f.Close()
		return nil, fmt.Errorf("history: bad segment index %s", path)
	}
	n := int64(binary.BigEndian.Uint64(hdr[8:]))
	if st, err := f.Stat(); err != nil || st.Size() != 16+16*n {
		f.Close()
		return nil, fmt.Errorf("history: segment index %s has the wrong size", path)
	}
	return &segmentIndex{f: f, n: n}, nil
}

func (x *segmentIndex) close() { x.f.Close() }

func (x *segmentIndex) lookup(key [32]byte) (uint64, error) {
	want := keyPrefix(key)
	var e [16]byte
	lo, hi := int64(0), x.n
	for lo < hi {
		mid := (lo + hi) / 2
		if _, err := x.f.ReadAt(e[:], 16+16*mid); err != nil {
			return 0, err
		}
		p := binary.BigEndian.Uint64(e[:8])
		switch {
		case p == want:
			return binary.BigEndian.Uint64(e[8:]), nil
		case p < want:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return 0, nil
}

// Through returns the last slot recorded, and whether any history exists.
func (s *Store) Through() (uint64, bool) {
	_, through, ok := s.Range()
	return through, ok
}

// TruncateFrom drops every chunk that covers slot or later, so history can be
// re-recorded after a fold was retried or the accounts store was rewound. A
// chunk that straddles slot is dropped whole.
func (s *Store) TruncateFrom(slot uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.segments) > 0 {
		seg := s.segments[len(s.segments)-1]
		keep := len(seg.chunks)
		for keep > 0 && seg.chunks[keep-1].to >= slot {
			keep--
		}
		if keep == len(seg.chunks) {
			return nil
		}
		for _, c := range seg.chunks[keep:] {
			if err := os.Remove(c.path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		if keep == 0 {
			if seg.index != nil {
				seg.index.close()
			}
			if err := os.RemoveAll(seg.dir); err != nil {
				return err
			}
			s.segments = s.segments[:len(s.segments)-1]
			continue
		}
		// The segment is open again with fewer chunks: rebuild its table.
		if seg.index != nil {
			seg.index.close()
			seg.index = nil
			if err := os.Remove(filepath.Join(seg.dir, segmentIndexName)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		seg.chunks = seg.chunks[:keep]
		seg.masks = make(map[[32]byte]uint64)
		for i, c := range seg.chunks {
			bit := uint64(1) << i
			if err := c.reader.keys(func(k [32]byte) { seg.masks[k] |= bit }); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

// Reset deletes all history.
func (s *Store) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seg := range s.segments {
		if seg.index != nil {
			seg.index.close()
		}
		if err := os.RemoveAll(seg.dir); err != nil {
			return err
		}
	}
	s.segments = nil
	return nil
}
