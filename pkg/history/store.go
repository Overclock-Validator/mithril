package history

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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
	// ErrBeforeHistory: the slot is older than the oldest retained chunk (or
	// than the block before it, when that chunk kept a base bank record).
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
	sigs     bool // the chunk's transactions are in the signature index
}

type segment struct {
	dir    string
	chunks []*chunkMeta
	index  *segmentIndex          // closed segments
	masks  map[[32]byte]uint64    // open segment
	since  map[[32]byte]sinceFull // open segment: work since each key's last full copy
	// sigFiles is the segment's signature index, ascending: once the segment
	// is closed, one file for all of it; before that, one per chunk.
	sigFiles []*sigFile
	// base is the bank record of the block before the segment's first chunk,
	// kept with that chunk; nil when it was not recorded.
	base *SlotBank
}

// readFrom is the oldest slot whose accounts can be read: the segment's base
// block when it has one. A segment's first record of each account carries
// the account's full version before the segment, so the version then is
// known without an older chunk.
func (s *segment) readFrom() uint64 {
	if s.base != nil {
		return s.base.Slot
	}
	return s.from()
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
	lastBank      uint64     // slot of the newest bank record, when lastBankKnown
	lastBankKnown bool
	banks         bankFiles
	repaired      string
}

// Options configure a Store.
type Options struct {
	SegmentChunks int // chunks per segment, 1..64; default 64
	KeepSegments  int // closed segments retained; default 48
}

// Open opens or creates a history store in dir. A damaged or missing chunk
// ends the readable history: it and everything after it are deleted, since
// later records depend on it (Repaired says what was dropped).
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
	cut := false
	for i, name := range segDirs {
		path := filepath.Join(dir, name)
		if cut {
			if err := os.RemoveAll(path); err != nil {
				s.Close()
				return nil, err
			}
			continue
		}
		var expect uint64
		have := len(s.segments) > 0
		if have {
			expect = s.segments[len(s.segments)-1].to() + 1
		}
		seg, damaged, err := s.loadSegment(path, i == len(segDirs)-1, expect, have)
		if err != nil {
			s.Close()
			return nil, err
		}
		if seg != nil {
			s.segments = append(s.segments, seg)
		}
		if damaged {
			cut = true
			s.repaired = fmt.Sprintf("a damaged or missing chunk in %s: history after slot %d was deleted", name, expect-1)
			if seg != nil {
				s.repaired = fmt.Sprintf("a damaged or missing chunk in %s: history after slot %d was deleted", name, seg.to())
			}
		}
	}
	return s, nil
}

// Repaired describes history Open had to delete, or is empty.
func (s *Store) Repaired() string { return s.repaired }

func segmentDirName(from uint64) string    { return fmt.Sprintf("seg-%020d", from) }
func chunkFileName(from, to uint64) string { return fmt.Sprintf("chunk-%020d-%020d.hst", from, to) }

func parseRange(name, prefix, suffix string) (from, to uint64, ok bool) {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return 0, 0, false
	}
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix), "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	from, err1 := strconv.ParseUint(parts[0], 10, 64)
	to, err2 := strconv.ParseUint(parts[1], 10, 64)
	return from, to, err1 == nil && err2 == nil && from <= to
}

// loadSegment opens one segment directory. Chunks must continue from expect
// (when have) and each other without a gap; at the first one that does not,
// or that cannot be opened, the rest of the segment is deleted and damaged is
// true.
func (s *Store) loadSegment(dir string, last bool, expect uint64, have bool) (seg *segment, damaged bool, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, false, err
	}
	var found []*chunkMeta
	var banks, sigNames []string
	hasIndex := false
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".tmp"):
			_ = os.Remove(filepath.Join(dir, name))
		case name == segmentIndexName:
			hasIndex = true
		case strings.HasSuffix(name, ".hsg"):
			sigNames = append(sigNames, name)
		case strings.HasSuffix(name, ".hbk"), strings.HasSuffix(name, ".hsc"):
			banks = append(banks, name)
		default:
			if from, to, ok := parseRange(name, "chunk-", ".hst"); ok {
				found = append(found, &chunkMeta{from: from, to: to, path: filepath.Join(dir, name)})
			}
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].from < found[j].from })
	seg = &segment{dir: dir}
	for _, c := range found {
		if have && c.from != expect {
			damaged = true
			break
		}
		r, err := openChunk(c.path)
		if err != nil || r.from != c.from || r.to != c.to {
			damaged = true
			break
		}
		c.reader = r
		seg.chunks = append(seg.chunks, c)
		expect, have = c.to+1, true
	}
	if damaged {
		_ = os.Remove(filepath.Join(dir, segmentIndexName))
		hasIndex, last = false, true
		for _, c := range found[len(seg.chunks):] {
			if err := os.Remove(c.path); err != nil && !os.IsNotExist(err) {
				return nil, damaged, err
			}
		}
	}
	// Bank and sidecar files belong to a chunk; any other (a fold interrupted
	// between the writes, or a dropped chunk) is removed.
	kept := make(map[[2]uint64]bool, len(seg.chunks))
	for _, c := range seg.chunks {
		kept[[2]uint64{c.from, c.to}] = true
	}
	for _, b := range banks {
		from, to, ok := parseRange(b, "bank-", ".hbk")
		if !ok {
			from, to, ok = parseSidecarName(b)
		}
		if !ok || !kept[[2]uint64{from, to}] {
			_ = os.Remove(filepath.Join(dir, b))
		}
	}
	if len(seg.chunks) == 0 {
		return nil, damaged, os.RemoveAll(dir)
	}
	seg.base = readBase(seg.chunks[0])
	seg.loadSigs(sigNames)
	open := last && len(seg.chunks) < s.segmentChunks
	if !open {
		// A segment closed by a fold that stopped before merging.
		merged, err := seg.mergeSigs()
		if err != nil {
			return nil, damaged, err
		}
		if merged != nil {
			seg.useMergedSigs(merged)
		}
	}
	if hasIndex && !open {
		idx, err := openSegmentIndex(filepath.Join(dir, segmentIndexName))
		if err == nil {
			seg.index = idx
			return seg, damaged, nil
		}
		// A bad or old-format index is rebuilt from the chunks.
	}
	// An open segment, or one whose index is missing or bad: rebuild the
	// table from the chunks themselves.
	if err := seg.rebuildMasks(); err != nil {
		return nil, damaged, err
	}
	if !open {
		if err := closeSegment(seg); err != nil {
			return nil, damaged, err
		}
	}
	return seg, damaged, nil
}

func (seg *segment) rebuildMasks() error {
	seg.masks = make(map[[32]byte]uint64)
	seg.since = make(map[[32]byte]sinceFull)
	for i, c := range seg.chunks {
		bit := uint64(1) << i
		if err := c.reader.records(func(r *Record) {
			seg.masks[r.Key] |= bit
			seg.since[r.Key] = seg.since[r.Key].after(r)
		}); err != nil {
			return err
		}
	}
	return nil
}

// A chunk repeats an account's full copy once a read from the last one would
// cross fullEveryChunks chunks or apply more than fullEveryBytesFactor times
// the account's size in differences, so a read of an account that changes
// every fold touches a bounded number of chunks.
const (
	fullEveryChunks      = 8
	fullEveryBytesFactor = 2
)

// sinceFull is the read work from a key's last full copy in the open segment
// through its newest chunk: chunks holding it, and difference bytes.
type sinceFull struct {
	chunks uint32
	bytes  uint64
}

func diffBytes(r *Record) uint64 {
	n := uint64(0)
	for _, d := range r.Diffs {
		n += uint64(len(d.Bytes))
	}
	return n
}

func (f sinceFull) after(r *Record) sinceFull {
	if r.Full != nil {
		return sinceFull{chunks: 1, bytes: diffBytes(r)}
	}
	return sinceFull{chunks: f.chunks + 1, bytes: f.bytes + diffBytes(r)}
}

// needsFull reports whether this chunk should repeat the key's full copy.
func (f sinceFull) needsFull(prev *Version, added uint64) bool {
	if f.chunks == 0 {
		return true // the key's first chunk in the segment
	}
	size := uint64(len(prev.Data)) + 64
	return f.chunks >= fullEveryChunks || f.bytes+added > fullEveryBytesFactor*size
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
	return s.AppendKeys(from, to, keys, nil)
}

// AppendKeys records one fold covering slots [from, to] from per-key changes
// the caller has already turned into differences, with the fold's bank
// context when bank is not nil. Keys without differences are skipped. It must
// follow the previous call's range without a gap.
func (s *Store) AppendKeys(from, to uint64, keys []KeyHistory, bank *BankData) error {
	if from > to {
		return fmt.Errorf("history: empty range %d..%d", from, to)
	}
	s.mu.RLock()
	var last *segment
	if n := len(s.segments); n > 0 {
		last = s.segments[n-1]
	}
	if !s.lastBankKnown {
		s.lastBank, s.lastBankKnown = s.lastBankSlot(), true
	}
	parent := s.lastBank
	s.mu.RUnlock()
	if last != nil && from != last.to()+1 {
		return fmt.Errorf("history: append %d..%d does not follow %d", from, to, last.to())
	}
	// A new segment starts when the open one is full.
	newSegment := last == nil || last.masks == nil || len(last.chunks) >= s.segmentChunks
	if bank != nil && bank.Base != nil {
		b := bank.Base
		if b.Slot.Slot >= from || b.Chunk.Slot != b.Slot.Slot {
			return fmt.Errorf("history: base bank record for slot %d does not precede %d..%d", b.Slot.Slot, from, to)
		}
		if !newSegment {
			// Only a segment's first chunk keeps one.
			c := *bank
			c.Base = nil
			bank = &c
		} else if b.Slot.Slot > parent {
			parent = b.Slot.Slot
		}
	}
	if bank != nil {
		for i := range bank.Slots {
			r := &bank.Slots[i]
			if r.Slot < from || r.Slot > to || (i > 0 && r.Slot <= bank.Slots[i-1].Slot) {
				return fmt.Errorf("history: bank record for slot %d out of order in %d..%d", r.Slot, from, to)
			}
			r.Parent = parent
			parent = r.Slot
		}
	}
	sort.Slice(keys, func(i, j int) bool { return lessKey(keys[i].Key, keys[j].Key) })

	var since map[[32]byte]sinceFull
	if !newSegment {
		since = last.since
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
		if kh.Prev != nil && !kh.Prev.Absent && since[kh.Key].needsFull(kh.Prev, diffBytes(r)) {
			r.Full = kh.Prev
		}
		records = append(records, r)
	}

	// A segment about to close merges its chunks' signature files first,
	// outside the lock: readers keep using the chunk files until the swap.
	var merged *sigFile
	if newSegment && last != nil && last.masks != nil {
		s.mu.RLock()
		var err error
		merged, err = last.mergeSigs()
		s.mu.RUnlock()
		if err != nil {
			return err
		}
	}

	segDir := ""
	if newSegment {
		segDir = filepath.Join(s.dir, segmentDirName(from))
		if err := os.MkdirAll(segDir, 0o755); err != nil {
			return err
		}
		if err := syncDir(s.dir); err != nil {
			return err
		}
	} else {
		segDir = last.dir
	}
	path := filepath.Join(segDir, chunkFileName(from, to))
	if bank != nil {
		for kind, data := range bank.Sidecars {
			if !validSidecarKind(kind) {
				return fmt.Errorf("history: invalid sidecar kind %q", kind)
			}
			if err := writeFileSync(filepath.Join(segDir, sidecarFileName(kind, from, to)), encodeSidecar(data)); err != nil {
				return err
			}
		}
		if err := writeFileSync(filepath.Join(segDir, bankFileName(from, to)), encodeBank(bank)); err != nil {
			return err
		}
	}
	var sigs *sigFile
	if bank != nil && bank.Signatures != nil {
		var err error
		if sigs, err = writeChunkSigs(filepath.Join(segDir, sigChunkFileName(from, to)), from, to, bank.Signatures); err != nil {
			return err
		}
	}
	reader, err := writeChunk(path, from, to, records)
	if err != nil {
		return err
	}
	meta := &chunkMeta{from: from, to: to, path: path, reader: reader, sigs: sigs != nil}

	s.mu.Lock()
	defer s.mu.Unlock()
	if bank != nil && len(bank.Slots) > 0 {
		s.lastBank = bank.Slots[len(bank.Slots)-1].Slot
	}
	if newSegment {
		if last != nil && last.masks != nil {
			if err := closeSegment(last); err != nil {
				return err
			}
			if merged != nil {
				last.useMergedSigs(merged)
			}
		}
		last = &segment{dir: segDir, masks: make(map[[32]byte]uint64), since: make(map[[32]byte]sinceFull)}
		if bank != nil && bank.Base != nil {
			r := bank.Base.Slot
			r.Parent = 0
			last.base = &r
		}
		s.segments = append(s.segments, last)
	}
	bit := uint64(1) << len(last.chunks)
	for _, r := range records {
		last.masks[r.Key] |= bit
		last.since[r.Key] = last.since[r.Key].after(r)
	}
	last.chunks = append(last.chunks, meta)
	if sigs != nil {
		last.sigFiles = append(last.sigFiles, sigs)
	}
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
	return s.accountAtLocked(key, slot, nil)
}

// AccountsAt is AccountAt for several keys at one slot, sharing decompressed
// blocks between them. errs[i] is nil, ErrUnchanged, ErrBeforeHistory,
// ErrAfterHistory or a read error, with the same meaning as AccountAt's.
func (s *Store) AccountsAt(keys [][32]byte, slot uint64) (versions []*Version, errs []error, through uint64) {
	versions, errs, through, _ = s.AccountsAtLimit(keys, slot, NoLimit)
	return versions, errs, through
}

// NoLimit disables AccountsAtLimit's data budget.
const NoLimit = ^uint64(0)

// ErrDataLimit: the versions read exceed the caller's data budget.
var ErrDataLimit = errors.New("history: account data exceeds the read budget")

// AccountsAtLimit is AccountsAt that stops with ErrDataLimit once the data of
// the versions read, duplicates included, exceeds maxDataBytes. A single
// version can overshoot the budget by at most its own size.
func (s *Store) AccountsAtLimit(keys [][32]byte, slot uint64, maxDataBytes uint64) (versions []*Version, errs []error, through uint64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if n := len(s.segments); n > 0 {
		through = s.segments[n-1].to()
	}
	versions = make([]*Version, len(keys))
	errs = make([]error, len(keys))
	cache := blockCache{}
	remaining := maxDataBytes
	for i, k := range keys {
		versions[i], _, errs[i] = s.accountAtLocked(k, slot, cache)
		if v := versions[i]; v != nil && maxDataBytes != NoLimit {
			if uint64(len(v.Data)) > remaining {
				return nil, nil, through, ErrDataLimit
			}
			remaining -= uint64(len(v.Data))
		}
	}
	return versions, errs, through, nil
}

func (s *Store) accountAtLocked(key [32]byte, slot uint64, cache blockCache) (*Version, uint64, error) {
	n := len(s.segments)
	if n == 0 {
		return nil, 0, ErrAfterHistory
	}
	through := s.segments[n-1].to()
	if slot < s.segments[0].readFrom() {
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
		// Chunks holding the key, ascending (a closed segment's index may
		// name extra chunks through a prefix collision).
		var held [maxSegmentChunks]int
		idx := held[:0]
		for m := mask; m != 0; m &= m - 1 {
			c := bits.TrailingZeros64(m)
			if c >= len(seg.chunks) {
				break
			}
			idx = append(idx, c)
		}
		// The last chunk starting at or before the slot: later ones hold only
		// later differences.
		hi := -1
		if j == i {
			for k, c := range idx {
				if seg.chunks[c].from <= slot {
					hi = k
				}
			}
		}
		// Walk back to the nearest full copy at or before the slot.
		var recs []*Record // descending
		start := &Version{Absent: true}
		for k := hi; k >= 0; k-- {
			r, err := seg.chunks[idx[k]].reader.lookupCached(key, cache)
			if err != nil {
				return nil, through, err
			}
			if r == nil {
				continue
			}
			recs = append(recs, r)
			if r.Full != nil {
				start = r.Full
				break
			}
		}
		if len(recs) == 0 {
			// The account first changed after the slot within this segment, or
			// in a later one: its version then is its first record's full copy
			// (none: it did not exist). Chunks without a record for it are
			// prefix collisions in a closed segment's index.
			found := false
			for _, c := range idx[hi+1:] {
				r, err := seg.chunks[c].reader.lookupCached(key, cache)
				if err != nil {
					return nil, through, err
				}
				if r == nil {
					continue
				}
				if r.Full != nil {
					return r.Full, through, nil
				}
				found = true
				break
			}
			if found {
				return &Version{Absent: true}, through, nil
			}
			continue
		}
		v := start.Clone()
		for k := len(recs) - 1; k >= 0; k-- {
			for _, d := range recs[k].Diffs {
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
// chunks holding that key: magic, count u64, sorted (prefix u64 BE, mask u64)
// pairs, then a crc32 of everything before it. Prefixes can collide; masks of
// colliding keys are merged and a reader checks the full key in the chunk.
// Open keeps every fenceStride-th prefix in memory, so a lookup is one read.
const (
	segmentIndexName  = "index.hsi"
	segmentIndexMagic = "MHSEGIX2"
	fenceStride       = 512
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
	buf := make([]byte, 0, 16+16*len(merged)+4)
	buf = append(buf, segmentIndexMagic...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(merged)))
	for _, e := range merged {
		buf = binary.BigEndian.AppendUint64(buf, e.prefix)
		buf = binary.BigEndian.AppendUint64(buf, e.mask)
	}
	buf = binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
	path := filepath.Join(seg.dir, segmentIndexName)
	if err := writeFileSync(path, buf); err != nil {
		return err
	}
	idx, err := openSegmentIndex(path)
	if err != nil {
		return err
	}
	seg.index = idx
	seg.masks, seg.since = nil, nil
	return nil
}

type segmentIndex struct {
	path   string
	n      uint64
	fences []uint64 // prefix of entry i*fenceStride
}

func openSegmentIndex(path string) (*segmentIndex, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) < 20 || string(b[:8]) != segmentIndexMagic {
		return nil, fmt.Errorf("history: bad segment index %s", path)
	}
	n := binary.BigEndian.Uint64(b[8:])
	if n > uint64(len(b)-20)/16 || 16+16*n+4 != uint64(len(b)) {
		return nil, fmt.Errorf("history: segment index %s has the wrong size", path)
	}
	body := b[:len(b)-4]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		return nil, fmt.Errorf("history: segment index %s checksum", path)
	}
	x := &segmentIndex{path: path, n: n}
	for i := uint64(0); i < n; i += fenceStride {
		x.fences = append(x.fences, binary.BigEndian.Uint64(b[16+16*i:]))
	}
	return x, nil
}

func (x *segmentIndex) close() {}

func (x *segmentIndex) lookup(key [32]byte) (uint64, error) {
	want := keyPrefix(key)
	// The last fence at or below want starts the only stride that can hold it.
	f := sort.Search(len(x.fences), func(i int) bool { return x.fences[i] > want }) - 1
	if f < 0 {
		return 0, nil
	}
	lo := uint64(f) * fenceStride
	cnt := min(uint64(fenceStride), x.n-lo)
	buf := make([]byte, 16*cnt)
	fh, err := os.Open(x.path)
	if err != nil {
		return 0, err
	}
	_, err = fh.ReadAt(buf, int64(16+16*lo))
	fh.Close()
	if err != nil {
		return 0, err
	}
	i := sort.Search(int(cnt), func(i int) bool { return binary.BigEndian.Uint64(buf[16*i:]) >= want })
	if i < int(cnt) && binary.BigEndian.Uint64(buf[16*i:]) == want {
		return binary.BigEndian.Uint64(buf[16*i+8:]), nil
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
		s.lastBankKnown = false
		s.banks.clear()
		// The index goes first: a crash part way leaves an open segment whose
		// table is rebuilt from the chunks that remain.
		if seg.index != nil {
			seg.index.close()
			seg.index = nil
			if err := os.Remove(filepath.Join(seg.dir, segmentIndexName)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		if err := seg.truncateSigs(keep); err != nil {
			return err
		}
		for _, c := range seg.chunks[keep:] {
			if err := os.Remove(c.path); err != nil && !os.IsNotExist(err) {
				return err
			}
			if err := os.Remove(c.bankPath()); err != nil && !os.IsNotExist(err) {
				return err
			}
			for _, p := range c.sidecarPaths() {
				if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
		}
		if keep == 0 {
			if err := os.RemoveAll(seg.dir); err != nil {
				return err
			}
			s.segments = s.segments[:len(s.segments)-1]
			continue
		}
		// The segment is open again with fewer chunks: rebuild its table.
		seg.chunks = seg.chunks[:keep]
		return seg.rebuildMasks()
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
	s.lastBankKnown = false
	s.banks.clear()
	return nil
}
