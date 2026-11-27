package accountsdb

// Measurement only. With MITHRIL_HISTORY_MEASURE=<dir>, every fold reports how
// large account history would be if versions were kept every N slots, stored
// whole or as diffs against the previous kept version. It reads the store for
// the version an account had before the run first saw it, and never changes
// what the fold writes.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
	"github.com/klauspost/compress/zstd"
)

// Spacings measured, in slots. 128 is the fold size.
var hmSpacings = []uint64{1, 4, 16, 64, 128, 256, 512, 1024}

// Spacings whose windows are also compressed and sampled to disk.
var hmCompressSpacings = map[uint64]bool{16: true, 128: true, 256: true}

const (
	hmFoldSlots      = 128
	hmCompactHeader  = 85 // key, lamports, owner, executable, rent epoch, data length
	hmGapMerge       = 8  // equal runs this short stay inside one diff range
	hmZeroRunMin     = 4
	hmSampleCapBytes = 3 << 30
)

var hmVoteOwner = [32]byte(addresses.VoteProgramAddr)

type hmVer struct {
	lamports  uint64
	owner     [32]byte
	exec      bool
	rentEpoch uint64
	data      []byte
	absent    bool
}

func hmFromAccount(a *accounts.Account) *hmVer {
	if a == nil {
		return &hmVer{absent: true}
	}
	if a.Lamports == 0 && len(a.Data) == 0 && a.Owner == [32]byte{} {
		return &hmVer{absent: true}
	}
	return &hmVer{lamports: a.Lamports, owner: a.Owner, exec: a.Executable, rentEpoch: a.RentEpoch, data: bytes.Clone(a.Data)}
}

type HMClassStats struct {
	Versions     uint64 `json:"versions"`
	Created      uint64 `json:"created"`
	Closed       uint64 `json:"closed"`
	Unchanged    uint64 `json:"unchanged"`
	WholeBytes   uint64 `json:"whole_bytes"`    // fold record encoding: 136-byte header + data padded to 8
	WholeZRBytes uint64 `json:"whole_zr_bytes"` // compact header + data with zero runs dropped
	DiffBytes    uint64 `json:"diff_bytes"`     // diff against the previous kept version
	DiffZRBytes  uint64 `json:"diff_zr_bytes"`  // same, zero runs dropped inside ranges
	ChangedBytes uint64 `json:"changed_bytes"`  // data bytes that differ
	Ranges       uint64 `json:"ranges"`
}

func (s *HMClassStats) add(o HMClassStats) {
	s.Versions += o.Versions
	s.Created += o.Created
	s.Closed += o.Closed
	s.Unchanged += o.Unchanged
	s.WholeBytes += o.WholeBytes
	s.WholeZRBytes += o.WholeZRBytes
	s.DiffBytes += o.DiffBytes
	s.DiffZRBytes += o.DiffZRBytes
	s.ChangedBytes += o.ChangedBytes
	s.Ranges += o.Ranges
}

type HMCompressStats struct {
	RawWhole       uint64 `json:"raw_whole"`
	ZstdWhole      uint64 `json:"zstd_whole"`
	RawDiff        uint64 `json:"raw_diff"`
	ZstdDiff       uint64 `json:"zstd_diff"`
	ZstdDiffOwner  uint64 `json:"zstd_diff_owner_sorted"`
	CompressNanos  uint64 `json:"compress_ns"`
	DiffEncodeNano uint64 `json:"diff_encode_ns"`
}

type hmOwner struct {
	Owner     string `json:"owner"`
	Versions  uint64 `json:"versions"`
	Whole     uint64 `json:"whole_bytes"`
	Diff      uint64 `json:"diff_bytes"`
	DiffZR    uint64 `json:"diff_zr_bytes"`
	ZstdOwner uint64 `json:"zstd_owner_sorted_bytes,omitempty"`
}

type hmSpacing struct {
	n          uint64
	window     int64
	pending    map[[32]byte]*hmVer
	prev       map[[32]byte]*hmVer
	closedEnd  uint64 // last slot covered by a closed window
	vote       HMClassStats
	other      HMClassStats
	compress   HMCompressStats
	owners     map[[32]byte]*hmOwner
	sampleFile *os.File
	sampled    uint64
}

type historyMeasurer struct {
	mu       sync.Mutex
	dir      string
	base     uint64
	lastSlot uint64
	started  bool
	spacings []*hmSpacing
	folds    map[[32]byte]uint64 // bit f set when fold f changed the key
	isVote   map[[32]byte]bool
	enc      *zstd.Encoder
	windows  *os.File
	batches  uint64
	readNs   uint64
	totalNs  uint64
	seg      *hmSegState
}

var (
	hmOnce     sync.Once
	hmInstance *historyMeasurer
)

func historyMeasure() *historyMeasurer {
	hmOnce.Do(func() {
		dir := os.Getenv("MITHRIL_HISTORY_MEASURE")
		if dir == "" {
			return
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "history measure: %v\n", err)
			return
		}
		m, err := newHistoryMeasurer(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "history measure: %v\n", err)
			return
		}
		hmInstance = m
	})
	return hmInstance
}

func newHistoryMeasurer(dir string) (*historyMeasurer, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	w, err := os.Create(filepath.Join(dir, "windows.jsonl"))
	if err != nil {
		return nil, err
	}
	seg, err := newHMSegState()
	if err != nil {
		return nil, err
	}
	m := &historyMeasurer{dir: dir, folds: map[[32]byte]uint64{}, isVote: map[[32]byte]bool{}, enc: enc, windows: w, seg: seg}
	for _, n := range hmSpacings {
		sp := &hmSpacing{n: n, window: -1, pending: map[[32]byte]*hmVer{}, prev: map[[32]byte]*hmVer{}}
		if hmCompressSpacings[n] {
			sp.owners = map[[32]byte]*hmOwner{}
			f, err := os.Create(filepath.Join(dir, fmt.Sprintf("diffs-%d.zst", n)))
			if err != nil {
				return nil, err
			}
			sp.sampleFile = f
		}
		m.spacings = append(m.spacings, sp)
	}
	return m, nil
}

// observe is called by CommitBatch before it takes the fold lock, while the
// store still holds every key's version from before this batch.
func (m *historyMeasurer) observe(db *AccountsDb, deltas []accounts.SlotDelta) {
	if len(deltas) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	start := time.Now()
	if !m.started {
		m.base = deltas[0].Slot - 1
		m.lastSlot = m.base
		m.started = true
	}
	if deltas[0].Slot <= m.lastSlot {
		fmt.Fprintf(os.Stderr, "history measure: ignoring batch from slot %d at or below %d\n", deltas[0].Slot, m.lastSlot)
		return
	}

	// Versions before the run first saw a key come from the store.
	var unseen []solana.PublicKey
	queued := map[[32]byte]bool{}
	for _, sd := range deltas {
		for _, a := range sd.Delta {
			if a == nil {
				continue
			}
			k := [32]byte(a.Key)
			if _, ok := m.folds[k]; ok || queued[k] {
				continue
			}
			queued[k] = true
			unseen = append(unseen, a.Key)
		}
	}
	if len(unseen) > 0 {
		rs := time.Now()
		before, err := db.GetAccountsBatch(context.Background(), deltas[0].Slot-1, unseen)
		m.readNs += uint64(time.Since(rs))
		if err != nil {
			fmt.Fprintf(os.Stderr, "history measure: read previous versions: %v\n", err)
			before = make([]*accounts.Account, len(unseen))
		}
		for i, pk := range unseen {
			v := hmFromAccount(before[i])
			k := [32]byte(pk)
			for _, sp := range m.spacings {
				sp.prev[k] = v
			}
			if !v.absent {
				m.seg.cur[k] = v
			}
			m.folds[k] = 0
		}
	}

	for _, sd := range deltas {
		s := sd.Slot
		fold := (s - m.base - 1) / hmFoldSlots
		for _, sp := range m.spacings {
			w := int64((s - m.base - 1) / sp.n)
			if sp.window != w {
				if sp.window >= 0 {
					m.closeWindow(sp)
				}
				sp.window = w
			}
		}
		m.segBeginSlot(s)
		for _, a := range sd.Delta {
			if a == nil {
				continue
			}
			k := [32]byte(a.Key)
			v := hmFromAccount(a)
			if a.Lamports == 0 {
				v.absent = false // a closed account is a version, not a missing key
			}
			for _, sp := range m.spacings {
				sp.pending[k] = v
			}
			if fold < 64 {
				m.folds[k] |= 1 << fold
			}
			m.isVote[k] = a.Owner == hmVoteOwner
			m.segChange(s, k, v)
		}
		m.segEndSlot()
		m.lastSlot = s
	}
	m.batches++
	m.totalNs += uint64(time.Since(start))
	m.writeSummary()
	m.writeSegments()
}

func (m *historyMeasurer) closeWindow(sp *hmSpacing) {
	type entry struct {
		key   [32]byte
		owner [32]byte
		diff  []byte
		whole []byte
	}
	var vote, other HMClassStats
	var entries []entry
	var rawWhole, rawDiff []byte
	es := time.Now()
	keys := make([][32]byte, 0, len(sp.pending))
	for k := range sp.pending {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	for _, k := range keys {
		cur := sp.pending[k]
		prev := sp.prev[k]
		if prev == nil {
			prev = &hmVer{absent: true}
		}
		st := &other
		if cur.owner == hmVoteOwner || (!prev.absent && prev.owner == hmVoteOwner) {
			st = &vote
		}
		st.Versions++
		st.WholeBytes += hdrLen + ((uint64(len(cur.data)) + 7) &^ 7)
		st.WholeZRBytes += hmCompactHeader + uint64(hmZeroRunSize(cur.data))
		diff, zr, changed, ranges := hmEncodeDiff(nil, prev, cur)
		switch {
		case prev.absent:
			st.Created++
		case cur.lamports == 0:
			st.Closed++
		}
		if !prev.absent && hmSame(prev, cur) {
			st.Unchanged++
			diff, zr = nil, 0
		}
		st.DiffBytes += uint64(len(diff))
		st.DiffZRBytes += uint64(zr)
		st.ChangedBytes += uint64(changed)
		st.Ranges += uint64(ranges)
		if sp.owners != nil && st == &other {
			o := sp.owners[cur.owner]
			if o == nil {
				o = &hmOwner{Owner: solana.PublicKey(cur.owner).String()}
				sp.owners[cur.owner] = o
			}
			o.Versions++
			o.Whole += hdrLen + ((uint64(len(cur.data)) + 7) &^ 7)
			o.Diff += uint64(len(diff))
			o.DiffZR += uint64(zr)
		}
		if sp.owners != nil && len(diff) > 0 {
			whole := hmEncodeWhole(nil, cur)
			entries = append(entries, entry{key: k, owner: cur.owner, diff: diff, whole: whole})
			rawWhole = append(rawWhole, whole...)
			rawDiff = append(rawDiff, diff...)
		}
		sp.prev[k] = cur
	}
	sp.pending = make(map[[32]byte]*hmVer, len(sp.pending))
	sp.closedEnd = m.base + uint64(sp.window+1)*sp.n
	sp.vote.add(vote)
	sp.other.add(other)

	var cs HMCompressStats
	if sp.owners != nil {
		cs.DiffEncodeNano = uint64(time.Since(es))
		cst := time.Now()
		cs.RawWhole = uint64(len(rawWhole))
		cs.RawDiff = uint64(len(rawDiff))
		cs.ZstdWhole = uint64(len(m.enc.EncodeAll(rawWhole, nil)))
		cs.ZstdDiff = uint64(len(m.enc.EncodeAll(rawDiff, nil)))
		sort.SliceStable(entries, func(i, j int) bool { return bytes.Compare(entries[i].owner[:], entries[j].owner[:]) < 0 })
		var byOwner []byte
		for i := 0; i < len(entries); {
			j := i
			var group []byte
			for j < len(entries) && entries[j].owner == entries[i].owner {
				group = append(group, entries[j].diff...)
				j++
			}
			byOwner = append(byOwner, group...)
			if o := sp.owners[entries[i].owner]; o != nil {
				o.ZstdOwner += uint64(len(m.enc.EncodeAll(group, nil)))
			}
			i = j
		}
		cs.ZstdDiffOwner = uint64(len(m.enc.EncodeAll(byOwner, nil)))
		cs.CompressNanos = uint64(time.Since(cst))
		sp.compress.RawWhole += cs.RawWhole
		sp.compress.RawDiff += cs.RawDiff
		sp.compress.ZstdWhole += cs.ZstdWhole
		sp.compress.ZstdDiff += cs.ZstdDiff
		sp.compress.ZstdDiffOwner += cs.ZstdDiffOwner
		sp.compress.CompressNanos += cs.CompressNanos
		sp.compress.DiffEncodeNano += cs.DiffEncodeNano
		// Diff samples for later dictionary experiments: owner, key, window end, diff.
		if sp.sampleFile != nil && sp.sampled < hmSampleCapBytes {
			var rec []byte
			for _, e := range entries {
				rec = append(rec, e.owner[:]...)
				rec = append(rec, e.key[:]...)
				rec = binary.AppendUvarint(rec, sp.closedEnd)
				rec = binary.AppendUvarint(rec, uint64(len(e.diff)))
				rec = append(rec, e.diff...)
			}
			sp.sampled += uint64(len(rec))
			if _, err := sp.sampleFile.Write(m.enc.EncodeAll(rec, nil)); err != nil {
				fmt.Fprintf(os.Stderr, "history measure: sample write: %v\n", err)
			}
		}
	}
	line, _ := json.Marshal(map[string]any{"n": sp.n, "end": sp.closedEnd, "vote": vote, "other": other, "compress": cs})
	m.windows.Write(append(line, '\n'))
}

func hmSame(a, b *hmVer) bool {
	return a.lamports == b.lamports && a.owner == b.owner && a.exec == b.exec && a.rentEpoch == b.rentEpoch && bytes.Equal(a.data, b.data)
}

func hmEncodeWhole(dst []byte, v *hmVer) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, v.lamports)
	dst = append(dst, v.owner[:]...)
	if v.exec {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.LittleEndian.AppendUint64(dst, v.rentEpoch)
	dst = binary.AppendUvarint(dst, uint64(len(v.data)))
	return append(dst, v.data...)
}

// Diff flags.
const (
	hmFlagLamports = 1 << iota
	hmFlagOwner
	hmFlagExec
	hmFlagRent
	hmFlagLength
	hmFlagCreated
)

// hmEncodeDiff encodes cur relative to prev: a flags byte, changed header
// fields, then byte ranges as (gap from previous range end, length, bytes).
// Equal runs of up to hmGapMerge bytes are kept inside a range. It returns the
// encoding, its size with zero runs dropped inside ranges, the number of data
// bytes that differ and the number of ranges.
func hmEncodeDiff(dst []byte, prev, cur *hmVer) (out []byte, zrSize, changed, ranges int) {
	if prev.absent {
		dst = append(dst, hmFlagCreated)
		w := hmEncodeWhole(nil, cur)
		dst = append(dst, w...)
		return dst, 1 + hmCompactHeader + hmZeroRunSize(cur.data), len(cur.data), 1
	}
	var flags byte
	var hdr []byte
	if prev.lamports != cur.lamports {
		flags |= hmFlagLamports
		hdr = binary.LittleEndian.AppendUint64(hdr, cur.lamports)
	}
	if prev.owner != cur.owner {
		flags |= hmFlagOwner
		hdr = append(hdr, cur.owner[:]...)
	}
	if prev.exec != cur.exec {
		flags |= hmFlagExec
	}
	if prev.rentEpoch != cur.rentEpoch {
		flags |= hmFlagRent
		hdr = binary.LittleEndian.AppendUint64(hdr, cur.rentEpoch)
	}
	if len(prev.data) != len(cur.data) {
		flags |= hmFlagLength
		hdr = binary.AppendUvarint(hdr, uint64(len(cur.data)))
	}
	dst = append(dst, flags)
	dst = append(dst, hdr...)
	zrSize = 1 + len(hdr)

	var body []byte
	p, c := prev.data, cur.data
	m := min(len(p), len(c))
	last := 0
	i := 0
	for i < m {
		// Skip equal 8-byte words.
		for i+8 <= m && binary.LittleEndian.Uint64(p[i:]) == binary.LittleEndian.Uint64(c[i:]) {
			i += 8
		}
		for i < m && p[i] == c[i] {
			i++
		}
		if i >= m {
			break
		}
		start := i
		lastDiff := i
		for i < m {
			if p[i] != c[i] {
				lastDiff = i
				changed++
			} else if i-lastDiff > hmGapMerge {
				break
			}
			i++
		}
		end := lastDiff + 1
		body = binary.AppendUvarint(body, uint64(start-last))
		body = binary.AppendUvarint(body, uint64(end-start))
		body = append(body, c[start:end]...)
		zrSize += hmUvarintLen(uint64(start-last)) + hmUvarintLen(uint64(end-start)) + hmZeroRunSize(c[start:end])
		ranges++
		last = end
		i = end
	}
	if len(c) > m {
		body = binary.AppendUvarint(body, uint64(m-last))
		body = binary.AppendUvarint(body, uint64(len(c)-m))
		body = append(body, c[m:]...)
		zrSize += hmUvarintLen(uint64(m-last)) + hmUvarintLen(uint64(len(c)-m)) + hmZeroRunSize(c[m:])
		changed += len(c) - m
		ranges++
	}
	dst = binary.AppendUvarint(dst, uint64(ranges))
	zrSize += hmUvarintLen(uint64(ranges))
	dst = append(dst, body...)
	return dst, zrSize, changed, ranges
}

func hmUvarintLen(x uint64) int {
	return (bits.Len64(x|1) + 6) / 7
}

// hmZeroRunSize is the size of b as literal runs and zero runs, each run
// prefixed by its length (zero runs shorter than hmZeroRunMin stay literal).
func hmZeroRunSize(b []byte) int {
	size := 0
	lit := 0
	i := 0
	for i < len(b) {
		if b[i] != 0 {
			lit++
			i++
			continue
		}
		j := i
		for j < len(b) && b[j] == 0 {
			j++
		}
		if j-i >= hmZeroRunMin {
			if lit > 0 {
				size += hmUvarintLen(uint64(lit)<<1) + lit
				lit = 0
			}
			size += hmUvarintLen(uint64(j-i)<<1 | 1)
		} else {
			lit += j - i
		}
		i = j
	}
	if lit > 0 {
		size += hmUvarintLen(uint64(lit)<<1) + lit
	}
	return size
}

type hmSpacingSummary struct {
	N            uint64           `json:"n"`
	SlotsCovered uint64           `json:"slots_covered"`
	Vote         HMClassStats     `json:"vote"`
	Other        HMClassStats     `json:"other"`
	Compress     *HMCompressStats `json:"compress,omitempty"`
	TopOwners    []*hmOwner       `json:"top_owners,omitempty"`
}

type hmDistinct struct {
	Folds        int     `json:"folds"`
	DistinctAll  float64 `json:"distinct_all"`
	DistinctNonV float64 `json:"distinct_nonvote"`
	EntriesAll   float64 `json:"key_fold_entries_all"`
}

func (m *historyMeasurer) writeSummary() {
	out := struct {
		Base         uint64             `json:"base_slot"`
		Last         uint64             `json:"last_slot"`
		Batches      uint64             `json:"batches"`
		ReadMs       float64            `json:"prev_read_ms"`
		MeasureMs    float64            `json:"measure_ms"`
		DistinctKeys int                `json:"distinct_keys_run"`
		Spacings     []hmSpacingSummary `json:"spacings"`
		Distinct     []hmDistinct       `json:"distinct_over_folds"`
	}{Base: m.base, Last: m.lastSlot, Batches: m.batches, ReadMs: float64(m.readNs) / 1e6, MeasureMs: float64(m.totalNs) / 1e6, DistinctKeys: len(m.folds)}
	for _, sp := range m.spacings {
		s := hmSpacingSummary{N: sp.n, Vote: sp.vote, Other: sp.other}
		if sp.closedEnd > m.base {
			s.SlotsCovered = sp.closedEnd - m.base
		}
		if sp.owners != nil {
			c := sp.compress
			s.Compress = &c
			owners := make([]*hmOwner, 0, len(sp.owners))
			for _, o := range sp.owners {
				owners = append(owners, o)
			}
			sort.Slice(owners, func(i, j int) bool { return owners[i].Diff > owners[j].Diff })
			if len(owners) > 30 {
				owners = owners[:30]
			}
			s.TopOwners = owners
		}
		out.Spacings = append(out.Spacings, s)
	}
	// Distinct keys changed within k consecutive complete folds, averaged over
	// every starting fold.
	complete := int((m.lastSlot - m.base) / hmFoldSlots)
	if complete > 64 {
		complete = 64
	}
	for _, k := range []int{1, 2, 4, 8, 16, 24, 32, 64} {
		if k > complete {
			break
		}
		starts := complete - k + 1
		var all, nonv, entries float64
		for key, mask := range m.folds {
			vote := m.isVote[key]
			for st := 0; st < starts; st++ {
				w := (mask >> uint(st)) & ((1 << uint(k)) - 1)
				if k == 64 {
					w = mask
				}
				if w == 0 {
					continue
				}
				all++
				entries += float64(bits.OnesCount64(w))
				if !vote {
					nonv++
				}
			}
		}
		out.Distinct = append(out.Distinct, hmDistinct{Folds: k, DistinctAll: all / float64(starts), DistinctNonV: nonv / float64(starts), EntriesAll: entries / float64(starts)})
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return
	}
	tmp := filepath.Join(m.dir, "summary.json.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err == nil {
		_ = os.Rename(tmp, filepath.Join(m.dir, "summary.json"))
	}
}
