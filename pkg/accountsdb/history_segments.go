package accountsdb

// Measurement only (MITHRIL_HISTORY_MEASURE). Models a history store cut into
// segments of P slots: for every account changed in a segment, a keyframe (its
// version before the first change in the segment) plus one forward diff per
// slot that changed it. Reports compressed sizes, chain lengths and the cost
// of rebuilding versions from keyframe plus diffs, checked against the real
// versions.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/klauspost/compress/zstd"
)

var hmSegmentSizes = []uint64{128, 256, 512, 1024}

type hmSegDiff struct {
	off uint32 // slot offset within the segment
	d   []byte
}

type hmSegEntry struct {
	keyframe *hmVer // nil when the account did not exist before its first change
	diffs    []hmSegDiff
}

type HMClassSeg struct {
	Keys         uint64 `json:"keys"`
	Diffs        uint64 `json:"diffs"`
	KeyframeRaw  uint64 `json:"keyframe_raw"`
	KeyframeZstd uint64 `json:"keyframe_zstd"`
	KeyframeBest uint64 `json:"keyframe_zstd_best"`
	DiffRaw      uint64 `json:"diff_raw"`
	DiffZstd     uint64 `json:"diff_zstd"`
	DiffBest     uint64 `json:"diff_zstd_best"`
}

func (c *HMClassSeg) add(o HMClassSeg) {
	c.Keys += o.Keys
	c.Diffs += o.Diffs
	c.KeyframeRaw += o.KeyframeRaw
	c.KeyframeZstd += o.KeyframeZstd
	c.KeyframeBest += o.KeyframeBest
	c.DiffRaw += o.DiffRaw
	c.DiffZstd += o.DiffZstd
	c.DiffBest += o.DiffBest
}

type HMSegmentStats struct {
	P             uint64     `json:"p"`
	Segments      uint64     `json:"segments"`
	SlotsCovered  uint64     `json:"slots_covered"`
	Vote          HMClassSeg `json:"vote"`
	Other         HMClassSeg `json:"other"`
	ChainMax      uint64     `json:"chain_max"`
	ChainOver64   uint64     `json:"chains_over_64"`
	ChainOver256  uint64     `json:"chains_over_256"`
	Rebuilds      uint64     `json:"rebuilds"`
	RebuildDiffs  uint64     `json:"rebuild_diffs_applied"`
	RebuildNs     uint64     `json:"rebuild_ns"`
	RebuildBad    uint64     `json:"rebuild_mismatches"`
	CompressNs    uint64     `json:"compress_ns"`
	CompressBest  uint64     `json:"compress_best_ns"`
	DecompressNs  uint64     `json:"decompress_ns"`
	DecompressRaw uint64     `json:"decompress_bytes"`
}

type hmSegment struct {
	p       uint64
	idx     int64
	entries map[[32]byte]*hmSegEntry
	stats   HMSegmentStats
}

type HMPerSlot struct {
	Slots    uint64 `json:"slots"`
	DiffRaw  uint64 `json:"diff_raw"`
	DiffZstd uint64 `json:"diff_zstd"`
	ZstdNs   uint64 `json:"zstd_ns"`
}

type hmSegState struct {
	cur      map[[32]byte]*hmVer // newest version of every key seen
	segments []*hmSegment
	encBest  *zstd.Encoder
	dec      *zstd.Decoder
	perSlot  HMPerSlot
	slotBuf  []byte
	slotKeys [][32]byte
	slotDiff map[[32]byte][]byte
}

func newHMSegState() (*hmSegState, error) {
	best, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	s := &hmSegState{cur: map[[32]byte]*hmVer{}, encBest: best, dec: dec, slotDiff: map[[32]byte][]byte{}}
	for _, p := range hmSegmentSizes {
		s.segments = append(s.segments, &hmSegment{p: p, idx: -1, entries: map[[32]byte]*hmSegEntry{}})
	}
	return s, nil
}

// beginSlot closes any segment that slot s leaves.
func (m *historyMeasurer) segBeginSlot(s uint64) {
	for _, sg := range m.seg.segments {
		idx := int64((s - m.base - 1) / sg.p)
		if sg.idx != idx {
			if sg.idx >= 0 {
				m.closeSegment(sg)
			}
			sg.idx = idx
		}
	}
}

// segChange records that key k became v during slot s.
func (m *historyMeasurer) segChange(s uint64, k [32]byte, v *hmVer) {
	st := m.seg
	prev := st.cur[k]
	if prev == nil {
		prev = &hmVer{absent: true}
	}
	if !prev.absent && hmSame(prev, v) {
		return
	}
	d, _, _, _ := hmEncodeDiff(nil, prev, v)
	if _, ok := st.slotDiff[k]; !ok {
		st.slotKeys = append(st.slotKeys, k)
	}
	st.slotDiff[k] = d // a key changes at most once per slot delta; keep the last
	for _, sg := range st.segments {
		e := sg.entries[k]
		if e == nil {
			e = &hmSegEntry{}
			if !prev.absent {
				e.keyframe = prev
			}
			sg.entries[k] = e
		}
		off := uint32((s - m.base - 1) % sg.p)
		if n := len(e.diffs); n > 0 && e.diffs[n-1].off == off {
			// Same slot again: re-encode against the version before this slot.
			e.diffs = e.diffs[:n-1]
			base := e.keyframe
			for _, x := range e.diffs {
				base, _ = hmApplyDiff(base, x.d)
			}
			if base == nil {
				base = &hmVer{absent: true}
			}
			d, _, _, _ = hmEncodeDiff(nil, base, v)
		}
		e.diffs = append(e.diffs, hmSegDiff{off: off, d: d})
	}
	st.cur[k] = v
}

// segEndSlot compresses this slot's diffs as one frame, as a writer that
// flushes every slot would.
func (m *historyMeasurer) segEndSlot() {
	st := m.seg
	sort.Slice(st.slotKeys, func(i, j int) bool { return bytes.Compare(st.slotKeys[i][:], st.slotKeys[j][:]) < 0 })
	buf := st.slotBuf[:0]
	for _, k := range st.slotKeys {
		d := st.slotDiff[k]
		buf = append(buf, k[:]...)
		buf = binary.AppendUvarint(buf, uint64(len(d)))
		buf = append(buf, d...)
	}
	t := time.Now()
	z := m.enc.EncodeAll(buf, nil)
	st.perSlot.ZstdNs += uint64(time.Since(t))
	st.perSlot.Slots++
	st.perSlot.DiffRaw += uint64(len(buf))
	st.perSlot.DiffZstd += uint64(len(z))
	st.slotBuf = buf
	st.slotKeys = st.slotKeys[:0]
	clear(st.slotDiff)
}

func (m *historyMeasurer) closeSegment(sg *hmSegment) {
	keys := make([][32]byte, 0, len(sg.entries))
	for k := range sg.entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	var kf, df [2][]byte // [0] other, [1] vote
	var cls [2]HMClassSeg
	st := &sg.stats
	rt := time.Now()
	for _, k := range keys {
		e := sg.entries[k]
		c := 0
		if m.isVote[k] {
			c = 1
		}
		cls[c].Keys++
		cls[c].Diffs += uint64(len(e.diffs))
		if e.keyframe != nil {
			kf[c] = append(kf[c], k[:]...)
			kf[c] = hmEncodeWhole(kf[c], e.keyframe)
		}
		df[c] = append(df[c], k[:]...)
		df[c] = binary.AppendUvarint(df[c], uint64(len(e.diffs)))
		for _, x := range e.diffs {
			df[c] = binary.AppendUvarint(df[c], uint64(x.off))
			df[c] = binary.AppendUvarint(df[c], uint64(len(x.d)))
			df[c] = append(df[c], x.d...)
		}
		n := uint64(len(e.diffs))
		st.ChainMax = max(st.ChainMax, n)
		if n > 64 {
			st.ChainOver64++
		}
		if n > 256 {
			st.ChainOver256++
		}
		// Rebuild the segment-end version from keyframe plus every diff.
		v := e.keyframe
		ok := true
		for _, x := range e.diffs {
			var err error
			if v, err = hmApplyDiff(v, x.d); err != nil {
				ok = false
				break
			}
		}
		st.Rebuilds++
		st.RebuildDiffs += n
		if !ok || v == nil || !hmSame(v, m.seg.cur[k]) {
			st.RebuildBad++
		}
	}
	st.RebuildNs += uint64(time.Since(rt))
	for c := 0; c < 2; c++ {
		cls[c].KeyframeRaw = uint64(len(kf[c]))
		cls[c].DiffRaw = uint64(len(df[c]))
		t := time.Now()
		kz := m.enc.EncodeAll(kf[c], nil)
		dz := m.enc.EncodeAll(df[c], nil)
		st.CompressNs += uint64(time.Since(t))
		cls[c].KeyframeZstd = uint64(len(kz))
		cls[c].DiffZstd = uint64(len(dz))
		t = time.Now()
		cls[c].KeyframeBest = uint64(len(m.seg.encBest.EncodeAll(kf[c], nil)))
		cls[c].DiffBest = uint64(len(m.seg.encBest.EncodeAll(df[c], nil)))
		st.CompressBest += uint64(time.Since(t))
		t = time.Now()
		for _, z := range [][]byte{kz, dz} {
			if out, err := m.seg.dec.DecodeAll(z, nil); err == nil {
				st.DecompressRaw += uint64(len(out))
			}
		}
		st.DecompressNs += uint64(time.Since(t))
	}
	st.Other.add(cls[0])
	st.Vote.add(cls[1])
	st.Segments++
	st.SlotsCovered = uint64(sg.idx+1) * sg.p
	sg.entries = make(map[[32]byte]*hmSegEntry, len(sg.entries))
	line, _ := json.Marshal(map[string]any{"p": sg.p, "segment": sg.idx, "other": cls[0], "vote": cls[1]})
	m.windows.Write(append(line, '\n'))
}

func (m *historyMeasurer) writeSegments() {
	out := struct {
		PerSlot  HMPerSlot        `json:"per_slot_frames"`
		Segments []HMSegmentStats `json:"segments"`
	}{PerSlot: m.seg.perSlot}
	for _, sg := range m.seg.segments {
		s := sg.stats
		s.P = sg.p
		out.Segments = append(out.Segments, s)
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return
	}
	tmp := filepath.Join(m.dir, "segments.json.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err == nil {
		_ = os.Rename(tmp, filepath.Join(m.dir, "segments.json"))
	}
}

var errHMDiff = errors.New("history measure: malformed diff")

// hmApplyDiff rebuilds a version from prev (nil when absent) and an
// hmEncodeDiff encoding.
func hmApplyDiff(prev *hmVer, d []byte) (*hmVer, error) {
	if len(d) == 0 {
		return nil, errHMDiff
	}
	flags := d[0]
	d = d[1:]
	if flags&hmFlagCreated != 0 {
		if len(d) < 49 {
			return nil, errHMDiff
		}
		v := &hmVer{lamports: binary.LittleEndian.Uint64(d)}
		copy(v.owner[:], d[8:40])
		v.exec = d[40] == 1
		v.rentEpoch = binary.LittleEndian.Uint64(d[41:])
		n, k := binary.Uvarint(d[49:])
		if k <= 0 || 49+k+int(n) > len(d) {
			return nil, errHMDiff
		}
		v.data = bytes.Clone(d[49+k : 49+k+int(n)])
		return v, nil
	}
	if prev == nil || prev.absent {
		return nil, errHMDiff
	}
	v := &hmVer{lamports: prev.lamports, owner: prev.owner, exec: prev.exec, rentEpoch: prev.rentEpoch, data: bytes.Clone(prev.data)}
	if flags&hmFlagLamports != 0 {
		v.lamports = binary.LittleEndian.Uint64(d)
		d = d[8:]
	}
	if flags&hmFlagOwner != 0 {
		copy(v.owner[:], d[:32])
		d = d[32:]
	}
	if flags&hmFlagExec != 0 {
		v.exec = !v.exec
	}
	if flags&hmFlagRent != 0 {
		v.rentEpoch = binary.LittleEndian.Uint64(d)
		d = d[8:]
	}
	if flags&hmFlagLength != 0 {
		n, k := binary.Uvarint(d)
		if k <= 0 {
			return nil, errHMDiff
		}
		d = d[k:]
		if int(n) < len(v.data) {
			v.data = v.data[:n]
		} else {
			v.data = append(v.data, make([]byte, int(n)-len(v.data))...)
		}
	}
	ranges, k := binary.Uvarint(d)
	if k <= 0 {
		return nil, errHMDiff
	}
	d = d[k:]
	pos := 0
	for r := uint64(0); r < ranges; r++ {
		gap, k := binary.Uvarint(d)
		if k <= 0 {
			return nil, errHMDiff
		}
		d = d[k:]
		n, k2 := binary.Uvarint(d)
		if k2 <= 0 || int(n) > len(d)-k2 {
			return nil, errHMDiff
		}
		d = d[k2:]
		pos += int(gap)
		if pos+int(n) > len(v.data) {
			return nil, errHMDiff
		}
		copy(v.data[pos:], d[:n])
		d = d[n:]
		pos += int(n)
	}
	if len(d) != 0 {
		return nil, errHMDiff
	}
	return v, nil
}
