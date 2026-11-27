package accountsdb

// Measurement only (MITHRIL_HISTORY_MEASURE with MITHRIL_HISTORY_MEASURE_XOR=1).
// Models a history store with a full copy of every changed account at each
// segment start, XOR differences between consecutive versions (readable in
// both directions), and an optional chain-depth limit: after D differences an
// account gets another full copy. Compares XOR with forward-only differences
// and times rebuilding versions in both directions.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

var (
	hmXorSegSizes = []uint64{128, 256, 512, 1024, 1280}
	hmXorDepths   = []int{0, 64, 256}
)

type hmXDiff struct {
	f, x []byte // forward-only and XOR encodings of the same change
}

// hmXRun is a chain that starts at a full copy (nil: the account did not exist).
type hmXRun struct {
	kf    *hmVer
	depth bool // the full copy was added by the depth limit
	diffs []hmXDiff
}

type hmXEntry struct{ runs []*hmXRun }

type HMXStats struct {
	P            uint64 `json:"p"`
	Depth        int    `json:"depth_limit"`
	Segments     uint64 `json:"segments"`
	SlotsCovered uint64 `json:"slots_covered"`
	Keys         uint64 `json:"keys"`
	Diffs        uint64 `json:"diffs"`
	StartCopies  uint64 `json:"start_copies"`
	DepthCopies  uint64 `json:"depth_copies"`
	FullRaw      uint64 `json:"full_raw"`
	FullZstd     uint64 `json:"full_zstd"`
	FwdRaw       uint64 `json:"fwd_raw"`
	FwdZstd      uint64 `json:"fwd_zstd"`
	XorRaw       uint64 `json:"xor_raw"`
	XorZstd      uint64 `json:"xor_zstd"`
	RunMax       uint64 `json:"run_max"`
	RunsOver64   uint64 `json:"runs_over_64"`
	RunsOver256  uint64 `json:"runs_over_256"`
	RunsOver1024 uint64 `json:"runs_over_1024"`
	XorApplied   uint64 `json:"xor_applied"`
	XorApplyNs   uint64 `json:"xor_apply_ns"`
	FwdApplied   uint64 `json:"fwd_applied"`
	FwdApplyNs   uint64 `json:"fwd_apply_ns"`
	RebuildBad   uint64 `json:"rebuild_mismatches"`
	ReverseBad   uint64 `json:"reverse_mismatches"`
	Runs         uint64 `json:"runs"`
}

type hmXSeg struct {
	p       uint64
	depth   int
	idx     int64
	entries map[[32]byte]*hmXEntry
	st      HMXStats
}

type hmXorState struct {
	segs []*hmXSeg
}

func newHMXorState() *hmXorState {
	s := &hmXorState{}
	for _, p := range hmXorSegSizes {
		for _, d := range hmXorDepths {
			s.segs = append(s.segs, &hmXSeg{p: p, depth: d, idx: -1, entries: map[[32]byte]*hmXEntry{}})
		}
	}
	return s
}

func (m *historyMeasurer) xorBeginSlot(s uint64) {
	for _, sg := range m.xor.segs {
		idx := int64((s - m.base - 1) / sg.p)
		if sg.idx != idx {
			if sg.idx >= 0 {
				m.closeXorSegment(sg)
			}
			sg.idx = idx
		}
	}
}

// xorChange records that key k became v; it maintains m.seg.cur itself.
func (m *historyMeasurer) xorChange(k [32]byte, v *hmVer) {
	prev := m.seg.cur[k]
	if prev == nil {
		prev = &hmVer{absent: true}
	}
	if !prev.absent && hmSame(prev, v) {
		return
	}
	f, _, _, _ := hmEncodeDiff(nil, prev, v)
	x := hmEncodeXor(nil, prev, v)
	for _, sg := range m.xor.segs {
		e := sg.entries[k]
		if e == nil {
			r := &hmXRun{}
			if !prev.absent {
				r.kf = prev
			}
			e = &hmXEntry{runs: []*hmXRun{r}}
			sg.entries[k] = e
		}
		r := e.runs[len(e.runs)-1]
		if sg.depth > 0 && len(r.diffs) >= sg.depth {
			// The full run ends at prev; store prev whole and chain on from it,
			// so every run is anchored at both ends.
			r = &hmXRun{kf: prev, depth: true}
			e.runs = append(e.runs, r)
		}
		r.diffs = append(r.diffs, hmXDiff{f: f, x: x})
	}
	m.seg.cur[k] = v
}

func (m *historyMeasurer) closeXorSegment(sg *hmXSeg) {
	keys := make([][32]byte, 0, len(sg.entries))
	for k := range sg.entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	var full, fwd, xr []byte
	st := &sg.st
	for _, k := range keys {
		e := sg.entries[k]
		st.Keys++
		for ri, r := range e.runs {
			st.Runs++
			if r.kf != nil {
				if r.depth {
					st.DepthCopies++
				} else {
					st.StartCopies++
				}
				full = append(full, k[:]...)
				full = hmEncodeWhole(full, r.kf)
			}
			n := uint64(len(r.diffs))
			st.Diffs += n
			st.RunMax = max(st.RunMax, n)
			if n > 64 {
				st.RunsOver64++
			}
			if n > 256 {
				st.RunsOver256++
			}
			if n > 1024 {
				st.RunsOver1024++
			}
			fwd = append(fwd, k[:]...)
			xr = append(xr, k[:]...)
			fwd = binary.AppendUvarint(fwd, n)
			xr = binary.AppendUvarint(xr, n)
			for _, d := range r.diffs {
				fwd = binary.AppendUvarint(fwd, uint64(len(d.f)))
				fwd = append(fwd, d.f...)
				xr = binary.AppendUvarint(xr, uint64(len(d.x)))
				xr = append(xr, d.x...)
			}
			// The run ends at the next run's full copy, or at the version the
			// account holds at the end of the segment.
			end := m.seg.cur[k]
			if ri+1 < len(e.runs) {
				end = e.runs[ri+1].kf
			}
			if n == 0 {
				continue
			}
			// Forward-only rebuild.
			t := time.Now()
			v := r.kf
			ok := true
			for _, d := range r.diffs {
				var err error
				if v, err = hmApplyDiff(v, d.f); err != nil {
					ok = false
					break
				}
			}
			st.FwdApplyNs += uint64(time.Since(t))
			st.FwdApplied += n
			// XOR rebuild forward, then back to the start.
			t = time.Now()
			xv := r.kf
			for _, d := range r.diffs {
				var err error
				if xv, err = hmApplyXor(xv, d.x, true); err != nil {
					ok = false
					break
				}
			}
			st.XorApplyNs += uint64(time.Since(t))
			st.XorApplied += n
			if !ok || v == nil || xv == nil || !hmSame(v, end) || !hmSame(xv, end) {
				st.RebuildBad++
				continue
			}
			back := end
			for i := len(r.diffs) - 1; i >= 0; i-- {
				var err error
				if back, err = hmApplyXor(back, r.diffs[i].x, false); err != nil {
					break
				}
			}
			if (r.kf == nil) != (back == nil) || (r.kf != nil && !hmSame(back, r.kf)) {
				st.ReverseBad++
			}
		}
	}
	st.FullRaw += uint64(len(full))
	st.FwdRaw += uint64(len(fwd))
	st.XorRaw += uint64(len(xr))
	st.FullZstd += uint64(len(m.enc.EncodeAll(full, nil)))
	st.FwdZstd += uint64(len(m.enc.EncodeAll(fwd, nil)))
	st.XorZstd += uint64(len(m.enc.EncodeAll(xr, nil)))
	st.Segments++
	st.SlotsCovered = uint64(sg.idx+1) * sg.p
	sg.entries = make(map[[32]byte]*hmXEntry, len(sg.entries))
}

func (m *historyMeasurer) writeXor() {
	var out []HMXStats
	for _, sg := range m.xor.segs {
		s := sg.st
		s.P, s.Depth = sg.p, sg.depth
		out = append(out, s)
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return
	}
	tmp := filepath.Join(m.dir, "xor.json.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err == nil {
		_ = os.Rename(tmp, filepath.Join(m.dir, "xor.json"))
	}
}

// XOR difference: flags, header fields XORed (owner, lamports, rent epoch;
// executable as a flag), old and new data lengths when they differ, XORed
// byte ranges over the common length, then the bytes past it (new bytes when
// the account grew, old bytes when it shrank). The same encoding turns the old
// version into the new one and back.
const (
	hmXLamports = 1 << iota
	hmXOwner
	hmXExec
	hmXRent
	hmXLength
	hmXCreated
)

func hmEncodeXor(dst []byte, prev, cur *hmVer) []byte {
	if prev.absent {
		dst = append(dst, hmXCreated)
		return hmEncodeWhole(dst, cur)
	}
	var flags byte
	var hdr []byte
	if l := prev.lamports ^ cur.lamports; l != 0 {
		flags |= hmXLamports
		hdr = binary.LittleEndian.AppendUint64(hdr, l)
	}
	if prev.owner != cur.owner {
		flags |= hmXOwner
		for i := range prev.owner {
			hdr = append(hdr, prev.owner[i]^cur.owner[i])
		}
	}
	if prev.exec != cur.exec {
		flags |= hmXExec
	}
	if r := prev.rentEpoch ^ cur.rentEpoch; r != 0 {
		flags |= hmXRent
		hdr = binary.LittleEndian.AppendUint64(hdr, r)
	}
	p, c := prev.data, cur.data
	if len(p) != len(c) {
		flags |= hmXLength
		hdr = binary.AppendUvarint(hdr, uint64(len(p)))
		hdr = binary.AppendUvarint(hdr, uint64(len(c)))
	}
	dst = append(dst, flags)
	dst = append(dst, hdr...)
	m := min(len(p), len(c))
	var body []byte
	ranges := 0
	last, i := 0, 0
	for i < m {
		for i+8 <= m && binary.LittleEndian.Uint64(p[i:]) == binary.LittleEndian.Uint64(c[i:]) {
			i += 8
		}
		for i < m && p[i] == c[i] {
			i++
		}
		if i >= m {
			break
		}
		start, lastDiff := i, i
		for i < m {
			if p[i] != c[i] {
				lastDiff = i
			} else if i-lastDiff > hmGapMerge {
				break
			}
			i++
		}
		end := lastDiff + 1
		body = binary.AppendUvarint(body, uint64(start-last))
		body = binary.AppendUvarint(body, uint64(end-start))
		for j := start; j < end; j++ {
			body = append(body, p[j]^c[j])
		}
		ranges++
		last, i = end, end
	}
	dst = binary.AppendUvarint(dst, uint64(ranges))
	dst = append(dst, body...)
	if len(c) > m {
		dst = append(dst, c[m:]...)
	} else if len(p) > m {
		dst = append(dst, p[m:]...)
	}
	return dst
}

// hmApplyXor turns base into the other version of a change: forward from the
// old version, or back from the new one. nil stands for an absent account.
func hmApplyXor(base *hmVer, d []byte, forward bool) (*hmVer, error) {
	if len(d) == 0 {
		return nil, errHMDiff
	}
	flags := d[0]
	d = d[1:]
	if flags&hmXCreated != 0 {
		if !forward {
			return nil, nil
		}
		return hmApplyDiff(nil, append([]byte{hmFlagCreated}, d...))
	}
	if base == nil || base.absent {
		return nil, errHMDiff
	}
	v := &hmVer{lamports: base.lamports, owner: base.owner, exec: base.exec, rentEpoch: base.rentEpoch}
	if flags&hmXLamports != 0 {
		v.lamports ^= binary.LittleEndian.Uint64(d)
		d = d[8:]
	}
	if flags&hmXOwner != 0 {
		for i := range v.owner {
			v.owner[i] ^= d[i]
		}
		d = d[32:]
	}
	if flags&hmXExec != 0 {
		v.exec = !v.exec
	}
	if flags&hmXRent != 0 {
		v.rentEpoch ^= binary.LittleEndian.Uint64(d)
		d = d[8:]
	}
	oldLen, newLen := len(base.data), len(base.data)
	if flags&hmXLength != 0 {
		o, k := binary.Uvarint(d)
		if k <= 0 {
			return nil, errHMDiff
		}
		d = d[k:]
		nl, k2 := binary.Uvarint(d)
		if k2 <= 0 {
			return nil, errHMDiff
		}
		d = d[k2:]
		oldLen, newLen = int(o), int(nl)
	}
	from, to := oldLen, newLen
	if !forward {
		from, to = newLen, oldLen
	}
	if len(base.data) != from {
		return nil, errHMDiff
	}
	m := min(oldLen, newLen)
	v.data = make([]byte, m, to)
	copy(v.data, base.data[:m])
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
		if pos+int(n) > m {
			return nil, errHMDiff
		}
		for j := 0; j < int(n); j++ {
			v.data[pos+j] ^= d[j]
		}
		d = d[n:]
		pos += int(n)
	}
	if to > m {
		if len(d) != to-m {
			return nil, errHMDiff
		}
		v.data = append(v.data, d...)
	} else if len(d) != max(oldLen, newLen)-m {
		return nil, errHMDiff
	}
	return v, nil
}
