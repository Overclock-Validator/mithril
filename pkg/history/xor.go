// Package history keeps past account versions so that RPC can read an
// account as of any recent slot, and replay a past block from exact state.
//
// Each fold hands the store the per-slot versions it is folding. The store
// writes one immutable chunk per fold: for every account the fold changed, a
// full copy of its version before the first change in the current segment
// (only the first time the segment sees it), then one XOR difference per slot
// that changed it. A segment is a run of consecutive chunks; full copies
// restart at every segment, so old segments can be deleted whole.
//
// The store is a record of the past, not of current state: the accounts
// database stays the source of current values.
package history

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/bits"
)

// Version is one account state. Absent means the account did not exist.
type Version struct {
	Lamports   uint64
	Owner      [32]byte
	Executable bool
	RentEpoch  uint64
	Data       []byte
	Absent     bool
}

// Same reports whether two versions are identical.
func Same(a, b *Version) bool {
	if a.Absent || b.Absent {
		return a.Absent == b.Absent
	}
	return a.Lamports == b.Lamports && a.Owner == b.Owner && a.Executable == b.Executable &&
		a.RentEpoch == b.RentEpoch && bytes.Equal(a.Data, b.Data)
}

// Clone returns a deep copy.
func (v *Version) Clone() *Version {
	c := *v
	c.Data = bytes.Clone(v.Data)
	return &c
}

var errDiff = errors.New("history: malformed difference")

// XOR difference flags.
const (
	xLamports = 1 << iota
	xOwner
	xExec
	xRent
	xLength
	xCreated // previous version absent: the encoding holds the whole new version
	xDeleted // new version absent: the encoding holds the whole old version
)

// gapMerge keeps short equal runs inside one range: a range header costs
// about as much as a few bytes.
const gapMerge = 8

// AppendWhole appends a version's full encoding: lamports, owner, executable,
// rent epoch, data length, data.
func AppendWhole(dst []byte, v *Version) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, v.Lamports)
	dst = append(dst, v.Owner[:]...)
	if v.Executable {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.LittleEndian.AppendUint64(dst, v.RentEpoch)
	dst = binary.AppendUvarint(dst, uint64(len(v.Data)))
	return append(dst, v.Data...)
}

// ReadWhole decodes AppendWhole's encoding and returns the rest.
func ReadWhole(b []byte) (*Version, []byte, error) {
	if len(b) < 49 {
		return nil, nil, errDiff
	}
	v := &Version{Lamports: binary.LittleEndian.Uint64(b)}
	copy(v.Owner[:], b[8:40])
	v.Executable = b[40] == 1
	v.RentEpoch = binary.LittleEndian.Uint64(b[41:])
	n, k := binary.Uvarint(b[49:])
	if k <= 0 || uint64(len(b)-49-k) < n {
		return nil, nil, errDiff
	}
	start := 49 + k
	v.Data = bytes.Clone(b[start : start+int(n)])
	return v, b[start+int(n):], nil
}

// AppendXor appends the XOR difference between prev and cur. The same bytes
// turn prev into cur (forward) and cur into prev (backward).
func AppendXor(dst []byte, prev, cur *Version) []byte {
	switch {
	case prev.Absent && cur.Absent:
		return append(dst, 0, 0)
	case prev.Absent:
		return AppendWhole(append(dst, xCreated), cur)
	case cur.Absent:
		return AppendWhole(append(dst, xDeleted), prev)
	}
	var flags byte
	var hdr [96]byte
	h := hdr[:0]
	if l := prev.Lamports ^ cur.Lamports; l != 0 {
		flags |= xLamports
		h = binary.LittleEndian.AppendUint64(h, l)
	}
	if prev.Owner != cur.Owner {
		flags |= xOwner
		for i := range prev.Owner {
			h = append(h, prev.Owner[i]^cur.Owner[i])
		}
	}
	if prev.Executable != cur.Executable {
		flags |= xExec
	}
	if r := prev.RentEpoch ^ cur.RentEpoch; r != 0 {
		flags |= xRent
		h = binary.LittleEndian.AppendUint64(h, r)
	}
	p, c := prev.Data, cur.Data
	if len(p) != len(c) {
		flags |= xLength
		h = binary.AppendUvarint(h, uint64(len(p)))
		h = binary.AppendUvarint(h, uint64(len(c)))
	}
	dst = append(dst, flags)
	dst = append(dst, h...)
	m := min(len(p), len(c))
	// Ranges are written after counting them, so build them in place behind a
	// reserved count and move them if the count needs more than one byte.
	countAt := len(dst)
	dst = append(dst, 0)
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
			} else if i-lastDiff > gapMerge {
				break
			}
			i++
		}
		end := lastDiff + 1
		dst = binary.AppendUvarint(dst, uint64(start-last))
		dst = binary.AppendUvarint(dst, uint64(end-start))
		for j := start; j < end; j++ {
			dst = append(dst, p[j]^c[j])
		}
		ranges++
		last, i = end, end
	}
	if n := uvarintLen(uint64(ranges)); n == 1 {
		dst[countAt] = byte(ranges)
	} else {
		body := bytes.Clone(dst[countAt+1:])
		dst = binary.AppendUvarint(dst[:countAt], uint64(ranges))
		dst = append(dst, body...)
	}
	if len(c) > m {
		dst = append(dst, c[m:]...)
	} else if len(p) > m {
		dst = append(dst, p[m:]...)
	}
	return dst
}

func uvarintLen(x uint64) int { return (bits.Len64(x|1) + 6) / 7 }

// ApplyXor applies d to v in place, forward (old to new) or backward (new to
// old). v's data buffer is reused when it has room.
func ApplyXor(v *Version, d []byte, forward bool) error {
	if len(d) == 0 {
		return errDiff
	}
	flags := d[0]
	d = d[1:]
	if flags == 0 && len(d) == 1 && d[0] == 0 {
		return nil // absent to absent
	}
	if flags&(xCreated|xDeleted) != 0 {
		toWhole := (flags&xCreated != 0) == forward
		if !toWhole {
			*v = Version{Absent: true}
			return nil
		}
		w, rest, err := ReadWhole(d)
		if err != nil || len(rest) != 0 {
			return errDiff
		}
		*v = *w
		return nil
	}
	if v.Absent {
		return errDiff
	}
	if flags&xLamports != 0 {
		if len(d) < 8 {
			return errDiff
		}
		v.Lamports ^= binary.LittleEndian.Uint64(d)
		d = d[8:]
	}
	if flags&xOwner != 0 {
		if len(d) < 32 {
			return errDiff
		}
		for i := range v.Owner {
			v.Owner[i] ^= d[i]
		}
		d = d[32:]
	}
	if flags&xExec != 0 {
		v.Executable = !v.Executable
	}
	if flags&xRent != 0 {
		if len(d) < 8 {
			return errDiff
		}
		v.RentEpoch ^= binary.LittleEndian.Uint64(d)
		d = d[8:]
	}
	oldLen, newLen := len(v.Data), len(v.Data)
	if flags&xLength != 0 {
		o, k := binary.Uvarint(d)
		if k <= 0 {
			return errDiff
		}
		d = d[k:]
		n, k2 := binary.Uvarint(d)
		if k2 <= 0 {
			return errDiff
		}
		d = d[k2:]
		oldLen, newLen = int(o), int(n)
	}
	from, to := oldLen, newLen
	if !forward {
		from, to = newLen, oldLen
	}
	if len(v.Data) != from {
		return errDiff
	}
	m := min(oldLen, newLen)
	ranges, k := binary.Uvarint(d)
	if k <= 0 {
		return errDiff
	}
	d = d[k:]
	pos := 0
	for r := uint64(0); r < ranges; r++ {
		gap, k := binary.Uvarint(d)
		if k <= 0 {
			return errDiff
		}
		d = d[k:]
		n, k2 := binary.Uvarint(d)
		if k2 <= 0 || n > uint64(len(d)-k2) {
			return errDiff
		}
		d = d[k2:]
		pos += int(gap)
		if pos+int(n) > m {
			return errDiff
		}
		xorInto(v.Data[pos:pos+int(n)], d[:n])
		d = d[n:]
		pos += int(n)
	}
	tail := max(oldLen, newLen) - m
	if len(d) != tail {
		return errDiff
	}
	if to > m {
		v.Data = append(v.Data[:m], d...)
	} else {
		v.Data = v.Data[:m]
	}
	return nil
}

func xorInto(dst, src []byte) {
	i := 0
	for ; i+8 <= len(src); i += 8 {
		binary.LittleEndian.PutUint64(dst[i:], binary.LittleEndian.Uint64(dst[i:])^binary.LittleEndian.Uint64(src[i:]))
	}
	for ; i < len(src); i++ {
		dst[i] ^= src[i]
	}
}
