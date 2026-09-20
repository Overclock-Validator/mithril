package sbpf

import "unsafe"

// memRegion describes one of the fixed 4 GiB virtual address windows
// (rodata, stack, heap, input) as a contiguous host buffer, so that the
// interpreter can translate the common case without a function call.
//
// rlen / wlen are the readable / writable byte lengths (wlen == 0 for
// read-only windows). gapShift/gapMask implement SBPF v0 stack frame gaps
// exactly like Agave's MemoryRegion::vm_gap_shift (gapShift = 63 and
// gapMask = 0 for windows without gaps, which makes the gap logic a no-op).
// A window that needs special handling (VASA input regions, ...) has
// rlen = wlen = 0 and falls back to translateInternal.
type memRegion struct {
	base     unsafe.Pointer // host address of window offset `start`
	start    uint64         // offset of the region within its 4 GiB window
	rlen     uint64
	wlen     uint64
	gapShift uint64
	gapMask  uint64
	dirty    uint64 // 4 KiB page bitmap of fast-path writes (stack/heap only matter)
}

// emptyRegion never matches any access.
var emptyRegion = memRegion{gapShift: 63}

// A uint64 dirty bitmap can describe exactly 64 pages of 4 KiB.
const fastDirtyBytes = 64 * 4096

const numFastRegions = 6 // index 5 is a permanently empty catch-all

// fastRead returns a host pointer for a size-byte read at vma, or nil if the
// access is not covered by the fast path (caller falls back to Read*).
func (ip *Interpreter) fastRead(vma uint64, size uint64) unsafe.Pointer {
	reg := &ip.regions[min(vma>>32, numFastRegions-1)]
	lo := vma & 0xffffffff
	inGap := (lo >> reg.gapShift) & 1
	// Truncating to 32 bits makes lo < start wrap to a value >= 2^32-start,
	// which is always > rlen (start+rlen < 2^32), so one compare suffices.
	off := uint64(uint32((((lo & reg.gapMask) >> 1) | (lo &^ reg.gapMask)) - reg.start))
	if off+size > reg.rlen || inGap != 0 {
		return nil
	}
	return unsafe.Add(reg.base, off)
}

// fastWrite is the write counterpart of fastRead; it also records the dirty
// range so Finish only needs to zero what was touched.
func (ip *Interpreter) fastWrite(vma uint64, size uint64) unsafe.Pointer {
	reg := &ip.regions[min(vma>>32, numFastRegions-1)]
	lo := vma & 0xffffffff
	inGap := (lo >> reg.gapShift) & 1
	off := uint64(uint32((((lo & reg.gapMask) >> 1) | (lo &^ reg.gapMask)) - reg.start))
	if off+size > reg.wlen || inGap != 0 {
		return nil
	}
	// Mark the 4 KiB page (and, conservatively, the next one, since an access
	// is at most 8 bytes and may straddle a page boundary) as dirty.
	reg.dirty |= 3 << (off >> 12)
	return unsafe.Add(reg.base, off)
}
