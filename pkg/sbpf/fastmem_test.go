package sbpf

import (
	"math/rand"
	"testing"
	"unsafe"
)

// Keep the previous translation formula independent of the optimized one:
// compare accepted pointers and dirty tracking, including accesses that fall
// back to the full mapper. The fast path is used only for 1/2/4/8-byte accesses.
func TestFastMemoryTranslationMatchesPrevious(t *testing.T) {
	data := make([]byte, 4*StackFrameSize)
	for _, gaps := range []bool{false, true} {
		for _, start := range []uint64{0, 4096, 0xffff0000} {
			if gaps && start != 0 {
				continue
			} // Only the stack has gaps.
			for _, writable := range []bool{false, true} {
				reg := memRegion{base: unsafe.Pointer(&data[0]), start: start, rlen: uint64(len(data))}
				shift := uint64(63)
				if gaps {
					reg.gapBit = StackFrameSize
					reg.gapMask = GapMask
					shift = 12
				}
				if writable {
					reg.wlen = reg.rlen
				}
				rng := rand.New(rand.NewSource(42))
				offsets := []uint64{0, start, start - 1, start + reg.rlen - 1, start + reg.rlen, 0xffffffff}
				// Cover both sides of every stack gap/page boundary and access width.
				for page := uint64(0); page <= 8; page++ {
					for delta := int64(-8); delta <= 8; delta++ {
						offsets = append(offsets, uint64(int64(start+page*StackFrameSize)+delta)&0xffffffff)
					}
				}
				for i := 0; i < 1000; i++ {
					offsets = append(offsets, uint64(rng.Uint32()), start+uint64(rng.Intn(8*StackFrameSize)))
				}
				for _, lo := range offsets {
					lo &= 0xffffffff
					for _, size := range []uint64{1, 2, 4, 8} {
						oldOff := uint64(uint32((((lo & reg.gapMask) >> 1) | (lo &^ reg.gapMask)) - reg.start))
						oldGap := (lo>>shift)&1 != 0
						var wantRead, wantWrite unsafe.Pointer
						var wantDirty uint64
						if !oldGap && oldOff+size <= reg.rlen {
							wantRead = unsafe.Add(reg.base, oldOff)
						}
						if !oldGap && oldOff+size <= reg.wlen {
							wantWrite = unsafe.Add(reg.base, oldOff)
							wantDirty = 3 << (oldOff >> 12)
						}
						ip := Interpreter{}
						ip.regions[VaddrStack>>32] = reg
						gotRead := ip.fastRead(VaddrStack+lo, size)
						gotWrite := ip.fastWrite(VaddrStack+lo, size)
						if gotRead != wantRead || gotWrite != wantWrite || ip.regions[VaddrStack>>32].dirty != wantDirty {
							t.Fatalf("gaps=%t start=%x write=%t offset=%x size=%d: translation or dirty pages differ", gaps, start, writable, lo, size)
						}
					}
				}
			}
		}
	}
}
