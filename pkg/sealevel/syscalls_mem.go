package sealevel

import (
	"encoding/binary"
	"math/bits"

	//"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/util"
)

func MemOpConsume(execCtx *ExecutionCtx, n uint64) error {
	cost := max(cu.CUMemOpBaseCost, n/cu.CUCpiBytesPerUnit)
	return execCtx.ComputeMeter.Consume(cost)
}

// memmoveImplInternal copies n bytes from src to dst within VM memory.
//
// Translation order matches Agave (mem_ops.rs memmove): the destination is
// translated first (AccessType::Store — this triggers any copy-on-write /
// direct-mapping materialization and, when both operands are invalid, produces
// a Store fault at dst rather than a Load fault at src), then the source. This
// intentionally differs from the previous implementation, which translated src
// first. Because dst is materialized before src is read, there is no stale-src
// hazard. copy() has memmove semantics, so overlapping src/dst is handled
// correctly (used by sol_memmove_; sol_memcpy_ rejects overlap before calling).
func memmoveImplInternal(vm sbpf.VM, dst, src, n uint64) error {
	dstBuf, err := vm.Translate(dst, n, true)
	if err != nil {
		return err
	}
	srcBuf, err := vm.Translate(src, n, false)
	if err != nil {
		return err
	}
	copy(dstBuf, srcBuf)
	return nil
}

// SyscallMemcpyImpl is the implementation of the memcpy (sol_memcpy_) syscall.
// Overlapping src and dst for a given n bytes to be copied results in an error being returned.
func SyscallMemcpyImpl(vm sbpf.VM, dst, src, n uint64) (uint64, error) {
	//mlog.Log.Debugf("SyscallMemcpy")

	execCtx := executionCtx(vm)
	err := MemOpConsume(execCtx, n)
	if err != nil {
		return syscallErr(err)
	}

	// memcpy when src and dst are overlapping results in undefined behaviour,
	// hence check if there is an overlap and return early with an error if so.
	if !isNonOverlapping(src, n, dst, n) {
		return syscallErr(SyscallErrCopyOverlapping)
	}

	if n == 0 {
		return syscallSuccess(0)
	}

	err = memmoveImplInternal(vm, dst, src, n)
	if err != nil {
		return syscallErr(err)
	} else {
		return syscallSuccess(0)
	}
}

var SyscallMemcpy = sbpf.SyscallFunc3(SyscallMemcpyImpl)

// SyscallMemmoveImpl is the implementation for the memmove (sol_memmove_) syscall.
func SyscallMemmoveImpl(vm sbpf.VM, dst, src, n uint64) (uint64, error) {
	//mlog.Log.Debugf("SyscallMemmove")

	execCtx := executionCtx(vm)
	err := MemOpConsume(execCtx, n)
	if err != nil {
		return syscallCuErr()
	}

	err = memmoveImplInternal(vm, dst, src, n)
	if err != nil {
		return syscallErr(err)
	} else {
		return syscallSuccess(0)
	}
}

var SyscallMemmove = sbpf.SyscallFunc3(SyscallMemmoveImpl)

// SyscallMemcmpImpl is the implementation for the memcmp (sol_memcmp_) syscall.
func SyscallMemcmpImpl(vm sbpf.VM, addr1, addr2, n, resultAddr uint64) (uint64, error) {
	//mlog.Log.Debugf("SyscallMemcmp")

	execCtx := executionCtx(vm)
	err := MemOpConsume(execCtx, n)
	if err != nil {
		return syscallCuErr()
	}

	slice1, err := vm.Translate(addr1, n, false)
	if err != nil {
		return syscallErr(err)
	}

	slice2, err := vm.Translate(addr2, n, false)
	if err != nil {
		return syscallErr(err)
	}

	cmpResult := memcmpBytes(slice1, slice2, n)

	resultSlice, err := vm.Translate(resultAddr, 4, true)
	if err != nil {
		return syscallErr(err)
	}

	binary.LittleEndian.PutUint32(resultSlice, uint32(cmpResult))

	return syscallSuccess(0)
}

var SyscallMemcmp = sbpf.SyscallFunc4(SyscallMemcmpImpl)

// SyscallMemcmpImpl is the implementation for the memset (sol_memset_) syscall.
func SyscallMemsetImpl(vm sbpf.VM, dst, c, n uint64) (uint64, error) {
	//mlog.Log.Debugf("SyscallMemset")

	execCtx := executionCtx(vm)
	err := MemOpConsume(execCtx, n)
	if err != nil {
		return syscallCuErr()
	}

	mem, err := vm.Translate(dst, n, true)
	if err != nil {
		return syscallErr(err)
	}

	fillBytes(mem, byte(c))

	return syscallSuccess(0)
}

// memsetDoublingThreshold is the buffer size above which fillBytes switches
// from a simple byte loop to a doubling copy. Below it the loop wins (no call /
// setup overhead); above it copy() (which the runtime lowers to a vectorized
// memmove) dominates.
const memsetDoublingThreshold = 32

// fillBytes sets every byte of dst to v. Equivalent to a byte loop but much
// faster for large buffers: it seeds one byte then repeatedly doubles the
// filled region with copy(), which the Go runtime implements as a vectorized
// memmove. v==0 uses the runtime's optimized memclr.
func fillBytes(dst []byte, v byte) {
	if len(dst) == 0 {
		return
	}
	if v == 0 {
		clear(dst)
		return
	}
	if len(dst) <= memsetDoublingThreshold {
		for i := range dst {
			dst[i] = v
		}
		return
	}
	dst[0] = v
	for filled := 1; filled < len(dst); {
		filled += copy(dst[filled:], dst[:filled])
	}
}

// memcmpBytes compares the first n bytes of a and b, returning
// int32(a[i]) - int32(b[i]) at the first differing byte i (matching Agave's
// memcmp result), or 0 if equal. It compares 8 bytes at a time and locates the
// first differing byte within a mismatching word via the low set bit of the
// XOR (little-endian byte order), then falls back to a byte loop for the tail.
// a and b must each have at least n bytes.
func memcmpBytes(a, b []byte, n uint64) int32 {
	i := uint64(0)
	for ; i+8 <= n; i += 8 {
		x := binary.LittleEndian.Uint64(a[i:])
		y := binary.LittleEndian.Uint64(b[i:])
		if x != y {
			j := i + uint64(bits.TrailingZeros64(x^y)>>3)
			return int32(a[j]) - int32(b[j])
		}
	}
	for ; i < n; i++ {
		if a[i] != b[i] {
			return int32(a[i]) - int32(b[i])
		}
	}
	return 0
}

var SyscallMemset = sbpf.SyscallFunc3(SyscallMemsetImpl)

// SyscallMemcmpImpl is the implementation for the memset (sol_memset_) syscall.
func SyscallAllocFreeImpl(vm sbpf.VM, size, freeAddr uint64) (uint64, error) {
	//mlog.Log.Debugf("SyscallAllocFreeImpl")

	execCtx := executionCtx(vm)

	// this is a free() call, but this is a bump allocator, so do nothing
	if freeAddr != 0 {
		return syscallSuccess(0)
	}

	var align uint64
	if execCtx.CheckAligned() {
		align = 8
	} else {
		align = 1
	}

	heapSize := util.AlignUp(vm.HeapSize(), align)
	heapAddr := safemath.SaturatingAddU64(heapSize, sbpf.VaddrHeap)
	heapSize = safemath.SaturatingAddU64(heapSize, size)

	if heapSize > vm.HeapMax() {
		return syscallSuccess(0)
	}

	vm.UpdateHeapSize(heapSize)

	return syscallSuccess(heapAddr)
}

var SyscallAllocFree = sbpf.SyscallFunc2(SyscallAllocFreeImpl)
