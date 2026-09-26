package sealevel

import (
	"bytes"
	"encoding/binary"

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

// memmoveImplInternal copies n bytes from src to dst inside the VM without a
// temporary buffer. The source is translated first so a bad source address is
// reported before a bad destination, as before. Go's copy has memmove
// semantics, so overlapping ranges within one region are handled; and when
// the destination translation grows or copy-on-writes an account region, the
// source slice still refers to the previous backing buffer, whose bytes are
// exactly what the old read-then-write sequence would have copied.
func memmoveImplInternal(vm sbpf.VM, dst, src, n uint64) error {
	// Agave's touch_slice_mut / translate_slice return empty slices before
	// address lookup for zero length. CU was already charged by the syscall.
	if n == 0 {
		return nil
	}
	srcMem, err := vm.Translate(src, n, false)
	if err != nil {
		return err
	}
	dstMem, err := vm.Translate(dst, n, true)
	if err != nil {
		return err
	}
	copy(dstMem, srcMem)
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

// memcmpResult returns the C memcmp result of two equal-length slices: zero
// when they are equal, otherwise the difference of the first differing bytes
// as unsigned values, matching Agave's `(b1 as i32) - (b2 as i32)`.
func memcmpResult(a, b []byte) int32 {
	if bytes.Equal(a, b) {
		return 0
	}
	// The slices differ: skip equal 8-byte words, then locate the byte.
	i := 0
	for i+8 <= len(a) && binary.LittleEndian.Uint64(a[i:]) == binary.LittleEndian.Uint64(b[i:]) {
		i += 8
	}
	for ; i < len(a); i++ {
		if a[i] != b[i] {
			return int32(a[i]) - int32(b[i])
		}
	}
	return 0
}

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

	cmpResult := memcmpResult(slice1, slice2)

	resultSlice, err := vm.Translate(resultAddr, 4, true)
	if err != nil {
		return syscallErr(err)
	}

	binary.LittleEndian.PutUint32(resultSlice, uint32(cmpResult))

	return syscallSuccess(0)
}

var SyscallMemcmp = sbpf.SyscallFunc4(SyscallMemcmpImpl)

// memsetBytes fills mem with c using the runtime's block clear for zero and a
// doubling copy otherwise, instead of a byte-at-a-time loop.
func memsetBytes(mem []byte, c byte) {
	if len(mem) == 0 {
		return
	}
	if c == 0 {
		clear(mem)
		return
	}
	mem[0] = c
	for filled := 1; filled < len(mem); filled *= 2 {
		copy(mem[filled:], mem[:filled])
	}
}

// SyscallMemsetImpl is the implementation for the memset (sol_memset_) syscall.
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

	memsetBytes(mem, byte(c))

	return syscallSuccess(0)
}

var SyscallMemset = sbpf.SyscallFunc3(SyscallMemsetImpl)

// SyscallAllocFreeImpl is the implementation for the alloc/free (sol_alloc_free_) syscall.
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
