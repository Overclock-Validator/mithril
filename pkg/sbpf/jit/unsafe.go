package jit

import "unsafe"

func unsafeSliceAddr(b []byte) unsafe.Pointer {
	return unsafe.Pointer(unsafe.SliceData(b))
}
