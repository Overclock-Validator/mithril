package accountsdb

import (
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/util"
)

// PageSize is the alignment used for O_DIRECT I/O.
const PageSize = 4096

// AlignedAlloc allocates a page-aligned byte slice of the given size. This is
// required for O_DIRECT I/O, where both the buffer address and the I/O length
// must be page-aligned.
func AlignedAlloc(size int) []byte {
	// Allocate extra bytes so we can find a page-aligned start within.
	raw := make([]byte, size+PageSize)
	addr := uintptr(unsafe.Pointer(&raw[0]))
	offset := int(util.AlignUp(int(addr), PageSize) - int(addr))
	return raw[offset : offset+size]
}
