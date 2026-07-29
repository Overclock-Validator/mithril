package accounts

import (
	"fmt"
	"testing"
)

// resizeOldFill reproduces the pre-fix extend path so the two can be compared
// in one binary. make already returns zeroed memory; this walked the new tail
// writing fillVal a byte at a time on top of it.
func resizeOldFill(a *Account, newLen uint64, fillVal byte) {
	currentDataLen := uint64(len(a.Data))
	if newLen > currentDataLen {
		newData := make([]byte, newLen)
		copy(newData, a.Data)
		for count := currentDataLen; count < newLen; count++ {
			newData[count] = fillVal
		}
		a.Data = newData
		return
	}
	a.Data = a.Data[:newLen]
}

// resizeSizes spans the range that actually occurs. 165 bytes is an SPL token
// account, which is overwhelmingly the most common allocation on mainnet; the
// upper end is MAX_PERMITTED_DATA_LENGTH, reachable by a program deploy.
var resizeSizes = []uint64{
	165,       // SPL token account / ATA
	1 << 10,   // 1 KiB
	10 << 10,  // 10 KiB, the per-instruction realloc cap
	100 << 10, // 100 KiB
	1 << 20,   // 1 MiB
	10 << 20,  // 10 MiB, MAX_PERMITTED_DATA_LENGTH
}

// BenchmarkResizeZeroFill compares the shipped Resize against the old one at
// each size. The gap is entirely the redundant second pass over the tail: both
// variants call make, which already zeroes.
func BenchmarkResizeZeroFill(b *testing.B) {
	for _, size := range resizeSizes {
		b.Run(fmt.Sprintf("size=%d/old", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				acct := Account{}
				resizeOldFill(&acct, size, 0)
			}
		})
		b.Run(fmt.Sprintf("size=%d/new", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				acct := Account{}
				acct.Resize(size, 0)
			}
		})
	}
}
