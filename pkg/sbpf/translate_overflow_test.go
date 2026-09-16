package sbpf

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTranslateRejectsOverflowingRange(t *testing.T) {
	ip := &Interpreter{ro: make([]byte, 32), heap: make([]byte, 32), input: make([]byte, 32)}
	for _, base := range []uint64{VaddrProgram, VaddrHeap, VaddrInput} {
		for _, write := range []bool{false, true} {
			for _, offset := range []uint64{1, 31, 33, 0xffffffff} {
				for _, size := range []uint64{^uint64(0), ^uint64(0) - 15} {
					_, err := ip.Translate(base+offset, size, write)
					require.Error(t, err, "base=%x offset=%d size=%d write=%v", base, offset, size, write)
				}
			}
		}
		got, err := ip.Translate(base+31, 1, false)
		require.NoError(t, err)
		require.Len(t, got, 1)
		_, err = ip.Translate(base+31, 2, false)
		require.Error(t, err)
	}
}
