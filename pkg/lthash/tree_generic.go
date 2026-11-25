//go:build !amd64 || purego

package lthash

const useTreeAVX512 = false

func compress16(cv *[8][16]uint32, msg *byte, ctr *[32]uint32, blen, flags uint32) {
	panic("lthash: no 16-lane compression on this platform")
}

func treeRootOutput(m *[16]uint32, dst []byte) bool { return false }
