//go:build amd64 && !purego

package lthash

import "golang.org/x/sys/cpu"

// useAVX2 selects the vector lane loops. cpu.X86.HasAVX2 already includes
// the operating-system XSAVE/YMM-state check. Tests flip it to compare the
// two implementations on the same machine.
var useAVX2 = cpu.X86.HasAVX2

func mixIn(dst, src *[numElements]uint16) {
	if useAVX2 {
		mixInAVX2(dst, src)
		return
	}
	mixInGeneric(dst, src)
}

func mixOut(dst, src *[numElements]uint16) {
	if useAVX2 {
		mixOutAVX2(dst, src)
		return
	}
	mixOutGeneric(dst, src)
}

//go:noescape
func mixInAVX2(dst, src *[numElements]uint16)

//go:noescape
func mixOutAVX2(dst, src *[numElements]uint16)
