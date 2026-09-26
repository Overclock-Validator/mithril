//go:build !amd64 || purego

package lthash

func mixIn(dst, src *[numElements]uint16)  { mixInGeneric(dst, src) }
func mixOut(dst, src *[numElements]uint16) { mixOutGeneric(dst, src) }
