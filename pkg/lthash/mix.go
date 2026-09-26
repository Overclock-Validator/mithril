package lthash

// mixInGeneric and mixOutGeneric are the portable lane loops. Every
// architecture-specific implementation must produce identical results: the
// lanes are independent uint16 additions and subtractions modulo 2^16.
func mixInGeneric(dst, src *[numElements]uint16) {
	for i := range numElements {
		dst[i] += src[i]
	}
}

func mixOutGeneric(dst, src *[numElements]uint16) {
	for i := range numElements {
		dst[i] -= src[i]
	}
}
