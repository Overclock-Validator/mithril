//go:build !linux

package metrics

func readBlockIO(string) (BlockIOObservation, bool) {
	return BlockIOObservation{}, false
}
