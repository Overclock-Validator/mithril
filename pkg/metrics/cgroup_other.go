//go:build !linux

package metrics

func readCgroup() (CgroupObservation, cgroupBinding, bool) {
	return CgroupObservation{}, cgroupBinding{}, false
}
