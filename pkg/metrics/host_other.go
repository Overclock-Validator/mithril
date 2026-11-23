//go:build !linux

package metrics

func readHost() (HostObservation, string, bool) {
	return HostObservation{}, "", false
}
