//go:build !linux

package metrics

func readProcessIO() (ProcessIOObservation, bool) {
	return ProcessIOObservation{}, false
}
