//go:build !linux

package metrics

func readInterfaceTraffic() (InterfaceTrafficObservation, bool) {
	return InterfaceTrafficObservation{}, false
}
