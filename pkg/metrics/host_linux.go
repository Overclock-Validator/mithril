//go:build linux

package metrics

import "time"

func readHost() (HostObservation, string, bool) {
	o, online := readHostFiles(readHostFile, time.Now)
	return o, online, true
}
