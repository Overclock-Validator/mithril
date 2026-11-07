//go:build linux

package metrics

import (
	"strconv"
	"time"
)

func readProcessIO() (ProcessIOObservation, bool) {
	// The kernel documents torn 64-bit reads on 32-bit architectures. Do not
	// present those counters as an exact source on an unsupported word size.
	if strconv.IntSize != 64 {
		return ProcessIOObservation{}, false
	}
	return readProcessIORecord(readHostFile, time.Now), true
}
