package replay

import (
	"bytes"
	"strconv"
	"syscall"
)

// processCounters are cumulative getrusage counters for this process.
type processCounters struct {
	involuntarySwitches, voluntarySwitches uint64
	majorFaults, minorFaults               uint64
}

func readProcessCounters() (processCounters, bool) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return processCounters{}, false
	}
	return processCounters{
		involuntarySwitches: uint64(ru.Nivcsw),
		voluntarySwitches:   uint64(ru.Nvcsw),
		majorFaults:         uint64(ru.Majflt),
		minorFaults:         uint64(ru.Minflt),
	}, true
}

// pressureTotals are the host's cumulative pressure-stall times, microseconds.
type pressureTotals struct {
	cpu, io, memory uint64
}

// parsePressureSomeTotal reads the "some" line's total= field from a
// /proc/pressure file, in microseconds.
func parsePressureSomeTotal(data []byte) (uint64, bool) {
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if !bytes.HasPrefix(line, []byte("some ")) {
			continue
		}
		for _, field := range bytes.Fields(line) {
			if v, ok := bytes.CutPrefix(field, []byte("total=")); ok {
				n, err := strconv.ParseUint(string(v), 10, 64)
				return n, err == nil
			}
		}
	}
	return 0, false
}
