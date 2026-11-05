package metrics

import (
	"math"
	"runtime"
	"strconv"
	"strings"
)

// ProcessRSSBreakdown is the kernel's approximate resident-memory split.
// It is not Go heap usage or a measurement of private/exclusive memory.
type ProcessRSSBreakdown struct {
	AnonymousBytes uint64
	FileBytes      uint64
	SharedBytes    uint64
}

// ReadProcessRSSBreakdown reads only the current process on Linux. Unsupported,
// missing, oversized, or malformed observations return false, never zero data.
func ReadProcessRSSBreakdown() (ProcessRSSBreakdown, bool) {
	if runtime.GOOS != "linux" {
		return ProcessRSSBreakdown{}, false
	}
	data, err := readHostFile("/proc/self/status", 16*1024, false)
	if err != nil {
		return ProcessRSSBreakdown{}, false
	}
	value, err := parseProcessRSSBreakdown(data)
	return value, err == nil
}

func parseProcessRSSBreakdown(data []byte) (ProcessRSSBreakdown, error) {
	var value ProcessRSSBreakdown
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return value, errHostRecord
	}
	var seen uint8
	for line := range strings.SplitSeq(string(data), "\n") {
		key, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		var target *uint64
		var bit uint8
		switch key {
		case "RssAnon":
			target, bit = &value.AnonymousBytes, 1
		case "RssFile":
			target, bit = &value.FileBytes, 2
		case "RssShmem":
			target, bit = &value.SharedBytes, 4
		default:
			continue
		}
		fields := strings.Fields(rest)
		if seen&bit != 0 || len(fields) != 2 || fields[1] != "kB" {
			return ProcessRSSBreakdown{}, errHostRecord
		}
		n, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil || n > math.MaxUint64/1024 {
			return ProcessRSSBreakdown{}, errHostRecord
		}
		*target, seen = n*1024, seen|bit
	}
	if seen != 7 {
		return ProcessRSSBreakdown{}, errHostRecord
	}
	return value, nil
}
