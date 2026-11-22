package metrics

import (
	"strconv"
	"strings"
	"time"
)

// BlockIOCounters mirrors the named Linux block-stat fields. Sector counts use
// 512-byte units, regardless of the filesystem block size. InFlight is a gauge.
// Times are aggregate milliseconds and need not equal elapsed wall time.
type BlockIOCounters struct {
	ReadsCompleted, ReadsMerged, ReadSectors, ReadMilliseconds     uint64
	WritesCompleted, WritesMerged, WriteSectors, WriteMilliseconds uint64
	InFlight, BusyMilliseconds, WeightedMilliseconds               uint64
	DiscardsCompleted, DiscardsMerged, DiscardSectors              uint64
	DiscardMilliseconds, FlushesCompleted, FlushMilliseconds       uint64
}

// BlockIOObservation describes the configured directory's mounted block layer,
// not this process or necessarily a physical drive. Roles on the same device
// share these totals and must not be summed. No device rates are inferred from
// major/minor numbers: they do not establish a device's incarnation.
type BlockIOObservation struct {
	StartedAt, SampledAt time.Time
	Major, Minor         uint32
	Known                bool
	HasDiscard, HasFlush bool
	Counters             BlockIOCounters
}

// BlockIOSampler reads only a supplied operator-configured directory's device.
// It performs no device inventory scan and creates no timers or goroutines.
type BlockIOSampler struct{}

// NewBlockIOSampler performs no OS reads and returns nil when disabled.
func NewBlockIOSampler(enabled bool) *BlockIOSampler {
	if !enabled {
		return nil
	}
	return &BlockIOSampler{}
}

// Sample reports unknown mappings rather than substituting another device.
// Unsupported operating systems return false without reading paths.
func (s *BlockIOSampler) Sample(directory string) (BlockIOObservation, bool) {
	if s == nil {
		return BlockIOObservation{}, false
	}
	return readBlockIO(directory)
}

func blockIOFields(c *BlockIOCounters) [17]*uint64 {
	return [17]*uint64{&c.ReadsCompleted, &c.ReadsMerged, &c.ReadSectors, &c.ReadMilliseconds,
		&c.WritesCompleted, &c.WritesMerged, &c.WriteSectors, &c.WriteMilliseconds,
		&c.InFlight, &c.BusyMilliseconds, &c.WeightedMilliseconds, &c.DiscardsCompleted,
		&c.DiscardsMerged, &c.DiscardSectors, &c.DiscardMilliseconds, &c.FlushesCompleted, &c.FlushMilliseconds}
}

func parseBlockIO(data []byte) (BlockIOCounters, int, error) {
	var counters BlockIOCounters
	if len(data) == 0 || len(data) > 4096 || data[len(data)-1] != '\n' || strings.Contains(string(data[:len(data)-1]), "\n") {
		return counters, 0, errIORecord
	}
	values := strings.Fields(string(data))
	if len(values) != 11 && len(values) != 15 && len(values) != 17 {
		return counters, 0, errIORecord
	}
	fields := blockIOFields(&counters)
	for i, value := range values {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil || strconv.FormatUint(parsed, 10) != value {
			return BlockIOCounters{}, 0, errIORecord
		}
		*fields[i] = parsed
	}
	return counters, len(values), nil
}
