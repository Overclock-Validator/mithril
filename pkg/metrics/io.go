package metrics

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

// ProcessIOCounters preserves Linux /proc/self/io accounting. Character counts
// include cached and non-disk I/O. WriteBytes counts dirtied-page attribution,
// not completed device writes; CancelledWriteBytes must not be subtracted.
// Linux includes waited-for children in these process-accounting totals.
type ProcessIOCounters struct {
	ReadCharacters, WriteCharacters uint64
	ReadSyscalls, WriteSyscalls     uint64
	ReadBytes, WriteBytes           uint64
	CancelledWriteBytes             uint64
}

// ProcessIOObservation is a bounded read, not an atomic transaction or a disk
// health verdict. Known distinguishes an unreadable source from seven zeros.
type ProcessIOObservation struct {
	StartedAt, SampledAt time.Time
	Counters             ProcessIOCounters
	Known                bool
	Window               ProcessIOWindow
	HasWindow            bool
}

// ProcessIOWindow contains differences across two valid same-process reads.
// Duration is local monotonic elapsed time; Counters are not rates or net writes.
type ProcessIOWindow struct {
	StartedAt, EndedAt time.Time
	Duration           time.Duration
	Counters           ProcessIOCounters
}

// ProcessIOSampler is confined to the existing diagnostics owner. It creates
// no timer, background worker or observation when disabled.
type ProcessIOSampler struct {
	previous ProcessIOObservation
}

// NewProcessIOSampler selects observation before the diagnostics owner starts.
// Construction performs no clock, filesystem or process reads.
func NewProcessIOSampler(enabled bool) *ProcessIOSampler {
	if !enabled {
		return nil
	}
	return &ProcessIOSampler{}
}

// Sample returns false for disabled/unsupported platforms. Read errors remain
// supported observations with unknown counters and clear the interval baseline.
func (s *ProcessIOSampler) Sample() (ProcessIOObservation, bool) {
	if s == nil {
		return ProcessIOObservation{}, false
	}
	o, supported := readProcessIO()
	if !supported {
		return ProcessIOObservation{}, false
	}
	return s.observe(o), true
}

func (s *ProcessIOSampler) observe(o ProcessIOObservation) ProcessIOObservation {
	previous := s.previous
	s.previous = o
	if !o.Known {
		s.previous = ProcessIOObservation{}
		return o
	}
	elapsed := o.SampledAt.Sub(previous.SampledAt)
	if !previous.Known || elapsed <= 0 || elapsed > 30*time.Second {
		return o
	}
	var delta ProcessIOCounters
	current, old, fields := processIOFields(&o.Counters), processIOFields(&previous.Counters), processIOFields(&delta)
	for i, value := range current {
		if *value < *old[i] {
			return o
		}
		*fields[i] = *value - *old[i]
	}
	o.Window, o.HasWindow = ProcessIOWindow{StartedAt: previous.SampledAt, EndedAt: o.SampledAt, Duration: elapsed, Counters: delta}, true
	return o
}

func processIOFields(c *ProcessIOCounters) [7]*uint64 {
	return [7]*uint64{&c.ReadCharacters, &c.WriteCharacters, &c.ReadSyscalls, &c.WriteSyscalls, &c.ReadBytes, &c.WriteBytes, &c.CancelledWriteBytes}
}

var errIORecord = errors.New("invalid I/O observation record")

func parseProcessIO(data []byte) (ProcessIOCounters, error) {
	var result ProcessIOCounters
	fields := processIOFields(&result)
	names := [...]string{"rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes", "cancelled_write_bytes"}
	var seen [7]bool
	if len(data) == 0 || len(data) > 4096 || data[len(data)-1] != '\n' {
		return ProcessIOCounters{}, errIORecord
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok || len(key) == 0 {
			return ProcessIOCounters{}, errIORecord
		}
		for i, name := range names {
			if key != name {
				continue
			}
			value := strings.TrimSpace(value)
			parsed, err := strconv.ParseUint(value, 10, 64)
			if seen[i] || err != nil || strconv.FormatUint(parsed, 10) != value {
				return ProcessIOCounters{}, errIORecord
			}
			seen[i], *fields[i] = true, parsed
		}
	}
	for _, present := range seen {
		if !present {
			return ProcessIOCounters{}, errIORecord
		}
	}
	return result, nil
}

func readProcessIORecord(read func(string, int, bool) ([]byte, error), now func() time.Time) ProcessIOObservation {
	o := ProcessIOObservation{StartedAt: now()}
	data, err := read("/proc/self/io", 4096, false)
	o.SampledAt = now()
	if err != nil || o.SampledAt.Before(o.StartedAt) || o.SampledAt.Sub(o.StartedAt) > time.Second {
		return o
	}
	o.Counters, err = parseProcessIO(data)
	o.Known = err == nil
	return o
}
