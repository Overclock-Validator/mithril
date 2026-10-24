package sealevel

import "strings"

const (
	maxRecordedLogBytes   = 10_000
	maxRecordedLogEntries = 10_000
)

type Logger interface {
	Log(s string)
}

// LogRecorder retains messages in call order. Its zero value is unbounded.
// Logs must not be modified while recording.
type LogRecorder struct {
	Logs []string

	bounded      bool
	bytesWritten int
	truncated    bool
}

// NewBoundedLogRecorder retains fewer than 10,000 content bytes and at most
// 10,001 entries. It inserts one "Log truncated" marker when the byte or
// 10,000-entry threshold is reached. The marker counts toward the entry
// threshold, but not the byte budget. Later messages may fit after a rejection.
// This does not limit allocations made by callers before Log.
func NewBoundedLogRecorder() *LogRecorder {
	return &LogRecorder{bounded: true}
}

func (r *LogRecorder) Log(s string) {
	if !r.bounded {
		r.Logs = append(r.Logs, s)
		return
	}
	if len(r.Logs) >= maxRecordedLogEntries || len(s) >= maxRecordedLogBytes-r.bytesWritten {
		if !r.truncated {
			r.Logs = append(r.Logs, "Log truncated")
			r.truncated = true
		}
		return
	}
	r.bytesWritten += len(s)
	r.Logs = append(r.Logs, strings.Clone(s))
}
