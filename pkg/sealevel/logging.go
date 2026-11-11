package sealevel

type Logger interface {
	Log(s string)
}

// LogDiscarder is implemented by a Logger that drops every line. The log
// syscalls then skip formatting the line. They still charge the same compute
// units and fail on the same inputs.
type LogDiscarder interface {
	DiscardsLogs() bool
}

// logsDiscarded reports whether the context's logger drops every line.
func (execCtx *ExecutionCtx) logsDiscarded() bool {
	d, ok := execCtx.Log.(LogDiscarder)
	return ok && d.DiscardsLogs()
}

type LogRecorder struct {
	Logs []string
}

func (r *LogRecorder) Log(s string) {
	r.Logs = append(r.Logs, s)
}
