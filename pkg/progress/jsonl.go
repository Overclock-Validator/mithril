package progress

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// JSONLEmitter appends one-line JSON progress events for external tools to tail.
// Append-only, mutex-serialized, fail-soft; empty path and nil receiver are no-ops.
type JSONLEmitter struct {
	mu sync.Mutex
	f  *os.File
}

const JSONLFileName = "progress.jsonl"

// NewJSONLEmitter opens path in append+create mode. Empty path returns a no-op
// emitter. Returns a wrapped error if the file cannot be opened.
func NewJSONLEmitter(path string) (*JSONLEmitter, error) {
	if path == "" {
		return &JSONLEmitter{}, nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open jsonl %s: %w", path, err)
	}
	return &JSONLEmitter{f: f}, nil
}

// Emit appends one JSON line, stamping "ts" (RFC3339Nano) and overwriting any
// caller-supplied "ts". Nil/disabled emitters are no-ops; errors go to stderr.
func (e *JSONLEmitter) Emit(event map[string]any) {
	if e == nil {
		return
	}
	if event == nil {
		event = map[string]any{}
	}
	event["ts"] = time.Now().UTC().Format(time.RFC3339Nano)

	data, err := json.Marshal(event)
	if err != nil {
		fmt.Fprintf(os.Stderr, "progress jsonl: marshal failed: %v\n", err)
		return
	}
	data = append(data, '\n')

	// Check e.f under the lock — Close() nils it under the same lock.
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.f == nil {
		return
	}
	if _, err := e.f.Write(data); err != nil {
		fmt.Fprintf(os.Stderr, "progress jsonl: write failed: %v\n", err)
	}
}

// Close closes the underlying file. Safe on a nil/disabled emitter; later Emit
// calls become no-ops.
func (e *JSONLEmitter) Close() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.f != nil {
		_ = e.f.Close()
		e.f = nil
	}
}
