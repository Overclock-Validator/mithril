package dashboardcmd

import "sync"

// ringBuffer keeps the tail of a process's stderr (oldest bytes dropped past
// cap) so a fast-failing start can surface its real error. Concurrent-safe.
type ringBuffer struct {
	mu  sync.Mutex
	buf []byte
	cap int
}

func newRingBuffer(capacity int) *ringBuffer {
	return &ringBuffer{cap: capacity}
}

// Write appends p, evicting from the head to stay within cap. Always reports
// len(p) (never short-writes) for io.MultiWriter.
func (r *ringBuffer) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf = append(r.buf, p...)
	if over := len(r.buf) - r.cap; over > 0 {
		r.buf = r.buf[over:]
	}
	return len(p), nil
}

// String returns a snapshot of the current buffer content.
func (r *ringBuffer) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return string(r.buf)
}
