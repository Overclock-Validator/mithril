// Package spinlock acquires sync mutexes with a short bounded spin before
// falling back to the parking lock.
//
// sync.RWMutex never spins, and sync.Mutex spins only briefly and not when the
// local run queue is non-empty. Replay workers hold the locks this is used for
// for a few hundred nanoseconds, but a parked goroutine needs a scheduler
// round trip to wake (tens to hundreds of microseconds while every P is busy).
// Retrying the Try variants for a few microseconds avoids most of those parks.
package spinlock

import (
	"os"
	"strconv"
	"sync"
)

// defaultAttempts bounds the spin at roughly 3 to 6 microseconds on current
// x86 cores before the caller parks as it would have without spinning.
const defaultAttempts = 32

// attempts is MITHRIL_LOCK_SPIN when set (0 disables spinning).
var attempts = func() int {
	n, err := strconv.Atoi(os.Getenv("MITHRIL_LOCK_SPIN"))
	if err != nil || n < 0 {
		return defaultAttempts
	}
	return min(n, 1024)
}()

// backoff pauses for a number of PAUSE instructions that doubles per attempt,
// capped so a spinner rechecks the lock at least every ~200 ns.
func backoff(attempt int) {
	for range 1 << min(attempt, 4) {
		pause()
	}
}

// Lock acquires mu, spinning briefly on TryLock before blocking.
func Lock(mu *sync.Mutex) {
	if mu.TryLock() {
		return
	}
	for i := range attempts {
		backoff(i)
		if mu.TryLock() {
			return
		}
	}
	mu.Lock()
}

// RLock read-locks mu, spinning briefly on TryRLock before blocking.
func RLock(mu *sync.RWMutex) {
	if mu.TryRLock() {
		return
	}
	for i := range attempts {
		backoff(i)
		if mu.TryRLock() {
			return
		}
	}
	mu.RLock()
}

// WLock write-locks mu, spinning briefly on TryLock before blocking.
func WLock(mu *sync.RWMutex) {
	if mu.TryLock() {
		return
	}
	for i := range attempts {
		backoff(i)
		if mu.TryLock() {
			return
		}
	}
	mu.Lock()
}
