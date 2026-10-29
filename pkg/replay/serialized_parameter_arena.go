package replay

import (
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/arena"
)

// Banks alternate between two parameter arenas. Opening a bank switches to the
// arena that was cleared in the background since its last use and starts
// clearing the one the previous bank used, so the bank does not wait for the
// previous bank's 100-300 MB of zeroing. A bank that opens before that clearing
// has finished (fast catch-up blocks) makes the cleaner drop its pauses and
// finish at full speed, then waits for it.
var parameterArenas struct {
	active, spare *arena.Arena[byte]
	spareClean    chan struct{} // closed once spare is zeroed
	hurry         *atomic.Bool  // set when a bank is waiting for spareClean
}

// Clear 4 MiB at a time with a short pause after each piece, so the clearing
// does not slow the next bank's account loading with a burst at full memory
// bandwidth. On Linux a 100 µs sleep lasts about 1 ms, so a 150 MB prefix takes
// about 40 ms paced (2-3 ms unpaced): longer than a typical catch-up block,
// hence the hurry signal.
const (
	parameterArenaClearChunk = 4 << 20
	parameterArenaClearPause = 100 * time.Microsecond
)

// parameterArenaPause runs between cleared chunks unless the cleaner is hurried
// (replaced in tests).
var parameterArenaPause = func() { time.Sleep(parameterArenaClearPause) }

// Called only after the preceding bank's users of parameter memory have finished.
func resetSerializedParameterArena() {
	if SerializedParameterArena == nil {
		return
	}
	pair := &parameterArenas
	if pair.active != SerializedParameterArena {
		// First reset, or the arena was replaced: pair it with a fresh spare.
		pair.active = SerializedParameterArena
		pair.spare = arena.New[byte](SerializedParameterArena.Capacity())
		pair.spareClean = make(chan struct{})
		pair.hurry = new(atomic.Bool)
		close(pair.spareClean)
	}
	pair.hurry.Store(true)
	<-pair.spareClean
	dirty := pair.active
	pair.active, pair.spare = pair.spare, dirty
	cleaned, hurry := make(chan struct{}), new(atomic.Bool)
	pair.spareClean, pair.hurry = cleaned, hurry
	go func() {
		dirty.ResetPaced(parameterArenaClearChunk, func() {
			if !hurry.Load() {
				parameterArenaPause()
			}
		})
		close(cleaned)
	}()
	SerializedParameterArena = pair.active
}

// waitSerializedParameterArenaCleared waits for the background clearing of the
// spare arena (tests).
func waitSerializedParameterArenaCleared() {
	if parameterArenas.spareClean != nil {
		parameterArenas.hurry.Store(true)
		<-parameterArenas.spareClean
	}
}
