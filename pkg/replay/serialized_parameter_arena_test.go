package replay

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/stretchr/testify/require"
)

// Banks alternate between two arenas; each is fully zeroed in the background
// before it is handed to a bank again.
func TestSerializedParameterArenaAlternatesClearedArenas(t *testing.T) {
	oldArena, oldPair := SerializedParameterArena, parameterArenas
	t.Cleanup(func() {
		waitSerializedParameterArenaCleared()
		SerializedParameterArena, parameterArenas = oldArena, oldPair
	})
	first := arena.New[byte](4096)
	SerializedParameterArena = first
	fill := func(n uint64, v byte) []byte {
		data, ok := SerializedParameterArena.AllocN(n)
		require.True(t, ok)
		for i := range data {
			data[i] = v
		}
		return data
	}
	a := fill(1000, 1)

	resetSerializedParameterArena()
	second := SerializedParameterArena
	require.NotSame(t, first, second)
	require.EqualValues(t, 4096, second.Capacity())
	require.Zero(t, second.Used())
	b := fill(3000, 2) // more than the first bank used
	waitSerializedParameterArenaCleared()
	require.Equal(t, make([]byte, 1000), a)

	resetSerializedParameterArena()
	require.Same(t, first, SerializedParameterArena)
	require.Zero(t, first.Used())
	whole, ok := first.AllocN(4096)
	require.True(t, ok)
	require.Equal(t, make([]byte, 4096), whole, "an arena is handed out only after it is fully zeroed")
	waitSerializedParameterArenaCleared()
	require.Equal(t, make([]byte, 3000), b)

	// Replacing the arena (node start, tests) pairs the new one with a fresh spare.
	replacement := arena.New[byte](512)
	SerializedParameterArena = replacement
	resetSerializedParameterArena()
	require.NotSame(t, replacement, SerializedParameterArena)
	require.NotSame(t, first, SerializedParameterArena)
	require.NotSame(t, second, SerializedParameterArena)
	require.EqualValues(t, 512, SerializedParameterArena.Capacity())
}

// A bank that opens while the spare is still being cleared must not sit out the
// cleaner's pauses (about 1 ms each on Linux, roughly 40 ms for 150 MB): the
// cleaner finishes at full speed, and the arena is still fully zeroed before
// it is handed out.
func TestSerializedParameterArenaWaitingBankHurriesCleaner(t *testing.T) {
	oldArena, oldPair, oldPause := SerializedParameterArena, parameterArenas, parameterArenaPause
	release := make(chan struct{})
	var pauses atomic.Int64
	parameterArenaPause = func() { pauses.Add(1); <-release } // a pause that never ends on its own
	t.Cleanup(func() {
		close(release)
		waitSerializedParameterArenaCleared()
		SerializedParameterArena, parameterArenas, parameterArenaPause = oldArena, oldPair, oldPause
	})

	const size = 3*parameterArenaClearChunk + 100 // three pauses when not hurried
	first := arena.New[byte](size)
	SerializedParameterArena = first
	used, ok := first.AllocN(size)
	require.True(t, ok)
	for i := range used {
		used[i] = 7
	}

	resetSerializedParameterArena() // starts clearing first; it parks in its first pause
	require.Eventually(t, func() bool { return pauses.Load() == 1 }, 5*time.Second, time.Millisecond)

	done := make(chan struct{})
	go func() {
		resetSerializedParameterArena() // the next bank needs first back now
		close(done)
	}()
	// The parked pause keeps blocking, but the remaining chunks skip theirs.
	select {
	case <-done:
		t.Fatal("the waiting bank cannot finish while the cleaner is parked in a pause it entered before the hurry")
	case <-time.After(20 * time.Millisecond):
	}
	release <- struct{}{} // end only the pause already in progress
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a waiting bank must hurry the cleaner instead of sitting out its pauses")
	}
	require.EqualValues(t, 1, pauses.Load(), "no pause after the bank started waiting")
	require.Same(t, first, SerializedParameterArena)
	require.Zero(t, first.Used())
	whole, ok := first.AllocN(size)
	require.True(t, ok)
	require.Equal(t, make([]byte, size), whole, "a hurried arena is still fully zeroed")
}
