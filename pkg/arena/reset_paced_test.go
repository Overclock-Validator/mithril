package arena

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResetPacedClearsEverythingInChunks(t *testing.T) {
	a := New[byte](100)
	d, ok := a.AllocN(95)
	require.True(t, ok)
	for i := range d {
		d[i] = 7
	}
	pauses := 0
	a.ResetPaced(10, func() { pauses++ })
	require.Equal(t, make([]byte, 95), d)
	require.Equal(t, 9, pauses, "a pause between each of the 10 chunks")
	require.Zero(t, a.Used())
	a.ResetPaced(10, func() { t.Fatal("nothing to clear") })
}
