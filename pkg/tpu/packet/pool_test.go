package packet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPoolProvidesMaxTransactionBuffer(t *testing.T) {
	pool := NewPool(1)
	buf, slot, ok := pool.Acquire()
	require.True(t, ok)
	require.Len(t, buf, DataSize)
	require.Equal(t, 4096, DataSize)
	pool.Release(slot)
}
