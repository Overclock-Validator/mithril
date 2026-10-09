package sealevel

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRecentBlockhashRefreshPreservesUniqueness(t *testing.T) {
	var recent SysvarRecentBlockhashes
	for i := byte(1); i <= recentBlockhashesMaxEntries; i++ {
		recent.PushLatest([32]byte{i}, uint64(i))
	}
	// Re-registering an existing hash refreshes its fee and order without
	// evicting the oldest entry. Empty sleep-mode genesis banks do this.
	require.Equal(t, [32]byte{}, recent.PushLatest([32]byte{42}, 5000))
	require.Len(t, recent, recentBlockhashesMaxEntries)
	require.Equal(t, [32]byte{42}, recent[0].Blockhash)
	require.Equal(t, uint64(5000), recent[0].FeeCalculator.LamportsPerSignature)
	require.Equal(t, [32]byte{1}, recent[len(recent)-1].Blockhash)
	seen := make(map[[32]byte]bool)
	for _, entry := range recent {
		require.False(t, seen[entry.Blockhash])
		seen[entry.Blockhash] = true
	}
	require.Equal(t, [32]byte{1}, recent.PushLatest([32]byte{151}, 6000))
}
