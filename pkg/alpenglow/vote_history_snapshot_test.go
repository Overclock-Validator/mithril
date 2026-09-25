package alpenglow

import (
	"crypto/ed25519"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestReservedHistorySnapshotIsImmutable(t *testing.T) {
	identity := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	node := solana.PublicKey(identity.Public().(ed25519.PublicKey))
	h := NewVoteHistory(node, 10)
	h.ReservationRequired = true
	block := BlockID{Slot: 11, Hash: solana.Hash{11}}
	require.NoError(t, h.AddVote(NewNotarizationVote(11, block.Hash)))
	h.NotarizedBlocks[block] = true
	h.AddParentReady(12, block)
	snapshot, err := PrepareReservedVoteHistory(h, identity)
	require.NoError(t, err)
	// Mutate/prune every transport-backed collection after taking the snapshot.
	h.SetRoot(20)
	require.NoError(t, h.AddVote(NewSkipVote(21)))
	for i := range identity {
		identity[i] = 0
	}
	dir := t.TempDir()
	require.NoError(t, SaveReservedVoteHistorySnapshot(dir, snapshot))
	loaded, err := LoadVoteHistory(dir, node)
	require.NoError(t, err)
	require.Equal(t, uint64(10), loaded.Root)
	require.True(t, loaded.VotedAt(11))
	require.True(t, loaded.IsBlockNotarized(block))
	require.True(t, loaded.IsParentReady(12, block))
	require.False(t, loaded.HasSkipped(21))
}

func TestReservedHistorySnapshotRequiresEnrollmentAndValidHistory(t *testing.T) {
	identity := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	h := NewVoteHistory(solana.PublicKey(identity.Public().(ed25519.PublicKey)), 10)
	_, err := PrepareReservedVoteHistory(h, identity)
	require.Error(t, err)
	h.ReservationRequired = true
	h.Voted[11] = true // Inconsistent with canonical VotesCast.
	_, err = PrepareReservedVoteHistory(h, identity)
	require.Error(t, err)
	require.Error(t, SaveReservedVoteHistorySnapshot(t.TempDir(), &VoteHistorySnapshot{}))
}
