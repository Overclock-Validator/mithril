package epochstakes

import (
	"encoding/json"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestEpochStakesPreserveHistoricalCollector(t *testing.T) {
	const epoch = 1054
	vote, node, collector := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	expected := collector
	cache := NewEpochStakesCache()
	cache.PutEpoch(epoch, map[solana.PublicKey]uint64{vote: 1}, map[solana.PublicKey]*VoteAccount{vote: {NodePubkey: node, BlockRevenueCollector: &collector}}, 1)
	collector[0] = 9 // Neither caller writes nor compatibility accessor writes may alter the snapshot.
	copy := cache.EpochStakesAccts(epoch)
	*copy[vote].BlockRevenueCollector = collector
	snap, ok := cache.Snapshot(epoch)
	require.True(t, ok)
	require.Equal(t, expected, *snap.VoteAccounts[vote].BlockRevenueCollector)
	raw, err := cache.SerializeEpoch(epoch)
	require.NoError(t, err)
	restored := NewEpochStakesCache()
	_, err = restored.DeserializeAndLoadEpoch(raw)
	require.NoError(t, err)
	got, ok := restored.Snapshot(epoch)
	require.True(t, ok)
	require.Equal(t, expected, *got.VoteAccounts[vote].BlockRevenueCollector)
	var persisted PersistedEpochStakes
	require.NoError(t, json.Unmarshal(raw, &persisted))
	generation := restored.Generation(epoch)
	persisted.VoteAccts[vote.String()].BlockRevenueCollector = "not-a-key"
	invalid, err := json.Marshal(persisted)
	require.NoError(t, err)
	_, err = restored.DeserializeAndLoadEpoch(invalid)
	require.Error(t, err)
	require.Equal(t, generation, restored.Generation(epoch))
	persisted.VoteAccts[vote.String()].BlockRevenueCollector = ""
	legacy, err := json.Marshal(persisted)
	require.NoError(t, err)
	_, err = restored.DeserializeAndLoadEpoch(legacy)
	require.NoError(t, err)
	got, ok = restored.Snapshot(epoch)
	require.True(t, ok)
	require.Nil(t, got.VoteAccounts[vote].BlockRevenueCollector)
	// The all-zero address is a real (reserved) address, distinct from absent metadata.
	persisted.VoteAccts[vote.String()].BlockRevenueCollector = solana.PublicKey{}.String()
	zero, err := json.Marshal(persisted)
	require.NoError(t, err)
	_, err = restored.DeserializeAndLoadEpoch(zero)
	require.NoError(t, err)
	got, _ = restored.Snapshot(epoch)
	require.NotNil(t, got.VoteAccounts[vote].BlockRevenueCollector)
	require.True(t, got.VoteAccounts[vote].BlockRevenueCollector.IsZero())
}
