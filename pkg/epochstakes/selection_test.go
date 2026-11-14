package epochstakes

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestSelectionPublicationAndCompatibilityDowngrade(t *testing.T) {
	cache := NewEpochStakesCache()
	vote, node := solana.PublicKey{1}, solana.PublicKey{2}
	stakes := map[solana.PublicKey]uint64{vote: 10}
	votes := map[solana.PublicKey]*VoteAccount{vote: {NodePubkey: node}}

	firstGeneration := cache.PutEpochWithSelection(7, stakes, votes, 10, SelectionVATFiltered)
	first, ok := cache.Snapshot(7)
	require.True(t, ok)
	require.Equal(t, firstGeneration, first.Generation)
	require.Equal(t, SelectionVATFiltered, first.Selection)

	stakes[vote] = 20
	votes[vote].NodePubkey = solana.PublicKey{3}
	require.Equal(t, uint64(10), first.Stakes[vote])
	require.Equal(t, node, first.VoteAccounts[vote].NodePubkey)

	cache.PutEntry(7, vote, 11, &VoteAccount{NodePubkey: node})
	updated, ok := cache.Snapshot(7)
	require.True(t, ok)
	require.Greater(t, updated.Generation, first.Generation)
	require.Equal(t, SelectionUnknown, updated.Selection)
	require.Equal(t, SelectionVATFiltered, first.Selection)

	cache.PutEpochWithSelection(7, stakes, votes, 20, SelectionVATFiltered)
	cache.PutTotalEpochStake(7, 21)
	updated, ok = cache.Snapshot(7)
	require.True(t, ok)
	require.Equal(t, SelectionUnknown, updated.Selection)

	cache.PutEpochWithSelection(7, stakes, votes, 20, Selection(255))
	updated, ok = cache.Snapshot(7)
	require.True(t, ok)
	require.Equal(t, SelectionUnknown, updated.Selection)

	cache.PutEpochWithSelection(7, stakes, votes, 20, SelectionVATFiltered)
	cache.ClearEpochStakes(7)
	cleared, ok := cache.Snapshot(7)
	require.False(t, ok)
	require.Equal(t, SelectionUnknown, cleared.Selection)
}

func TestSelectionPersistenceAndSoftDecode(t *testing.T) {
	cache := NewEpochStakesCache()
	vote, node := solana.PublicKey{4}, solana.PublicKey{5}
	cache.PutEpochWithSelection(9, map[solana.PublicKey]uint64{vote: 30},
		map[solana.PublicKey]*VoteAccount{vote: {NodePubkey: node}}, 30, SelectionVATFiltered)

	encoded, err := cache.SerializeEpoch(9)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"selection":"vat_filtered"`)
	loaded := NewEpochStakesCache()
	_, err = loaded.DeserializeAndLoadEpoch(encoded)
	require.NoError(t, err)
	snapshot, ok := loaded.Snapshot(9)
	require.True(t, ok)
	require.Equal(t, SelectionVATFiltered, snapshot.Selection)
	require.Equal(t, uint64(30), snapshot.Stakes[vote])

	cache.PutEpochWithSelection(10, map[solana.PublicKey]uint64{vote: 30},
		map[solana.PublicKey]*VoteAccount{vote: {NodePubkey: node}}, 30, SelectionUnfiltered)
	unfiltered, err := cache.SerializeEpoch(10)
	require.NoError(t, err)
	require.Contains(t, string(unfiltered), `"selection":"unfiltered"`)
	loaded = NewEpochStakesCache()
	_, err = loaded.DeserializeAndLoadEpoch(unfiltered)
	require.NoError(t, err)
	snapshot, ok = loaded.Snapshot(10)
	require.True(t, ok)
	require.Equal(t, SelectionUnfiltered, snapshot.Selection)

	cache.PutEpoch(11, map[solana.PublicKey]uint64{vote: 30},
		map[solana.PublicKey]*VoteAccount{vote: {NodePubkey: node}}, 30)
	unknown, err := cache.SerializeEpoch(11)
	require.NoError(t, err)
	require.NotContains(t, string(unknown), `"selection"`)

	var original map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &original))
	for name, raw := range map[string]json.RawMessage{
		"missing":     nil,
		"unknown":     json.RawMessage(`"unknown"`),
		"future":      json.RawMessage(`"future"`),
		"wrong type":  json.RawMessage(`123`),
		"null":        json.RawMessage(`null`),
		"object type": json.RawMessage(`{"kind":"vat_filtered"}`),
	} {
		t.Run(name, func(t *testing.T) {
			document := make(map[string]json.RawMessage, len(original))
			for key, value := range original {
				document[key] = bytes.Clone(value)
			}
			if raw == nil {
				delete(document, "selection")
			} else {
				document["selection"] = raw
			}
			data, err := json.Marshal(document)
			require.NoError(t, err)
			decoded := NewEpochStakesCache()
			_, err = decoded.DeserializeAndLoadEpoch(data)
			require.NoError(t, err)
			got, ok := decoded.Snapshot(9)
			require.True(t, ok)
			require.Equal(t, SelectionUnknown, got.Selection)
			require.Equal(t, uint64(30), got.Stakes[vote])
		})
	}
}
