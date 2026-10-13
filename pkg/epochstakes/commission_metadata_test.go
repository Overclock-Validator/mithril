package epochstakes

import (
	"encoding/json"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestCommissionMetadataSurvivesRestartAndOwnsItsMemory(t *testing.T) {
	for _, bps := range []uint16{0, 1234, 10000, 65535} {
		vote := solana.PublicKey{1}
		commission := bps
		cache := NewEpochStakesCache()
		cache.PutEpoch(1058, map[solana.PublicKey]uint64{vote: 42}, map[solana.PublicKey]*VoteAccount{vote: {InflationRewardsCommissionBps: &commission}}, 42)
		commission++
		*cache.EpochStakesAccts(1058)[vote].InflationRewardsCommissionBps = 7
		raw, err := cache.SerializeEpoch(1058)
		require.NoError(t, err)
		require.True(t, HasInflationCommissionMetadata(raw))
		restored := NewEpochStakesCache()
		_, err = restored.DeserializeAndLoadEpoch(raw)
		require.NoError(t, err)
		got, _ := restored.Snapshot(1058)
		require.Equal(t, bps, *got.VoteAccounts[vote].InflationRewardsCommissionBps)
		var old PersistedEpochStakes
		require.NoError(t, json.Unmarshal(raw, &old))
		old.VoteAccts[vote.String()].InflationRewardsCommissionBps = nil
		legacy, err := json.Marshal(old)
		require.NoError(t, err)
		require.False(t, HasInflationCommissionMetadata(legacy))
		_, err = restored.DeserializeAndLoadEpoch(legacy)
		require.NoError(t, err)
		got, _ = restored.Snapshot(1058)
		require.Nil(t, got.VoteAccounts[vote].InflationRewardsCommissionBps)
	}
}

func TestRestoreCommissionsOnlyFillsMissingFieldsInSameGeneration(t *testing.T) {
	zero, full := uint16(0), uint16(10000)
	old := PersistedEpochStakes{Epoch: 1058, TotalStake: 42, Stakes: map[string]uint64{"vote": 42}, VoteAccts: map[string]*VoteAccountJSON{
		"vote": {Lamports: 123}, "explicit-zero": {InflationRewardsCommissionBps: &zero}, "no-source": {},
	}}
	source := PersistedEpochStakes{Epoch: 1058, TotalStake: 999, Stakes: map[string]uint64{"vote": 999}, VoteAccts: map[string]*VoteAccountJSON{
		"vote": {Lamports: 999, InflationRewardsCommissionBps: &full}, "explicit-zero": {InflationRewardsCommissionBps: &full}, "source-only": {InflationRewardsCommissionBps: &full},
	}}
	encode := func(v any) []byte { b, e := json.Marshal(v); require.NoError(t, e); return b }
	raw := encode(old)
	repaired, err := RestoreInflationCommissionMetadata(raw, encode(source))
	require.NoError(t, err)
	old.VoteAccts["vote"].InflationRewardsCommissionBps = &full
	require.JSONEq(t, string(encode(old)), string(repaired))
	require.False(t, HasInflationCommissionMetadata(repaired), "missing history must stay unknown")
	source.Epoch++
	_, err = RestoreInflationCommissionMetadata(raw, encode(source))
	require.ErrorContains(t, err, "from epoch 1059")
	_, err = RestoreInflationCommissionMetadata([]byte("invalid"), encode(source))
	require.Error(t, err)
}
