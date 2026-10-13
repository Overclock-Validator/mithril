package node

import (
	"encoding/json"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/snapshot"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestRefreshManifestSeedRestoresClearedEpochStakes(t *testing.T) {
	schedule := sealevel.SysvarEpochSchedule{
		SlotsPerEpoch:            432000,
		LeaderScheduleSlotOffset: 432000,
	}
	manifest := &snapshot.SnapshotManifest{
		Bank: &snapshot.DeserializableVersionedBank{
			Slot:          100,
			Epoch:         7,
			EpochSchedule: schedule,
		},
		VersionedEpochStakes: []snapshot.VersionedEpochStakesPair{
			{Epoch: 7, Val: snapshot.VersionedEpochStakes{TotalStake: 42}},
		},
	}
	s := state.NewReadyState(manifest.Bank.Slot, manifest.Bank.Epoch, "", "", 0, 0)
	s.ManifestEpochSchedule = &state.ManifestEpochScheduleSeed{
		SlotsPerEpoch:            schedule.SlotsPerEpoch,
		LeaderScheduleSlotOffset: schedule.LeaderScheduleSlotOffset,
	}
	s.ManifestEpochStakes = nil

	refreshManifestSeedFromManifest(t.TempDir(), s, manifest)

	require.Contains(t, s.ManifestEpochStakes, uint64(7))
}

func TestRefreshManifestSeedRestoresDiscardedCommissions(t *testing.T) {
	key := solana.PublicKey{32}
	bps := uint16(10000)
	manifest := &snapshot.SnapshotManifest{
		Bank:                 &snapshot.DeserializableVersionedBank{Slot: 451934700, Epoch: 1058, EpochSchedule: sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000, Warmup: true, FirstNormalEpoch: 14, FirstNormalSlot: 524256}},
		VersionedEpochStakes: []snapshot.VersionedEpochStakesPair{{Epoch: 1058, Val: snapshot.VersionedEpochStakes{TotalStake: 42, Stakes: snapshot.Stake{VoteAccounts: []snapshot.VoteAccountsPair{{Key: key, Stake: 42, Value: snapshot.VoteAccount{InflationRewardsCommissionBps: &bps}}}}}}},
	}
	s := state.NewReadyState(manifest.Bank.Slot, 1058, "", "", 0, 0)
	snapshot.PopulateManifestSeed(s, manifest)
	var legacy epochstakes.PersistedEpochStakes
	require.NoError(t, json.Unmarshal([]byte(s.ManifestEpochStakes[1058]), &legacy))
	legacy.VoteAccts[key.String()].InflationRewardsCommissionBps = nil
	raw, err := json.Marshal(legacy)
	require.NoError(t, err)
	s.ManifestEpochStakes[1058] = string(raw)
	dir := t.TempDir()
	refreshManifestSeedFromManifest(dir, s, manifest)
	require.True(t, epochstakes.HasInflationCommissionMetadata([]byte(s.ManifestEpochStakes[1058])))
	restored, err := state.LoadState(dir)
	require.NoError(t, err)
	require.Equal(t, s.ManifestEpochStakes, restored.ManifestEpochStakes)
}
