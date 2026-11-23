package snapshot

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	bin "github.com/gagliardetto/binary"
	"github.com/stretchr/testify/require"
)

func TestPopulateManifestSeedKeepsManifestEpochFrame(t *testing.T) {
	manifest := &SnapshotManifest{
		Bank: &DeserializableVersionedBank{
			Slot:  463538376,
			Epoch: 1073,
			EpochSchedule: sealevel.SysvarEpochSchedule{
				SlotsPerEpoch:            432000,
				LeaderScheduleSlotOffset: 432000,
			},
		},
		VersionedEpochStakes: []VersionedEpochStakesPair{
			{
				Epoch: 1073,
				Val: VersionedEpochStakes{
					TotalStake: 42,
					Stakes:     Stake{},
				},
			},
		},
	}
	mithrilState := state.NewReadyState(manifest.Bank.Slot, 1073, "", "", 0, 0)

	PopulateManifestSeed(mithrilState, manifest)

	require.Equal(t, uint64(432000), mithrilState.ManifestEpochSchedule.SlotsPerEpoch)
	require.Equal(t, uint64(432000), mithrilState.ManifestEpochSchedule.LeaderScheduleSlotOffset)

	data, exists := mithrilState.ManifestEpochStakes[1073]
	if !exists {
		t.Fatalf("expected manifest epoch stakes for epoch 1073, got keys %#v", mithrilState.ManifestEpochStakes)
	}
	var persisted epochstakes.PersistedEpochStakes
	if err := json.Unmarshal([]byte(data), &persisted); err != nil {
		t.Fatalf("failed to decode persisted epoch stakes: %v", err)
	}
	if persisted.Epoch != 1073 {
		t.Fatalf("persisted epoch = %d, want 1073", persisted.Epoch)
	}
}

func TestPopulateManifestSeedTargetTimingReplacement(t *testing.T) {
	s := &state.MithrilState{StateSchemaVersion: state.CurrentStateSchemaVersion}
	for _, tc := range []struct {
		name string
		ns   bin.Uint128
		want uint64
	}{
		{"full", bin.Uint128{Lo: 400_000_000}, 400_000_000},
		{"incremental", bin.Uint128{Lo: 350_000_000}, 350_000_000},
		{"zero replacement", bin.Uint128{}, 0},
		{"max uint64", bin.Uint128{Lo: math.MaxUint64}, math.MaxUint64},
		{"too wide replacement", bin.Uint128{Hi: 1, Lo: 200_000_000}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &SnapshotManifest{Bank: &DeserializableVersionedBank{Slot: s.ManifestParentSlot + 1, NsPerSlot: tc.ns,
				EpochSchedule: sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}}}
			m.Bank.Hash[0] = byte(m.Bank.Slot)
			PopulateManifestSeed(s, m)
			require.Equal(t, tc.want, s.ManifestNsPerSlot)
			require.Equal(t, m.Bank.Slot, s.ManifestParentSlot)
			require.NotEmpty(t, s.ManifestParentBankhash)
			raw, err := json.Marshal(s)
			require.NoError(t, err)
			var restored state.MithrilState
			require.NoError(t, json.Unmarshal(raw, &restored))
			require.Equal(t, s.ManifestNsPerSlot, restored.ManifestNsPerSlot)
			require.Equal(t, s.ManifestParentSlot, restored.ManifestParentSlot)
			require.Equal(t, s.ManifestParentBankhash, restored.ManifestParentBankhash)
			require.Equal(t, s.ManifestEpochSchedule, restored.ManifestEpochSchedule)
			require.Equal(t, s.StateSchemaVersion, restored.StateSchemaVersion)
			if tc.want == 0 {
				require.NotContains(t, string(raw), "manifest_ns_per_slot")
			}
		})
	}
	var legacy state.MithrilState
	require.NoError(t, json.Unmarshal([]byte(`{"state_schema_version":3,"manifest_parent_slot":7}`), &legacy))
	require.Zero(t, legacy.ManifestNsPerSlot)
	require.Equal(t, state.CurrentStateSchemaVersion, legacy.StateSchemaVersion)
}
