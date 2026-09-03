package snapshot

import (
	"encoding/json"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
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

func TestPopulateManifestSeedPersistsAndClearsAlpenglowBlockID(t *testing.T) {
	blockID := solana.Hash{1, 2, 3, 4}
	manifest := &SnapshotManifest{
		Bank:    &DeserializableVersionedBank{Slot: 123},
		BlockID: &blockID,
	}
	mithrilState := state.NewReadyState(manifest.Bank.Slot, 0, "", "", 0, 0)

	PopulateManifestSeed(mithrilState, manifest)
	require.Equal(t, blockID.String(), mithrilState.ManifestParentAlpenglowBlockID)

	manifest.BlockID = nil
	PopulateManifestSeed(mithrilState, manifest)
	require.Empty(t, mithrilState.ManifestParentAlpenglowBlockID)
}
