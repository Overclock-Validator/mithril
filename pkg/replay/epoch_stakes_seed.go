package replay

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/state"
)

// seedEpochStakesForExecution mirrors Agave Bank::current_epoch_stakes:
// the effective stakes for bank epoch E are stored under leader-schedule key
// E+1. Both forms of sol_get_epoch_stake use this generation. Consensus and
// leader selection still use key E; substituting that older generation here
// makes program output depend on whether replay started within the epoch.
// Startup passes the parent's epoch: a child across a boundary inherits that
// view until handleEpochTransition creates or restores the new generation.
func seedEpochStakesForExecution(block *b.Block, bankEpoch uint64) error {
	stakesEpoch := safemath.SaturatingAddU64(bankEpoch, 1)
	snapshot, ok := global.EpochStakesSnapshot(stakesEpoch)
	if !ok || len(snapshot.Stakes) == 0 {
		return fmt.Errorf("no current epoch stakes for execution in epoch %d (cache key %d)", bankEpoch, stakesEpoch)
	}
	block.EpochStakesPerVoteAcct = maps.Clone(snapshot.Stakes)
	block.TotalEpochStake = snapshot.TotalStake
	return nil
}

type manifestEpochStakeSeed struct {
	sourceEpoch  uint64
	runtimeEpoch uint64
	data         []byte
}

func prepareManifestEpochStakesForRuntime(mithrilState *state.MithrilState, currentEpoch uint64, snapshotEpoch uint64) ([]manifestEpochStakeSeed, bool, error) {
	if mithrilState == nil || len(mithrilState.ManifestEpochStakes) == 0 {
		return nil, false, fmt.Errorf("state file missing manifest_epoch_stakes - delete AccountsDB and rebuild from snapshot")
	}
	if snapshotEpoch == 0 {
		snapshotEpoch = currentEpoch
	}

	keys := sortedManifestEpochStakeKeys(mithrilState.ManifestEpochStakes)
	needsRebase := !manifestEpochStakeKeyExists(mithrilState.ManifestEpochStakes, currentEpoch) &&
		!manifestEpochStakeKeyExists(mithrilState.ManifestEpochStakes, snapshotEpoch)
	sourceSnapshotEpoch := snapshotEpoch
	if needsRebase {
		var ok bool
		sourceSnapshotEpoch, ok = inferManifestSourceSnapshotEpoch(keys, mithrilState.ManifestParentSlot)
		if !ok {
			return nil, false, fmt.Errorf("state file manifest_epoch_stakes has no epoch keys")
		}
	}

	seeds := make([]manifestEpochStakeSeed, 0, len(keys))
	for _, sourceEpoch := range keys {
		runtimeEpoch := sourceEpoch
		if needsRebase {
			var ok bool
			runtimeEpoch, ok = rebaseManifestEpoch(sourceEpoch, sourceSnapshotEpoch, snapshotEpoch)
			if !ok {
				return nil, false, fmt.Errorf("cannot rebase manifest epoch %d from source snapshot epoch %d to runtime snapshot epoch %d",
					sourceEpoch, sourceSnapshotEpoch, snapshotEpoch)
			}
		}

		data := []byte(mithrilState.ManifestEpochStakes[sourceEpoch])
		if runtimeEpoch != sourceEpoch {
			var persisted epochstakes.PersistedEpochStakes
			if err := json.Unmarshal(data, &persisted); err != nil {
				return nil, false, fmt.Errorf("failed to decode manifest epoch %d stakes for rebase: %w", sourceEpoch, err)
			}
			persisted.Epoch = runtimeEpoch
			rebased, err := json.Marshal(persisted)
			if err != nil {
				return nil, false, fmt.Errorf("failed to encode manifest epoch %d stakes rebased to %d: %w", sourceEpoch, runtimeEpoch, err)
			}
			data = rebased
		}

		seeds = append(seeds, manifestEpochStakeSeed{
			sourceEpoch:  sourceEpoch,
			runtimeEpoch: runtimeEpoch,
			data:         data,
		})
	}

	return seeds, needsRebase, nil
}

func sortedManifestEpochStakeKeys(stakes map[uint64]string) []uint64 {
	keys := make([]uint64, 0, len(stakes))
	for epoch := range stakes {
		keys = append(keys, epoch)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func manifestEpochStakeKeyExists(stakes map[uint64]string, epoch uint64) bool {
	_, exists := stakes[epoch]
	return exists
}

func inferManifestSourceSnapshotEpoch(keys []uint64, parentSlot uint64) (uint64, bool) {
	if len(keys) == 0 {
		return 0, false
	}

	// The affected devnet snapshots have manifest Bank epoch data serialized in
	// the 432k-slot frame even though the EpochSchedule sysvar account uses 8192.
	const agaveDefaultSlotsPerEpoch = 432000
	if parentSlot > 0 {
		legacyEpoch := parentSlot / agaveDefaultSlotsPerEpoch
		if uint64SliceContains(keys, legacyEpoch) {
			return legacyEpoch, true
		}
	}

	// VersionedEpochStakes normally carries the snapshot epoch plus the next
	// leader-schedule epoch, so the penultimate key is the best fallback.
	if len(keys) >= 2 {
		return keys[len(keys)-2], true
	}
	return keys[0], true
}

func uint64SliceContains(values []uint64, needle uint64) bool {
	idx := sort.Search(len(values), func(i int) bool { return values[i] >= needle })
	return idx < len(values) && values[idx] == needle
}

func rebaseManifestEpoch(sourceEpoch uint64, sourceSnapshotEpoch uint64, runtimeSnapshotEpoch uint64) (uint64, bool) {
	if runtimeSnapshotEpoch >= sourceSnapshotEpoch {
		return sourceEpoch + (runtimeSnapshotEpoch - sourceSnapshotEpoch), true
	}

	delta := sourceSnapshotEpoch - runtimeSnapshotEpoch
	if sourceEpoch < delta {
		return 0, false
	}
	return sourceEpoch - delta, true
}
