package metrics

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountIndexReplaySnapshotIsCompactAndRoundTrips(t *testing.T) {
	typeOfStats := reflect.TypeOf(AccountIndex{})
	for i := 0; i < typeOfStats.NumField(); i++ {
		field := typeOfStats.Field(i)
		assert.Contains(t, []reflect.Kind{reflect.Bool, reflect.Uint64}, field.Type.Kind(), field.Name)
	}

	want := AccountIndex{
		Enabled:                          true,
		RootGeneration:                   2,
		MinimumCoveredSequence:           3,
		BaseKeys:                         4,
		BaseArtifactBytes:                5,
		ExtentCatalogEntries:             6,
		ExtentCatalogCapacity:            7,
		ExtentCatalogRemaining:           1,
		DeltaKeys:                        8,
		DeltaArtifactBytes:               9,
		CheckpointSelectedBytes:          27,
		CheckpointBuildReservedBytes:     28,
		CheckpointObsoleteBytes:          29,
		CheckpointPhysicalBytes:          30,
		MaxCheckpointSelectedBytes:       31,
		MaxCheckpointPhysicalBytes:       32,
		CheckpointSelectedHighWaterBytes: 33,
		CheckpointBuildHighWaterBytes:    34,
		CheckpointObsoleteHighWaterBytes: 35,
		CheckpointPhysicalHighWaterBytes: 36,
		CheckpointReservationRejects:     37,
		CheckpointPressureRebases:        38,
		HotKeys:                          10,
		HotBytes:                         11,
		WALSequence:                      12,
		WALBytes:                         13,
		SealCount:                        14,
		RebaseCount:                      15,
		RewriteCount:                     16,
		SealsInProgress:                  17,
		RebasesInProgress:                18,
		ObsoleteBaseGenerationsPending:   19,
		SealRetriesPending:               20,
		RebaseRetriesPending:             21,
		RewriteInProgress:                true,
		MaintenanceErrors:                22,
		FoldCommits:                      23,
		FoldWALFrames:                    24,
		OversizedFoldCommits:             25,
		LargestFoldKeys:                  26,
		WorkingSetHeldSlots:              39,
		WorkingSetRetainedBytes:          40,
		WorkingSetHighWaterBytes:         41,
		WorkingSetMaxRetainedBytes:       42,
		WorkingSetLargestSlotBytes:       43,
		WorkingSetHighWaterOverageBytes:  44,
		FatalError:                       true,
	}
	encoded, err := json.Marshal(BlockReplay{Slot: 99, AccountIndex: want})
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "Lineage")
	assert.NotContains(t, string(encoded), "Path")
	assert.NotContains(t, string(encoded), "Shards")

	var decoded BlockReplay
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, want, decoded.AccountIndex)
}
