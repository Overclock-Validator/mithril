package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/stretchr/testify/assert"
)

func TestAccountIndexMetricsFromStatsMapsEveryAggregate(t *testing.T) {
	stats := accountsdb.ProductionAccountIndexStats{
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
		FatalError:                       true,
	}
	want := metrics.AccountIndex{
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
		FatalError:                       true,
	}
	assert.Equal(t, want, accountIndexMetricsFromStats(stats))
	assert.Equal(t, metrics.AccountIndex{}, snapshotAccountIndexMetrics(nil))
	assert.Equal(t, metrics.AccountIndex{}, snapshotAccountIndexMetrics(&accountsdb.AccountsDb{}))
}

func TestApplyWorkingSetMetricsMapsCapacityState(t *testing.T) {
	got := metrics.AccountIndex{Enabled: true}
	applyWorkingSetMetrics(&got, accounts.WorkingSetStats{
		HeldSlots:             3,
		RetainedBytes:         4,
		HighWaterBytes:        5,
		MaximumBytes:          6,
		LargestSlotBytes:      7,
		HighWaterOverageBytes: 8,
	})

	assert.True(t, got.Enabled)
	assert.Equal(t, uint64(3), got.WorkingSetHeldSlots)
	assert.Equal(t, uint64(4), got.WorkingSetRetainedBytes)
	assert.Equal(t, uint64(5), got.WorkingSetHighWaterBytes)
	assert.Equal(t, uint64(6), got.WorkingSetMaxRetainedBytes)
	assert.Equal(t, uint64(7), got.WorkingSetLargestSlotBytes)
	assert.Equal(t, uint64(8), got.WorkingSetHighWaterOverageBytes)
}
