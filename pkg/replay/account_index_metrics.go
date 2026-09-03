package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

func snapshotAccountIndexMetrics(accountsDb *accountsdb.AccountsDb) metrics.AccountIndex {
	return accountIndexMetricsFromStats(accountsDb.AccountIndexStats())
}

func applyWorkingSetMetrics(dst *metrics.AccountIndex, stats accounts.WorkingSetStats) {
	if dst == nil {
		return
	}
	dst.WorkingSetHeldSlots = stats.HeldSlots
	dst.WorkingSetRetainedBytes = stats.RetainedBytes
	dst.WorkingSetHighWaterBytes = stats.HighWaterBytes
	dst.WorkingSetMaxRetainedBytes = stats.MaximumBytes
	dst.WorkingSetLargestSlotBytes = stats.LargestSlotBytes
	dst.WorkingSetHighWaterOverageBytes = stats.HighWaterOverageBytes
}

func accountIndexMetricsFromStats(stats accountsdb.ProductionAccountIndexStats) metrics.AccountIndex {
	return metrics.AccountIndex{
		Enabled:                          stats.Enabled,
		RootGeneration:                   stats.RootGeneration,
		MinimumCoveredSequence:           stats.MinimumCoveredSequence,
		BaseKeys:                         stats.BaseKeys,
		BaseArtifactBytes:                stats.BaseArtifactBytes,
		ExtentCatalogEntries:             stats.ExtentCatalogEntries,
		ExtentCatalogCapacity:            stats.ExtentCatalogCapacity,
		ExtentCatalogRemaining:           stats.ExtentCatalogRemaining,
		DeltaKeys:                        stats.DeltaKeys,
		DeltaArtifactBytes:               stats.DeltaArtifactBytes,
		CheckpointSelectedBytes:          stats.CheckpointSelectedBytes,
		CheckpointBuildReservedBytes:     stats.CheckpointBuildReservedBytes,
		CheckpointObsoleteBytes:          stats.CheckpointObsoleteBytes,
		CheckpointPhysicalBytes:          stats.CheckpointPhysicalBytes,
		MaxCheckpointSelectedBytes:       stats.MaxCheckpointSelectedBytes,
		MaxCheckpointPhysicalBytes:       stats.MaxCheckpointPhysicalBytes,
		CheckpointSelectedHighWaterBytes: stats.CheckpointSelectedHighWaterBytes,
		CheckpointBuildHighWaterBytes:    stats.CheckpointBuildHighWaterBytes,
		CheckpointObsoleteHighWaterBytes: stats.CheckpointObsoleteHighWaterBytes,
		CheckpointPhysicalHighWaterBytes: stats.CheckpointPhysicalHighWaterBytes,
		CheckpointReservationRejects:     stats.CheckpointReservationRejects,
		CheckpointPressureRebases:        stats.CheckpointPressureRebases,
		HotKeys:                          stats.HotKeys,
		HotBytes:                         stats.HotBytes,
		WALSequence:                      stats.WALSequence,
		WALBytes:                         stats.WALBytes,
		SealCount:                        stats.SealCount,
		RebaseCount:                      stats.RebaseCount,
		RewriteCount:                     stats.RewriteCount,
		SealsInProgress:                  stats.SealsInProgress,
		RebasesInProgress:                stats.RebasesInProgress,
		ObsoleteBaseGenerationsPending:   stats.ObsoleteBaseGenerationsPending,
		SealRetriesPending:               stats.SealRetriesPending,
		RebaseRetriesPending:             stats.RebaseRetriesPending,
		RewriteInProgress:                stats.RewriteInProgress,
		MaintenanceErrors:                stats.MaintenanceErrors,
		FoldCommits:                      stats.FoldCommits,
		FoldWALFrames:                    stats.FoldWALFrames,
		OversizedFoldCommits:             stats.OversizedFoldCommits,
		LargestFoldKeys:                  stats.LargestFoldKeys,
		FatalError:                       stats.FatalError,
	}
}
