package accountsdb

// ProductionAccountIndexStats is a compact, low-cardinality snapshot of the
// V2 account index. Maintenance counts are process-local; key and byte totals
// describe the immutable root generation pinned while this snapshot is taken.
//
// BaseArtifactBytes includes the shared extent catalog and every root-selected
// base .stmh and .scan file. DeltaArtifactBytes includes every selected delta
// .stmh, exact record file, and its fixed-size publication descriptor. These
// are durable artifact sizes, not resident-set measurements.
type ProductionAccountIndexStats struct {
	Enabled                          bool
	RootGeneration                   uint64
	MinimumCoveredSequence           uint64
	BaseKeys                         uint64
	BaseArtifactBytes                uint64
	ExtentCatalogEntries             uint64
	ExtentCatalogCapacity            uint64
	ExtentCatalogRemaining           uint64
	DeltaKeys                        uint64
	DeltaArtifactBytes               uint64
	CheckpointSelectedBytes          uint64
	CheckpointBuildReservedBytes     uint64
	CheckpointObsoleteBytes          uint64
	CheckpointPhysicalBytes          uint64
	MaxCheckpointSelectedBytes       uint64
	MaxCheckpointPhysicalBytes       uint64
	CheckpointSelectedHighWaterBytes uint64
	CheckpointBuildHighWaterBytes    uint64
	CheckpointObsoleteHighWaterBytes uint64
	CheckpointPhysicalHighWaterBytes uint64
	CheckpointReservationRejects     uint64
	CheckpointPressureRebases        uint64
	HotKeys                          uint64
	HotBytes                         uint64
	WALSequence                      uint64
	WALBytes                         uint64
	SealCount                        uint64
	RebaseCount                      uint64
	RewriteCount                     uint64
	SealsInProgress                  uint64
	RebasesInProgress                uint64
	ObsoleteBaseGenerationsPending   uint64
	SealRetriesPending               uint64
	RebaseRetriesPending             uint64
	RewriteInProgress                bool
	MaintenanceErrors                uint64
	FoldCommits                      uint64
	FoldWALFrames                    uint64
	OversizedFoldCommits             uint64
	LargestFoldKeys                  uint64
	FatalError                       bool
}

// RuntimeStats returns one aggregate snapshot without allocating a per-shard
// statistics slice or performing filesystem I/O. It intentionally exposes no
// artifact paths or root lineage identifier.
func (index *ProductionAccountIndex) RuntimeStats() ProductionAccountIndexStats {
	if index == nil {
		return ProductionAccountIndexStats{}
	}
	stats := ProductionAccountIndexStats{Enabled: true}

	index.poisonMu.Lock()
	stats.FatalError = index.poison != nil
	index.poisonMu.Unlock()

	if index.mutable == nil {
		stats.FatalError = true
	} else {
		index.mutable.stateMu.RLock()
		stats.HotKeys = index.mutable.hotKeys
		stats.HotBytes = index.mutable.hotBytes
		stats.WALSequence = index.mutable.seq
		if index.mutable.offset > 0 {
			stats.WALBytes = uint64(index.mutable.offset)
		}
		stats.SealCount = index.mutable.sealCount
		stats.RebaseCount = index.mutable.rebaseCount
		stats.RewriteCount = index.mutable.rewriteCount
		stats.SealsInProgress = uint64(index.mutable.sealsInProgress)
		stats.RebasesInProgress = uint64(index.mutable.rebasesInProgress)
		stats.RewriteInProgress = index.mutable.rewriting
		stats.MaintenanceErrors = index.mutable.maintenanceErrors
		stats.FatalError = stats.FatalError || index.mutable.poison != nil
		stats.CheckpointPressureRebases = index.mutable.checkpointPressureRebases
		for shardID := range index.mutable.shards {
			shard := &index.mutable.shards[shardID]
			if shard.sealErr != nil && !shard.sealRetryAt.IsZero() {
				stats.SealRetriesPending++
			}
			if shard.rebaseErr != nil && !shard.rebaseRetryAt.IsZero() {
				stats.RebaseRetriesPending++
			}
		}
		index.mutable.stateMu.RUnlock()
	}
	checkpointStats := index.checkpointBudget.snapshot()
	stats.CheckpointSelectedBytes = checkpointStats.SelectedBytes
	stats.CheckpointBuildReservedBytes = checkpointStats.BuildReservedBytes
	stats.CheckpointObsoleteBytes = checkpointStats.ObsoleteBytes
	stats.CheckpointPhysicalBytes = checkpointStats.PhysicalBytes
	stats.MaxCheckpointSelectedBytes = checkpointStats.MaxSelectedBytes
	stats.MaxCheckpointPhysicalBytes = checkpointStats.MaxPhysicalBytes
	stats.CheckpointSelectedHighWaterBytes = checkpointStats.SelectedHighWaterBytes
	stats.CheckpointBuildHighWaterBytes = checkpointStats.BuildReservedHighWaterBytes
	stats.CheckpointObsoleteHighWaterBytes = checkpointStats.ObsoleteHighWaterBytes
	stats.CheckpointPhysicalHighWaterBytes = checkpointStats.PhysicalHighWaterBytes
	stats.CheckpointReservationRejects = checkpointStats.ReservationRejects

	index.gcMu.Lock()
	for _, obsolete := range index.obsoleteBases {
		pending := false
		for _, resource := range obsolete.resources {
			select {
			case <-resource.Done():
			default:
				pending = true
			}
		}
		if pending {
			stats.ObsoleteBaseGenerationsPending++
		}
	}
	index.gcMu.Unlock()

	if index.view == nil {
		stats.FatalError = true
		return stats
	}
	view, err := index.view.Acquire()
	if err != nil {
		stats.FatalError = true
		return stats
	}
	defer view.Close()

	root := view.Catalog()
	payload, ok := view.Payload().(*ShardedImmutableIndex)
	if root == nil || !ok || payload == nil || len(payload.shards) != len(root.Shards) {
		stats.FatalError = true
		return stats
	}
	stats.RootGeneration = root.Generation
	stats.MinimumCoveredSequence = root.CoveredSequence
	stats.ExtentCatalogCapacity = streamIndexMaxExtents
	if payload.extents == nil || payload.extents.Len() < 0 || uint64(payload.extents.Len()) > stats.ExtentCatalogCapacity {
		stats.FatalError = true
	} else {
		stats.ExtentCatalogEntries = uint64(payload.extents.Len())
		stats.ExtentCatalogRemaining = stats.ExtentCatalogCapacity - stats.ExtentCatalogEntries
	}
	stats.BaseArtifactBytes = saturatingAccountIndexStatAdd(
		stats.BaseArtifactBytes,
		root.SharedExtentCatalog.Size,
	)
	for shardID := range root.Shards {
		selected := root.Shards[shardID]
		stats.BaseArtifactBytes = saturatingAccountIndexStatAdd(
			stats.BaseArtifactBytes,
			selected.BaseIndex.Size,
			selected.BaseRecords.Size,
		)
		base := payload.shards[shardID].base
		if base == nil {
			stats.FatalError = true
		} else {
			stats.BaseKeys = saturatingAccountIndexStatAdd(stats.BaseKeys, base.NumKeys())
		}

		if selected.DeltaGeneration == 0 {
			continue
		}
		stats.DeltaArtifactBytes = saturatingAccountIndexStatAdd(
			stats.DeltaArtifactBytes,
			selected.DeltaIndex.Size,
			selected.DeltaRecords.Size,
			deltaCheckpointDescriptorSize,
		)
		delta := payload.shards[shardID].delta
		if delta == nil {
			stats.FatalError = true
		} else {
			stats.DeltaKeys = saturatingAccountIndexStatAdd(stats.DeltaKeys, delta.Len())
		}
	}
	return stats
}

func saturatingAccountIndexStatAdd(total uint64, values ...uint64) uint64 {
	for _, value := range values {
		if ^uint64(0)-total < value {
			return ^uint64(0)
		}
		total += value
	}
	return total
}

// AccountIndexStats exposes the production index snapshot to replay and
// operators without exporting the index internals through AccountsDb.
func (accountsDb *AccountsDb) AccountIndexStats() ProductionAccountIndexStats {
	if accountsDb == nil || accountsDb.ProductionIndex == nil {
		return ProductionAccountIndexStats{}
	}
	stats := accountsDb.ProductionIndex.RuntimeStats()
	stats.FoldCommits = accountsDb.foldCommits.Load()
	stats.FoldWALFrames = accountsDb.foldWALFrames.Load()
	stats.OversizedFoldCommits = accountsDb.foldOversized.Load()
	stats.LargestFoldKeys = accountsDb.foldMaxKeys.Load()
	return stats
}
