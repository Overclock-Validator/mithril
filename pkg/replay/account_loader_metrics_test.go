package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/stretchr/testify/assert"
)

func TestRecordAccountLoaderBatchStatsIncludesPlanningAndPhysicalIO(t *testing.T) {
	source := accountsdb.BatchReadStats{
		RequestedKeys:              101,
		UniqueKeys:                 89,
		DuplicateKeys:              12,
		DurableKeys:                80,
		UniqueDurableKeys:          73,
		DeltaIndexProbes:           71,
		DeltaIndexHits:             67,
		DeltaIndexTombstones:       3,
		BaseIndexProbes:            6,
		BaseIndexCandidates:        5,
		BaseIndexHits:              4,
		BaseIndexFalsePositives:    1,
		AppendVecReadRanges:        9,
		AppendVecPreadCalls:        7,
		AppendVecRequestedBytes:    123_456,
		AppendVecPhysicalReadBytes: 131_072,
	}
	var destination metrics.AccountLoader

	recordAccountLoaderBatchStats(&destination, source)

	assert.Equal(t, source.RequestedKeys, destination.RequestedKeys)
	assert.Equal(t, source.UniqueKeys, destination.UniqueKeys)
	assert.Equal(t, source.DuplicateKeys, destination.DuplicateKeys)
	assert.Equal(t, source.DurableKeys, destination.DurableKeys)
	assert.Equal(t, source.UniqueDurableKeys, destination.UniqueDurableKeys)
	assert.Equal(t, source.DeltaIndexProbes, destination.DeltaIndexProbes)
	assert.Equal(t, source.DeltaIndexHits, destination.DeltaIndexHits)
	assert.Equal(t, source.DeltaIndexTombstones, destination.DeltaIndexTombstones)
	assert.Equal(t, source.BaseIndexProbes, destination.BaseIndexProbes)
	assert.Equal(t, source.BaseIndexCandidates, destination.BaseIndexCandidates)
	assert.Equal(t, source.BaseIndexHits, destination.BaseIndexHits)
	assert.Equal(t, source.BaseIndexFalsePositives, destination.BaseIndexFalsePositives)
	assert.Equal(t, source.AppendVecReadRanges, destination.AppendVecReadRanges)
	assert.Equal(t, source.AppendVecPreadCalls, destination.AppendVecPreadCalls)
	assert.Equal(t, source.AppendVecRequestedBytes, destination.AppendVecRequestedBytes)
	assert.Equal(t, source.AppendVecPhysicalReadBytes, destination.AppendVecPhysicalReadBytes)
}
