package accountsdb

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultProductionAccountIndexConfigIsValidAndMemoryBounded(t *testing.T) {
	config := DefaultProductionAccountIndexConfig()
	require.NoError(t, config.Validate())
	assert.Equal(t, DefaultPersistentIndexShards, config.ShardCount)
	assert.Equal(t, 30*time.Second, config.SealMaxAge)
	assert.LessOrEqual(t, config.MaxHotBytes, uint64(512<<20))
	assert.Equal(t, uint64(1<<30), config.MaxCheckpointSelectedBytes)
	assert.Equal(t, uint64(2<<30), config.MaxCheckpointPhysicalBytes)
	assert.GreaterOrEqual(t, config.RebaseKeys, config.SealKeys)
	assert.Positive(t, config.CheckpointWorkers)
	assert.Positive(t, config.MaxConcurrentSeals)
	assert.LessOrEqual(t, config.MaxConcurrentSeals, config.ShardCount)
	assert.Positive(t, config.RebaseWorkers)
}

func TestProductionAccountIndexConfigRejectsUnsafeSettings(t *testing.T) {
	valid := DefaultProductionAccountIndexConfig()
	tests := []struct {
		name string
		edit func(*ProductionAccountIndexConfig)
		want string
	}{
		{"non-power-of-two shards", func(c *ProductionAccountIndexConfig) { c.ShardCount = 1000 }, "power of two"},
		{"memory below key envelope", func(c *ProductionAccountIndexConfig) { c.MaxHotBytes = c.MaxHotKeys }, "cannot hold"},
		{"physical checkpoint budget below selected", func(c *ProductionAccountIndexConfig) {
			c.MaxCheckpointPhysicalBytes = c.MaxCheckpointSelectedBytes - 1
		}, "must be >= selected"},
		{"selected checkpoint budget cannot seal maximum hot shard", func(c *ProductionAccountIndexConfig) {
			minimum, err := checkpointBuildReservationBytes(c.MaxHotKeys)
			require.NoError(t, err)
			c.MaxCheckpointSelectedBytes = minimum - 1
		}, "cannot reserve one worst-case hot shard"},
		{"zero seal threshold", func(c *ProductionAccountIndexConfig) { c.SealKeys = 0 }, "seal keys"},
		{"rebase below seal", func(c *ProductionAccountIndexConfig) { c.RebaseKeys = c.SealKeys - 1 }, "rebase keys"},
		{"tiny journal rewrite", func(c *ProductionAccountIndexConfig) { c.JournalRewriteBytes = 1 }, "at least"},
		{"zero checkpoint workers", func(c *ProductionAccountIndexConfig) { c.CheckpointWorkers = 0 }, "checkpoint workers"},
		{"zero concurrent seals", func(c *ProductionAccountIndexConfig) { c.MaxConcurrentSeals = 0 }, "concurrent seals"},
		{"seals exceed shards", func(c *ProductionAccountIndexConfig) { c.MaxConcurrentSeals = c.ShardCount + 1 }, "concurrent seals"},
		{"zero rebase workers", func(c *ProductionAccountIndexConfig) { c.RebaseWorkers = 0 }, "rebase workers"},
		{"parallel rebase workers", func(c *ProductionAccountIndexConfig) { c.RebaseWorkers = 2 }, "serializes shared extent lineage"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			test.edit(&config)
			require.ErrorContains(t, config.Validate(), test.want)
		})
	}
}

func TestCurrentProductionAccountIndexConfigConvertsUnitsAndDetectsOverflow(t *testing.T) {
	oldShardCount := ProductionAccountIndexShardCount
	oldMaxHotKeys := ProductionAccountIndexMaxHotKeys
	oldMaxHotMB := ProductionAccountIndexMaxHotMB
	oldMaxCheckpointSelectedMB := ProductionAccountIndexMaxCheckpointSelectedMB
	oldMaxCheckpointPhysicalMB := ProductionAccountIndexMaxCheckpointPhysicalMB
	oldSealKeys := ProductionAccountIndexSealKeys
	oldSealAge := ProductionAccountIndexSealMaxAgeMS
	oldRebaseKeys := ProductionAccountIndexRebaseKeys
	oldRewriteMB := ProductionAccountIndexJournalRewriteMB
	oldCheckpointWorkers := ProductionAccountIndexCheckpointWorkers
	oldMaxConcurrentSeals := ProductionAccountIndexMaxConcurrentSeals
	oldRebaseWorkers := ProductionAccountIndexRebaseWorkers
	t.Cleanup(func() {
		ProductionAccountIndexShardCount = oldShardCount
		ProductionAccountIndexMaxHotKeys = oldMaxHotKeys
		ProductionAccountIndexMaxHotMB = oldMaxHotMB
		ProductionAccountIndexMaxCheckpointSelectedMB = oldMaxCheckpointSelectedMB
		ProductionAccountIndexMaxCheckpointPhysicalMB = oldMaxCheckpointPhysicalMB
		ProductionAccountIndexSealKeys = oldSealKeys
		ProductionAccountIndexSealMaxAgeMS = oldSealAge
		ProductionAccountIndexRebaseKeys = oldRebaseKeys
		ProductionAccountIndexJournalRewriteMB = oldRewriteMB
		ProductionAccountIndexCheckpointWorkers = oldCheckpointWorkers
		ProductionAccountIndexMaxConcurrentSeals = oldMaxConcurrentSeals
		ProductionAccountIndexRebaseWorkers = oldRebaseWorkers
	})

	ProductionAccountIndexMaxHotMB = 384
	ProductionAccountIndexMaxCheckpointSelectedMB = 768
	ProductionAccountIndexMaxCheckpointPhysicalMB = 1536
	ProductionAccountIndexJournalRewriteMB = 256
	ProductionAccountIndexSealMaxAgeMS = 45_000
	config, err := CurrentProductionAccountIndexConfig()
	require.NoError(t, err)
	assert.Equal(t, uint64(384<<20), config.MaxHotBytes)
	assert.Equal(t, uint64(768<<20), config.MaxCheckpointSelectedBytes)
	assert.Equal(t, uint64(1536<<20), config.MaxCheckpointPhysicalBytes)
	assert.Equal(t, uint64(256<<20), config.JournalRewriteBytes)
	assert.Equal(t, 45*time.Second, config.SealMaxAge)

	ProductionAccountIndexMaxHotMB = math.MaxUint64
	_, err = CurrentProductionAccountIndexConfig()
	require.ErrorContains(t, err, "overflows uint64")
	ProductionAccountIndexMaxHotMB = 384
	ProductionAccountIndexMaxCheckpointSelectedMB = math.MaxUint64
	_, err = CurrentProductionAccountIndexConfig()
	require.ErrorContains(t, err, "selected checkpoint bytes")
	ProductionAccountIndexMaxCheckpointSelectedMB = 768
	ProductionAccountIndexMaxCheckpointPhysicalMB = math.MaxUint64
	_, err = CurrentProductionAccountIndexConfig()
	require.ErrorContains(t, err, "physical checkpoint bytes")
}

func TestProductionAccountIndexConfigRejectsPersistedShardMismatch(t *testing.T) {
	config := DefaultProductionAccountIndexConfig()
	catalog := &RootIndexCatalog{ShardCount: uint32(config.ShardCount)}
	require.NoError(t, config.ValidateCatalogShardCount(catalog))
	catalog.ShardCount /= 2
	require.ErrorContains(t, config.ValidateCatalogShardCount(catalog), "does not match persisted")
}
