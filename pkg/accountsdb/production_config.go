package accountsdb

import (
	"fmt"
	"math"
	"runtime"
	"time"
)

const (
	DefaultProductionAccountIndexSealMaxAge                 = 30 * time.Second
	DefaultProductionAccountIndexMaxCheckpointSelectedBytes = uint64(1 << 30)
	DefaultProductionAccountIndexMaxCheckpointPhysicalBytes = uint64(2 << 30)
	productionAccountIndexMinJournalBytes                   = uint64(1 << 20)
	productionAccountIndexMaxWorkers                        = 4096
)

// ProductionAccountIndexConfig contains operator-tunable V2 index limits.
// Durable format choices are intentionally narrow: ShardCount is persisted in
// the root catalog and may not change when reopening an existing AccountsDB.
// The remaining fields bound memory or background maintenance work and may be
// changed across restarts.
type ProductionAccountIndexConfig struct {
	ShardCount  int
	MaxHotKeys  uint64
	MaxHotBytes uint64
	// MaxCheckpointSelectedBytes bounds the exact delta artifacts named by
	// the current root. MaxCheckpointPhysicalBytes additionally includes every
	// in-flight build reservation and reader-pinned obsolete delta generation.
	MaxCheckpointSelectedBytes uint64
	MaxCheckpointPhysicalBytes uint64
	SealKeys                   uint64
	SealMaxAge                 time.Duration
	RebaseKeys                 uint64
	JournalRewriteBytes        uint64
	CheckpointWorkers          int
	MaxConcurrentSeals         int
	RebaseWorkers              int
}

// DefaultProductionAccountIndexConfig is deliberately conservative about
// background I/O. In particular, the 30-second age threshold avoids creating
// tiny checkpoints across 1024 mostly-idle shards; global memory pressure can
// still force an earlier seal.
func DefaultProductionAccountIndexConfig() ProductionAccountIndexConfig {
	gomaxprocs := runtime.GOMAXPROCS(0)
	return ProductionAccountIndexConfig{
		ShardCount:                 DefaultPersistentIndexShards,
		MaxHotKeys:                 DefaultShardedMutableMaxHotKeys,
		MaxHotBytes:                DefaultShardedMutableMaxHotBytes,
		MaxCheckpointSelectedBytes: DefaultProductionAccountIndexMaxCheckpointSelectedBytes,
		MaxCheckpointPhysicalBytes: DefaultProductionAccountIndexMaxCheckpointPhysicalBytes,
		SealKeys:                   DefaultShardedMutableSealKeys,
		SealMaxAge:                 DefaultProductionAccountIndexSealMaxAge,
		RebaseKeys:                 DefaultShardedMutableRebaseKeys,
		JournalRewriteBytes:        DefaultShardedMutableJournalRewriteAt,
		CheckpointWorkers:          max(1, gomaxprocs),
		MaxConcurrentSeals:         DefaultShardedMutableMaxConcurrentSeals(),
		// Every shard build may extend the one shared extent-ordinal catalog.
		// Until that catalog has a merge protocol, rebases must serialize.
		RebaseWorkers: 1,
	}
}

var (
	ProductionAccountIndexShardCount              = DefaultProductionAccountIndexConfig().ShardCount
	ProductionAccountIndexMaxHotKeys              = DefaultProductionAccountIndexConfig().MaxHotKeys
	ProductionAccountIndexMaxHotMB                = DefaultProductionAccountIndexConfig().MaxHotBytes >> 20
	ProductionAccountIndexMaxCheckpointSelectedMB = DefaultProductionAccountIndexConfig().MaxCheckpointSelectedBytes >> 20
	ProductionAccountIndexMaxCheckpointPhysicalMB = DefaultProductionAccountIndexConfig().MaxCheckpointPhysicalBytes >> 20
	ProductionAccountIndexSealKeys                = DefaultProductionAccountIndexConfig().SealKeys
	ProductionAccountIndexSealMaxAgeMS            = int64(DefaultProductionAccountIndexConfig().SealMaxAge / time.Millisecond)
	ProductionAccountIndexRebaseKeys              = DefaultProductionAccountIndexConfig().RebaseKeys
	ProductionAccountIndexJournalRewriteMB        = DefaultProductionAccountIndexConfig().JournalRewriteBytes >> 20
	ProductionAccountIndexCheckpointWorkers       = DefaultProductionAccountIndexConfig().CheckpointWorkers
	ProductionAccountIndexMaxConcurrentSeals      = DefaultProductionAccountIndexConfig().MaxConcurrentSeals
	ProductionAccountIndexRebaseWorkers           = DefaultProductionAccountIndexConfig().RebaseWorkers
)

// CurrentProductionAccountIndexConfig snapshots the CLI/config-bound globals
// and converts human-friendly MiB/millisecond units exactly once at startup.
func CurrentProductionAccountIndexConfig() (ProductionAccountIndexConfig, error) {
	maxHotBytes, err := mibToBytes("account-index maximum hot memory", ProductionAccountIndexMaxHotMB)
	if err != nil {
		return ProductionAccountIndexConfig{}, err
	}
	journalBytes, err := mibToBytes("account-index journal rewrite threshold", ProductionAccountIndexJournalRewriteMB)
	if err != nil {
		return ProductionAccountIndexConfig{}, err
	}
	maxCheckpointSelectedBytes, err := mibToBytes(
		"account-index maximum selected checkpoint bytes",
		ProductionAccountIndexMaxCheckpointSelectedMB,
	)
	if err != nil {
		return ProductionAccountIndexConfig{}, err
	}
	maxCheckpointPhysicalBytes, err := mibToBytes(
		"account-index maximum physical checkpoint bytes",
		ProductionAccountIndexMaxCheckpointPhysicalMB,
	)
	if err != nil {
		return ProductionAccountIndexConfig{}, err
	}
	if ProductionAccountIndexSealMaxAgeMS <= 0 {
		return ProductionAccountIndexConfig{}, fmt.Errorf(
			"accountsdb: account-index seal maximum age must be > 0ms (got %d)",
			ProductionAccountIndexSealMaxAgeMS,
		)
	}
	if ProductionAccountIndexSealMaxAgeMS > math.MaxInt64/int64(time.Millisecond) {
		return ProductionAccountIndexConfig{}, fmt.Errorf(
			"accountsdb: account-index seal maximum age %dms overflows time.Duration",
			ProductionAccountIndexSealMaxAgeMS,
		)
	}
	config := ProductionAccountIndexConfig{
		ShardCount:                 ProductionAccountIndexShardCount,
		MaxHotKeys:                 ProductionAccountIndexMaxHotKeys,
		MaxHotBytes:                maxHotBytes,
		MaxCheckpointSelectedBytes: maxCheckpointSelectedBytes,
		MaxCheckpointPhysicalBytes: maxCheckpointPhysicalBytes,
		SealKeys:                   ProductionAccountIndexSealKeys,
		SealMaxAge:                 time.Duration(ProductionAccountIndexSealMaxAgeMS) * time.Millisecond,
		RebaseKeys:                 ProductionAccountIndexRebaseKeys,
		JournalRewriteBytes:        journalBytes,
		CheckpointWorkers:          ProductionAccountIndexCheckpointWorkers,
		MaxConcurrentSeals:         ProductionAccountIndexMaxConcurrentSeals,
		RebaseWorkers:              ProductionAccountIndexRebaseWorkers,
	}
	if err := config.Validate(); err != nil {
		return ProductionAccountIndexConfig{}, err
	}
	return config, nil
}

func mibToBytes(label string, mib uint64) (uint64, error) {
	if mib > math.MaxUint64>>20 {
		return 0, fmt.Errorf("accountsdb: %s %dMiB overflows uint64", label, mib)
	}
	return mib << 20, nil
}

// Validate rejects settings that would be misleading or could make forward
// progress impossible under the deterministic mutable-index accounting model.
func (config ProductionAccountIndexConfig) Validate() error {
	config = config.withCheckpointBudgetDefaults()
	if err := ValidatePersistentIndexShardCount(config.ShardCount); err != nil {
		return fmt.Errorf("accountsdb: invalid production account-index shard count: %w", err)
	}
	if config.MaxHotKeys == 0 {
		return fmt.Errorf("accountsdb: account-index maximum hot keys must be > 0")
	}
	if config.MaxHotBytes < DefaultShardedMutableBytesPerKey ||
		config.MaxHotKeys > config.MaxHotBytes/DefaultShardedMutableBytesPerKey {
		return fmt.Errorf(
			"accountsdb: account-index hot memory %d bytes cannot hold configured %d keys at %d bytes/key",
			config.MaxHotBytes,
			config.MaxHotKeys,
			DefaultShardedMutableBytesPerKey,
		)
	}
	if config.MaxCheckpointSelectedBytes == 0 {
		return fmt.Errorf("accountsdb: account-index maximum selected checkpoint bytes must be > 0")
	}
	if config.MaxCheckpointPhysicalBytes < config.MaxCheckpointSelectedBytes {
		return fmt.Errorf(
			"accountsdb: account-index maximum physical checkpoint bytes %d must be >= selected bytes %d",
			config.MaxCheckpointPhysicalBytes,
			config.MaxCheckpointSelectedBytes,
		)
	}
	maxHotCheckpointReservation, err := checkpointBuildReservationBytes(config.MaxHotKeys)
	if err != nil {
		return fmt.Errorf("accountsdb: validate maximum checkpoint build: %w", err)
	}
	if config.MaxCheckpointSelectedBytes < maxHotCheckpointReservation {
		return fmt.Errorf(
			"accountsdb: account-index maximum selected checkpoint bytes %d cannot reserve one worst-case hot shard (%d bytes for %d keys)",
			config.MaxCheckpointSelectedBytes,
			maxHotCheckpointReservation,
			config.MaxHotKeys,
		)
	}
	if config.SealKeys == 0 || config.SealKeys > config.MaxHotKeys {
		return fmt.Errorf(
			"accountsdb: account-index seal keys must be in [1,%d] (got %d)",
			config.MaxHotKeys,
			config.SealKeys,
		)
	}
	if config.SealMaxAge <= 0 {
		return fmt.Errorf("accountsdb: account-index seal maximum age must be > 0 (got %s)", config.SealMaxAge)
	}
	if config.RebaseKeys < config.SealKeys || config.RebaseKeys > config.MaxHotKeys {
		return fmt.Errorf(
			"accountsdb: account-index rebase keys must be in [%d,%d] (got %d)",
			config.SealKeys,
			config.MaxHotKeys,
			config.RebaseKeys,
		)
	}
	if config.JournalRewriteBytes < productionAccountIndexMinJournalBytes {
		return fmt.Errorf(
			"accountsdb: account-index journal rewrite threshold must be at least %d bytes (got %d)",
			productionAccountIndexMinJournalBytes,
			config.JournalRewriteBytes,
		)
	}
	if config.CheckpointWorkers < 1 || config.CheckpointWorkers > productionAccountIndexMaxWorkers {
		return fmt.Errorf(
			"accountsdb: account-index checkpoint workers must be in [1,%d] (got %d)",
			productionAccountIndexMaxWorkers,
			config.CheckpointWorkers,
		)
	}
	if config.MaxConcurrentSeals < 1 || config.MaxConcurrentSeals > config.ShardCount {
		return fmt.Errorf(
			"accountsdb: account-index maximum concurrent seals must be in [1,%d] (got %d)",
			config.ShardCount,
			config.MaxConcurrentSeals,
		)
	}
	if config.RebaseWorkers != 1 {
		return fmt.Errorf(
			"accountsdb: account-index rebase workers must be 1 because the current format serializes shared extent lineage (got %d)",
			config.RebaseWorkers,
		)
	}
	return nil
}

// withCheckpointBudgetDefaults preserves source compatibility for callers
// constructing the pre-budget config struct directly. CLI/config startup
// always supplies explicit values; zero in either newly-added field means the
// conservative production default, never an unbounded budget.
func (config ProductionAccountIndexConfig) withCheckpointBudgetDefaults() ProductionAccountIndexConfig {
	if config.MaxCheckpointSelectedBytes == 0 {
		config.MaxCheckpointSelectedBytes = DefaultProductionAccountIndexMaxCheckpointSelectedBytes
	}
	if config.MaxCheckpointPhysicalBytes == 0 {
		config.MaxCheckpointPhysicalBytes = DefaultProductionAccountIndexMaxCheckpointPhysicalBytes
	}
	return config
}

// ValidateCatalogShardCount prevents an operator typo from silently changing
// the durable routing function of an existing AccountsDB.
func (config ProductionAccountIndexConfig) ValidateCatalogShardCount(catalog *RootIndexCatalog) error {
	if catalog == nil {
		return fmt.Errorf("accountsdb: nil root catalog")
	}
	if int(catalog.ShardCount) != config.ShardCount {
		return fmt.Errorf(
			"accountsdb: configured account-index shard count %d does not match persisted root catalog %d; rebuild from a fresh snapshot to change it",
			config.ShardCount,
			catalog.ShardCount,
		)
	}
	return nil
}
