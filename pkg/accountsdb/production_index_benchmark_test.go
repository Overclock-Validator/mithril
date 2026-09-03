package accountsdb

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

const (
	productionIndexBenchmarkBaseKeys  = 1_000_000
	productionIndexBenchmarkBatchKeys = 30_000
	// Sixteen shards keep fixture publication practical while leaving enough
	// keys per shard that StreamHash's fixed file overhead does not dominate
	// the reported bytes/key. Routing and lookup code are the production path.
	productionIndexBenchmarkShards = 16
)

var (
	productionIndexBenchmarkEntrySink  AccountIndexEntry
	productionIndexBenchmarkSourceSink accountIndexSource
	productionIndexBenchmarkFoundSink  bool
)

type productionIndexBenchmarkRecord struct {
	key   solana.PublicKey
	entry AccountIndexEntry
	class uint8
}

type productionIndexBenchmarkSource struct {
	records []productionIndexBenchmarkRecord
}

func (source *productionIndexBenchmarkSource) Scan(
	ctx context.Context,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) error {
	for i := range source.records {
		if i&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := visit(source.records[i].key, source.records[i].entry); err != nil {
			return err
		}
	}
	return ctx.Err()
}

type productionIndexBenchmarkFootprint struct {
	baseKeys        uint64
	baseIndexBytes  uint64
	baseScanBytes   uint64
	extentBytes     uint64
	deltaKeys       uint64
	deltaIndex      uint64
	deltaRecords    uint64
	deltaDescriptor uint64
}

// BenchmarkProductionAccountIndex uses the complete durable production stack.
// Base and checkpoint construction, WAL writes, snapshot capture and result
// validation all remain outside the timed regions.
func BenchmarkProductionAccountIndex(b *testing.B) {
	records := productionIndexBenchmarkRecords(productionIndexBenchmarkBaseKeys)
	root := b.TempDir()
	config := productionIndexBenchmarkConfig()
	if err := InitializeProductionAccountIndex(
		context.Background(),
		root,
		&productionIndexBenchmarkSource{records: records},
		config,
	); err != nil {
		b.Fatal(err)
	}
	index, err := OpenProductionAccountIndex(root, config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := index.Close(); err != nil {
			b.Errorf("close production index: %v", err)
		}
	})

	queries, expected := productionIndexBenchmarkQueries(records, productionIndexBenchmarkBatchKeys)
	baseFootprint := measureProductionIndexBenchmarkFootprint(b, index)
	if baseFootprint.baseKeys != productionIndexBenchmarkBaseKeys {
		b.Fatalf("base contains %d keys, want %d", baseFootprint.baseKeys, productionIndexBenchmarkBaseKeys)
	}

	b.Run("base_batch_30k_pinned", func(b *testing.B) {
		snapshot, err := index.NewSnapshot(queries)
		if err != nil {
			b.Fatal(err)
		}
		defer snapshot.Close()
		values := make([]deltaIndexValue, len(queries))
		sources := make([]accountIndexSource, len(queries))
		found := make([]bool, len(queries))
		if err := snapshot.LookupBatch(context.Background(), values, sources, found); err != nil {
			b.Fatal(err)
		}
		validateProductionIndexBenchmarkBatch(b, expected, queries, values, sources, found, false)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := snapshot.LookupBatch(context.Background(), values, sources, found); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportProductionIndexBaseFootprint(b, baseFootprint)
		b.ReportMetric(float64(len(queries)), "keys/op")
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/float64(b.N*len(queries)),
			"ns/key",
		)
	})

	b.Run("single_base", func(b *testing.B) {
		key := queries[len(queries)/2]
		entry, source, found, err := index.LookupCandidate(key)
		if err != nil || !found || source != accountIndexSourceBase {
			b.Fatalf("initial single lookup = (%+v,%v,%v,%v)", entry, source, found, err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			entry, source, found, err = index.LookupCandidate(key)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportProductionIndexBaseFootprint(b, baseFootprint)
		productionIndexBenchmarkEntrySink = entry
		productionIndexBenchmarkSourceSink = source
		productionIndexBenchmarkFoundSink = found
	})

	deltaMutations := make([]deltaIndexMutation, 0, len(records)/3)
	for i := range records {
		if records[i].class != 1 {
			continue
		}
		deltaMutations = append(deltaMutations, liveDeltaMutation(records[i].key, productionIndexBenchmarkUpdatedEntry(records[i], 1)))
	}
	if err := index.Apply(deltaMutations, nil, true); err != nil {
		b.Fatal(err)
	}
	if err := index.ForceSeal(context.Background()); err != nil {
		b.Fatal(err)
	}
	hotMutations := make([]deltaIndexMutation, 0, len(queries)/3)
	for _, key := range queries {
		record := expected[key]
		if record.class != 2 {
			continue
		}
		hotMutations = append(hotMutations, liveDeltaMutation(key, productionIndexBenchmarkUpdatedEntry(record, 2)))
	}
	if err := index.Apply(hotMutations, nil, true); err != nil {
		b.Fatal(err)
	}
	mixedFootprint := measureProductionIndexBenchmarkFootprint(b, index)

	b.Run("mixed_hot_delta_base_batch_30k_pinned", func(b *testing.B) {
		snapshot, err := index.NewSnapshot(queries)
		if err != nil {
			b.Fatal(err)
		}
		defer snapshot.Close()
		values := make([]deltaIndexValue, len(queries))
		sources := make([]accountIndexSource, len(queries))
		found := make([]bool, len(queries))
		if err := snapshot.LookupBatch(context.Background(), values, sources, found); err != nil {
			b.Fatal(err)
		}
		validateProductionIndexBenchmarkBatch(b, expected, queries, values, sources, found, true)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := snapshot.LookupBatch(context.Background(), values, sources, found); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportProductionIndexBaseFootprint(b, mixedFootprint)
		reportProductionIndexDeltaFootprint(b, mixedFootprint)
		b.ReportMetric(float64(len(queries)), "keys/op")
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/float64(b.N*len(queries)),
			"ns/key",
		)
	})
}

// BenchmarkSnapshotImmutableHandoff isolates the startup work removed by the
// prepared snapshot path. The retained case performs the immutable-artifact
// stability portion of adoption; the reopen case repeats every full immutable
// SHA/CRC/semantic/StreamHash verification. Selector/WAL reads and mutable
// runtime construction are intentionally outside both cases. Use
// -benchtime=1x when comparing a large fixture on development hardware.
func BenchmarkSnapshotImmutableHandoff(b *testing.B) {
	records := productionIndexBenchmarkRecords(productionIndexBenchmarkBaseKeys)
	root := b.TempDir()
	config := productionIndexBenchmarkConfig()
	if err := InitializeProductionAccountIndex(
		context.Background(),
		root,
		&productionIndexBenchmarkSource{records: records},
		config,
	); err != nil {
		b.Fatal(err)
	}
	catalog, err := ReadRootIndexCatalog(root)
	if err != nil {
		b.Fatal(err)
	}
	retained, err := OpenShardedImmutableIndex(root, catalog)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := retained.closeUnmanaged(); err != nil {
			b.Errorf("close retained immutable generation: %v", err)
		}
	})

	b.Run("retained_metadata_adoption_check", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := retained.validatePreparedSnapshotArtifactPathsStable(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("full_immutable_reopen", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			reopened, err := OpenShardedImmutableIndex(root, catalog)
			if err != nil {
				b.Fatal(err)
			}
			if err := reopened.closeUnmanaged(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func productionIndexBenchmarkConfig() ProductionAccountIndexConfig {
	const maxHotKeys = uint64(500_000)
	workers := min(4, runtime.GOMAXPROCS(0))
	return ProductionAccountIndexConfig{
		ShardCount:          productionIndexBenchmarkShards,
		MaxHotKeys:          maxHotKeys,
		MaxHotBytes:         maxHotKeys * DefaultShardedMutableBytesPerKey,
		SealKeys:            450_000,
		SealMaxAge:          time.Hour,
		RebaseKeys:          maxHotKeys,
		JournalRewriteBytes: 1 << 30,
		CheckpointWorkers:   max(1, workers),
		MaxConcurrentSeals:  1,
		RebaseWorkers:       1,
	}
}

func productionIndexBenchmarkRecords(count int) []productionIndexBenchmarkRecord {
	records := make([]productionIndexBenchmarkRecord, count)
	step := uint64(math.MaxUint64) / uint64(count)
	for i := range records {
		var key solana.PublicKey
		binary.BigEndian.PutUint64(key[:8], uint64(i)*step)
		binary.LittleEndian.PutUint64(key[24:], uint64(i+1))
		records[i] = productionIndexBenchmarkRecord{
			key: key,
			entry: AccountIndexEntry{
				Slot:   1,
				FileId: 1,
				Offset: uint64(i+1) * 8,
			},
			class: uint8(i % 3),
		}
	}
	return records
}

func productionIndexBenchmarkQueries(
	records []productionIndexBenchmarkRecord,
	count int,
) ([]solana.PublicKey, map[solana.PublicKey]productionIndexBenchmarkRecord) {
	if count > len(records) {
		panic("production account-index benchmark query count exceeds fixture size")
	}
	queries := make([]solana.PublicKey, count)
	want := make(map[solana.PublicKey]productionIndexBenchmarkRecord, count)
	// 7,919 is coprime to 1,000,000. It yields a deterministic sample spread
	// across the full mapping while interleaving hot/delta/base classes.
	for i := range queries {
		record := records[(i*7_919)%len(records)]
		queries[i] = record.key
		want[record.key] = record
	}
	return queries, want
}

func productionIndexBenchmarkUpdatedEntry(record productionIndexBenchmarkRecord, epoch uint64) AccountIndexEntry {
	return AccountIndexEntry{
		Slot:   100 + epoch,
		FileId: 100 + epoch,
		Offset: record.entry.Offset,
	}
}

func validateProductionIndexBenchmarkBatch(
	b *testing.B,
	want map[solana.PublicKey]productionIndexBenchmarkRecord,
	queries []solana.PublicKey,
	values []deltaIndexValue,
	sources []accountIndexSource,
	found []bool,
	mixed bool,
) {
	b.Helper()
	for i, key := range queries {
		record := want[key]
		wantEntry := record.entry
		wantSource := accountIndexSourceBase
		if mixed && record.class != 0 {
			wantEntry = productionIndexBenchmarkUpdatedEntry(record, uint64(record.class))
			wantSource = accountIndexSourceDelta
		}
		if !found[i] || sources[i] != wantSource || values[i].Tombstone || values[i].Entry != wantEntry {
			b.Fatalf(
				"lookup %d = (%+v,%v,%v), want (%+v,%v,true)",
				i, values[i], sources[i], found[i], wantEntry, wantSource,
			)
		}
	}
}

func measureProductionIndexBenchmarkFootprint(
	b *testing.B,
	index *ProductionAccountIndex,
) productionIndexBenchmarkFootprint {
	b.Helper()
	view, err := index.view.Acquire()
	if err != nil {
		b.Fatal(err)
	}
	defer view.Close()
	catalog := view.Catalog()
	payload, ok := view.Payload().(*ShardedImmutableIndex)
	if !ok || payload == nil {
		b.Fatal("invalid immutable benchmark payload")
	}
	footprint := productionIndexBenchmarkFootprint{
		extentBytes: catalog.SharedExtentCatalog.Size,
	}
	for shardID, shard := range catalog.Shards {
		footprint.baseKeys += payload.BaseShard(uint32(shardID)).NumKeys()
		footprint.baseIndexBytes += shard.BaseIndex.Size
		footprint.baseScanBytes += shard.BaseRecords.Size
		if shard.DeltaGeneration == 0 {
			continue
		}
		checkpoint := payload.DeltaShard(uint32(shardID))
		if checkpoint == nil {
			b.Fatalf("catalog shard %d selects a missing delta", shardID)
		}
		footprint.deltaKeys += checkpoint.Len()
		footprint.deltaIndex += shard.DeltaIndex.Size
		footprint.deltaRecords += shard.DeltaRecords.Size
		descriptorName := filepath.Base(makeDeltaCheckpointPaths("", shard.DeltaGeneration).descriptor)
		descriptorPath := filepath.Join(
			index.root,
			filepath.FromSlash(filepath.Dir(shard.DeltaIndex.RelativePath)),
			descriptorName,
		)
		info, err := os.Stat(descriptorPath)
		if err != nil {
			b.Fatalf("stat delta descriptor: %v", err)
		}
		footprint.deltaDescriptor += uint64(info.Size())
	}
	return footprint
}

func reportProductionIndexBaseFootprint(b *testing.B, footprint productionIndexBenchmarkFootprint) {
	b.Helper()
	if footprint.baseKeys == 0 {
		return
	}
	keys := float64(footprint.baseKeys)
	b.ReportMetric(float64(footprint.baseIndexBytes)/keys, "base-stmh-B/key")
	b.ReportMetric(float64(footprint.baseScanBytes)/keys, "base-scan-B/key")
	b.ReportMetric(float64(footprint.baseIndexBytes+footprint.baseScanBytes+footprint.extentBytes)/keys, "base-disk-B/key")
}

func reportProductionIndexDeltaFootprint(b *testing.B, footprint productionIndexBenchmarkFootprint) {
	b.Helper()
	if footprint.deltaKeys == 0 {
		return
	}
	keys := float64(footprint.deltaKeys)
	b.ReportMetric(float64(footprint.deltaIndex)/keys, "delta-stmh-B/key")
	b.ReportMetric(float64(footprint.deltaRecords)/keys, "delta-record-B/key")
	b.ReportMetric(
		float64(footprint.deltaIndex+footprint.deltaRecords+footprint.deltaDescriptor)/keys,
		"delta-total-B/key",
	)
}
