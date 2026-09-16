package rsrecover

import (
	"fmt"
	"testing"

	"github.com/klauspost/reedsolomon"
)

var (
	benchmarkBytesSink []byte
	benchmarkPlanSink  DataSubsetPlan
)

type recoveryBenchmarkFixture struct {
	original  [][]byte
	available [][]byte
	presence  uint64
	missing   []int
}

func makeRecoveryBenchmarkFixture(b *testing.B, missingCount int) recoveryBenchmarkFixture {
	b.Helper()
	original := encodedFixture(b, 987)
	missing := make([]int, missingCount)
	coding := make([]int, missingCount)
	for index := 0; index < missingCount; index++ {
		// A non-prefix pattern exercises coefficient selection without making
		// the benchmark depend on random-number generation.
		missing[index] = (index*11 + 3) % DataShards
		coding[index] = (index*7 + 5) % CodingShards
	}
	available := availableFixture(original, missing, coding)
	presence, err := Presence(available)
	if err != nil {
		b.Fatal(err)
	}
	return recoveryBenchmarkFixture{
		original:  original,
		available: available,
		presence:  presence,
		missing:   missing,
	}
}

func BenchmarkRecoverOneData(b *testing.B) {
	fixture := makeRecoveryBenchmarkFixture(b, 1)
	missing := fixture.missing[0]
	b.Run("direct/prepare", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			plan, err := PrepareRecoverOneData(fixture.presence, missing)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkBytesSink = plan.coefficients[:]
		}
	})
	b.Run("direct/execute", func(b *testing.B) {
		plan, err := PrepareRecoverOneData(fixture.presence, missing)
		if err != nil {
			b.Fatal(err)
		}
		dst := make([]byte, len(fixture.original[missing]))
		b.ReportAllocs()
		b.SetBytes(int64(len(dst)))
		b.ResetTimer()
		for b.Loop() {
			if err := plan.Recover(fixture.available, dst); err != nil {
				b.Fatal(err)
			}
			benchmarkBytesSink = dst
		}
	})
	b.Run("direct/prepare-and-execute", func(b *testing.B) {
		dst := make([]byte, len(fixture.original[missing]))
		b.ReportAllocs()
		b.SetBytes(int64(len(dst)))
		b.ResetTimer()
		for b.Loop() {
			plan, err := PrepareRecoverOneData(fixture.presence, missing)
			if err != nil {
				b.Fatal(err)
			}
			if err := plan.Recover(fixture.available, dst); err != nil {
				b.Fatal(err)
			}
			benchmarkBytesSink = dst
		}
	})
	benchmarkGeneralRecovery(b, fixture, false)
	benchmarkGeneralRecovery(b, fixture, true)
}

func BenchmarkRecoverDataSubset(b *testing.B) {
	for _, missingCount := range []int{2, 4, 8, 16, 24, 32} {
		fixture := makeRecoveryBenchmarkFixture(b, missingCount)
		b.Run(fmt.Sprintf("missing-%02d/reduced/prepare", missingCount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				plan, err := PrepareRecoverDataSubset(fixture.presence)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkPlanSink = plan
			}
		})
		b.Run(fmt.Sprintf("missing-%02d/reduced/execute", missingCount), func(b *testing.B) {
			plan, err := PrepareRecoverDataSubset(fixture.presence)
			if err != nil {
				b.Fatal(err)
			}
			dst := make([][]byte, missingCount)
			for index := range dst {
				dst[index] = make([]byte, len(fixture.original[0]))
			}
			b.ReportAllocs()
			b.SetBytes(int64(missingCount * len(fixture.original[0])))
			b.ResetTimer()
			for b.Loop() {
				if err := plan.Recover(fixture.available, dst); err != nil {
					b.Fatal(err)
				}
				benchmarkBytesSink = dst[0]
			}
		})
		b.Run(fmt.Sprintf("missing-%02d/reduced/prepare-and-execute", missingCount), func(b *testing.B) {
			dst := make([][]byte, missingCount)
			for index := range dst {
				dst[index] = make([]byte, len(fixture.original[0]))
			}
			b.ReportAllocs()
			b.SetBytes(int64(missingCount * len(fixture.original[0])))
			b.ResetTimer()
			for b.Loop() {
				plan, err := PrepareRecoverDataSubset(fixture.presence)
				if err != nil {
					b.Fatal(err)
				}
				if err := plan.Recover(fixture.available, dst); err != nil {
					b.Fatal(err)
				}
				benchmarkBytesSink = dst[0]
			}
		})
		b.Run(fmt.Sprintf("missing-%02d", missingCount), func(b *testing.B) {
			benchmarkGeneralRecovery(b, fixture, false)
			benchmarkGeneralRecovery(b, fixture, true)
		})
	}
}

func BenchmarkRecoverAllDataFromCoding(b *testing.B) {
	fixture := makeRecoveryBenchmarkFixture(b, DataShards)
	plan, err := PrepareRecoverAllDataFromCoding(fixture.presence)
	if err != nil {
		b.Fatal(err)
	}
	dst := make([][]byte, DataShards)
	for index := range dst {
		dst[index] = make([]byte, len(fixture.original[0]))
	}
	b.Run("coding-involution/execute", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(DataShards * len(fixture.original[0])))
		for b.Loop() {
			if err := plan.Recover(fixture.available, dst); err != nil {
				b.Fatal(err)
			}
			benchmarkBytesSink = dst[0]
		}
	})
	benchmarkGeneralRecovery(b, fixture, false)
	benchmarkGeneralRecovery(b, fixture, true)
}

func benchmarkGeneralRecovery(b *testing.B, fixture recoveryBenchmarkFixture, inversionCache bool) {
	name := "general/cache-off"
	if inversionCache {
		name = "general/cache-on-repeated-pattern"
	}
	b.Run(name, func(b *testing.B) {
		encoder, err := reedsolomon.New(
			DataShards,
			CodingShards,
			reedsolomon.WithInversionCache(inversionCache),
		)
		if err != nil {
			b.Fatal(err)
		}
		shards := append([][]byte(nil), fixture.available...)
		required := make([]bool, TotalShards)
		outputs := make(map[int][]byte, len(fixture.missing))
		for _, index := range fixture.missing {
			required[index] = true
			outputs[index] = make([]byte, 0, len(fixture.original[index]))
			shards[index] = outputs[index]
		}
		if inversionCache {
			if err := encoder.ReconstructSome(shards, required); err != nil {
				b.Fatal(err)
			}
			for _, index := range fixture.missing {
				outputs[index] = shards[index][:0]
				shards[index] = outputs[index]
			}
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(fixture.missing) * len(fixture.original[0])))
		b.ResetTimer()
		for b.Loop() {
			if err := encoder.ReconstructSome(shards, required); err != nil {
				b.Fatal(err)
			}
			benchmarkBytesSink = shards[fixture.missing[0]]
			for _, index := range fixture.missing {
				outputs[index] = shards[index][:0]
				shards[index] = outputs[index]
			}
		}
	})
}
