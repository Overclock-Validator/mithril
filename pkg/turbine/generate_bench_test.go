package turbine

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/gagliardetto/solana-go"
	"github.com/klauspost/reedsolomon"
)

const (
	benchmarkTransactionCount = 50_000
	benchmarkTransactionBytes = 1_232
)

var (
	benchmarkEncoderSink reedsolomon.Encoder
	benchmarkPacketsSink [][]byte
	benchmarkRootSink    solana.Hash
	benchmarkByteSink    byte
)

func benchmarkLeaderKey() solana.PrivateKey {
	var seed [ed25519.SeedSize]byte
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	return solana.PrivateKey(ed25519.NewKeyFromSeed(seed[:]))
}

func benchmarkPayload(size int) []byte {
	payload := make([]byte, size)
	var state uint64 = 0x9e3779b97f4a7c15
	for i := range payload {
		// A deterministic, non-zero corpus avoids accidentally benchmarking a
		// special all-zero input while keeping fixture construction out of the
		// timed region.
		state ^= state << 7
		state ^= state >> 9
		state ^= state << 8
		payload[i] = byte(state)
	}
	return payload
}

// BenchmarkReedSolomonEncode32x32 isolates the arithmetic kernel used by one
// unsigned 32+32 chained FEC set. Encoder construction, shred parsing, Merkle
// hashing, signing, and packet copies are intentionally outside this result.
func BenchmarkReedSolomonEncode32x32(b *testing.B) {
	const shardBytes = 987 // unsigned chained 32+32 shreds with proof size 6
	encoder, err := reedsolomon.New(dataShredsPerFECBlock, codingShredsPerFECBlock)
	if err != nil {
		b.Fatal(err)
	}
	shards := make([][]byte, dataShredsPerFECBlock+codingShredsPerFECBlock)
	for i := range shards {
		shards[i] = make([]byte, shardBytes)
		if i < dataShredsPerFECBlock {
			copy(shards[i], benchmarkPayload(shardBytes))
		}
	}

	b.SetBytes(dataShredsPerFECBlock * shardBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := encoder.Encode(shards); err != nil {
			b.Fatal(err)
		}
	}
	benchmarkByteSink = shards[len(shards)-1][shardBytes-1]
}

// BenchmarkReedSolomonNew32x32 measures work that finishErasureBatch currently
// repeats for every FEC set even though the 32+32 shape never changes.
func BenchmarkReedSolomonNew32x32(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder, err := reedsolomon.New(dataShredsPerFECBlock, codingShredsPerFECBlock)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkEncoderSink = encoder
	}
}

// BenchmarkMakeShredsFromData reports the complete current generator cost,
// including packet construction, Reed-Solomon coding, chained Merkle trees,
// one Ed25519 signature per FEC set, and proof materialization.
func BenchmarkMakeShredsFromData(b *testing.B) {
	const blockBytes = benchmarkTransactionCount * benchmarkTransactionBytes
	cases := []struct {
		name         string
		size         int
		isLastInSlot bool
	}{
		{name: "one-unsigned-fec", size: dataShredsPerFECBlock * dataCapacity(proofEntriesFor32x32, false)},
		{name: "block-50000x1232", size: blockBytes, isLastInSlot: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			leader := benchmarkLeaderKey()
			payload := benchmarkPayload(tc.size)
			gen := ShredGenerator{Slot: 10, ParentSlot: 9, Version: 1}

			b.SetBytes(int64(tc.size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				packets, root, _, _, err := gen.MakeShredsFromData(leader, payload, tc.isLastInSlot, solana.Hash{}, 0, 0)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkPacketsSink = packets
				benchmarkRootSink = root
			}
			b.StopTimer()
			if len(benchmarkPacketsSink) == 0 || len(benchmarkPacketsSink)%64 != 0 {
				b.Fatalf("unexpected packet count %d", len(benchmarkPacketsSink))
			}
			b.ReportMetric(float64(len(benchmarkPacketsSink)), "packets/op")
			b.ReportMetric(float64(len(benchmarkPacketsSink)/64), "FEC-sets/op")
			if tc.size == blockBytes {
				b.ReportMetric(benchmarkTransactionCount, "transactions/op")
			}
		})
	}
}

// BenchmarkMakeShreds50000TargetBatches1232 models the producer's target-sized
// component stream without retaining a multi-gigabyte output. It measures only
// the 61.6 MB transaction payload; entry framing is deliberately outside this
// erasure-coding benchmark.
func BenchmarkMakeShreds50000TargetBatches1232(b *testing.B) {
	const inputBytes = benchmarkTransactionCount * benchmarkTransactionBytes
	leader := benchmarkLeaderKey()
	payload := benchmarkPayload(inputBytes)
	gen := ShredGenerator{Slot: 10, ParentSlot: 9, Version: 1}

	b.SetBytes(inputBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var (
			root      solana.Hash
			dataIndex uint32
			codeIndex uint32
		)
		var (
			offset      int
			components  int
			packetCount int
			fecSetCount int
		)
		for offset < len(payload) {
			end := min(offset+costmodel.DefaultTargetBatchBytes, len(payload))
			packets, nextRoot, nextData, nextCode, err := gen.MakeShredsFromData(
				leader,
				payload[offset:end],
				end == len(payload),
				root,
				dataIndex,
				codeIndex,
			)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkPacketsSink = packets
			components++
			packetCount += len(packets)
			fecSetCount += len(packets) / (dataShredsPerFECBlock + codingShredsPerFECBlock)
			root, dataIndex, codeIndex = nextRoot, nextData, nextCode
			offset = end
		}
		b.ReportMetric(float64(components), "components/op")
		b.ReportMetric(float64(fecSetCount), "FEC-sets/op")
		b.ReportMetric(float64(packetCount), "packets/op")
		benchmarkRootSink = root
	}
	b.StopTimer()
	b.ReportMetric(benchmarkTransactionCount, "transactions/op")
}

func TestBenchmarkBlockPayloadAccounting(t *testing.T) {
	const blockBytes = benchmarkTransactionCount * benchmarkTransactionBytes
	unsignedBatch := dataShredsPerFECBlock * dataCapacity(proofEntriesFor32x32, false)
	signedBatch := dataShredsPerFECBlock * dataCapacity(proofEntriesFor32x32, true)
	unsignedBytes := blockBytes - signedBatch
	unsignedFECs := (unsignedBytes + unsignedBatch - 1) / unsignedBatch
	totalFECs := unsignedFECs + 1
	if totalFECs != 2000 {
		t.Fatalf("50k x 1232 payload maps to %d FEC sets, want 2000 (%s)", totalFECs, fmt.Sprintf("%d bytes", blockBytes))
	}
}

func TestProducerTargetMatchesTwoTypicalFECPayloads(t *testing.T) {
	want := 2 * dataShredsPerFECBlock * dataCapacity(proofEntriesFor32x32, false)
	if costmodel.DefaultTargetBatchBytes != want {
		t.Fatalf("producer target = %d, want two typical FEC payloads = %d", costmodel.DefaultTargetBatchBytes, want)
	}
}

func TestMakeShredsFromDataStableBytes(t *testing.T) {
	unsignedBatch := dataShredsPerFECBlock * dataCapacity(proofEntriesFor32x32, false)
	signedBatch := dataShredsPerFECBlock * dataCapacity(proofEntriesFor32x32, true)
	tests := []struct {
		name         string
		size         int
		isLastInSlot bool
		want         string
	}{
		{name: "unsigned-one-fec", size: unsignedBatch, want: "f9334f1835240df21d4b48a09f35b3ff90578122d30d0527608c10d38d0911f7"},
		{name: "signed-one-fec", size: signedBatch, isLastInSlot: true, want: "bfa398c445509c5e1345553bbe84fea04e86001caf441008b4014d07c6e36ccd"},
		{name: "two-unsigned-one-signed", size: 2*unsignedBatch + signedBatch, isLastInSlot: true, want: "1374b3b1cc35dab1fddb248a8924be73b8a22d2c42a63ce87663b3ffff3762a5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gen := ShredGenerator{Slot: 10, ParentSlot: 9, Version: 1, ReferenceTick: 17}
			packets, _, _, _, err := gen.MakeShredsFromData(
				benchmarkLeaderKey(), benchmarkPayload(tt.size), tt.isLastInSlot,
				solana.Hash{3}, 7, 11,
			)
			if err != nil {
				t.Fatal(err)
			}
			h := sha256.New()
			for _, packet := range packets {
				_, _ = h.Write(packet)
			}
			got := hex.EncodeToString(h.Sum(nil))
			if got != tt.want {
				t.Fatalf("packet digest %s, want %s", got, tt.want)
			}
		})
	}
}
