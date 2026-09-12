package turbine

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sigverify"
	"github.com/gagliardetto/solana-go"
)

// BenchmarkEntryPrefetchAssembly runs actual assembler ingestion, component
// decoding, final Merkle identity checks, transaction verification, and final
// publication. Source transactions use the same generated 228/1232-byte or
// captured fixtures as BenchmarkTransactionVerificationFlow. Generation,
// packet parsing and shred-signature authentication happen outside the timer.
// There is no network I/O, loss/recovery, replay, PoH or footer-bankhash execution.
//
// Complete component bursts are scheduled across 200 ms for tip scenarios;
// catchup offers all shreds immediately. This is a workload model, not a
// measured cluster arrival distribution. A 60 KiB signed-transaction budget
// defines components; actual entry overhead and FEC padding are generated.
// Pools persist across iterations; the identical fixture slot is reset between
// samples outside the timer. Use fixed counts (e.g. -benchtime=3x).
func BenchmarkEntryPrefetchAssembly(b *testing.B) {
	flowConfigureBackend(b)
	for _, source := range flowBenchmarkFixtures(b) {
		b.Run(source.name, func(b *testing.B) {
			fixture := makeAssemblyFlowFixture(b, source.blk)
			for _, workers := range []int{2, 4} {
				b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
					for _, target := range []int{4, 8} {
						b.Run(fmt.Sprintf("target_%d", target), func(b *testing.B) {
							for _, arrival := range []struct {
								name string
								span time.Duration
							}{{"catchup", 0}, {"tip_200ms", 200 * time.Millisecond}} {
								b.Run(arrival.name, func(b *testing.B) {
									for _, overlap := range []bool{false, true} {
										name := "overlap_off"
										if overlap {
											name = "overlap_on"
										}
										b.Run(name, func(b *testing.B) {
											runAssemblyFlowBenchmark(b, source.blk, fixture, workers, target, arrival.span, overlap)
										})
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

type assemblyFlowFixture struct {
	components [][]*Shred
	blockID    solana.Hash
	parentID   solana.Hash
	bankhash   solana.Hash
	shreds     int
}

func makeAssemblyFlowFixture(tb testing.TB, source *block.Block) assemblyFlowFixture {
	tb.Helper()
	fixture := assemblyFlowFixture{parentID: solana.Hash{31}, bankhash: solana.Hash{47}}
	var seed [ed25519.SeedSize]byte
	seed[0] = 193 // Public deterministic benchmark leader, not validator material.
	leader := solana.PrivateKey(ed25519.NewKeyFromSeed(seed[:]))
	public := solana.PublicKeyFromBytes(ed25519.PrivateKey(leader).Public().(ed25519.PublicKey))
	generator := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7, ReferenceTick: 63}
	var nextData, nextCode uint32
	var chained solana.Hash
	var roots []solana.Hash
	appendComponent := func(component BlockComponent, last bool) {
		raw, err := MarshalBlockComponent(component)
		if err != nil {
			tb.Fatal(err)
		}
		generated, dataEnd, codeEnd, err := generator.makeShredsFromData(leader, raw, last, chained, nextData, nextCode)
		if err != nil {
			tb.Fatal(err)
		}
		chained, nextData, nextCode = generated.chainedMerkleRoot, dataEnd, codeEnd
		roots = append(roots, generated.fecSetRoots...)
		var data []*Shred
		for _, packet := range generated.packets {
			shred, err := ParseShred(packet)
			if err != nil {
				tb.Fatal(err)
			}
			if shred.Type != ShredTypeData {
				continue
			}
			if err := shred.VerifySignature(public); err != nil {
				tb.Fatal(err)
			}
			data = append(data, shred)
		}
		fixture.components = append(fixture.components, data)
		fixture.shreds += len(data)
	}
	appendComponent(NewBlockHeader(99, fixture.parentID), false)
	for i, txs := range flowComponents(source.Transactions, 60*1024) {
		entry := Entry{NumHashes: 1, Txns: make([]solana.Transaction, len(txs))}
		binary.LittleEndian.PutUint64(entry.Hash[:], uint64(i+1))
		for j, tx := range txs {
			entry.Txns[j] = *tx
		}
		appendComponent(BlockComponent{EntryBatch: []Entry{entry}}, false)
	}
	appendComponent(NewBlockFooter(BlockFooter{BankHash: fixture.bankhash}), true)
	if nextData > maxDataShredsPerSlot {
		tb.Fatalf("fixture requires %d data shreds; assembler limit is %d", nextData, maxDataShredsPerSlot)
	}
	fixture.blockID = DoubleMerkleBlockID(99, fixture.parentID, roots)
	return fixture
}

func runAssemblyFlowBenchmark(b *testing.B, source *block.Block, fixture assemblyFlowFixture, workers, target int, span time.Duration, overlap bool) {
	v := newTransactionVerifierWithBatchTarget(workers, 2*workers*target, target, nil)
	defer v.closeAndWait()
	if err := v.verifyBlock(source); err != nil {
		b.Fatal(err)
	}
	a := NewSlotAssembler()
	a.verifyTransactions = v.verifyBlockContext
	a.SetKnownAlpenglowBlockID(99, fixture.parentID)
	a.SetKnownAlpenglowBlockID(100, fixture.blockID)
	if overlap {
		prefetch := newEntryPrefetchPool(context.Background(), a, v)
		defer prefetch.closeAndWait()
	}
	var ready, parse, preparation, joins, arrivals []time.Duration
	var early uint64
	var cpu float64
	before := sigverify.Stats()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		a.ResetSlot(100)
		cpuStarted := flowCPUSeconds(b)
		b.StartTimer()
		started := time.Now()
		var work *slotCompletionWork
		for componentIndex, component := range fixture.components {
			if span > 0 {
				time.Sleep(time.Until(started.Add(flowArrivalOffset(componentIndex, len(fixture.components), span))))
			}
			for _, shred := range component {
				candidate, err := a.addShredFrom(shred, false)
				if err != nil {
					b.Fatal(err)
				}
				if candidate != nil {
					if work != nil {
						b.Fatal("slot claimed completion more than once")
					}
					work = candidate
				}
			}
		}
		if work == nil {
			b.Fatal("all generated data shreds did not complete the slot")
		}
		processed := a.processCompletion(context.Background(), work)
		completed, err := a.finalizeCompletion(work, processed)
		b.StopTimer()
		cpu += flowCPUSeconds(b) - cpuStarted
		if err != nil || completed == nil {
			b.Fatalf("completion failed: block=%v error=%v", completed != nil, err)
		}
		if !completed.TransactionSignaturesVerified() || len(completed.Transactions) != len(source.Transactions) ||
			!completed.HasAlpenglowBlockID || solana.Hash(completed.AlpenglowBlockID) != fixture.blockID ||
			!completed.HasExpectedBankhash || completed.ExpectedBankhash != fixture.bankhash {
			b.Fatal("completion changed transaction coverage or authenticated block metadata")
		}
		ready = append(ready, processed.timings.FullToReady)
		parse = append(parse, processed.timings.TransactionParse)
		preparation = append(preparation, processed.timings.EarlyPreparationWait)
		joins = append(joins, processed.timings.TransactionSigverify)
		arrivals = append(arrivals, processed.timings.ShredCollection)
		early += processed.timings.EarlyVerifiedTransactions
	}
	after := sigverify.Stats()
	b.ReportMetric(cpu*1000/float64(b.N), "cpu-ms/block")
	b.ReportMetric(cpu/b.Elapsed().Seconds(), "avg_cpu_cores")
	b.ReportMetric(float64(early)/float64(b.N), "early_verified_tx/block")
	b.ReportMetric(float64(len(source.Transactions)), "tx/block")
	b.ReportMetric(float64(fixture.shreds), "data_shreds/block")
	b.ReportMetric(float64(len(fixture.components)), "components/block")
	if batches := after.Batches - before.Batches; batches > 0 {
		b.ReportMetric(float64(after.Signatures-before.Signatures)/float64(batches), "mean_width")
	}
	flowReportPercentiles(b, ready, "full_to_ready")
	flowReportPercentiles(b, parse, "completion_parse")
	flowReportPercentiles(b, preparation, "preparation_wait")
	flowReportPercentiles(b, joins, "completion_sigverify")
	flowReportPercentiles(b, arrivals, "collection")
	if after.InternalFaultFallbacks != before.InternalFaultFallbacks {
		b.Fatal("signature verifier used an internal fault fallback")
	}
	var signatures uint64
	for _, tx := range source.Transactions {
		signatures += uint64(len(tx.Signatures))
	}
	if got, want := after.Signatures-before.Signatures, signatures*uint64(b.N); got != want {
		b.Fatalf("verified %d signatures; want %d, exactly once per retained transaction", got, want)
	}
}
