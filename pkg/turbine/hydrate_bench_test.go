package turbine

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"testing"

	"github.com/gagliardetto/solana-go"
)

// hydrateFixturePackets returns a generated Merkle slot (header, entry
// batches of real-sized transactions, footer) as raw packets, in the order a
// receiver spools them: each FEC set's data shreds followed by its coding
// shreds.
func hydrateFixturePackets(tb testing.TB, slot uint64, txCount int) [][]byte {
	tb.Helper()
	source := flowBenchmarkFixtures(tb)[0].blk
	txs := source.Transactions
	if len(txs) > txCount {
		txs = txs[:txCount]
	}
	var seed [ed25519.SeedSize]byte
	seed[0] = 211
	leader := solana.PrivateKey(ed25519.NewKeyFromSeed(seed[:]))
	gen := ShredGenerator{Slot: slot, ParentSlot: slot - 1, Version: 7, ReferenceTick: 63}
	var nextData, nextCode uint32
	var chained solana.Hash
	var out [][]byte
	add := func(component BlockComponent, last bool) {
		raw, err := MarshalBlockComponent(component)
		if err != nil {
			tb.Fatal(err)
		}
		packets, root, dataEnd, codeEnd, err := gen.MakeShredsFromData(leader, raw, last, chained, nextData, nextCode)
		if err != nil {
			tb.Fatal(err)
		}
		chained, nextData, nextCode = root, dataEnd, codeEnd
		out = append(out, packets...)
	}
	add(NewBlockHeader(slot-1, solana.Hash{31}), false)
	for i, batch := range flowComponents(txs, 60*1024) {
		entry := Entry{NumHashes: 1, Txns: make([]solana.Transaction, len(batch))}
		binary.LittleEndian.PutUint64(entry.Hash[:], uint64(i+1))
		for j, tx := range batch {
			entry.Txns[j] = *tx
		}
		add(BlockComponent{EntryBatch: []Entry{entry}}, false)
	}
	add(NewBlockFooter(BlockFooter{BankHash: solana.Hash{47}}), true)
	return out
}

func newHydrateBenchReceiver(tb testing.TB, slot uint64, packets [][]byte) *UDPReceiver {
	tb.Helper()
	spool, err := OpenShredSpool(tb.TempDir(), 0)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(spool.Close)
	for _, p := range packets {
		spool.Append(slot, p)
	}
	r := NewUDPReceiver("127.0.0.1:0")
	r.SetShredSpool(spool)
	return r
}

// BenchmarkHydrateSlot measures loading one spooled slot: spool read, shred
// parsing, assembly, completion and entry decoding. The assembler slot is
// reset outside the timer between samples.
func BenchmarkHydrateSlot(b *testing.B) {
	const slot = 500
	packets := hydrateFixturePackets(b, slot, 1500)
	r := newHydrateBenchReceiver(b, slot, packets)
	var data int
	for _, p := range packets {
		if s, err := ParseShred(p); err == nil && s.Type == ShredTypeData {
			data++
		}
	}
	b.ReportMetric(float64(data), "data-shreds")
	b.ReportMetric(float64(len(packets)-data), "coding-shreds")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r.assembler.ResetSlot(slot)
		b.StartTimer()
		work, ok := r.hydrateSlot(slot)
		if !ok || work == nil {
			b.Fatal("slot did not complete from the spool")
		}
		processed := r.assembler.processCompletion(context.Background(), work)
		blk, err := r.assembler.finalizeCompletion(work, processed)
		if err != nil || blk == nil {
			b.Fatalf("finalize: %v", err)
		}
	}
}
