package blockprod

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	txwire "github.com/Overclock-Validator/mithril/pkg/tpu/wire"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
)

// These complete producer CPU workloads compare branch heads using identical
// pre-signed, parsed fixtures and a packet-counting sink. No execution, admission,
// worker queue, routing, UDP, or receiver work is timed.
const branchComparisonOneFECBytes = 32 * 963
const branchComparisonSlotEntryCap = 20*1024*1024 - 48

func branchMaxSizeTransferPool(tb testing.TB) ([][]byte, []solana.Transaction) {
	tb.Helper()
	payer, destination := txfixture.PayerPubkey(), txfixture.DestPubkey()
	privateKey := txfixture.PayerPrivateKey()
	wires := make([][]byte, 512)
	txns := make([]solana.Transaction, len(wires))
	for i := range wires {
		memo := bytes.Repeat([]byte("m"), 981)
		copy(memo, fmt.Sprintf("mithril-max-wire-%04d:", i))
		tx, err := solana.NewTransaction([]solana.Instruction{
			system.NewTransferInstruction(uint64(i+1), payer, destination).Build(),
			solana.NewInstruction(solana.MemoProgramID, nil, memo),
		}, txfixture.TestBlockhash(), solana.TransactionPayer(payer))
		if err != nil {
			tb.Fatal(err)
		}
		_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
			if key == payer {
				return &privateKey
			}
			return nil
		})
		if err != nil {
			tb.Fatal(err)
		}
		raw, err := tx.MarshalBinary()
		if err != nil {
			tb.Fatal(err)
		}
		if len(raw) != txwire.PacketDataSize {
			tb.Fatalf("wire size %d, expected %d", len(raw), txwire.PacketDataSize)
		}
		if _, err := txwire.Sanitize(raw); err != nil {
			tb.Fatal(err)
		}
		decoded, err := solana.TransactionFromBytes(raw)
		if err != nil {
			tb.Fatal(err)
		}
		if err := decoded.VerifySignatures(); err != nil {
			tb.Fatal(err)
		}
		encoded, err := decoded.MarshalBinary()
		if err != nil || !bytes.Equal(encoded, raw) {
			tb.Fatal("canonical transaction round trip changed bytes")
		}
		wires[i], txns[i] = raw, *decoded
	}
	return wires, txns
}

func branchComparisonFixtures(tb testing.TB, maximum bool) ([][]byte, []solana.Transaction) {
	tb.Helper()
	if maximum {
		return branchMaxSizeTransferPool(tb)
	}
	wires := txfixture.PrecomputeTransferPool(512)
	txns := make([]solana.Transaction, len(wires))
	for i, raw := range wires {
		if len(raw) != 215 {
			tb.Fatalf("transfer size %d, want 215", len(raw))
		}
		if _, err := txwire.Sanitize(raw); err != nil {
			tb.Fatal(err)
		}
		tx, err := solana.TransactionFromBytes(raw)
		if err != nil {
			tb.Fatal(err)
		}
		if err := tx.VerifySignatures(); err != nil {
			tb.Fatal(err)
		}
		encoded, err := tx.MarshalBinary()
		if err != nil || !bytes.Equal(encoded, raw) {
			tb.Fatal("transfer round trip changed bytes")
		}
		txns[i] = *tx
	}
	return wires, txns
}

type branchComparisonSink struct{ packets int }

func (s *branchComparisonSink) Broadcast(packets [][]byte) error {
	s.packets += len(packets)
	return nil
}

type branchComparisonStats struct {
	batches, packets, transactions, maxDataShreds, maxEntryBytes int
	blockID                                                      solana.Hash
}

func branchComparisonRun(tb testing.TB, wires [][]byte, txns []solana.Transaction, slots []int, limits costmodel.Limits) branchComparisonStats {
	tb.Helper()
	var stats branchComparisonStats
	var parentID, parentRoot solana.Hash
	leader := txfixture.PayerPrivateKey()
	for number, count := range slots {
		slot := uint64(100 + number)
		builder := NewEntryBuilder(limits, solana.Hash{})
		sink := &branchComparisonSink{}
		session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
			Leader: leader, Slot: slot, ParentSlot: slot - 1, Version: 1, Broadcaster: sink,
			ParentBlockID: parentID, ParentChainedMerkleRoot: parentRoot,
		})
		if err := session.BroadcastHeader(parentID); err != nil {
			tb.Fatal(err)
		}
		entryBytes := 0
		for i := 0; i < count; i++ {
			index := stats.transactions % len(txns)
			entries, size, flushed := builder.Append(txns[index], len(wires[index]))
			stats.transactions++
			if flushed {
				if err := session.BroadcastEntryBatch(entries); err != nil {
					tb.Fatal(err)
				}
				stats.batches++
				entryBytes += size
			}
		}
		if entries, size := builder.Flush(); len(entries) != 0 {
			if err := session.BroadcastEntryBatch(entries); err != nil {
				tb.Fatal(err)
			}
			stats.batches++
			entryBytes += size
		}
		if err := session.BroadcastFooter(solana.Hash{1}, 0, nil, nil); err != nil {
			tb.Fatal(err)
		}
		if err := session.BroadcastEndingTickLast(builder.CurrentEntryHash()); err != nil {
			tb.Fatal(err)
		}
		parentID = session.BlockID(slot-1, parentID)
		parentRoot = session.ChainedMerkleRoot()
		dataShreds := sink.packets / 2
		if dataShreds > costmodel.DefaultMaxDataShredsPerSlot {
			tb.Fatal("slot exceeds data-shred budget")
		}
		if entryBytes > branchComparisonSlotEntryCap {
			tb.Fatal("slot exceeds entry-byte budget")
		}
		if dataShreds > stats.maxDataShreds {
			stats.maxDataShreds = dataShreds
		}
		if entryBytes > stats.maxEntryBytes {
			stats.maxEntryBytes = entryBytes
		}
		stats.packets += sink.packets
	}
	stats.blockID = parentID
	return stats
}

func branchComparisonCheck(tb testing.TB, got branchComparisonStats, wireSize int, slots []int, batchLimit uint64) {
	tb.Helper()
	perBatch := (int(batchLimit) - 56) / wireSize
	var batches, packets, transactions int
	for _, count := range slots {
		full, tail := count/perBatch, count%perBatch
		batches += full
		fecs := full * ((56 + perBatch*wireSize + branchComparisonOneFECBytes - 1) / branchComparisonOneFECBytes)
		if tail != 0 {
			batches++
			fecs += (56 + tail*wireSize + branchComparisonOneFECBytes - 1) / branchComparisonOneFECBytes
		}
		packets += (fecs + 3) * 64 // Header, footer, and signed ending tick each use one FEC set.
		transactions += count
	}
	if got.batches != batches || got.packets != packets || got.transactions != transactions {
		tb.Fatalf("workload counts %+v, want batches=%d packets=%d transactions=%d", got, batches, packets, transactions)
	}
	if got.blockID == (solana.Hash{}) {
		tb.Fatal("empty block commitment")
	}
}

func TestProducerBranchComparisonFixtures(t *testing.T) {
	for _, maximum := range []bool{false, true} {
		wires, txns := branchComparisonFixtures(t, maximum)
		limits := costmodel.DefaultLimits()
		slots := []int{101, 100}
		got := branchComparisonRun(t, wires, txns, slots, limits)
		branchComparisonCheck(t, got, len(wires[0]), slots, limits.MaxBatchBytes)
		t.Logf("wire=%d batch-target=%d batches=%d packets=%d", len(wires[0]), limits.MaxBatchBytes, got.batches, got.packets)
	}
}

func BenchmarkProducerBranchHeads(b *testing.B) {
	for _, workload := range []struct {
		name    string
		maximum bool
		slots   []int
	}{
		{name: "small-215B", slots: []int{50000}},
		{name: "maximum-1232B", maximum: true, slots: []int{16667, 16667, 16666}},
	} {
		wires, txns := branchComparisonFixtures(b, workload.maximum)
		for _, target := range []struct {
			name  string
			bytes uint64
		}{
			{name: "default"},
			{name: "one-fec", bytes: branchComparisonOneFECBytes},
		} {
			b.Run(workload.name+"/"+target.name, func(b *testing.B) {
				limits := costmodel.DefaultLimits()
				if target.bytes != 0 {
					limits.MaxBatchBytes = target.bytes
				}
				var last branchComparisonStats
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					last = branchComparisonRun(b, wires, txns, workload.slots, limits)
				}
				b.StopTimer()
				branchComparisonCheck(b, last, len(wires[0]), workload.slots, limits.MaxBatchBytes)
				b.ReportMetric(float64(last.transactions), "transactions/op")
				b.ReportMetric(float64(len(workload.slots)), "slots/op")
				b.ReportMetric(float64(len(wires[0])), "wire-B/tx")
				b.ReportMetric(float64(limits.MaxBatchBytes), "batch-target-B")
				b.ReportMetric(float64(last.batches), "entry-batches/op")
				b.ReportMetric(float64(last.packets), "packets/op")
				b.ReportMetric(float64(last.maxDataShreds), "max-data-shreds/slot")
				b.ReportMetric(float64(last.maxEntryBytes), "max-entry-B/slot")
			})
		}
	}
}
