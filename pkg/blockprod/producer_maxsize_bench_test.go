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

// Generate exactly 1,232-byte, single-signature legacy transactions. Padding is
// a real UTF-8 memo instruction, rather than trailing bytes after a transaction.
func maxSizeTransferPool(tb testing.TB) ([][]byte, []solana.Transaction) {
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
		if len(raw) != costmodel.PacketDataSize {
			tb.Fatalf("wire size %d, expected %d", len(raw), costmodel.PacketDataSize)
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

func TestMaxSizeProducerFixture(t *testing.T) {
	wires, txns := maxSizeTransferPool(t)
	t.Logf("verified %d signed transactions at %d bytes each", len(wires), len(wires[0]))
	builder := NewEntryBuilder(costmodel.DefaultLimits(), solana.Hash{})
	for i := 0; i < 50; i++ {
		entries, batchBytes, flushed := builder.Append(txns[i], len(wires[i]))
		if i < 49 && flushed {
			t.Fatalf("premature flush at %d", i)
		}
		if i == 49 {
			if !flushed || len(entries) != 1 || len(entries[0].Txns) != 49 || batchBytes != 60424 {
				t.Fatal("unexpected full-batch size")
			}
		}
	}
}

// 50k maximum-size transactions require two slots under the current 32,768
// data-shred cap. Each iteration completes two slots of 25k transactions.
func BenchmarkProducer50kMaxSize(b *testing.B) {
	const transactions = 50000
	const transactionsPerSlot = 25000
	wires, txns := maxSizeTransferPool(b)
	leader := txfixture.PayerPrivateKey()
	var lastBatches, lastPackets, lastMaxSlotDataShreds int
	b.ReportAllocs()
	b.ResetTimer()
	for round := 0; round < b.N; round++ {
		var parentID, parentRoot solana.Hash
		totalBatches, totalPackets, maxSlotDataShreds := 0, 0, 0
		for block := 0; block < transactions/transactionsPerSlot; block++ {
			slot := uint64(100 + block)
			builder := NewEntryBuilder(costmodel.DefaultLimits(), solana.Hash{})
			sink := &benchmarkPacketBroadcaster{}
			session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
				Leader: leader, Slot: slot, ParentSlot: slot - 1, Version: 1, Broadcaster: sink,
				ParentBlockID: parentID, ParentChainedMerkleRoot: parentRoot,
			})
			if err := session.BroadcastHeader(parentID); err != nil {
				b.Fatal(err)
			}
			for i := 0; i < transactionsPerSlot; i++ {
				idx := (block*transactionsPerSlot + i) % len(txns)
				entries, _, flushed := builder.Append(txns[idx], len(wires[idx]))
				if flushed {
					if err := session.BroadcastEntryBatch(entries); err != nil {
						b.Fatal(err)
					}
					totalBatches++
				}
			}
			if entries, _ := builder.Flush(); len(entries) > 0 {
				if err := session.BroadcastEntryBatch(entries); err != nil {
					b.Fatal(err)
				}
				totalBatches++
			}
			if err := session.BroadcastFooter(solana.Hash{1}, 0, nil, nil); err != nil {
				b.Fatal(err)
			}
			if err := session.BroadcastEndingTickLast(builder.CurrentEntryHash()); err != nil {
				b.Fatal(err)
			}
			parentID = session.BlockID(slot-1, parentID)
			parentRoot = session.ChainedMerkleRoot()
			// The generator emits equal numbers of data and coding shreds.
			dataShreds := sink.packets / 2
			if dataShreds > costmodel.DefaultMaxDataShredsPerSlot {
				b.Fatal("slot exceeds data-shred limit")
			}
			if dataShreds > maxSlotDataShreds {
				maxSlotDataShreds = dataShreds
			}
			totalPackets += sink.packets
		}
		lastBatches, lastPackets, lastMaxSlotDataShreds = totalBatches, totalPackets, maxSlotDataShreds
	}
	b.StopTimer()
	b.ReportMetric(transactions, "transactions/op")
	b.ReportMetric(transactions/transactionsPerSlot, "slots/op")
	b.ReportMetric(float64(len(wires[0])), "wire-B/tx")
	b.ReportMetric(float64(lastBatches), "entry-batches/op")
	b.ReportMetric(float64(lastPackets), "packets/op")
	b.ReportMetric(float64(lastMaxSlotDataShreds), "max-data-shreds/slot")
}
