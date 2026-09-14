package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/gagliardetto/solana-go"
)

var checkpointBenchmarkPayload []byte
var checkpointBenchmarkCapture TransactionStatusSnapshot

func BenchmarkTransactionStatusCheckpointCapture(b *testing.B) {
	// A private, not-yet-published fixture with the same complete 300-root
	// metadata as an imported cache. 1.5 million keys encode to roughly 30 MB.
	c := newTransactionStatusCache(true)
	c.coverageFromGenesis = false
	c.rootedSinceSeed = maxTransactionStatusRoots
	c.rootedThrough = maxTransactionStatusRoots
	for slot := uint64(1); slot <= maxTransactionStatusRoots; slot++ {
		keys := make(map[transactionStatusKey]struct{}, 5000)
		var seed [16]byte
		binary.LittleEndian.PutUint64(seed[:8], slot)
		for i := uint64(0); i < 5000; i++ {
			binary.LittleEndian.PutUint64(seed[8:], i)
			hash := sha256.Sum256(seed[:])
			var key transactionStatusKey
			copy(key[:], hash[:])
			keys[key] = struct{}{}
		}
		c.tip = &transactionStatusNode{slot: slot, parent: c.tip,
			delta: transactionStatusDelta{solana.Hash{1}: {keyIndex: 7, keys: keys}}}
	}
	view, err := c.CaptureSnapshotThrough(maxTransactionStatusRoots)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("SynchronousBaseline", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			checkpointBenchmarkPayload, err = legacyStatusSnapshotForTest(c, maxTransactionStatusRoots)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("CaptureOnReplay", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			checkpointBenchmarkCapture, err = c.CaptureSnapshotThrough(maxTransactionStatusRoots)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("EncodeOnWorker", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			checkpointBenchmarkPayload, err = view.MarshalBinary()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
