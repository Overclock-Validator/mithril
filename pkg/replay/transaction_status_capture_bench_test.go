package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/gagliardetto/solana-go"
)

var checkpointBenchmarkPayload []byte
var checkpointBenchmarkCapture TransactionStatusSnapshot

func checkpointEncodingFixture() *TransactionStatusCache {
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
	return c
}

func BenchmarkTransactionStatusCheckpointCapture(b *testing.B) {
	c := checkpointEncodingFixture()
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

// Moving 300-root windows at several checkpoint cadences. Fixtures and initial
// cache warming are excluded; allocating new node headers and encoding/output
// allocation are included. This measures encoding only, not fsync or account
// checkpoint work. Cold encodes represent first startup/all-new windows.
func BenchmarkTransactionStatusCheckpointEncoding(b *testing.B) {
	c := checkpointEncodingFixture()
	view, err := c.CaptureSnapshotThrough(300)
	if err != nil {
		b.Fatal(err)
	}
	seed := view.(*transactionStatusSnapshot).nodes
	for _, advance := range []int{0, 1, 8, 32, defaultFoldBatchSlots, 300} {
		for _, cached := range []bool{false, true} {
			b.Run(fmt.Sprintf("new=%d/cached=%t", advance, cached), func(b *testing.B) {
				nodes := append([]*transactionStatusNode(nil), seed...)
				if cached {
					_, _ = marshalTransactionStatusNodes(nodes, 300, true, false)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					copy(nodes, nodes[advance:])
					for i := 300 - advance; i < 300; i++ {
						nodes[i] = &transactionStatusNode{slot: uint64(301 + n*advance + i), delta: seed[i].delta}
					}
					var err error
					if cached {
						checkpointBenchmarkPayload, err = marshalTransactionStatusNodes(nodes, 300, true, false)
					} else {
						checkpointBenchmarkPayload, err = marshalTransactionStatusNodesUncached(nodes, 300, true, false)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				b.SetBytes(int64(len(checkpointBenchmarkPayload)))
			})
		}
	}
}
