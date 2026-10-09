package turbine

import (
	"fmt"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestEntryBatchTransactionsPreservesEntryOwnershipAndOrder(t *testing.T) {
	entries := []Entry{{}, {Txns: make([]solana.Transaction, 3)}, {}, {Txns: make([]solana.Transaction, 2)}}
	txs := entryBatchTransactions(entries)
	require.Len(t, txs, 5)
	for i := 0; i < 3; i++ {
		require.Same(t, &entries[1].Txns[i], txs[i])
	}
	for i := 0; i < 2; i++ {
		require.Same(t, &entries[3].Txns[i], txs[3+i])
	}
	require.Empty(t, entryBatchTransactions(nil))
	require.Empty(t, entryBatchTransactions([]Entry{{}, {}}))
}

var entryBatchBenchmarkSink []*solana.Transaction

func BenchmarkEntryBatchTransactions(b *testing.B) {
	for _, count := range []int{4, 49, 269, 33760} {
		b.Run(fmt.Sprintf("tx_%d", count), func(b *testing.B) {
			entries := []Entry{{Txns: make([]solana.Transaction, count/2)}, {}, {Txns: make([]solana.Transaction, count-count/2)}}
			b.ReportAllocs()
			for b.Loop() {
				entryBatchBenchmarkSink = entryBatchTransactions(entries)
			}
		})
	}
}
