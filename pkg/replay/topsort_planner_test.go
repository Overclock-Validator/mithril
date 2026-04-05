package replay

import (
	"encoding/json"
	"slices"
	"sort"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/google/go-cmp/cmp"
)

func testSig(b byte) solana.Signature {
	var sig [64]byte
	for i := 0; i < 64; i++ {
		sig[i] = b
	}
	return sig
}

func testPk(b byte) solana.PublicKey {
	var pk [32]byte
	for i := 0; i < 32; i++ {
		pk[i] = b
	}
	return pk
}

func testTx(sigbyte byte) *solana.Transaction {
	return &solana.Transaction{
		Signatures: []solana.Signature{testSig(sigbyte)},
	}
}

func testTxs(n int) []*solana.Transaction {
	out := make([]*solana.Transaction, n)
	for i := range out {
		out[i] = testTx(byte(i))
	}
	return out
}

func testTxMeta(readAcctBytes []byte, writeAcctBytes []byte) *rpc.TransactionMeta {
	tm := &rpc.TransactionMeta{}
	for _, r := range readAcctBytes {
		tm.LoadedAddresses.ReadOnly = append(tm.LoadedAddresses.ReadOnly, testPk(r))
	}
	for _, w := range writeAcctBytes {
		tm.LoadedAddresses.Writable = append(tm.LoadedAddresses.Writable, testPk(w))
	}
	return tm
}

// Graph test cases, many taken from https://github.com/apfitzge/prio-graph
type graphTestCase struct {
	name            string
	b               *block.Block
	sortedTxIndices [][]int
}

var tests = []graphTestCase{
	{
		"ReadAfterWriteSequential",
		&block.Block{
			Transactions: testTxs(2),
			TxMetas: []*rpc.TransactionMeta{
				testTxMeta(nil, []byte{0}),
				testTxMeta([]byte{0}, nil),
			},
		},
		[][]int{{0}, {1}},
	},
	{
		"WriteAfterReadSequential",
		&block.Block{
			Transactions: testTxs(2),
			TxMetas: []*rpc.TransactionMeta{
				testTxMeta([]byte{0}, nil),
				testTxMeta(nil, []byte{0}),
			},
		},
		[][]int{{0}, {1}},
	},
	{
		"ReadonlyExecuteAllParallel",
		&block.Block{
			Transactions: testTxs(3),
			TxMetas: []*rpc.TransactionMeta{
				testTxMeta([]byte{0}, nil),
				testTxMeta([]byte{0}, nil),
				testTxMeta([]byte{0}, nil),
			},
		},
		[][]int{{0, 1, 2}},
	},
	{
		"ChainedTxsExecuteSequentially",
		&block.Block{
			Transactions: testTxs(3),
			TxMetas: []*rpc.TransactionMeta{
				testTxMeta(nil, []byte{0}),
				testTxMeta(nil, []byte{0}),
				testTxMeta(nil, []byte{0}),
			},
		},
		[][]int{{0}, {1}, {2}},
	},
	{
		"DisjointWritesExecuteAllParallel",
		&block.Block{
			Transactions: testTxs(3),
			TxMetas: []*rpc.TransactionMeta{
				testTxMeta(nil, []byte{0}),
				testTxMeta(nil, []byte{1}),
				testTxMeta(nil, []byte{2}),
			},
		},
		[][]int{{0, 1, 2}},
	},
	{
		"MultipleChains",
		&block.Block{
			Transactions: testTxs(8),
			TxMetas: []*rpc.TransactionMeta{
				testTxMeta(nil, []byte{0}),
				testTxMeta(nil, []byte{0}),
				testTxMeta(nil, []byte{1}),
				testTxMeta(nil, []byte{0}),
				testTxMeta(nil, []byte{1}),
				testTxMeta(nil, []byte{2}),
				testTxMeta(nil, []byte{1}),
				testTxMeta(nil, []byte{0}),
			},
		},
		[][]int{{0, 2, 5}, {1, 4}, {3, 6}, {7}},
	},
	{
		"Join",
		&block.Block{
			Transactions: testTxs(6),
			TxMetas: []*rpc.TransactionMeta{
				testTxMeta(nil, []byte{0}),
				testTxMeta(nil, []byte{1}),
				testTxMeta(nil, []byte{1}),
				testTxMeta(nil, []byte{0}),
				testTxMeta(nil, []byte{0, 1}),
				testTxMeta(nil, []byte{0, 1}),
			},
		},
		[][]int{{0, 1}, {3, 2}, {4}, {5}},
	},
	{
		"Fork",
		&block.Block{
			Transactions: testTxs(6),
			TxMetas: []*rpc.TransactionMeta{
				testTxMeta(nil, []byte{0, 1}),
				testTxMeta(nil, []byte{0, 1}),
				testTxMeta(nil, []byte{1}),
				testTxMeta(nil, []byte{1}),
				testTxMeta(nil, []byte{0}),
				testTxMeta(nil, []byte{0}),
			},
		},
		[][]int{{0}, {1}, {2, 4}, {3, 5}},
	},
	{
		"ForkAndJoin",
		&block.Block{
			Transactions: testTxs(9),
			TxMetas: []*rpc.TransactionMeta{
				testTxMeta(nil, []byte{0, 1}),
				testTxMeta(nil, []byte{0, 1}),
				testTxMeta(nil, []byte{1}),
				testTxMeta(nil, []byte{1}),
				testTxMeta(nil, []byte{0}),
				testTxMeta(nil, []byte{0, 1}),
				testTxMeta(nil, []byte{1}),
				testTxMeta(nil, []byte{0}),
				testTxMeta(nil, []byte{0}),
			},
		},
		[][]int{{0}, {1}, {2, 4}, {3}, {5}, {6, 7}, {8}},
	},
}

func runStream(b *block.Block) []int {
	do := make(chan int, len(b.Transactions))
	done := make(chan int, len(b.Transactions))
	go TopsortPlannerStream(b, do, done)
	var sort []int
	for len(sort) < len(b.Transactions) {
		task := <-do
		sort = append(sort, task)
		done <- task
	}
	return sort
}

func flatten(x [][]int) []int {
	var out []int
	for _, y := range x {
		out = append(out, y...)
	}
	return out
}

func TestTopsort(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("Batch", func(t *testing.T) {
				got := TopsortPlanner(test.b)
				if diff := cmp.Diff(test.sortedTxIndices, got); diff != "" {
					t.Errorf("-want +got:\n%s", diff)
				}
			})
			t.Run("Streaming", func(t *testing.T) {
				got := runStream(test.b)
				if diff := cmp.Diff(flatten(test.sortedTxIndices), got); diff != "" {
					t.Errorf("-want +got:\n%s", diff)
				}
			})
		})
	}
}

func TestTopsortResolvedTxsIgnoreTxMetaLoadedAddresses(t *testing.T) {
	sharedReadonly := testPk(42)
	addressTable := testPk(99)

	mkResolvedLookupTx := func(sigbyte byte) *solana.Transaction {
		tx := testTx(sigbyte)
		tx.Message.SetVersion(solana.MessageVersionV0)
		tx.Message.AccountKeys = []solana.PublicKey{testPk(sigbyte + 100)}
		tx.Message.Header.NumRequiredSignatures = 1
		tx.Message.SetAddressTableLookups([]solana.MessageAddressTableLookup{{
			AccountKey:      addressTable,
			ReadonlyIndexes: []uint8{0},
		}})
		if err := tx.Message.SetAddressTables(map[solana.PublicKey]solana.PublicKeySlice{
			addressTable: {sharedReadonly},
		}); err != nil {
			t.Fatalf("SetAddressTables: %v", err)
		}
		if err := tx.Message.ResolveLookups(); err != nil {
			t.Fatalf("ResolveLookups: %v", err)
		}
		return tx
	}

	b := &block.Block{
		Transactions: []*solana.Transaction{
			mkResolvedLookupTx(1),
			mkResolvedLookupTx(2),
		},
		TxMetas: []*rpc.TransactionMeta{
			testTxMeta([]byte{42}, nil),
			testTxMeta([]byte{42}, nil),
		},
	}

	got := TopsortPlanner(b)
	want := [][]int{{0, 1}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("-want +got:\n%s", diff)
	}
}

func mustMarshal(b *block.Block) []byte {
	bBytes, err := json.Marshal(b)
	if err != nil {
		panic(err)
	}
	return bBytes
}

func unwrap(txs []tx) []int {
	i := make([]int, len(txs))
	for ti, tx := range txs {
		i[ti] = int(tx)
	}
	return i
}

func FuzzBlockToDependencyGraph(f *testing.F) {
	for _, test := range tests {
		f.Add(mustMarshal(test.b))
	}

	f.Fuzz(func(t *testing.T, blockBytes []byte) {
		b := &block.Block{}
		err := json.Unmarshal(blockBytes, b)
		if err != nil {
			t.Skip("skipping unmarshalable block")
		}
		if len(b.Transactions) != len(b.TxMetas) {
			t.Skip("skipping malformed block, not all txs have txmetas")
		}
		for _, tx := range b.Transactions {
			if tx.Message.IsResolved() {
				t.Skip("skipping resolved tx")
			}
		}

		adjList, inDegrees := blockToDependencyGraph(b)
		if len(adjList) != len(b.Transactions) {
			t.Errorf("len(adjList)=%d != len(b.Transactions)=%d", len(adjList), len(b.Transactions))
		}
		if len(adjList) != len(inDegrees) {
			t.Errorf("len(adjList)=%d != len(inDegrees)=%d", len(adjList), len(inDegrees))
		}
		for node, inDegree := range inDegrees {
			if inDegree < 0 {
				t.Errorf("node=%d had negative inDegree=%d", node, inDegree)
			}
		}

		for u, vs := range adjList {
			for _, v := range vs {
				if int(v) < u {
					t.Errorf("a tx v=%d later in the block depended on a tx u=%d earlier in the block", v, u)
				}
			}
			vs0 := unwrap(vs)
			if !sort.IntsAreSorted(vs0) {
				t.Errorf("neighbors list wasn't sorted")
			}
			uncompactedVs := make([]int, len(vs0))
			copy(uncompactedVs, vs0)
			if len(uncompactedVs) != len(slices.Compact(vs0)) {
				t.Errorf("node=%d neighbors list=%+v had duplicates %v", u, uncompactedVs, slices.Compact(vs0))
			}
		}
	})
}
