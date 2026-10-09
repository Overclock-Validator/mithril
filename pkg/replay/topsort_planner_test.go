package replay

import (
	"encoding/binary"
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

func plannerAccountsConflict(a, b plannerTransactionAccounts) bool {
	bAccounts := make(map[solana.PublicKey]struct{}, len(b.accounts))
	for _, account := range b.accounts {
		bAccounts[account.publicKey] = struct{}{}
	}
	for _, account := range a.writable() {
		if _, exists := bAccounts[account.publicKey]; exists {
			return true
		}
	}

	aAccounts := make(map[solana.PublicKey]struct{}, len(a.accounts))
	for _, account := range a.accounts {
		aAccounts[account.publicKey] = struct{}{}
	}
	for _, account := range b.writable() {
		if _, exists := aAccounts[account.publicKey]; exists {
			return true
		}
	}
	return false
}

func plannerAccountModes(accounts plannerTransactionAccounts) map[solana.PublicKey]bool {
	modes := make(map[solana.PublicKey]bool, len(accounts.accounts))
	for _, account := range accounts.readonly() {
		modes[account.publicKey] = false
	}
	for _, account := range accounts.writable() {
		modes[account.publicKey] = true
	}
	return modes
}

func dependencyPlanReachableFrom(plan *dependencyPlan, from int) []bool {
	visited := make([]bool, len(plan.inDegree))
	stack := []uint32{uint32(from)}
	visited[from] = true
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		start, end := plan.offsets[current], plan.offsets[current+1]
		for _, dependent := range plan.dependents[start:end] {
			if !visited[dependent] {
				visited[dependent] = true
				stack = append(stack, dependent)
			}
		}
	}
	return visited
}

func assertConflictOrderReachable(t *testing.T, accounts []plannerTransactionAccounts, plan *dependencyPlan) {
	t.Helper()
	for earlier := range accounts {
		reachable := dependencyPlanReachableFrom(plan, earlier)
		for later := earlier + 1; later < len(accounts); later++ {
			if !plannerAccountsConflict(accounts[earlier], accounts[later]) {
				continue
			}
			if !reachable[later] {
				t.Fatalf("conflicting transaction %d does not reach later transaction %d", earlier, later)
			}
		}
	}
}

func TestDependencyPlanPreservesConflictOrder(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accounts, available := plannerAccountsForBlock(test.b)
			if !available {
				t.Fatal("dependency planner unavailable")
			}
			assertConflictOrderReachable(t, accounts, buildDependencyPlan(accounts))
		})
	}
}

func TestDependencyPlanPrunesTransitiveWriterEdges(t *testing.T) {
	const transactionCount = 1_000
	sharedAccount := testPk(0x7f)
	transactions := make([]plannerTransactionAccounts, transactionCount)
	for idx := range transactions {
		transactions[idx] = newPlannerTransactionAccounts(1, func(add func(solana.PublicKey, bool)) {
			add(sharedAccount, true)
		})
	}

	plan := buildDependencyPlan(transactions)
	if got, want := len(plan.dependents), transactionCount-1; got != want {
		t.Fatalf("dependency edge count = %d, want chain of %d", got, want)
	}
	for idx := 0; idx < transactionCount-1; idx++ {
		start, end := plan.offsets[idx], plan.offsets[idx+1]
		if end-start != 1 || plan.dependents[start] != uint32(idx+1) {
			t.Fatalf("transaction %d dependents = %v, want [%d]", idx, plan.dependents[start:end], idx+1)
		}
	}
}

func TestPlannerAccountDeduplicationPromotesWritable(t *testing.T) {
	account := testPk(0x5a)
	accounts := newPlannerTransactionAccounts(2, func(add func(solana.PublicKey, bool)) {
		add(account, false)
		add(account, true)
	})
	if len(accounts.readonly()) != 0 {
		t.Fatalf("readonly accounts = %v, want none", accounts.readonly())
	}
	writable := make([]solana.PublicKey, len(accounts.writable()))
	for idx, access := range accounts.writable() {
		writable[idx] = access.publicKey
	}
	if diff := cmp.Diff([]solana.PublicKey{account}, writable); diff != "" {
		t.Fatalf("writable accounts mismatch (-want +got):\n%s", diff)
	}
}

func TestPlannerResolvedMessageAndMetadataAgree(t *testing.T) {
	payer := testPk(0x61)
	table := testPk(0x62)
	loadedWritable := testPk(0x63)
	loadedReadonly := testPk(0x64)
	msg := solana.Message{
		Header: solana.MessageHeader{
			NumRequiredSignatures: 1,
		},
		AccountKeys: []solana.PublicKey{payer},
		AddressTableLookups: solana.MessageAddressTableLookupSlice{
			{
				AccountKey:      table,
				WritableIndexes: []uint8{0},
				ReadonlyIndexes: []uint8{1},
			},
		},
	}
	msg.SetVersion(solana.MessageVersionV0)
	transaction := &solana.Transaction{Message: msg}
	txMeta := &rpc.TransactionMeta{
		LoadedAddresses: rpc.LoadedAddresses{
			Writable: []solana.PublicKey{loadedWritable},
			ReadOnly: []solana.PublicKey{loadedReadonly},
		},
	}
	beforeResolution := plannerAccountsFromMeta(transaction, txMeta)

	if err := transaction.Message.SetAddressTables(map[solana.PublicKey]solana.PublicKeySlice{
		table: {loadedWritable, loadedReadonly},
	}); err != nil {
		t.Fatalf("set address tables: %v", err)
	}
	if err := transaction.Message.ResolveLookups(); err != nil {
		t.Fatalf("resolve lookups: %v", err)
	}

	afterResolutionFromMeta := plannerAccountsFromMeta(transaction, txMeta)
	afterResolutionFromMessage := plannerAccountsFromMessage(&transaction.Message)
	if diff := cmp.Diff(plannerAccountModes(beforeResolution), plannerAccountModes(afterResolutionFromMeta)); diff != "" {
		t.Fatalf("metadata access changed across resolution (-before +after):\n%s", diff)
	}
	if diff := cmp.Diff(plannerAccountModes(beforeResolution), plannerAccountModes(afterResolutionFromMessage)); diff != "" {
		t.Fatalf("resolved-message access differs from metadata (-meta +message):\n%s", diff)
	}

	resolvedBlock := &block.Block{
		Transactions: []*solana.Transaction{transaction},
		TxMetas:      []*rpc.TransactionMeta{{}},
	}
	blockAccounts, available := plannerAccountsForBlock(resolvedBlock)
	if !available {
		t.Fatal("planner unavailable for resolved message with incomplete metadata")
	}
	if diff := cmp.Diff(
		plannerAccountModes(afterResolutionFromMessage),
		plannerAccountModes(blockAccounts[0]),
	); diff != "" {
		t.Fatalf("incomplete metadata hid resolved lookup accounts (-message +block):\n%s", diff)
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

		plannerAccounts, available := plannerAccountsForBlock(b)
		if !available {
			t.Skip("skipping block whose planner accounts are unavailable")
		}
		plan := buildDependencyPlan(plannerAccounts)
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
		if len(plannerAccounts) <= 128 {
			assertConflictOrderReachable(t, plannerAccounts, plan)
		}
	})
}

func FuzzDependencyPlanPreservesConflictOrder(f *testing.F) {
	f.Add([]byte{4, 1, 0, 1, 1, 2, 0, 2, 1})
	f.Add([]byte{8, 3, 1, 2, 0, 4, 1, 3, 0, 2, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			t.Skip()
		}
		transactionCount := 1 + int(data[0]%64)
		transactions := make([]plannerTransactionAccounts, transactionCount)
		cursor := 1
		nextByte := func() byte {
			value := data[cursor%len(data)]
			cursor++
			return value
		}
		for idx := range transactions {
			accessCount := 1 + int(nextByte()%8)
			builder := newPlannerTransactionAccountBuilder(accessCount)
			for range accessCount {
				account := testPk(nextByte() % 32)
				writable := nextByte()&1 != 0
				builder.add(account, writable)
			}
			transactions[idx] = builder.finish()
		}

		plan := buildDependencyPlan(transactions)
		for prerequisite := range plan.inDegree {
			start, end := plan.offsets[prerequisite], plan.offsets[prerequisite+1]
			for _, dependent := range plan.dependents[start:end] {
				if int(dependent) <= prerequisite {
					t.Fatalf("edge %d -> %d does not point forward", prerequisite, dependent)
				}
			}
		}
		assertConflictOrderReachable(t, transactions, plan)
	})
}

var benchmarkDependencyPlanSink *dependencyPlan

func benchmarkPlannerBlock(transactionCount int, sharedWriter bool) *block.Block {
	transactions := make([]*solana.Transaction, transactionCount)
	sharedAccount := testPk(0xe0)
	readonlyProgram := testPk(0xe1)
	for idx := range transactions {
		payer := solana.PublicKey{}
		binary.LittleEndian.PutUint64(payer[:8], uint64(idx+1))
		if sharedWriter {
			payer = sharedAccount
		}
		uniqueWritable := solana.PublicKey{}
		binary.LittleEndian.PutUint64(uniqueWritable[:8], uint64(transactionCount+idx+1))
		uniqueReadonly := solana.PublicKey{}
		binary.LittleEndian.PutUint64(uniqueReadonly[:8], uint64(2*transactionCount+idx+1))
		transactions[idx] = &solana.Transaction{
			Message: solana.Message{
				Header: solana.MessageHeader{
					NumRequiredSignatures:       1,
					NumReadonlyUnsignedAccounts: 2,
				},
				AccountKeys: []solana.PublicKey{
					payer,
					uniqueWritable,
					readonlyProgram,
					uniqueReadonly,
				},
			},
		}
	}
	return &block.Block{Transactions: transactions}
}

func BenchmarkDependencyPlan(b *testing.B) {
	for _, test := range []struct {
		name         string
		sharedWriter bool
	}{
		{name: "independent", sharedWriter: false},
		{name: "shared_writer", sharedWriter: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			block := benchmarkPlannerBlock(10_000, test.sharedWriter)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				plan, available := dependencyPlanForBlock(block)
				if !available {
					b.Fatal("dependency planner unavailable")
				}
				benchmarkDependencyPlanSink = plan
			}
		})
	}
}
