package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// chainTestTx is a legacy transaction whose accounts are the given writable
// keys followed by the given read-only keys.
func chainTestTx(writable, readonly []byte) *solana.Transaction {
	keys := make([]solana.PublicKey, 0, len(writable)+len(readonly))
	for _, key := range writable {
		keys = append(keys, testPk(key))
	}
	for _, key := range readonly {
		keys = append(keys, testPk(key))
	}
	return &solana.Transaction{Message: solana.Message{
		Header:      solana.MessageHeader{NumReadonlyUnsignedAccounts: uint8(len(readonly))},
		AccountKeys: keys,
	}}
}

// longestDependencyChain measures one block with a table of its own.
func longestDependencyChain(transactions []*solana.Transaction, computeUnits []uint64) (cu uint64, txs uint32, ok bool) {
	var meter longestChainMeter
	return meter.measure(transactions, computeUnits)
}

type chainTestStep struct {
	writable []byte
	readonly []byte
	cu       uint64
}

func chainOf(steps ...chainTestStep) ([]*solana.Transaction, []uint64) {
	transactions := make([]*solana.Transaction, len(steps))
	computeUnits := make([]uint64, len(steps))
	for idx, step := range steps {
		transactions[idx] = chainTestTx(step.writable, step.readonly)
		computeUnits[idx] = step.cu
	}
	return transactions, computeUnits
}

func TestLongestDependencyChain(t *testing.T) {
	w := func(keys ...byte) []byte { return keys }
	r := func(keys ...byte) []byte { return keys }
	cases := []struct {
		name    string
		steps   []chainTestStep
		wantCU  uint64
		wantTxs uint32
	}{
		{name: "empty block"},
		{
			name: "independent transactions give the single largest",
			steps: []chainTestStep{
				{writable: w(1), cu: 300},
				{writable: w(2), cu: 900},
				{writable: w(3), cu: 500},
			},
			wantCU: 900, wantTxs: 1,
		},
		{
			name: "chain through one shared writable account",
			steps: []chainTestStep{
				{writable: w(1, 9), cu: 100},
				{writable: w(2, 9), cu: 200},
				{writable: w(3, 9), cu: 300},
			},
			wantCU: 600, wantTxs: 3,
		},
		{
			name: "reads of the same account do not link",
			steps: []chainTestStep{
				{writable: w(1), readonly: r(9), cu: 100},
				{writable: w(2), readonly: r(9), cu: 200},
				{writable: w(3), readonly: r(9), cu: 300},
			},
			wantCU: 300, wantTxs: 1,
		},
		{
			name: "read after write links",
			steps: []chainTestStep{
				{writable: w(9), cu: 400},
				{writable: w(1), readonly: r(9), cu: 50},
			},
			wantCU: 450, wantTxs: 2,
		},
		{
			name: "write after read links",
			steps: []chainTestStep{
				{writable: w(1), readonly: r(9), cu: 50},
				{writable: w(9), cu: 400},
			},
			wantCU: 450, wantTxs: 2,
		},
		{
			name: "a write after several reads takes the largest reader",
			steps: []chainTestStep{
				{writable: w(1), readonly: r(9), cu: 100},
				{writable: w(2), readonly: r(9), cu: 700},
				{writable: w(3), readonly: r(9), cu: 300},
				{writable: w(9), cu: 10},
			},
			wantCU: 710, wantTxs: 2,
		},
		{
			name: "readers after a write each extend the writer, not each other",
			steps: []chainTestStep{
				{writable: w(9), cu: 100},
				{writable: w(1), readonly: r(9), cu: 20},
				{writable: w(2), readonly: r(9), cu: 30},
			},
			wantCU: 130, wantTxs: 2,
		},
		{
			name: "a write clears the readers before it",
			steps: []chainTestStep{
				{writable: w(1), readonly: r(9), cu: 900},
				{writable: w(9), cu: 1},
				{writable: w(9), cu: 1},
				{writable: w(2), readonly: r(9), cu: 1},
			},
			wantCU: 903, wantTxs: 4,
		},
		{
			name: "an older conflict is reached through the latest writer",
			steps: []chainTestStep{
				{writable: w(9), cu: 500},
				{writable: w(9), cu: 1},
				{writable: w(9, 8), cu: 1},
				{writable: w(8), cu: 40},
			},
			wantCU: 542, wantTxs: 4,
		},
		{
			name: "the heaviest of several predecessors is taken",
			steps: []chainTestStep{
				{writable: w(1), cu: 100},
				{writable: w(2), cu: 800},
				{writable: w(3), cu: 300},
				{writable: w(1, 2, 3), cu: 5},
			},
			wantCU: 805, wantTxs: 2,
		},
		{
			name: "a long cheap chain loses to one expensive transaction",
			steps: []chainTestStep{
				{writable: w(9), cu: 10},
				{writable: w(9), cu: 10},
				{writable: w(9), cu: 10},
				{writable: w(1), cu: 1000},
			},
			wantCU: 1000, wantTxs: 1,
		},
		{
			name: "equal CU prefers the longer chain",
			steps: []chainTestStep{
				{writable: w(1), cu: 600},
				{writable: w(9), cu: 200},
				{writable: w(9), cu: 400},
			},
			wantCU: 600, wantTxs: 2,
		},
		{
			name: "a transaction that consumed nothing still links",
			steps: []chainTestStep{
				{writable: w(9), cu: 70},
				{writable: w(9, 8), cu: 0},
				{writable: w(8), cu: 30},
			},
			wantCU: 100, wantTxs: 3,
		},
		{
			name: "a block that consumed nothing still reports its chain length",
			steps: []chainTestStep{
				{writable: w(9)},
				{writable: w(9)},
			},
			wantCU: 0, wantTxs: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			transactions, computeUnits := chainOf(tc.steps...)
			cu, txs, ok := longestDependencyChain(transactions, computeUnits)
			require.True(t, ok)
			require.Equal(t, tc.wantCU, cu, "chain CU")
			require.Equal(t, tc.wantTxs, txs, "chain transactions")
		})
	}
}

// The same transactions in a different order form different chains: the block
// order decides which side of a conflict waits.
func TestLongestDependencyChainOrderMatters(t *testing.T) {
	reader := chainTestStep{writable: []byte{1}, readonly: []byte{9}, cu: 100}
	otherReader := chainTestStep{writable: []byte{2}, readonly: []byte{9}, cu: 100}
	writer := chainTestStep{writable: []byte{9}, cu: 100}

	transactions, computeUnits := chainOf(reader, otherReader, writer)
	cu, txs, ok := longestDependencyChain(transactions, computeUnits)
	require.True(t, ok)
	require.Equal(t, uint64(200), cu, "the writer waits for one reader")
	require.Equal(t, uint32(2), txs)

	transactions, computeUnits = chainOf(reader, writer, otherReader)
	cu, txs, ok = longestDependencyChain(transactions, computeUnits)
	require.True(t, ok)
	require.Equal(t, uint64(300), cu, "the writer waits for a reader and the second reader for the writer")
	require.Equal(t, uint32(3), txs)
}

// A failed transaction held its account locks and consumed CU like any other,
// so the chain counts whatever it consumed. Failure is not an input at all:
// the measurement only sees the consumed figure.
func TestLongestDependencyChainCountsFailedTransactionCU(t *testing.T) {
	const failedCU = 1_400_000
	transactions, computeUnits := chainOf(
		chainTestStep{writable: []byte{9}, cu: 5_000},
		chainTestStep{writable: []byte{9}, cu: failedCU}, // ran out of compute
		chainTestStep{writable: []byte{9}, cu: 7_000},
	)
	cu, txs, ok := longestDependencyChain(transactions, computeUnits)
	require.True(t, ok)
	require.Equal(t, uint64(5_000+failedCU+7_000), cu)
	require.Equal(t, uint32(3), txs)
}

// Accounts loaded from an address table conflict like static ones, with the
// writability the lookup gives them.
func TestLongestDependencyChainUsesResolvedLookupAccounts(t *testing.T) {
	table := testPk(0x70)
	loadedWritable := testPk(0x71)
	loadedReadonly := testPk(0x72)
	lookupTx := func(payer byte) *solana.Transaction {
		msg := solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys: []solana.PublicKey{testPk(payer)},
			AddressTableLookups: solana.MessageAddressTableLookupSlice{{
				AccountKey:      table,
				WritableIndexes: []uint8{0},
				ReadonlyIndexes: []uint8{1},
			}},
		}
		msg.SetVersion(solana.MessageVersionV0)
		return &solana.Transaction{Message: msg}
	}
	resolve := func(transaction *solana.Transaction) {
		require.NoError(t, transaction.Message.SetAddressTables(map[solana.PublicKey]solana.PublicKeySlice{
			table: {loadedWritable, loadedReadonly},
		}))
		require.NoError(t, transaction.Message.ResolveLookups())
	}

	first, second := lookupTx(1), lookupTx(2)
	_, _, ok := longestDependencyChain([]*solana.Transaction{first, second}, []uint64{300, 400})
	require.False(t, ok, "unresolved lookups leave the accounts unknown")

	resolve(first)
	resolve(second)
	cu, txs, ok := longestDependencyChain([]*solana.Transaction{first, second}, []uint64{300, 400})
	require.True(t, ok)
	require.Equal(t, uint64(700), cu, "both write the same loaded account")
	require.Equal(t, uint32(2), txs)

	// A static reader of the read-only loaded account links to neither; a
	// static writer of it waits for both.
	reader := chainTestTx([]byte{3}, nil)
	reader.Message.AccountKeys = append(reader.Message.AccountKeys, loadedReadonly)
	reader.Message.Header.NumReadonlyUnsignedAccounts = 1
	writer := chainTestTx([]byte{4}, nil)
	writer.Message.AccountKeys = append(writer.Message.AccountKeys, loadedReadonly)
	cu, txs, ok = longestDependencyChain(
		[]*solana.Transaction{first, second, reader, writer},
		[]uint64{300, 400, 50, 60},
	)
	require.True(t, ok)
	require.Equal(t, uint64(760), cu)
	require.Equal(t, uint32(3), txs)
}

func TestLongestDependencyChainRejectsMismatchedInputs(t *testing.T) {
	transactions, _ := chainOf(chainTestStep{writable: []byte{1}, cu: 1})
	_, _, ok := longestDependencyChain(transactions, nil)
	require.False(t, ok)
	_, _, ok = longestDependencyChain([]*solana.Transaction{nil}, []uint64{1})
	require.False(t, ok)
}

// syntheticChainBlock builds a block of legacy transactions over a skewed
// account population: a unique fee payer each, a few hot writable accounts,
// and a small set of widely read accounts, as on mainnet.
func syntheticChainBlock(seed int64, transactions, accountsPerTransaction int) ([]*solana.Transaction, []uint64) {
	rng := rand.New(rand.NewSource(seed))
	// Hash the identifier so keys are spread like real ones.
	key := func(space byte, n int) solana.PublicKey {
		var id [9]byte
		id[0] = space
		binary.LittleEndian.PutUint64(id[1:], uint64(n))
		return sha256.Sum256(id[:])
	}
	const (
		hotWritable  = 40
		warmWritable = 3000
		commonRead   = 60
		rareRead     = 2000
	)
	txs := make([]*solana.Transaction, transactions)
	computeUnits := make([]uint64, transactions)
	for txIdx := range txs {
		writable := max(2, accountsPerTransaction/3)
		readonly := accountsPerTransaction - writable
		keys := make([]solana.PublicKey, 0, accountsPerTransaction)
		seen := make(map[solana.PublicKey]struct{}, accountsPerTransaction)
		add := func(pk solana.PublicKey) bool {
			if _, dup := seen[pk]; dup {
				return false
			}
			seen[pk] = struct{}{}
			keys = append(keys, pk)
			return true
		}
		add(key(0, txIdx)) // fee payer
		for len(keys) < writable {
			if rng.Intn(4) == 0 {
				add(key(1, rng.Intn(hotWritable)))
			} else {
				add(key(2, rng.Intn(warmWritable)))
			}
		}
		for len(keys) < writable+readonly {
			if rng.Intn(2) == 0 {
				add(key(3, rng.Intn(commonRead)))
			} else {
				add(key(4, rng.Intn(rareRead)))
			}
		}
		txs[txIdx] = &solana.Transaction{Message: solana.Message{
			Header: solana.MessageHeader{
				NumRequiredSignatures:       1,
				NumReadonlyUnsignedAccounts: uint8(readonly),
			},
			AccountKeys: keys,
		}}
		computeUnits[txIdx] = uint64(1_000 + rng.Intn(400_000))
	}
	return txs, computeUnits
}

// referenceLongestChain applies the definition directly: every earlier
// transaction is compared with every later one, using the planner's own
// account views.
func referenceLongestChain(t *testing.T, transactions []*solana.Transaction, computeUnits []uint64) (uint64, uint32) {
	t.Helper()
	views, available := plannerAccountsForBlock(&block.Block{Transactions: transactions})
	require.True(t, available)
	conflict := func(earlier, later plannerTransactionAccounts) bool {
		for _, a := range earlier.accounts {
			for _, b := range later.accounts {
				if a.publicKey == b.publicKey && (a.writable || b.writable) {
					return true
				}
			}
		}
		return false
	}
	paths := make([]chainPath, len(transactions))
	var longest chainPath
	for later := range transactions {
		var before chainPath
		for earlier := 0; earlier < later; earlier++ {
			if conflict(views[earlier], views[later]) && paths[earlier].exceeds(before) {
				before = paths[earlier]
			}
		}
		paths[later] = chainPath{cu: before.cu + computeUnits[later], txs: before.txs + 1}
		if paths[later].exceeds(longest) {
			longest = paths[later]
		}
	}
	return longest.cu, longest.txs
}

// The linear pass must agree with the pairwise definition, and with the
// longest path through the plan replay actually schedules.
func TestLongestDependencyChainMatchesDefinitionAndPlan(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		transactions, computeUnits := syntheticChainBlock(seed, 300, 6+int(seed)*3)
		if seed%2 == 0 {
			// Equal weights make ties, so the tie rule is exercised too.
			for idx := range computeUnits {
				computeUnits[idx] = 1000
			}
		}
		cu, txs, ok := longestDependencyChain(transactions, computeUnits)
		require.True(t, ok)

		wantCU, wantTxs := referenceLongestChain(t, transactions, computeUnits)
		require.Equal(t, wantCU, cu, "seed %d: CU against the pairwise definition", seed)
		require.Equal(t, wantTxs, txs, "seed %d: length against the pairwise definition", seed)

		plan, available := dependencyPlanForBlock(&block.Block{Transactions: transactions})
		require.True(t, available)
		paths := make([]chainPath, len(transactions))
		var longest chainPath
		for tx := range transactions {
			paths[tx] = chainPath{cu: paths[tx].cu + computeUnits[tx], txs: paths[tx].txs + 1}
			if paths[tx].exceeds(longest) {
				longest = paths[tx]
			}
			for _, dependent := range plan.dependents[plan.offsets[tx]:plan.offsets[tx+1]] {
				if paths[tx].exceeds(paths[dependent]) {
					paths[dependent] = paths[tx]
				}
			}
		}
		require.Equal(t, longest.cu, cu, "seed %d: CU against the dependency plan", seed)
		require.Equal(t, longest.txs, txs, "seed %d: length against the dependency plan", seed)
		require.Greater(t, txs, uint32(1), "seed %d: the synthetic block has conflicts", seed)
	}
}

func TestLongestDependencyChainIsDeterministic(t *testing.T) {
	transactions, computeUnits := syntheticChainBlock(11, 400, 16)
	wantCU, wantTxs, ok := longestDependencyChain(transactions, computeUnits)
	require.True(t, ok)
	for range 20 {
		cu, txs, ok := longestDependencyChain(transactions, computeUnits)
		require.True(t, ok)
		require.Equal(t, wantCU, cu)
		require.Equal(t, wantTxs, txs)
	}
}

// withCleanBlockMetrics gives the test an empty per-block metrics record and
// restores the previous one afterwards.
func withCleanBlockMetrics(t *testing.T) {
	t.Helper()
	previous := metrics.GlobalBlockReplay
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
	t.Cleanup(func() { metrics.GlobalBlockReplay = previous })
}

func requirePassDone(t *testing.T, pass *longestChainPass, why string) {
	t.Helper()
	select {
	case <-pass.done:
	default:
		t.Fatalf("pass still running: %s", why)
	}
}

// The runner measures on its own goroutine and the row gets the same numbers
// a direct measurement gives, with the pass's duration and the join's wait.
func TestLongestChainRunnerRecordsItsBank(t *testing.T) {
	withCleanBlockMetrics(t)
	transactions, computeUnits := syntheticChainBlock(3, 300, 14)
	wantCU, wantTxs, ok := longestDependencyChain(transactions, computeUnits)
	require.True(t, ok)

	var runner longestChainRunner
	bank := &sealevel.SlotCtx{Slot: 10}
	runner.start(bank, transactions, computeUnits)
	runner.record(bank)
	require.Equal(t, wantCU, metrics.GlobalBlockReplay.LongestChainCU)
	require.Equal(t, wantTxs, metrics.GlobalBlockReplay.LongestChainTransactions)
	require.Equal(t, uint64(1), metrics.GlobalBlockReplay.LongestChainMeasurement.Count)
	require.NotZero(t, metrics.GlobalBlockReplay.LongestChainMeasurement.SumNanoseconds)
	require.Equal(t, uint64(1), metrics.GlobalBlockReplay.LongestChainWait.Count)
	require.Nil(t, runner.last, "a recorded pass is forgotten")

	// The pass is consumed: a second row for the same bank gets nothing.
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
	runner.record(bank)
	require.Zero(t, metrics.GlobalBlockReplay.LongestChainTransactions)
	require.Zero(t, metrics.GlobalBlockReplay.LongestChainMeasurement.Count)
}

// A bank replay did not execute has no pass. Its row stays empty, and it does
// not take the result of a bank that finalized earlier and was never recorded.
func TestLongestChainRunnerIgnoresBanksWithoutAPass(t *testing.T) {
	withCleanBlockMetrics(t)
	transactions, computeUnits := syntheticChainBlock(4, 100, 8)
	var runner longestChainRunner
	runner.record(nil)
	runner.record(&sealevel.SlotCtx{Slot: 1})
	require.Zero(t, metrics.GlobalBlockReplay.LongestChainMeasurement.Count)

	abandoned := runner.start(&sealevel.SlotCtx{Slot: 2}, transactions, computeUnits)
	runner.record(&sealevel.SlotCtx{Slot: 3}) // an adopted bank's row
	require.Zero(t, metrics.GlobalBlockReplay.LongestChainTransactions, "another bank's chain is not this bank's")
	require.Zero(t, metrics.GlobalBlockReplay.LongestChainMeasurement.Count)
	require.Same(t, abandoned, runner.last, "the unrecorded pass is still tracked")

	runner.drain()
	requirePassDone(t, abandoned, "drain joins the unrecorded pass")
	require.Nil(t, runner.last)
	runner.drain() // nothing in flight
}

// A bank that finalizes and is then abandoned (never recorded) leaves its pass
// behind. The next bank's pass waits for it instead of sharing the meter's
// table with it, and still gets its own result; the abandoned bank's result is
// dropped.
func TestLongestChainRunnerAbandonedBankDoesNotDisturbTheNext(t *testing.T) {
	withCleanBlockMetrics(t)
	// A large block so the first pass is still running when the next starts.
	firstTxs, firstCU := syntheticChainBlock(5, 2000, 30)
	secondTxs, secondCU := syntheticChainBlock(6, 200, 10)
	wantCU, wantTxs, ok := longestDependencyChain(secondTxs, secondCU)
	require.True(t, ok)

	var runner longestChainRunner
	first, second := &sealevel.SlotCtx{Slot: 20}, &sealevel.SlotCtx{Slot: 21}
	abandoned := runner.start(first, firstTxs, firstCU)
	runner.start(second, secondTxs, secondCU)

	runner.record(first)
	require.Zero(t, metrics.GlobalBlockReplay.LongestChainMeasurement.Count, "the superseded bank records nothing")

	runner.record(second)
	requirePassDone(t, abandoned, "the next pass ran only after the abandoned one finished")
	require.Equal(t, wantCU, metrics.GlobalBlockReplay.LongestChainCU)
	require.Equal(t, wantTxs, metrics.GlobalBlockReplay.LongestChainTransactions)
	require.Nil(t, runner.last)
}

// Passes started back to back without a join run one at a time on the shared
// meter, in order, and each produces its own block's result. Run under the
// race detector this is the check that two passes never share the table.
func TestLongestChainRunnerSerializesPasses(t *testing.T) {
	withCleanBlockMetrics(t)
	const blocks = 24
	type want struct {
		cu  uint64
		txs uint32
	}
	wants := make([]want, blocks)
	passes := make([]*longestChainPass, blocks)
	var runner longestChainRunner
	for i := range blocks {
		transactions, computeUnits := syntheticChainBlock(int64(100+i), 150+10*i, 6+i%20)
		cu, txs, ok := longestDependencyChain(transactions, computeUnits)
		require.True(t, ok)
		wants[i] = want{cu: cu, txs: txs}
		passes[i] = runner.start(&sealevel.SlotCtx{Slot: uint64(i)}, transactions, computeUnits)
	}
	runner.drain()
	require.Nil(t, runner.last)
	for i, pass := range passes {
		requirePassDone(t, pass, "the last pass being done means every earlier one is")
		require.True(t, pass.ok, "block %d", i)
		require.Equal(t, wants[i].cu, pass.cu, "block %d chain CU", i)
		require.Equal(t, wants[i].txs, pass.txs, "block %d chain transactions", i)
	}
}

// abandon is what a failed block tail calls: the pass is over when it
// returns, and nothing is recorded for the bank afterwards.
func TestLongestChainRunnerAbandonJoins(t *testing.T) {
	withCleanBlockMetrics(t)
	transactions, computeUnits := syntheticChainBlock(8, 1500, 24)
	var runner longestChainRunner
	bank := &sealevel.SlotCtx{Slot: 30}
	pass := runner.start(bank, transactions, computeUnits)
	runner.abandon(pass)
	requirePassDone(t, pass, "abandon waits for the pass")
	require.Nil(t, runner.last)
	runner.abandon(nil)

	runner.record(bank)
	require.Zero(t, metrics.GlobalBlockReplay.LongestChainTransactions)
	require.Zero(t, metrics.GlobalBlockReplay.LongestChainMeasurement.Count)
}

// A block whose accounts cannot be derived records the pass and the wait but
// no chain.
func TestLongestChainRunnerRecordsNoChainWhenUnmeasurable(t *testing.T) {
	withCleanBlockMetrics(t)
	var runner longestChainRunner
	bank := &sealevel.SlotCtx{Slot: 40}
	runner.start(bank, []*solana.Transaction{nil}, []uint64{1})
	runner.record(bank)
	require.Equal(t, uint64(1), metrics.GlobalBlockReplay.LongestChainMeasurement.Count)
	require.Equal(t, uint64(1), metrics.GlobalBlockReplay.LongestChainWait.Count)
	require.Zero(t, metrics.GlobalBlockReplay.LongestChainCU)
	require.Zero(t, metrics.GlobalBlockReplay.LongestChainTransactions)
}

// BenchmarkLongestChainHandoff measures what the replay goroutine itself pays
// per block: starting the pass and joining it. The block is empty, so the
// pass does no work, and the join here also waits for the goroutine to be
// scheduled, which in replay happens during the block's tail.
func BenchmarkLongestChainHandoff(b *testing.B) {
	previous := metrics.GlobalBlockReplay
	defer func() { metrics.GlobalBlockReplay = previous }()
	var runner longestChainRunner
	bank := &sealevel.SlotCtx{}
	b.ReportAllocs()
	for b.Loop() {
		runner.start(bank, nil, nil)
		runner.record(bank)
	}
}

// BenchmarkLongestDependencyChain measures the pass itself: one call per
// replayed block, on the pass's own goroutine in replay.
func BenchmarkLongestDependencyChain(b *testing.B) {
	for _, size := range []struct{ transactions, accounts int }{
		{500, 10}, {500, 30}, {1000, 10}, {1000, 20}, {1000, 30},
	} {
		b.Run(fmt.Sprintf("txs=%d/accounts=%d", size.transactions, size.accounts), func(b *testing.B) {
			transactions, computeUnits := syntheticChainBlock(7, size.transactions, size.accounts)
			var meter longestChainMeter
			b.ReportAllocs()
			for b.Loop() {
				if _, _, ok := meter.measure(transactions, computeUnits); !ok {
					b.Fatal("chain unavailable")
				}
			}
		})
	}
}
