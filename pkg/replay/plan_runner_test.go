package replay

import (
	"encoding/binary"
	"math/rand"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// Every transaction runs exactly once, and only after all its prerequisites
// have committed, for any worker count.
func TestRunDependencyPlan(t *testing.T) {
	for _, priority := range []bool{false, true} {
		testRunDependencyPlan(t, priority)
	}
	runDependencyPlan(&dependencyPlan{}, 4, planSchedule{}, func(int, int) { t.Fatal("empty plan ran a transaction") })
}

func TestPlanHeightsAndReadyHeap(t *testing.T) {
	x, y := testPk(1), testPk(2)
	write := func(keys ...solana.PublicKey) plannerTransactionAccounts {
		return newPlannerTransactionAccounts(2, func(add func(solana.PublicKey, bool)) {
			for _, k := range keys {
				add(k, true)
			}
		})
	}
	// 0 -> 1 -> 2 on x; 3 alone on y.
	plan := buildDependencyPlan([]plannerTransactionAccounts{write(x), write(x), write(x), write(y)})
	height := planHeights(plan, nil)
	if want := []int32{3, 2, 1, 1}; !slices.Equal(height, want) {
		t.Fatalf("heights = %v, want %v", height, want)
	}
	h := &readyHeap{height: height}
	for _, tx := range []int32{3, 2, 0, 1} {
		h.push(tx)
	}
	var order []int32
	for range 4 {
		order = append(order, h.pop())
	}
	if want := []int32{0, 1, 2, 3}; !slices.Equal(order, want) {
		t.Fatalf("pop order = %v, want highest first, then block order %v", order, want)
	}
}

func testRunDependencyPlan(t *testing.T, priority bool) {
	for seed := int64(0); seed < 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		transactions := randomPlannerTransactions(rng, 30+rng.Intn(120), 2+rng.Intn(12))
		plan := buildDependencyPlan(transactions)
		n := len(transactions)
		prerequisites := make([][]uint32, n)
		for tx := range n {
			for _, dependent := range plan.dependents[plan.offsets[tx]:plan.offsets[tx+1]] {
				prerequisites[dependent] = append(prerequisites[dependent], uint32(tx))
			}
		}
		for _, workers := range []int{1, 2, 4, 8} {
			fresh := buildDependencyPlan(transactions)
			committed := make([]atomic.Bool, n)
			runs := make([]atomic.Int32, n)
			var violations atomic.Int32
			runDependencyPlan(fresh, workers, planSchedule{priority: priority}, func(_, tx int) {
				runs[tx].Add(1)
				for _, p := range prerequisites[tx] {
					if !committed[p].Load() {
						violations.Add(1)
					}
				}
				if tx%3 == 0 {
					runtime.Gosched()
				}
				committed[tx].Store(true)
			})
			if violations.Load() != 0 {
				t.Fatalf("seed %d, %d workers: %d transactions ran before a prerequisite committed", seed, workers, violations.Load())
			}
			for tx := range n {
				if got := runs[tx].Load(); got != 1 {
					t.Fatalf("seed %d, %d workers: transaction %d ran %d times", seed, workers, tx, got)
				}
			}
		}
	}
}

func TestWeightedHeightsAndRequestedKCU(t *testing.T) {
	x, y := testPk(1), testPk(2)
	write := func(key solana.PublicKey) plannerTransactionAccounts {
		return newPlannerTransactionAccounts(1, func(add func(solana.PublicKey, bool)) { add(key, true) })
	}
	// 0 -> 1 -> 2 on x (light), 3 alone on y (heavy).
	plan := buildDependencyPlan([]plannerTransactionAccounts{write(x), write(x), write(x), write(y)})
	weights := []int32{10, 10, 10, 200}
	height := planHeights(plan, func(tx int) int32 { return weights[tx] })
	if want := []int32{30, 20, 10, 200}; !slices.Equal(height, want) {
		t.Fatalf("weighted heights = %v, want %v", height, want)
	}

	payer, program := testPk(9), testPk(8)
	tx := func(instrs ...solana.CompiledInstruction) *solana.Transaction {
		return &solana.Transaction{Message: solana.Message{
			AccountKeys:  []solana.PublicKey{payer, program, addresses.ComputeBudgetProgramAddr},
			Instructions: instrs,
		}}
	}
	call := solana.CompiledInstruction{ProgramIDIndex: 1}
	limit := func(units uint32) solana.CompiledInstruction {
		data := append([]byte{sealevel.ComputeBudgetInstrTypeSetComputeUnitLimit}, binary.LittleEndian.AppendUint32(nil, units)...)
		return solana.CompiledInstruction{ProgramIDIndex: 2, Data: data}
	}
	for _, c := range []struct {
		name string
		tx   *solana.Transaction
		want int32
	}{
		{"explicit limit", tx(limit(300_000), call), 301},
		{"default per instruction", tx(call, call), 401},
		{"capped", tx(limit(5_000_000), call), 1401},
		{"compute budget only", tx(limit(0)), 1},
	} {
		if got := requestedKCU(c.tx); got != c.want {
			t.Fatalf("%s: requestedKCU = %d, want %d", c.name, got, c.want)
		}
	}
}

func randomPlannerTransactions(rng *rand.Rand, count, keys int) []plannerTransactionAccounts {
	transactions := make([]plannerTransactionAccounts, count)
	for idx := range transactions {
		transactions[idx] = newPlannerTransactionAccounts(4, func(add func(solana.PublicKey, bool)) {
			for range 1 + rng.Intn(3) {
				add(testPk(byte(rng.Intn(keys))), rng.Intn(3) == 0)
			}
		})
	}
	return transactions
}
