package replay

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Replace VM execution with a known CU charge. Offers, worker reservations,
// read-version checks, claims and waste accounting take their normal paths.
// These unsigned fixtures measure admission behavior, not execution time.
func newCapPressureBlock(t *testing.T, size int, usedCU uint64, capKCU int64) *workAhead {
	t.Helper()
	enabled, verify := workAheadEnabled, workAheadVerify
	capBefore, budgetBefore := workAheadWasteCapKCU, workAheadWasteBudgetKCU
	execBefore := workAheadLoadAndExecute
	workAheadEnabled, workAheadVerify = true, false
	workAheadWasteCapKCU, workAheadWasteBudgetKCU = capKCU, 48_000
	t.Cleanup(func() {
		workAheadEnabled, workAheadVerify = enabled, verify
		workAheadWasteCapKCU, workAheadWasteBudgetKCU = capBefore, budgetBefore
		workAheadLoadAndExecute = execBefore
	})

	slot, cleanup := newCommitTestSlotCtx()
	t.Cleanup(cleanup)
	txs := make([]*solana.Transaction, size)
	execute := make([]bool, size)
	limit := []byte{sealevel.ComputeBudgetInstrTypeSetComputeUnitLimit, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(limit[1:], uint32(usedCU))
	for i := range txs {
		var key solana.PublicKey
		binary.LittleEndian.PutUint64(key[:], uint64(i+1))
		require.NoError(t, slot.Accounts.SetAccountWithoutLock(key, &accounts.Account{Key: key, Lamports: 10}))
		txs[i] = &solana.Transaction{
			Signatures: make([]solana.Signature, 1),
			Message: solana.Message{
				Header:       solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
				AccountKeys:  []solana.PublicKey{key, addresses.ComputeBudgetProgramAddr},
				Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 1, Data: limit}},
			},
		}
		execute[i] = true
	}
	slot.Accounts = accounts.NewOverlayAccounts(slot.Accounts)
	workAheadLoadAndExecute = func(input LoadAndExecuteTransactionInput) LoadAndExecuteTransactionOutput {
		if _, err := input.SlotCtx.GetAccountShared(input.Transaction.Message.AccountKeys[0]); err != nil {
			t.Errorf("reading fixture account: %v", err)
			return LoadAndExecuteTransactionOutput{}
		}
		ctx := &sealevel.ExecutionCtx{ComputeMeter: cu.NewComputeMeter(usedCU)}
		if err := ctx.ComputeMeter.Consume(usedCU); err != nil {
			t.Errorf("metering fixture result: %v", err)
			return LoadAndExecuteTransactionOutput{}
		}
		return LoadAndExecuteTransactionOutput{ExecCtx: ctx}
	}
	w := newWorkAhead(slot, &block.Block{Transactions: txs}, execute, nil)
	require.NotNil(t, w)
	pool := workAheadArenas
	workAheadArenas = make(chan *arena.Arena[sealevel.BorrowedAccount], 8)
	for range cap(workAheadArenas) {
		workAheadArenas <- arena.New[sealevel.BorrowedAccount](1)
	}
	t.Cleanup(func() {
		w.finish(slot.Slot)
		clearWorkAheadRan(slot.Slot)
		workAheadArenas = pool
	})
	return w
}

// Supply an eligible offer directly so every cap sees the same candidates.
// The fixture controls the order and skips dependency discovery and prediction.
func runCapCandidate(t *testing.T, w *workAhead, i int) bool {
	t.Helper()
	w.offer(i, nil)
	e := w.entry(i)
	if e.state == workAheadDropped {
		return false
	}
	require.Len(t, w.queue, 1)
	next := <-w.queue
	require.Equal(t, i, next)
	w.run(next)
	require.Equal(t, workAheadFinished, e.state)
	return true
}

func claimCapCandidate(t *testing.T, w *workAhead, i int, stale bool) {
	t.Helper()
	if stale {
		key := w.txs[i].Message.AccountKeys[0]
		require.NoError(t, w.overlay.SetAccountWithoutLock(key, &accounts.Account{Key: key, Lamports: 11}))
	}
	out, release := w.claim(i)
	release()
	require.Equal(t, !stale, out != nil)
}

func TestWorkAheadCapPressureBlocks(t *testing.T) {
	// Move the burst of stale reads within a block. All caps get the same
	// candidates in the same order.
	for _, tc := range []struct {
		name                         string
		keptBefore, stale, keptAfter int
		usedCU                       uint64
		wantStale, wantKept          [3]int // at 8M, 12M and 16M
	}{
		{"large block, all results reusable", 64, 0, 0, 999_999, [3]int{0, 0, 0}, [3]int{64, 64, 64}},
		{"10M discarded then reusable work", 0, 10, 12, 999_999, [3]int{8, 10, 10}, [3]int{0, 12, 12}},
		{"same work with the discards last", 12, 10, 0, 999_999, [3]int{8, 10, 10}, [3]int{12, 12, 12}},
		{"14M discarded then reusable work", 0, 14, 12, 999_999, [3]int{8, 12, 14}, [3]int{0, 0, 12}},
		{"every result stale", 0, 24, 0, 999_999, [3]int{8, 12, 16}, [3]int{0, 0, 0}},
		{"many small discards at the boundary", 0, 240, 8, 49_999, [3]int{160, 240, 240}, [3]int{0, 0, 8}},
	} {
		for j, capKCU := range []int64{8_000, 12_000, 16_000} {
			t.Run(fmt.Sprintf("%s/%dM", tc.name, capKCU/1000), func(t *testing.T) {
				n := tc.keptBefore + tc.stale + tc.keptAfter
				w := newCapPressureBlock(t, n, tc.usedCU, capKCU)
				for i := range n {
					if runCapCandidate(t, w, i) {
						claimCapCandidate(t, w, i, i >= tc.keptBefore && i < tc.keptBefore+tc.stale)
					}
				}
				stale, kept := tc.wantStale[j], tc.wantKept[j]
				require.EqualValues(t, stale+kept, w.stats.executed.Load())
				require.EqualValues(t, stale, w.stats.invalid.Load())
				require.EqualValues(t, kept, w.stats.adopted.Load())
				require.EqualValues(t, n-stale-kept, w.stats.wasteCapped.Load())
				require.EqualValues(t, uint64(stale)*(tc.usedCU/1000+1), w.wastedKCU.Load())
				require.Zero(t, w.reservedKCU.Load())
				require.Zero(t, w.stats.overBudget.Load())
			})
		}
	}
}

func TestWorkAheadCapWithOutstandingResults(t *testing.T) {
	for _, tc := range []struct {
		name             string
		staleAfterCutoff bool
		wantWasteKCU     int64
		wantAdopted      int64
	}{
		{"remaining results reusable", false, 12_000, 7},
		{"remaining results stale", true, 19_000, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newCapPressureBlock(t, 21, 999_999, 12_000)
			for i := range 11 {
				require.True(t, runCapCandidate(t, w, i))
				claimCapCandidate(t, w, i, true)
			}
			for i := 11; i < 19; i++ {
				require.True(t, runCapCandidate(t, w, i))
			}
			// All eight executions have finished; their read sets will be
			// checked individually when each transaction reaches its turn.
			require.EqualValues(t, 8_000, w.reservedKCU.Load())
			w.offer(19, nil) // already queued when the 12th result is discarded
			require.Len(t, w.queue, 1)
			claimCapCandidate(t, w, 11, true)
			require.EqualValues(t, 12_000, w.wastedKCU.Load())
			w.run(<-w.queue)
			require.Equal(t, workAheadDropped, w.entry(19).state)
			require.False(t, runCapCandidate(t, w, 20)) // fresh offer after cutoff
			for i := 12; i < 19; i++ {
				claimCapCandidate(t, w, i, tc.staleAfterCutoff)
			}
			require.EqualValues(t, 19, w.stats.executed.Load())
			require.Equal(t, tc.wantAdopted, w.stats.adopted.Load())
			require.EqualValues(t, 2, w.stats.wasteCapped.Load())
			require.Equal(t, tc.wantWasteKCU, w.wastedKCU.Load())
			require.Zero(t, w.reservedKCU.Load())
			require.Zero(t, w.stats.overBudget.Load())
		})
	}
}
