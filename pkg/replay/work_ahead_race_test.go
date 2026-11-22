package replay

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// raceFixture: one transaction whose valid early execution is still running
// at its turn.
func raceFixture(t *testing.T) (*workAhead, *workAheadEntry, solana.PublicKey, solana.PublicKey) {
	t.Helper()
	payer, other := testPk(1), testPk(2)
	parent := accounts.NewMemAccountsWithLen(2)
	for _, key := range []solana.PublicKey{payer, other} {
		if err := parent.SetAccountWithoutLock(key, &accounts.Account{Key: key, Lamports: 10}); err != nil {
			t.Fatal(err)
		}
	}
	overlay := accounts.NewOverlayAccounts(parent)
	slotCtx := &sealevel.SlotCtx{Accounts: overlay}
	tx := &solana.Transaction{Message: solana.Message{AccountKeys: []solana.PublicKey{payer, other}}}
	w := &workAhead{slotCtx: slotCtx, overlay: overlay, txs: []*solana.Transaction{tx}, entries: make([]atomic.Pointer[workAheadEntry], 1)}
	e := &workAheadEntry{state: workAheadRunning, finished: make(chan struct{}), began: time.Now()}
	e.keys, e.versions, e.base = workAheadReadSet(slotCtx, overlay, tx)
	w.entries[0].Store(e)
	return w, e, payer, other
}

func withRace(t *testing.T, exec func(LoadAndExecuteTransactionInput) LoadAndExecuteTransactionOutput) {
	t.Helper()
	savedRace, savedExec, savedAfter := workAheadRace, workAheadLoadAndExecute, workAheadRaceAfter
	workAheadRace, workAheadLoadAndExecute, workAheadRaceAfter = true, exec, 0
	t.Cleanup(func() { workAheadRace, workAheadLoadAndExecute, workAheadRaceAfter = savedRace, savedExec, savedAfter })
}

// The private run at the turn finishes while the early execution still runs:
// its result is used, and the early execution is abandoned, dropping its
// result when it ends.
func TestWorkAheadRacePrivateRunWins(t *testing.T) {
	fresh := &TransactionError{InstructionError: errors.New("fresh")}
	withRace(t, func(in LoadAndExecuteTransactionInput) LoadAndExecuteTransactionOutput {
		if !in.Speculative || in.SpeculativeCancel == nil {
			t.Error("the private run must be speculative and cancellable")
		}
		var out LoadAndExecuteTransactionOutput
		out.ProcessingResult.TransactionError = fresh
		return out
	})
	w, e, _, _ := raceFixture(t)
	out, release := w.claim(0)
	release()
	if out == nil || out.ProcessingResult.TransactionError != fresh {
		t.Fatalf("private result not used: %+v", out)
	}
	if !e.abandoned || w.stats.race.won.Load() != 1 {
		t.Fatalf("abandoned %v won %d", e.abandoned, w.stats.race.won.Load())
	}
	// the slow early execution ends later and is dropped
	var late LoadAndExecuteTransactionOutput
	late.ProcessingResult.TransactionError = &TransactionError{}
	w.ran(0, e, late, false, false)
	if e.state != workAheadDropped {
		t.Fatalf("abandoned early execution kept, state %d", e.state)
	}
}

// The early execution ends first: ran stops the private run through its
// cancel flag and the early result is adopted under the usual checks.
func TestWorkAheadRaceEarlyRunWins(t *testing.T) {
	w, e, payer, other := raceFixture(t)
	early := &sealevel.ExecutionCtx{TransactionContext: &sealevel.TransactionCtx{}}
	for _, k := range []solana.PublicKey{payer, other} {
		early.TransactionContext.Accounts.Accounts = append(early.TransactionContext.Accounts.Accounts, &accounts.Account{Key: k})
	}
	withRace(t, func(in LoadAndExecuteTransactionInput) LoadAndExecuteTransactionOutput {
		go func() {
			time.Sleep(5 * time.Millisecond)
			w.ran(0, e, LoadAndExecuteTransactionOutput{ExecCtx: early}, false, false)
		}()
		for !in.SpeculativeCancel.Load() {
			time.Sleep(100 * time.Microsecond)
		}
		var out LoadAndExecuteTransactionOutput // as a cancelled run returns
		out.ProcessingResult.TransactionError = &TransactionError{}
		return out
	})
	out, release := w.claim(0)
	if out == nil || out.ExecCtx != early {
		t.Fatalf("early result not adopted: %+v", out)
	}
	release()
	if e.abandoned || w.stats.race.lost.Load() != 1 || w.stats.adopted.Load() != 1 {
		t.Fatalf("abandoned %v lost %d adopted %d", e.abandoned, w.stats.race.lost.Load(), w.stats.adopted.Load())
	}
}

// A private run that produced nothing usable (here a panic) leaves the turn
// to the early execution.
func TestWorkAheadRacePanicFallsBack(t *testing.T) {
	w, e, _, _ := raceFixture(t)
	earlyErr := &TransactionError{}
	withRace(t, func(LoadAndExecuteTransactionInput) LoadAndExecuteTransactionOutput { panic("private") })
	go func() {
		time.Sleep(5 * time.Millisecond)
		var done LoadAndExecuteTransactionOutput
		done.ProcessingResult.TransactionError = earlyErr
		w.ran(0, e, done, false, false)
	}()
	out, release := w.claim(0)
	if out == nil || out.ProcessingResult.TransactionError != earlyErr || e.abandoned {
		t.Fatalf("early result not adopted after a private panic: %+v", out)
	}
	release()
}

// With a start delay, an early execution that ends within it is waited for
// and no private run starts.
func TestWorkAheadRaceDelaySkipsShortWaits(t *testing.T) {
	w, e, _, _ := raceFixture(t)
	withRace(t, func(LoadAndExecuteTransactionInput) LoadAndExecuteTransactionOutput {
		t.Error("a private run started for an early execution that ended within the delay")
		return LoadAndExecuteTransactionOutput{}
	})
	workAheadRaceAfter = time.Second
	done := &TransactionError{}
	go func() {
		time.Sleep(2 * time.Millisecond)
		var out LoadAndExecuteTransactionOutput
		out.ProcessingResult.TransactionError = done
		w.ran(0, e, out, false, false)
	}()
	out, release := w.claim(0)
	if out == nil || out.ProcessingResult.TransactionError != done || w.stats.race.won.Load()+w.stats.race.lost.Load() != 0 {
		t.Fatalf("early result not adopted after a short wait: %+v", out)
	}
	release()
}

// The private run returns its pool arena whether it wins or loses.
func TestWorkAheadRaceReturnsArena(t *testing.T) {
	saved := workAheadArenas
	workAheadArenas = make(chan *arena.Arena[sealevel.BorrowedAccount], 1)
	workAheadArenas <- arena.New[sealevel.BorrowedAccount](4)
	t.Cleanup(func() { workAheadArenas = saved })
	withRace(t, func(in LoadAndExecuteTransactionInput) LoadAndExecuteTransactionOutput {
		if in.Arena == nil {
			t.Error("the private run did not take a pool arena")
		}
		var out LoadAndExecuteTransactionOutput
		out.ProcessingResult.TransactionError = &TransactionError{}
		return out
	})
	w, _, _, _ := raceFixture(t)
	out, release := w.claim(0)
	if out == nil || len(workAheadArenas) != 0 {
		t.Fatalf("won: out %v, pool %d", out, len(workAheadArenas))
	}
	release()
	if len(workAheadArenas) != 1 {
		t.Fatal("the winning private run kept its arena after release")
	}
}
