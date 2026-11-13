package replay

import (
	"encoding/binary"
	"math"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

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

// The offering dispatcher must dispatch exactly like dispatchDependencyPlan,
// and offer a transaction only with the exact set of its prerequisites that
// have not committed, after all of them were dispatched.
func TestDispatchDependencyPlanOffering(t *testing.T) {
	for seed := int64(0); seed < 50; seed++ {
		rng := rand.New(rand.NewSource(seed))
		transactions := randomPlannerTransactions(rng, 40+rng.Intn(80), 3+rng.Intn(10))
		plan := buildDependencyPlan(transactions)
		n := len(transactions)
		prerequisites := make([][]uint32, n)
		for tx := range n {
			for _, dependent := range plan.dependents[plan.offsets[tx]:plan.offsets[tx+1]] {
				prerequisites[dependent] = append(prerequisites[dependent], uint32(tx))
			}
		}

		var mu sync.Mutex
		completed := make([]bool, n)
		received := make([]bool, n)
		offered := make([]bool, n)
		var failures []string
		fail := func(msg string) {
			failures = append(failures, msg)
		}
		const maxUnfinished = 2
		offer := func(tx int, unfinished []uint32) {
			mu.Lock()
			defer mu.Unlock()
			offered[tx] = true
			if len(unfinished) == 0 || len(unfinished) > maxUnfinished {
				fail("offer with an empty or oversized unfinished set")
			}
			if received[tx] {
				fail("offer after dispatch")
			}
			// The worker marks a commit before the dispatcher sees it, so only a
			// prerequisite left out must be visibly committed here.
			for _, p := range prerequisites[tx] {
				if !slices.Contains(unfinished, p) && !completed[p] {
					fail("a prerequisite left out of the unfinished set has not committed")
				}
			}
			for _, p := range unfinished {
				if !slices.Contains(prerequisites[tx], p) {
					fail("unfinished set names a non-prerequisite")
				}
			}
		}

		out := make(chan int, n)
		done := make(chan int, n)
		go dispatchDependencyPlanOffering(plan, out, done, maxUnfinished, offer)
		var inflight []int
		finished, sent, closed := 0, 0, false
		for finished < n {
			if len(inflight) == 0 && !closed {
				if tx, open := <-out; open {
					inflight, sent = append(inflight, tx), sent+1
				} else {
					closed = true
				}
			}
		drain:
			for !closed {
				select {
				case tx, open := <-out:
					if !open {
						closed = true
						break drain
					}
					inflight, sent = append(inflight, tx), sent+1
				default:
					break drain
				}
			}
			if len(inflight) == 0 {
				t.Fatalf("seed %d: dispatcher closed after %d of %d transactions", seed, sent, n)
			}
			mu.Lock()
			for _, tx := range inflight {
				if !received[tx] {
					received[tx] = true
					for _, p := range prerequisites[tx] {
						if !completed[p] {
							fail("dispatched before a prerequisite committed")
						}
					}
				}
			}
			pick := rng.Intn(len(inflight))
			tx := inflight[pick]
			inflight = append(inflight[:pick], inflight[pick+1:]...)
			completed[tx] = true
			mu.Unlock()
			finished++
			done <- tx
		}
		for tx := range out {
			t.Fatalf("seed %d: dispatcher sent transaction %d after all %d were dispatched", seed, tx, n)
		}
		mu.Lock()
		if len(failures) > 0 {
			t.Fatalf("seed %d: %s", seed, failures[0])
		}
		mu.Unlock()
	}
}

// A writer, two readers of its account, then a second writer. Each reader is
// offered once the writer is dispatched; the second writer once both readers
// are, with both unfinished, and again with the one left after a commit.
func TestDispatchDependencyPlanOfferingSequence(t *testing.T) {
	x := testPk(1)
	access := func(writable bool) plannerTransactionAccounts {
		return newPlannerTransactionAccounts(1, func(add func(solana.PublicKey, bool)) { add(x, writable) })
	}
	plan := buildDependencyPlan([]plannerTransactionAccounts{access(true), access(false), access(false), access(true)})
	type offered struct {
		tx         int
		unfinished []uint32
	}
	out, done := make(chan int, 4), make(chan int, 4)
	offers := make(chan offered, 8)
	go dispatchDependencyPlanOffering(plan, out, done, 2, func(tx int, unfinished []uint32) {
		offers <- offered{tx, append([]uint32(nil), unfinished...)}
	})
	expect := func(tx int, unfinished ...uint32) {
		t.Helper()
		got := <-offers
		if got.tx != tx || !slices.Equal(got.unfinished, unfinished) {
			t.Fatalf("offer = %d %v, want %d %v", got.tx, got.unfinished, tx, unfinished)
		}
	}
	if got := <-out; got != 0 {
		t.Fatalf("first dispatch = %d, want 0", got)
	}
	expect(1, 0)
	expect(2, 0)
	done <- 0
	readers := []int{<-out, <-out}
	slices.Sort(readers)
	if !slices.Equal(readers, []int{1, 2}) {
		t.Fatalf("readers dispatched = %v", readers)
	}
	expect(3, 1, 2)
	done <- 1
	expect(3, 2)
	done <- 2
	if got := <-out; got != 3 {
		t.Fatalf("last dispatch = %d, want 3", got)
	}
	done <- 3
	if _, open := <-out; open {
		t.Fatal("dispatcher did not close after the last transaction")
	}
	select {
	case got := <-offers:
		t.Fatalf("unexpected offer %d %v", got.tx, got.unfinished)
	default:
	}
}

func TestWorkAheadAdmissible(t *testing.T) {
	x, y, payerA, payerB := testPk(1), testPk(2), testPk(3), testPk(4)
	accounts := func(add func(func(solana.PublicKey, bool))) plannerTransactionAccounts {
		return newPlannerTransactionAccounts(3, add)
	}
	access := []plannerTransactionAccounts{
		accounts(func(a func(solana.PublicKey, bool)) { a(payerA, true); a(x, false) }),             // 0: reads x
		accounts(func(a func(solana.PublicKey, bool)) { a(payerA, true); a(x, false); a(y, true) }), // 1: reads x, writes y
		accounts(func(a func(solana.PublicKey, bool)) { a(payerB, true); a(x, true); a(y, false) }), // 2: writes x, reads y
		accounts(func(a func(solana.PublicKey, bool)) { a(payerA, true); a(testPk(5), true) }),      // 3: unrelated writer
		accounts(func(a func(solana.PublicKey, bool)) { a(payerA, true); a(x, true); a(y, false) }), // 4: like 2, shares payerA
	}
	accessOf := func(i int) plannerTransactionAccounts { return access[i] }
	payerOf := func(i int) solana.PublicKey {
		if i == 2 {
			return payerB
		}
		return payerA
	}
	failing := map[int]bool{}
	predict := func(i, _ int) bool { return failing[i] }
	check := func(name string, prerequisites []uint32, tx, depth int, wantOK bool, wantAssumed int) {
		t.Helper()
		ok, assumed := workAheadAdmissible(accessOf, payerOf, predict, prerequisites, tx, depth, false)
		if ok != wantOK || (ok && assumed != wantAssumed) {
			t.Fatalf("%s: got ok=%v assumed=%d, want ok=%v assumed=%d", name, ok, assumed, wantOK, wantAssumed)
		}
	}
	// Write-after-read only (M1).
	check("pure reader", []uint32{0}, 2, 0, true, 0)
	check("reader that also writes what tx reads", []uint32{1}, 2, 0, false, 0)
	check("unrelated writer", []uint32{3}, 2, 0, true, 0)
	// Failure speculation (M2): 1 writes y, which 2 reads.
	check("conflict without depth", []uint32{1}, 2, 1, false, 0)
	failing[1] = true
	check("predicted failure", []uint32{1}, 2, 1, true, 1)
	check("predicted failure at depth 0", []uint32{1}, 2, 0, false, 0)
	check("predicted failure plus a pure reader", []uint32{0, 1}, 2, 1, true, 1)
	check("predicted failure whose payer tx uses", []uint32{1}, 4, 1, false, 0)
	failing[4] = true
	check("two predicted failures at depth 1", []uint32{1, 4}, 2, 1, false, 0)
	ok, assumed := workAheadAdmissible(accessOf, payerOf, predict, []uint32{1}, 4, 1, true)
	if !ok || assumed != 1 {
		t.Fatalf("with payer prediction, a predicted failure whose payer tx uses must be admissible (ok=%v assumed=%d)", ok, assumed)
	}
}

// An empty plan starts no offer goroutine, and finish must not wait for one
// (a block with no planned transactions hung replay at the tip).
func TestWorkAheadFinishAfterEmptyPlan(t *testing.T) {
	saved := workAheadEnabled
	workAheadEnabled = true
	defer func() { workAheadEnabled = saved }()
	slotCtx := &sealevel.SlotCtx{Accounts: accounts.NewOverlayAccounts(accounts.NewMemAccountsWithLen(1)), Features: features.NewFeaturesDefault()}
	w := newWorkAhead(slotCtx, &b.Block{}, nil, nil)
	if w == nil {
		t.Fatal("work-ahead off")
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		runDependencyPlan(&dependencyPlan{}, 4, w, planSchedule{}, nil, func(int, int) {})
		w.finish(0)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("finish blocked after an empty plan")
	}
}

// With MITHRIL_WORK_AHEAD_NONCE, durable-nonce transactions are eligible and
// their failure is predicted as the nonce rollback publishes the payer: the fee
// taken and a rent-exempt payer's epoch normalized on its pre-fee balance. An
// ordinary failure after it keeps the (now normalized) loaded epoch.
func TestWorkAheadPredictNoncePayer(t *testing.T) {
	saved := workAheadNonce
	defer func() { workAheadNonce = saved }()
	payer, nonce, x, program := testPk(1), testPk(2), testPk(3), testPk(4)
	advance := binary.LittleEndian.AppendUint32(nil, sealevel.SystemProgramInstrTypeAdvanceNonceAccount)
	nonceTx := &solana.Transaction{Signatures: []solana.Signature{{1}}, Message: solana.Message{
		Header:      solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 2},
		AccountKeys: []solana.PublicKey{payer, nonce, x, addresses.SystemProgramAddr, program},
		Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 3, Accounts: []uint16{1, 0}, Data: advance},
			{ProgramIDIndex: 4, Accounts: []uint16{2}}},
	}}
	plainTx := func() *solana.Transaction {
		return &solana.Transaction{Signatures: []solana.Signature{{1}}, Message: solana.Message{
			Header:       solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
			AccountKeys:  []solana.PublicKey{payer, x, program},
			Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 2, Accounts: []uint16{1}}},
		}}
	}
	workAheadNonce = false
	if workAheadEligible(nonceTx) {
		t.Fatal("durable-nonce transactions are ineligible without MITHRIL_WORK_AHEAD_NONCE")
	}
	workAheadNonce = true
	if !workAheadEligible(nonceTx) {
		t.Fatal("durable-nonce transactions are eligible with MITHRIL_WORK_AHEAD_NONCE")
	}
	for _, c := range []struct {
		lamports  uint64
		wantEpoch uint64
	}{{1_000_000, math.MaxUint64}, {500_000, 7}} { // default rent: 890880 exempts an empty account
		parent := accounts.NewMemAccountsWithLen(3)
		if err := parent.SetAccountWithoutLock(payer, &accounts.Account{Key: payer, Lamports: c.lamports, Owner: [32]byte(addresses.SystemProgramAddr), RentEpoch: 7}); err != nil {
			t.Fatal(err)
		}
		overlay := accounts.NewOverlayAccounts(parent)
		slotCtx := &sealevel.SlotCtx{Accounts: overlay, Features: features.NewFeaturesDefault()}
		w := &workAhead{slotCtx: slotCtx, overlay: overlay, txs: []*solana.Transaction{nonceTx, plainTx(), plainTx()}, execute: []bool{true, true, true}}
		w.outcome = make([]atomic.Uint32, 3)
		w.failFee = make([]atomic.Uint64, 3)
		e := &workAheadEntry{nUnfinished: 1}
		e.unfinished[0] = 1
		e.keys, e.versions, _ = workAheadReadSet(slotCtx, overlay, w.txs[2])
		got := w.predictPayers(e, 2)[payer]
		if got == nil || got.Lamports != c.lamports-10_000 || got.RentEpoch != c.wantEpoch {
			t.Fatalf("balance %d: predicted payer = %+v, want %d lamports and rent epoch %d", c.lamports, got, c.lamports-10_000, c.wantEpoch)
		}
	}
}

func TestWorkAheadPredictPayer(t *testing.T) {
	payer, x, program := testPk(1), testPk(2), testPk(3)
	tx := func() *solana.Transaction {
		return &solana.Transaction{
			Signatures: []solana.Signature{{1}},
			Message: solana.Message{
				Header:       solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
				AccountKeys:  []solana.PublicKey{payer, x, program},
				Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 2, Accounts: []uint16{1}}},
			},
		}
	}
	parent := accounts.NewMemAccountsWithLen(3)
	if err := parent.SetAccountWithoutLock(payer, &accounts.Account{Key: payer, Lamports: 1_000_000, Owner: [32]byte(addresses.SystemProgramAddr), RentEpoch: 7}); err != nil {
		t.Fatal(err)
	}
	overlay := accounts.NewOverlayAccounts(parent)
	slotCtx := &sealevel.SlotCtx{Accounts: overlay, Features: features.NewFeaturesDefault()}
	w := &workAhead{slotCtx: slotCtx, overlay: overlay, txs: []*solana.Transaction{tx(), tx(), tx()}, execute: []bool{true, true, true}}
	w.outcome = make([]atomic.Uint32, 3)
	w.failFee = make([]atomic.Uint64, 3)

	fee, ok := w.failureFee(0)
	if !ok || fee != 5000 {
		t.Fatalf("failure fee = %d (%v), want 5000 for one signature and no priority fee", fee, ok)
	}
	e := &workAheadEntry{nUnfinished: 1}
	e.unfinished[0] = 0
	e.keys, e.versions, _ = workAheadReadSet(slotCtx, overlay, w.txs[1])
	overrides := w.predictPayers(e, 1)
	if got := overrides[payer]; got == nil || got.Lamports != 995_000 || got.RentEpoch != 7 {
		t.Fatalf("predicted payer = %+v, want 995000 lamports and the loaded rent epoch", got)
	}

	// The failure path publishes the payer with only the fee taken.
	key := [32]byte(payer)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: payer, Lamports: 995_000, Owner: [32]byte(addresses.SystemProgramAddr), RentEpoch: 7}); err != nil {
		t.Fatal(err)
	}
	if !w.readSetUnchanged(e) {
		t.Fatal("the payer as the failure path publishes it must validate by value")
	}
	// The prerequisite succeeded and spent more.
	if err := overlay.SetAccount(&key, &accounts.Account{Key: payer, Lamports: 900_000, Owner: [32]byte(addresses.SystemProgramAddr), RentEpoch: 7}); err != nil {
		t.Fatal(err)
	}
	if w.readSetUnchanged(e) {
		t.Fatal("any other payer balance must invalidate the early result")
	}
	// A prerequisite that already committed is not charged again.
	w.outcome[0].Store(2)
	if overrides := w.predictPayers(&workAheadEntry{nUnfinished: 1}, 1); overrides != nil {
		t.Fatal("a committed prerequisite must not be predicted")
	}
	// In a run 0 -> 1 -> 2 of one payer, 2 overtaking 1 while 0 is also
	// uncommitted is charged both fees.
	w.outcome[0].Store(0)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: payer, Lamports: 1_000_000, Owner: [32]byte(addresses.SystemProgramAddr), RentEpoch: 7}); err != nil {
		t.Fatal(err)
	}
	run := &workAheadEntry{nUnfinished: 1}
	run.unfinished[0] = 1
	if got := w.predictPayers(run, 2)[payer]; got == nil || got.Lamports != 990_000 {
		t.Fatalf("predicted payer in a run = %+v, want both earlier fees taken (990000)", got)
	}
}

func TestWorkAheadEligible(t *testing.T) {
	payer, program := testPk(9), testPk(8)
	tx := func(keys []solana.PublicKey, programIdx uint16, data []byte) *solana.Transaction {
		return &solana.Transaction{
			Signatures: []solana.Signature{{1}},
			Message: solana.Message{
				AccountKeys:  keys,
				Instructions: []solana.CompiledInstruction{{ProgramIDIndex: programIdx, Data: data}},
			},
		}
	}
	if !workAheadEligible(tx([]solana.PublicKey{payer, program}, 1, nil)) {
		t.Fatal("an ordinary transaction must be eligible")
	}
	for _, loader := range []solana.PublicKey{addresses.BpfLoaderUpgradeableAddr, addresses.BpfLoader2Addr, addresses.BpfLoaderDeprecatedAddr, addresses.LoaderV4Addr} {
		if workAheadEligible(tx([]solana.PublicKey{payer, program, loader}, 1, nil)) {
			t.Fatalf("a transaction naming loader %s must not be eligible", loader)
		}
	}
	advance := binary.LittleEndian.AppendUint32(nil, sealevel.SystemProgramInstrTypeAdvanceNonceAccount)
	if workAheadEligible(tx([]solana.PublicKey{payer, addresses.SystemProgramAddr}, 1, advance)) {
		t.Fatal("a durable-nonce transaction must not be eligible")
	}
	if workAheadEligible(tx([]solana.PublicKey{payer, solana.VoteProgramID}, 1, nil)) {
		t.Fatal("a vote must not be eligible")
	}
}

func TestWorkAheadClaimValidatesReadSet(t *testing.T) {
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
	w := &workAhead{slotCtx: slotCtx, overlay: overlay, txs: []*solana.Transaction{tx, tx, tx, tx}, entries: make([]atomic.Pointer[workAheadEntry], 4)}

	finishedEntry := func() *workAheadEntry {
		e := &workAheadEntry{state: workAheadFinished, finished: make(chan struct{})}
		close(e.finished)
		e.keys, e.versions, _ = workAheadReadSet(slotCtx, overlay, tx)
		e.output.ExecCtx = &sealevel.ExecutionCtx{}
		return e
	}

	w.entries[0].Store(finishedEntry())
	if out, release := w.claim(0); out == nil || out.ExecCtx == nil {
		t.Fatal("an unchanged read set must be adopted")
	} else {
		release()
	}

	w.entries[1].Store(finishedEntry())
	key := [32]byte(other)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: other, Lamports: 11}); err != nil {
		t.Fatal(err)
	}
	if out, _ := w.claim(1); out != nil {
		t.Fatal("a store to an account in the read set must invalidate the execution")
	}

	w.entries[2].Store(&workAheadEntry{state: workAheadQueued, finished: make(chan struct{})})
	if out, _ := w.claim(2); out != nil || w.entries[2].Load().state != workAheadDropped {
		t.Fatal("an execution that never started must be dropped at the transaction's turn")
	}

	running := finishedEntry()
	running.state = workAheadRunning
	running.finished = make(chan struct{})
	w.entries[3].Store(running)
	go func() {
		running.mu.Lock()
		running.state = workAheadFinished
		close(running.finished)
		running.mu.Unlock()
	}()
	if out, release := w.claim(3); out == nil {
		t.Fatal("a running execution must be waited for and adopted")
	} else {
		release()
	}

	// A running execution whose read set already changed is abandoned, and
	// claim returns without waiting for it (with MITHRIL_WORK_AHEAD_JOIN_DOOMED
	// it joins first: TestWorkAheadClaimJoinsAbandoned).
	stale := finishedEntry()
	stale.state = workAheadRunning
	stale.finished = make(chan struct{})
	w.entries[0].Store(stale)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: other, Lamports: 12}); err != nil {
		t.Fatal(err)
	}
	claimed := make(chan *LoadAndExecuteTransactionOutput, 1)
	go func() {
		out, _ := w.claim(0)
		claimed <- out
	}()
	select {
	case out := <-claimed:
		if out != nil || !stale.abandoned {
			t.Fatal("a stale running execution must be abandoned, not adopted")
		}
	case <-time.After(time.Second):
		t.Fatal("claim waited for a doomed execution")
	}
	close(stale.finished)
}

func TestWorkAheadAccountPredictor(t *testing.T) {
	payerA, payerB, payerC, x, y := testPk(1), testPk(2), testPk(3), testPk(4), testPk(5)
	tx := func(readonlyUnsigned uint8, keys ...solana.PublicKey) *solana.Transaction {
		return &solana.Transaction{Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: readonlyUnsigned},
			AccountKeys: keys,
		}}
	}
	w := &workAhead{txs: []*solana.Transaction{
		tx(0, payerA, x),    // 0 writes x
		tx(0, payerB, y),    // 1 writes y
		tx(1, payerA, x, y), // 2 writes x, reads y: last writer of x is 0
		tx(0, payerC, x, y), // 3 writes x and y: last writers 2 (x) and 1 (y)
		tx(0, payerA),       // 4 writes only its payer
	}}
	w.access = make([]atomic.Pointer[plannerTransactionAccounts], len(w.txs))
	w.outcome = make([]atomic.Uint32, len(w.txs))
	w.buildLastWriters()
	if got, want := w.lastWriter, []int32{-1, -1, 0, 2, -1}; !slices.Equal(got, want) {
		t.Fatalf("last writers = %v, want %v (payer excluded, most recent writer wins)", got, want)
	}
	saved := workAheadPredict
	workAheadPredict = "account"
	defer func() { workAheadPredict = saved }()
	w.outcome[0].Store(1)
	w.outcome[2].Store(2)
	if w.predictFailureFor(2, 0) {
		t.Fatal("a successful last writer must not predict failure")
	}
	if !w.predictFailureFor(3, 0) {
		t.Fatal("a failed last writer must predict failure")
	}
	if w.predictFailureFor(4, 0) {
		t.Fatal("a transaction with no earlier writer must not be predicted to fail")
	}
}

// A chain A -> B -> C of writers of one account: B is offered when A is
// dispatched; C only once B is dispatched, or as soon as B's early execution is
// reported started.
func TestDispatchDependencyPlanEarlyStartOffers(t *testing.T) {
	x := testPk(1)
	writer := func() plannerTransactionAccounts {
		return newPlannerTransactionAccounts(1, func(add func(solana.PublicKey, bool)) { add(x, true) })
	}
	plan := buildDependencyPlan([]plannerTransactionAccounts{writer(), writer(), writer()})
	out, done, started := make(chan int, 3), make(chan int, 3), make(chan int, 3)
	type offered struct {
		tx         int
		unfinished []uint32
	}
	offers := make(chan offered, 8)
	go dispatchDependencyPlanWith(plan, out, done, dispatchHooks{
		maxUnfinished: 1,
		offer: func(tx int, unfinished []uint32) {
			offers <- offered{tx, append([]uint32(nil), unfinished...)}
		},
		started: started,
	})
	if got := <-out; got != 0 {
		t.Fatalf("first dispatch = %d", got)
	}
	if got := <-offers; got.tx != 1 || !slices.Equal(got.unfinished, []uint32{0}) {
		t.Fatalf("offer = %v, want B with A unfinished", got)
	}
	started <- 1
	if got := <-offers; got.tx != 2 || !slices.Equal(got.unfinished, []uint32{1}) {
		t.Fatalf("offer = %v, want C with B unfinished once B started early", got)
	}
	done <- 0
	if got := <-out; got != 1 {
		t.Fatalf("second dispatch = %d", got)
	}
	done <- 1
	if got := <-out; got != 2 {
		t.Fatalf("third dispatch = %d", got)
	}
	done <- 2
	if _, open := <-out; open {
		t.Fatal("dispatcher did not close")
	}
	select {
	case got := <-offers:
		t.Fatalf("unexpected offer %v: B's turn must not offer C again", got)
	default:
	}
}

func TestWorkAheadProgramPredictor(t *testing.T) {
	// Unique program ids keep the process-wide program table free of other tests.
	botProgram, swapProgram, pool, payerA, payerB := testPk(0x61), testPk(0x62), testPk(0x63), testPk(0x64), testPk(0x65)
	tx := func(payer solana.PublicKey, programs ...solana.PublicKey) *solana.Transaction {
		keys := []solana.PublicKey{payer, pool}
		var instrs []solana.CompiledInstruction
		for _, p := range append([]solana.PublicKey{addresses.ComputeBudgetProgramAddr}, programs...) {
			keys = append(keys, p)
			instrs = append(instrs, solana.CompiledInstruction{ProgramIDIndex: uint16(len(keys) - 1)})
		}
		return &solana.Transaction{Message: solana.Message{
			Header:       solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: uint8(len(programs) + 1)},
			AccountKeys:  keys,
			Instructions: instrs,
		}}
	}
	txs := []*solana.Transaction{
		tx(payerA, botProgram, swapProgram), // 0
		tx(payerB, swapProgram, botProgram), // 1: same combination, other order
		tx(payerA, swapProgram),             // 2: different combination
	}
	w := &workAhead{txs: txs, execute: []bool{true, true, true}, acctFails: map[solana.PublicKey][2]uint32{}}
	w.access = make([]atomic.Pointer[plannerTransactionAccounts], len(txs))
	w.outcome = make([]atomic.Uint32, len(txs))
	w.durNs = make([]atomic.Int64, len(txs))
	w.progKey = make([]atomic.Uint64, len(txs))

	if w.programKey(0) != w.programKey(1) {
		t.Fatal("program key must not depend on instruction order or compute budget")
	}
	if w.programKey(0) == w.programKey(2) {
		t.Fatal("different program combinations must have different keys")
	}

	// The bot combination failed 9 of 10 times at ~400us; the plain swap
	// succeeded 10 times at ~50us.
	for i := range 10 {
		recordProgramStat(w.programKey(0), i != 0, 400*time.Microsecond)
		recordProgramStat(w.programKey(2), false, 50*time.Microsecond)
	}
	if p := w.failureProbability(0); p < 0.5 {
		t.Fatalf("bot failure probability = %.2f, want >= 0.5", p)
	}
	if p := w.failureProbability(2); p >= 0.5 {
		t.Fatalf("swap failure probability = %.2f, want < 0.5", p)
	}
	if d := w.expectedDuration(0); d < 300*time.Microsecond || d > 500*time.Microsecond {
		t.Fatalf("expected bot duration = %v, want about 400us", d)
	}

	// observe: a committed failure counts against the pool, not the payer.
	w.outcome[0].Store(2)
	w.durNs[0].Store(int64(400 * time.Microsecond))
	w.observe(0)
	if c := w.acctFails[pool]; c != [2]uint32{1, 1} {
		t.Fatalf("pool counts = %v, want one failure of one", c)
	}
	if _, ok := w.acctFails[payerA]; ok {
		t.Fatal("the fee payer must not count as a pool account")
	}

	saved := workAheadPredict
	savedGain := workAheadMinGain
	savedUnchanged := workAheadUnchanged
	defer func() { workAheadPredict, workAheadMinGain, workAheadUnchanged = saved, savedGain, savedUnchanged }()
	workAheadPredict = "program"
	workAheadUnchanged = false // this test is about failure prediction only
	workAheadMinGain = 0
	if !w.predictFailureFor(1, 2) || w.predictFailureFor(2, 1) {
		t.Fatal("the program predictor must flag the bot combination and not the plain swap")
	}
	// Expected gain = p(fail) x min(400us, 50us) is well under 100us.
	workAheadMinGain = 100 * time.Microsecond
	if w.predictFailureFor(1, 2) {
		t.Fatal("the gain gate must reject a predecessor whose expected overlap is too small")
	}
	workAheadMinGain = 10 * time.Microsecond
	if !w.predictFailureFor(1, 2) {
		t.Fatal("the gain gate must admit a predecessor above the threshold")
	}
}

// The waste budget is a hard bound in compute units: offers stop once discarded
// work plus outstanding reservations reach it; a run queued before that point
// is refused at launch; and concurrent reservations never exceed the budget.
func TestWorkAheadWasteBudget(t *testing.T) {
	saved := workAheadWasteBudgetKCU
	defer func() { workAheadWasteBudgetKCU = saved }()
	workAheadWasteBudgetKCU = 1000
	tx := &solana.Transaction{Message: solana.Message{
		AccountKeys:  []solana.PublicKey{testPk(1), addresses.ComputeBudgetProgramAddr, testPk(3)},
		Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 1, Data: []byte{sealevel.ComputeBudgetInstrTypeSetComputeUnitLimit, 0x20, 0xa1, 0x07, 0x00}}, {ProgramIDIndex: 2}},
	}} // 500,000 CU requested: 501 kCU reserved
	w := &workAhead{entries: make([]atomic.Pointer[workAheadEntry], 3), txs: []*solana.Transaction{tx, tx, tx}}

	// fresh offer over budget
	w.wastedKCU.Store(1000)
	w.offer(1, []uint32{0})
	if e := w.entries[1].Load(); e == nil || e.state != workAheadDropped || w.stats.overBudget.Load() != 1 {
		t.Fatalf("offer over budget left entry %+v", e)
	}

	// queued before the threshold, launched after it
	w.wastedKCU.Store(0)
	e := &workAheadEntry{state: workAheadQueued, finished: make(chan struct{})}
	w.entries[2].Store(e)
	w.wastedKCU.Store(600)
	w.run(2)
	if e.state != workAheadDropped || w.reservedKCU.Load() != 0 {
		t.Fatalf("launch over budget: state %d, reserved %d", e.state, w.reservedKCU.Load())
	}

	// concurrent reservations stay within the budget
	w.wastedKCU.Store(0)
	var granted atomic.Int64
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w.reserve(&workAheadEntry{}, tx) {
				granted.Add(1)
			}
		}()
	}
	wg.Wait()
	if granted.Load() != 1 || w.reservedKCU.Load() != 501 {
		t.Fatalf("granted %d reservations, %d kCU reserved; want 1 within 1000 kCU", granted.Load(), w.reservedKCU.Load())
	}
}

// With MITHRIL_WORK_AHEAD_JOIN_DOOMED, claim joins an invalid early execution
// that is still running before the canonical execution may proceed, so the two
// never overlap.
func TestWorkAheadClaimJoinsAbandoned(t *testing.T) {
	savedCancel, savedJoin := workAheadCancel, workAheadJoinDoomed
	workAheadCancel, workAheadJoinDoomed = true, true
	defer func() { workAheadCancel, workAheadJoinDoomed = savedCancel, savedJoin }()
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
	e := &workAheadEntry{state: workAheadRunning, finished: make(chan struct{})}
	e.keys, e.versions, e.base = workAheadReadSet(slotCtx, overlay, tx)
	w.entries[0].Store(e)
	key := [32]byte(other)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: other, Lamports: 11}); err != nil {
		t.Fatal(err)
	}
	var ended atomic.Bool
	go func() {
		time.Sleep(20 * time.Millisecond)
		ended.Store(true)
		close(e.finished)
	}()
	if out, _ := w.claim(0); out != nil {
		t.Fatal("an invalid running execution must not be adopted")
	}
	if !e.cancel.Load() {
		t.Fatal("invalid running attempt was not cancelled")
	}
	if !ended.Load() || !e.abandoned {
		t.Fatal("claim returned before the abandoned execution ended")
	}
}

// A transaction reaches its turn while its early execution, now doomed (a
// prerequisite predicted to fail succeeded), is still running, as transactions
// crafted to fail predictions would arrange. claim returns at once, so the
// doomed run's length is not added to the chain; when the doomed run ends it
// drops its result and is charged as waste, and finish still joins it before
// the block ends.
func TestWorkAheadDoomedRunDoesNotDelayItsTurn(t *testing.T) {
	saved := workAheadJoinDoomed
	workAheadJoinDoomed = false
	defer func() { workAheadJoinDoomed = saved }()
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
	e := &workAheadEntry{state: workAheadRunning, finished: make(chan struct{}), reservedKCU: 200}
	w.reservedKCU.Store(200)
	e.keys, e.versions, e.base = workAheadReadSet(slotCtx, overlay, tx)
	w.entries[0].Store(e)
	key := [32]byte(other)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: other, Lamports: 11}); err != nil {
		t.Fatal(err)
	}

	claimed := make(chan *LoadAndExecuteTransactionOutput, 1)
	go func() {
		out, release := w.claim(0)
		release()
		claimed <- out
	}()
	select {
	case out := <-claimed:
		if out != nil {
			t.Fatal("a doomed early execution was adopted")
		}
	case <-time.After(time.Second):
		t.Fatal("claim waited for the doomed execution")
	}
	if !e.abandoned || w.stats.unjoined.Load() != 1 {
		t.Fatalf("abandoned %v, unjoined %d", e.abandoned, w.stats.unjoined.Load())
	}

	finished := make(chan struct{})
	go func() {
		w.finish(0)
		close(finished)
	}()
	select {
	case <-finished:
		t.Fatal("finish returned while a doomed execution was still running")
	case <-time.After(20 * time.Millisecond):
	}
	w.ran(0, e, LoadAndExecuteTransactionOutput{}, false, false) // the doomed run ends
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("finish did not return after the doomed execution ended")
	}
	if e.state != workAheadDropped || w.reservedKCU.Load() != 0 || w.wastedKCU.Load() != e.usedKCU || e.usedKCU == 0 {
		t.Fatalf("state %d, reserved %d, wasted %d (used %d)", e.state, w.reservedKCU.Load(), w.wastedKCU.Load(), e.usedKCU)
	}
}

// Once a block's discarded early compute reaches MITHRIL_WORK_AHEAD_WASTE_CAP_KCU
// no early execution is admitted or launched, whatever the waste budget allows.
func TestWorkAheadWasteCap(t *testing.T) {
	saved := workAheadWasteCapKCU
	workAheadWasteCapKCU = 100
	defer func() { workAheadWasteCapKCU = saved }()
	tx := &solana.Transaction{Message: solana.Message{AccountKeys: []solana.PublicKey{testPk(1)}}}
	w := &workAhead{entries: make([]atomic.Pointer[workAheadEntry], 3), txs: []*solana.Transaction{tx, tx, tx}}

	w.wastedKCU.Store(99)
	if w.overWasteCap() {
		t.Fatal("capped below the cap")
	}
	w.wastedKCU.Store(100)
	w.offer(1, []uint32{0})
	if e := w.entries[1].Load(); e == nil || e.state != workAheadDropped || w.stats.wasteCapped.Load() != 1 {
		t.Fatalf("offer at the cap left entry %+v", e)
	}
	e := &workAheadEntry{state: workAheadQueued, finished: make(chan struct{})}
	w.entries[2].Store(e)
	w.run(2)
	if e.state != workAheadDropped || w.reservedKCU.Load() != 0 || w.stats.wasteCapped.Load() != 2 {
		t.Fatalf("launch at the cap: state %d, reserved %d, capped %d", e.state, w.reservedKCU.Load(), w.stats.wasteCapped.Load())
	}
}

// Inputs compare strictly: zero-lamport accounts differ by owner, data or flags,
// and a zero-lamport account differs from a missing one.
func TestSpecSameInputStrict(t *testing.T) {
	a := &accounts.Account{Key: testPk(1), Owner: [32]byte(addresses.SystemProgramAddr)}
	b := &accounts.Account{Key: testPk(1), Owner: [32]byte(testPk(9))}
	if specSameInput(a, b) || specSameInput(a, nil) || !specSameInput(a, a) || !specSameInput(nil, nil) {
		t.Fatal("strict input equality")
	}
	if !specSameAccount(a, b) {
		t.Fatal("output comparison still treats zero-lamport accounts as equal")
	}
}

// compareOutputs catches differences in fields adoption consumes, failures included.
func TestCompareOutputs(t *testing.T) {
	fail := func(fee uint64) *LoadAndExecuteTransactionOutput {
		return &LoadAndExecuteTransactionOutput{
			ProcessingResult: TransactionProcessingResult{TransactionError: &TransactionError{ErrorType: TransactionErrorInstructionError}},
			FeeInfo:          &fees.TxFeeInfo{TotalFee: fee},
		}
	}
	if d := compareOutputs(fail(5000), fail(5000)); d != "" {
		t.Fatalf("equal outputs differ: %s", d)
	}
	if d := compareOutputs(fail(5000), fail(10000)); d == "" {
		t.Fatal("a fee difference on a failed result must be reported")
	}
	// an error's account index is compared by value, not by pointer
	idxA, idxB, idxC := uint8(2), uint8(2), uint8(3)
	withIndex := func(i *uint8) *LoadAndExecuteTransactionOutput {
		o := fail(5000)
		o.ProcessingResult.TransactionError.AccountIndex = i
		return o
	}
	if d := compareOutputs(withIndex(&idxA), withIndex(&idxB)); d != "" {
		t.Fatalf("equal account indexes differ: %s", d)
	}
	if d := compareOutputs(withIndex(&idxA), withIndex(&idxC)); d == "" {
		t.Fatal("different account indexes must be reported")
	}
	if d := compareOutputs(withIndex(&idxA), withIndex(nil)); d == "" {
		t.Fatal("a missing account index must be reported")
	}
	noop := fail(5000)
	noop.ProcessedAsNoOp = true
	if d := compareOutputs(fail(5000), noop); d == "" {
		t.Fatal("a no-op classification difference must be reported")
	}
}

// With the value check, an account stored again with identical content still
// validates, but only when no version moved while the execution ran, and never
// when the content differs.
func TestWorkAheadValueCheck(t *testing.T) {
	saved := workAheadValueCheck
	defer func() { workAheadValueCheck = saved }()
	payer, pool := testPk(1), testPk(2)
	parent := accounts.NewMemAccountsWithLen(2)
	for _, key := range []solana.PublicKey{payer, pool} {
		if err := parent.SetAccountWithoutLock(key, &accounts.Account{Key: key, Lamports: 10, Data: []byte{1, 2, 3}}); err != nil {
			t.Fatal(err)
		}
	}
	overlay := accounts.NewOverlayAccounts(parent)
	slotCtx := &sealevel.SlotCtx{Accounts: overlay}
	tx := &solana.Transaction{Message: solana.Message{AccountKeys: []solana.PublicKey{payer, pool}}}
	w := &workAhead{slotCtx: slotCtx, overlay: overlay}
	e := &workAheadEntry{}
	e.keys, e.versions, e.base = workAheadReadSet(slotCtx, overlay, tx)
	e.stable = w.versionsUnchanged(e)
	key := [32]byte(pool)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: pool, Lamports: 10, Data: []byte{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	workAheadValueCheck = false
	if w.readSetUnchanged(e) {
		t.Fatal("without the value check a new version invalidates")
	}
	workAheadValueCheck = true
	if !w.readSetUnchanged(e) {
		t.Fatal("an identical new version validates with the value check")
	}
	e.stable = false
	if w.readSetUnchanged(e) || !w.readSetMatches(e, true) {
		t.Fatal("a version that moved during the execution only passes the running-time check")
	}
	e.stable = true
	if err := overlay.SetAccount(&key, &accounts.Account{Key: pool, Lamports: 10, Data: []byte{1, 2, 4}}); err != nil {
		t.Fatal(err)
	}
	if w.readSetUnchanged(e) {
		t.Fatal("different content never validates")
	}
}

// resetUnchangedStats empties the process-wide unchanged table, so a test's
// history does not depend on earlier tests or repeated runs.
func resetUnchangedStats() { resetUnchangedHistory() }

// Without the value check the unchanged table learns only what the version
// check can adopt: an account the transaction never borrowed for writing. One
// it wrote back with identical content counts as changed, because its commit
// installs a new version. No write originals are needed.
func TestWorkAheadUnchangedLearningVersionOnly(t *testing.T) {
	saved, savedValue, savedRole := workAheadUnchanged, workAheadValueCheck, workAheadUnchangedByRole
	defer func() {
		workAheadUnchanged, workAheadValueCheck, workAheadUnchangedByRole = saved, savedValue, savedRole
	}()
	workAheadUnchanged, workAheadValueCheck, workAheadUnchangedByRole = true, false, false
	resetUnchangedStats()
	program, payer, untouched, rewritten := testPk(0x81), testPk(0x82), testPk(0x83), testPk(0x84)
	commit := func(touchUntouched bool) {
		execCtx := &sealevel.ExecutionCtx{TransactionContext: &sealevel.TransactionCtx{
			AllInstructions: []sealevel.Instruction{{ProgramId: program}},
			Accounts: sealevel.TransactionAccounts{
				Accounts:  []*accounts.Account{{Key: payer, Lamports: 1}, {Key: untouched, Lamports: 5}, {Key: rewritten, Lamports: 7}},
				Touched:   []bool{true, touchUntouched, true},
				AcctMetas: []*sealevel.AccountMeta{{Pubkey: payer, IsWritable: true}, {Pubkey: untouched, IsWritable: true}, {Pubkey: rewritten, IsWritable: true}},
			},
		}}
		learnUnchanged(execCtx)
	}
	for range 3 {
		commit(false)
	}
	key := programKeyOf([]solana.PublicKey{program})
	if !predictsUnchanged(unchangedKey{key, [32]byte(untouched)}) {
		t.Fatal("an account never borrowed for writing is predicted unchanged")
	}
	if predictsUnchanged(unchangedKey{key, [32]byte(rewritten)}) {
		t.Fatal("an account written back, even identically, is not predicted unchanged without the value check")
	}
	if predictsUnchanged(unchangedKey{key, [32]byte(payer)}) {
		t.Fatal("the fee payer is not learned")
	}
	commit(true)
	if predictsUnchanged(unchangedKey{key, [32]byte(untouched)}) {
		t.Fatal("a commit that borrowed the account for writing stops the prediction")
	}
}

// Commits teach the unchanged table: an untouched writable account, or a written
// one equal to its version before the write, is unchanged; the payer is skipped.
// Overtaking needs every shared account predicted unchanged and the payer not shared.
func TestWorkAheadUnchangedLearning(t *testing.T) {
	saved, savedValue, savedRole := workAheadUnchanged, workAheadValueCheck, workAheadUnchangedByRole
	defer func() {
		workAheadUnchanged, workAheadValueCheck, workAheadUnchangedByRole = saved, savedValue, savedRole
	}()
	workAheadUnchanged, workAheadValueCheck, workAheadUnchangedByRole = true, true, false
	resetUnchangedStats()
	program, payer, config, pool := testPk(0x71), testPk(0x72), testPk(0x73), testPk(0x74)
	original := &accounts.Account{Key: config, Lamports: 5, Data: []byte{9}}
	commit := func(poolChanged bool) {
		poolAfter := &accounts.Account{Key: pool, Lamports: 7}
		if poolChanged {
			poolAfter = &accounts.Account{Key: pool, Lamports: 8}
		}
		execCtx := &sealevel.ExecutionCtx{TransactionContext: &sealevel.TransactionCtx{
			AllInstructions: []sealevel.Instruction{{ProgramId: program}},
			Accounts: sealevel.TransactionAccounts{
				Accounts:  []*accounts.Account{{Key: payer, Lamports: 1}, {Key: config, Lamports: 5, Data: []byte{9}}, poolAfter},
				Touched:   []bool{true, true, true},
				Originals: []*accounts.Account{nil, original, {Key: pool, Lamports: 7}},
				AcctMetas: []*sealevel.AccountMeta{{Pubkey: payer, IsWritable: true}, {Pubkey: config, IsWritable: true}, {Pubkey: pool, IsWritable: true}},
			},
		}}
		learnUnchanged(execCtx)
	}
	for range 3 {
		commit(false)
	}
	key := programKeyOf([]solana.PublicKey{program})
	if !predictsUnchanged(unchangedKey{key, [32]byte(config)}) || !predictsUnchanged(unchangedKey{key, [32]byte(pool)}) {
		t.Fatal("three unchanged commits predict unchanged")
	}
	if predictsUnchanged(unchangedKey{key, [32]byte(payer)}) {
		t.Fatal("the fee payer is not learned")
	}
	commit(true)
	if predictsUnchanged(unchangedKey{key, [32]byte(pool)}) || !predictsUnchanged(unchangedKey{key, [32]byte(config)}) {
		t.Fatal("a changed commit stops predicting that account")
	}

	mk := func(p solana.PublicKey, keys ...solana.PublicKey) *solana.Transaction {
		all := append([]solana.PublicKey{p}, keys...)
		all = append(all, program)
		return &solana.Transaction{Message: solana.Message{
			Header:       solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
			AccountKeys:  all,
			Instructions: []solana.CompiledInstruction{{ProgramIDIndex: uint16(len(all) - 1)}},
		}}
	}
	other := testPk(0x75)
	txs := []*solana.Transaction{mk(payer, config, pool), mk(other, config), mk(other, pool), mk(payer, config)}
	w := &workAhead{txs: txs}
	w.access = make([]atomic.Pointer[plannerTransactionAccounts], len(txs))
	w.progKey = make([]atomic.Uint64, len(txs))
	if !w.predictUnchangedFor(0, 1) {
		t.Fatal("a successor sharing only the predicted-unchanged config may overtake")
	}
	if w.predictUnchangedFor(0, 2) {
		t.Fatal("a successor sharing the pool, which last changed, may not")
	}
	if w.predictUnchangedFor(0, 3) {
		t.Fatal("a successor sharing the predecessor's fee payer may not")
	}
}

// No prediction where an earlier validation stage reads slot state that the
// prediction would change: a payer drained to zero (the loader treats
// zero-lamport accounts specially), or the transaction's own nonce account
// (the age check reads it).
func TestWorkAheadPredictPayerGuards(t *testing.T) {
	saved := workAheadNonce
	defer func() { workAheadNonce = saved }()
	workAheadNonce = true
	payer, x, program := testPk(1), testPk(2), testPk(3)
	plain := func() *solana.Transaction {
		return &solana.Transaction{Signatures: []solana.Signature{{1}}, Message: solana.Message{
			Header:       solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
			AccountKeys:  []solana.PublicKey{payer, x, program},
			Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 2, Accounts: []uint16{1}}},
		}}
	}
	advance := binary.LittleEndian.AppendUint32(nil, sealevel.SystemProgramInstrTypeAdvanceNonceAccount)
	other := testPk(4)
	usesPayerAsNonce := &solana.Transaction{Signatures: []solana.Signature{{1}}, Message: solana.Message{
		Header:       solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
		AccountKeys:  []solana.PublicKey{other, payer, addresses.SystemProgramAddr},
		Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 2, Accounts: []uint16{1, 0}, Data: advance}},
	}}
	for _, c := range []struct {
		name     string
		lamports uint64
		next     *solana.Transaction
	}{{"drained payer", 5000, plain()}, {"own nonce account", 1_000_000, usesPayerAsNonce}} {
		parent := accounts.NewMemAccountsWithLen(3)
		if err := parent.SetAccountWithoutLock(payer, &accounts.Account{Key: payer, Lamports: c.lamports, Owner: [32]byte(addresses.SystemProgramAddr)}); err != nil {
			t.Fatal(err)
		}
		overlay := accounts.NewOverlayAccounts(parent)
		slotCtx := &sealevel.SlotCtx{Accounts: overlay, Features: features.NewFeaturesDefault()}
		w := &workAhead{slotCtx: slotCtx, overlay: overlay, txs: []*solana.Transaction{plain(), c.next}, execute: []bool{true, true}}
		w.outcome = make([]atomic.Uint32, 2)
		w.failFee = make([]atomic.Uint64, 2)
		e := &workAheadEntry{nUnfinished: 1}
		e.keys, e.versions, _ = workAheadReadSet(slotCtx, overlay, w.txs[1])
		if got := w.predictPayers(e, 1); got != nil || e.predicted != nil {
			t.Fatalf("%s: predicted %+v, want no prediction", c.name, got)
		}
	}
}

// A prerequisite that commits successfully invalidates the early executions
// that assumed it fails: a queued one is dropped, a running one abandoned and
// asked to stop; finished and other entries are untouched.
func TestWorkAheadInvalidateAssumers(t *testing.T) {
	w := &workAhead{entries: make([]atomic.Pointer[workAheadEntry], 5), assumedBy: make([]atomic.Pointer[[]int32], 5)}
	states := map[int]int{1: workAheadQueued, 2: workAheadRunning, 3: workAheadFinished, 4: workAheadQueued}
	for tx, st := range states {
		e := &workAheadEntry{state: st, finished: make(chan struct{})}
		w.entries[tx].Store(e)
	}
	assumers := []int32{1, 2, 3}
	w.assumedBy[0].Store(&assumers)
	oldCancel := workAheadCancel
	workAheadCancel = true
	defer func() { workAheadCancel = oldCancel }()

	w.invalidateAssumers(0)
	if got := w.entries[1].Load().state; got != workAheadDropped {
		t.Fatalf("queued assumer state %d, want dropped", got)
	}
	running := w.entries[2].Load()
	if running.state != workAheadRunning || !running.abandoned || !running.cancel.Load() {
		t.Fatalf("running assumer: state %d abandoned %v cancel %v", running.state, running.abandoned, running.cancel.Load())
	}
	if got := w.entries[3].Load().state; got != workAheadFinished {
		t.Fatalf("finished assumer changed to %d; the turn's read-set check decides it", got)
	}
	if got := w.entries[4].Load().state; got != workAheadQueued {
		t.Fatalf("unrelated entry changed to %d", got)
	}
	if w.stats.earlyDropped.Load() != 1 || w.stats.earlyAbandoned.Load() != 1 {
		t.Fatalf("stats dropped %d abandoned %d", w.stats.earlyDropped.Load(), w.stats.earlyAbandoned.Load())
	}
	w.invalidateAssumers(1) // no assumers recorded
}

// An early execution abandoned by early invalidation must never be adopted,
// even when its read set turned out unchanged (the prerequisite did not store
// the shared account): claim waits for it and executes normally.
func TestWorkAheadClaimNeverAdoptsAbandoned(t *testing.T) {
	w := &workAhead{entries: make([]atomic.Pointer[workAheadEntry], 1)}
	e := &workAheadEntry{state: workAheadRunning, abandoned: true, finished: make(chan struct{})}
	w.entries[0].Store(e)
	go func() {
		e.mu.Lock()
		e.state = workAheadDropped // as run does for an abandoned execution
		close(e.finished)
		e.mu.Unlock()
	}()
	out, release := w.claim(0)
	release()
	if out != nil {
		t.Fatal("adopted an abandoned early execution")
	}
}

// A recovered panic is reported as a panic even when its message is empty,
// and its zero output is never adopted.
func TestWorkAheadEmptyPanicIsAPanic(t *testing.T) {
	saved := workAheadLoadAndExecute
	defer func() { workAheadLoadAndExecute = saved }()
	workAheadLoadAndExecute = func(LoadAndExecuteTransactionInput) LoadAndExecuteTransactionOutput { panic("") }
	out, panicked, msg := workAheadExecute(LoadAndExecuteTransactionInput{})
	if !panicked || msg != "" || workAheadUsable(&out) {
		t.Fatalf("panic(\"\"): panicked %v msg %q usable %v", panicked, msg, workAheadUsable(&out))
	}
}

// claim adopts only a finished result that publishing can use: a zero output
// (no error, no execution context) is discarded even with an unchanged (here
// empty) read set and no panic recorded.
func TestWorkAheadClaimRejectsUnusableOutput(t *testing.T) {
	w := &workAhead{entries: make([]atomic.Pointer[workAheadEntry], 2)}
	w.entries[0].Store(&workAheadEntry{state: workAheadFinished, finished: make(chan struct{})})
	if out, release := w.claim(0); out != nil {
		release()
		t.Fatal("adopted an output with neither an error nor an execution context")
	}
	if w.stats.invalid.Load() != 1 {
		t.Fatalf("invalid %d, want 1", w.stats.invalid.Load())
	}
	w.entries[1].Store(&workAheadEntry{state: workAheadClaimed, finished: make(chan struct{})})
	defer func() {
		if recover() == nil {
			t.Fatal("a turn reached twice must stop, not process the transaction again")
		}
	}()
	w.claim(1)
}

// The waste budget stays a hard bound while results wait for their turn: a
// finished execution keeps the compute it used reserved until it is adopted
// or discarded, and a panicked one keeps its whole reservation. Before, a
// finished result released its reservation at once, so two discarded results
// could together exceed the budget.
func TestWorkAheadBudgetHoldsFinishedResults(t *testing.T) {
	saved := workAheadWasteBudgetKCU
	defer func() { workAheadWasteBudgetKCU = saved }()
	workAheadWasteBudgetKCU = 1000
	tx := &solana.Transaction{Message: solana.Message{
		AccountKeys:  []solana.PublicKey{testPk(1), addresses.ComputeBudgetProgramAddr, testPk(3)},
		Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 1, Data: []byte{sealevel.ComputeBudgetInstrTypeSetComputeUnitLimit, 0x20, 0xa1, 0x07, 0x00}}, {ProgramIDIndex: 2}},
	}} // 500,000 CU requested: 501 kCU reserved
	w := &workAhead{entries: make([]atomic.Pointer[workAheadEntry], 3), txs: []*solana.Transaction{tx, tx, tx}}
	used := func(cus uint64) LoadAndExecuteTransactionOutput {
		ctx := &sealevel.ExecutionCtx{ComputeMeter: cu.NewComputeMeter(1_400_000)}
		require.NoError(t, ctx.ComputeMeter.Consume(cus))
		return LoadAndExecuteTransactionOutput{ExecCtx: ctx}
	}
	launch := func(i int) *workAheadEntry {
		e := &workAheadEntry{state: workAheadRunning, finished: make(chan struct{})}
		w.entries[i].Store(e)
		require.True(t, w.reserve(e, tx))
		return e
	}

	a := launch(0)
	w.ran(0, a, used(450_000), false, false)
	require.Equal(t, int64(451), w.reservedKCU.Load(), "a finished result keeps what it used")
	b := launch(1)
	require.False(t, w.reserve(&workAheadEntry{}, tx), "451 held + 501 running + 501 more exceeds 1000")
	w.ran(1, b, LoadAndExecuteTransactionOutput{}, true, false)
	require.Equal(t, int64(451+501), w.reservedKCU.Load(), "a panicked execution keeps its whole reservation")

	// a is discarded and b adopted: waste plus liability never exceeds the budget
	a.mu.Lock()
	w.settle(a, true)
	a.mu.Unlock()
	b.mu.Lock()
	w.settle(b, false)
	b.mu.Unlock()
	require.Equal(t, int64(451), w.wastedKCU.Load())
	require.Zero(t, w.reservedKCU.Load())
	require.True(t, w.reserve(&workAheadEntry{}, tx), "451 wasted + 501 fits")
	require.False(t, w.reserve(&workAheadEntry{}, tx))
}
