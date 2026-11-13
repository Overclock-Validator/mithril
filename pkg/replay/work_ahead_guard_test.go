package replay

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// A footer mismatch at a slot that ran early executions switches work-ahead
// off and asks for a re-replay; the re-replay's mismatch, other slots and
// other errors pass through unchanged.
func TestWorkAheadFooterShutoff(t *testing.T) {
	workAheadTripped.Store(false)
	defer workAheadTripped.Store(false)
	other := errors.New("other")
	if workAheadFooterShutoff(other) != other || workAheadFooterShutoff(nil) != nil {
		t.Fatal("unrelated errors pass through")
	}
	markWorkAheadRan(100)
	plain := &FooterBankhashMismatch{Slot: 101}
	if err := workAheadFooterShutoff(plain); err != error(plain) || workAheadTripped.Load() {
		t.Fatal("a slot without early executions is an ordinary mismatch")
	}
	wrapped := &streamingFinalizeError{err: fmt.Errorf("tail: %w", &FooterBankhashMismatch{Slot: 100})}
	err := workAheadFooterShutoff(wrapped)
	var spec *SpeculationFooterMismatch
	if !errors.As(err, &spec) || spec.Slot != 100 || !workAheadTripped.Load() {
		t.Fatalf("speculation mismatch not recognised: %v", err)
	}
	if !errors.Is(err, error(wrapped)) {
		t.Fatal("the original error stays reachable")
	}
	again := &FooterBankhashMismatch{Slot: 100}
	if err := workAheadFooterShutoff(again); err != error(again) {
		t.Fatal("the re-replay ran without early executions; its mismatch is an ordinary one")
	}
	slotCtx := &sealevel.SlotCtx{Slot: 200, Accounts: accounts.NewOverlayAccounts(accounts.NewMemAccounts())}
	savedEnabled := workAheadEnabled
	workAheadEnabled = true
	defer func() { workAheadEnabled = savedEnabled }()
	if newWorkAhead(slotCtx, &b.Block{Slot: 200}, nil, nil) != nil {
		t.Fatal("work-ahead must stay off once tripped")
	}
}

// A block that ran early executions is refused unless it carries a footer bank
// hash; the refusal switches work-ahead off and asks for a re-replay. Blocks
// without early executions, and blocks with a hash, are unaffected.
func TestWorkAheadRequiresFooterHash(t *testing.T) {
	workAheadTripped.Store(false)
	defer workAheadTripped.Store(false)
	markWorkAheadRan(300)
	if err := workAheadRequireFooterHash(&b.Block{Slot: 301}, nil); err != nil {
		t.Fatalf("a block without early executions needs no footer hash here: %v", err)
	}
	if err := workAheadRequireFooterHash(&b.Block{Slot: 300, HasExpectedBankhash: true}, nil); err != nil {
		t.Fatalf("a block with a footer hash passes: %v", err)
	}
	if err := workAheadRequireFooterHash(nil, nil); err != nil {
		t.Fatal("no block, no refusal")
	}
	err := workAheadRequireFooterHash(&b.Block{Slot: 300}, []byte{1})
	if err == nil {
		t.Fatal("a block that ran early executions and has no footer hash must be refused")
	}
	shut := workAheadFooterShutoff(fmt.Errorf("finalize: %w", err))
	var spec *SpeculationFooterMismatch
	if !errors.As(shut, &spec) || spec.Slot != 300 || !workAheadTripped.Load() {
		t.Fatalf("the refusal must go through the shutoff: %v", shut)
	}
	if err := workAheadRequireFooterHash(&b.Block{Slot: 300}, []byte{1}); err != nil {
		t.Fatal("the re-replay runs without early executions and is judged by the ordinary rules")
	}
}

// After the shutoff, the replay of the same slot without early executions says
// which explanation holds: a different hash that matches the footer confirms a
// speculation fault; the same hash, still mismatching, clears speculation; and
// the error a halting node prints carries that.
func TestWorkAheadReplayVerdict(t *testing.T) {
	footer := [32]byte{0xF0}
	withEarly, plainGood, third := []byte{0xA1}, footer[:], []byte{0xC3}
	trip := func(slot uint64, computed []byte, hasFooter bool) {
		t.Helper()
		workAheadTripped.Store(false)
		workAheadLastVerdict.Store(nil)
		markWorkAheadRan(slot)
		var err error = &FooterBankhashMismatch{Slot: slot, Footer: footer, Computed: computed}
		if !hasFooter {
			err = workAheadRequireFooterHash(&b.Block{Slot: slot}, computed)
		}
		var spec *SpeculationFooterMismatch
		if shut := workAheadFooterShutoff(err); !errors.As(shut, &spec) {
			t.Fatalf("the first attempt must trip the shutoff: %v", shut)
		}
	}
	defer workAheadTripped.Store(false)
	last := func() WorkAheadVerdict {
		if v := workAheadLastVerdict.Load(); v != nil {
			return *v
		}
		return ""
	}
	withFooter := func(slot uint64) *b.Block {
		return &b.Block{Slot: slot, HasExpectedBankhash: true, ExpectedBankhash: footer}
	}

	// the plain replay matches the footer: speculation was at fault
	trip(400, withEarly, true)
	if err := workAheadReplayVerdict(withFooter(400), plainGood, nil); err != nil || last() != WorkAheadVerdictSpeculationFault {
		t.Fatalf("verdict %q, err %v", last(), err)
	}
	// the verdict is given once
	workAheadLastVerdict.Store(nil)
	if err := workAheadReplayVerdict(withFooter(400), plainGood, nil); err != nil || last() != "" {
		t.Fatal("a later replay of the slot has nothing to compare with")
	}

	// the plain replay computes the same hash and still mismatches: not speculation
	trip(401, withEarly, true)
	plainMismatch := &FooterBankhashMismatch{Slot: 401, Footer: footer, Computed: withEarly}
	err := workAheadReplayVerdict(withFooter(401), withEarly, plainMismatch)
	var mismatch *FooterBankhashMismatch
	if last() != WorkAheadVerdictNotSpeculation || !errors.As(err, &mismatch) || !bytes.Contains([]byte(err.Error()), []byte("not caused by speculation")) {
		t.Fatalf("verdict %q, err %v", last(), err)
	}
	if again := workAheadFooterShutoff(err); !errors.Is(again, err) {
		t.Fatal("the plain replay's mismatch stays an ordinary mismatch")
	}

	// a third hash: inconclusive
	trip(402, withEarly, true)
	err = workAheadReplayVerdict(withFooter(402), third, &FooterBankhashMismatch{Slot: 402, Footer: footer, Computed: third})
	if last() != WorkAheadVerdictInconclusive || err == nil {
		t.Fatalf("verdict %q, err %v", last(), err)
	}

	// another block at the slot: inconclusive
	trip(403, withEarly, true)
	other := &b.Block{Slot: 403, HasExpectedBankhash: true, ExpectedBankhash: [32]byte{0xEE}}
	if err := workAheadReplayVerdict(other, third, nil); err != nil || last() != WorkAheadVerdictInconclusive {
		t.Fatalf("verdict %q, err %v", last(), err)
	}

	// no footer hash: the attempts agree, or they do not
	trip(404, withEarly, false)
	if err := workAheadReplayVerdict(&b.Block{Slot: 404}, withEarly, nil); err != nil || last() != WorkAheadVerdictConsistentUnchecked {
		t.Fatalf("verdict %q, err %v", last(), err)
	}
	trip(405, withEarly, false)
	if err := workAheadReplayVerdict(&b.Block{Slot: 405}, third, nil); err != nil || last() != WorkAheadVerdictDifferUnchecked {
		t.Fatalf("verdict %q, err %v", last(), err)
	}

	// other slots, and a node whose shutoff never tripped, are untouched
	trip(406, withEarly, true)
	plain := &FooterBankhashMismatch{Slot: 999, Footer: footer, Computed: third}
	workAheadLastVerdict.Store(nil)
	if err := workAheadReplayVerdict(withFooter(999), third, plain); err != error(plain) || last() != "" {
		t.Fatal("a mismatch at another slot is returned unchanged")
	}
	workAheadTripped.Store(false)
	if err := workAheadReplayVerdict(withFooter(406), third, plain); err != error(plain) || last() != "" {
		t.Fatal("without a tripped shutoff there is no verdict to give")
	}
	workAheadSuspect.mu.Lock()
	workAheadSuspect.set = false
	workAheadSuspect.mu.Unlock()
}

// Work-ahead never runs on a locally produced block.
func TestWorkAheadNeverOnLocalProduction(t *testing.T) {
	workAheadTripped.Store(false)
	savedEnabled := workAheadEnabled
	workAheadEnabled = true
	defer func() { workAheadEnabled = savedEnabled }()
	slotCtx := &sealevel.SlotCtx{Slot: 200, Accounts: accounts.NewOverlayAccounts(accounts.NewMemAccounts())}
	if newWorkAhead(slotCtx, &b.Block{Slot: 200}, nil, nil) == nil {
		t.Fatal("replayed block: work-ahead expected")
	}
	if newWorkAhead(slotCtx, &b.Block{Slot: 200, FromLocalProduction: true}, nil, nil) != nil {
		t.Fatal("locally produced block executed early")
	}
}

// An upgradeable program's program data is in the read set, so an upgrade
// committed after the early execution began (a store to the program data)
// invalidates it.
func TestWorkAheadProgramUpgradeInvalidates(t *testing.T) {
	payer, program, programData := testPk(1), testPk(2), testPk(3)
	state := sealevel.UpgradeableLoaderState{Type: sealevel.UpgradeableLoaderStateTypeProgram}
	state.Program.ProgramDataAddress = programData
	var buf bytes.Buffer
	if err := state.MarshalWithEncoder(bin.NewBinEncoder(&buf)); err != nil {
		t.Fatal(err)
	}
	parent := accounts.NewMemAccountsWithLen(3)
	for _, a := range []*accounts.Account{
		{Key: payer, Lamports: 10},
		{Key: program, Lamports: 10, Owner: [32]byte(addresses.BpfLoaderUpgradeableAddr), Executable: true, Data: buf.Bytes()},
		{Key: programData, Lamports: 10, Owner: [32]byte(addresses.BpfLoaderUpgradeableAddr), Data: []byte{1}},
	} {
		if err := parent.SetAccountWithoutLock(a.Key, a); err != nil {
			t.Fatal(err)
		}
	}
	overlay := accounts.NewOverlayAccounts(parent)
	slotCtx := &sealevel.SlotCtx{Accounts: overlay}
	tx := &solana.Transaction{Message: solana.Message{AccountKeys: []solana.PublicKey{payer, program}}}
	keys, versions, _ := workAheadReadSet(slotCtx, overlay, tx)
	found := false
	for _, k := range keys {
		found = found || k == [32]byte(programData)
	}
	if !found {
		t.Fatal("program data missing from the read set")
	}
	w := &workAhead{slotCtx: slotCtx, overlay: overlay, txs: []*solana.Transaction{tx}, entries: make([]atomic.Pointer[workAheadEntry], 1)}
	e := &workAheadEntry{state: workAheadFinished, finished: make(chan struct{}), keys: keys, versions: versions}
	close(e.finished)
	e.output.ExecCtx = &sealevel.ExecutionCtx{}
	w.entries[0].Store(e)
	key := [32]byte(programData)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: programData, Lamports: 10, Owner: [32]byte(addresses.BpfLoaderUpgradeableAddr), Data: []byte{2}}); err != nil {
		t.Fatal(err)
	}
	if out, _ := w.claim(0); out != nil {
		t.Fatal("an early execution survived an upgrade of its program")
	}
}

// An early result that loaded an account outside its read set is never
// adopted: the version check could not have covered it.
func TestWorkAheadReadSetCovers(t *testing.T) {
	payer, other, hidden := testPk(1), testPk(2), testPk(3)
	parent := accounts.NewMemAccountsWithLen(2)
	for _, key := range []solana.PublicKey{payer, other} {
		if err := parent.SetAccountWithoutLock(key, &accounts.Account{Key: key, Lamports: 10}); err != nil {
			t.Fatal(err)
		}
	}
	overlay := accounts.NewOverlayAccounts(parent)
	slotCtx := &sealevel.SlotCtx{Accounts: overlay}
	tx := &solana.Transaction{Message: solana.Message{AccountKeys: []solana.PublicKey{payer, other}}}
	w := &workAhead{slotCtx: slotCtx, overlay: overlay, txs: []*solana.Transaction{tx, tx}, entries: make([]atomic.Pointer[workAheadEntry], 2)}
	entry := func(loaded ...solana.PublicKey) *workAheadEntry {
		e := &workAheadEntry{state: workAheadFinished, finished: make(chan struct{})}
		close(e.finished)
		e.keys, e.versions, _ = workAheadReadSet(slotCtx, overlay, tx)
		txCtx := &sealevel.TransactionCtx{}
		for _, k := range loaded {
			txCtx.Accounts.Accounts = append(txCtx.Accounts.Accounts, &accounts.Account{Key: k})
		}
		e.output.ExecCtx = &sealevel.ExecutionCtx{TransactionContext: txCtx}
		e.uncovered = !w.readSetCovers(e) // as ran does
		return e
	}
	w.entries[0].Store(entry(payer, other, sealevel.SysvarInstructionsAddr))
	if out, release := w.claim(0); out == nil {
		t.Fatal("declared accounts and the instructions sysvar are covered")
	} else {
		release()
	}
	w.entries[1].Store(entry(payer, other, hidden))
	if out, _ := w.claim(1); out != nil || w.stats.uncovered.Load() != 1 {
		t.Fatal("a result that loaded an undeclared account was adopted")
	}
}

// Pressure counts only ready work queued while more were queued than workers
// idle and early executions ran; drain runs from the last commit to the end.
func TestWorkAheadPressure(t *testing.T) {
	var none *workAheadPressure
	none.begin(1) // nil-safe when off
	none.queued(0, 1, 1)
	none.taken(0)
	none.log(0, 0, 0)

	p := &workAheadPressure{}
	p.begin(4)
	p.idleBegin()
	p.queued(0, 1, 3) // an idle worker is waiting: not pressure
	p.idleEnd()
	p.queued(1, 1, 0) // no early execution running: ordinary contention
	p.queued(2, 2, 2) // pressure
	p.taken(0)
	p.taken(1)
	p.taken(2)
	p.taken(2) // taken once
	p.taken(3) // never queued (handed straight to the committing worker)
	if p.enqueued.Load() != 3 || p.pressured.Load() != 1 || p.pressureNs.Load() <= 0 || p.maxWaitNs.Load() != p.pressureNs.Load() {
		t.Fatalf("enqueued %d pressured %d pressure-ns %d max %d", p.enqueued.Load(), p.pressured.Load(), p.pressureNs.Load(), p.maxWaitNs.Load())
	}
	p.started(2)
	p.started(5)
	p.started(3)
	if p.peakRunning.Load() != 5 {
		t.Fatalf("peak running %d", p.peakRunning.Load())
	}
	p.lastCommit(2)
	p.drained()
	if p.busyAtDrain.Load() != 2 || p.drainNs.Load() < 0 {
		t.Fatalf("busy at drain %d, drain %d", p.busyAtDrain.Load(), p.drainNs.Load())
	}
}
